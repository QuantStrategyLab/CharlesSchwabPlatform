#!/usr/bin/env python3
"""Read-only strategy parity check for live Cloud Run services (RS-08).

Reports only non-sensitive identity fields so research can bind to what live
actually runs: serving revision, image digest, package commits stamped inside
the image, manifest trend window and config hashes, strategy_release identity
fields, presence (never values) of config/signal env vars, recent log reason
codes, and the public market-data history length/basis the platform code would
fetch. Never prints account values, positions, tokens, chat ids, account ids or
full configs. Never changes Cloud Run, secrets, traffic, or trading; the image
probe runs with ``--network none`` and no environment.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys

SAFE_ENV_VALUES = {"STRATEGY_PROFILE", "NOTIFY_LANG", "CASH_ONLY_EXECUTION", "IBKR_CASH_ONLY_EXECUTION",
                   "IBKR_DRY_RUN_ONLY", "RUNTIME_TARGET_ENABLED", "INCOME_LAYER_ENABLED",
                   "OPTION_OVERLAY_ENABLED", "OPTION_GROWTH_OVERLAY_ENABLED", "OPTION_INCOME_OVERLAY_ENABLED"}
PRESENCE_PATTERN = re.compile(r"(STRATEGY_CONFIG|MARKET_SIGNAL|FEATURE_SNAPSHOT|RUNTIME_OVERRIDE|TREND|PLUGIN_MOUNTS)")
SENSITIVE_PATTERN = re.compile(r"(TOKEN|SECRET|PASSWORD|KEY|CHAT|ACCOUNT|CREDENTIAL)", re.IGNORECASE)

PROBE = r'''
import hashlib, inspect, json, pathlib
from importlib import metadata as m
out = {}
p = pathlib.Path("/app/UES_REVISION")
out["ues_revision_stamp"] = p.read_text().strip() if p.exists() else None
for dist in ("us-equity-strategies", "quant-platform-kit"):
    try:
        du = json.loads(m.distribution(dist).read_text("direct_url.json") or "{}")
        out[dist] = {"commit": (du.get("vcs_info") or {}).get("commit_id"), "version": m.version(dist)}
    except Exception as exc:
        out[dist] = {"error": type(exc).__name__}
def h(obj, **kw):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str, **kw).encode()).hexdigest()
try:
    from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest as s, tqqq_growth_income_manifest as t
    for name, mod in (("soxl_soxx_trend_income", s), ("tqqq_growth_income", t)):
        c = dict(mod.default_config)
        out[name] = {"trend_ma_window": c.get("trend_ma_window"),
                     "window_keys": {k: v for k, v in c.items() if isinstance(v, (int, float)) and not isinstance(v, bool)
                                     and ("window" in k or "lookback" in k or "rsi" in k or "percentile" in k)},
                     "flags": {k: v for k, v in c.items() if isinstance(v, bool) and ("rsi" in k or "dynamic" in k)},
                     "default_config_sha256": {"compact": h(c, separators=(",", ":")), "spaced": h(c)}}
except Exception as exc:
    out["manifest_error"] = type(exc).__name__
try:
    from quant_platform_kit.common import runtime_inputs as ri
    out["qpk_indicator_builder_source_sha256"] = hashlib.sha256(
        inspect.getsource(ri.build_semiconductor_rotation_indicators_from_history).encode()).hexdigest()
    out["qpk_required_lookback_trend140"] = ri.required_semiconductor_rotation_history_lookback(trend_ma_window=140)
    out["qpk_required_lookback_trend150"] = ri.required_semiconductor_rotation_history_lookback(trend_ma_window=150)
except Exception as exc:
    out["qpk_indicator_error"] = type(exc).__name__
try:
    from quant_platform_kit.ibkr import market_data as imd
    out["ibkr_duration_420D"] = imd._normalize_duration_for_ibkr("420 D")
except Exception as exc:
    out["ibkr_duration_error"] = type(exc).__name__
try:
    import application.runtime_strategy_adapters as a
    if hasattr(a, "fetch_yfinance_historical_candles"):
        src = inspect.getsource(a)
        out["ibkr_yf_period_420D"] = a._duration_to_yfinance_period("420 D")
        out["ibkr_yf_uses_adj_close_first"] = src.find('"Adj Close"') != -1 and src.find('"Adj Close"') < src.find('_frame_column(frame, "Close")')
        out["ibkr_yf_auto_adjust_false"] = "auto_adjust=False" in src
    out["platform_adapter_source_sha256"] = hashlib.sha256(inspect.getsource(a).encode()).hexdigest()
except Exception as exc:
    out["platform_adapter_error"] = type(exc).__name__
try:
    from quant_platform_kit.schwab import market_data as smd
    src = inspect.getsource(smd)
    out["schwab_price_history_two_years"] = "Period.TWO_YEARS" in src
except Exception as exc:
    out["schwab_md_error"] = type(exc).__name__
print("PROBE_JSON " + json.dumps(out, sort_keys=True))
'''

YF_PROBE = r'''
import json
out = {}
try:
    import application.runtime_strategy_adapters as a
    import yfinance as yf
    for sym, dur in (("SOXL", "420 D"), ("SOXX", "420 D"), ("QQQ", "2 Y")):
        c = a.fetch_yfinance_historical_candles(sym, duration=dur)
        out[sym] = {"duration": dur, "rows": len(c), "first": str(c[0]["as_of"])[:10] if c else None,
                    "last": str(c[-1]["as_of"])[:10] if c else None}
        f = yf.download(sym, period=a._duration_to_yfinance_period(dur), interval="1d", auto_adjust=False,
                        progress=False, threads=False)
        cols = [str(x[0] if isinstance(x, tuple) else x) for x in f.columns]
        out[sym]["has_adj_close"] = "Adj Close" in cols
        if "Adj Close" in cols and len(f):
            adj = f["Adj Close"].squeeze(); raw = f["Close"].squeeze()
            out[sym]["adj_over_close_first_row"] = round(float(adj.iloc[0] / raw.iloc[0]), 6)
            out[sym]["adj_over_close_last_row"] = round(float(adj.iloc[-1] / raw.iloc[-1]), 6)
except Exception as exc:
    out["error"] = type(exc).__name__ + ":" + str(exc)[:120]
print("YF_JSON " + json.dumps(out, sort_keys=True))
'''


def gcloud(*args: str) -> str:
    return subprocess.run(["gcloud", *args], check=True, capture_output=True, text=True).stdout


def sha12(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:12]


def env_report(containers: list) -> dict:
    out: dict = {"names": [], "values": {}, "presence": {}, "release": None, "mentions_trend_ma_window": []}
    for c in containers:
        for e in c.get("env", []) or []:
            name = e.get("name", "")
            out["names"].append(name)
            value = e.get("value")
            if value is None:
                if PRESENCE_PATTERN.search(name):
                    out["presence"][name] = "secret_ref"
                continue
            if "trend_ma_window" in value:
                out["mentions_trend_ma_window"].append(name)
            if name in SAFE_ENV_VALUES:
                out["values"][name] = value
            elif name == "RUNTIME_TARGET_JSON":
                try:
                    rt = json.loads(value)
                    rel = rt.get("strategy_release") or {}
                    out["release"] = {k: rel.get(k) for k in ("release_id", "strategy_revision", "config_sha256",
                                                             "manifest_sha256", "plugin_bundle_sha256",
                                                             "effective_session")}
                    binding = ((rt.get("runtime_risk_limits") or {}).get("binding") or {})
                    out["release"]["binding_ues_revision"] = binding.get("ues_revision")
                    out["release"]["strategy_profile"] = rt.get("strategy_profile")
                except (ValueError, AttributeError):
                    out["release"] = {"error": "unparseable"}
                out["presence"][name] = "set:" + sha12(value)
            elif PRESENCE_PATTERN.search(name) and not SENSITIVE_PATTERN.search(name.replace("PLUGIN_MOUNTS", "")):
                out["presence"][name] = ("set:" + sha12(value)) if value.strip() else "empty"
    out["names"] = sorted(n for n in out["names"] if not SENSITIVE_PATTERN.search(n)) + [
        f"<{sum(1 for n in out['names'] if SENSITIVE_PATTERN.search(n))} sensitive-named vars omitted>"]
    return out


def image_probe(image: str) -> dict:
    host = image.split("/")[0]
    if host.endswith(("pkg.dev", "gcr.io")):
        subprocess.run(["gcloud", "auth", "configure-docker", host, "--quiet"], capture_output=True, text=True, check=False)
    pull = subprocess.run(["docker", "pull", "-q", image], capture_output=True, text=True, check=False)
    if pull.returncode != 0:
        return {"status": "PULL_FAILED", "reason": (pull.stderr.strip().splitlines() or ["?"])[-1][:200]}
    res: dict = {"status": "OK"}
    run = subprocess.run(["docker", "run", "--rm", "--network", "none", "--workdir", "/app", "--entrypoint", "python",
                          image, "-c", PROBE], capture_output=True, text=True, timeout=300, check=False)
    line = next((x for x in run.stdout.splitlines() if x.startswith("PROBE_JSON ")), None)
    res["probe"] = json.loads(line[11:]) if line else {"error": (run.stderr.strip().splitlines() or ["?"])[-1][:200]}
    if os.environ.get("PARITY_YF_PROBE") == "1":
        yf = subprocess.run(["docker", "run", "--rm", "--workdir", "/app", "--entrypoint", "python", image, "-c",
                             YF_PROBE], capture_output=True, text=True, timeout=300, check=False)
        line = next((x for x in yf.stdout.splitlines() if x.startswith("YF_JSON ")), None)
        res["public_history_probe"] = json.loads(line[8:]) if line else {
            "error": (yf.stderr.strip().splitlines() or ["?"])[-1][:200]}
    return res


LOG_KEYS = ("strategy_runtime_binding_mismatch", "strategy_runtime_capability_status", "strategy_runtime_risk_reject",
            "market_signal_inputs_loaded")
FIELD_RE = re.compile(r"(profile|reasons|installed_ues_revision|policy_ues_revision|capital_base_status|"
                      r"runtime_risk_status|risk_flags|reference_type|materialized_count|fallback_used)=('[^']*'|\S+)")


def log_report(service: str, project: str, days: int) -> dict:
    flt = (f'resource.type="cloud_run_revision" AND resource.labels.service_name="{service}" AND ('
           + " OR ".join(f'"{k}"' for k in LOG_KEYS) + ")")
    try:
        raw = gcloud("logging", "read", flt, f"--project={project}", f"--freshness={days}d", "--limit=300",
                     "--format=json")
    except subprocess.CalledProcessError as exc:
        return {"status": "READ_FAILED", "reason": (exc.stderr.strip().splitlines() or ["?"])[-1][:200]}
    summary: dict = {}
    for entry in json.loads(raw or "[]"):
        payload = entry.get("jsonPayload") or {}
        text = entry.get("textPayload") or str(payload.get("message") or "") or json.dumps(payload)
        key = next((k for k in LOG_KEYS if k in text), None)
        fields = {k: v.strip("'") for k, v in FIELD_RE.findall(text)}
        rec = (key, entry.get("resource", {}).get("labels", {}).get("revision_name"),
               json.dumps(fields, sort_keys=True))
        day = (entry.get("timestamp") or "")[:10]
        slot = summary.setdefault(rec, {"count": 0, "first": day, "last": day})
        slot["count"] += 1
        slot["first"] = min(slot["first"], day)
        slot["last"] = max(slot["last"], day)
    return {"status": "OK", "days": days, "events": [
        {"key": k, "revision": r, "fields": json.loads(f), **v} for (k, r, f), v in sorted(summary.items())]}


def main() -> int:
    project = os.environ["GCP_PROJECT_ID"]
    region = os.environ.get("CLOUD_RUN_REGION") or "us-central1"
    services = [s.strip() for s in os.environ.get("PARITY_SERVICES", "").split(",") if s.strip()]
    days = int(os.environ.get("PARITY_LOG_DAYS") or "21")
    report: dict = {"project": project, "services": {}}
    probed: dict = {}
    for svc in services:
        item: dict = {}
        try:
            spec = json.loads(gcloud("run", "services", "describe", svc, f"--project={project}",
                                     f"--region={region}", "--format=json"))
        except subprocess.CalledProcessError as exc:
            report["services"][svc] = {"status": "DESCRIBE_FAILED",
                                       "reason": (exc.stderr.strip().splitlines() or ["?"])[-1][:200]}
            continue
        traffic = [t for t in spec.get("status", {}).get("traffic", []) if t.get("percent")]
        item["traffic"] = [{"revision": t.get("revisionName"), "percent": t.get("percent")} for t in traffic]
        item["revisions"] = {}
        for t in traffic:
            rev = t.get("revisionName")
            rspec = json.loads(gcloud("run", "revisions", "describe", rev, f"--project={project}",
                                      f"--region={region}", "--format=json"))
            containers = rspec.get("spec", {}).get("containers", [])
            image = containers[0].get("image") if containers else None
            digest = rspec.get("status", {}).get("imageDigest")
            meta = rspec.get("metadata", {})
            rinfo = {"image": image, "image_digest": digest, "created": meta.get("creationTimestamp"),
                     "env": env_report(containers)}
            ref = digest or image
            if ref:
                if ref not in probed:
                    probed[ref] = image_probe(ref)
                rinfo["image_probe"] = probed[ref]
            item["revisions"][rev] = rinfo
        item["logs"] = log_report(svc, project, days)
        report["services"][svc] = item
    print(json.dumps(report, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
