#!/usr/bin/env python3
"""Read-only RS-08 follow-up checks (A: release evidence, B: Schwab signal price basis).

A) Searches ``PARITY_EVIDENCE_BUCKET`` for objects whose name or content refers
   to the given release ids / evidence sha256 values, then reports which fields
   the bundle declares for ``config_sha256`` (key names only) and whether the
   config payload found in the bundle re-hashes to the released value under a
   few canonical JSON encodings.
B) Reads the last N live execution reports of a profile, takes the reported
   trend-symbol price and 140-day trend MA, and recomputes the MA from public
   yfinance history on a split-only basis and a dividend-adjusted basis.
   Reports only relative-error buckets, the best-matching basis label and
   counts.

Never prints account values, positions, targets, object paths that could carry
account scopes, tokens or full configs. Never writes anything.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
MAX_OBJECT_BYTES = 2_000_000


def gcloud(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gcloud", *args], capture_output=True, text=True, check=check, timeout=600)


def storage_ls(pattern: str) -> tuple[list[str], str | None]:
    res = gcloud("storage", "ls", pattern, check=False)
    if res.returncode != 0:
        err = (res.stderr.strip().splitlines() or ["?"])[-1]
        if "matched no objects" in err or "One or more URLs matched no objects" in err:
            return [], None
        return [], err[:200]
    return [x.strip() for x in res.stdout.splitlines() if x.strip() and not x.strip().endswith(":")], None


_CAT_CACHE: dict[str, str | None] = {}


def storage_cat(uri: str) -> str | None:
    if uri in _CAT_CACHE:
        return _CAT_CACHE[uri]
    _CAT_CACHE[uri] = _storage_cat(uri)
    return _CAT_CACHE[uri]


def _storage_cat(uri: str) -> str | None:
    res = gcloud("storage", "cat", uri, check=False)
    if res.returncode != 0 or len(res.stdout) > MAX_OBJECT_BYTES:
        return None
    return res.stdout


def sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def canonical_hashes(obj: Any) -> dict[str, str]:
    return {
        "sorted_compact": sha(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)),
        "sorted_compact_utf8": sha(json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                                              default=str)),
        "sorted_spaced": sha(json.dumps(obj, sort_keys=True, default=str)),
        "unsorted_compact": sha(json.dumps(obj, separators=(",", ":"), default=str)),
    }


def key_paths(obj: Any, prefix: str = "", depth: int = 0) -> list[str]:
    out: list[str] = []
    if depth > 4:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else str(k)
            out.append(path)
            out.extend(key_paths(v, path, depth + 1))
    return out


def find_values(obj: Any, key: str) -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = []

    def walk(o: Any, path: str) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                p = f"{path}.{k}" if path else k
                if k == key:
                    found.append((p, v))
                walk(v, p)
        elif isinstance(o, list):
            for i, v in enumerate(o[:200]):
                walk(v, f"{path}[{i}]")

    walk(obj, "")
    return found


def part_a(bucket: str) -> dict:
    releases = json.loads(os.environ.get("PARITY_RELEASES_JSON") or "[]")
    names, err = storage_ls(f"gs://{bucket}/**")
    if err:
        return {"status": "STORAGE_LIST_FAILED", "reason": err}
    out: dict = {"status": "OK", "bucket_objects_listed": len(names), "releases": {}}
    for rel in releases:
        rid, cfg, ev = rel["release_id"], rel["config_sha256"], rel["evidence_sha256"]
        tokens = (rid, cfg, ev, cfg[:16], ev[:16])
        by_name = [n for n in names if any(t in n for t in tokens)]
        candidates = [n for n in names if n.endswith(".json") and "/execution-reports/" not in n and any(
            s in n.lower() for s in ("release", "evidence", "strategy-lifecycle", "promotion", "activation"))]
        hits: list[dict] = []
        scanned = 0
        for uri in by_name + [c for c in candidates if c not in by_name][:250]:
            text = storage_cat(uri)
            scanned += 1
            if text is None or not any(t in text for t in tokens[:3]):
                continue
            entry: dict = {"object_sha256_prefix": sha(uri)[:12], "matched_by_name": uri in by_name,
                           "content_sha256_equals_evidence": sha(text) == ev or sha(text.strip()) == ev,
                           "mentions": [lbl for lbl, t in (("release_id", rid), ("config_sha256", cfg),
                                                           ("evidence_sha256", ev)) if t in text]}
            try:
                doc = json.loads(text)
            except ValueError:
                entry["json"] = False
                hits.append(entry)
                continue
            entry["json"] = True
            entry["top_level_keys"] = sorted(doc.keys())[:40] if isinstance(doc, dict) else type(doc).__name__
            entry["config_related_key_paths"] = [p for p in key_paths(doc) if re.search(
                r"config|canonical|normaliz|hash_input|sha256_input", p, re.I)][:40]
            recompute: dict[str, Any] = {}
            for key in ("config", "strategy_config", "merged_runtime_config", "runtime_config", "config_payload",
                        "canonical_config", "effective_config"):
                for path, value in find_values(doc, key)[:5]:
                    if isinstance(value, (dict, list)):
                        hashes = canonical_hashes(value)
                        recompute[path] = {"matches": [k for k, h in hashes.items() if h == cfg],
                                           "type": type(value).__name__, "n_keys": len(value)}
                    elif isinstance(value, str) and value.strip().startswith("{"):
                        recompute[path] = {"raw_string_sha_matches": sha(value) == cfg,
                                           "parsed_matches": [k for k, h in canonical_hashes(json.loads(value)).items()
                                                              if h == cfg]}
            entry["config_recompute"] = recompute
            entry["declared_canonicalization"] = [
                {"path": p, "value": v if isinstance(v, (str, int, bool)) and len(str(v)) < 80 else type(v).__name__}
                for k in ("canonicalization", "canonical_json", "hash_algorithm", "normalization", "config_hash_method")
                for p, v in find_values(doc, k)[:3]]
            hits.append(entry)
            if len(hits) >= 5:
                break
        out["releases"][rid] = {"objects_matching_by_name": len(by_name), "objects_scanned": scanned,
                                "hits": hits}
    return out


def _bucket_err(rel: float | None) -> str:
    if rel is None or math.isnan(rel):
        return "n/a"
    for edge, label in ((1e-6, "<1e-6"), (1e-5, "<1e-5"), (1e-4, "<1e-4"), (1e-3, "<1e-3"), (1e-2, "<1e-2")):
        if rel < edge:
            return label
    return ">=1e-2"


def part_b(bucket: str) -> dict:
    prefix = os.environ.get("PARITY_REPORT_PREFIX") or "execution-reports"
    platform_hint = os.environ.get("PARITY_REPORT_PLATFORM_HINT") or "schwab"
    profile = os.environ.get("PARITY_REPORT_PROFILE") or "soxl_soxx_trend_income"
    window = int(os.environ.get("PARITY_TREND_WINDOW") or "140")
    want = int(os.environ.get("PARITY_REPORT_SESSIONS") or "10")
    names, err = storage_ls(f"gs://{bucket}/{prefix}/**")
    if err:
        return {"status": "STORAGE_LIST_FAILED", "reason": err}
    months = {(date.today() - timedelta(days=d)).strftime("%Y-%m") for d in (0, 31, 62)}
    sel = [n for n in names if n.endswith(".json") and f"/{profile}/" in n
           and platform_hint in n.split(f"/{prefix}/", 1)[-1].split("/", 1)[0].lower()
           and any(f"/{m}/" in n for m in months)]
    reports: dict[date, dict] = {}
    for uri in sorted(sel, reverse=True):
        text = storage_cat(uri)
        if not text:
            continue
        try:
            rep = json.loads(text)
        except ValueError:
            continue
        if rep.get("dry_run") or rep.get("status") != "ok":
            continue
        sig = ((rep.get("diagnostics") or {}).get("signal") or {})
        if not sig.get("trend_ma") or not sig.get("trend_price"):
            continue
        started = rep.get("started_at")
        try:
            session = datetime.fromisoformat(str(started).replace("Z", "+00:00")).astimezone(NY).date()
        except ValueError:
            continue
        if session not in reports:
            reports[session] = {"symbol": str(sig.get("trend_symbol") or "SOXX").upper(),
                                "price": float(sig["trend_price"]), "ma": float(sig["trend_ma"])}
        if len(reports) >= want:
            break
    out: dict = {"status": "OK", "candidate_objects": len(sel), "sessions_with_trend_fields": len(reports),
                 "window": window, "per_session": [], "soxl_ma_trend_in_report": False,
                 "history_row_count_in_report": False}
    if not reports:
        return out
    import pandas as pd
    import yfinance as yf

    symbols = sorted({r["symbol"] for r in reports.values()})
    start = (min(reports) - timedelta(days=330)).isoformat()
    frames = {}
    for s in symbols:
        f = yf.download(s, start=start, interval="1d", auto_adjust=False, progress=False, threads=False)
        if isinstance(f.columns, pd.MultiIndex):
            f.columns = [c[0] for c in f.columns]
        frames[s] = f
    wins = {"split_only": 0, "dividend_adjusted": 0, "tie_or_na": 0}
    for session in sorted(reports):
        r = reports[session]
        f = frames[r["symbol"]]
        prior = f[f.index.date < session]
        row: dict[str, Any] = {"session": session.isoformat(), "symbol": r["symbol"],
                               "prior_rows_available": int(len(prior))}
        if len(prior) < window - 1:
            row["status"] = "INSUFFICIENT_PUBLIC_HISTORY"
            out["per_session"].append(row)
            continue
        raw_last = float(prior["Close"].iloc[-1])
        errs = {}
        for label, col in (("split_only", "Close"), ("dividend_adjusted", "Adj Close")):
            series = prior[col].astype(float)
            series = series * (raw_last / float(series.iloc[-1]))  # basis as of the run date
            values = [*series.iloc[-(window - 1):].tolist(), r["price"]]
            ma = sum(values) / window
            errs[label] = abs(ma - r["ma"]) / r["ma"]
            row[f"{label}_rel_err_bucket"] = _bucket_err(errs[label])
            row[f"{label}_log10_rel_err"] = round(math.log10(max(errs[label], 1e-12)), 2)
        best = min(errs, key=errs.get)
        if abs(errs["split_only"] - errs["dividend_adjusted"]) < 1e-9:
            best = "tie_or_na"
        row["best_basis"] = best
        wins[best] += 1
        out["per_session"].append(row)
    out["best_basis_counts"] = wins
    return out


def main() -> int:
    bucket = os.environ.get("PARITY_EVIDENCE_BUCKET") or "qsl-runtime-logs-shared"
    report: dict = {"bucket": bucket}
    if os.environ.get("PARITY_RELEASES_JSON"):
        report["A_release_evidence"] = part_a(bucket)
    if os.environ.get("PARITY_SCHWAB_REPORTS") == "1":
        report["B_signal_price_basis"] = part_b(bucket)
    print(json.dumps(report, indent=1, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
