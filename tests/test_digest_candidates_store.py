from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "digest_candidates_store.py"
spec = importlib.util.spec_from_file_location("digest_candidates_store_for_test", SCRIPT)
store = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = store
assert spec.loader is not None
spec.loader.exec_module(store)


def _write(tmp_path, rows):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"schema_version": "qsl.digest_candidates.v1", "runs": rows}), encoding="utf-8")
    return path


def test_summary_is_redacted(tmp_path, capsys):
    path = _write(tmp_path, [{"business_day": "2026-10-12", "equity": 987.65, "opaque_account_uid": "secret-uid", "fill_count": None}])
    store.main(["summarize", "--path", str(path)])
    out = capsys.readouterr().out
    assert "987" not in out and "secret-uid" not in out
    assert json.loads(out) == {"business_days": ["2026-10-12"], "equity_present": True, "fill_count_null": True, "runs": 1, "undated_rows": 0}


def test_object_uri_single_day(tmp_path):
    path = _write(tmp_path, [{"business_day": "2026-10-12"}])
    assert store.object_uri(path, platform="schwab", target="live", uri="gs://b/p/") == "gs://b/p/schwab/2026-10-12/live.json"


def test_object_uri_empty_when_undated_or_mixed(tmp_path):
    assert store.object_uri(_write(tmp_path, [{}]), platform="schwab", target="live", uri="gs://b") == ""
    mixed = _write(tmp_path, [{"business_day": "2026-10-09"}, {"business_day": "2026-10-12"}])
    assert store.object_uri(mixed, platform="schwab", target="live", uri="gs://b") == ""


def test_object_uri_rejects_unsafe_input(tmp_path):
    path = _write(tmp_path, [{"business_day": "2026-10-12"}])
    with pytest.raises(ValueError):
        store.object_uri(path, platform="../x", target="live", uri="gs://b")
    with pytest.raises(ValueError):
        store.object_uri(path, platform="schwab", target="live", uri="https://b")


def test_workflow_is_dry_run_without_store_variable():
    wf = (Path(__file__).resolve().parents[1] / ".github/workflows/digest-candidates-daily.yml").read_text(encoding="utf-8")
    assert "DIGEST_CANDIDATES_GCS_URI: ${{ vars.DIGEST_CANDIDATES_GCS_URI || '' }}" in wf
    assert "dry_run: vars.DIGEST_CANDIDATES_GCS_URI is unset" in wf
    assert "upload-artifact" not in wf
    assert "--publish" not in wf
