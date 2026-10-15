from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "runtime_guard_alert_dedupe.py"
spec = importlib.util.spec_from_file_location("runtime_guard_alert_dedupe_for_test", SCRIPT)
dedupe = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = dedupe
spec.loader.exec_module(dedupe)

T0 = dt.datetime(2026, 10, 11, 8, 0, tzinfo=dt.timezone.utc)
ISSUES = ["Cloud Run 失败日志 2 条：svc"]


def _decide(tmp_path, mode, *, issues=ISSUES, ids=("a", "b"), at=T0):
    return dedupe.decide(
        name="Guard",
        issues=issues,
        event_ids=ids,
        now=at,
        mode=mode,
        state_path=str(tmp_path / "state.json"),
    )


def test_off_mode_always_sends_and_writes_no_state(tmp_path):
    assert _decide(tmp_path, "off").send is True
    assert _decide(tmp_path, "off").send is True
    assert not (tmp_path / "state.json").exists()


def test_unknown_mode_falls_back_to_off(tmp_path, monkeypatch):
    monkeypatch.delenv("RUNTIME_GUARD_ALERT_DEDUPE_MODE", raising=False)
    assert dedupe.resolve_mode("bogus") == "off"
    assert dedupe.resolve_mode(None) == "off"


def test_on_mode_suppresses_repeat_of_same_entries(tmp_path):
    first = _decide(tmp_path, "on")
    assert first.send is True and first.reason == "new_failure_events"
    second = _decide(tmp_path, "on", at=T0 + dt.timedelta(minutes=30))
    assert second.send is False and second.would_suppress is True
    assert second.reason == "repeat_within_reminder_window"


def test_new_failure_entry_always_alerts(tmp_path):
    _decide(tmp_path, "on")
    later = _decide(
        tmp_path,
        "on",
        issues=["Cloud Run 失败日志 3 条：svc"],
        ids=("a", "b", "c"),
        at=T0 + dt.timedelta(minutes=30),
    )
    assert later.send is True and later.new_event_count == 1


def test_new_issue_type_alerts_even_without_entries(tmp_path):
    _decide(tmp_path, "on")
    other = _decide(
        tmp_path,
        "on",
        issues=ISSUES + ["Scheduler 日志查询失败"],
        ids=("a", "b"),
        at=T0 + dt.timedelta(minutes=30),
    )
    assert other.send is True and other.reason == "new_issue_type"


def test_count_change_alone_is_same_issue_type(tmp_path):
    assert dedupe.issue_fingerprint("G", ["失败 2 条"]) == dedupe.issue_fingerprint("G", ["失败 5 条"])


def test_reminder_after_window(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNTIME_GUARD_ALERT_DEDUPE_REMINDER_HOURS", "6")
    _decide(tmp_path, "on", ids=())
    assert _decide(tmp_path, "on", ids=(), at=T0 + dt.timedelta(hours=5)).send is False
    assert _decide(tmp_path, "on", ids=(), at=T0 + dt.timedelta(hours=6)).reason == "reminder_due"


def test_recovery_clears_issue_state_so_recurrence_alerts(tmp_path):
    _decide(tmp_path, "on", ids=())
    dedupe.clear_issues(mode="on", state_path=str(tmp_path / "state.json"), now=T0 + dt.timedelta(minutes=30))
    again = _decide(tmp_path, "on", ids=(), at=T0 + dt.timedelta(minutes=60))
    assert again.send is True and again.reason == "new_issue_type"


def test_dry_run_never_suppresses_but_reports_would_suppress(tmp_path):
    _decide(tmp_path, "dry_run")
    second = _decide(tmp_path, "dry_run", at=T0 + dt.timedelta(minutes=30))
    assert second.send is True
    assert second.would_suppress is True
    assert "would_suppress=true" in second.log_line()


def test_state_holds_hashes_only(tmp_path):
    entry = {"insertId": "abc123", "textPayload": "secret-ish log text U00000001"}
    eid = dedupe.event_id(entry)
    _decide(tmp_path, "on", ids=(eid,))
    raw = (tmp_path / "state.json").read_text(encoding="utf-8")
    assert "abc123" not in raw and "U00000001" not in raw and "svc" not in raw
    assert set(json.loads(raw)) == {"version", "events", "issues"}


def test_old_events_are_pruned(tmp_path):
    _decide(tmp_path, "on", ids=("old",))
    _decide(tmp_path, "on", ids=("new",), at=T0 + dt.timedelta(hours=49))
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert len(state["events"]) == 1


def test_event_id_without_insert_id_is_stable():
    entry = {"timestamp": "2026-10-11T08:00:00Z", "logName": "x", "resource": {"labels": {"service_name": "s"}}}
    assert dedupe.event_id(entry) == dedupe.event_id(dict(entry))
