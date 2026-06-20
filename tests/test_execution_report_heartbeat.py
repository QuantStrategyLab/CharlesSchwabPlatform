from __future__ import annotations

import datetime as dt

from scripts import execution_report_heartbeat as heartbeat


def test_heartbeat_skips_when_runtime_target_is_disabled(monkeypatch, capsys):
    monkeypatch.setenv("RUNTIME_HEARTBEAT_NAME", "CharlesSchwab disabled runtime")
    monkeypatch.setenv("RUNTIME_TARGET_ENABLED", "false")
    monkeypatch.setattr(
        heartbeat,
        "_list_gcs_objects",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("GCS should not be queried")),
    )

    result = heartbeat.main(now=dt.datetime(2026, 6, 20, 23, 10, tzinfo=dt.timezone.utc))

    assert result == 0
    output = capsys.readouterr().out
    assert "Execution report heartbeat skipped for CharlesSchwab disabled runtime" in output
    assert "runtime target is disabled" in output


def test_heartbeat_skips_when_runtime_target_json_is_disabled(monkeypatch, capsys):
    monkeypatch.delenv("RUNTIME_TARGET_ENABLED", raising=False)
    monkeypatch.setenv("RUNTIME_HEARTBEAT_NAME", "CharlesSchwab disabled runtime")
    monkeypatch.setenv("RUNTIME_TARGET_JSON", '{"runtime_target_enabled":false}')
    monkeypatch.setattr(
        heartbeat,
        "_list_gcs_objects",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("GCS should not be queried")),
    )

    result = heartbeat.main(now=dt.datetime(2026, 6, 20, 23, 10, tzinfo=dt.timezone.utc))

    assert result == 0
    output = capsys.readouterr().out
    assert "Execution report heartbeat skipped for CharlesSchwab disabled runtime" in output
    assert "runtime target is disabled" in output


def test_heartbeat_skips_outside_runtime_target_scheduler_day(monkeypatch, capsys):
    monkeypatch.setenv("RUNTIME_HEARTBEAT_NAME", "CharlesSchwab monthly runtime")
    monkeypatch.setenv(
        "RUNTIME_TARGET_JSON",
        '{"scheduler":{"timezone":"America/New_York","main_time":"45 15 25-28 * *"}}',
    )
    monkeypatch.setattr(
        heartbeat,
        "_list_gcs_objects",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("GCS should not be queried")),
    )

    result = heartbeat.main(now=dt.datetime(2026, 6, 20, 23, 10, tzinfo=dt.timezone.utc))

    assert result == 0
    output = capsys.readouterr().out
    assert "Execution report heartbeat skipped for CharlesSchwab monthly runtime" in output
    assert "expected day(s)=25,26,27,28" in output


def test_heartbeat_does_not_skip_inside_runtime_target_scheduler_day(monkeypatch):
    monkeypatch.setenv(
        "RUNTIME_TARGET_JSON",
        '{"scheduler":{"timezone":"America/New_York","main_time":"45 15 25-28 * *"}}',
    )
    now = dt.datetime(2026, 6, 25, 23, 10, tzinfo=dt.timezone.utc)

    reason = heartbeat._heartbeat_skip_reason_for_schedule(
        now - dt.timedelta(hours=36),
        now,
    )

    assert reason is None


def test_heartbeat_does_not_skip_when_lookback_includes_scheduler_day(monkeypatch):
    monkeypatch.setenv(
        "RUNTIME_TARGET_JSON",
        '{"scheduler":{"timezone":"America/New_York","main_time":"45 15 25-28 * *"}}',
    )

    reason = heartbeat._heartbeat_skip_reason_for_schedule(
        dt.datetime(2026, 6, 28, 20, 0, tzinfo=dt.timezone.utc),
        dt.datetime(2026, 6, 29, 20, 0, tzinfo=dt.timezone.utc),
    )

    assert reason is None
