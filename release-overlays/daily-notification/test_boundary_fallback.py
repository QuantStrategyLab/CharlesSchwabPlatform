from notifications import boundary, unified


def test_coverage_alert_failure_does_not_send_routine(monkeypatch, capsys):
    calls = []
    def publish(*args, coverage_failure, **kwargs):
        return coverage_failure()
    def fail_alert():
        raise RuntimeError("synthetic alert failure")
    monkeypatch.setattr(unified, "publish_cycle", publish)
    assert boundary.publish_cycle_safely(None, config=None, legacy=lambda: calls.append("routine"),
                                       coverage_failure=fail_alert) is False
    assert calls == []
    assert "cycle_observation_unknown" in capsys.readouterr().out
