import datetime as dt

from application.monitor_dispatcher import dispatch_due_monitors


def test_dispatch_due_monitors_selects_probe_window():
    targets = [
        {
            "service_name": "charles-schwab-quant-service",
            "service_url": "https://svc-sg.example.run.app",
            "strategy_profile": "soxl_soxx_trend_income",
            "account_scope": "default",
            "scheduler": {
                "timezone": "America/New_York",
                "probe_time": "35 9,15 * * *",
                "precheck_time": "45 9 * * *",
            },
        }
    ]
    calls = []

    class Response:
        status_code = 200

    result = dispatch_due_monitors(
        targets,
        now=dt.datetime(2026, 6, 18, 13, 35, tzinfo=dt.timezone.utc),
        token_fetcher=lambda audience: f"token:{audience}",
        post_fn=lambda url, **kwargs: calls.append((url, kwargs)) or Response(),
    )

    assert result["ok"] is True
    assert result["dispatches_due"] == 1
    assert calls[0][0] == "https://svc-sg.example.run.app/probe"
    assert calls[0][1]["headers"]["Authorization"] == "Bearer token:https://svc-sg.example.run.app"


def test_dispatch_due_monitors_skips_disabled_target():
    result = dispatch_due_monitors(
        [
            {
                "service_name": "charles-schwab-quant-service",
                "service_url": "https://svc-hk.example.run.app",
                "runtime_target_enabled": "false",
                "scheduler": {
                    "timezone": "America/New_York",
                    "probe_time": "35 9 * * *",
                },
            }
        ],
        now=dt.datetime(2026, 6, 18, 13, 35, tzinfo=dt.timezone.utc),
        token_fetcher=lambda _audience: "token",
        post_fn=lambda *_args, **_kwargs: None,
    )

    assert result["dispatches_due"] == 0
    assert result["results"] == []
