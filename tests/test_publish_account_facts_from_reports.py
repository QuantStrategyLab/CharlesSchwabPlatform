import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from scripts import publish_account_facts_from_reports as publisher


def _gcloud_result(items, *, returncode=0, stderr=""):
    return SimpleNamespace(
        returncode=returncode,
        stdout=json.dumps(items),
        stderr=stderr,
    )


def _valid_report(
    *,
    observed_at="2026-09-30T22:21:00Z",
    run_id="20260930T222000Z",
    started_at="2026-09-30T22:20:00Z",
    finished_at="2026-09-30T22:25:00Z",
):
    return {
        "schema_version": "runtime_report.v1",
        "platform": "charles_schwab",
        "project_id": publisher.PROJECT_ID,
        "service_name": "synthetic-service",
        "strategy_profile": publisher.STRATEGY_PROFILE,
        "account_scope": None,
        "status": "ok",
        "errors": [],
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "runtime_target": {
            "strategy_profile": publisher.STRATEGY_PROFILE,
            "account_scope": "live",
            "account_selector": ["live"],
        },
        "diagnostics": {"runtime_revision": "service-00007-abc"},
        "summary": {
            "account_observation": {
                "account_hash": "synthetic-account-hash",
                "currency": None,
                "observed_at": observed_at,
                "net_assets": "123.45",
                "net_assets_source": "liquidationValue",
                "net_assets_currency": "USD",
                "net_assets_currency_source": "owner_confirmed",
            }
        },
    }


def test_valid_same_cycle_observation_projects_with_native_time_and_empty_cash():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    uri = prefix + "2026-09/20260930T222000Z.json"
    body = publisher.project_schwab_account_facts_history(
        _valid_report(),
        source_report_uri=uri,
        report_prefix=prefix,
        expected_service_name="synthetic-service",
        expected_runtime_revision="service-00007-abc",
        expected_target_id="synthetic-target",
        now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
    )

    assert body["schema_version"] == publisher.HISTORY_SCHEMA
    assert body["account_hash"] == "synthetic-account-hash"
    assert body["observed_started_at"] == "2026-09-30T22:21:00Z"
    assert body["observed_finished_at"] == "2026-09-30T22:21:00Z"
    assert body["broker_reported_balances"][0]["currency_source"] == "owner_confirmed"
    assert body["cash"] == []


def test_cash_and_raw_account_type_project_as_independent_same_report_facts():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    report = _valid_report()
    report["summary"]["account_observation"].update(
        {
            "cash_balance": "123.4500",
            "cash_balance_source": "cashBalance",
            "cash_currency": "USD",
            "cash_currency_source": "owner_confirmed",
            "broker_account_type": "PROVIDER_UNKNOWN",
            "broker_account_type_source": "securitiesAccount.type",
        }
    )

    body = publisher.project_schwab_account_facts_history(
        report,
        source_report_uri=prefix + "2026-09/20260930T222000Z.json",
        report_prefix=prefix,
        expected_service_name="synthetic-service",
        expected_runtime_revision="service-00007-abc",
        expected_target_id="synthetic-target",
        expected_cash_currency="USD",
        now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
    )

    assert body["broker_reported_balances"] == [
        {
            "currency": "USD",
            "net_assets": "123.45",
            "source_tag": "liquidationValue",
            "currency_source": "owner_confirmed",
        }
    ]
    assert body["cash"] == [
        {
            "currency": "USD",
            "cash_balance": "123.4500",
            "source_tag": "cashBalance",
            "currency_source": "owner_confirmed",
        }
    ]
    assert body["broker_account_type"] == {
        "value": "PROVIDER_UNKNOWN",
        "source_tag": "securitiesAccount.type",
    }
    assert body["account_hash"] == "synthetic-account-hash"
    assert body["source_binding"]["id"] == publisher._binding_id(
        "synthetic-account-hash", "synthetic-service"
    )


def test_cash_requires_native_source_and_separate_exact_usd_confirmation():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    uri = prefix + "2026-09/20260930T222000Z.json"
    cases = (
        {"cash_balance": "1.25", "cash_balance_source": "cashBalance"},
        {
            "cash_balance": "1.25",
            "cash_balance_source": "cashBalance",
            "cash_currency": "USD",
        },
        {
            "cash_balance": "1.25",
            "cash_balance_source": "cashBalance",
            "cash_currency": "USD",
            "cash_currency_source": "owner_confirmed_wrongly",
        },
        {
            "cash_balance": "1.25",
            "cash_balance_source": "cashAvailableForTrading",
            "cash_currency": "USD",
            "cash_currency_source": "owner_confirmed",
        },
        {
            "cash_balance": "1234567890123456.123456789",
            "cash_balance_source": "cashBalance",
            "cash_currency": "USD",
            "cash_currency_source": "owner_confirmed",
        },
        {
            "cash_balance": "1e999999999",
            "cash_balance_source": "cashBalance",
            "cash_currency": "USD",
            "cash_currency_source": "owner_confirmed",
        },
    )
    for additions in cases:
        report = _valid_report()
        report["summary"]["account_observation"].update(additions)
        body = publisher.project_schwab_account_facts_history(
            report,
            source_report_uri=uri,
            report_prefix=prefix,
            expected_service_name="synthetic-service",
            expected_runtime_revision="service-00007-abc",
            expected_target_id="synthetic-target",
            now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
        )
        assert body["cash"] == []
        assert body["broker_reported_balances"][0]["net_assets"] == "123.45"

    for configured_currency in (None, "EUR"):
        report = _valid_report()
        report["summary"]["account_observation"].update(
            {
                "cash_balance": "1.25",
                "cash_balance_source": "cashBalance",
                "cash_currency": "USD",
                "cash_currency_source": "owner_confirmed",
            }
        )
        body = publisher.project_schwab_account_facts_history(
            report,
            source_report_uri=uri,
            report_prefix=prefix,
            expected_service_name="synthetic-service",
            expected_runtime_revision="service-00007-abc",
            expected_target_id="synthetic-target",
            expected_cash_currency=configured_currency,
            now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
        )
        assert body["cash"] == []


def test_optional_raw_type_is_validated_but_legacy_report_remains_accepted():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    uri = prefix + "2026-09/20260930T222000Z.json"
    for token in ("CASH", "MARGIN", "PROVIDER_UNKNOWN"):
        report = _valid_report()
        report["summary"]["account_observation"].update(
            {
                "broker_account_type": token,
                "broker_account_type_source": "securitiesAccount.type",
            }
        )
        body = publisher.project_schwab_account_facts_history(
            report,
            source_report_uri=uri,
            report_prefix=prefix,
            expected_service_name="synthetic-service",
            expected_runtime_revision="service-00007-abc",
            expected_target_id="synthetic-target",
            now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
        )
        assert body["broker_account_type"] == {
            "value": token,
            "source_tag": "securitiesAccount.type",
        }

    for token, source in (("bad token", "securitiesAccount.type"), ("MARGIN", "wrong_source")):
        report = _valid_report()
        report["summary"]["account_observation"].update(
            {"broker_account_type": token, "broker_account_type_source": source}
        )
        body = publisher.project_schwab_account_facts_history(
            report,
            source_report_uri=uri,
            report_prefix=prefix,
            expected_service_name="synthetic-service",
            expected_runtime_revision="service-00007-abc",
            expected_target_id="synthetic-target",
            now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
        )
        assert "status" not in body
        assert "broker_account_type" not in body

    legacy = _valid_report()
    body = publisher.project_schwab_account_facts_history(
        legacy,
        source_report_uri=uri,
        report_prefix=prefix,
        expected_service_name="synthetic-service",
        expected_runtime_revision="service-00007-abc",
        expected_target_id="synthetic-target",
        now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
    )
    assert "broker_account_type" not in body
    assert body["cash"] == []


def test_stale_snapshot_is_rejected_without_relabeling_time():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    uri = prefix + "2026-09/20260929T222000Z.json"
    body = publisher.project_schwab_account_facts_history(
        _valid_report(
            observed_at="2026-09-29T22:21:00Z",
            run_id="20260929T222000Z",
            started_at="2026-09-29T22:20:00Z",
            finished_at="2026-09-29T22:25:00Z",
        ),
        source_report_uri=uri,
        report_prefix=prefix,
        expected_service_name="synthetic-service",
        expected_runtime_revision="service-00007-abc",
        expected_target_id="synthetic-target",
        now=datetime(2026, 10, 1, 11, 20, tzinfo=timezone.utc),
    )

    assert body == {"status": "skipped", "reason": "observation_out_of_window"}


def test_latest_report_selection_covers_delayed_previous_utc_day_and_months():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    older = prefix + "2026-09/20260929T231000Z.json"
    newest = prefix + "2026-09/20260930T222000Z.json"
    calls = []

    def list_objects(command, **_kwargs):
        calls.append(command[4])
        if "20260929T" in command[4]:
            return _gcloud_result([{"url": older}])
        if "20260930T" in command[4]:
            return _gcloud_result([{"url": newest}])
        return _gcloud_result([], returncode=1, stderr="matched no objects")

    with patch.object(publisher.subprocess, "run", side_effect=list_objects):
        selected = publisher._latest_recent_report_uri(
            report_prefix=prefix,
            now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
        )

    assert selected == newest
    assert any("20260929T" in item for item in calls)
    assert any("20260930T" in item for item in calls)
    assert any("20261001T" in item for item in calls)


def test_latest_report_listing_failure_does_not_fall_back_to_an_older_month():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"

    def list_objects(command, **_kwargs):
        if "20260929T" in command[4]:
            return _gcloud_result(
                [], returncode=1, stderr="permission denied"
            )
        return _gcloud_result([{"url": prefix + "2026-10/20261001T001000Z.json"}])

    with patch.object(publisher.subprocess, "run", side_effect=list_objects):
        selected = publisher._latest_recent_report_uri(
            report_prefix=prefix,
            now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
        )

    assert selected is None


def test_empty_daily_prefix_handles_real_subprocess_bytes_output():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    completed = SimpleNamespace(
        returncode=1,
        stdout="",
        stderr="matched no objects",
    )
    with patch.object(publisher.subprocess, "run", return_value=completed) as run:
        selected = publisher._latest_recent_report_uri(
            report_prefix=prefix,
            now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
        )

    assert selected is None
    assert run.call_count == 3
    assert all(call.kwargs.get("text") is True for call in run.call_args_list)


def test_untrusted_or_wrong_prefix_is_not_listed():
    with patch.object(publisher.subprocess, "run") as run:
        selected = publisher._latest_recent_report_uri(
            report_prefix="gs://example-bucket/other-platform/",
            now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
        )

    assert selected is None
    run.assert_not_called()


def test_current_serving_revision_allows_zero_percent_tag_without_percent():
    service = {
        "status": {
            "traffic": [
                {"revisionName": "service-00007-abc", "percent": 100},
                {"revisionName": "service-00006-def", "tag": "previous", "url": "https://example.invalid"},
            ]
        }
    }
    with patch.object(publisher, "_gcloud_json", return_value=service):
        assert publisher._current_serving_revision("service") == "service-00007-abc"
        assert publisher._service_revision_matches("service-00007-abc", "service")


def test_malformed_cloud_run_traffic_percent_is_rejected():
    for invalid_percent in (None, True, -1, 101, "100"):
        service = {
            "status": {
                "traffic": [
                    {"revisionName": "service-00007-abc", "percent": 100},
                    {"revisionName": "service-00006-def", "tag": "previous", "percent": invalid_percent},
                ]
            }
        }
        with patch.object(publisher, "_gcloud_json", return_value=service):
            assert publisher._current_serving_revision("service") is None


def test_scheduled_mode_is_rejected_outside_actions_without_remote_calls():
    with patch.dict("os.environ", {}, clear=True), patch.object(publisher.subprocess, "run") as run:
        assert publisher.main(["--latest-scheduled-report"]) == 2
    run.assert_not_called()


def test_latest_invalid_report_is_not_replaced_by_an_older_report(capsys):
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    latest_uri = prefix + "2026-09/20260930T222000Z.json"
    env = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_EVENT_NAME": "schedule",
        "SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN": "synthetic-token",
        "ACCOUNT_FACTS_SYNC_URL": publisher.SYNC_URL,
        "SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX": prefix,
        "SCHWAB_ACCOUNT_FACTS_SERVICE_NAME": "synthetic-service",
        "SCHWAB_NET_ASSETS_CURRENCY": "USD",
        "SCHWAB_ACCOUNT_FACTS_TARGET_ID": "synthetic-target",
    }
    with (
        patch.dict("os.environ", env, clear=True),
        patch.object(publisher, "_current_serving_revision", return_value="service-00007-abc"),
        patch.object(publisher, "_latest_recent_report_uri", return_value=latest_uri),
        patch.object(publisher, "_service_revision_matches", return_value=True),
        patch.object(publisher, "_read_report", return_value={"status": "error"}) as read,
        patch.object(publisher, "_publish_once") as publish,
    ):
        assert publisher.main(["--latest-scheduled-report"]) == 2

    read.assert_called_once_with(latest_uri)
    publish.assert_not_called()
    assert capsys.readouterr().out == "skipped:report_invalid\n"


def _response(payload, *, status=200):
    class Response:
        def __init__(self):
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            if isinstance(payload, bytes):
                return payload
            return json.dumps(payload).encode("utf-8")

    return Response()


def test_publish_once_distinguishes_new_and_unchanged_observations():
    for payload, expected in (
        ({"ok": True, "stored": True, "unchanged": False}, {"status": "published"}),
        (
            {"ok": True, "stored": True, "unchanged": True},
            {"status": "unchanged", "reason": "observation_unchanged"},
        ),
    ):
        opener = SimpleNamespace(open=lambda *_args, **_kwargs: _response(payload))
        with patch.object(publisher, "build_opener", return_value=opener) as build:
            result = publisher._publish_once(
                {"synthetic": True}, sync_url=publisher.SYNC_URL, token="synthetic-token"
            )
        assert result == expected
        build.assert_called_once()


def test_publish_once_rejects_invalid_or_false_response_without_leaking_body():
    sentinel = "PRIVATE_RESPONSE_SENTINEL"
    for payload in (
        b"not-json",
        {"ok": False, "stored": True, "unchanged": False, "detail": sentinel},
        {"ok": True, "stored": True, "detail": sentinel},
    ):
        opener = SimpleNamespace(open=lambda *_args, **_kwargs: _response(payload))
        with patch.object(publisher, "build_opener", return_value=opener) as build:
            result = publisher._publish_once(
                {"synthetic": True}, sync_url=publisher.SYNC_URL, token="synthetic-token"
            )
        assert result == {
            "status": "skipped",
            "reason": "publish_failed",
            "category": "response_invalid",
            "http_status": 200,
        }
        assert sentinel not in str(result)
        build.assert_called_once()


def test_main_treats_unchanged_as_success_without_relabeling_observation(capsys):
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    now = datetime.now(timezone.utc)
    archived_at = now.replace(second=0, microsecond=0)
    observed_at = archived_at + (now - archived_at) / 2
    run_id = archived_at.strftime("%Y%m%dT%H%M%SZ")
    uri = prefix + f"{archived_at:%Y-%m}/{run_id}.json"
    env = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN": "synthetic-token",
        "ACCOUNT_FACTS_SYNC_URL": publisher.SYNC_URL,
        "SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX": prefix,
        "SCHWAB_ACCOUNT_FACTS_SERVICE_NAME": "synthetic-service",
        "SCHWAB_NET_ASSETS_CURRENCY": "USD",
        "SCHWAB_ACCOUNT_FACTS_TARGET_ID": "synthetic-target",
    }
    with (
        patch.dict("os.environ", env, clear=True),
        patch.object(publisher, "_service_revision_matches", return_value=True),
        patch.object(
            publisher,
            "_read_report",
            return_value=_valid_report(
                observed_at=observed_at.isoformat(),
                run_id=run_id,
                started_at=archived_at.isoformat(),
                finished_at=(archived_at + timedelta(minutes=1)).isoformat(),
            ),
        ),
        patch.object(
            publisher,
            "_publish_once",
            return_value={"status": "unchanged", "reason": "observation_unchanged"},
        ),
    ):
        assert publisher.main(["--report-uri", uri, "--expected-runtime-revision", "service-00007-abc"]) == 0

    assert capsys.readouterr().out == "unchanged:observation_unchanged\n"


def test_native_account_selector_matching_observation_projects():
    """Pinned native identity uses [account_hash], not legacy ["live"]."""
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    uri = prefix + "2026-09/20260930T222000Z.json"
    report = _valid_report()
    native_hash = report["summary"]["account_observation"]["account_hash"]
    report["runtime_target"]["account_selector"] = [native_hash]
    body = publisher.project_schwab_account_facts_history(
        report,
        source_report_uri=uri,
        report_prefix=prefix,
        expected_service_name="synthetic-service",
        expected_runtime_revision="service-00007-abc",
        expected_target_id="schwab-primary",
        now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
    )
    assert body.get("status") != "skipped"
    assert body["broker_reported_balances"][0]["net_assets"] == "123.45"
    assert body["target_id"] == "schwab-primary"


def test_native_account_selector_mismatch_skips():
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    uri = prefix + "2026-09/20260930T222000Z.json"
    report = _valid_report()
    report["runtime_target"]["account_selector"] = ["OTHER-NATIVE-HASH"]
    body = publisher.project_schwab_account_facts_history(
        report,
        source_report_uri=uri,
        report_prefix=prefix,
        expected_service_name="synthetic-service",
        expected_runtime_revision="service-00007-abc",
        expected_target_id="schwab-primary",
        now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc),
    )
    assert body == {"status": "skipped", "reason": "runtime_target_selector_mismatch"}
