import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant_platform_kit.common import build_runtime_assembly, build_runtime_target  # noqa: E402
from application.runtime_reporting_adapters import build_runtime_reporting_adapters  # noqa: E402


def test_runtime_reporting_adapters_start_run_builds_report_with_runtime_target():
    observed = {}

    def fake_report_builder(**kwargs):
        observed["report_builder"] = kwargs
        return {"run_id": kwargs["run_id"]}

    adapters = build_runtime_reporting_adapters(
        runtime_assembly=build_runtime_assembly(
            platform="charles_schwab",
            deploy_target="cloud_run",
            service_name="charles-schwab-platform",
            strategy_profile="tqqq_growth_income",
            runtime_target=build_runtime_target(
                platform_id="charles_schwab",
                strategy_profile="tqqq_growth_income",
                dry_run_only=True,
                service_name="charles-schwab-platform",
            ),
            project_id="project-1",
        ),
        strategy_domain="us_equity",
        managed_symbols=("TQQQ", "BOXX", "SPYI", "QQQI"),
        benchmark_symbol="QQQ",
        strategy_display_name="TQQQ Growth Income",
        strategy_display_name_localized="TQQQ 增长收益",
        dry_run=True,
        signal_effective_after_trading_days=1,
        report_base_dir="/tmp/reports",
        report_gcs_prefix_uri="gs://bucket/reports",
        run_id_builder=lambda: "run-001",
        event_logger=lambda *_args, **_kwargs: {},
        report_builder=fake_report_builder,
        report_persister=lambda *_args, **_kwargs: None,
        printer=lambda *_args, **_kwargs: None,
        clock=lambda: datetime(2026, 4, 21, tzinfo=timezone.utc),
    )

    log_context, report = adapters.start_run()

    assert log_context.run_id == "run-001"
    assert log_context.runtime_target.platform_id == "charles_schwab"
    assert observed["report_builder"]["runtime_target"].platform_id == "charles_schwab"
    assert observed["report_builder"]["runtime_target"].execution_mode == "paper"
    assert report == {"run_id": "run-001"}


@pytest.mark.parametrize("rows,expected_count", [(None, 1), ([], 0)], ids=["nonempty", "empty"])
def test_actual_probe_qpk_writer_readback_publisher_prepare_roundtrip(tmp_path, monkeypatch, rows, expected_count):
    import json
    from quant_platform_kit.common import runtime_reports
    from test_request_handling import _run_external_flow_probe
    from scripts import publish_account_facts_from_reports as publisher

    def no_cloud(*_args, **_kwargs):
        raise AssertionError("offline local writer must not upload")
    monkeypatch.setattr(runtime_reports, "upload_runtime_report_to_cloud", no_cloud)
    adapters = build_runtime_reporting_adapters(
        runtime_assembly=build_runtime_assembly(
            platform="charles_schwab", deploy_target="cloud_run", service_name="synthetic-service",
            strategy_profile=publisher.STRATEGY_PROFILE, project_id=publisher.PROJECT_ID,
        ),
        strategy_domain="us_equity", managed_symbols=(), benchmark_symbol="",
        strategy_display_name="Synthetic", strategy_display_name_localized="Synthetic", dry_run=False,
        signal_effective_after_trading_days=None, report_base_dir=str(tmp_path), report_gcs_prefix_uri=None,
        run_id_builder=lambda: "synthetic", event_logger=lambda *_args, **_kwargs: {},
        report_builder=runtime_reports.build_runtime_report_base, report_persister=runtime_reports.persist_runtime_report,
    )
    observed = {}
    def persist(report):
        observed["path"] = adapters.persist_execution_report(report)
        return observed["path"]
    def finalize(report, **kwargs):
        return runtime_reports.finalize_runtime_report(report, finished_at="2026-09-30T22:25:00Z", **kwargs)
    result, client, original = _run_external_flow_probe(rows=rows, persist=persist, finalizer=finalize)
    assert result == ("Probe OK", 200)
    client.get_transactions.assert_called_once()
    saved = json.loads(Path(observed["path"]).read_text())
    assert saved["account_external_flows"] == original["account_external_flows"]
    assert saved["account_external_flows"]["source_complete"] is False
    assert isinstance(saved["account_external_flows"]["transactions"], list)
    assert len(saved["account_external_flows"]["transactions"]) == expected_count
    prefix = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
    kwargs = dict(source_report_uri=prefix + "2026-09/20260930T222000Z.json", report_prefix=prefix,
                  expected_service_name="synthetic-service", expected_runtime_revision="service-00007-abc",
                  expected_target_id="synthetic-target", now=datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc))
    actual = publisher.project_schwab_account_facts_history(saved, external_flow_enabled=True, **kwargs)
    assert "externalFlow" in actual
    assert actual == publisher.project_schwab_account_facts_history(original, external_flow_enabled=True, **kwargs)
    payload = publisher.prepare_sync_payload(actual, include_external_flow=True)
    assert payload["externalFlow"]["windows"][0]["coverage"] == "incomplete"
    assert len(payload["externalFlow"]["windows"][0]["records"]) == expected_count
    (tmp_path / "actual-chain-payload.json").write_text(json.dumps(payload, indent=2))
    # OFF and ON without a source keep the same actual archive projection.
    saved.pop("account_external_flows")
    assert publisher.project_schwab_account_facts_history(saved, external_flow_enabled=True, **kwargs) == publisher.project_schwab_account_facts_history(saved, **kwargs)


def test_probe_writer_failure_does_not_claim_an_archive(capsys):
    from test_request_handling import _run_external_flow_probe
    def fail(_report):
        raise OSError("synthetic writer unavailable")
    result, _client, _report = _run_external_flow_probe(persist=fail)
    assert result == ("Probe OK", 200)
    output = capsys.readouterr().out
    assert "failed to persist execution report" in output
    assert "execution_report synthetic-local-report" not in output
