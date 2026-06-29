import sys
from datetime import datetime, timezone
from pathlib import Path


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
