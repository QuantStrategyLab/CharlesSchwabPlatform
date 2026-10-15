"""Synthetic fixtures only — no real account numbers or live broker data."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.project_digest_candidates import (  # noqa: E402
    PLATFORM_ID,
    main,
    project_digest_candidates,
)


def _daily(*, activity: str = "no_signal", fills_source: str = "not_connected", fill_count=None, status: str = "ok"):
    return {
        "platform": "schwab",
        "records": [
            {
                "platform": "schwab",
                "target_key": "charles-schwab-quant-service|soxl_soxx_trend_income|live",
                "target": {
                    "service": "charles-schwab-quant-service",
                    "strategy_profile": "soxl_soxx_trend_income",
                    "account_scope": "live",
                },
                "business_date": "2026-10-08",
                "status": status,
                "runs": [
                    {
                        "run_id": "run." + ("a" * 32),
                        "activity": activity,
                        "execution_lane": "live",
                    }
                ],
                "fills": {"source": fills_source, "records": [], "count": fill_count},
            }
        ],
    }


def _facts(*, net_assets: str = "12345.67"):
    return {
        "schema_version": "schwab_account_snapshot_history.v1",
        "broker_reported_balances": [
            {
                "currency": "USD",
                "net_assets": net_assets,
                "source_tag": "liquidationValue",
                "currency_source": "owner_confirmed",
            }
        ],
    }


def test_unknown_fills_stay_null_not_zero():
    payload = project_digest_candidates(
        daily_projection=_daily(),
        opaque_account_uid="acct_opaque_synthetic",
        target_id="schwab/synthetic-target",
    )
    assert payload["producer_status"] == "projected"
    assert len(payload["runs"]) == 1
    row = payload["runs"][0]
    assert row["platform_id"] == PLATFORM_ID
    assert row["actually_ran"] is True
    assert row["fill_count"] is None
    assert row["order_count"] is None
    assert row["field_status"]["fill_count"] == "counts_unknown"
    assert row["field_status"]["order_count"] == "counts_unknown"
    assert "schwab_fills_not_connected" in row["reason_code"]
    assert row["cycle_count"] == 1
    assert row["opaque_account_uid"] == "acct_opaque_synthetic"
    assert row["target_id"] == "schwab/synthetic-target"
    assert row["signal_summary"] == "no_signal"
    assert row["rebalance_kind"] == "no_rebalance"
    assert "equity" not in row


def test_missing_identity_marked_not_invented():
    payload = project_digest_candidates(daily_projection=_daily(activity="filled"))
    row = payload["runs"][0]
    assert row["opaque_account_uid"] == ""
    assert row["target_id"] == ""
    assert "opaque_account_uid_absent" in row["reason_code"]
    assert "target_id_absent" in row["reason_code"]
    assert row["rebalance_kind"] == "rebalance"
    assert row["status"] == "ok"


def test_equity_only_from_account_facts_never_guessed():
    payload = project_digest_candidates(
        daily_projection=_daily(activity="no_rebalance"),
        opaque_account_uid="acct_opaque_synthetic",
        target_id="schwab/synthetic-target",
        account_facts=_facts(),
    )
    row = payload["runs"][0]
    assert row["equity"] == pytest.approx(12345.67)
    assert row["equity_currency"] == "USD"


def test_invalid_equity_text_omitted():
    payload = project_digest_candidates(
        daily_projection=_daily(),
        opaque_account_uid="acct_opaque_synthetic",
        target_id="schwab/synthetic-target",
        account_facts=_facts(net_assets="not-a-number"),
    )
    assert "equity" not in payload["runs"][0]
    assert "account_facts_equity_absent" in payload["runs"][0]["reason_code"]


def test_no_covering_runs_yields_empty():
    daily = _daily()
    daily["records"][0]["runs"] = [{"activity": "insufficient"}]
    payload = project_digest_candidates(
        daily_projection=daily,
        opaque_account_uid="acct_opaque_synthetic",
        target_id="schwab/synthetic-target",
    )
    assert payload["runs"] == []
    assert payload["producer_status"] == "empty"


def test_alert_status_for_failed_activity():
    payload = project_digest_candidates(
        daily_projection=_daily(activity="failed", status="failed"),
        opaque_account_uid="acct_opaque_synthetic",
        target_id="schwab/synthetic-target",
    )
    assert payload["runs"][0]["status"] == "alert"


def test_cli_writes_safe_summary(tmp_path, capsys):
    daily_path = tmp_path / "daily.json"
    facts_path = tmp_path / "facts.json"
    out_path = tmp_path / "candidates.json"
    daily_path.write_text(json.dumps(_daily()), encoding="utf-8")
    facts_path.write_text(json.dumps(_facts()), encoding="utf-8")
    code = main(
        [
            "--daily-projection",
            str(daily_path),
            "--account-facts",
            str(facts_path),
            "--opaque-account-uid",
            "acct_opaque_synthetic",
            "--target-id",
            "schwab/synthetic-target",
            "--output",
            str(out_path),
        ]
    )
    assert code == 0
    summary = capsys.readouterr().out.strip()
    printed = json.loads(summary)
    assert printed["status"] == "projected"
    assert printed["runs"] == 1
    # CLI summary must not echo identity or equity.
    assert "acct_opaque" not in summary
    assert "12345" not in summary
    text = out_path.read_text(encoding="utf-8")
    assert "12345.67" in text
    assert "schwab/synthetic-target" in text
    assert '"fill_count":null' in text.replace(" ", "")


def test_business_day_filter():
    payload = project_digest_candidates(
        daily_projection=_daily(),
        opaque_account_uid="acct_opaque_synthetic",
        target_id="schwab/synthetic-target",
        business_day="2026-10-07",
    )
    assert payload["runs"] == []
    assert payload["producer_status"] == "empty"


def test_equity_without_covering_runs_emits_row():
    """Account-facts equity must not require daily covering runs."""
    daily = _daily()
    daily["records"][0]["runs"] = []  # no covering activity
    payload = project_digest_candidates(
        daily_projection=daily,
        opaque_account_uid="acct_opaque_synthetic",
        target_id="schwab-primary",
        account_facts=_facts(net_assets="2500.00"),
    )
    assert payload["producer_status"] == "projected"
    assert payload["producer_reason"] == "equity_without_covering_runs"
    assert len(payload["runs"]) == 1
    row = payload["runs"][0]
    assert row["actually_ran"] is True
    assert row["cycle_count"] == 0
    assert row["fill_count"] is None
    assert row["equity"] == pytest.approx(2500.0)
    assert "equity_from_archive_facts" in row["reason_code"]
    assert "holdings" not in row
