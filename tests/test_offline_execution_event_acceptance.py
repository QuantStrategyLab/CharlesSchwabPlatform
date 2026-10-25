from __future__ import annotations

import json

from application.offline_execution_event_acceptance import run_offline_execution_event_acceptance


def test_acceptance_runner_reports_all_frozen_cases_and_fixed_values(tmp_path):
    result = run_offline_execution_event_acceptance(tmp_path)
    json.dumps(result)
    assert result["schema_version"] == "schwab_offline_execution_event_acceptance.v1"
    assert result["research_only"] is True
    assert result["offline"] is True
    assert result["no_account_connection"] is True
    cases = {case["case_id"]: case for case in result["cases"]}
    assert set(cases) == {"E01", "E02", "E03", "E04", "E05", "E06"}

    fixed = cases["E01"]["assertions"]
    assert fixed["shares"] == "3"
    assert fixed["principal"] == "299.00"
    assert fixed["fees"] == "0.60"
    assert fixed["cash_economic_delta"] == "-299.60"
    assert cases["E01"]["classification"] == "implemented_offline"
    assert cases["E02"]["assertions"]["restart_snapshot_equal"] is True
    assert cases["E03"]["assertions"]["owner_totals_equal_account"] is True
    assert cases["E04"]["assertions"]["unknown_identity_stays_unbound"] is True
    assert cases["E04"]["assertions"]["unknown_identity_books_no_fill"] is True
    assert cases["E04"]["assertions"]["unbound_owner_reservation"] == "100.00"
    assert cases["E04"]["assertions"]["unbound_account_reservation"] == "100.00"
    assert cases["E04"]["assertions"]["binding_reservation_not_double_counted"] is True
    assert cases["E05"]["classification"] == "verified-safe-reject"
    assert cases["E06"]["classification"] == "supported"


def test_acceptance_runner_exposes_native_limits_separately(tmp_path):
    result = run_offline_execution_event_acceptance(tmp_path)
    native = result["native_adapter"]
    assert native["order_lookup"] == "documented_surface"
    assert native["exact_order_id_lookup"] == "documented_surface"
    assert native["order_status_enum"] == "documented_surface"
    assert native["stable_per_fill_id"] == "unsupported_unverified"
    assert native["per_fill_fee"] == "unsupported_unverified"
    assert native["correction_reversal_fields"] == "unsupported_unverified"
