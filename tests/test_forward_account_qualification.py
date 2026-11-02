"""Synthetic adverse cases for report-only forward accounting qualification."""
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from application.account_observation import build_account_observation
from scripts import publish_account_facts_from_reports as publisher
from test_publish_account_facts_from_reports import _valid_report


PREFIX = 'gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/'


def project(report):
    return publisher.project_schwab_account_facts_history(
        report, source_report_uri=PREFIX + '2026-11/20261103T050000Z.json',
        report_prefix=PREFIX, expected_service_name='synthetic-service',
        expected_runtime_revision='service-00007-abc', expected_target_id='synthetic-target',
        expected_cash_currency='USD', now=datetime(2026, 11, 4, tzinfo=timezone.utc),
    )


def report_at(when='2026-11-04T00:01:00Z'):
    return _valid_report(observed_at=when, run_id='20261103T050000Z',
                         started_at='2026-11-03T05:00:00Z', finished_at='2026-11-04T00:02:00Z')


def test_stock_scope_never_claims_full_account_completeness():
    snapshot = SimpleNamespace(as_of=datetime(2026, 11, 3, 6, tzinfo=timezone.utc),
        total_equity=100, positions=(), metadata={'account_hash': 'synthetic',
        'total_equity_source': 'broker_liquidation_value', 'broker_stock_inventory': {
            'positions_field_present': True, 'native_positions_count': 1,
            'positions': [{'instrument': {'assetType': 'OPTION', 'symbol': 'FAKE OPTION'},
                           'longQuantity': '2', 'shortQuantity': '0'}]}})
    result = build_account_observation(snapshot, net_assets_currency='USD')
    assert result['broker_reported_positions_complete'] is False
    assert result['broker_native_position_facts'][0] == {
        'asset_type': 'OPTION', 'symbol': 'FAKE OPTION', 'long_quantity': '2',
        'short_quantity': '0', 'quantity': '2',
    }
    assert result['broker_reported_positions'] == []


@pytest.mark.parametrize('legacy_complete', [True, False])
def test_archive_stock_scope_is_retained_without_promoting_legacy_complete(legacy_complete):
    report = report_at()
    report['summary']['account_observation'].update(
        broker_reported_positions=[], broker_reported_positions_scope='stocks_only',
        broker_reported_positions_complete=legacy_complete, broker_stock_inventory_status='available',
        broker_reported_positions_observed_at='2026-11-04T00:01:00Z')
    result = project(report)
    assert result['broker_reported_positions'] == []
    assert result['broker_reported_positions_complete'] is False
    assert result['forward_reconciliation']['inventory']['status'] == 'UNRECONCILED'


def test_missing_cash_source_time_and_opening_are_unknown_not_zero_or_midnight():
    report = report_at()
    original = deepcopy(report)
    result = project(report)
    facts = result['forward_reconciliation']
    assert facts['complete'] is False and facts['status'] == 'UNRECONCILED'
    assert facts['cash']['native_cash_balance'] is None
    assert facts['cash']['settled_cash'] is None
    assert facts['cash']['currency'] is None
    assert facts['timing']['source_as_of'] is None
    assert facts['timing']['collection_started_at'] is None
    assert facts['timing']['observed_at'] == '2026-11-04T00:01:00Z'
    assert facts['timing']['reporting_date_ny'] == '2026-11-03'
    assert facts['opening']['reason'] == 'opening_snapshot_missing'
    assert report == original
    assert 'forward_reconciliation' not in publisher.prepare_sync_payload(result)


def test_native_cash_requires_independent_currency_and_source():
    report = report_at()
    obs = report['summary']['account_observation']
    obs.update(cash_balance='7.50', cash_balance_source='cashBalance')
    cash = project(report)['forward_reconciliation']['cash']
    assert cash['native_cash_balance'] is None
    obs.update(cash_currency='USD', cash_currency_source='owner_confirmed')
    cash = project(report)['forward_reconciliation']['cash']
    assert cash['native_cash_balance'] == '7.50' and cash['currency'] == 'USD'
    assert cash['status'] == 'UNRECONCILED' and cash['settled_cash'] is None


@pytest.mark.parametrize('start,complete,rows,reason', [
    ('2026-11-03T05:00:00Z', True, [], 'ledger_categories_unqualified'),
    ('2026-11-03T04:59:59Z', True, [], 'pre_start_window'),
    ('2026-11-03T05:00:00Z', True, [{}] * 3000, 'page_or_window_unproven'),
    ('2026-11-03T05:00:00Z', False, [], 'page_or_window_unproven'),
])
def test_external_source_receipt_never_proves_full_ledger(start, complete, rows, reason):
    report = report_at()
    report['account_external_flows'] = {
        'account_hash': report['summary']['account_observation']['account_hash'],
        'period_start': start, 'period_end': '2026-11-04T00:01:00Z',
        'currency': 'USD', 'source_complete': complete, 'transactions': rows,
    }
    result = project(report)['forward_reconciliation']['events']
    assert result['status'] == 'UNRECONCILED' and result['complete'] is False
    assert result['reason'] == reason
    assert all(v == 'UNRECONCILED' for v in result['categories'].values())


def test_untrusted_diagnostics_do_not_escape_in_fixed_qualification_reasons():
    report = report_at()
    report['account_external_flows'] = {'account_hash': 'PRIVATE_CANARY', 'error': 'PRIVATE_CANARY'}
    facts = project(report)['forward_reconciliation']
    assert 'PRIVATE_CANARY' not in str(facts)
    assert facts['events']['reason'] == 'account_binding_unproven'


def test_replayed_report_is_requalified_without_persisted_success():
    report = report_at()
    first = project(report)
    report['summary']['account_observation']['broker_reported_positions_complete'] = True
    second = project(report)
    assert second['forward_reconciliation'] == first['forward_reconciliation']
    assert second['forward_reconciliation']['complete'] is False


@pytest.mark.parametrize('bad', [False, True])
def test_pinned_same_response_collection_window_is_preserved_only_with_digest_and_time_binding(bad):
    snapshot = SimpleNamespace(as_of=datetime(2026, 11, 3, 6, tzinfo=timezone.utc),
        total_equity=100, positions=(), metadata={'account_hash': 'synthetic',
        'total_equity_source': 'broker_liquidation_value', 'source_digest_sha256': 'a' * 64,
        'schwab_position_quantities': {
            'source_digest_sha256': ('b' if bad else 'a') * 64,
            'observation_started_at': '2026-11-03T05:59:57Z',
            'observation_completed_at': '2026-11-03T06:00:00Z',
            'observation_window_valid': True, 'time_semantics': 'client_observation_window'}})
    obs = build_account_observation(snapshot, net_assets_currency='USD')
    if bad:
        assert 'broker_collection_window' not in obs
    else:
        assert obs['broker_collection_window']['started_at'] == '2026-11-03T05:59:57Z'
        report = report_at('2026-11-03T06:00:00Z')
        report['summary']['account_observation'] = obs
        timing = project(report)['forward_reconciliation']['timing']
        assert timing['collection_started_at'] == '2026-11-03T05:59:57Z'
        assert timing['collection_finished_at'] == '2026-11-03T06:00:00Z'
        assert timing['source_as_of'] is None


@pytest.mark.parametrize('value', ['2026-11-03T05:00:00.0000001Z', '2026-11-03T05:00:00',
                                   '2026-11-03', True, 'PRIVATE_CANARY'])
def test_unrepresentable_or_naive_timing_cannot_become_a_verified_receipt(value):
    from application.forward_account_qualification import parse_observation_instant
    assert parse_observation_instant(value) is None
