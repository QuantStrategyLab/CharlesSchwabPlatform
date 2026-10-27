"""Inspect existing reports without credentials, broker IO or publication."""
import builtins
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from scripts import publish_account_facts_from_reports as publisher

PRIVATE = "PRIVATE_INSPECTION_CANARY"


@pytest.fixture
def case(monkeypatch):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    started, observed, finished = now - timedelta(seconds=10), now - timedelta(seconds=5), now - timedelta(seconds=1)
    run_id = started.strftime('%Y%m%dT%H%M%SZ')
    prefix = 'gs://synthetic-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/'
    uri = prefix + started.strftime('%Y-%m/') + run_id + '.json'
    env = dict(GITHUB_ACTIONS='true', GITHUB_EVENT_NAME='workflow_dispatch', GITHUB_REF='refs/heads/main',
        GITHUB_REPOSITORY='QuantStrategyLab/CharlesSchwabPlatform', GITHUB_RUN_ATTEMPT='1', GITHUB_SHA='a' * 40,
        SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX=prefix, SCHWAB_ACCOUNT_FACTS_SERVICE_NAME='synthetic-service',
        SCHWAB_ACCOUNT_FACTS_TARGET_ID='synthetic-target', SCHWAB_NET_ASSETS_CURRENCY='USD',
        SCHWAB_CASH_CURRENCY='USD', SCHWAB_ACCOUNT_FACTS_INCLUDE_POSITIONS='true',
        SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN=PRIVATE, ACCOUNT_FACTS_SYNC_URL=PRIVATE)
    for key, value in env.items(): monkeypatch.setenv(key, value)
    observation = dict(account_hash=PRIVATE, currency=None, observed_at=observed.isoformat(),
        net_assets='12345.67', net_assets_source='liquidationValue', net_assets_currency='USD',
        net_assets_currency_source='owner_confirmed', broker_stock_inventory_status='unknown',
        broker_stock_inventory_diagnostics=dict(positions_field_present=False, linked_positions_field_present=False,
                                               native_positions_count=None, projected_stock_count=None))
    report = dict(schema_version='runtime_report.v1', platform=publisher.PLATFORM, project_id=publisher.PROJECT_ID,
        service_name='synthetic-service', strategy_profile=publisher.STRATEGY_PROFILE, account_scope='live',
        status='ok', errors=[], run_id=run_id, started_at=started.isoformat(), finished_at=finished.isoformat(),
        runtime_target=dict(strategy_profile=publisher.STRATEGY_PROFILE, account_scope='live', account_selector=[PRIVATE]),
        diagnostics=dict(runtime_revision='synthetic-service-00007-abc'), summary=dict(account_observation=observation))
    case = dict(report=report, observation=observation, serving='synthetic-service-00007-abc', uri=uri, calls=[])
    monkeypatch.setattr(publisher, '_current_serving_revision', lambda _: case['serving'])
    monkeypatch.setattr(publisher, '_latest_recent_report_uri', lambda **_: case['uri'])
    monkeypatch.setattr(publisher, '_read_report', lambda _: case['report'])
    def forbidden(*_args, **_kwargs): pytest.fail('inspection attempted an external side effect')
    monkeypatch.setattr(publisher, '_publish_once', forbidden)
    monkeypatch.setattr(publisher.subprocess, 'run', forbidden)
    original_getenv = publisher.os.getenv
    def getenv(key, *args):
        assert key not in {'SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN', 'ACCOUNT_FACTS_SYNC_URL'}, 'must branch before sync credential access'
        return original_getenv(key, *args)
    monkeypatch.setattr(publisher.os, 'getenv', getenv)
    original_import = builtins.__import__
    def checked_import(name, *args, **kwargs):
        assert name.split('.')[0] not in {'schwab', 'httpx', 'requests', 'main'}, 'must not import broker/runtime clients'
        return original_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', checked_import)
    return case


def _invoke(capsys):
    code = publisher.main(['--latest-report', '--inspect-only'])
    raw = capsys.readouterr().out
    assert PRIVATE not in raw and 'gs://' not in raw and 'https://' not in raw
    assert '12345.67' not in raw and '17.3125' not in raw and '347.08' not in raw and 'ZZZSYN' not in raw
    result = json.loads(raw)
    assert result['broker_requested'] is result['published'] is result['notifications_requested'] is False
    return code, result


def _available(case, *, empty=False):
    observation = case['observation']
    positions = [] if empty else [dict(symbol='ZZZSYN', quantity='17.3125', market_value='347.08',
                                      currency='USD', currency_source='owner_confirmed')]
    observation.update(broker_stock_inventory_status='available', broker_reported_positions=positions,
        broker_reported_positions_scope='stocks_only', broker_reported_positions_complete=True,
        broker_reported_positions_observed_at=observation['observed_at'])
    observation['broker_stock_inventory_diagnostics'].update(positions_field_present=True,
        native_positions_count=len(positions), projected_stock_count=len(positions))


def test_missing_positions_stay_unknown_without_token_read_import_or_publish(case, capsys):
    code, result = _invoke(capsys)
    assert code == 0 and result['status'] == 'inspected' and result['report_identity_verified'] is True
    assert result['inventory_status'] == 'unknown'
    assert result['native_positions_present'] is result['linked_positions_present'] is False
    assert result['native_positions_count'] is result['report_positions_count'] is None
    assert result['report_positions_present'] is result['projected_positions_present'] is result['payload_positions_retained'] is False
    assert result['include_positions_enabled'] is True
    assert result['inspection_source_sha'] == 'a' * 40
    assert len(result['serving_revision_sha256']) == 64 and case['serving'] not in json.dumps(result)


@pytest.mark.parametrize('enabled,empty', [(True, False), (False, False), (True, True)])
def test_native_list_projection_and_payload_retention_are_separate(case, monkeypatch, capsys, enabled, empty):
    _available(case, empty=empty)
    monkeypatch.setenv('SCHWAB_ACCOUNT_FACTS_INCLUDE_POSITIONS', str(enabled).lower())
    code, result = _invoke(capsys)
    assert code == 0 and result['inventory_status'] == 'available'
    assert result['report_positions_count'] == (0 if empty else 1)
    assert result['native_positions_count'] == (0 if empty else 1)
    assert result['report_positions_present'] is result['projected_positions_present'] is True
    assert result['payload_positions_retained'] is enabled


@pytest.mark.parametrize('change', ['identity', 'selector', 'revision', 'stale', 'unavailable', 'serving_changed'])
def test_source_failure_cannot_expose_inventory_counts(case, monkeypatch, capsys, change):
    _available(case)
    if change == 'identity': case['report']['service_name'] = PRIVATE
    elif change == 'selector': case['report']['runtime_target']['account_selector'] = ['different-synthetic-account']
    elif change == 'revision': case['report']['diagnostics']['runtime_revision'] = 'synthetic-service-other'
    elif change == 'stale': case['observation']['observed_at'] = '2000-01-01T00:00:00Z'
    elif change == 'unavailable': case['report'] = None
    else:
        revisions = iter([case['serving'], 'synthetic-service-other'])
        monkeypatch.setattr(publisher, '_current_serving_revision', lambda _: next(revisions))
    code, result = _invoke(capsys)
    assert code == 2 and result['status'] == 'skipped'
    assert result['report_identity_verified'] is False
    assert result['native_positions_count'] is result['report_positions_count'] is None
    assert result['payload_positions_retained'] is None


def test_untrusted_diagnostics_only_project_typed_closed_fields(case, capsys):
    case['observation']['broker_stock_inventory_diagnostics'] = dict(positions_field_present=PRIVATE,
        linked_positions_field_present={'raw': PRIVATE}, native_positions_count=True, projected_stock_count=-1,
        unlisted=PRIVATE, reason=PRIVATE)
    code, result = _invoke(capsys)
    assert code == 0
    assert result['native_positions_present'] is result['linked_positions_present'] is None
    assert result['native_positions_count'] is result['projected_stock_count'] is None


@pytest.mark.parametrize('key,value', [('GITHUB_REF', 'refs/heads/other'), ('GITHUB_RUN_ATTEMPT', '2'),
    ('GITHUB_REPOSITORY', 'other/repository')])
def test_inspection_rejects_nonprotected_context_before_report_read(case, monkeypatch, capsys, key, value):
    monkeypatch.setenv(key, value)
    monkeypatch.setattr(publisher, '_read_report', lambda _: pytest.fail('must not read report'))
    code, result = _invoke(capsys)
    assert code == 2 and result['reason'] == 'inspection_context_required'


def test_read_exception_never_exposes_native_message(case, monkeypatch, capsys):
    def fail(_): raise RuntimeError(PRIVATE)
    monkeypatch.setattr(publisher, '_read_report', fail)
    code, result = _invoke(capsys)
    assert code == 2 and result['reason'] == 'inventory_inspection_failed'


def test_inspection_cannot_select_an_arbitrary_report_uri(case, capsys):
    assert publisher.main(['--report-uri', case['uri'], '--expected-runtime-revision', case['serving'], '--inspect-only']) == 2
    assert capsys.readouterr().out == 'skipped:inspection_requires_manual_latest_report\n'


def test_workflow_inspection_step_never_receives_sync_or_broker_token():
    source = Path('.github/workflows/account-facts-sync.yml').read_text()
    inspect = source.split('- name: Inspect inventory in the latest existing report without publishing', 1)[1].split(
        '- name: Project and publish the selected archived report', 1)[0]
    assert 'if: ${{ inputs.inspect_only }}' in inspect
    assert '--latest-report --inspect-only' in inspect
    assert 'TOKEN' not in inspect and 'SYNC_URL' not in inspect
    assert 'REPORT_URI:' not in inspect and 'EXPECTED_RUNTIME_REVISION:' not in inspect
    assert 'VALID_INSPECTION_SELECTOR:' in inspect
    publish = source.split('- name: Project and publish the selected archived report', 1)[1]
    assert 'if: ${{ !inputs.inspect_only }}' in publish
    assert 'SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN' in publish
