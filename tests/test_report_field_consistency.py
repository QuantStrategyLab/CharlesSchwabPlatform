"""Early and archived producers retain the same execution facts."""
import copy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from application.runtime_report_summary import summarize_execution_cycle_result
from notifications import unified


@pytest.mark.parametrize('status', ['blocked', 'unknown', 'failed', 'no_action', 'pending_reconciliation'])
@pytest.mark.parametrize('observations', [None, {}, {'watch': 1}])
def test_early_and_archived_candidates_agree(status, observations):
    from application.runtime_report_summary import execution_report_status
    target = dict(platform_id='schwab', strategy_profile='fake-strategy', service_name='fake-service',
                  account_scope='fake', account_selector=['fake'], deployment_selector='fake', dry_run_only=False)
    now = datetime.now(timezone.utc)
    report = dict(schema_version='runtime_report.v1', platform='schwab', runtime_target=target,
                  strategy_profile='fake-strategy', service_name='fake-service', run_id='fake-run',
                  started_at=now.isoformat(), dry_run=False, summary={})
    if observations is not None:
        report['summary']['routine_observation_counts'] = observations
    result = SimpleNamespace(execution={'execution_status': status}, submitted_orders=[], trade_logs=[])
    original = copy.deepcopy(report)
    with unified.notification_context(report):
        early = unified._cycle_projection(result, {'opaque_account_uid': 'fake-opaque', 'target_id': 'fake-target'}, SimpleNamespace(dry_run_only=False))
    assert report == original
    summary = summarize_execution_cycle_result(result, dry_run=False, existing_summary=report['summary'])
    report.update(summary=summary, status=execution_report_status(summary))
    assert report['status'] == ('ok' if status in {'no_action', 'pending_reconciliation'} else 'error')
    archive = unified._project_runs([report], expected_target=target, opaque_account_uid='fake-opaque', target_id='fake-target', now=now)
    # Events are merged separately by the real store; compare canonical facts.
    early["runs"][0].pop("notification_events")
    archive["runs"][0].pop("notification_events")
    assert early == archive
    row = archive['runs'][0]
    assert ('routine_observation_counts' in row) == (observations is not None)
    if observations is not None:
        assert row['routine_observation_counts'] == observations
    assert row['execution_status'] == ('order_submitted' if status == 'pending_reconciliation' else status)


def test_missing_execution_fails_closed():
    from application.runtime_report_summary import execution_report_status
    assert execution_report_status({}) == 'error'


def test_early_projection_uses_complete_execution_summary(monkeypatch):
    observed = []
    monkeypatch.setattr(unified, '_project_runs', lambda reports, **kw: observed.extend(reports) or {'runs': []})
    result = SimpleNamespace(execution={'execution_status': 'blocked'}, submitted_orders=[], trade_logs=[])
    source = {'runtime_target': {'platform_id': 'schwab'}, 'summary': {'routine_observation_counts': {}}}
    with unified.notification_context(source):
        unified._cycle_projection(result, {'opaque_account_uid': 'fake', 'target_id': 'fake'}, SimpleNamespace(dry_run_only=False))
    assert observed[0]['status'] == 'error'
    assert observed[0]['summary']['result'] == 'blocked'
    assert observed[0]['summary']['routine_observation_counts'] == {}


def test_unknown_and_observed_empty_remain_distinct():
    missing = unified._canonical_candidate_row({'source_run_id': 'fake-run'})
    empty = unified._canonical_candidate_row({'source_run_id': 'fake-run', 'routine_observation_counts': {}})
    assert missing != empty


def test_current_execution_overrides_stale_summary_without_mutating_it():
    previous = {'execution_status': 'completed', 'result': 'completed', 'routine_observation_counts': {}}
    result = SimpleNamespace(execution={'execution_status': 'blocked'}, submitted_orders=[], trade_logs=[])
    summary = summarize_execution_cycle_result(result, dry_run=False, existing_summary=previous)
    assert summary['execution_status'] == summary['result'] == 'blocked'
    assert summary['routine_observation_counts'] == {}
    assert previous['execution_status'] == 'completed'
