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


@pytest.mark.parametrize('previous', [{}, {'execution_status': 'completed', 'result': 'completed'}])
def test_completed_zero_order_cycle_is_healthy_without_submission_status(monkeypatch, previous):
    from application.runtime_report_summary import execution_report_status
    from notifications.accepted_orders import cycle_notification_evidence
    from scripts import execution_report_heartbeat as heartbeat
    from test_notification_quiet import NOW, mock_heartbeat
    from test_runtime_daily_report_projection import report

    result = SimpleNamespace(execution={'notification_cycle_completed': True}, submitted_orders=[], trade_logs=[])
    before = copy.deepcopy(result.execution)
    summary = summarize_execution_cycle_result(result, dry_run=False, existing_summary=previous)
    assert summary['notification_digest'] == cycle_notification_evidence(result.execution, [])
    assert summary['notification_digest']['routine'] is True
    assert execution_report_status(summary) == 'ok'
    assert summary['execution_status'] == summary['result'] == 'no_action'
    source = report('no_action', summary=summary, status=execution_report_status(summary),
                    started_at='2026-10-05T12:00:00Z', finished_at='2026-10-05T12:01:00Z', diagnostics={})
    sent = mock_heartbeat(monkeypatch, source)
    assert heartbeat.main(NOW) == 0
    assert sent == []
    assert result.execution == before


@pytest.mark.parametrize('change', [
    {'notification_cycle_completed': False}, {'notification_cycle_completed': 1},
    {'notification_order_issue': True}, {'errors': ['fake-error']},
    {'signal_metadata': {'errors': ['fake-error']}}, {'orders_pending_count': 1},
    {'broker_submission_done': True}, {'execution_status': 'unknown'},
    {'execution_status': 'blocked'}, {'execution_status': 'failed'},
])
@pytest.mark.parametrize('dry_run', [False, True])
def test_completion_marker_cannot_hide_missing_or_conflicting_evidence(change, dry_run):
    from application.runtime_report_summary import execution_report_status
    result = SimpleNamespace(execution={'notification_cycle_completed': True, **change},
                             submitted_orders=[], trade_logs=[])
    summary = summarize_execution_cycle_result(result, dry_run=dry_run)
    assert execution_report_status(summary) == 'error'


def test_completion_marker_does_not_establish_submission_outcome():
    from application.runtime_report_summary import execution_report_status
    result = SimpleNamespace(execution={'notification_cycle_completed': True},
                             submitted_orders=[{'status': 'unknown'}], trade_logs=[])
    assert execution_report_status(summarize_execution_cycle_result(result, dry_run=False)) == 'error'


@pytest.mark.parametrize('previous', [{}, {'execution_status': 'completed', 'result': 'completed'}])
def test_completed_empty_dry_run_is_healthy_without_live_digest_evidence(previous):
    from application.runtime_report_summary import execution_report_status
    from notifications.accepted_orders import cycle_notification_evidence

    result = SimpleNamespace(execution={'notification_cycle_completed': True}, submitted_orders=[], trade_logs=[])
    before = copy.deepcopy(result)
    previous_before = copy.deepcopy(previous)
    summary = summarize_execution_cycle_result(result, dry_run=True, existing_summary=previous)
    assert execution_report_status(summary) == 'ok'
    assert summary['execution_status'] == summary['result'] == 'dry_run_completed'
    assert summary['orders_previewed_count'] == summary['orders_submitted_count'] == 0
    assert summary['broker_submission_done'] is False
    assert summary['dry_run_order_preview_available'] is False
    assert summary['notification_digest'] == cycle_notification_evidence(result.execution, [], dry_run=True)
    assert summary['notification_digest']['routine'] is False
    assert summary['notification_digest']['order_count'] is None
    assert summary['notification_digest']['fill_count'] is None
    assert result == before and previous == previous_before


@pytest.mark.parametrize('dry_run', [False, True])
def test_empty_execution_without_completion_is_not_healthy(dry_run):
    from application.runtime_report_summary import execution_report_status
    result = SimpleNamespace(execution={}, submitted_orders=[], trade_logs=[])
    assert execution_report_status(summarize_execution_cycle_result(result, dry_run=dry_run)) == 'error'
