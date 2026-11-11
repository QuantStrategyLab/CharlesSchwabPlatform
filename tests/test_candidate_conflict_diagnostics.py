"""Fake-only conflict evidence; no candidate values may reach diagnostics."""
from copy import deepcopy
import json

import pytest
from google.api_core.exceptions import PreconditionFailed

from notifications import unified
from scripts import emit_unified_digest_candidates as publisher
from test_runtime_candidate_scope import payload, setup


def test_conflict_entrypoint_reports_bounded_fields_without_values(monkeypatch, capsys):
    client, store, env = setup(monkeypatch)
    first = payload()
    first['runs'][0].update(as_of='2026-10-27T20:00:00Z', status='ok')
    unified.persist_projected_candidate(first, env=env, store=store)
    before = deepcopy(client.data)
    changed = deepcopy(first)
    changed['runs'][0].update(order_count=2, status='<PROJECT_ID>-secret',
        as_of='2026-10-28T04:00:00+08:00', block_reason=None)
    changed['runs'][0]['<PROJECT_ID>-private-key'] = '<PROJECT_ID>-private-value'
    monkeypatch.setattr(publisher, 'emit_completed_candidates',
        lambda _: unified.persist_projected_candidate(changed, env=env, store=store))
    monkeypatch.setattr(publisher, 'warn_coverage_unconfirmed', lambda *args: None)
    assert publisher.main() == 1
    output = capsys.readouterr()
    assert '<PROJECT_ID>' not in output.out + output.err
    assert len(output.out) < 12000
    result = json.loads(output.out)
    assert result['stage'] == 'candidate_merge'
    assert result['error_code'] == 'notification_candidate_source_conflict'
    diagnostic = result['conflict']
    fields = {item['field']: item for item in diagnostic['fields']}
    assert fields['order_count']['classification'] == 'changed'
    assert fields['order_count']['prior_type'] == 'null'
    assert fields['order_count']['candidate_type'] == 'integer'
    assert fields['as_of']['classification'] == 'representation_only'
    assert fields['block_reason']['classification'] == 'representation_only'
    assert fields['status']['classification'] == 'changed'
    assert diagnostic['unlisted_fields_changed'] is True
    assert client.data == before


@pytest.mark.parametrize('field,value', [('order_count', 2), ('fill_count', 1),
    ('status', 'failed'), ('execution_status', 'blocked'),
    ('as_of', '2026-10-27T20:01:00Z'), ('target_id', '<PROJECT_ID>-other'),
    ('source_run_id', None)])
def test_changed_facts_fail_without_writing(monkeypatch, field, value):
    client, store, env = setup(monkeypatch)
    first = payload()
    first['runs'][0]['as_of'] = '2026-10-27T20:00:00Z'
    unified.persist_projected_candidate(first, env=env, store=store)
    before = deepcopy(client.data)
    changed = deepcopy(first)
    changed['runs'][0][field] = value
    with pytest.raises(ValueError):
        unified.persist_projected_candidate(changed, env=env, store=store)
    assert client.data == before


def test_cas_conflict_and_restart_preserve_winning_content(monkeypatch):
    client, store, env = setup(monkeypatch)
    unified.persist_projected_candidate(payload(), env=env, store=store)
    before = deepcopy(client.data)
    original_blob = store._blob
    def racing_blob(suffix):
        blob = original_blob(suffix)
        def conflict(*args, **kwargs):
            raise PreconditionFailed('<PROJECT_ID>-secret')
        blob.upload_from_string = conflict
        return blob
    monkeypatch.setattr(store, '_blob', racing_blob)
    with pytest.raises(unified.CandidateStorageError) as caught:
        unified.persist_projected_candidate(payload('two'), env=env, store=store)
    assert caught.value.category == 'generation_conflict'
    assert client.data == before
    restarted = unified.GCSNotificationStore(env, client=client)
    assert unified.persist_projected_candidate(payload(), env=env, store=restarted)
    assert client.data == before


def test_event_only_merge_rejects_changed_identity(monkeypatch):
    client, store, env = setup(monkeypatch)
    first = payload()
    unified.persist_projected_candidate(first, env=env, store=store)
    before = deepcopy(client.data)
    changed = deepcopy(first)
    row = changed['runs'][0]
    event = {k: row[k] for k in ('platform_id', 'opaque_account_uid', 'target_id',
                               'strategy_profile', 'business_day', 'source_run_id')}
    event.update(dedupe_key='<PROJECT_ID>-event', target_id='<PROJECT_ID>-other')
    row['notification_events'] = [event]
    with pytest.raises(ValueError, match='notification_event_identity_unknown'):
        unified.persist_projected_candidate(changed, env=env, store=store)
    assert client.data == before


def test_diagnostic_budget_and_vocabulary_ignore_private_content():
    prior = {'status': '<PROJECT_ID>-secret'}
    candidate = {'status': {'<PROJECT_ID>-secret': ['<PROJECT_ID>-secret'] * 10000}}
    candidate.update({f'<PROJECT_ID>-private-{i}': '<PROJECT_ID>-secret' for i in range(10000)})
    error = unified.CandidateConflictError(prior, candidate, candidate)
    output = json.dumps(publisher._failure_diagnostic(error, 'projection'))
    assert len(output) < 6000
    assert '<PROJECT_ID>' not in output
    error._fields.append(('<PROJECT_ID>-private', 'string', 'string', False, False))
    error._fields[0] = ('status', '<PROJECT_ID>-private', 'string', False, False)
    assert '<PROJECT_ID>' not in json.dumps(publisher._failure_diagnostic(error, 'projection'))
