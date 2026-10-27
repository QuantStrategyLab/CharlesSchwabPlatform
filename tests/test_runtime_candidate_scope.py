"""Single-target final report publication under the approved object scope."""
import copy
import json
from types import SimpleNamespace
import pytest
from notifications import unified


class Blob:
    def __init__(self, client, name):
        self.client,self.name,self.generation=client,name,None
    def reload(self, **kwargs):
        from google.api_core.exceptions import NotFound
        if self.name not in self.client.data:raise NotFound('synthetic')
        self.generation=self.client.data[self.name][0]
    def download_as_text(self, **kwargs):
        assert kwargs.get('if_generation_match')==self.generation
        return json.dumps(self.client.data[self.name][1])
    def upload_from_string(self, body, **kwargs):
        from google.api_core.exceptions import PreconditionFailed
        generation=self.client.data.get(self.name,(0,None))[0]
        if kwargs['if_generation_match']!=generation:raise PreconditionFailed('synthetic')
        self.client.data[self.name]=(generation+1,json.loads(body))


class Client:
    def __init__(self):self.data={}
    def list_blobs(self, *args, **kwargs):raise AssertionError('bucket listing is forbidden')


def setup(monkeypatch):
    from google.cloud import storage
    client=Client()
    monkeypatch.setattr(storage.Blob,'from_string',lambda name,client:Blob(client,name))
    env={unified.FLAG:'true','DIGEST_CANDIDATES_GCS_URI':'gs://synthetic-candidates/reports','DIGEST_STORE_TARGET':'primary-live'}
    return client,unified.GCSNotificationStore(env,client=client),env


def payload(run='one'):
    return {'schema_version':'qsl.digest_candidates.v1','producer_status':'projected','runs':[{
        'platform_id':'schwab','opaque_account_uid':'synthetic','target_id':'synthetic',
        'strategy_profile':'synthetic','source_run_id':run,'business_day':'2026-10-27',
        'actually_ran':True,'cycle_count':1,'order_count':None,'fill_count':None,
        'field_status':{'order_count':'unknown','fill_count':'unknown'}}]}


def test_final_report_writer_merges_runs_once_without_listing_or_inventing_counts(monkeypatch):
    client,store,env=setup(monkeypatch)
    unified.persist_projected_candidate(payload(),env=env,store=store)
    unified.persist_projected_candidate(payload('two'),env=env,store=store)
    before=copy.deepcopy(client.data)
    unified.persist_projected_candidate(payload(),env=env,store=store)
    assert client.data==before
    assert len(client.data)==1
    name,(version,value)=next(iter(client.data.items()))
    assert name=='gs://synthetic-candidates/reports/schwab/2026-10-27/primary-live.json'
    assert len(value['runs'])==2 and version==2
    assert all(row['order_count'] is None and row['fill_count'] is None for row in value['runs'])


def test_conflicting_evidence_or_other_identity_is_never_overwritten(monkeypatch):
    client,store,env=setup(monkeypatch)
    unified.persist_projected_candidate(payload(),env=env,store=store)
    before=copy.deepcopy(client.data)
    changed=payload();changed['runs'][0]['order_count']=3
    with pytest.raises(ValueError):unified.persist_projected_candidate(changed,env=env,store=store)
    changed=payload('two');changed['runs'][0]['target_id']='other'
    with pytest.raises(ValueError):unified.persist_projected_candidate(changed,env=env,store=store)
    assert client.data==before


def test_staged_event_uses_only_the_approved_marker_directory(monkeypatch):
    client,store,_=setup(monkeypatch)
    fact={'dedupe_key':'synthetic','coverage_status':'known','business_day':'2026-10-27'}
    assert store.persist(fact)
    assert all('/schwab/notification-markers/' in name for name in client.data)


def test_completed_real_report_keeps_actual_alias_and_unknown_fill_count(monkeypatch):
    from test_notification_qrs_roundtrip import producer as make_producer
    client,store,_=setup(monkeypatch)
    source=make_producer.__wrapped__(monkeypatch)
    report=source.run('completed-source',({'status':'filled','broker_order_id':'synthetic'},))
    original=copy.deepcopy(report)
    assert unified.persist_completed_report(report,env=source.env,store=store)
    assert report==original and report['platform'] in {'charles_schwab','schwab'}
    value=next(iter(client.data.values()))[1]
    assert value['runs'][0]['fill_count'] is None
    assert value['runs'][0]['field_status']['fill_count']=='unknown'
    assert value['runs'][0]['source_run_id']=='completed-source'


def test_new_receipt_observation_merges_into_same_run_without_changing_counts(monkeypatch):
    client,store,env=setup(monkeypatch)
    first=payload();unified.persist_projected_candidate(first,env=env,store=store)
    observed=payload();row=observed['runs'][0]
    row['notification_events']=[{k:row[k] for k in ('platform_id','opaque_account_uid','target_id','strategy_profile','business_day','source_run_id')} | {
        'dedupe_key':'synthetic-receipt','event_kind':'healthy_heartbeat'}]
    unified.persist_projected_candidate(observed,env=env,store=store)
    unified.persist_projected_candidate(first,env=env,store=store)
    result=next(iter(client.data.values()))[1]['runs']
    assert len(result)==1 and result[0]['cycle_count']==1
    assert result[0]['order_count'] is None and len(result[0]['notification_events'])==1
