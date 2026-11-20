import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from zarrsum.submissions import (
    ContractMismatch, Journal, ReconciliationRequired, SlurmScheduler,
    encoded, immutable_bytes, read_document, sha,
)


class Scheduler:
    namespace='test-reef'
    def __init__(self):self.calls=0;self.jobs=[];self.find_calls=0
    def submit(self,script,token):
        self.calls+=1;self.jobs.append('12345');return '12345'
    def find(self,intent):self.find_calls+=1;return self.jobs


def fixture(tmp_path):
    source=tmp_path/'recipe.json';source.write_text('{"seed":42}')
    contract={'release_id':'a'*64,'payload_policy':'trusted-retained-stat',
              'scheduler_namespace':'test-reef','input_files':[{'path':str(source),'sha256':sha(source)}]}
    journal=Journal(tmp_path/'record')
    calls=[]
    def callback(name):
        def run(root,previous):
            calls.append(name)
            output=root/(name+'.json')
            immutable_bytes(output,encoded({'release_id':contract['release_id'],'stage':name}))
            return {'release_id':contract['release_id'],'artifacts':[str(output)]}
        return run
    callbacks={name:callback(name) for name in ['preparation','registration','snapshot']}
    return journal,contract,callbacks,calls,Scheduler()


def render(root,steps):return b'#!/bin/bash\nexit 0\n'


def test_submission_retry_never_dispatches_twice(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    first=journal.resume(contract,callbacks,render,scheduler)
    second=journal.resume(contract,callbacks,render,scheduler)
    assert first==second and first['job_id']=='12345'
    assert scheduler.calls==1 and calls==['preparation','registration','snapshot']


@pytest.mark.parametrize('stage',['preparation','registration','snapshot'])
def test_interrupted_action_reconciles_and_keeps_exact_contract(tmp_path,stage):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    def crash(event,value):
        if event=='after-action' and value==stage:raise SystemExit('crash')
    with pytest.raises(SystemExit):journal.resume(contract,callbacks,render,scheduler,crash)
    assert scheduler.calls==0
    result=journal.resume(contract,callbacks,render,scheduler)
    assert result['job_id']=='12345' and scheduler.calls==1
    assert calls.count(stage)==2


def test_acceptance_before_recording_is_reconciled(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    def crash(event,value):
        if event=='after-acceptance':raise SystemExit('crash')
    with pytest.raises(SystemExit):journal.resume(contract,callbacks,render,scheduler,crash)
    assert read_document(journal.root/'state.json')['job'] is None
    result=journal.resume(contract,callbacks,render,scheduler)
    assert result['job_id']=='12345' and result['reconciled']
    assert scheduler.calls==1 and scheduler.find_calls==1


@pytest.mark.parametrize('jobs',[[],['12345','23456']])
def test_uncertain_intent_refuses_resubmission(tmp_path,jobs):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    def crash(event,value):
        if event=='after-intent':raise SystemExit('crash')
    with pytest.raises(SystemExit):journal.resume(contract,callbacks,render,scheduler,crash)
    scheduler.jobs=jobs
    with pytest.raises(ReconciliationRequired,match='No resubmission'):
        journal.resume(contract,callbacks,render,scheduler)
    assert scheduler.calls==0


def test_query_failure_refuses_resubmission(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    def crash(event,value):
        if event=='after-intent':raise SystemExit('crash')
    with pytest.raises(SystemExit):journal.resume(contract,callbacks,render,scheduler,crash)
    scheduler.find=lambda intent:(_ for _ in ()).throw(ReconciliationRequired('query failed'))
    with pytest.raises(ReconciliationRequired):journal.resume(contract,callbacks,render,scheduler)
    assert scheduler.calls==0


@pytest.mark.parametrize('field,value',[('release_id','b'*64),('payload_policy','fresh-bytes'),('scheduler_namespace','another-cluster')])
def test_changed_submission_contract_is_refused(tmp_path,field,value):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    def crash(event,value):
        if event=='after-action':raise SystemExit('crash')
    with pytest.raises(SystemExit):journal.resume(contract,callbacks,render,scheduler,crash)
    replacement={**contract,field:value}
    with pytest.raises(ContractMismatch):journal.resume(replacement,callbacks,render,scheduler)
    assert scheduler.calls==0


def test_changed_input_provenance_is_refused_before_effects(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    Path(contract['input_files'][0]['path']).write_text('{"seed":43}')
    with pytest.raises(ContractMismatch):journal.resume(contract,callbacks,render,scheduler)
    assert not calls and scheduler.calls==0


def test_changed_snapshot_is_refused_after_dispatch(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    journal.resume(contract,callbacks,render,scheduler)
    path=journal.root/'snapshot.json';path.chmod(0o644);path.write_text('{}')
    with pytest.raises(ContractMismatch,match='provenance changed'):
        journal.resume(contract,callbacks,render,scheduler)
    assert scheduler.calls==1


def test_changed_script_is_refused_after_dispatch(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    journal.resume(contract,callbacks,render,scheduler)
    with pytest.raises(ContractMismatch):
        journal.resume(contract,callbacks,lambda root,steps:b'changed',scheduler)
    assert scheduler.calls==1


def test_wrong_stage_release_is_refused(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    callbacks['preparation']=lambda root,steps:{'release_id':'b'*64,'artifacts':[]}
    with pytest.raises(ContractMismatch):journal.resume(contract,callbacks,render,scheduler)
    assert scheduler.calls==0


def test_concurrent_controller_is_refused(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    with journal.locked():
        with pytest.raises(ReconciliationRequired,match='Another controller'):
            Journal(journal.root).resume(contract,callbacks,render,scheduler)
    assert scheduler.calls==0


def test_actual_slurm_command_parser_deduplicates_queue_and_accounting():
    token='dv-'+'0'*32+'-'+'a'*12
    seen=[]
    def runner(command,**kwargs):
        seen.append(command)
        if command[0]=='scontrol':out='ClusterName = test-reef\n'
        elif command[0]=='squeue':out=f'12345|{os.getuid()}|{token}\n'
        elif command[0]=='sacct':out=f'12345|{os.getuid()}|{token}|COMPLETED\n'
        else:raise AssertionError(command)
        return SimpleNamespace(returncode=0,stdout=out)
    scheduler=SlurmScheduler(runner)
    assert scheduler.find({'owner_uid':os.getuid(),'scheduler_namespace':'test-reef','created_epoch':1_700_000_000,'token':token})==['12345']
    assert len(seen)==3


def test_malformed_sbatch_reply_keeps_dispatch_uncertain():
    def runner(command,**kwargs):
        return SimpleNamespace(returncode=0,stdout='ClusterName = test-reef\n' if command[0]=='scontrol' else 'not-a-job\n')
    scheduler=SlurmScheduler(runner)
    with pytest.raises(ReconciliationRequired):scheduler.submit('/tmp/script','token')


def test_missing_intent_after_acceptance_never_creates_another_dispatch(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    def crash(event,value):
        if event=='after-acceptance':raise SystemExit('crash')
    with pytest.raises(SystemExit):journal.resume(contract,callbacks,render,scheduler,crash)
    (journal.root/'dispatch-intent.json').unlink()
    with pytest.raises(ReconciliationRequired,match='intent is missing'):
        journal.resume(contract,callbacks,render,scheduler)
    assert scheduler.calls==1


def test_missing_progress_never_resets_the_record(tmp_path):
    journal,contract,callbacks,calls,scheduler=fixture(tmp_path)
    journal.resume(contract,callbacks,render,scheduler)
    (journal.root/'state.json').unlink()
    with pytest.raises(ReconciliationRequired,match='progress is missing'):
        journal.resume(contract,callbacks,render,scheduler)
    assert scheduler.calls==1
