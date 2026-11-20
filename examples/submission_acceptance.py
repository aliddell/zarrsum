"""Kill real controllers at workflow boundaries and recover two actual CPU jobs."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from submission_workflow import A,B,DATASET,PREVIOUS,SOURCE,DurableCatalog,make_contract
from zarrsum.catalog_releases import publish_ready
from zarrsum.metadata_releases import Repository
from zarrsum.submissions import SlurmScheduler,atomic_document,encoded,read_document,sha

TASK=Path('/mnt/main0/home/aliddell/scratch/restartable-submission-20261119')
RUNTIME=Path(__file__).resolve().parents[1]


def attempt(root,name,record,contract,catalog,expected,kill=None):
    command=[sys.executable,'-B','-s',str(RUNTIME/'examples/submission_workflow.py'),str(record),
             '--contract',str(contract),'--catalog',str(catalog)]
    if kill:command+=['--kill-at',kill]
    result=subprocess.run(command,capture_output=True,text=True,timeout=150)
    (root/(name+'.stdout')).write_text(result.stdout)
    (root/(name+'.stderr')).write_text(result.stderr)
    atomic_document(root/(name+'.json'),{'returncode':result.returncode,'expected':expected,
                                       'hook':kill,'time_ns':time.time_ns()})
    if result.returncode!=expected:
        raise AssertionError(f'{name}: expected {expected}, got {result.returncode}; inspect stderr')
    return result


def new_case(root,name,namespace):
    parent=root/name;parent.mkdir()
    contract=make_contract(RUNTIME,namespace)
    contract['submission_key']=name+'-'+uuid.uuid4().hex
    contract['acceptance_gate']=True
    request=parent/'requested-config.json'
    request.write_bytes(encoded({'release_id':A,'payload_policy':contract['payload_policy'],
                                 'recipe_sha256':contract['recipe_sha256']}))
    contract['request_config']=str(request)
    contract['input_files'].append({'path':str(request),'sha256':sha(request)})
    incoming=parent/'incoming-contract.json';incoming.write_bytes(encoded(contract))
    return parent,parent/'journal',incoming


def events(record):
    return [json.loads(line) for line in (record/'events.jsonl').read_text().splitlines()]


def wait_for_job(record,job_id,scheduler):
    intent=read_document(record/'dispatch-intent.json')
    assert scheduler.find(intent)==[job_id]
    atomic_document(record/'allow-loader',{'job_id':job_id})
    until=time.monotonic()+300
    while time.monotonic()<until:
        query=subprocess.run(['sacct','-n','-P','-X','-j',job_id,'-o','JobIDRaw,State,ExitCode'],capture_output=True,text=True,timeout=30)
        query.check_returncode()
        rows=[line.strip().split('|') for line in query.stdout.splitlines() if line.strip()]
        row=next((value for value in rows if value[0]==job_id),None)
        if row and row[1].split()[0] not in ['PENDING','RUNNING','CONFIGURING','COMPLETING','REQUEUED']:
            if row[1]!='COMPLETED' or row[2]!='0:0':raise AssertionError(f'CPU job {job_id}: {row}')
            verification=read_document(record/'worker-verification.json')
            assert verification['job_id']==job_id and verification['release_id']==A
            assert verification['samples']==512 and verification['worker_exitcodes']==[0,0,0,0]
            assert scheduler.find(intent)==[job_id]
            return verification
        time.sleep(2)
    raise TimeoutError('The bounded CPU job did not complete: '+job_id)


def main():
    root=TASK/'runs'/('acceptance-'+time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+uuid.uuid4().hex[:6])
    root.mkdir(parents=True)
    atomic_document(TASK/'active-acceptance.json',{'root':str(root),'pid':os.getpid()})
    scheduler=SlurmScheduler()
    catalog=root/'catalog-state.json'
    summary={'result':'running','root':str(root),'namespace':scheduler.namespace,'A':A,'B':B,
             'started_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'actual_jobs':[]}
    def save():atomic_document(root/'result.json',summary)
    save()
    parent,record,incoming=new_case(root,'boundary-retries',scheduler.namespace)
    original=read_document(incoming)
    attempt(root,'01-kill-after-preparation',record,incoming,catalog,-signal.SIGKILL,'after-action:preparation')
    assert not (record/'dispatch-intent.json').exists()
    # B becomes discoverable between attempts. Exact-A submission must not change.
    remote=DurableCatalog(catalog)
    publish_ready(Repository(SOURCE/'retained',shared_filesystem=read_document(TASK/'retained-fixture.json')['storage_identity']),DATASET,B,'reef',remote.client,root/'advertised-B.json')
    for value in remote.records.values():value['is_latest']=value['version']==B
    remote.persist();remote.client.close()
    atomic_document(root/'B-became-available.json',{'release_id':B,'time_ns':time.time_ns()})
    attempt(root,'02-kill-after-registration',record,incoming,catalog,-signal.SIGKILL,'after-action:registration')
    attempt(root,'03-kill-after-snapshot',record,incoming,catalog,-signal.SIGKILL,'after-action:snapshot')
    assert not (record/'dispatch-intent.json').exists()
    for index,(field,value) in enumerate([('release_id',B),('payload_policy','fresh-bytes')]):
        replacement={**original,field:value}
        altered=parent/('altered-'+field+'.json');altered.write_bytes(encoded(replacement))
        attempt(root,'04-refuse-'+str(index),record,altered,catalog,2)
    request=Path(original['request_config']);saved_request=request.read_bytes()
    request.write_text('{"release_id":"changed"}')
    attempt(root,'05-refuse-changed-provenance',record,incoming,catalog,2)
    request.write_bytes(saved_request)
    completed=attempt(root,'06-submit-original-A',record,incoming,catalog,0)
    first=json.loads(completed.stdout.splitlines()[-1]);job1=first['job_id']
    summary['actual_jobs'].append(job1);save()
    retry=attempt(root,'07-repeat-known-submission',record,incoming,catalog,0)
    assert json.loads(retry.stdout.splitlines()[-1])['job_id']==job1
    assert sum(value['event']=='submit-call-start' for value in events(record))==1
    verified1=wait_for_job(record,job1,scheduler)
    snapshot=record/'catalog-snapshot.json';snapshot_bytes=snapshot.read_bytes()
    snapshot.chmod(0o644);snapshot.write_text('{}')
    attempt(root,'08-refuse-changed-snapshot',record,incoming,catalog,2)
    snapshot.write_bytes(snapshot_bytes);snapshot.chmod(0o444)
    script=record/'job.sbatch';script_bytes=script.read_bytes()
    script.chmod(0o644);script.write_bytes(script_bytes+b'\n# altered\n')
    attempt(root,'09-refuse-changed-script',record,incoming,catalog,2)
    script.write_bytes(script_bytes);script.chmod(0o444)
    attempt(root,'10-known-job-after-restoring-provenance',record,incoming,catalog,0)
    assert scheduler.find(read_document(record/'dispatch-intent.json'))==[job1]
    parent2,record2,incoming2=new_case(root,'accepted-before-record',scheduler.namespace)
    attempt(root,'11-kill-after-scheduler-acceptance',record2,incoming2,catalog,-signal.SIGKILL,'after-acceptance')
    assert read_document(record2/'state.json')['job'] is None
    intent2=read_document(record2/'dispatch-intent.json')
    observed=scheduler.find(intent2)
    assert len(observed)==1
    atomic_document(root/'accepted-without-recorded-id.json',{'job_ids':observed,'journal_job':None,'token':intent2['token']})
    recovered=attempt(root,'12-reconcile-accepted-job',record2,incoming2,catalog,0)
    second=json.loads(recovered.stdout.splitlines()[-1]);job2=second['job_id']
    summary['actual_jobs'].append(job2);save()
    assert second['reconciled'] and [job2]==observed and job2!=job1
    attempt(root,'13-repeat-reconciled-submission',record2,incoming2,catalog,0)
    assert sum(value['event']=='submit-call-start' for value in events(record2))==1
    verified2=wait_for_job(record2,job2,scheduler)
    parent3,record3,incoming3=new_case(root,'intent-without-positive-evidence',scheduler.namespace)
    attempt(root,'14-kill-after-intent-before-submit',record3,incoming3,catalog,-signal.SIGKILL,'after-intent')
    intent3=read_document(record3/'dispatch-intent.json')
    assert scheduler.find(intent3)==[]
    attempt(root,'15-uncertain-intent-requires-reconciliation',record3,incoming3,catalog,3)
    attempt(root,'16-repeating-uncertain-intent-still-stops',record3,incoming3,catalog,3)
    assert not any(value['event']=='submit-call-start' for value in events(record3))
    assert scheduler.find(intent3)==[]
    assert len(read_document(catalog)['records'])==22
    assert all('is_latest' not in event['params'] for event in read_document(catalog)['events'])
    outputs=[read_document(path/'loader/result.json') for path in [record,record2]]
    baseline=read_document(Path((PREVIOUS/'owned-projects-root.txt').read_text().strip())/'acceptance-4684100/baseline-A/result.json')
    for output in outputs:
        assert output['selection']==baseline['selection'][:512]
        assert output['raw_reads']==baseline['raw_reads'][:len(output['raw_reads'])]
        assert output['batches']==baseline['batches'][:8]
    summary.update(result='passed',finished_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
        three_action_crashes_recovered=True,B_became_available_but_A_stayed_pinned=True,
        acceptance_before_id_record_reconciled=True,actual_jobs_unique=True,
        uncertain_intent_refused_without_resubmission=True,changed_contract_and_artifacts_refused=True,
        catalog_records=22,duplicate_catalog_records=False,no_latest_lookup=True,
        verifications=[verified1,verified2],all_1024_samples_match_A_baseline=True,
        scope='Actual SLURM CPU jobs and pinned Katamari data path; owned schema-backed HTTP fixture; no model training')
    save();print(json.dumps(summary,indent=2))


if __name__=='__main__':main()
