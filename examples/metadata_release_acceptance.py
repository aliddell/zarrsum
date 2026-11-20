"""Keep an actual Katamari A reader active while metadata release B becomes ready."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time

from zarrsum.metadata_releases import Repository, canonical, write_json

TASK = Path('/mnt/main0/home/aliddell/scratch/catalog-integration-20261119')
CATALOG_SNAPSHOTS = {}
MODEL = Path('/mnt/main0/home/aliddell/scratch/katamari-release-job-20261119/jobkit/snapshot-v2')
DATASET = 'hpa-metadata-acceptance'


def wait_for(path, child, timeout=180):
    until=time.monotonic()+timeout
    while not path.exists():
        if child.poll() is not None:raise RuntimeError(f'Child exited {child.returncode}; missing {path}')
        if time.monotonic()>until:raise TimeoutError(str(path))
        time.sleep(0.1)


def successful(child, log, timeout=180):
    try:
        status=child.wait(timeout=timeout)
    finally:
        log.close()
    if status:raise RuntimeError(f'Loader exited {status}; inspect {log.name}')


def start_loader(root, name, release_id, checkout, labels, batches=8, **extra):
    extra = {**CATALOG_SNAPSHOTS.get(release_id, {}), **extra}
    spec={'release_id':release_id,'input_root':str(checkout),'record_dir':str(root/name),
          'expected_labels':labels,'batches':batches, **extra}
    path=root/(name+'.spec.json')
    write_json(path,spec)
    log=(root/(name+'.log')).open('x')
    child=subprocess.Popen([sys.executable,str(Path(__file__).with_name('metadata_release_katamari.py')),str(path)],stdout=log,stderr=subprocess.STDOUT)
    return child,log


def validation(root, name, release_id, checkout):
    result=root/name/'result.json'
    value=json.loads(result.read_bytes())
    assert value['result']=='passed' and value['release_id']==release_id
    return {'result':'passed','release_id':release_id,'input_root':str(checkout),
            'result_file':str(result),'result_sha256':hashlib.sha256(result.read_bytes()).hexdigest(),
            'worker_exitcodes':value['worker_exitcodes'],'samples':value['samples']}


def refuse_binding(repo, release_id):
    try:repo.bind(DATASET,release_id,'reef')
    except ValueError:return
    raise AssertionError('An unready release was selectable')


def main():
    root=Path(os.environ['DV_CATALOG_PROJECT_ROOT'])/('acceptance-'+os.environ['SLURM_JOB_ID'])
    root.mkdir(parents=True,exist_ok=False)
    stage=root/'reef'
    stage.mkdir()
    repo=Repository(root/'retained')
    seed=TASK/'schema-source'
    source=root/'publisher-source'
    shutil.copytree(seed,source)
    paths=sorted(str(p.relative_to(source)) for p in source.rglob('*') if p.is_file())
    meta=[path for path in paths if '/c/' not in path]
    assert len(paths)<=10_000
    layouts=[]
    for path in paths:
        if Path(path).name=='zarr.json':
            value=json.loads((source/path).read_bytes())
            if value['node_type']=='array':layouts.append({'path':path,'shape':value['shape'],'codecs':value['codecs']})
    assert layouts, 'The input must contain real Zarr v3 arrays'
    store_count=len(layouts)
    a, initial=repo.publish(DATASET,'A',source,paths,meta)
    shutil.rmtree(source)
    repo.stage(a,'reef',stage/'A')
    labels_a=['microtubules','target protein','nuclei','ER']
    labels_b=['microtubules','target protein','nuclei','endoplasmic reticulum']
    process,log=start_loader(root,'baseline-A',a,stage/'A',labels_a,batches=64)
    successful(process,log)
    repo.promote(a,'reef',validation(root,'baseline-A',a,stage/'A'))
    repo.recommend(DATASET,a,'reef')
    from catalog_fixture import CatalogFixture
    from zarrsum.catalog_releases import publish_ready, freeze_manifest
    catalog=CatalogFixture()
    publish_ready(repo,DATASET,a,'reef',catalog.client,root/'catalog-A-registration.json')
    freeze_manifest(repo,DATASET,a,'reef',catalog.client,root/'catalog-A-snapshot.json')
    CATALOG_SNAPSHOTS[a]={'catalog_snapshot':str(root/'catalog-A-snapshot.json'),
                          'catalog_snapshot_sha256':hashlib.sha256((root/'catalog-A-snapshot.json').read_bytes()).hexdigest()}
    a_binding=repo.bind(DATASET,a,'reef')
    write_json(root/'bound-A-before-update.json',a_binding)
    started=root/'active-A-started.json'
    gate=root/'B-is-ready.json'
    active,active_log=start_loader(root,'active-A',a,stage/'A',labels_a,batches=64,
                                  started=str(started),finish_gate=str(gate),slow=True)
    result={'job_id':os.environ['SLURM_JOB_ID'],'host':socket.gethostname(),
            'started_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
            'result':'running','format':'metadata-release/experimental-1','A':a,
            'files':len(paths),'metadata_files':len(meta),'v3_arrays':len(layouts),
            'initial_publication':initial,'scope':'Local catalog fixture and real Katamari CPU data path; cooperative immutable payloads'}
    write_json(root/'started.json',result)
    try:
        wait_for(started,active)
        update_started=time.time_ns()
        replacements={}
        document=repo.manifest(a)
        payload_objects={str(repo.object_path(e['blake3'])) for e in document['entries'] if e['kind']=='payload'}
        # This scope rejects payload opens in the publisher and staging process.
        audit_active=[True]
        payload_opens=[]
        def audit(event,args):
            if audit_active[0] and event=='open' and str(args[0]) in payload_objects:
                payload_opens.append(str(args[0]))
                raise AssertionError('Metadata publication or staging opened a payload')
        sys.addaudithook(audit)
        for entry in document['entries']:
            if Path(entry['path']).name!='zarr.json':continue
            value=json.loads(repo.object_path(entry['blake3']).read_bytes())
            channels=value.get('attributes',{}).get('ome',{}).get('omero',{}).get('channels')
            if channels:
                assert [c['label'] for c in channels]==labels_a
                channels[-1]['label']=labels_b[-1]
                value['attributes']['dca']['channels'][-1]['name']=labels_b[-1]
                replacements[entry['path']]=canonical(value)
        assert replacements
        b,cost=repo.update(a,'B',replacements)
        result.update(B=b,metadata_changes=len(replacements),update_cost=cost)
        refuse_binding(repo,b)
        before_calls=len(catalog.events)
        try:publish_ready(repo,DATASET,b,'reef',catalog.client,root/'unready-B.json')
        except ValueError:pass
        else:raise AssertionError('Unready B was published to the catalog')
        assert len(catalog.events)==before_calls
        assert len(catalog.records)==store_count
        assert repo.recommendation(DATASET)==a
        killed_marker=root/'interrupted-member.json'
        killer_code="""
import sys,time,json
from pathlib import Path
from zarrsum.metadata_releases import Repository
blocked=set(json.loads(Path(sys.argv[5]).read_text()))
def audit(event,args):
    if event=='open' and str(args[0]) in blocked:raise AssertionError('Payload opened by stage')
sys.addaudithook(audit)
def pause(path):
    Path(sys.argv[4]).write_text(path)
    time.sleep(180)
Repository(sys.argv[1]).stage(sys.argv[2],'reef',sys.argv[3],after_member=pause)
"""
        write_json(root/'payload-object-paths.json',sorted(payload_objects))
        with (root/'interrupted-stage.log').open('x') as killed_log:
            killed=subprocess.Popen([sys.executable,'-c',killer_code,str(repo.root),b,str(stage/'B'),str(killed_marker),str(root/'payload-object-paths.json')],stdout=killed_log,stderr=subprocess.STDOUT)
            try:
                wait_for(killed_marker,killed,30)
                killed.kill()
                assert killed.wait(timeout=10)==-signal.SIGKILL
            finally:
                if killed.poll() is None:killed.kill();killed.wait()
        assert not (stage/'B').exists()
        refuse_binding(repo,b)
        assert repo.recommendation(DATASET)==a
        orphan_dirs=list(stage.glob('.staging-*'))
        assert len(orphan_dirs)==1
        b_receipt=repo.stage(b,'reef',stage/'B')
        refuse_binding(repo,b)
        assert repo.recommendation(DATASET)==a
        audit_active[0]=False
        process,log=start_loader(root,'validate-B',b,stage/'B',labels_b)
        successful(process,log)
        repo.promote(b,'reef',validation(root,'validate-B',b,stage/'B'))
        repo.recommend(DATASET,b,'reef')
        publish_ready(repo,DATASET,b,'reef',catalog.client,root/'catalog-B-registration.json')
        freeze_manifest(repo,DATASET,b,'reef',catalog.client,root/'catalog-B-snapshot.json')
        CATALOG_SNAPSHOTS[b]={'catalog_snapshot':str(root/'catalog-B-snapshot.json'),
                              'catalog_snapshot_sha256':hashlib.sha256((root/'catalog-B-snapshot.json').read_bytes()).hexdigest()}
        assert len(catalog.records)==store_count*2
        for name,expected_labels in [('A',labels_a),('B',labels_b)]:
            snapshot=json.loads((root/('catalog-'+name+'-snapshot.json')).read_bytes())
            assert all(entry['catalog_channels']==expected_labels for entry in snapshot['entries'].values())
        catalog.save(root/'scientific-catalog-export.json')
        b_binding=repo.bind(DATASET,b,'reef')
        ready_time=time.time_ns()
        write_json(gate,{'release_id':b,'time_ns':ready_time})
        successful(active,active_log)
        assert a_binding['input_root']==str(stage/'A')
        process,log=start_loader(root,'new-job-B',b,Path(b_binding['input_root']),labels_b)
        successful(process,log)
        # Fresh A preparation uses retained objects after the recommendation moved to B.
        repo.stage(a,'reef-recovery',stage/'recovered-A')
        process,log=start_loader(root,'recovered-A',a,stage/'recovered-A',labels_a,batches=64)
        successful(process,log)
        repo.promote(a,'reef-recovery',validation(root,'recovered-A',a,stage/'recovered-A'))
        recovered=repo.bind(DATASET,a,'reef-recovery')
        outputs={name:json.loads((root/name/'result.json').read_bytes()) for name in ['baseline-A','active-A','validate-B','new-job-B','recovered-A']}
        for name in ['active-A','recovered-A']:
            for field in ['batches','raw_reads','selection']:
                assert outputs[name][field]==outputs['baseline-A'][field],(name,field)
        for field in ['batches','raw_reads','selection']:
            assert outputs['new-job-B'][field]==outputs['validate-B'][field],field
        for old,new in zip(outputs['baseline-A']['batches'][:8],outputs['new-job-B']['batches']):
            assert old['image']==new['image']
            assert old['image_global_crops']==new['image_global_crops']
            assert old['channel_names']!=new['channel_names']
            for key in old:
                if key!='channel_names':assert old[key]==new[key],key
        times=[json.loads(line)['time_ns'] for path in (root/'active-A/audit').glob('times-*.json') for line in path.read_text().splitlines()]
        concurrent=[t for t in times if update_started<t<ready_time]
        assert concurrent, 'No actual image read occurred while B was prepared and validated'
        payload_entries=[e for e in document['entries'] if e['kind']=='payload']
        assert all((stage/'A'/e['path']).stat().st_ino==(stage/'B'/e['path']).stat().st_ino for e in payload_entries)
        assert all((stage/'A'/path).stat().st_ino!=(stage/'B'/path).stat().st_ino for path in replacements)
        assert repo.recommendation(DATASET)==b
        result.update(result='passed',catalog_records=store_count*2,catalog_stores_per_release=store_count,catalog_identity=['canonical_id','version','project'],
                      catalog_unready_B_caused_no_HTTP_calls=True, actual_catalog_client='0.7.0, 945c168',
                      catalog_transport='httpx.MockTransport; actual request/response models',
                      actual_training_csv_from_catalog_snapshot=True,B_ready_time_ns=ready_time,update_started_time_ns=update_started,
                      concurrent_A_read_batches=len(concurrent),payload_opens_during_B_publication_and_staging=payload_opens,
                      stage_receipt=b_receipt,interrupted_stage_exit=-signal.SIGKILL,
                      interrupted_final_absent=True,unready_binding_refused=True,
                      unready_recommendation_unchanged=True,shared_payload_files=len(payload_entries),
                      A_binding_unchanged=True,A_recovered_after_B=True,
                      old_A_matches_baseline=True,new_B_has_updated_labels=True,B_image_crops_unchanged=True,
                      A_recovery_binding=recovered,producer_source_removed=not source.exists(),
                      loader_checks={name:{'samples':v['samples'],'batches':len(v['batches']),
                                          'worker_exitcodes':v['worker_exitcodes'],'labels':v['channel_labels']} for name,v in outputs.items()},
                      finished_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
        for path in orphan_dirs:shutil.rmtree(path)
        result['owned_interrupted_scratch_removed']=all(not p.exists() for p in orphan_dirs)
        write_json(root/'result.json',result)
        print(json.dumps({k:v for k,v in result.items() if k not in ['A_recovery_binding']},indent=2))
    finally:
        if active.poll() is None:
            if not gate.exists():write_json(gate,{'result':'cleanup-after-failure'})
            try:active.wait(timeout=30)
            except subprocess.TimeoutExpired:active.kill();active.wait()
        active_log.close()


if __name__=='__main__':main()
