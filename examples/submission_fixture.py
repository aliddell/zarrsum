"""Build a new explicitly shared NFS fixture without reinterpreting older receipts."""
import json
import os
from pathlib import Path
import subprocess
import sys

from catalog_fixture import CatalogFixture
from zarrsum.catalog_releases import freeze_manifest,publish_ready
from zarrsum.metadata_releases import Repository,canonical,nfs_pool_identity
from zarrsum.submissions import atomic_document,read_document

TASK=Path('/mnt/main0/home/aliddell/scratch/restartable-submission-20261119')
PREVIOUS=Path('/mnt/main0/home/aliddell/scratch/catalog-integration-20261119')
BASE=Path((PREVIOUS/'owned-projects-root.txt').read_text().strip())/'acceptance-4684100'
RUNTIME=Path(__file__).resolve().parents[1]
DATASET='hpa-metadata-acceptance'


def validate(root,name,release_id,checkout,labels):
    spec={'release_id':release_id,'input_root':str(checkout),'record_dir':str(root/name),
          'batches':8,'expected_labels':labels}
    atomic_document(root/(name+'.spec.json'),spec)
    with (root/(name+'.log')).open('x') as log:
        subprocess.run([sys.executable,'-B','-s',str(RUNTIME/'examples/metadata_release_katamari.py'),
                        str(root/(name+'.spec.json'))],stdout=log,stderr=subprocess.STDOUT,check=True)
    value=read_document(root/name/'result.json')
    assert value['worker_exitcodes']==[0,0,0,0] and value['samples']==512
    return {'release_id':release_id,'input_root':str(checkout),'result':'passed','loader_result':str(root/name/'result.json')}


def main():
    root=Path((TASK/'new-fixture-root.txt').read_text().strip())
    retained=root/'retained';retained.mkdir()
    identity=nfs_pool_identity(retained)
    repo=Repository(retained,shared_filesystem=identity)
    source=PREVIOUS/'schema-source'
    paths=sorted(str(p.relative_to(source)) for p in source.rglob('*') if p.is_file())
    a,_=repo.publish(DATASET,'A',source,paths,[p for p in paths if '/c/' not in p])
    expected=read_document(BASE/'result.json')
    assert a==expected['A']
    repo.stage(a,'reef',root/'reef/A')
    labels_a=['microtubules','target protein','nuclei','ER']
    labels_b=['microtubules','target protein','nuclei','endoplasmic reticulum']
    repo.promote(a,'reef',validate(root,'validate-A',a,root/'reef/A',labels_a))
    replacements={}
    for entry in repo.manifest(a)['entries']:
        if Path(entry['path']).name!='zarr.json':continue
        value=json.loads(repo.object_path(entry['blake3']).read_bytes())
        channels=value.get('attributes',{}).get('ome',{}).get('omero',{}).get('channels')
        if channels:
            channels[-1]['label']=labels_b[-1]
            value['attributes']['dca']['channels'][-1]['name']=labels_b[-1]
            replacements[entry['path']]=canonical(value)
    b,_=repo.update(a,'B',replacements)
    assert b==expected['B']
    repo.stage(b,'reef',root/'reef/B')
    repo.promote(b,'reef',validate(root,'validate-B',b,root/'reef/B',labels_b))
    for role,baseline in [('validate-A','baseline-A'),('validate-B','new-job-B')]:
        current=read_document(root/role/'result.json')
        original=read_document(BASE/baseline/'result.json')
        for field in ['batches','selection','raw_reads']:
            assert current[field]==original[field][:len(current[field])],(role,field)
    remote=CatalogFixture()
    publish_ready(repo,DATASET,a,'reef',remote.client,root/'catalog-A-registration.json')
    freeze_manifest(repo,DATASET,a,'reef',remote.client,root/'catalog-A-snapshot.json')
    remote.client.close()
    result={'result':'passed','A':a,'B':b,'root':str(root),'storage_identity':identity,
            'job_id':os.environ['SLURM_JOB_ID'],'initial_full_ingest':True,
            'original_legacy_pool_unchanged':True,'two_actual_loaders_passed':True}
    atomic_document(root/'result.json',result)
    atomic_document(TASK/'retained-fixture.json',result)
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
