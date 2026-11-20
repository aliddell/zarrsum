"""Resume the owned catalog-to-Katamari fixture through a durable journal."""
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import tempfile

from catalog_fixture import CatalogFixture
from zarrsum.catalog_releases import PROJECT, freeze_manifest, publish_ready
from zarrsum.metadata_releases import Repository
from zarrsum.submissions import (
    ContractMismatch, Journal, ReconciliationRequired, SlurmScheduler,
    atomic_document, encoded, immutable_bytes, read_document, sha,
)

TASK=Path('/mnt/main0/home/aliddell/scratch/restartable-submission-20261119')
PREVIOUS=Path('/mnt/main0/home/aliddell/scratch/catalog-integration-20261119')
MODEL=Path('/mnt/main0/home/aliddell/scratch/katamari-release-job-20261119/jobkit/snapshot-v2')
SOURCE=Path(read_document(TASK/'retained-fixture.json')['root'])
A=json.loads((SOURCE/'result.json').read_bytes())['A']
B=json.loads((SOURCE/'result.json').read_bytes())['B']
DATASET='hpa-metadata-acceptance'


class DurableCatalog(CatalogFixture):
    def __init__(self,path):
        self.path=Path(path)
        super().__init__()
        if self.path.exists():
            value=read_document(self.path)
            self.records={record['id']:record for record in value['records']}
            self.events=value['events']

    def persist(self):
        atomic_document(self.path,{'records':list(self.records.values()),'events':self.events,
                                   'transport':'httpx.MockTransport; no deployed API'})

    def handle(self,request):
        response=super().handle(request)
        self.persist()
        return response


def contract_inputs(runtime):
    paths=list((runtime/'src/zarrsum').glob('*.py'))
    paths.extend(runtime/'examples'/name for name in [
        'submission_workflow.py','submission_worker.py','catalog_fixture.py','metadata_release_katamari.py'])
    paths.extend([MODEL/'training-config.json',MODEL/'source-identity.json',
                  MODEL/'projects/katamari/train.py',MODEL/'projects/katamari/katamari/data/release_inputs.py',
                  MODEL/'projects/katamari/katamari/data/build_dataloaders.py',
                  TASK/'retained-fixture.json',SOURCE/'result.json',SOURCE/'catalog-A-snapshot.json',
                  SOURCE/'retained/shared-filesystem.json',
                  SOURCE/'retained/manifests'/A,SOURCE/'reef/A/.release-receipt.json',
                  SOURCE/'reef/A/manifest.csv',SOURCE/'reef/A/centroids.parquet'])
    return [{'path':str(path.resolve()),'sha256':sha(path)} for path in sorted(set(paths))]


def make_contract(runtime,namespace):
    source_snapshot=read_document(SOURCE/'catalog-A-snapshot.json')
    refs=[{key:value[key] for key in ['canonical_id','version','project']}
          for value in source_snapshot['entries'].values()]
    return {'version':1,'release_id':A,'payload_policy':'trusted-retained-stat',
            'scheduler_namespace':namespace,'catalog_refs':refs,'catalog_project':PROJECT,
            'input_root':str(SOURCE/'reef/A'),'retained_repository':str(SOURCE/'retained'),
            'source_dataset':DATASET,'source_site':'reef','input_files':contract_inputs(runtime),
            'storage_identity':read_document(TASK/'retained-fixture.json')['storage_identity'],
            'recipe_sha256':sha(MODEL/'training-config.json'),
            'model_base':'7b68612daf7e03b1e98f6973768da25fda476373',
            'model_policy':'Prior owned frozen CPU data-path package; no training checkpoint resume'}


def callbacks(contract,catalog_path):
    def check_source():
        if contract['payload_policy']!='trusted-retained-stat':
            raise ContractMismatch('The fixture does not implement another payload policy')
        if contract.get('request_config'):
            requested=read_document(contract['request_config'])
            if requested!={'release_id':contract['release_id'],'payload_policy':contract['payload_policy'],
                           'recipe_sha256':contract['recipe_sha256']}:
                raise ContractMismatch('Requested recipe or data policy differs')
        repo=Repository(contract['retained_repository'],shared_filesystem=contract['storage_identity'])
        bound=repo.bind(contract['source_dataset'],contract['release_id'],contract['source_site'])
        if bound['input_root']!=contract['input_root']:
            raise ContractMismatch('Prepared source location changed')
        if bound['preparation']['payload_verification']!='read-only-object-stat':
            raise ContractMismatch('Prepared source policy changed')
        return repo,bound

    def preparation(root,previous):
        repo,bound=check_source()
        output=root/'prepared.json'
        value={'release_id':contract['release_id'],'input_root':bound['input_root'],
               'payload_policy':contract['payload_policy'],
               'source_receipt_sha256':sha(Path(bound['input_root'])/'.release-receipt.json'),
               'storage_identity':contract['storage_identity']}
        immutable_bytes(output,encoded(value))
        return {'release_id':contract['release_id'],'artifacts':[str(output)]}

    def registration(root,previous):
        repo,bound=check_source()
        client=DurableCatalog(catalog_path)
        descriptor,temporary=tempfile.mkstemp(prefix='.catalog-registration-',dir=root)
        os.close(descriptor);Path(temporary).unlink()
        try:
            publish_ready(repo,contract['source_dataset'],contract['release_id'],
                          contract['source_site'],client.client,temporary)
            output=root/'registered.json'
            immutable_bytes(output,Path(temporary).read_bytes())
            return {'release_id':contract['release_id'],'artifacts':[str(output)]}
        finally:
            client.client.close();Path(temporary).unlink(missing_ok=True)

    def snapshot(root,previous):
        repo,bound=check_source()
        client=DurableCatalog(catalog_path)
        descriptor,temporary=tempfile.mkstemp(prefix='.catalog-snapshot-',dir=root)
        os.close(descriptor);Path(temporary).unlink()
        try:
            freeze_manifest(repo,contract['source_dataset'],contract['release_id'],
                            contract['source_site'],client.client,temporary)
            output=root/'catalog-snapshot.json'
            immutable_bytes(output,Path(temporary).read_bytes())
            frozen=read_document(output)
            if any(entry['version']!=contract['release_id'] for entry in frozen['entries'].values()):
                raise ContractMismatch('Catalog selected another version')
            spec={'release_id':contract['release_id'],'input_root':bound['input_root'],
                  'record_dir':str(root/'loader'),'batches':8,
                  'expected_labels':['microtubules','target protein','nuclei','ER'],
                  'catalog_snapshot':str(output),'catalog_snapshot_sha256':sha(output),
                  'submission_contract_sha256':hashlib.sha256(encoded(contract)).hexdigest()}
            immutable_bytes(root/'job.json',encoded(spec))
            return {'release_id':contract['release_id'],'artifacts':[str(output),str(root/'job.json')],
                    'snapshot_sha256':sha(output),'job_spec_sha256':sha(root/'job.json')}
        finally:
            client.client.close();Path(temporary).unlink(missing_ok=True)
    return {'preparation':preparation,'registration':registration,'snapshot':snapshot,'_verify':check_source}


def renderer(runtime,contract):
    def render(root,stages):
        quote=lambda path:shlex.quote(str(path))
        expected=stages['snapshot']['result']['job_spec_sha256']
        digest=hashlib.sha256(encoded(contract)).hexdigest()
        return f'''#!/bin/bash
#SBATCH --partition=cpu
#SBATCH --qos=low
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=48G
#SBATCH --time=00:10:00
#SBATCH --output={root}/slurm-%j.log
set -euo pipefail
unset PYTHONHOME LD_PRELOAD LD_LIBRARY_PATH EVORUN_WORKDIR EVORUN_SNAPSHOT
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1
export PYTHONPATH={quote(str(runtime/'src')+':'+str(runtime/'examples')+':'+str(PREVIOUS/'vendor')+':'+str(MODEL/'harness')+':'+str(MODEL))}
export TMPDIR="/tmp/restartable-submission-$SLURM_JOB_ID"
mkdir -p "$TMPDIR"
trap 'rm -rf -- "$TMPDIR"' EXIT
srun --ntasks=1 {quote(PREVIOUS/'environment/bin/python')} -B -s {quote(runtime/'examples/submission_worker.py')} {quote(root)} {expected} {digest}
'''.encode()
    return render


def kill_hook(name):
    def hook(event,value):
        if name==event or name==event+':'+str(value):
            os.kill(os.getpid(),signal.SIGKILL)
    return hook


def main():
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('record',type=Path)
    parser.add_argument('--contract',type=Path,required=True)
    parser.add_argument('--catalog',type=Path,required=True)
    parser.add_argument('--kill-at')
    args=parser.parse_args()
    contract=read_document(args.contract)
    runtime=Path(__file__).resolve().parents[1]
    try:
        actions=callbacks(contract,args.catalog)
        result=Journal(args.record).resume(contract,actions,
                renderer(runtime,contract),SlurmScheduler(),kill_hook(args.kill_at),verifier=actions['_verify'])
    except ContractMismatch as error:
        print(json.dumps({'state':'refused','reason':str(error)}),flush=True)
        raise SystemExit(2)
    except ReconciliationRequired as error:
        print(json.dumps({'state':'reconciliation-required','reason':str(error)}),flush=True)
        raise SystemExit(3)
    print(json.dumps(result),flush=True)


if __name__=='__main__':main()
