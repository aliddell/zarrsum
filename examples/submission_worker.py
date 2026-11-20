"""Verify the submitted contract before running the bounded actual Katamari entry."""
import hashlib
import os
from pathlib import Path
import sys
import time

from zarrsum.submissions import encoded, immutable_bytes, read_document, sha, verify_inputs


def main():
    root=Path(sys.argv[1])
    contract=read_document(root/'contract.json')
    assert hashlib.sha256(encoded(contract)).hexdigest()==sys.argv[3]
    verify_inputs(contract)
    assert sha(root/'job.json')==sys.argv[2]
    spec=read_document(root/'job.json')
    assert spec['release_id']==contract['release_id']
    assert spec['submission_contract_sha256']==sys.argv[3]
    assert sha(spec['catalog_snapshot'])==spec['catalog_snapshot_sha256']
    from submission_workflow import callbacks
    # Reconcile source identity and policy before any model imports.
    callbacks(contract,root/'unused-catalog.json')['preparation'](root,{})
    if contract.get('acceptance_gate'):
        until=time.monotonic()+180
        while not (root/'allow-loader').exists():
            if time.monotonic()>until:raise TimeoutError('Acceptance driver did not release the loader')
            time.sleep(0.2)
    from metadata_release_katamari import run
    run(spec)
    result=read_document(root/'loader/result.json')
    assert result['release_id']==contract['release_id'] and result['worker_exitcodes']==[0,0,0,0]
    immutable_bytes(root/'worker-verification.json',encoded({
        'job_id':os.environ['SLURM_JOB_ID'],'contract_sha256':sys.argv[3],
        'release_id':contract['release_id'],'snapshot_sha256':spec['catalog_snapshot_sha256'],
        'payload_policy':contract['payload_policy'],'samples':result['samples'],
        'worker_exitcodes':result['worker_exitcodes'],'stopped_before_model':True}))


if __name__=='__main__':main()
