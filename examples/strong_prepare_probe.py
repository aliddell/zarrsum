"""Check merged strong preparation with the actual S3 fixture and 1024-key option."""
import json
import os
from pathlib import Path
import shutil
import tempfile

from s3_fixture import HpaS3Fixture, RELEASE_ID
from zarrsum.locations import prepare


def run(task):
    previous=Path('/mnt/main0/home/aliddell/scratch/katamari-release-job-20261119')
    evidence=task/('strong-prepare-'+os.environ['SLURM_JOB_ID'])
    scratch=Path(tempfile.mkdtemp(prefix='combined-prepare-',dir=os.environ['TMPDIR']))
    try:
        with HpaS3Fixture(scratch/'private',evidence/'fixture',previous/'fixture/.venv/bin/zarrsum-release',
                          previous/'jobkit/snapshot-v2/bin/minio',previous/'jobkit/snapshot-v2/bin/rclone',
                          previous/'jobkit/fixture-assets/hpa-seed-store','valid',previous/'fixture/.venv/bin/python') as fixture:
            saved={key:os.environ.get(key) for key in fixture.environment}
            os.environ.update(fixture.environment)
            try:
                result=prepare(json.loads(fixture.reference.read_bytes()),scratch/'cache',scratch/'checkout',
                               evidence/'preparation',local_platform='reef',remotes=fixture.remotes,
                               location_remotes=fixture.location_remotes,binary=str(previous/'jobkit/snapshot-v2/bin/rclone'),
                               timeout=60,transfer_batch_size=1024)
                assert result['release_id']==RELEASE_ID and result['result']=='verified'
                assert not result['cache_hit'] and result['checkout']['files']==294
                assert result['fetch']['transfer_batch_size']==1024
                (evidence/'result.json').write_text(json.dumps({'result':'passed','release_id':RELEASE_ID,
                    'files':294,'batch_size':1024,'strong_verification_unchanged':True,
                    'selected_location':result['selected_location']},indent=2)+'\n')
            finally:
                for key,value in saved.items():
                    if value is None:os.environ.pop(key,None)
                    else:os.environ[key]=value
    finally:
        shutil.rmtree(scratch)


if __name__=='__main__':run(Path('/mnt/main0/home/aliddell/scratch/catalog-integration-20261119'))
