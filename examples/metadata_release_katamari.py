"""Run the existing Katamari entry against a staged metadata release, then stop."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time

MODEL = Path('/mnt/main0/home/aliddell/scratch/katamari-release-job-20261119/jobkit/snapshot-v2')


def configure():
    import bootstrap_env
    bootstrap_env.apply(MODEL)
    os.environ.update(KATAMARI_MANIFESTS_REPO=str(MODEL/'manifest-package/repository.git'),
                      KATAMARI_MANIFEST_WORKTREES=str(MODEL/'manifest-package/worktrees'),
                      KATAMARI_MANIFESTS_READ_ONLY='1', DATA_PATH_MAPPINGS='[]',
                      PYTHONDONTWRITEBYTECODE='1', PYTHONNOUSERSITE='1',
                      AWS_CONFIG_FILE='/dev/null', AWS_SHARED_CREDENTIALS_FILE='/dev/null')


configure()
from check_support import AuditedReader, summarize


class TimedReader(AuditedReader):
    def fetch_reads(self, reads):
        value = super().fetch_reads(reads)
        with (Path(os.environ['DV_AUDIT_DIR'])/('times-'+str(os.getpid())+'.json')).open('a') as stream:
            stream.write(json.dumps({'time_ns':time.time_ns(),'fetch_index':self.fetch_index-1,'pid':os.getpid()})+'\n')
        return value


class DataComplete(BaseException):
    pass


def run(spec):
    from omegaconf import OmegaConf
    import torch
    from projects.katamari.katamari.config.schema import ReleaseInputsConfig
    from projects.katamari.katamari.data.release_inputs import _bind_chammi
    import projects.katamari.katamari.data.build_dataloaders as builders
    import projects.katamari.train as train
    from zarrsum.metadata_releases import write_json

    record = Path(spec['record_dir'])
    record.mkdir(parents=True, exist_ok=False)
    checkout = Path(spec['input_root']).resolve()
    audit = record/'audit'
    audit.mkdir()
    local_tmp = Path(os.environ['TMPDIR'])/record.name
    local_tmp.mkdir()
    os.environ.update(DV_SOURCE_ROOT=str(checkout), DV_AUDIT_DIR=str(audit),
                      DV_DATA_AUDIT_ACTIVE='1', METADATA_CACHE_ROOT=str(record/'metadata'),
                      EVORUN_WORKDIR=str(local_tmp), PROJECT_ROOT=str(MODEL))
    torch.set_num_threads(1)
    cfg = OmegaConf.create(json.loads((MODEL/'training-config.json').read_text()))
    cfg.initialize_cloud = False
    cfg.release_reference = None
    cfg.paths.output_dir = str(record)
    cfg.release_inputs = OmegaConf.merge(OmegaConf.structured(ReleaseInputsConfig), {
        'dataset_key':'chammi75', 'source_prefix':'/mnt/main0/projects/katamari/data_prod/'})
    bindings = _bind_chammi(cfg, checkout, record/'derived')
    if spec.get('catalog_snapshot'):
        from zarrsum.catalog_releases import bind_training_csv
        generated=record/'derived/catalog-resolved.csv'
        catalog=bind_training_csv(spec['catalog_snapshot'],spec['catalog_snapshot_sha256'],
                                 spec['release_id'],checkout,checkout/'manifest.csv',generated)
        cfg.data.train_dataloaders.datasets.chammi75.dataset.configs=[str(generated)]
        write_json(record/'catalog-binding.json',{
            'release_id':catalog['release_id'], 'source_snapshot':spec['catalog_snapshot'],
            'snapshot_sha256':spec['catalog_snapshot_sha256'], 'catalog_entries':len(catalog['entries']),
            'actual_training_csv':str(generated), 'is_latest_used':False})
    cfg.data.train_dataloaders.reader._target_ = 'metadata_release_katamari.TimedReader'
    cfg.data.train_dataloaders.sampler.num_batches_per_epoch = spec['batches']
    cfg.data.release_id = spec['release_id']
    cfg.data.release_receipt = str(checkout/'.release-receipt.json')
    cfg.data.release_receipt_sha256 = hashlib.sha256((checkout/'.release-receipt.json').read_bytes()).hexdigest()
    write_json(record/'training-config.json', OmegaConf.to_container(cfg, resolve=True))
    write_json(record/'binding.json', {**spec, 'metadata_checksums':bindings,
               'receipt_sha256':cfg.data.release_receipt_sha256,
               'verification':'fresh-metadata-and-trusted-retained-payloads'})
    built = builders.build_dataloaders
    original_check = train.check_manifests
    checks = []

    def checked(config):
        original_check(config)
        checks.append(time.time_ns())

    train.check_manifests = checked

    def consume(data_cfg):
        assert checks, 'The actual manifest check did not run'
        loader, validation = built(data_cfg)
        assert validation is None and loader.num_workers == 4
        iterator = iter(loader)
        batches=[]
        selection=[]
        error=None
        try:
            for ordinal, batch in enumerate(iterator):
                batches.append(summarize(batch))
                selection.extend(batch['manifest_row'].tolist())
                write_json(record/('batch-'+str(ordinal)+'.json'), {'time_ns':time.time_ns()})
                if ordinal==0 and spec.get('started'):
                    write_json(Path(spec['started']), {'time_ns':time.time_ns(),'release_id':spec['release_id']})
                if spec.get('slow'):
                    time.sleep(0.5)
                if ordinal==spec['batches']-1 and spec.get('finish_gate'):
                    until=time.monotonic()+180
                    while not Path(spec['finish_gate']).exists():
                        if time.monotonic()>until:raise TimeoutError('B did not finish')
                        time.sleep(0.1)
            assert len(batches)==spec['batches']
        except BaseException as caught:
            error=caught
        finally:
            if hasattr(iterator,'_shutdown_workers'):iterator._shutdown_workers()
            workers=[{'pid':w.pid,'exitcode':w.exitcode,'alive':w.is_alive()}
                     for w in getattr(iterator,'_workers',[])]
            write_json(record/'workers.json',{'workers':workers,'read_error':None if error is None else repr(error)})
        if error:raise error
        assert len(workers)==4 and all(w['exitcode']==0 and not w['alive'] for w in workers)
        events=[json.loads(line) for p in audit.glob('*.jsonl') for line in p.read_text().splitlines()]
        raw=[]
        for ordinal in range(len(batches)):
            found=[event for event in events if event['worker']==ordinal%4 and event['fetch_index']==ordinal//4]
            assert len(found)==1
            raw.extend(found[0]['reads'])
        labels={tuple(row) for batch in batches for row in batch['channel_names']}
        assert labels=={tuple(spec['expected_labels'])}, labels
        result={'result':'passed','release_id':spec['release_id'],'input_root':str(checkout),
                'batches':batches,'raw_reads':raw,'selection':selection,
                'samples':len(selection),'worker_exitcodes':[w['exitcode'] for w in workers],
                'channel_labels':[list(row) for row in sorted(labels)],
                'metadata_check_time_ns':checks[0], 'finished_time_ns':time.time_ns(),
                'entry_source':train.__file__, 'builder_source':builders.__file__,
                'stop_before_model':True,'config_blake3':None}
        write_json(record/'result.json',result)
        raise DataComplete()

    builders.build_dataloaders=consume
    try:
        train.main_from_py(cfg)
    except DataComplete:
        return
    raise AssertionError('The actual training entry did not reach the bounded loader')


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('spec',type=Path)
    args=parser.parse_args()
    run(json.loads(args.spec.read_bytes()))
