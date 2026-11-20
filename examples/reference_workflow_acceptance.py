"""Run the user CLI over owned catalog HTTP and keep a Katamari A job fixed during B."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

from zarrsum import revision_refs as refs

FIXTURE = Path('/mnt/main0/projects/aliddell-resume-shared-pool-mif200yr')
OLD_A = 'c391e72a666f51edc94498d62e8f393889a126477b357724b9f30b4a803ebc0e'
PROJECT = 'Reference workflow acceptance'


def now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def reef(path):
    return {'storage_platform': 'reef', 'location_uri': 'reef://' + Path(path).relative_to('/mnt/main0/projects').as_posix()}


def wait(path, process, timeout=180):
    until = time.monotonic() + timeout
    while not path.exists():
        if process.poll() is not None:
            raise RuntimeError(f'Child exited {process.returncode}; expected {path}')
        if time.monotonic() >= until:
            raise TimeoutError(str(path))
        time.sleep(0.1)


def cli(root, name, arguments, expected=0):
    started = time.time_ns()
    run = subprocess.run([sys.executable, '-B', '-s', '-m', 'zarrsum.reference_workflow', *arguments],
                         capture_output=True, text=True, timeout=120)
    (root / (name + '.stdout')).write_text(run.stdout)
    (root / (name + '.stderr')).write_text(run.stderr)
    refs.save(root / (name + '.invocation.json'), {'arguments': arguments, 'exit': run.returncode,
              'expected': expected, 'started_ns': started, 'finished_ns': time.time_ns()})
    if run.returncode != expected:
        raise RuntimeError(f'CLI {name}: exit {run.returncode}; inspect {root}')
    return json.loads(run.stdout)


def worker(root, name, snapshot, receipt, labels, *, batches=8, expected=0, **extra):
    spec = {'snapshot': str(snapshot), 'snapshot_id': receipt['snapshot_id'],
            'destination': str(root / ('checkout-' + name)), 'record_dir': str(root / ('loader-' + name)),
            'verification': 'reference-only', 'batches': batches, 'expected_labels': labels, **extra}
    path = root / (name + '.job.json')
    refs.save(path, spec)
    log = (root / (name + '.job.log')).open('x')
    environment = {key: value for key, value in os.environ.items() if key != 'CATALOG_API_TOKEN'}
    child = subprocess.Popen([sys.executable, '-B', '-s', str(Path(__file__).with_name('reference_job_worker.py')), str(path)],
                             env=environment, stdout=log, stderr=subprocess.STDOUT)
    return child, log, expected


def finished(task):
    process, log, expected = task
    try:
        status = process.wait(timeout=240)
    finally:
        log.close()
    if status != expected:
        raise RuntimeError(f'Worker exit {status}, expected {expected}; inspect {log.name}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    shared = Path(tempfile.mkdtemp(prefix='aliddell-reference-workflow-', dir='/mnt/main0/projects'))
    refs.save(root / 'shared-root.json', {'root': str(shared)})
    started = now()
    source = FIXTURE / 'reef/A'
    old = json.loads((FIXTURE / 'retained/manifests' / OLD_A).read_bytes())
    spec = {'dataset': 'hpa-reference-workflow', 'authority': 'owned-retained-pool:test',
            'revision': 'A', 'payload_revision': OLD_A, 'payloads': [], 'metadata': {}}
    for entry in old['entries']:
        text = None
        if entry['kind'] == 'metadata':
            try:
                text = (source / entry['path']).read_text()
            except UnicodeDecodeError:
                pass
        if text is not None:
            spec['metadata'][entry['path']] = text
        else:
            spec['payloads'].append({key: entry[key] for key in ('path', 'size', 'blake3')})
    a, registration = refs.register(spec, 'producer-digest')
    a_id = refs.document_id(a)
    refs.save(root / 'A.release.json', a)
    bmeta = dict(a['metadata'])
    changed = 0
    for name, text in list(bmeta.items()):
        if name.endswith('.ome.zarr/zarr.json'):
            value = json.loads(text)
            value['attributes']['dca']['channels'][0]['name'] = 'microtubules release B'
            value['attributes']['ome']['omero']['channels'][0]['label'] = 'microtubules release B'
            bmeta[name] = json.dumps(value, sort_keys=True)
            changed += 1
    assert changed == 11
    b, updated = refs.revise_metadata(a, a_id, 'B', bmeta)
    b_id = refs.document_id(b)
    refs.save(root / 'B.release.json', b)
    retained = shared / 'payloads'
    retained.mkdir()
    for entry in a['payloads']:
        target = retained / entry['path']
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(source / entry['path'], target)
    denied = shared / 'denied-copy'
    denied.mkdir()
    denied.chmod(0)
    templates = []
    from biocurator.catalog.canonical_id import canonical_id
    for name, text in a['metadata'].items():
        if name.endswith('.ome.zarr/zarr.json'):
            attrs = json.loads(text)['attributes']['dca']
            relative = name.removeprefix('data/').removesuffix('/zarr.json')
            templates.append({'canonical_id': canonical_id(relative), 'project': PROJECT,
                              'name': 'HPA reference workflow: ' + Path(relative).name,
                              'modality': 'imaging', 'dataset_type': 'processed',
                              'governance': {'access_scope': 'internal'},
                              'metadata': {'data_summary': {'channels': attrs['channels'], 'dca_version': attrs['version']}}})
    templates.sort(key=lambda item: item['canonical_id'])
    config = {'store': reef(shared / 'release-documents'), 'payload_locations': [reef(denied), reef(retained)],
              'retention_assertion': 'Owned NFS fixture; no provider hold; owner can mutate hard links',
              'catalog_records': templates}
    refs.save(root / 'A.publication.json', config)
    bconfig = json.loads(json.dumps(config))
    for template in bconfig['catalog_records']:
        template['metadata']['data_summary']['channels'][0]['name'] = 'microtubules release B'
    refs.save(root / 'B.publication.json', bconfig)
    endpoint = root / 'catalog-endpoint.json'
    catalog_log = (root / 'catalog-server.log').open('x')
    server = subprocess.Popen([sys.executable, '-B', '-s', str(Path(__file__).with_name('reference_catalog_server.py')),
                               '--state', str(root / 'catalog-state.json'), '--endpoint', str(endpoint)],
                              stdout=catalog_log, stderr=subprocess.STDOUT)
    active = None
    try:
        wait(endpoint, server)
        url = json.loads(endpoint.read_bytes())['base_url']
        os.environ['CATALOG_API_TOKEN'] = 'local-fixture-only'
        catalog_args = ['--catalog-url', url]
        publication_args = ['--local-platform', 'reef', '--verification', 'reference-only', *catalog_args]
        publish_a = cli(root, 'publish-A', ['publish', str(root / 'A.release.json'),
                                            '--publication', str(root / 'A.publication.json'), *publication_args])
        assert publish_a['payload_bytes_read'] == 0
        assert 'Permission denied' in publish_a['attempts'][0]['reason']
        def resolve(label, release_id):
            return cli(root, 'resolve-' + label, ['resolve', '--canonical-id', templates[0]['canonical_id'],
                       '--release-id', release_id, '--project', PROJECT, '--local-platform', 'reef',
                       '--output', str(root / (label + '.resolved.json')), *catalog_args])
        ra = resolve('A', a_id)
        cli_prepared = cli(root, 'cli-prepare-A', ['prepare', str(root / 'A.resolved.json'),
                              '--snapshot-id', ra['snapshot_id'], '--destination', str(root / 'checkout-cli-A'),
                              '--local-platform', 'reef', '--verification', 'reference-only'])
        assert cli_prepared['payload_bytes_read'] == 0
        labels_a = ['microtubules', 'target protein', 'nuclei', 'ER']
        labels_b = ['microtubules release B', 'target protein', 'nuclei', 'ER']
        finished(worker(root, 'baseline-A', root / 'A.resolved.json', ra, labels_a, batches=32))
        gate = root / 'B-loader-complete.json'
        began = root / 'active-A-started.json'
        active = worker(root, 'active-A', root / 'A.resolved.json', ra, labels_a, batches=32,
                        started=str(began), finish_gate=str(gate), slow=True)
        wait(began, active[0])
        publish_b = cli(root, 'publish-B', ['publish', str(root / 'B.release.json'),
                                            '--publication', str(root / 'B.publication.json'), *publication_args])
        ready_b = time.time_ns()
        rb = resolve('B', b_id)
        listing = cli(root, 'list-versions', ['list', '--canonical-id', templates[0]['canonical_id'],
                                               '--project', PROJECT, *catalog_args])
        assert {item['version'] for item in listing['versions']} == {a_id, b_id}
        finished(worker(root, 'new-B', root / 'B.resolved.json', rb, labels_b))
        refs.save(gate, {'time_ns': time.time_ns()})
        finished(active)
        active = None
        # A frozen resolution can prepare another job without catalog access.
        server.terminate()
        server.wait(timeout=10)
        cli_offline = cli(root, 'cli-offline-prepare-A', ['prepare', str(root / 'A.resolved.json'),
                             '--snapshot-id', ra['snapshot_id'], '--destination', str(root / 'checkout-cli-offline-A'),
                             '--local-platform', 'reef', '--verification', 'reference-only'])
        assert cli_offline['release_id'] == a_id and cli_offline['payload_bytes_read'] == 0
        finished(worker(root, 'offline-catalog-A', root / 'A.resolved.json', ra, labels_a))
        retained.rename(shared / 'payloads-offline')
        try:
            negative = worker(root, 'missing-source', root / 'A.resolved.json', ra, labels_a, expected=2)
            finished(negative)
            assert not (root / 'loader-missing-source').exists()
            assert not (root / 'checkout-missing-source').exists()
        finally:
            (shared / 'payloads-offline').rename(retained)
        baseline = json.loads((root / 'loader-baseline-A/result.json').read_bytes())
        consumed = []
        for name in ('active-A', 'new-B', 'offline-catalog-A'):
            observed = json.loads((root / ('loader-' + name) / 'result.json').read_bytes())
            count = observed['samples']
            assert observed['selection'] == baseline['selection'][:count]
            assert observed['raw_reads'] == baseline['raw_reads'][:len(observed['raw_reads'])]
            actual = [{key: value for key, value in batch.items() if key != 'channel_names'} for batch in observed['batches']]
            expected = [{key: value for key, value in batch.items() if key != 'channel_names'} for batch in baseline['batches'][:len(actual)]]
            assert actual == expected
            assert observed['worker_exitcodes'] == [0, 0, 0, 0]
            prepared = json.loads((root / (name + '.job.prepared.json')).read_bytes())
            assert prepared['payload_bytes_read'] == 0 and prepared['snapshot_id'] == (rb if name == 'new-B' else ra)['snapshot_id']
            consumed.append({'name': name, 'samples': count, 'release_id': observed['release_id'],
                             'labels': observed['channel_labels'], 'worker_exitcodes': observed['worker_exitcodes']})
        timestamps = [json.loads(line)['time_ns'] for path in (root / 'loader-active-A/audit').glob('times-*.json') for line in path.read_text().splitlines()]
        assert min(timestamps) < ready_b < max(timestamps)
        state = json.loads((root / 'catalog-state.json').read_bytes())
        assert len(state['records']) == 22
        assert not any('is_latest' in event['params'] for event in state['events'])
        refs.save(root / 'result.json', {'result': 'passed', 'job_id': os.getenv('SLURM_JOB_ID'),
                   'host': socket.gethostname(), 'started_utc': started, 'ended_utc': now(),
                   'A': a_id, 'B': b_id, 'registration': registration, 'metadata_update': updated,
                   'publish_A': publish_a, 'publish_B': publish_b, 'catalog_records': 22,
                   'shared_root': str(shared), 'active_A_reads_before_and_after_B': True,
                   'B_publication_time_ns': ready_b, 'active_A_first_fetch_ns': min(timestamps),
                   'active_A_last_fetch_ns': max(timestamps), 'consumed': consumed,
                   'baseline_samples': baseline['samples'], 'clean_worker_exits': 16,
                   'catalog_offline_replay': True, 'unavailable_sources_refused_before_loader': True,
                   'model_constructed': False, 'catalog_transport': 'owned loopback HTTP; actual client and models'})
    finally:
        denied.chmod(0o700)
        if active and active[0].poll() is None:
            active[0].terminate()
            active[0].wait(timeout=30)
            active[1].close()
        if server.poll() is None:
            server.terminate()
            server.wait(timeout=10)
        catalog_log.close()
        os.environ.pop('CATALOG_API_TOKEN', None)


if __name__ == '__main__':
    main()
