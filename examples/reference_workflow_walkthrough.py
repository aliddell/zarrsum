"""Run publish, list, resolve, and prepare with disposable data and an owned HTTP catalog."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from blake3 import blake3
from zarrsum import revision_refs as refs
from reference_workflow_acceptance import cli, reef, wait


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--mode', choices=('producer-digest', 'producer-revision', 'size-mtime'), default='producer-revision')
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    shared = Path(tempfile.mkdtemp(prefix='aliddell-reference-demo-', dir='/mnt/main0/projects'))
    retained = shared / 'retained-pixels'
    retained.mkdir()
    data = b'demo' * 1024
    (retained / 'pixels.bin').write_bytes(data)
    spec = {'dataset': 'reference-demo', 'authority': 'owned-producer:demo',
            'revision': 'metadata-A', 'payload_revision': 'pixels-001',
            'payloads': [{'path': 'pixels.bin', 'size': len(data)}],
            'metadata': {'metadata.json': '{"channel":"ER"}'}}
    if args.mode == 'producer-digest':
        spec['payloads'][0]['blake3'] = blake3(data).hexdigest()
    a, receipt = refs.register(spec, args.mode, retained)
    b, update = refs.revise_metadata(a, refs.document_id(a), 'metadata-B', {'metadata.json': '{"channel":"ER revised"}'})
    for label, value in [('A', a), ('B', b)]:
        refs.save(root / (label + '.release.json'), value)
    config = {'store': reef(shared / 'release-documents'), 'payload_locations': [reef(retained)],
              'retention_assertion': 'Disposable owned demo; no provider retention hold',
              'catalog_records': [{'canonical_id': 'reference-demo/store', 'project': 'Reference demo',
                 'name': 'Reference workflow demo', 'modality': 'imaging', 'governance': {'access_scope': 'internal'}, 'metadata': {}}]}
    refs.save(root / 'publication.json', config)
    endpoint = root / 'catalog-endpoint.json'
    with (root / 'catalog-server.log').open('x') as log:
        server = subprocess.Popen([sys.executable, '-B', '-s', str(Path(__file__).with_name('reference_catalog_server.py')),
                                   '--state', str(root / 'catalog-state.json'), '--endpoint', str(endpoint)],
                                  stdout=log, stderr=subprocess.STDOUT)
        saved_token = os.environ.get('CATALOG_API_TOKEN')
        try:
            wait(endpoint, server)
            url = json.loads(endpoint.read_bytes())['base_url']
            os.environ['CATALOG_API_TOKEN'] = 'local-fixture-only'
            policy = 'size-mtime' if args.mode == 'size-mtime' else 'reference-only'
            resolutions = {}
            for label, value in [('A', a), ('B', b)]:
                rid = refs.document_id(value)
                published = cli(root, 'publish-' + label, ['publish', str(root / (label + '.release.json')),
                     '--publication', str(root / 'publication.json'), '--catalog-url', url,
                     '--local-platform', 'reef', '--verification', policy])
                resolved = cli(root, 'resolve-' + label, ['resolve', '--canonical-id', 'reference-demo/store',
                     '--project', 'Reference demo', '--release-id', rid, '--catalog-url', url,
                     '--local-platform', 'reef', '--output', str(root / (label + '.resolved.json'))])
                prepared = cli(root, 'prepare-' + label, ['prepare', str(root / (label + '.resolved.json')),
                     '--snapshot-id', resolved['snapshot_id'], '--destination', str(shared / ('job-inputs-' + label)),
                     '--local-platform', 'reef', '--verification', policy])
                assert published['payload_bytes_read'] == prepared['payload_bytes_read'] == 0
                resolutions[label] = resolved
            listed = cli(root, 'list', ['list', '--canonical-id', 'reference-demo/store', '--project', 'Reference demo', '--catalog-url', url])
            assert len(listed['versions']) == 2
            assert (shared / 'job-inputs-A/metadata.json').read_text() == '{"channel":"ER"}'
            assert (shared / 'job-inputs-B/metadata.json').read_text() == '{"channel":"ER revised"}'
            result = {'result': 'passed', 'mode': args.mode, 'root': str(root), 'shared_root': str(shared),
                      'registration': receipt, 'metadata_update': update, 'resolutions': resolutions,
                      'catalog_transport': 'owned loopback HTTP; no deployed API',
                      'catalog_server_stopped_after_demo': True}
            refs.save(root / 'result.json', result)
            print(json.dumps(result, indent=2))
        finally:
            server.terminate()
            server.wait(timeout=10)
            if saved_token is None:
                os.environ.pop('CATALOG_API_TOKEN', None)
            else:
                os.environ['CATALOG_API_TOKEN'] = saved_token


if __name__ == '__main__':
    main()
