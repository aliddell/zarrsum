"""Bind explicit revision evidence to the existing owned Katamari fixture.

Hard links require cooperative retained storage. This harness does not enforce
immutability and does not make a new production preparation backend.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from zarrsum import revision_refs as refs

FIXTURE = Path('/mnt/main0/projects/aliddell-resume-shared-pool-mif200yr')
MODEL = Path('/mnt/main0/home/aliddell/scratch/katamari-release-job-20261119/jobkit/snapshot-v2')
OLD_A = 'c391e72a666f51edc94498d62e8f393889a126477b357724b9f30b4a803ebc0e'


def prepare(output, value, retained):
    rid = refs.document_id(value)
    binding = {'release_id': rid, 'authority': value['authority'],
               'payload_revision': value['payload_revision'], 'root': str(retained),
               'retention_assertion': 'Owned retained NFS fixture; owner can mutate hard links'}
    # The caller explicitly chooses trusted references for job preparation.
    receipt = refs.bind(value, binding, rid, verification='reference-only')
    output.mkdir()
    for name, source in receipt['payload_paths'].items():
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, target)
    for name, text in value['metadata'].items():
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        target.chmod(0o444)
    refs.save(output / '.release-receipt.json', {**receipt, 'payload_paths': 'owned declared hard links',
                                               'input_root': str(output)})
    return receipt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    source = FIXTURE / 'reef/A'
    original = json.loads((FIXTURE / 'retained/manifests' / OLD_A).read_bytes())
    spec = {'dataset': 'hpa-release-reference-acceptance', 'authority': 'owned-retained-pool:test',
            'revision': 'A', 'payload_revision': OLD_A, 'payloads': [], 'metadata': {}}
    for entry in original['entries']:
        name = entry['path']
        inline = None
        if entry['kind'] == 'metadata':
            try:
                inline = (source / name).read_text()
            except UnicodeDecodeError:
                pass
        if inline is not None:
            spec['metadata'][name] = inline
        else:
            spec['payloads'].append({key: entry[key] for key in ('path', 'size', 'blake3')})
    results = []
    for mode in ('producer-digest', 'producer-revision', 'immutable-reference'):
        case = root / mode
        case.mkdir()
        start = time.perf_counter()
        a, registration = refs.register(spec, mode)
        bmeta = dict(a['metadata'])
        changed = 0
        for name, raw in list(bmeta.items()):
            if name.endswith('/zarr.json'):
                value = json.loads(raw)
                if 'dca' in value.get('attributes', {}) and 'channels' in value['attributes']['dca']:
                    value['attributes']['dca']['channels'][0]['name'] = 'microtubules release B'
                    value['attributes']['ome']['omero']['channels'][0]['label'] = 'microtubules release B'
                    bmeta[name] = json.dumps(value, sort_keys=True, separators=(',', ':'))
                    changed += 1
        b, update = refs.revise_metadata(a, refs.document_id(a), 'B', bmeta)
        prepared_a = prepare(case / 'A', a, source)
        prepared_b = prepare(case / 'B', b, source)
        prep_seconds = time.perf_counter() - start
        assert changed == 11 and registration['payload_bytes_read'] == update['payload_bytes_read'] == 0
        assert prepared_a['payload_bytes_read'] == prepared_b['payload_bytes_read'] == 0
        consumed = []
        observed = []
        for label, release in [('A', a), ('B', b), ('A-after-B', a)]:
            is_b = label == 'B'
            checkout = case / ('B' if is_b else 'A')
            record = case / (mode + '-loader-' + label)
            labels = ['microtubules release B' if is_b else 'microtubules', 'target protein', 'nuclei', 'ER']
            job = {'release_id': refs.document_id(release), 'input_root': str(checkout),
                   'record_dir': str(record), 'batches': 2, 'expected_labels': labels}
            spec_path = case / (label + '.job.json')
            refs.save(spec_path, job)
            command = [sys.executable, '-B', '-s', str(Path(__file__).with_name('metadata_release_katamari.py')), str(spec_path)]
            run = subprocess.run(command, capture_output=True, text=True, timeout=180)
            (case / (label + '.stdout')).write_text(run.stdout)
            (case / (label + '.stderr')).write_text(run.stderr)
            if run.returncode:
                raise RuntimeError(f'Loader {mode}/{label} failed: see {case}')
            actual = json.loads((record / 'result.json').read_bytes())
            assert actual['samples'] == 128 and actual['worker_exitcodes'] == [0, 0, 0, 0]
            observed.append(actual)
            consumed.append({'label': label, 'record': str(record), 'returncode': run.returncode,
                             'samples': actual['samples'], 'worker_exitcodes': actual['worker_exitcodes']})
        baseline = json.loads((FIXTURE / 'validate-A/result.json').read_bytes())
        for actual in observed:
            assert actual['selection'] == baseline['selection'][:actual['samples']]
            assert actual['raw_reads'] == baseline['raw_reads'][:len(actual['raw_reads'])]
            stripped = [{k:v for k,v in batch.items() if k != 'channel_names'} for batch in actual['batches']]
            expected = [{k:v for k,v in batch.items() if k != 'channel_names'} for batch in baseline['batches'][:2]]
            assert stripped == expected
        assert observed[0]['batches'] == observed[2]['batches']
        refs.save(case / 'A.release.json', a)
        refs.save(case / 'B.release.json', b)
        results.append({'mode': mode, 'registration': registration, 'update': update,
                        'preparation_seconds': prep_seconds, 'prepared_A': {k:v for k,v in prepared_a.items() if k!='payload_paths'},
                        'prepared_B': {k:v for k,v in prepared_b.items() if k!='payload_paths'},
                        'changed_metadata_files': changed, 'loaders': consumed,
                        'raw_and_transformed_baseline_match': True, 'A_unchanged_after_B': True})
    refs.save(root / 'result.json', {'result': 'passed', 'job_id': os.getenv('SLURM_JOB_ID'),
                                   'model_base': '7b68612 plus archived private patch',
                                   'original_release': OLD_A, 'results': results,
                                   'claim': 'Zero payload reads during registration and reference-only preparation; actual loaders read consumed data'})


if __name__ == '__main__':
    main()
