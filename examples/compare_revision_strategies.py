"""Measure bounded registration work and expose the weaker contracts' limits."""
import argparse
import json
import os
from pathlib import Path
import platform
import time

from blake3 import blake3
from zarrsum import revision_refs as refs


def now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def location(root, value):
    return {'release_id': refs.document_id(value), 'authority': value['authority'],
            'payload_revision': value['payload_revision'], 'root': str(root),
            'retention_assertion': 'Owned mutable test fixture; no provider retention guarantee'}


def timed(action):
    start = time.perf_counter()
    value = action()
    return value, time.perf_counter() - start


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    payloads = root / 'payloads'
    payloads.mkdir()
    result = {'started_utc': now(), 'host': platform.node(), 'job_id': os.getenv('SLURM_JOB_ID'),
              'python': platform.python_version(), 'logical_files': 8, 'bytes': 1 << 30,
              'cache_state': 'uncontrolled; repeated reads; no cache flush',
              'storage': 'Reef scratch NFS; owned dense synthetic files',
              'copy_or_transfer_timed': False, 'trials': []}
    spec = {'dataset': 'dense-fixture', 'authority': 'owned-producer:test', 'revision': 'A',
            'payload_revision': 'retained-A', 'payloads': [],
            'metadata': {'zarr.json': '{"label":"A"}'}}
    started = time.perf_counter()
    for i in range(8):
        name = f'payload-{i:02d}'
        hashed = blake3()
        block = os.urandom(4 << 20)
        with (payloads / name).open('xb') as stream:
            for _ in range(32):
                stream.write(block)
                hashed.update(block)
            stream.flush()
            os.fsync(stream.fileno())
        spec['payloads'].append({'path': name, 'size': 128 << 20, 'blake3': hashed.hexdigest()})
    result['producer_write_and_hash_seconds'] = time.perf_counter() - started
    refs.save(root / 'descriptor.json', spec)
    orders = [list(refs.MODES), list(reversed(refs.MODES)), list(refs.MODES[2:] + refs.MODES[:2])]
    retained = {}
    for trial, order in enumerate(orders):
        for mode in order:
            (value, receipt), seconds = timed(lambda: refs.register(spec, mode, payloads, seed='trial-seed'))
            policy = ('full' if mode in ('full', 'producer-digest') else
                      'fingerprint' if mode in ('header-footer', 'random-blocks') else
                      'size-mtime' if mode == 'size-mtime' else 'reference-only')
            checked, check_seconds = timed(lambda: refs.bind(value, location(payloads, value), refs.document_id(value), verification=policy))
            (updated, update_receipt), update_seconds = timed(lambda: refs.revise_metadata(
                value, refs.document_id(value), 'B', {'zarr.json': '{"label":"B"}'}))
            assert value['metadata']['zarr.json'] == '{"label":"A"}'
            assert updated['payloads'] == value['payloads']
            row = {'trial': trial, 'mode': mode, 'registration_seconds': seconds,
                   'binding_seconds': check_seconds, 'metadata_update_seconds': update_seconds,
                   'registration': receipt, 'binding': {k: v for k, v in checked.items() if k != 'payload_paths'},
                   'update': update_receipt}
            result['trials'].append(row)
            retained[mode] = value
            refs.save(root / f'{trial}-{mode}.release.json', value)
            refs.save(root / f'{trial}-{mode}.B.release.json', updated)
    # These are descriptor entries, not existing files or a petabyte benchmark.
    huge = {'dataset': 'descriptor-only-2PB', 'authority': 'synthetic:assertion',
            'revision': 'A', 'payload_revision': 'not-retained',
            'payloads': [{'path': f'file-{i:05d}', 'size': 200_000_000_000} for i in range(9999)],
            'metadata': {'zarr.json': '{}'}}
    huge['payloads'][0]['size'] += 200_000_000_000
    result['descriptor_only'] = []
    for mode in ('producer-revision', 'immutable-reference'):
        (_, receipt), seconds = timed(lambda: refs.register(huge, mode))
        assert receipt['payload_bytes_declared'] == 2_000_000_000_000_000
        result['descriptor_only'].append({'mode': mode, 'seconds': seconds, 'receipt': receipt,
                                         'physical_payload_files_created': 0})
    # Choose one block outside every recorded sample in payload 0.
    sampled = retained['random-blocks']['payloads'][0]['ranges']
    sampled_indices = {offset // refs.BLOCK for offset, _ in sampled}
    index = next(i for i in range(1, (128 << 20) // refs.BLOCK - 1) if i not in sampled_indices)
    original_stamp = (payloads / 'payload-00').stat()
    with (payloads / 'payload-00').open('r+b') as stream:
        stream.seek(index * refs.BLOCK)
        stream.write(b'known middle corruption')
    os.utime(payloads / 'payload-00', ns=(original_stamp.st_atime_ns, original_stamp.st_mtime_ns))
    outcomes = {}
    for mode, policy in [('full', 'full'), ('producer-digest', 'full'), ('header-footer', 'fingerprint'),
                         ('random-blocks', 'fingerprint'), ('producer-revision', 'reference-only'),
                         ('immutable-reference', 'reference-only'), ('size-mtime', 'size-mtime')]:
        value = retained[mode]
        try:
            refs.bind(value, location(payloads, value), refs.document_id(value), verification=policy)
            outcomes[mode] = 'change missed under selected policy'
        except ValueError:
            outcomes[mode] = 'change detected'
    assert outcomes['full'] == outcomes['producer-digest'] == 'change detected'
    assert all(outcomes[m] == 'change missed under selected policy' for m in refs.MODES if m not in ('full', 'producer-digest'))
    result['known_unsampled_change'] = {'block': index, 'outcomes': outcomes,
                                      'size_and_mtime_preserved': True}
    # Also require the stored random fingerprint to detect a sampled-block change.
    value = retained['random-blocks']
    with (payloads / 'payload-00').open('r+b') as stream:
        stream.seek(sampled[0][0])
        stream.write(b'known sampled corruption')
    try:
        refs.bind(value, location(payloads, value), refs.document_id(value), verification='fingerprint')
    except ValueError:
        result['sampled_change_detected'] = True
    else:
        raise AssertionError('Sampled corruption was missed')
    for path in payloads.iterdir():
        path.unlink()
    payloads.rmdir()
    for mode in ('immutable-reference', 'producer-revision'):
        try:
            value = retained[mode]
            refs.bind(value, location(payloads, value), refs.document_id(value), verification='reference-only')
        except FileNotFoundError:
            pass
        else:
            raise AssertionError('Missing retention silently accepted')
    result.update(result='passed', ended_utc=now(), payload_fixture_removed=True,
                  missing_retention_refused=True)
    refs.save(root / 'result.json', result)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
