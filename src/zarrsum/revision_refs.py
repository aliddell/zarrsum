"""Experimental input references with explicit identity and verification policies.

A document digest identifies assertions. It is not a dataset content digest.
Bindings select one retained source. They never select a current or latest source.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import random
import secrets
import stat
import time

from blake3 import blake3

FORMAT = 'release-reference/experimental-1'
MODES = ('full', 'producer-digest', 'immutable-reference', 'producer-revision',
         'header-footer', 'random-blocks', 'size-mtime')
MAX_MEMBERS = 10_000
MAX_DOCUMENT = 16 << 20
BLOCK = 64 << 10


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def document_id(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def text(value):
    if not isinstance(value, str) or not value or len(value) > 4096 or any(ord(c) < 32 for c in value):
        raise ValueError('Invalid nonempty text field')
    return value


def member(value):
    text(value)
    if (PurePosixPath(value).is_absolute() or '\\' in value
            or any(part in ('', '.', '..') for part in value.split('/'))):
        raise ValueError('Unsafe member path')
    return value


def hexdigest(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('Invalid digest')
    return value


def validate_spec(spec):
    if not isinstance(spec, dict) or set(spec) != {'dataset', 'authority', 'revision', 'payload_revision', 'payloads', 'metadata'}:
        raise ValueError('Invalid descriptor fields')
    for field in ('dataset', 'authority', 'revision', 'payload_revision'):
        text(spec[field])
    if not isinstance(spec['payloads'], list) or not isinstance(spec['metadata'], dict):
        raise ValueError('Invalid membership')
    if not 0 < len(spec['payloads']) + len(spec['metadata']) <= MAX_MEMBERS:
        raise ValueError('Descriptor must contain 1 to 10000 declared physical members')
    names = {'.release-receipt.json'}
    for entry in spec['payloads']:
        if not isinstance(entry, dict) or not {'path', 'size'} <= set(entry) <= {'path', 'size', 'blake3'}:
            raise ValueError('Invalid payload fields')
        name = member(entry['path'])
        if name in names or type(entry['size']) is not int or entry['size'] < 0:
            raise ValueError('Duplicate member or invalid length')
        if 'blake3' in entry:
            hexdigest(entry['blake3'])
        names.add(name)
    for name, value in spec['metadata'].items():
        member(name)
        if name in names or not isinstance(value, str):
            raise ValueError('Duplicate member or non-text metadata')
        names.add(name)
    for name in names:
        if any('/'.join(name.split('/')[:i]) in names for i in range(1, len(name.split('/')))):
            raise ValueError('A file is also a parent path')
    if len(canonical(spec)) > MAX_DOCUMENT:
        raise ValueError('Descriptor exceeds the document bound')


def ranges(size, mode, seed):
    if mode == 'full':
        return [[0, size]] if size else []
    if mode == 'header-footer':
        if size <= 2 * BLOCK:
            return [[0, size]] if size else []
        return [[0, BLOCK], [size - BLOCK, BLOCK]]
    count = (size + BLOCK - 1) // BLOCK
    rng = random.Random(seed)
    indices = sorted(rng.sample(range(count), min(8, count)))
    return [[i * BLOCK, min(BLOCK, size - i * BLOCK)] for i in indices]


def stamp(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


@contextmanager
def open_member(root, name):
    """Open declared regular files without following member symlinks."""
    root = Path(root).resolve(strict=True)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        parts = member(name).split('/')
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
        with os.fdopen(fd, 'rb', buffering=0) as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError('Payload is not a regular file')
            yield stream
    finally:
        os.close(descriptor)


def read_fingerprint(stream, selected, size, mode):
    before = os.fstat(stream.fileno())
    if before.st_size != size:
        raise ValueError('Payload length differs')
    hashed = blake3()
    if mode != 'full':
        hashed.update(canonical({'size': size, 'ranges': selected}))
    count = 0
    for offset, length in selected:
        stream.seek(offset)
        remaining = length
        while remaining:
            block = stream.read(min(4 << 20, remaining))
            if not block:
                raise ValueError('Payload ended during verification')
            hashed.update(block)
            remaining -= len(block)
            count += len(block)
    if stamp(before) != stamp(os.fstat(stream.fileno())):
        raise ValueError('Payload changed during verification')
    return hashed.hexdigest(), count


def register(spec, mode, root=None, *, seed=None):
    """Register membership with the selected evidence. Do not copy payloads."""
    validate_spec(spec)
    if mode not in MODES:
        raise ValueError('Unknown registration mode')
    if mode in ('full', 'header-footer', 'random-blocks', 'size-mtime') and root is None:
        raise ValueError('This mode requires a payload source')
    seed = seed or secrets.token_hex(16)
    text(seed)
    entries = []
    count = 0
    for source in sorted(spec['payloads'], key=lambda item: item['path']):
        entry = {'path': source['path'], 'size': source['size']}
        if mode in ('full', 'header-footer', 'random-blocks'):
            selected = ranges(entry['size'], mode, seed + ':' + entry['path'])
            with open_member(root, entry['path']) as stream:
                fingerprint, n = read_fingerprint(stream, selected, entry['size'], mode)
            count += n
            if mode == 'full':
                if 'blake3' in source and source['blake3'] != fingerprint:
                    raise ValueError('Producer digest differs from fresh full digest')
                entry['blake3'] = fingerprint
            else:
                entry.update(ranges=selected, fingerprint_blake3=fingerprint)
        elif mode == 'size-mtime':
            with open_member(root, entry['path']) as stream:
                info = os.fstat(stream.fileno())
                if info.st_size != entry['size']:
                    raise ValueError('Payload length differs')
                entry['mtime_ns'] = info.st_mtime_ns
        elif mode == 'producer-digest':
            entry['blake3'] = hexdigest(source.get('blake3'))
        entries.append(entry)
    value = {'format': FORMAT, 'mode': mode, 'dataset': spec['dataset'],
             'authority': spec['authority'], 'revision': spec['revision'], 'payload_revision': spec['payload_revision'],
             'payloads': entries, 'metadata': dict(spec['metadata'])}
    if mode == 'random-blocks':
        value['seed'] = seed
    receipt = {'operation': 'registration', 'mode': mode, 'payload_bytes_read': count,
               'payload_bytes_declared': sum(item['size'] for item in entries),
               'payload_members': len(entries), 'metadata_members': len(spec['metadata']),
               'size_mtime_checks': len(entries) if mode == 'size-mtime' else 0,
               'fresh_full_payload_check': mode == 'full',
               'identity_basis': ('full-digests' if mode in ('full', 'producer-digest')
                                  else 'sampled-fingerprints' if mode in ('header-footer', 'random-blocks')
                                  else 'size-and-mtime' if mode == 'size-mtime'
                                  else 'publisher-asserted-revision'),
               'retention_verified': False, 'release_id': document_id(value)}
    return value, receipt


def validate_release(value, expected_id):
    if document_id(value) != hexdigest(expected_id):
        raise ValueError('Release document differs from the pinned ID')
    if value.get('format') != FORMAT or value.get('mode') not in MODES:
        raise ValueError('Invalid release format or mode')
    mode = value['mode']
    fields = {'format', 'mode', 'dataset', 'authority', 'revision', 'payload_revision', 'payloads', 'metadata'}
    if mode == 'random-blocks':
        fields.add('seed')
        text(value.get('seed'))
    if set(value) != fields:
        raise ValueError('Invalid release fields')
    spec = {key: value[key] for key in ('dataset', 'authority', 'revision', 'payload_revision', 'metadata')}
    spec['payloads'] = []
    for entry in value['payloads']:
        fields = {'path', 'size'}
        if mode in ('full', 'producer-digest'):
            fields.add('blake3')
            hexdigest(entry.get('blake3'))
        if mode in ('header-footer', 'random-blocks'):
            fields |= {'ranges', 'fingerprint_blake3'}
            hexdigest(entry.get('fingerprint_blake3'))
            if entry.get('ranges') != ranges(entry['size'], mode, value.get('seed', '') + ':' + entry['path']):
                raise ValueError('Invalid sample ranges')
        if mode == 'size-mtime':
            fields.add('mtime_ns')
            if type(entry.get('mtime_ns')) is not int:
                raise ValueError('Invalid nanosecond modification time')
        if set(entry) != fields:
            raise ValueError('Invalid payload evidence')
        spec['payloads'].append({key: entry[key] for key in ('path', 'size')})
    validate_spec(spec)


def revise_metadata(value, expected_id, revision, metadata):
    """Create a metadata revision without reopening inherited payloads."""
    validate_release(value, expected_id)
    result = json.loads(canonical(value))
    result['revision'] = text(revision)
    if result['revision'] == value['revision']:
        raise ValueError('A metadata change requires a new revision')
    result['metadata'] = metadata
    validate_release(result, document_id(result))
    return result, {'release_id': document_id(result), 'parent_release_id': expected_id,
                    'operation': 'metadata-revision', 'payload_bytes_read': 0,
                    'fresh_full_payload_check': False, 'retention_verified': False,
                    'inherited_identity_basis': value['mode']}


def bind(value, binding, expected_id, *, verification):
    """Check one exact source under an explicitly selected verification policy."""
    validate_release(value, expected_id)
    if set(binding) != {'release_id', 'authority', 'payload_revision', 'root', 'retention_assertion'}:
        raise ValueError('Invalid location binding')
    for key in ('release_id', 'authority', 'payload_revision'):
        if binding[key] != (expected_id if key == 'release_id' else value[key]):
            raise ValueError('Binding names another revision')
    text(binding['retention_assertion'])
    mode = value['mode']
    if verification not in ('full', 'fingerprint', 'reference-only', 'size-mtime'):
        raise ValueError('Select full, fingerprint, reference-only, or size-mtime verification')
    if verification == 'full' and mode not in ('full', 'producer-digest'):
        raise ValueError('Full verification requires trusted expected full digests')
    if verification == 'fingerprint' and mode not in ('header-footer', 'random-blocks'):
        raise ValueError('No recorded fingerprint evidence')
    if verification == 'size-mtime' and mode != 'size-mtime':
        raise ValueError('Size-and-mtime verification requires recorded timestamps')
    root = Path(binding['root']).resolve(strict=True)
    count = 0
    paths = {}
    for entry in value['payloads']:
        with open_member(root, entry['path']) as stream:
            info = os.fstat(stream.fileno())
            if info.st_size != entry['size']:
                raise ValueError('Payload length differs')
            if verification == 'size-mtime' and info.st_mtime_ns != entry['mtime_ns']:
                raise ValueError('Payload modification time differs')
            if verification in ('full', 'fingerprint'):
                selected = ranges(entry['size'], 'full', '') if verification == 'full' else entry['ranges']
                actual, n = read_fingerprint(stream, selected, entry['size'],
                                             'full' if verification == 'full' else mode)
                count += n
                expected = entry['blake3'] if verification == 'full' else entry['fingerprint_blake3']
                if actual != expected:
                    raise ValueError('Payload evidence differs')
        paths[entry['path']] = str(root / entry['path'])
    return {'release_id': expected_id, 'operation': 'binding', 'verification': verification,
            'payload_bytes_read': count, 'payload_paths': paths,
            'size_mtime_checks': len(paths) if verification == 'size-mtime' else 0,
            'fresh_full_payload_check': verification == 'full',
            'retention_assertion': binding['retention_assertion'], 'retention_verified': False,
            'warning': 'A path and revision assertion do not enforce immutability or retention.'}


def read_json(path):
    with Path(path).open('rb') as stream:
        raw = stream.read(MAX_DOCUMENT + 1)
    if len(raw) > MAX_DOCUMENT:
        raise ValueError('JSON exceeds the document bound')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate JSON field')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique)


def save(path, value):
    with Path(path).open('xb') as stream:
        stream.write(canonical(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    registration = commands.add_parser('register')
    registration.add_argument('descriptor')
    registration.add_argument('--mode', choices=MODES, required=True)
    registration.add_argument('--root')
    registration.add_argument('--seed')
    registration.add_argument('--output', required=True)
    check = commands.add_parser('bind')
    check.add_argument('release')
    check.add_argument('binding')
    check.add_argument('--release-id', required=True)
    check.add_argument('--verification', choices=('full', 'fingerprint', 'reference-only', 'size-mtime'), required=True)
    revision = commands.add_parser('revise-metadata')
    revision.add_argument('release')
    revision.add_argument('--release-id', required=True)
    revision.add_argument('--revision', required=True)
    revision.add_argument('--metadata', required=True)
    revision.add_argument('--output', required=True)
    args = parser.parse_args(argv)
    start = time.perf_counter()
    try:
        if args.command == 'register':
            value, receipt = register(read_json(args.descriptor), args.mode, args.root, seed=args.seed)
            save(args.output, value)
        elif args.command == 'revise-metadata':
            value, receipt = revise_metadata(read_json(args.release), args.release_id,
                                             args.revision, read_json(args.metadata))
            save(args.output, value)
        else:
            receipt = bind(read_json(args.release), read_json(args.binding), args.release_id,
                           verification=args.verification)
        receipt['elapsed_seconds'] = time.perf_counter() - start
        print(json.dumps(receipt, sort_keys=True))
        return 0
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(json.dumps({'state': 'refused', 'reason': str(error)}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
