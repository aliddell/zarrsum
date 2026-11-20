import copy
import json

import pytest
from blake3 import blake3

from zarrsum import revision_refs as refs


@pytest.fixture
def fixture(tmp_path):
    root = tmp_path / 'retained'
    root.mkdir()
    data = bytes(range(256)) * 2048
    (root / 'payload').write_bytes(data)
    spec = {'dataset': 'demo', 'authority': 'producer:test', 'revision': 'A', 'payload_revision': 'retained-A',
            'payloads': [{'path': 'payload', 'size': len(data), 'blake3': blake3(data).hexdigest()}],
            'metadata': {'zarr.json': '{"label":"A"}'}}
    return root, spec


def binding(root, value):
    return {'release_id': refs.document_id(value), 'authority': value['authority'],
            'payload_revision': value['payload_revision'], 'root': str(root),
            'retention_assertion': 'Owned fixture; no provider hold verified'}


@pytest.mark.parametrize('mode', refs.MODES)
def test_all_modes_have_explicit_receipts_and_roundtrip(fixture, mode):
    root, spec = fixture
    value, receipt = refs.register(spec, mode, root, seed='test-seed')
    policy = 'full' if mode in ('full', 'producer-digest') else 'fingerprint' if mode in ('header-footer', 'random-blocks') else 'size-mtime' if mode == 'size-mtime' else 'reference-only'
    result = refs.bind(value, binding(root, value), refs.document_id(value), verification=policy)
    assert result['fresh_full_payload_check'] == (policy == 'full')
    assert not result['retention_verified']
    if mode in ('producer-digest', 'immutable-reference', 'producer-revision'):
        assert receipt['payload_bytes_read'] == 0
    if mode == 'header-footer':
        assert receipt['payload_bytes_read'] == 2 * refs.BLOCK
    assert refs.read_json(write_json(root / (mode + '.json'), value)) == value


def write_json(path, value):
    refs.save(path, value)
    return path


@pytest.mark.parametrize('mode', ('producer-digest', 'immutable-reference', 'producer-revision'))
def test_registration_does_not_open_payloads(fixture, monkeypatch, mode):
    root, spec = fixture
    def forbidden(*args, **kwargs):
        raise AssertionError('Payload source opened')
    monkeypatch.setattr(refs, 'open_member', forbidden)
    value, receipt = refs.register(spec, mode, root / 'does-not-exist')
    assert receipt['payload_bytes_read'] == 0
    assert len(value['payloads']) == 1


@pytest.mark.parametrize('mode', refs.MODES)
def test_metadata_revision_preserves_old_document_and_reads_no_payload(fixture, monkeypatch, mode):
    root, spec = fixture
    a, _ = refs.register(spec, mode, root)
    before = refs.canonical(a)
    def forbidden(*args, **kwargs):
        raise AssertionError('Payload source opened')
    monkeypatch.setattr(refs, 'open_member', forbidden)
    b, receipt = refs.revise_metadata(a, refs.document_id(a), 'B', {'zarr.json': '{"label":"B"}'})
    assert refs.canonical(a) == before
    assert b['payloads'] == a['payloads'] and refs.document_id(a) != refs.document_id(b)
    assert receipt['payload_bytes_read'] == 0 and not receipt['fresh_full_payload_check']


def test_middle_corruption_is_missed_by_fixed_ranges_and_revision_assertion(fixture):
    root, spec = fixture
    full, _ = refs.register(spec, 'full', root)
    sampled, _ = refs.register(spec, 'header-footer', root)
    trusted, _ = refs.register(spec, 'producer-revision')
    with (root / 'payload').open('r+b') as stream:
        stream.seek(3 * refs.BLOCK)
        stream.write(b'changed')
    with pytest.raises(ValueError, match='evidence differs'):
        refs.bind(full, binding(root, full), refs.document_id(full), verification='full')
    assert not refs.bind(sampled, binding(root, sampled), refs.document_id(sampled), verification='fingerprint')['fresh_full_payload_check']
    assert refs.bind(trusted, binding(root, trusted), refs.document_id(trusted), verification='reference-only')['payload_bytes_read'] == 0


def test_sampled_block_corruption_is_detected(fixture):
    root, spec = fixture
    value, _ = refs.register(spec, 'random-blocks', root, seed='fixed')
    offset = value['payloads'][0]['ranges'][0][0]
    with (root / 'payload').open('r+b') as stream:
        stream.seek(offset)
        stream.write(b'changed')
    with pytest.raises(ValueError, match='evidence differs'):
        refs.bind(value, binding(root, value), refs.document_id(value), verification='fingerprint')


@pytest.mark.parametrize('mode', ('header-footer', 'random-blocks', 'producer-revision', 'immutable-reference', 'size-mtime'))
def test_weaker_modes_cannot_claim_full_verification(fixture, mode):
    root, spec = fixture
    value, _ = refs.register(spec, mode, root)
    with pytest.raises(ValueError, match='requires trusted expected full digests'):
        refs.bind(value, binding(root, value), refs.document_id(value), verification='full')


def test_missing_retained_source_has_no_fallback(fixture):
    root, spec = fixture
    value, _ = refs.register(spec, 'immutable-reference')
    (root / 'payload').unlink()
    with pytest.raises(FileNotFoundError):
        refs.bind(value, binding(root, value), refs.document_id(value), verification='reference-only')


@pytest.mark.parametrize('change', ('authority', 'payload_revision', 'release_id'))
def test_foreign_binding_is_refused(fixture, change):
    root, spec = fixture
    value, _ = refs.register(spec, 'full', root)
    location = binding(root, value)
    location[change] = 'another'
    with pytest.raises(ValueError, match='another revision'):
        refs.bind(value, location, refs.document_id(value), verification='full')


@pytest.mark.parametrize('path', ('../escape', '/absolute', 'a/./b', 'a//b', 'a\\b'))
def test_unsafe_paths_are_refused(fixture, path):
    root, spec = fixture
    spec['payloads'][0]['path'] = path
    with pytest.raises(ValueError):
        refs.register(spec, 'producer-revision')


def test_symlink_payload_is_refused(fixture, tmp_path):
    root, spec = fixture
    outside = tmp_path / 'outside'
    (root / 'payload').rename(outside)
    (root / 'payload').symlink_to(outside)
    with pytest.raises(OSError):
        refs.register(spec, 'full', root)


def test_symlink_parent_is_refused(fixture, tmp_path):
    root, spec = fixture
    (root / 'alias').symlink_to(root, target_is_directory=True)
    spec['payloads'][0]['path'] = 'alias/payload'
    with pytest.raises(OSError):
        refs.register(spec, 'full', root)


def test_tampering_and_duplicate_json_are_refused(fixture):
    root, spec = fixture
    value, _ = refs.register(spec, 'full', root)
    expected = refs.document_id(value)
    changed = copy.deepcopy(value)
    changed['metadata']['zarr.json'] = 'other'
    with pytest.raises(ValueError, match='pinned ID'):
        refs.bind(changed, binding(root, value), expected, verification='full')
    (root / 'duplicate.json').write_text('{"a":1,"a":2}')
    with pytest.raises(ValueError, match='Duplicate JSON'):
        refs.read_json(root / 'duplicate.json')


def test_full_digest_import_and_fresh_check_are_distinct(fixture):
    root, spec = fixture
    spec['payloads'][0]['blake3'] = '0' * 64
    value, receipt = refs.register(spec, 'producer-digest')
    assert not receipt['fresh_full_payload_check']
    with pytest.raises(ValueError, match='evidence differs'):
        refs.bind(value, binding(root, value), refs.document_id(value), verification='full')
    with pytest.raises(ValueError, match='Producer digest differs'):
        refs.register(spec, 'full', root)


def test_cli_never_overwrites_a_release(fixture, capsys):
    root, spec = fixture
    descriptor = write_json(root / 'descriptor.json', spec)
    args = ['register', str(descriptor), '--mode', 'producer-revision', '--output', str(root / 'release.json')]
    assert refs.main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first['payload_bytes_read'] == 0
    assert refs.main(args) == 2
    assert json.loads(capsys.readouterr().out)['state'] == 'refused'


def test_metadata_revision_cli_needs_no_payload_source(fixture, capsys):
    root, spec = fixture
    value, _ = refs.register(spec, 'producer-revision')
    write_json(root / 'A.json', value)
    write_json(root / 'metadata.json', {'zarr.json': '{"label":"B"}'})
    assert refs.main(['revise-metadata', str(root / 'A.json'), '--release-id', refs.document_id(value),
                      '--revision', 'B', '--metadata', str(root / 'metadata.json'),
                      '--output', str(root / 'B.json')]) == 0
    assert json.loads(capsys.readouterr().out)['payload_bytes_read'] == 0
    assert refs.read_json(root / 'B.json')['payload_revision'] == value['payload_revision']


@pytest.mark.parametrize('path', ('.release-receipt.json', '.release-receipt.json/child'))
def test_reserved_receipt_path_is_refused(fixture, path):
    root, spec = fixture
    spec['payloads'][0]['path'] = path
    with pytest.raises(ValueError):
        refs.register(spec, 'producer-revision')


def test_limit_counts_metadata_and_payload_files(fixture):
    root, spec = fixture
    spec['payloads'] = [{'path': f'file-{i}', 'size': 200_000_000_000} for i in range(10_000)]
    with pytest.raises(ValueError, match='10000'):
        refs.register(spec, 'producer-revision')


def test_metadata_change_cannot_reuse_release_revision(fixture):
    root, spec = fixture
    value, _ = refs.register(spec, 'producer-revision')
    with pytest.raises(ValueError, match='new revision'):
        refs.revise_metadata(value, refs.document_id(value), 'A', {'zarr.json': 'changed'})


def test_size_and_mtime_check_reads_no_payload(fixture, monkeypatch):
    from contextlib import contextmanager

    root, spec = fixture
    original_open = refs.open_member

    class StatOnly:
        def __init__(self, stream):
            self.stream = stream

        def fileno(self):
            return self.stream.fileno()

        def read(self, *args):
            raise AssertionError('A stat check must not read payload bytes')

    @contextmanager
    def stat_only(*args):
        with original_open(*args) as stream:
            yield StatOnly(stream)

    monkeypatch.setattr(refs, 'open_member', stat_only)
    value, receipt = refs.register(spec, 'size-mtime', root)
    checked = refs.bind(value, binding(root, value), refs.document_id(value), verification='size-mtime')
    assert value['payloads'][0]['mtime_ns'] == (root / 'payload').stat().st_mtime_ns
    assert receipt['payload_bytes_read'] == checked['payload_bytes_read'] == 0
    assert receipt['size_mtime_checks'] == checked['size_mtime_checks'] == 1
    assert receipt['identity_basis'] == 'size-and-mtime'
    assert not checked['fresh_full_payload_check']


def test_size_mtime_detects_timestamp_change_but_misses_restored_stamp(fixture):
    import os

    root, spec = fixture
    value, _ = refs.register(spec, 'size-mtime', root)
    path = root / 'payload'
    before = path.stat()
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000))
    with pytest.raises(ValueError, match='modification time differs'):
        refs.bind(value, binding(root, value), refs.document_id(value), verification='size-mtime')
    with path.open('r+b') as stream:
        stream.seek(1000)
        stream.write(b'changed')
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    result = refs.bind(value, binding(root, value), refs.document_id(value), verification='size-mtime')
    assert result['payload_bytes_read'] == 0 and not result['fresh_full_payload_check']


def test_size_mtime_refuses_wrong_length(fixture):
    root, spec = fixture
    value, _ = refs.register(spec, 'size-mtime', root)
    (root / 'payload').write_bytes(b'short')
    with pytest.raises(ValueError, match='length differs'):
        refs.bind(value, binding(root, value), refs.document_id(value), verification='size-mtime')
    with pytest.raises(ValueError, match='length differs'):
        refs.register(spec, 'size-mtime', root)


@pytest.mark.parametrize('mode', ('full', 'producer-digest', 'producer-revision', 'immutable-reference', 'header-footer', 'random-blocks'))
def test_size_mtime_does_not_invent_baseline_for_old_modes(fixture, mode):
    root, spec = fixture
    value, _ = refs.register(spec, mode, root)
    with pytest.raises(ValueError, match='requires recorded timestamps'):
        refs.bind(value, binding(root, value), refs.document_id(value), verification='size-mtime')


def test_size_mtime_timestamp_must_be_integer(fixture):
    root, spec = fixture
    value, _ = refs.register(spec, 'size-mtime', root)
    value['payloads'][0]['mtime_ns'] = True
    with pytest.raises(ValueError, match='nanosecond modification time'):
        refs.validate_release(value, refs.document_id(value))


def test_size_mtime_cli_roundtrip(fixture, capsys):
    root, spec = fixture
    descriptor = write_json(root / 'descriptor.json', spec)
    output = root / 'release.json'
    assert refs.main(['register', str(descriptor), '--mode', 'size-mtime', '--root', str(root), '--output', str(output)]) == 0
    registered = json.loads(capsys.readouterr().out)
    value = refs.read_json(output)
    source = write_json(root / 'binding.json', binding(root, value))
    assert refs.main(['bind', str(output), str(source), '--release-id', registered['release_id'], '--verification', 'size-mtime']) == 0
    checked = json.loads(capsys.readouterr().out)
    assert checked['payload_bytes_read'] == 0 and checked['size_mtime_checks'] == 1


def test_permission_failure_is_explicit_refusal(fixture, monkeypatch, capsys):
    from contextlib import contextmanager

    root, spec = fixture
    value, _ = refs.register(spec, 'producer-revision')
    release = write_json(root / 'release.json', value)
    source = write_json(root / 'binding.json', binding(root, value))

    @contextmanager
    def denied(*args):
        raise PermissionError(13, 'Permission denied', str(root / 'payload'))
        yield

    monkeypatch.setattr(refs, 'open_member', denied)
    assert refs.main(['bind', str(release), str(source), '--release-id', refs.document_id(value), '--verification', 'reference-only']) == 2
    result = json.loads(capsys.readouterr().out)
    assert result['state'] == 'refused' and 'Permission denied' in result['reason']
