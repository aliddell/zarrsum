import json
from pathlib import Path

import pytest
from blake3 import blake3

pytest.importorskip('catalog_client')
from catalog_fixture import CatalogFixture
from zarrsum import reference_workflow as flow
from zarrsum import revision_refs as refs


def uri(path):
    return {'storage_platform': 'sf_hpc', 'location_uri': 'sf_hpc://' + str(path)}


@pytest.fixture
def case(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'pixels').write_bytes(b'data')
    spec = {'dataset': 'demo', 'authority': 'producer:test', 'revision': 'A',
            'payload_revision': 'pixels-1',
            'payloads': [{'path': 'pixels', 'size': 4, 'blake3': blake3(b'data').hexdigest()}],
            'metadata': {'zarr.json': '{"label":"A"}'}}
    a, _ = refs.register(spec, 'producer-digest')
    catalog = CatalogFixture()
    value = {'source': source, 'store': uri(tmp_path / 'published'), 'a': a,
             'catalog': catalog, 'template': {'canonical_id': 'demo/store', 'project': 'test',
              'name': 'Demo', 'modality': 'imaging', 'dataset_type': 'processed',
              'governance': {'access_scope': 'internal'}, 'metadata': {}}}
    yield value
    catalog.client.close()


def publish(case, value=None, locations=None, templates=None, verification='reference-only'):
    value = value or case['a']
    return flow.publish(value, case['store'], locations or [uri(case['source'])],
                        templates or [case['template']], 'Owned fixture; no provider hold',
                        case['catalog'].client, platform='sf_hpc', verification=verification)


def resolve(case, value=None):
    return flow.resolve(case['catalog'].client, 'demo/store', refs.document_id(value or case['a']),
                        'test', platform='sf_hpc')


def prepare(snapshot, destination, verification='reference-only'):
    return flow.prepare(snapshot, refs.document_id(snapshot), destination,
                        platform='sf_hpc', verification=verification)


def test_publish_discover_resolve_and_prepare_use_real_catalog_contract(case, tmp_path):
    published = publish(case)
    assert published['payload_bytes_read'] == 0 and published['publication_complete']
    page = flow.versions(case['catalog'].client, 'demo/store', 'test')
    assert [record['version'] for record in page['versions']] == [refs.document_id(case['a'])]
    snapshot, resolved = resolve(case)
    assert resolved['snapshot_id'] == refs.document_id(snapshot)
    result = prepare(snapshot, tmp_path / 'job-A', 'full')
    assert result['payload_bytes_read'] == 4 and result['fresh_full_payload_check']
    assert (tmp_path / 'job-A/zarr.json').read_text() == '{"label":"A"}'
    record = next(iter(case['catalog'].records.values()))
    asset = record['locations'][0]
    assert asset['file_format'] == flow.ASSET_FORMAT and asset['asset_type'] == 'file'
    assert asset['checksum_alg'] == 'sha256' and asset['checksum'] != record['version']
    assert record['is_latest'] is False
    assert not any('is_latest' in event['params'] for event in case['catalog'].events)


def test_frozen_A_survives_B_publication_and_catalog_loss(case, tmp_path, monkeypatch):
    publish(case)
    snapshot_a, _ = resolve(case)
    prepare(snapshot_a, tmp_path / 'job-A')
    with (tmp_path / 'job-A/zarr.json').open() as active_a:
        b, _ = refs.revise_metadata(case['a'], refs.document_id(case['a']), 'B', {'zarr.json': '{"label":"B"}'})
        publish(case, b)
        snapshot_b, _ = resolve(case, b)
        prepare(snapshot_b, tmp_path / 'job-B')
        assert active_a.read() == '{"label":"A"}'
    def unavailable(*args, **kwargs):
        raise PermissionError('Catalog no longer accessible')
    monkeypatch.setattr(case['catalog'].client.datasets, 'get', unavailable)
    prepare(snapshot_a, tmp_path / 'job-A-again')
    assert (tmp_path / 'job-A-again/zarr.json').read_bytes() == (tmp_path / 'job-A/zarr.json').read_bytes()
    assert (tmp_path / 'job-B/zarr.json').read_text() == '{"label":"B"}'
    assert (tmp_path / 'job-A/pixels').stat().st_ino == (tmp_path / 'job-B/pixels').stat().st_ino
    assert (tmp_path / 'job-A/zarr.json').stat().st_ino != (tmp_path / 'job-B/zarr.json').stat().st_ino


def test_payload_fallback_keeps_same_release(case, tmp_path):
    missing = uri(tmp_path / 'unavailable')
    result = publish(case, locations=[missing, uri(case['source'])])
    assert len(result['attempts']) == 1
    snapshot, _ = resolve(case)
    checked = prepare(snapshot, tmp_path / 'job')
    assert checked['selected_location'] == uri(case['source'])
    assert len(checked['attempts']) == 1 and checked['release_id'] == refs.document_id(case['a'])


def test_permission_failure_can_use_declared_same_version_copy(case, tmp_path, monkeypatch):
    import shutil
    replica = tmp_path / 'replica'
    shutil.copytree(case['source'], replica)
    publish(case, locations=[uri(case['source']), uri(replica)])
    snapshot, _ = resolve(case)
    original = flow._link_payloads
    def denied_first(value, root, stage):
        if root == case['source']:
            raise PermissionError(13, 'Permission denied', str(root))
        return original(value, root, stage)
    monkeypatch.setattr(flow, '_link_payloads', denied_first)
    result = prepare(snapshot, tmp_path / 'job')
    assert result['selected_location'] == uri(replica)
    assert 'Permission denied' in result['attempts'][0]['reason']


def test_all_copies_unavailable_leave_no_checkout(case, tmp_path):
    publish(case)
    snapshot, _ = resolve(case)
    (case['source'] / 'pixels').unlink()
    with pytest.raises(ValueError, match='No declared copy'):
        prepare(snapshot, tmp_path / 'job')
    assert not (tmp_path / 'job').exists()
    assert not list(tmp_path.glob('.reference-stage-*'))


def test_unavailable_source_does_not_advertise_publication(case):
    (case['source'] / 'pixels').unlink()
    with pytest.raises(ValueError, match='No accessible exact payload'):
        publish(case)
    assert not case['catalog'].records and not case['catalog'].events


def test_partial_catalog_registration_has_no_complete_marker_and_retry_reconciles(case, tmp_path, monkeypatch):
    second = {**case['template'], 'canonical_id': 'demo/second'}
    original = case['catalog'].client.register
    def fail_second(request, **kwargs):
        if request.canonical_id == 'demo/second':
            raise OSError('Injected catalog failure')
        return original(request, **kwargs)
    monkeypatch.setattr(case['catalog'].client, 'register', fail_second)
    with pytest.raises(OSError, match='Injected'):
        publish(case, templates=[case['template'], second])
    assert len(case['catalog'].records) == 1
    with pytest.raises(ValueError, match='No complete accessible'):
        resolve(case)
    monkeypatch.setattr(case['catalog'].client, 'register', original)
    publish(case, templates=[case['template'], second])
    publish(case, templates=[case['template'], second])
    assert len(case['catalog'].records) == 2
    resolve(case)


@pytest.mark.parametrize('name', ('release.json', 'locations.json', 'catalog-requests.json', 'publication-complete.json'))
def test_changed_publication_artifact_is_refused(case, name):
    publish(case)
    root = Path(case['store']['location_uri'].partition('://')[2]) / refs.document_id(case['a'])
    path = root / name
    path.chmod(0o644)
    value = json.loads(path.read_bytes())
    if name == 'release.json': value['metadata']['zarr.json'] = 'changed'
    elif name == 'locations.json': value['payload_revision'] = 'other'
    elif name == 'catalog-requests.json': value['requests'] = []
    else: value['catalog_records'] = []
    path.write_bytes(refs.canonical(value))
    with pytest.raises(ValueError, match='No complete accessible'):
        resolve(case)


def test_changed_resolution_is_refused(case, tmp_path):
    publish(case)
    snapshot, receipt = resolve(case)
    snapshot['bindings']['locations'] = [uri(tmp_path / 'other')]
    with pytest.raises(ValueError, match='pinned ID'):
        flow.prepare(snapshot, receipt['snapshot_id'], tmp_path / 'job', platform='sf_hpc', verification='reference-only')


def test_existing_destination_preserved(case, tmp_path):
    publish(case)
    snapshot, _ = resolve(case)
    output = tmp_path / 'existing'
    output.mkdir()
    (output / 'keep').write_text('unchanged')
    with pytest.raises(FileExistsError):
        prepare(snapshot, output)
    assert (output / 'keep').read_text() == 'unchanged'


def test_full_verification_refuses_corrupt_payload_after_resolution(case, tmp_path):
    publish(case)
    snapshot, _ = resolve(case)
    (case['source'] / 'pixels').write_bytes(b'BAD!')
    with pytest.raises(ValueError, match='evidence differs'):
        prepare(snapshot, tmp_path / 'job', 'full')
    assert not (tmp_path / 'job').exists()


def test_foreign_catalog_publication_and_tombstones_are_refused(case):
    publish(case)
    record = next(iter(case['catalog'].records.values()))
    record['locations'][0]['checksum'] = '0' * 64
    with pytest.raises(ValueError, match='No complete accessible'):
        resolve(case)
    record['locations'][0]['tombstoned'] = True
    with pytest.raises(ValueError, match='No complete accessible'):
        resolve(case)


def test_requests_validate_before_any_catalog_operation(case):
    with pytest.raises(ValueError, match='generated'):
        publish(case, templates=[{**case['template'], 'version': 'latest'}])
    assert not case['catalog'].events


def test_other_platform_is_not_assumed_mounted(case, tmp_path):
    publish(case)
    with pytest.raises(ValueError, match='No complete accessible'):
        flow.resolve(case['catalog'].client, 'demo/store', refs.document_id(case['a']), 'test', platform='reef')


def test_hardlink_failure_is_not_a_silent_copy(case, tmp_path, monkeypatch):
    publish(case)
    snapshot, _ = resolve(case)
    def cross_device(*args, **kwargs):
        raise OSError(18, 'Invalid cross-device link')
    monkeypatch.setattr(flow.os, 'link', cross_device)
    with pytest.raises(ValueError, match='cross-device'):
        prepare(snapshot, tmp_path / 'job')
    assert not (tmp_path / 'job').exists()


def test_size_mtime_policy_survives_catalog_roundtrip(case, tmp_path):
    spec = {key: case['a'][key] for key in ('dataset', 'authority', 'revision', 'payload_revision', 'metadata')}
    spec['payloads'] = [{'path': 'pixels', 'size': 4}]
    value, _ = refs.register(spec, 'size-mtime', case['source'])
    publish(case, value, verification='size-mtime')
    snapshot, _ = resolve(case, value)
    result = prepare(snapshot, tmp_path / 'job', 'size-mtime')
    assert result['size_mtime_checks'] == 1 and result['payload_bytes_read'] == 0


@pytest.mark.parametrize('use_parent_alias', (False, True))
def test_checkout_inside_source_is_refused_before_any_write(case, tmp_path, use_parent_alias):
    publish(case)
    snapshot, _ = resolve(case)
    before = {str(path.relative_to(case['source'])) for path in case['source'].rglob('*')}
    if use_parent_alias:
        alias = tmp_path / 'source-alias'
        alias.symlink_to(case['source'], target_is_directory=True)
        destination = alias / 'job'
    else:
        destination = case['source'] / 'job'
    with pytest.raises(ValueError, match='separate from the retained source'):
        prepare(snapshot, destination)
    assert {str(path.relative_to(case['source'])) for path in case['source'].rglob('*')} == before


def test_replacing_a_catalog_version_with_another_manifest_is_refused(case, tmp_path):
    publish(case)
    case['store'] = uri(tmp_path / 'other-publication')
    with pytest.raises(ValueError, match='another release publication'):
        publish(case)
    assert len(case['catalog'].records) == 1


def test_failed_document_write_does_not_leave_a_final_artifact(tmp_path, monkeypatch):
    def fail_sync(*args):
        raise OSError('Injected flush failure')
    monkeypatch.setattr(flow.os, 'fsync', fail_sync)
    target = tmp_path / 'release.json'
    with pytest.raises(OSError, match='flush failure'):
        flow.immutable(target, b'new document')
    assert not target.exists()
    assert not list(tmp_path.glob('.publication-*'))


def test_changed_catalog_scientific_metadata_cannot_resolve_or_republish(case):
    publish(case)
    record = next(iter(case['catalog'].records.values()))
    record['metadata'] = {'sample': {'extra_note': 'changed externally'}}
    with pytest.raises(ValueError, match='No complete accessible'):
        resolve(case)
    with pytest.raises(ValueError, match='metadata differs'):
        publish(case)


def test_catalog_latest_label_does_not_redirect_exact_resolution(case):
    publish(case)
    record = next(iter(case['catalog'].records.values()))
    record['is_latest'] = True
    snapshot, _ = resolve(case)
    assert snapshot['release_id'] == refs.document_id(case['a'])
    assert snapshot['is_latest_used'] is False
