import csv
import hashlib
from pathlib import Path

import pytest

pytest.importorskip('catalog_client')

from zarrsum import catalog_releases as catalog
from zarrsum.metadata_releases import Repository, canonical
from catalog_fixture import CatalogFixture


@pytest.fixture
def ready(tmp_path, monkeypatch):
    source=tmp_path/'source'
    image=source/'data/demo/fov.ome.zarr'
    (image/'0/c').mkdir(parents=True)
    (image/'0/c/0').write_bytes(b'pixels')
    (image/'zarr.json').write_bytes(canonical({'zarr_format':3,'node_type':'group','attributes':{
        'dca':{'version':'0.2','channels':[{'index':0,'name':'ER'}]}}}))
    (source/'manifest.csv').write_text('path\n'+catalog.SOURCE_PREFIX+'demo/fov.ome.zarr\n')
    paths=[str(p.relative_to(source)) for p in source.rglob('*') if p.is_file()]
    repo=Repository(tmp_path/'repo')
    rid,_=repo.publish('demo','A',source,paths,['manifest.csv','data/demo/fov.ome.zarr/zarr.json'])
    root=tmp_path/'A'
    repo.stage(rid,'reef',root)
    monkeypatch.setattr(catalog,'reef_uri',lambda path:'reef://fixture/'+Path(path).relative_to(tmp_path).as_posix())
    monkeypatch.setattr(catalog,'filesystem_root',lambda value,platform:tmp_path/value['location_uri'].removeprefix('reef://fixture/'))
    return repo,rid,root


def promote(ready):
    repo,rid,root=ready
    repo.promote(rid,'reef',{'result':'passed','release_id':rid,'input_root':str(root)})


def test_unready_release_causes_no_catalog_call(ready,tmp_path):
    repo,rid,root=ready
    remote=CatalogFixture()
    with pytest.raises(ValueError,match='not ready'):
        catalog.publish_ready(repo,'demo',rid,'reef',remote.client,tmp_path/'request.json')
    assert not remote.events and not remote.records
    assert not (tmp_path/'request.json').exists()
    remote.client.close()


def test_actual_models_client_and_csv_bind_explicit_version(ready,tmp_path):
    promote(ready)
    repo,rid,root=ready
    remote=CatalogFixture()
    catalog.publish_ready(repo,'demo',rid,'reef',remote.client,tmp_path/'registration.json')
    snapshot=tmp_path/'snapshot.json'
    catalog.freeze_manifest(repo,'demo',rid,'reef',remote.client,snapshot)
    output=tmp_path/'training.csv'
    catalog.bind_training_csv(snapshot,hashlib.sha256(snapshot.read_bytes()).hexdigest(),rid,root,root/'manifest.csv',output)
    assert list(csv.DictReader(output.open()))==[{'path':str(root/'data/demo/fov.ome.zarr')}]
    assert len(remote.records)==1
    record=next(iter(remote.records.values()))
    assert record['version']==rid and record['canonical_id']=='demo/fov'
    assert record['locations'][0]['checksum'] is None
    assert all('is_latest' not in event['params'] for event in remote.events)
    remote.client.close()


def test_changed_snapshot_is_refused_before_training_csv(ready,tmp_path):
    promote(ready)
    repo,rid,root=ready
    remote=CatalogFixture()
    catalog.publish_ready(repo,'demo',rid,'reef',remote.client,tmp_path/'registration.json')
    snapshot=tmp_path/'snapshot.json'
    catalog.freeze_manifest(repo,'demo',rid,'reef',remote.client,snapshot)
    expected=hashlib.sha256(snapshot.read_bytes()).hexdigest()
    snapshot.write_text('{}')
    with pytest.raises(ValueError,match='changed after job binding'):
        catalog.bind_training_csv(snapshot,expected,rid,root,root/'manifest.csv',tmp_path/'training.csv')
    assert not (tmp_path/'training.csv').exists()
    remote.client.close()


def test_foreign_catalog_location_is_refused(ready,tmp_path):
    promote(ready)
    repo,rid,root=ready
    remote=CatalogFixture()
    catalog.publish_ready(repo,'demo',rid,'reef',remote.client,tmp_path/'registration.json')
    next(iter(remote.records.values()))['locations'][0]['location_uri']='reef://fixture/foreign'
    with pytest.raises(ValueError,match='does not match'):
        catalog.freeze_manifest(repo,'demo',rid,'reef',remote.client,tmp_path/'snapshot.json')
    remote.client.close()


def test_partial_catalog_registration_cannot_freeze_and_retry_is_idempotent(ready,tmp_path,monkeypatch):
    import shutil
    from catalog_client.exceptions import NotFoundError
    promote(ready)
    repo,a,a_root=ready
    remote=CatalogFixture()
    catalog.publish_ready(repo,'demo',a,'reef',remote.client,tmp_path/'registration-A.json')
    source=tmp_path/'source'
    shutil.copytree(source/'data/demo/fov.ome.zarr',source/'data/demo/other.ome.zarr')
    with (source/'manifest.csv').open('a') as stream:
        stream.write(catalog.SOURCE_PREFIX+'demo/other.ome.zarr\n')
    paths=[str(p.relative_to(source)) for p in source.rglob('*') if p.is_file()]
    b,_=repo.publish('demo','B',source,paths,[path for path in paths if '/c/' not in path])
    b_root=tmp_path/'B'
    repo.stage(b,'reef',b_root)
    repo.promote(b,'reef',{'result':'passed','release_id':b,'input_root':str(b_root)})
    create=remote.client.datasets.create
    def fail_other(request):
        if request.canonical_id.endswith('/other'):raise OSError('injected catalog failure')
        return create(request)
    monkeypatch.setattr(remote.client.datasets,'create',fail_other)
    with pytest.raises(OSError,match='injected catalog failure'):
        catalog.publish_ready(repo,'demo',b,'reef',remote.client,tmp_path/'registration-B-failed.json')
    assert len(remote.records)==2
    with pytest.raises(NotFoundError):
        catalog.freeze_manifest(repo,'demo',b,'reef',remote.client,tmp_path/'incomplete-snapshot.json')
    assert not (tmp_path/'incomplete-snapshot.json').exists()
    monkeypatch.setattr(remote.client.datasets,'create',create)
    catalog.publish_ready(repo,'demo',b,'reef',remote.client,tmp_path/'registration-B.json')
    assert len(remote.records)==3
    result=catalog.freeze_manifest(repo,'demo',b,'reef',remote.client,tmp_path/'complete-snapshot.json')
    assert len(result['entries'])==2
    assert repo.bind('demo',a,'reef')['input_root']==str(a_root)
    remote.client.close()
