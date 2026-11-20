"""Map ready metadata releases to the pinned Scientific Dataset Catalog contract.

The optional client is supplied by the application. Dataset version stores the
release digest. No catalog field is invented for readiness or checksum semantics.
"""
import csv
import hashlib
import json
from pathlib import Path

from .locations import filesystem_root
from .metadata_releases import write_json


PROJECT = 'Data versioning acceptance'
SOURCE_PREFIX = '/mnt/main0/projects/katamari/data_prod/'


def reef_uri(path):
    path = Path(path).resolve(strict=True)
    relative = path.relative_to('/mnt/main0/projects')
    uri = 'reef://' + relative.as_posix()
    assert filesystem_root({'storage_platform':'reef','location_uri':uri},'reef') == path
    return uri


def members(repo, dataset, release_id, site):
    from biocurator.catalog.canonical_id import canonical_id
    bound = repo.bind(dataset, release_id, site)
    root = Path(bound['input_root'])
    rows = list(csv.DictReader((root/'manifest.csv').open(newline='')))
    result = {}
    for row in rows:
        path = row['path']
        if not path.startswith(SOURCE_PREFIX):
            raise ValueError('Unexpected source prefix in the training manifest')
        relative = Path(path[len(SOURCE_PREFIX):])
        if '..' in relative.parts or relative.is_absolute():
            raise ValueError('Unsafe source path')
        image = (root/'data'/relative).resolve(strict=True)
        if not image.is_relative_to(root/'data'):
            raise ValueError('Image escaped the prepared release')
        result[path] = {'canonical_id':canonical_id(relative.as_posix()),
                        'path':str(image),'relative_path':relative.as_posix(),'location_uri':reef_uri(image),
                        'version':release_id,'project':PROJECT}
    return bound, rows, result


def publish_ready(repo, dataset, release_id, site, client, evidence):
    from catalog_client.models.dataset import DatasetRequest
    bound, rows, stores = members(repo, dataset, release_id, site)
    requests=[]
    # Validate the whole request set before any catalog write.
    for value in stores.values():
        attrs=json.loads((Path(value['path'])/'zarr.json').read_bytes())['attributes']
        request=DatasetRequest.model_validate({
            'canonical_id':value['canonical_id'],'version':release_id,'project':PROJECT,
            'name':'HPA release acceptance: '+Path(value['path']).name,
            'modality':'imaging','dataset_type':'processed',
            'governance':{'access_scope':'internal'},
            'metadata':{'data_summary':{'channels':attrs['dca']['channels'],
                                      'dca_version':attrs['dca']['version']}},
            'locations':[{'storage_platform':'reef','location_uri':value['location_uri'],
                          'asset_type':'folder','file_format':'ome-zarr v0.5'}],
            'is_latest':False})
        requests.append(request)
    registered=[]
    for request in requests:
        rid=client.register(request,error_on_duplicate=False)
        registered.append({'catalog_id':rid,'request':request.model_dump(mode='json')})
    result={'release_id':release_id,'input_root':bound['input_root'],
            'identity_fields':['canonical_id','version','project'],
            'catalog_records':registered,'manifest_rows':len(rows),
            'readiness_source':'preparation journal and actual loader validation',
            'checksum_field_used_for_release_id':False}
    write_json(evidence,result)
    return result


def freeze_manifest(repo, dataset, release_id, site, client, evidence):
    from catalog_client.models.dataset import DatasetRef
    bound, rows, stores=members(repo,dataset,release_id,site)
    selected={}
    for original, value in stores.items():
        response=client.datasets.get(DatasetRef(value['canonical_id'],release_id,PROJECT))
        if response.version!=release_id or response.canonical_id!=value['canonical_id'] or response.tombstoned:
            raise ValueError('Catalog returned another dataset revision')
        matching=[asset for asset in response.locations if not asset.tombstoned
                  and asset.storage_platform=='reef' and asset.asset_type=='folder'
                  and asset.location_uri==value['location_uri']]
        if len(matching)!=1:
            raise ValueError('Catalog location does not match the prepared exact version')
        actual=filesystem_root({'storage_platform':'reef','location_uri':matching[0].location_uri},'reef')
        if actual!=Path(value['path']):raise ValueError('Catalog path mismatch')
        selected[original]={**value,'catalog_id':response.id,
                            'catalog_channels':[channel.name for channel in response.metadata.data_summary.channels]}
    result={'release_id':release_id,'input_root':bound['input_root'],
            'project':PROJECT,'identity_fields':['canonical_id','version','project'],
            'entries':selected,'is_latest_used':False}
    write_json(evidence,result)
    return result


def bind_training_csv(snapshot, expected_hash, release_id, checkout, original_csv, output):
    raw=Path(snapshot).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=expected_hash:
        raise ValueError('Catalog snapshot changed after job binding')
    value=json.loads(raw)
    checkout=Path(checkout).resolve(strict=True)
    if value['release_id']!=release_id or json.loads((checkout/'.release-receipt.json').read_bytes())['release_id']!=release_id:
        raise ValueError('Catalog snapshot belongs to another release')
    with Path(original_csv).open(newline='') as source, Path(output).open('x',newline='') as target:
        reader=csv.DictReader(source)
        writer=csv.DictWriter(target,fieldnames=reader.fieldnames)
        writer.writeheader()
        used=set()
        for row in reader:
            entry=value['entries'].get(row['path'])
            if not entry or entry['version']!=release_id or entry['project']!=PROJECT:
                raise ValueError('Catalog snapshot does not cover the manifest')
            relative=Path(entry['relative_path'])
            if relative.is_absolute() or '..' in relative.parts:
                raise ValueError('Catalog snapshot path escaped the release')
            original=(Path(value['input_root'])/'data'/relative).resolve(strict=True)
            if original!=Path(entry['path']).resolve(strict=True):
                raise ValueError('Catalog source asset changed the image mapping')
            path=(checkout/'data'/relative).resolve(strict=True)
            expected=(checkout/'data'/row['path'][len(SOURCE_PREFIX):]).resolve(strict=True)
            if not path.is_relative_to(checkout/'data') or not row['path'].startswith(SOURCE_PREFIX) or path!=expected:
                raise ValueError('Catalog snapshot changed the image mapping')
            used.add(row['path'])
            row['path']=str(path)
            writer.writerow(row)
        if used!=set(value['entries']):raise ValueError('Catalog snapshot has foreign members')
    return value
