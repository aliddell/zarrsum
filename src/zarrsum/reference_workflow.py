"""Publish reference documents, resolve exact catalog versions, and prepare inputs.

Catalog versions identify release documents. Filesystem locations remain outside
that identity. Hard links require cooperative retained storage on one filesystem.
"""
from contextlib import contextmanager
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile

from . import revision_refs as refs
from .locations import filesystem_root, location

ASSET_FORMAT = 'zarrsum-release-reference/1'
BUNDLE_FORMAT = 'reference-publication/1'
SNAPSHOT_FORMAT = 'reference-resolution/1'
MAX_BYTES = 32 << 20
MAX_RECORDS = 256


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return refs.canonical(value) + b'\n'


def read_document(root, name):
    with refs.open_member(root, name) as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError('Workflow document exceeds the byte bound')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate workflow field')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique), raw


def immutable(path, raw):
    path = Path(path)
    if path.exists() or path.is_symlink():
        if path.is_symlink() or path.read_bytes() != raw:
            raise ValueError('Existing publication artifact differs: ' + str(path))
        return
    descriptor, temporary = tempfile.mkstemp(prefix='.publication-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        Path(temporary).chmod(0o444)
        os.link(temporary, path)
        sync(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def sync(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def locked(parent, name):
    parent = Path(parent).resolve()
    parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(parent / ('.' + name + '.workflow.lock'),
                         os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def filesystem_location(value, platform):
    value = location(value)
    return value, filesystem_root(value, platform)


def child_location(root, relative):
    value = location(root)
    refs.member(relative)
    return {**value, 'location_uri': value['location_uri'].rstrip('/') + '/' + relative}


def coordinates(request):
    return {key: request[key] for key in ('canonical_id', 'version', 'project')}


def payload_bindings(value, locations, retention):
    refs.text(retention)
    if not isinstance(locations, list) or not 1 <= len(locations) <= 16:
        raise ValueError('Supply 1 to 16 exact payload locations')
    normalized = [location(item) for item in locations]
    if any(item['storage_platform'] not in ('reef', 'sf_hpc') for item in normalized):
        raise ValueError('This workflow supports mounted filesystem payloads only')
    if len({refs.canonical(item) for item in normalized}) != len(normalized):
        raise ValueError('Duplicate payload location')
    return {'release_id': refs.document_id(value), 'authority': value['authority'],
            'payload_revision': value['payload_revision'], 'locations': normalized,
            'retention_assertion': retention}


def validate_bindings(value, bindings):
    expected = payload_bindings(value, bindings['locations'], bindings['retention_assertion'])
    if bindings != expected:
        raise ValueError('Payload locations belong to another exact revision')


def local_binding(value, bindings, root):
    return {'release_id': refs.document_id(value), 'authority': value['authority'],
            'payload_revision': value['payload_revision'], 'root': str(root),
            'retention_assertion': bindings['retention_assertion']}


def check_candidate(value, bindings, platform, verification):
    failures = []
    for candidate in bindings['locations']:
        try:
            _, root = filesystem_location(candidate, platform)
            checked = refs.bind(value, local_binding(value, bindings, root),
                                refs.document_id(value), verification=verification)
            return candidate, checked, failures
        except (OSError, ValueError) as error:
            failures.append({'location': candidate, 'reason': str(error)})
    raise ValueError('No accessible exact payload copy: ' + json.dumps(failures))


def verify_record(response, coordinate, expected_asset=None):
    if response.tombstoned or any(getattr(response, key) != value for key, value in coordinate.items()):
        raise ValueError('Catalog returned another dataset version')
    assets = [asset for asset in response.locations if not asset.tombstoned
              and asset.asset_type == 'file' and asset.file_format == ASSET_FORMAT]
    if expected_asset is not None and not any(
        all(getattr(asset, key) == value for key, value in expected_asset.items()) for asset in assets
    ):
        raise ValueError('Existing catalog version names another release publication')
    return assets


def verify_catalog_metadata(response, request):
    from catalog_client.models.dataset import DatasetRequest

    actual = DatasetRequest.model_validate(response.model_dump(mode='json')).model_dump(mode='json')
    expected = DatasetRequest.model_validate(request).model_dump(mode='json')
    for key, value in expected.items():
        if key not in ('locations', 'is_latest') and actual[key] != value:
            raise ValueError('Catalog dataset metadata differs from the published request: ' + key)
    return refs.document_id({key: value for key, value in expected.items() if key not in ('locations', 'is_latest')})


def publish(value, store, locations, templates, retention, client, *, platform, verification):
    """Publish bounded reference documents, then register actual catalog records."""
    from catalog_client.models.dataset import DatasetRef, DatasetRequest

    rid = refs.document_id(value)
    refs.validate_release(value, rid)
    store, root = filesystem_location(store, platform)
    bindings = payload_bindings(value, locations, retention)
    if not isinstance(templates, list) or not 1 <= len(templates) <= MAX_RECORDS:
        raise ValueError('Supply 1 to 256 catalog record templates')
    document = encoded(value)
    pointer = child_location(store, rid + '/release.json')
    asset = {**pointer, 'asset_type': 'file', 'file_format': ASSET_FORMAT,
             'encoding': 'utf-8', 'size_bytes': len(document),
             'checksum': sha(document), 'checksum_alg': 'sha256'}
    requests = []
    for template in templates:
        if any(key in template for key in ('version', 'locations', 'is_latest')):
            raise ValueError('Version and publication location are generated by the workflow')
        requests.append(DatasetRequest.model_validate({**template, 'version': rid,
                                                       'locations': [asset], 'is_latest': False}))
    coordinate_list = [coordinates(request.model_dump(mode='json')) for request in requests]
    if len({refs.canonical(item) for item in coordinate_list}) != len(coordinate_list):
        raise ValueError('Duplicate catalog coordinates')
    coordinate_list.sort(key=refs.canonical)
    selected, checked, attempts = check_candidate(value, bindings, platform, verification)
    bundle = root / rid
    if bundle.is_symlink():
        raise ValueError('Publication directory is a symlink')
    with locked(root, rid):
        bundle.mkdir(exist_ok=True)
        immutable(bundle / 'release.json', document)
        immutable(bundle / 'locations.json', encoded(bindings))
        request_document = {'format': BUNDLE_FORMAT, 'release_id': rid,
                            'requests': [request.model_dump(mode='json') for request in requests]}
        immutable(bundle / 'catalog-requests.json', encoded(request_document))
        registered = []
        for request, coordinate in zip(requests, [coordinates(r.model_dump(mode='json')) for r in requests]):
            catalog_id = client.register(request, error_on_duplicate=False)
            response = client.datasets.get(DatasetRef(**coordinate))
            verify_record(response, coordinate, asset)
            verify_catalog_metadata(response, request.model_dump(mode='json'))
            if response.id != catalog_id:
                raise ValueError('Catalog registration changed identity')
            registered.append({**coordinate, 'catalog_id': catalog_id})
        marker = {'format': BUNDLE_FORMAT, 'release_id': rid,
                  'release_sha256': sha(document), 'locations_sha256': sha(encoded(bindings)),
                  'requests_sha256': sha(encoded(request_document)),
                  'catalog_records': sorted(registered, key=refs.canonical)}
        immutable(bundle / 'publication-complete.json', encoded(marker))
    return {'result': 'published', 'release_id': rid, 'document_location': pointer,
            'catalog_records': registered, 'selected_payload_location': selected,
            'verification': checked['verification'], 'payload_bytes_read': checked['payload_bytes_read'],
            'attempts': attempts, 'publication_complete': True,
            'training_validation_performed': False}


def versions(client, canonical_id, project, *, limit=100):
    refs.text(canonical_id)
    refs.text(project)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('Version listing limit must be 1 to 100')
    page = client.datasets.list(canonical_id=canonical_id, project=project, limit=limit)
    entries = []
    for record in page.results:
        if not record.tombstoned and record.canonical_id == canonical_id and record.project == project:
            if any(asset.file_format == ASSET_FORMAT and not asset.tombstoned for asset in record.locations):
                entries.append({'canonical_id': canonical_id, 'version': record.version,
                                'project': project, 'name': record.name, 'catalog_id': record.id})
    return {'versions': entries, 'total_catalog_records': page.total,
            'truncated': page.total > limit, 'is_latest_used': False,
            'publication_availability_checked': False}


def resolve(client, canonical_id, release_id, project, *, platform):
    """Freeze one exact catalog lookup. Preparation needs no further catalog access."""
    from catalog_client.models.dataset import DatasetRef

    refs.hexdigest(release_id)
    coordinate = {'canonical_id': refs.text(canonical_id), 'version': release_id,
                  'project': refs.text(project)}
    response = client.datasets.get(DatasetRef(**coordinate))
    assets = verify_record(response, coordinate)
    failures = []
    for asset in assets:
        pointer = {'storage_platform': str(asset.storage_platform.value if hasattr(asset.storage_platform, 'value') else asset.storage_platform),
                   'location_uri': asset.location_uri}
        try:
            pointer, path = filesystem_location(pointer, platform)
            if path.name != 'release.json':
                raise ValueError('Unexpected release document path')
            root = path.parent
            value, raw = read_document(root, 'release.json')
            refs.validate_release(value, release_id)
            if asset.checksum_alg != 'sha256' or asset.checksum != sha(raw) or asset.size_bytes != len(raw):
                raise ValueError('Catalog file checksum or length differs')
            bindings, binding_raw = read_document(root, 'locations.json')
            validate_bindings(value, bindings)
            requests, request_raw = read_document(root, 'catalog-requests.json')
            marker, _ = read_document(root, 'publication-complete.json')
            if (marker['format'] != BUNDLE_FORMAT or marker['release_id'] != release_id
                    or marker['release_sha256'] != sha(raw)
                    or marker['locations_sha256'] != sha(binding_raw)
                    or marker['requests_sha256'] != sha(request_raw)
                    or requests['format'] != BUNDLE_FORMAT or requests['release_id'] != release_id
                    or {**coordinate, 'catalog_id': response.id} not in marker['catalog_records']):
                raise ValueError('Publication is incomplete or changed')
            matching = [request for request in requests['requests'] if coordinates(request) == coordinate]
            if len(matching) != 1:
                raise ValueError('Publication does not identify one exact catalog request')
            catalog_metadata_hash = verify_catalog_metadata(response, matching[0])
            snapshot = {'format': SNAPSHOT_FORMAT, 'release_id': release_id,
                        'catalog_record': {**coordinate, 'catalog_id': response.id},
                        'catalog_metadata_sha256': catalog_metadata_hash,
                        'document_location': pointer, 'release': value, 'bindings': bindings,
                        'publication_sha256': refs.document_id(marker), 'is_latest_used': False}
            if len(encoded(snapshot)) > MAX_BYTES:
                raise ValueError('Resolution exceeds the document bound')
            return snapshot, {'result': 'resolved', 'release_id': release_id,
                              'snapshot_id': refs.document_id(snapshot), 'attempts': failures,
                              'catalog_record': snapshot['catalog_record']}
        except (OSError, ValueError, KeyError, TypeError) as error:
            failures.append({'location': pointer, 'reason': str(error)})
    raise ValueError('No complete accessible release publication: ' + json.dumps(failures))


def _link_payloads(value, root, stage):
    for entry in value['payloads']:
        name = entry['path']
        target = stage / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with refs.open_member(root, name) as source:
            before = os.fstat(source.fileno())
            os.link(root / name, target, follow_symlinks=False)
            linked = target.lstat()
            if not stat.S_ISREG(linked.st_mode) or (before.st_dev, before.st_ino) != (linked.st_dev, linked.st_ino):
                raise ValueError('Payload changed while creating the hard link')


def prepare(snapshot, snapshot_id, destination, *, platform, verification):
    """Materialize separate metadata and hard-linked payloads from a frozen resolution."""
    if refs.document_id(snapshot) != refs.hexdigest(snapshot_id):
        raise ValueError('Resolution snapshot differs from the pinned ID')
    if snapshot['format'] != SNAPSHOT_FORMAT or snapshot.get('is_latest_used') is not False:
        raise ValueError('Unsupported resolution snapshot')
    value = snapshot['release']
    rid = snapshot['release_id']
    refs.validate_release(value, rid)
    validate_bindings(value, snapshot['bindings'])
    if snapshot['catalog_record']['version'] != rid:
        raise ValueError('Catalog snapshot names another release')
    destination = Path(destination).absolute()
    destination = destination.parent.resolve() / destination.name
    if destination.name in ('', '.', '..'):
        raise ValueError('Select a new checkout path')
    for candidate in snapshot['bindings']['locations']:
        try:
            _, root = filesystem_location(candidate, platform)
        except (OSError, ValueError):
            continue
        if destination == root or root.is_relative_to(destination) or destination.is_relative_to(root):
            raise ValueError('Checkout must be separate from the retained source')
    attempts = []
    with locked(destination.parent, destination.name):
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(destination)
        for candidate in snapshot['bindings']['locations']:
            try:
                _, root = filesystem_location(candidate, platform)
                if destination == root or root.is_relative_to(destination) or destination.is_relative_to(root):
                    raise ValueError('Checkout must be separate from the retained source')
                with tempfile.TemporaryDirectory(prefix='.reference-stage-', dir=destination.parent) as temporary:
                    stage = Path(temporary) / 'checkout'
                    stage.mkdir()
                    _link_payloads(value, root, stage)
                    for name, text in value['metadata'].items():
                        path = stage / name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        with path.open('x', encoding='utf-8', newline='') as stream:
                            stream.write(text)
                            stream.flush()
                            os.fsync(stream.fileno())
                        path.chmod(0o444)
                    checked = refs.bind(value, local_binding(value, snapshot['bindings'], stage),
                                        rid, verification=verification)
                    receipt = {key: item for key, item in checked.items() if key != 'payload_paths'}
                    receipt.update(result='prepared', input_root=str(destination), snapshot_id=snapshot_id,
                                   selected_location=candidate, attempts=attempts,
                                   catalog_record=snapshot['catalog_record'],
                                   payload_materialization='hardlink; cooperative retained owner',
                                   metadata_sha256={name: sha(text.encode('utf-8')) for name, text in value['metadata'].items()})
                    immutable(stage / '.release-receipt.json', encoded(receipt))
                    for directory, _, _ in os.walk(stage, topdown=False):
                        sync(directory)
                    if destination.exists() or destination.is_symlink():
                        raise FileExistsError(destination)
                    os.rename(stage, destination)
                    sync(destination.parent)
                    return receipt
            except (OSError, ValueError) as error:
                attempts.append({'location': candidate, 'reason': str(error)})
    raise ValueError('No declared copy could prepare the exact release: ' + json.dumps(attempts))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    publication = commands.add_parser('publish')
    publication.add_argument('release')
    publication.add_argument('--publication', required=True, help='Store, payload locations, retention assertion, and catalog templates')
    listing = commands.add_parser('list')
    resolution = commands.add_parser('resolve')
    for item in (listing, resolution):
        item.add_argument('--canonical-id', required=True)
        item.add_argument('--project', required=True)
    resolution.add_argument('--release-id', required=True)
    resolution.add_argument('--output', required=True)
    preparation = commands.add_parser('prepare')
    preparation.add_argument('snapshot')
    preparation.add_argument('--snapshot-id', required=True)
    preparation.add_argument('--destination', required=True)
    for item in (publication, resolution, preparation):
        item.add_argument('--local-platform', choices=('reef', 'sf_hpc'), required=True)
    for item in (publication, preparation):
        item.add_argument('--verification', choices=('full', 'fingerprint', 'reference-only', 'size-mtime'), required=True)
    for item in (publication, listing, resolution):
        item.add_argument('--catalog-url', required=True)
        item.add_argument('--catalog-token-env', default='CATALOG_API_TOKEN')
    args = parser.parse_args(argv)
    client = None
    try:
        if args.command == 'prepare':
            result = prepare(read_document(Path(args.snapshot).absolute().parent, Path(args.snapshot).name)[0], args.snapshot_id, args.destination,
                             platform=args.local_platform, verification=args.verification)
        else:
            from catalog_client import CatalogClient
            token = os.environ.get(args.catalog_token_env)
            if not token:
                raise ValueError('Missing catalog credential environment variable')
            client = CatalogClient(base_url=args.catalog_url, api_token=token)
            if args.command == 'publish':
                config = refs.read_json(args.publication)
                if set(config) != {'store', 'payload_locations', 'retention_assertion', 'catalog_records'}:
                    raise ValueError('Unexpected publication configuration fields')
                result = publish(refs.read_json(args.release), config['store'], config['payload_locations'],
                                 config['catalog_records'], config['retention_assertion'], client,
                                 platform=args.local_platform, verification=args.verification)
            elif args.command == 'list':
                result = versions(client, args.canonical_id, args.project)
            else:
                snapshot, result = resolve(client, args.canonical_id, args.release_id, args.project,
                                           platform=args.local_platform)
                refs.save(args.output, snapshot)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        print(json.dumps({'state': 'refused', 'error_type': type(error).__name__, 'reason': str(error)}))
        return 2
    finally:
        if client is not None:
            client.close()


if __name__ == '__main__':
    raise SystemExit(main())
