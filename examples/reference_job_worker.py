"""Prepare a frozen reference before entering the owned Katamari loader fixture."""
import argparse
import json
from pathlib import Path

from zarrsum import reference_workflow as flow
from zarrsum import revision_refs as refs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('spec')
    args = parser.parse_args()
    spec = refs.read_json(args.spec)
    record = Path(spec['record_dir'])
    try:
        snapshot = flow.read_document(Path(spec['snapshot']).parent, Path(spec['snapshot']).name)[0]
        prepared = flow.prepare(snapshot, spec['snapshot_id'], spec['destination'],
                                platform='reef', verification=spec['verification'])
    except Exception as error:
        print(json.dumps({'state': 'preparation-refused', 'reason': str(error)}))
        return 2
    refs.save(Path(args.spec).with_suffix('.prepared.json'), prepared)
    from metadata_release_katamari import run
    configured = {key: spec[key] for key in ('record_dir', 'batches', 'expected_labels')}
    configured.update({key: spec[key] for key in ('started', 'finish_gate', 'slow') if key in spec})
    configured.update(release_id=prepared['release_id'], input_root=prepared['input_root'])
    run(configured)
    refs.save(record / 'reference-workflow.json', {
        'release_id': prepared['release_id'], 'snapshot_id': spec['snapshot_id'],
        'verification': prepared['verification'], 'catalog_record': prepared['catalog_record'],
        'payload_bytes_read_in_preparation': prepared['payload_bytes_read'],
        'catalog_lookup_in_worker': False})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
