"""Verify offline CLI behavior and non-secret configuration without a service."""

import json
import subprocess
import sys

import pytest

from zarrsum.releases import Store
from zarrsum.remote_cli import main


def test_alias_configuration_has_only_references_and_removal_is_local(tmp_path, capsys):
    config = tmp_path / "remotes.json"
    args = [
        "remote",
        "--config",
        str(config),
        "add",
        "sample",
        "--endpoint",
        "https://objects.example.test",
        "--bucket",
        "release-test",
        "--prefix",
        "retained",
        "--region",
        "test-region",
        "--profile",
        "private-profile",
        "--rclone-remote",
        "payload",
        "--rclone-config",
        "/private/rclone.conf",
    ]
    assert main(args) == 0
    value = json.loads(config.read_bytes())["sample"]
    assert value["profile"] == "private-profile" and not any(
        "secret" in key or "access_key" in key for key in value
    )
    assert main(["remote", "--config", str(config), "remove", "sample"]) == 0
    assert json.loads(config.read_bytes()) == {}


def test_offline_checkout_preserves_existing_and_detects_change(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"correct")
    store = Store(tmp_path / "store")
    rid = store.publish(
        source,
        {"version": 2, "partitions": 4, "files": ["data"], "grids": []},
        source_id="source",
        assume_immutable=True,
    )["release_id"]
    destination = tmp_path / "checkout"
    evidence = tmp_path / "evidence"
    assert (
        main(
            [
                "checkout",
                str(store.path),
                rid,
                str(destination),
                "--evidence",
                str(evidence),
            ]
        )
        == 0
    )
    (destination / "data").write_bytes(b"CORRUPT")
    with pytest.raises(ValueError, match="destination bytes mismatch"):
        store.verify(rid, destination)
    with pytest.raises(SystemExit):
        main(
            [
                "checkout",
                str(store.path),
                rid,
                str(destination),
                "--evidence",
                str(tmp_path / "refused"),
            ]
        )
    assert (destination / "data").read_bytes() == b"CORRUPT"


def test_default_import_needs_no_hermes():
    code = """
import importlib.abc
import sys
class AbsentHermes(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'hermes_core' or fullname.startswith('hermes_core.'):
            raise AssertionError('Default import attempted to load Hermes')
sys.meta_path.insert(0, AbsentHermes())
import zarrsum.remote_cli
"""
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.parametrize("timeout", ["0", "-1", "nan", "inf"])
def test_invalid_transfer_deadline_rejected_before_remote_access(tmp_path, timeout):
    with pytest.raises(SystemExit) as stopped:
        main(
            [
                "push",
                str(tmp_path / "store"),
                "0" * 64,
                "--evidence",
                str(tmp_path / "evidence"),
                "--remote",
                "test",
                "--remotes",
                str(tmp_path / "missing-config"),
                "--transfer-timeout",
                timeout,
            ]
        )
    assert stopped.value.code == 2
    assert not (tmp_path / "evidence").exists()
