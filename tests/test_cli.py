import argparse
import subprocess
import sys

from tsflash import cli


def test_module_entry_point_propagates_exit_code(tmp_path):
    # run_tui returns 1 for a missing image; `python -m tsflash` must pass it on
    result = subprocess.run(
        [sys.executable, "-m", "tsflash", "tui", str(tmp_path / "missing.img")],
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    assert "does not exist" in result.stderr


def _flash_args(image_file, target):
    return argparse.Namespace(image_path=str(image_file), target=target,
                              block_size="4M", non_interactive=True)


def test_flash_refuses_system_disk(image_file, monkeypatch):
    monkeypatch.setattr(cli, "validate_block_device", lambda path: (True, None))
    monkeypatch.setattr(cli, "disk_for_device", lambda path: {"mmcblk0"})
    monkeypatch.setattr(cli, "get_system_disks", lambda: {"mmcblk0"})

    def must_not_flash(*args, **kwargs):
        raise AssertionError("system disk must not be flashed")

    monkeypatch.setattr(cli, "flash_image", must_not_flash)

    assert cli.cmd_flash(_flash_args(image_file, "/dev/mmcblk0p2")) == 1


def test_flash_allows_other_disk(image_file, monkeypatch):
    monkeypatch.setattr(cli, "validate_block_device", lambda path: (True, None))
    monkeypatch.setattr(cli, "disk_for_device", lambda path: {"sda"})
    monkeypatch.setattr(cli, "get_system_disks", lambda: {"mmcblk0"})
    flashed = []
    monkeypatch.setattr(cli, "flash_image", lambda image, target, *a, **k: flashed.append(target))

    assert cli.cmd_flash(_flash_args(image_file, "/dev/sda")) == 0
    assert flashed == ["/dev/sda"]
