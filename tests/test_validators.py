import io
import os

import pytest

from tsflash import validators
from tsflash.validators import (
    _is_device_or_partition,
    _unescape_mount_field,
    disk_for_device,
    get_system_disks,
    is_mounted,
)


@pytest.mark.parametrize("name, base, expected", [
    ("sda", "sda", True),
    ("sda1", "sda", True),
    ("sda12", "sda", True),
    ("sdaa", "sda", False),
    ("sdaa1", "sda", False),
    ("sdb1", "sda", False),
    ("mmcblk0", "mmcblk0", True),
    ("mmcblk0p2", "mmcblk0", True),
    ("mmcblk01", "mmcblk0", False),
    ("mmcblk0boot0", "mmcblk0", False),
    ("nvme0n1p1", "nvme0n1", True),
    ("disk4s1", "disk4", True),
    ("disk4s1s1", "disk4", True),
    ("disk41s1", "disk4", False),
])
def test_is_device_or_partition(name, base, expected):
    assert _is_device_or_partition(name, base) is expected


def test_unescape_mount_field():
    assert _unescape_mount_field(r"/media/my\040card") == "/media/my card"
    assert _unescape_mount_field("/plain") == "/plain"


PROC_MOUNTS = """\
/dev/mmcblk0p2 / ext4 rw 0 0
/dev/mmcblk0p1 /boot/firmware vfat rw 0 0
/dev/sda1 /media/my\\040card vfat rw 0 0
/dev/sdaa1 /media/other vfat rw 0 0
tmpfs /run tmpfs rw 0 0
"""


@pytest.fixture
def fake_proc_mounts(monkeypatch):
    real_open = open

    def fake_open(path, *args, **kwargs):
        if path == "/proc/mounts":
            return io.StringIO(PROC_MOUNTS)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(validators, "open", fake_open, raising=False)


def test_is_mounted_matches_only_own_partitions(fake_proc_mounts):
    assert is_mounted("/dev/sda") == ["/media/my card"]
    assert is_mounted("/dev/sdaa") == ["/media/other"]
    assert is_mounted("/dev/mmcblk0") == ["/", "/boot/firmware"]
    assert is_mounted("/dev/sdb") == []


@pytest.fixture
def fake_sysfs(tmp_path, monkeypatch):
    """
    Minimal sysfs tree:
      sda (disk) with sda1, sda2 (partitions)
      sdb (disk) with sdb1, and dm-0 (device mapper) on top of sdb1
    """
    devices = tmp_path / "devices"
    class_block = tmp_path / "class" / "block"
    dev_block = tmp_path / "dev" / "block"
    class_block.mkdir(parents=True)
    dev_block.mkdir(parents=True)

    def add(path, partition=False, slaves=()):
        d = devices / path
        d.mkdir(parents=True)
        if partition:
            (d / "partition").write_text("1\n")
        if slaves:
            (d / "slaves").mkdir()
            for slave in slaves:
                (d / "slaves" / slave).symlink_to(class_block / slave)
        (class_block / d.name).symlink_to(d)
        return d

    add("pci0/usb1/host0/block/sda")
    add("pci0/usb1/host0/block/sda/sda1", partition=True)
    add("pci0/usb1/host0/block/sda/sda2", partition=True)
    add("pci0/usb2/host1/block/sdb")
    add("pci0/usb2/host1/block/sdb/sdb1", partition=True)
    add("virtual/block/dm-0", slaves=["sdb1"])

    monkeypatch.setattr(validators.platform, "system", lambda: "Linux")
    monkeypatch.setattr(validators, "SYSFS_CLASS_BLOCK", str(class_block))
    monkeypatch.setattr(validators, "SYSFS_DEV_BLOCK", str(dev_block))
    return tmp_path


def test_disk_for_device(fake_sysfs):
    assert disk_for_device("/dev/sda") == {"sda"}
    assert disk_for_device("/dev/sda2") == {"sda"}
    assert disk_for_device("/dev/dm-0") == {"sdb"}
    assert disk_for_device("/dev/unknown") == set()


def _map_mount_point(fake_sysfs, mount_point, sysfs_name):
    """Point the device number of mount_point at a block device in the fake sysfs."""
    st = os.stat(mount_point)
    link = fake_sysfs / "dev" / "block" / f"{os.major(st.st_dev)}:{os.minor(st.st_dev)}"
    link.symlink_to(fake_sysfs / "class" / "block" / sysfs_name)


def test_get_system_disks_resolves_partition_to_disk(fake_sysfs, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    _map_mount_point(fake_sysfs, root, "sda2")

    assert get_system_disks((str(root),)) == {"sda"}


def test_get_system_disks_through_device_mapper(fake_sysfs, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    _map_mount_point(fake_sysfs, root, "dm-0")

    assert get_system_disks((str(root),)) == {"sdb"}


def test_get_system_disks_skips_missing_mount_points(fake_sysfs, tmp_path):
    assert get_system_disks((str(tmp_path / "missing"),)) == set()
