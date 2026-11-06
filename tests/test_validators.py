import io
import os

import pytest

from tsflash import validators
from tsflash.validators import (
    _is_device_or_partition,
    _unescape_mount_field,
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
