"""macOS specific flashing and disk detection."""

import plistlib
import subprocess
import sys

import pytest

from tsflash import flash, validators
from tsflash.flash import flash_image, raw_device_path, unmount_device


@pytest.fixture
def darwin(monkeypatch):
    monkeypatch.setattr(flash.platform, "system", lambda: "Darwin")


def test_raw_device_path(darwin):
    assert raw_device_path("/dev/disk4") == "/dev/rdisk4"
    assert raw_device_path("/dev/rdisk4") == "/dev/rdisk4"
    assert raw_device_path("/dev/disk4s1") == "/dev/disk4s1"   # partitions unchanged


def test_raw_device_path_only_on_macos(monkeypatch):
    monkeypatch.setattr(flash.platform, "system", lambda: "Linux")
    assert raw_device_path("/dev/disk4") == "/dev/disk4"


def test_unmount_uses_diskutil_unmount_disk(darwin, monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(flash.subprocess, "run", fake_run)
    assert unmount_device("/dev/rdisk4") is True
    assert calls == [["diskutil", "unmountDisk", "/dev/disk4"]]


def test_unmount_failure(darwin, monkeypatch):
    monkeypatch.setattr(flash.subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(
        cmd, 1, stdout="", stderr="Unmount of disk4 failed: at least one volume could not be unmounted"))
    assert unmount_device("/dev/disk4") is False


def test_last_chunk_is_padded_to_sector(tmp_path, image_file, mapped_image, monkeypatch):
    """Raw devices only take whole sectors: the final partial chunk gets zero padding."""
    monkeypatch.setattr(flash, "device_geometry", lambda fd: (0, 512))
    target = tmp_path / "target"
    target.touch()
    progress = []

    flash_image(mapped_image, str(target), "300K", non_interactive=True,
                progress_callback=lambda *a: progress.append(a))

    data = image_file.read_bytes()
    written = target.read_bytes()
    assert len(written) % 512 == 0 and len(written) - len(data) < 512
    assert written[:len(data)] == data and written[len(data):] == b"\0" * (len(written) - len(data))
    assert progress[-1][0] == len(data)          # progress counts image bytes, not padding
    assert progress[-1][2] == 100.0


def test_padded_image_must_fit(tmp_path, mapped_image, monkeypatch):
    image_size = len(mapped_image)
    monkeypatch.setattr(flash, "device_geometry", lambda fd: (image_size, 512))  # no room for padding
    target = tmp_path / "target"
    target.touch()
    with pytest.raises(IOError, match="larger than target"):
        flash_image(mapped_image, str(target), "4M", non_interactive=True)


DISKUTIL = {
    "/": {"DeviceIdentifier": "disk3s3s1", "ParentWholeDisk": "disk3", "WholeDisk": False},
    "disk3": {"DeviceIdentifier": "disk3", "ParentWholeDisk": "disk3", "WholeDisk": True,
              "APFSPhysicalStores": [{"APFSPhysicalStore": "disk0s2"}]},
    "disk0s2": {"DeviceIdentifier": "disk0s2", "ParentWholeDisk": "disk0", "WholeDisk": False},
    "disk0": {"DeviceIdentifier": "disk0", "ParentWholeDisk": "disk0", "WholeDisk": True},
    "/dev/rdisk4s1": {"DeviceIdentifier": "disk4s1", "ParentWholeDisk": "disk4", "WholeDisk": False},
    "disk4": {"DeviceIdentifier": "disk4", "ParentWholeDisk": "disk4", "WholeDisk": True},
}


def fake_diskutil(cmd, **kwargs):
    info = DISKUTIL.get(cmd[-1])
    if info is None:
        return subprocess.CompletedProcess(cmd, 1, stdout=b"")
    return subprocess.CompletedProcess(cmd, 0, stdout=plistlib.dumps(info))


def test_physical_disks_resolve_apfs_containers():
    assert validators._macos_physical_disks("/", run=fake_diskutil) == {"disk0"}
    assert validators._macos_physical_disks("/dev/rdisk4s1", run=fake_diskutil) == {"disk4"}
    assert validators._macos_physical_disks("/dev/disk42", run=fake_diskutil) == set()


def test_system_disks_are_cached(monkeypatch):
    monkeypatch.setattr(validators.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(validators, "_macos_system_disks", None)
    calls = []

    def fake_physical(path, run=None, _depth=0):
        calls.append(path)
        return {"disk0"}

    monkeypatch.setattr(validators, "_macos_physical_disks", fake_physical)
    assert validators.get_system_disks() == {"disk0"}
    assert validators.get_system_disks() == {"disk0"}
    assert calls == ["/", "/System/Volumes/Data"]


@pytest.mark.skipif(sys.platform != "darwin", reason="needs macOS hdiutil")
def test_flash_to_attached_disk_image(tmp_path, image_file, mapped_image):
    """End to end on a real macOS raw device: a disk image attached without mounting."""
    backing = tmp_path / "disk.img"
    with open(backing, "wb") as f:
        f.truncate(4 << 20)
    out = subprocess.run(["hdiutil", "attach", "-nomount", "-imagekey",
                          "diskimage-class=CRawDiskImage", str(backing)],
                         capture_output=True, text=True, check=True).stdout
    device = out.split()[0]
    try:
        # Safety: only ever write to the disk image just attached
        info = plistlib.loads(subprocess.run(["diskutil", "info", "-plist", device],
                                             capture_output=True, check=True).stdout)
        assert info["BusProtocol"] == "Disk Image" and info["Internal"] is False

        flash_image(mapped_image, device, "4M", non_interactive=True)

        data = image_file.read_bytes()
        with open(raw_device_path(device), "rb", buffering=0) as f:
            written = f.read(len(data) + 511)
        assert written[:len(data)] == data
        assert written[len(data):] == b"\0" * 511
    finally:
        subprocess.run(["hdiutil", "detach", "-quiet", device], check=False)
