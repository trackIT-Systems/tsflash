"""Tests for the Linux sysfs backend, using a synthetic sysfs tree in tmp_path.

The tree models a typical setup: a USB 2.0/3.0 hub pair (buses 1 and 2) with
a card reader that enumerates on the USB 3.0 bus only, plus a second, plain
USB 2.0 device. Block devices are linked the way the kernel does it:
/sys/block/<name>/device points deep into the USB device's interface subtree.
"""

import pytest

from tsflash import usb
from tsflash.usb import enumerate_all_usb_ports, format_usb_output

HUB_VENDOR = "2109"


class FakeSysfs:
    def __init__(self, root):
        self.devices = root / "sys" / "bus" / "usb" / "devices"
        self.sys_block = root / "sys" / "block"
        self.dev = root / "dev"
        self.scsi = root / "sys" / "devices" / "scsi"
        for path in (self.devices, self.sys_block, self.dev, self.scsi):
            path.mkdir(parents=True)

    def add(self, name, maxchild=0, vendor=None, product=None, manufacturer=None,
            product_name=None, serial=None):
        path = self.devices / name
        path.mkdir()
        (path / "maxchild").write_text(f"{maxchild}\n")
        for attr, value in (("idVendor", vendor), ("idProduct", product),
                            ("manufacturer", manufacturer), ("product", product_name),
                            ("serial", serial)):
            if value is not None:
                (path / attr).write_text(f"{value}\n")
        return path

    def add_disk(self, name, *usb_path, dev_node=True):
        """Link /sys/block/<name>/device to a SCSI device below the given USB
        path components (e.g. "usb2", "2-1", "2-1.1")."""
        target = self.scsi.joinpath(*usb_path, f"{usb_path[-1]}:1.0", "host0",
                                    "target0:0:0", "0:0:0:0")
        target.mkdir(parents=True)
        (self.sys_block / name).mkdir()
        (self.sys_block / name / "device").symlink_to(target)
        if dev_node:
            (self.dev / name).touch()


@pytest.fixture
def sysfs(tmp_path, monkeypatch):
    fake = FakeSysfs(tmp_path)
    monkeypatch.setattr(usb.platform, "system", lambda: "Linux")
    monkeypatch.setattr(usb, "USB_SYSFS_PATH", fake.devices)
    monkeypatch.setattr(usb, "SYS_BLOCK_PATH", fake.sys_block)
    monkeypatch.setattr(usb, "DEV_PATH", fake.dev)
    return fake


@pytest.fixture
def hub_pair(sysfs):
    """Root hubs with one port each, and a 4-port hub on USB 2.0 and 3.0."""
    sysfs.add("usb1", maxchild=1, vendor="1d6b", product="0002")
    sysfs.add("usb2", maxchild=1, vendor="1d6b", product="0003")
    sysfs.add("1-1", maxchild=4, vendor=HUB_VENDOR, product="2817")
    sysfs.add("2-1", maxchild=4, vendor=HUB_VENDOR, product="0817")
    return sysfs


def test_enumerates_every_physical_port_once(hub_pair):
    ports = enumerate_all_usb_ports()
    assert sorted(ports) == ["1-1", "1-1.1", "1-1.2", "1-1.3", "1-1.4"]
    assert ports["1-1"]["vendor_id"] == f"0x{HUB_VENDOR}"


def test_empty_port_has_no_device_info(hub_pair):
    assert enumerate_all_usb_ports()["1-1.3"] == {
        "vendor_id": None, "product_id": None, "manufacturer": None,
        "product": None, "serial": None, "block_devices": []}


def test_usb3_only_card_reader_is_merged_into_usb2_port(hub_pair):
    hub_pair.add("2-1.2", vendor="0BDA", product="0316", manufacturer="Generic",
                 product_name="USB3.0-CRW", serial="2014")
    hub_pair.add_disk("sda", "usb2", "2-1", "2-1.2")

    reader = enumerate_all_usb_ports()["1-1.2"]
    assert reader == {"vendor_id": "0x0bda", "product_id": "0x0316",
                      "manufacturer": "Generic", "product": "USB3.0-CRW",
                      "serial": "2014", "block_devices": [str(hub_pair.dev / "sda")]}


def test_hub_does_not_claim_block_devices_of_its_children(hub_pair):
    hub_pair.add("2-1.2", vendor="0bda", product="0316")
    hub_pair.add_disk("sda", "usb2", "2-1", "2-1.2")

    assert enumerate_all_usb_ports()["1-1"]["block_devices"] == []


def test_each_port_gets_its_own_block_device(hub_pair):
    for port, disk in (("1-1.1", "sda"), ("1-1.4", "sdb")):
        hub_pair.add(port, vendor="05e3", product="0751")
        hub_pair.add_disk(disk, "usb1", "1-1", port)

    ports = enumerate_all_usb_ports()
    assert ports["1-1.1"]["block_devices"] == [str(hub_pair.dev / "sda")]
    assert ports["1-1.4"]["block_devices"] == [str(hub_pair.dev / "sdb")]
    assert ports["1-1.2"]["block_devices"] == []


def test_block_device_without_dev_node_is_ignored(hub_pair):
    hub_pair.add("1-1.1", vendor="05e3", product="0751")
    hub_pair.add_disk("sda", "usb1", "1-1", "1-1.1", dev_node=False)

    assert enumerate_all_usb_ports()["1-1.1"]["block_devices"] == []


def test_block_subdirectory_on_usb_device(hub_pair):
    path = hub_pair.add("1-1.1", vendor="05e3", product="0751")
    (path / "block" / "sdc").mkdir(parents=True)
    (hub_pair.dev / "sdc").touch()

    assert enumerate_all_usb_ports()["1-1.1"]["block_devices"] == [str(hub_pair.dev / "sdc")]


def test_nested_hubs_are_enumerated(hub_pair):
    hub_pair.add("1-1.4", maxchild=2, vendor="05e3", product="0610")
    hub_pair.add("1-1.4.2", vendor="0a5c", product="2712")

    ports = enumerate_all_usb_ports()
    assert {"1-1.4.1", "1-1.4.2"} <= set(ports)
    assert usb.is_rpiboot_device(ports["1-1.4.2"])
    assert usb.find_first_usb_hub(ports) == "1-1.4"


def test_hub_without_usb3_counterpart_is_kept(sysfs):
    sysfs.add("usb1", maxchild=1)
    sysfs.add("usb2", maxchild=1)
    sysfs.add("1-1", maxchild=2, vendor=HUB_VENDOR)
    sysfs.add("2-1", maxchild=2, vendor="1234")  # different hub on the USB 3.0 bus

    assert sorted(enumerate_all_usb_ports()) == [
        "1-1", "1-1.1", "1-1.2", "2-1", "2-1.1", "2-1.2"]


def test_unparseable_entries_are_skipped(sysfs):
    sysfs.add("usb1", maxchild=1)
    sysfs.add("usbX", maxchild=1)
    (sysfs.add("1-1") / "maxchild").write_text("garbage\n")

    assert sorted(enumerate_all_usb_ports()) == ["1-1"]


def test_missing_sysfs_raises(sysfs, monkeypatch):
    monkeypatch.setattr(usb, "USB_SYSFS_PATH", sysfs.devices / "missing")
    with pytest.raises(RuntimeError, match="sysfs not found"):
        enumerate_all_usb_ports()


def test_unsupported_platform_raises(monkeypatch):
    monkeypatch.setattr(usb.platform, "system", lambda: "Windows")
    with pytest.raises(RuntimeError, match="not supported on Windows"):
        enumerate_all_usb_ports()


def test_format_usb_output(hub_pair):
    hub_pair.add("1-1.1", vendor="0bda", product="0316", manufacturer="Generic",
                 product_name="CRW")
    hub_pair.add_disk("sda", "usb1", "1-1", "1-1.1")
    ports = enumerate_all_usb_ports()

    lines = format_usb_output(ports).splitlines()
    assert f"1-1.1: 0x0bda:0x0316 Generic CRW [{hub_pair.dev / 'sda'}]" in lines
    assert "1-1.2: (empty)" in lines
    assert '"1-1.1"' in format_usb_output(ports, json_output=True)
