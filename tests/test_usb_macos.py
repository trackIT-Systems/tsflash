"""Tests for the macOS I/O Registry backend, using synthetic `ioreg -a` output
modelled on a real dock (Microchip USB4206/USB7206 hub pair)."""

import plistlib
import subprocess

import pytest

from tsflash import usb_macos
from tsflash.usb_macos import enumerate_ports, parse_ioreg, unify_by_container_id

DOCK = "00000000-0000-001e-3503-191705000802"
NESTED = "00000000-0000-001e-3503-191705000602"


def port_node(cls, number, location, *children):
    return {
        "IOObjectClass": cls,
        "IORegistryEntryName": cls,
        "locationID": location,
        "usb-port-number": number.to_bytes(4, "little"),
        "port": number.to_bytes(4, "little"),
        "IORegistryEntryChildren": list(children),
    }


def device(name, vendor, product, location, *children, container=None, serial=None):
    node = {
        "IOObjectClass": "IOUSBHostDevice",
        "IORegistryEntryName": name,
        "USB Product Name": name,
        "USB Vendor Name": "Vendor",
        "idVendor": vendor,
        "idProduct": product,
        "locationID": location,
        "IORegistryEntryChildren": list(children),
    }
    if container:
        node["kUSBContainerID"] = container
    if serial:
        node["USB Serial Number"] = serial
    return node


def hub_driver(cls, *ports):
    return {"IOObjectClass": cls, "IORegistryEntryChildren": list(ports)}


def media(bsd_name, size, whole=True, *children):
    return {"IOObjectClass": "IOMedia", "BSD Name": bsd_name, "Size": size, "Whole": whole,
            "IORegistryEntryChildren": list(children)}


def storage_stack(*media_nodes):
    """Interface -> mass storage driver -> block storage driver -> media."""
    return {"IOObjectClass": "IOUSBHostInterface", "idVendor": 1, "locationID": 1,
            "IORegistryEntryChildren": [
                {"IOObjectClass": "IOUSBMassStorageDriver", "IORegistryEntryChildren": [
                    {"IOObjectClass": "IOBlockStorageDriver",
                     "IORegistryEntryChildren": list(media_nodes)}]}]}


def card_with_apfs(disk, size):
    """A disk with a partition holding an APFS container (synthesized whole disk)."""
    return media(disk, size, True,
                 media(f"{disk}s1", size, False,
                       {"IOObjectClass": "AppleAPFSContainerScheme", "IORegistryEntryChildren": [
                           media("disk99", size, True)]}))


def registry():
    """Controller 0 with a USB 2.0 and a USB 3.0 root port, each with one half of the dock."""
    usb2_hub = device(
        "USB4206 Smart Hub", 0x0424, 0x4206, 0x00100000,
        hub_driver("AppleUSB20Hub",
                   port_node("AppleUSB20HubPort", 1, 0x00110000),
                   port_node("AppleUSB20HubPort", 2, 0x00120000,
                             device("Card Reader", 0x0bda, 0x0158, 0x00120000,
                                    storage_stack(card_with_apfs("disk5", 8 << 30)))),
                   port_node("AppleUSB20HubPort", 3, 0x00130000),
                   port_node("AppleUSB20HubPort", 4, 0x00140000,
                             device("Nested USB2", 0x0424, 0x4216, 0x00140000,
                                    hub_driver("AppleUSB20Hub",
                                               port_node("AppleUSB20HubPort", 1, 0x00141000)),
                                    container=NESTED)),
                   port_node("AppleUSB20HubPort", 5, 0x00150000,
                             device("RPi", 0x0a5c, 0x2711, 0x00150000))),
        container=DOCK)
    usb3_hub = device(
        "USB7206 Smart Hub", 0x0424, 0x7206, 0x00200000,
        hub_driver("AppleUSB30Hub",
                   port_node("AppleUSB30HubPort", 1, 0x00210000),
                   port_node("AppleUSB30HubPort", 2, 0x00220000),
                   port_node("AppleUSB30HubPort", 3, 0x00230000,
                             device("Fast Reader", 0x05e3, 0x0749, 0x00230000,
                                    storage_stack(media("disk6", 32 << 30)), serial="R1")),
                   port_node("AppleUSB30HubPort", 4, 0x00240000,
                             device("Nested USB3", 0x0424, 0x7216, 0x00240000,
                                    hub_driver("AppleUSB30Hub",
                                               port_node("AppleUSB30HubPort", 1, 0x00241000,
                                                         device("SSD", 0x174c, 0x55aa, 0x00241000,
                                                                storage_stack(media("disk7", 1 << 30))))),
                                    container=NESTED))),
        container=DOCK)
    controller = {
        "IOObjectClass": "AppleT8132USBXHCI",
        "locationID": 0,
        "IORegistryEntryChildren": [
            port_node("AppleUSB20XHCIARMPort", 1, 0x00100000, usb2_hub),
            port_node("AppleUSB30XHCIARMPort", 2, 0x00200000, usb3_hub),
        ],
    }
    second_controller = {
        "IOObjectClass": "AppleT8132USBXHCI",
        "locationID": 0x01000000,
        "IORegistryEntryChildren": [port_node("AppleUSB20XHCIARMPort", 1, 0x01100000)],
    }
    return [controller, second_controller]


@pytest.fixture
def ioreg_data():
    return plistlib.dumps(registry(), fmt=plistlib.FMT_XML)


def test_parse_names_ports_like_libusb(ioreg_data):
    raw = parse_ioreg(ioreg_data)
    assert "0-1" in raw and "0-2" in raw and "1-1" in raw
    assert "0-1.4.1" in raw and "0-2.4.1" in raw
    assert raw["0-1.1"]["vendor_id"] is None          # empty port is listed
    assert raw["0-1.5"]["vendor_id"] == "0x0a5c"
    assert raw["0-1.5"]["product_id"] == "0x2711"


def test_disks_belong_to_their_own_device(ioreg_data):
    raw = parse_ioreg(ioreg_data)
    # Partitions and the synthesized APFS container (disk99) are not separate devices
    assert raw["0-1.2"]["block_devices"] == ["/dev/disk5"]
    # Hubs do not own the disks of devices below them
    assert raw["0-1"]["block_devices"] == []
    assert raw["0-2.4"]["block_devices"] == []
    assert raw["0-2.4.1"]["block_devices"] == ["/dev/disk7"]


def test_unify_merges_hub_halves_by_container_id(ioreg_data):
    ports = unify_by_container_id(parse_ioreg(ioreg_data))

    assert not any(p.startswith("0-2") for p in ports)  # USB 3.0 half folded in
    assert sorted(p for p in ports if p.startswith("0-1")) == [
        "0-1", "0-1.1", "0-1.2", "0-1.3", "0-1.4", "0-1.4.1", "0-1.5"]
    assert ports["0-1.2"]["block_devices"] == ["/dev/disk5"]   # USB 2.0 device
    assert ports["0-1.3"]["block_devices"] == ["/dev/disk6"]   # USB 3.0 device
    assert ports["0-1.3"]["serial"] == "R1"
    assert ports["0-1.4.1"]["block_devices"] == ["/dev/disk7"]  # nested hub pair
    assert ports["0-1"]["product"] == "USB7206 Smart Hub"        # USB 3.0 info preferred
    assert all(not k.startswith("_") for info in ports.values() for k in info)


def test_ambiguous_container_ids_are_not_merged(ioreg_data):
    raw = parse_ioreg(ioreg_data)
    raw["1-1"]["_container_id"] = DOCK   # a third hub claims the same container ID
    raw["1-1.1"] = dict(raw["1-1"], _container_id=None)
    ports = unify_by_container_id(raw)
    assert "0-2" in ports                # no unambiguous pair -> left as is


def test_enumerate_ports_and_disk_sizes(ioreg_data):
    def fake_run(cmd, **kwargs):
        assert cmd[0] == "ioreg"
        return subprocess.CompletedProcess(cmd, 0, stdout=ioreg_data)

    ports = enumerate_ports(run=fake_run)
    assert ports["0-1.3"]["block_devices"] == ["/dev/disk6"]
    assert usb_macos.disk_size("/dev/disk6") == 32 << 30
    assert usb_macos.disk_size("/dev/rdisk5") == 8 << 30
    assert usb_macos.disk_size("/dev/disk99") is None   # not a USB device's disk


def test_enumerate_ports_reports_ioreg_failure():
    def failing_run(cmd, **kwargs):
        raise subprocess.CalledProcessError(1, cmd)

    with pytest.raises(RuntimeError, match="ioreg"):
        enumerate_ports(run=failing_run)


def test_enumerate_dispatches_on_macos(monkeypatch, ioreg_data):
    from tsflash import usb
    monkeypatch.setattr(usb.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(usb_macos, "enumerate_ports", lambda: {"0-1": {"block_devices": []}})
    assert usb.enumerate_all_usb_ports() == {"0-1": {"block_devices": []}}
