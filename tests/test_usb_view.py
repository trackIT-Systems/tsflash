import argparse
import json

from rich.console import Console

from tsflash import cli
from tsflash.usb import find_first_usb_hub, port_sort_key
from tsflash.usb_view import print_usb_tree

EMPTY = {"vendor_id": None, "product_id": None, "manufacturer": None, "product": None,
         "serial": None, "block_devices": []}


def dev(name, vid="0x0bda", pid="0x0158", *block_devices):
    return dict(EMPTY, vendor_id=vid, product_id=pid, product=name, block_devices=list(block_devices))


PORTS = {
    "1-1": dev("Hub", "0x2109", "0x2817"),
    "1-1.1": dev("Reader", "0x0bda", "0x0158", "/dev/sda"),
    "1-1.2": dev("Empty Reader", "0x0bda", "0x0158", "/dev/sdb"),
    "1-1.3": dev("BCM2711 Boot", "0x0a5c", "0x2711"),
    "1-1.4": dev("Dual Reader", "0x05e3", "0x0749", "/dev/sdc", "/dev/sdd"),
    "1-1.5": dev("SSD", "0x174c", "0x55aa", "/dev/sde"),
    "1-1.10": EMPTY,
}
SIZES = {"/dev/sda": 32_000_000_000, "/dev/sdb": 0, "/dev/sdc": 8 << 30, "/dev/sdd": 8 << 30,
         "/dev/sde": 512 << 30}


def render(ports=PORTS, default="1-1"):
    console = Console(width=200, record=True, force_terminal=False)
    print_usb_tree(ports, {"sde"}, SIZES.get, default, console=console)
    return console.export_text()


def line_of(text, port):
    return next(i for i, line in enumerate(text.splitlines()) if f"{port}  " in line)


def test_port_sort_key_is_numeric():
    assert sorted(["1-1.10", "1-1.9", "1-1", "1-2", "10-1"], key=port_sort_key) == [
        "1-1", "1-1.9", "1-1.10", "1-2", "10-1"]


def test_first_hub_uses_numeric_order():
    ports = {"1-10": EMPTY, "1-10.1": EMPTY, "1-9": EMPTY, "1-9.1": EMPTY}
    assert find_first_usb_hub(ports) == "1-9"


def test_tree_is_nested_and_sorted():
    text = render()
    assert line_of(text, "1-1.5") < line_of(text, "1-1.10")
    assert text.splitlines()[0].startswith("1-1  Hub")
    assert "├── 1-1.1  Reader" in text and "└── 1-1.10  –" in text


def test_tree_explains_devices():
    text = render()
    assert "hub, 6 ports" in text and "monitored by default" in text
    assert "/dev/sda  32.0 GB" in text
    assert "/dev/sdb  no medium" in text
    assert "USB boot mode" in text
    assert "several block devices on one port - not handled" in text
    assert "/dev/sde  system disk - never flashed" in text
    assert "0bda:0158" in text


def test_default_marker_only_on_default_hub():
    assert "monitored by default" not in render(default="2-1")


def test_cmd_usb_json_is_unchanged(monkeypatch, capsys):
    monkeypatch.setattr(cli, "enumerate_all_usb_ports", lambda: dict(PORTS, **{"2-1": EMPTY}))
    args = argparse.Namespace(all=False, port=None, json=True)
    assert cli.cmd_usb(args) == 0
    data = json.loads(capsys.readouterr().out)
    assert set(data) == {p for p in PORTS}           # first hub only
    assert data["1-1.1"]["block_devices"] == ["/dev/sda"]


def test_cmd_usb_text(monkeypatch, capsys):
    monkeypatch.setattr(cli, "enumerate_all_usb_ports", lambda: PORTS)
    monkeypatch.setattr(cli, "get_system_disks", lambda: {"sde"})
    monkeypatch.setattr(cli, "block_device_size", SIZES.get)
    assert cli.cmd_usb(argparse.Namespace(all=False, port="1-1.4", json=False)) == 0
    out = capsys.readouterr().out
    assert "1-1.4  Dual Reader" in out and "1-1.1" not in out
