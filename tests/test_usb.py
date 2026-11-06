from tsflash import usb
from tsflash.usb import block_device_size, unify_ports


def port(vendor=None, block_devices=()):
    return {"vendor_id": vendor, "product_id": "0x0001" if vendor else None,
            "manufacturer": None, "product": None, "serial": None,
            "block_devices": list(block_devices)}


def test_unify_ports_merges_usb2_and_usb3_hub():
    raw = {
        "1-1": port("0x2109"), "1-1.1": port(), "1-1.2": port(),
        "2-1": port("0x2109"), "2-1.1": port("0x0bda", ["/dev/sda"]), "2-1.2": port(),
    }
    unified = unify_ports(raw)
    assert sorted(unified) == ["1-1", "1-1.1", "1-1.2"]
    assert unified["1-1.1"]["block_devices"] == ["/dev/sda"]


def test_unify_ports_keeps_usb3_port_without_usb2_counterpart():
    raw = {
        "1-1": port("0x2109"), "1-1.1": port(),
        "2-1": port("0x2109"), "2-1.1": port(), "2-1.5": port("0x0bda", ["/dev/sdb"]),
    }
    unified = unify_ports(raw)
    assert "2-1.5" in unified
    assert unified["2-1.5"]["block_devices"] == ["/dev/sdb"]


def test_block_device_size(tmp_path, monkeypatch):
    (tmp_path / "sda").mkdir()
    (tmp_path / "sda" / "size").write_text("2048\n")
    (tmp_path / "sdb").mkdir()
    (tmp_path / "sdb" / "size").write_text("0\n")
    monkeypatch.setattr(usb.platform, "system", lambda: "Linux")
    monkeypatch.setattr(usb, "SYSFS_CLASS_BLOCK", tmp_path)

    assert block_device_size("/dev/sda") == 2048 * 512
    assert block_device_size("/dev/sdb") == 0
    assert block_device_size("/dev/sdc") is None
