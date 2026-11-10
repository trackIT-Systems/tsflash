"""USB port enumeration on macOS, based on the I/O Registry (`ioreg`).

The I/O Registry (IOService plane) contains, below each xHCI controller:

    <controller>                         locationID 0xBB000000
      <root port>  (...XHCI...Port)      usb-port-number N
        IOUSBHostDevice                  idVendor, idProduct, kUSBContainerID, ...
          AppleUSB20Hub / AppleUSB30Hub
            AppleUSB20HubPort / ...30... usb-port-number N   (also present when empty)
              IOUSBHostDevice ...
          ... IOMedia                    BSD Name "diskN", Whole, Size

Ports are named like on Linux and like libusb/rpiboot do on macOS:
"<bus>-<port>.<port>...", with bus = locationID >> 24 and the port numbers of
the hub ports along the way (e.g. "0-1.4.2").

USB 3 hubs appear twice (a USB 2.0 and a USB 3.0 half, on macOS usually as
different root ports of the same controller). Both halves report the same
Container ID, which is used to merge them so every physical port is listed once.
"""

import logging
import plistlib
import subprocess
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

IOREG_COMMAND = ["ioreg", "-a", "-l", "-w0", "-r", "-c", "AppleUSBXHCI"]

# Sizes (bytes) of the whole disks seen during the last enumeration, by BSD name
_disk_sizes: Dict[str, int] = {}


def _empty_port(usb3: bool = False) -> dict:
    return {
        "vendor_id": None,
        "product_id": None,
        "manufacturer": None,
        "product": None,
        "serial": None,
        "block_devices": [],
        "_usb3": usb3,
        "_container_id": None,
    }


def _port_number(node: dict) -> Optional[int]:
    """Port number of a port node (little-endian data), see libusb darwin_usb.c."""
    for key in ("usb-port-number", "port"):
        value = node.get(key)
        if isinstance(value, (bytes, bytearray)) and value:
            return int.from_bytes(value[:4], "little")
        if isinstance(value, int):
            return value
    return None


def _is_port_node(node: dict) -> bool:
    cls = node.get("IOObjectClass", "")
    return cls.endswith("Port") and "USB" in cls and _port_number(node) is not None


def _is_usb_device(node: dict) -> bool:
    cls = node.get("IOObjectClass", "")
    # IOUSBHostDevice, or a subclass of it (IOUSBHostInterface also has idVendor,
    # but its class name does not end in "Device")
    return cls == "IOUSBHostDevice" or (
        cls.endswith("Device") and "idVendor" in node and "locationID" in node)


def _children(node: dict) -> List[dict]:
    return node.get("IORegistryEntryChildren", []) or []


def _format_id(value) -> Optional[str]:
    return f"0x{value:04x}" if isinstance(value, int) else None


def _find_disks(node: dict, disks: List[Tuple[str, int]]) -> None:
    """
    Collect whole disks below a USB device, without descending into nested
    USB ports/devices (they own their disks) or into a found disk (its
    partitions and synthesized APFS containers are not separate devices).
    """
    for child in _children(node):
        if _is_port_node(child) or _is_usb_device(child):
            continue
        if child.get("IOObjectClass") == "IOMedia" and child.get("Whole") and child.get("BSD Name"):
            disks.append((child["BSD Name"], int(child.get("Size") or 0)))
            continue
        _find_disks(child, disks)


def _walk(node: dict, bus: int, parent_path: str, ports: Dict[str, dict]) -> None:
    """Walk the registry below a controller or USB device, recording ports."""
    for child in _children(node):
        if _is_port_node(child):
            number = _port_number(child)
            path = f"{parent_path}.{number}" if parent_path else f"{bus}-{number}"
            info = _empty_port(usb3="30" in child.get("IOObjectClass", ""))
            ports[path] = info
            for device in _children(child):
                if _is_usb_device(device):
                    _fill_device(device, info)
                    _walk(device, bus, path, ports)
        elif not _is_usb_device(child):
            _walk(child, bus, parent_path, ports)


def _fill_device(device: dict, info: dict) -> None:
    info["vendor_id"] = _format_id(device.get("idVendor"))
    info["product_id"] = _format_id(device.get("idProduct"))
    info["manufacturer"] = device.get("USB Vendor Name") or device.get("kUSBVendorString")
    info["product"] = device.get("USB Product Name") or device.get("kUSBProductString")
    info["serial"] = device.get("USB Serial Number") or device.get("kUSBSerialNumberString")
    info["_container_id"] = device.get("kUSBContainerID")
    disks: List[Tuple[str, int]] = []
    _find_disks(device, disks)
    for name, size in disks:
        _disk_sizes[name] = size
    info["block_devices"] = sorted(f"/dev/{name}" for name, _ in disks)


def parse_ioreg(data: bytes) -> Dict[str, dict]:
    """
    Parse `ioreg -a` plist output of the USB controllers into raw ports.

    Returns:
        dict: port string -> port info (including private "_usb3" and
        "_container_id" keys used for merging)
    """
    roots = plistlib.loads(data)
    if isinstance(roots, dict):
        roots = [roots]
    ports: Dict[str, dict] = {}
    for controller in roots:
        location = controller.get("locationID")
        if not isinstance(location, int):
            continue
        _walk(controller, location >> 24, "", ports)
    return ports


def _is_hub(path: str, ports: Dict[str, dict]) -> bool:
    prefix = path + "."
    return any(p.startswith(prefix) for p in ports)


def unify_by_container_id(raw_ports: Dict[str, dict]) -> Dict[str, dict]:
    """
    Merge the USB 2.0 and USB 3.0 halves of hubs into one entry per physical port.

    Hubs with the same Container ID are halves of one physical hub; the USB 2.0
    half's name is kept. Ports below paired hubs are matched by port number
    (the USB 3 spec requires both halves to number their ports identically).
    """
    by_container: Dict[str, List[str]] = {}
    for path, info in raw_ports.items():
        container = info.get("_container_id")
        if container and _is_hub(path, raw_ports):
            by_container.setdefault(container, []).append(path)

    aliases: Dict[str, str] = {}  # USB 3.0 hub path -> USB 2.0 hub path
    for paths in by_container.values():
        usb2 = [p for p in paths if not raw_ports[p]["_usb3"]]
        usb3 = [p for p in paths if raw_ports[p]["_usb3"]]
        if len(usb2) == 1 and len(usb3) == 1:
            aliases[usb3[0]] = usb2[0]

    def translate(path: str) -> str:
        # Replace the longest aliased hub prefix (nested hubs are paired too)
        best = None
        for alias in aliases:
            if path == alias or path.startswith(alias + "."):
                if best is None or len(alias) > len(best):
                    best = alias
        return aliases[best] + path[len(best):] if best else path

    unified: Dict[str, dict] = {}
    # USB 2.0 entries first, so they define the canonical entry
    for path in sorted(raw_ports, key=lambda p: raw_ports[p]["_usb3"]):
        info = raw_ports[path]
        target = translate(path)
        if target not in unified:
            unified[target] = dict(info)
            continue
        merged = unified[target]
        for key in ("vendor_id", "product_id", "manufacturer", "product", "serial"):
            # Prefer USB 3.0 device information, like the Linux implementation
            if info.get(key) and (info["_usb3"] or not merged.get(key)):
                merged[key] = info[key]
        merged["block_devices"] = sorted(set(merged["block_devices"]) | set(info["block_devices"]))

    return {path: {k: v for k, v in info.items() if not k.startswith("_")}
            for path, info in unified.items()}


def enumerate_ports(run=subprocess.run) -> Dict[str, dict]:
    """Enumerate all USB ports (including empty ones) on macOS."""
    try:
        result = run(IOREG_COMMAND, capture_output=True, check=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as e:
        raise RuntimeError(f"Could not read the I/O Registry via ioreg: {e}") from e
    _disk_sizes.clear()
    try:
        raw_ports = parse_ioreg(result.stdout)
    except (plistlib.InvalidFileException, ValueError) as e:
        raise RuntimeError(f"Could not parse ioreg output: {e}") from e
    return unify_by_container_id(raw_ports)


def disk_size(device: str) -> Optional[int]:
    """Size of a whole disk ("/dev/diskN") as seen by the last enumeration."""
    name = device.rsplit("/", 1)[-1]
    if name.startswith("rdisk"):
        name = name[1:]
    return _disk_sizes.get(name)
