"""Human-readable tree view of USB ports for `tsflash usb`."""

import os
import sys
from typing import Callable, Dict, Iterable, List, Optional, Set

from rich.console import Console
from rich.text import Text
from rich.tree import Tree

from .usb import is_rpiboot_device, port_sort_key


def _parent(port: str) -> Optional[str]:
    return port.rsplit(".", 1)[0] if "." in port else None


def _children(port: str, ports: Iterable[str]) -> List[str]:
    return sorted((p for p in ports if _parent(p) == port), key=port_sort_key)


def _format_size(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1000 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1000.0


def _is_present(info: dict) -> bool:
    return info.get("vendor_id") is not None or info.get("product_id") is not None


def describe_port(port: str, info: dict, ports: Dict[str, dict], system_disks: Set[str],
                  device_size: Callable[[str], Optional[int]], default_port: Optional[str],
                  marker: str = "monitored by default") -> Text:
    """One tree line: port, device name, IDs and what tsflash would do with it."""
    line = Text()
    line.append(port, style="bold cyan")
    line.append("  ")

    if not _is_present(info):
        line.append("–", style="dim")
        return line

    name = " ".join(p for p in (info.get("manufacturer"), info.get("product")) if p)
    line.append(name or "Unknown device")
    if info.get("vendor_id") and info.get("product_id"):
        line.append(f"  {info['vendor_id'][2:]}:{info['product_id'][2:]}", style="dim")

    child_ports = _children(port, ports)
    if child_ports:
        line.append(f"  hub, {len(child_ports)} port{'s' if len(child_ports) != 1 else ''}",
                    style="dim")
    if port == default_port:
        line.append(f"  ◀ {marker}", style="bold yellow")

    block_devices = info.get("block_devices") or []
    flashable = []
    for device in block_devices:
        line.append("\n    ")
        line.append(device, style="yellow")
        size = device_size(device)
        if os.path.basename(device) in system_disks:
            line.append("  system disk - never flashed", style="bold red")
        elif not size:
            line.append("  no medium" if size == 0 else "  size unknown", style="dim")
        else:
            flashable.append(device)
            line.append(f"  {_format_size(size)}")
    if len(flashable) > 1:
        line.append("\n    ")
        line.append("several block devices on one port - not handled by tsflash", style="red")

    if not block_devices and is_rpiboot_device(info):
        line.append("\n    ")
        line.append("Raspberry Pi in USB boot mode - booted with rpiboot before flashing",
                    style="magenta")
    return line


def build_tree(ports: Dict[str, dict], system_disks: Set[str],
               device_size: Callable[[str], Optional[int]],
               default_port: Optional[str] = None,
               marker: str = "monitored by default") -> List[Tree]:
    """Build one rich Tree per top-level port in `ports`."""
    roots = sorted((p for p in ports if _parent(p) not in ports), key=port_sort_key)

    def add(node: Tree, port: str) -> None:
        for child in _children(port, ports):
            add(node.add(describe_port(child, ports[child], ports, system_disks,
                                       device_size, default_port, marker)), child)

    trees = []
    for root in roots:
        tree = Tree(describe_port(root, ports[root], ports, system_disks, device_size,
                                  default_port, marker), guide_style="dim")
        add(tree, root)
        trees.append(tree)
    return trees


def print_usb_tree(ports: Dict[str, dict], system_disks: Set[str],
                   device_size: Callable[[str], Optional[int]],
                   default_port: Optional[str] = None, hint: bool = True,
                   console: Optional[Console] = None) -> None:
    """Print the USB port tree, followed by a usage hint."""
    if console is None:
        # Don't wrap lines when piped (rich would assume 80 columns)
        console = Console(width=None if sys.stdout.isatty() else 200)
    for tree in build_tree(ports, system_disks, device_size, default_port):
        console.print(tree)
    if hint:
        console.print()
        console.print(Text(
            "Use the port names with --port (tui, usb) or -p (rpiboot). "
            "Without --port, tui and daemon monitor the hub marked ◀.", style="dim"))
