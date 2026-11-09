"""Setup screen of `tsflash tui` when started without an image.

One full-screen page with two lists, the images in the current directory and
the USB port tree. The arrow keys move the selection, Enter confirms the image
and then the hub (which starts monitoring), Esc goes back to the image. The lists are refreshed while the page
is shown, so a hub plugged in later appears (and is selected) by itself.
"""

import os
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

from rich.console import Console, ConsoleOptions, Group, RenderResult
from rich.layout import Layout
from rich.panel import Panel
from rich.segment import Segment
from rich.table import Table
from rich.text import Text

from .usb import find_first_usb_hub, port_sort_key
from .usb_view import _format_size, build_tree

IMAGE_SUFFIXES = (".img", ".iso")
IMAGES = "images"
PORTS = "ports"
PAGE_STEP = 5

IMAGE_MARKER = "▶ "
PORT_MARKER = "selected"


def find_images(directory: Path) -> List[Path]:
    """`.img` and `.iso` files in `directory`, newest first."""
    images = []
    try:
        entries = list(Path(directory).iterdir())
    except OSError:
        return []
    for path in entries:
        try:
            if path.suffix.lower() in IMAGE_SUFFIXES and path.is_file():
                images.append((path.stat().st_mtime, path))
        except OSError:
            continue
    return [path for _, path in sorted(images, key=lambda item: (-item[0], item[1].name))]


def find_hubs(ports: Dict[str, dict]) -> List[str]:
    """Ports with ports below them, in port order."""
    return [p for p in sorted(ports, key=port_sort_key)
            if any(q.startswith(p + ".") for q in ports)]


def display_path(path: Path) -> str:
    """Path with the (invoking user's) home directory shortened to ~."""
    sudo_user = os.environ.get("SUDO_USER")
    home = os.path.expanduser(f"~{sudo_user}" if sudo_user else "~")
    text = str(path)
    return "~" + text[len(home):] if text == home or text.startswith(home + "/") else text


class Window:
    """Renders the part of a renderable that fits, scrolled to the line containing `focus`."""

    def __init__(self, renderable, focus: Optional[str]):
        self.renderable = renderable
        self.focus = focus

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        lines = console.render_lines(self.renderable, options.update(height=None), pad=False)
        height = options.height or len(lines)
        index = 0
        if self.focus:
            index = next((i for i, line in enumerate(lines)
                          if self.focus in "".join(s.text for s in line)), 0)
        start = max(0, min(index - height // 2, len(lines) - height))
        for line in lines[start:start + height]:
            yield from line
            yield Segment.line()


class SetupScreen:
    """State and rendering of the setup screen; keys come in through handle_key()."""

    def __init__(self, directory: Path, ports: Dict[str, dict], port: Optional[str] = None,
                 image: Optional[Path] = None):
        """`port` and `image` preselect a hub and an image (e.g. from the command line)."""
        self.directory = Path(os.path.abspath(directory))
        self.extra_image = Path(os.path.abspath(image)) if image else None
        self.images: List[Path] = []
        self.image: Optional[Path] = None
        self.ports: Dict[str, dict] = {}
        self.hubs: List[str] = []
        self.port: Optional[str] = port
        self._port_chosen = port is not None
        self.focus = IMAGES
        self.message: Optional[str] = None
        self.update_images(find_images(self.directory))
        if self.extra_image is not None:
            self.image = self.extra_image
        self.update_ports(ports)

    # Data updates

    def update_images(self, images: List[Path]) -> None:
        """New image list; keeps the selected image if it is still there."""
        if self.extra_image is not None and self.extra_image not in images:
            images = [self.extra_image] + images   # preselected image from elsewhere
        self.images = images
        if self.image not in images:
            self.image = images[0] if images else None

    def update_ports(self, ports: Dict[str, dict]) -> None:
        """New port scan; keeps a hub chosen with the keys, else follows the default hub."""
        self.ports = ports
        self.hubs = find_hubs(ports)
        if self._port_chosen and self.port in ports and self.port not in self.hubs:
            self.hubs = sorted(self.hubs + [self.port], key=port_sort_key)  # e.g. from --port
        if not (self._port_chosen and self.port in self.hubs):
            self.port = find_first_usb_hub(ports)
            self._port_chosen = False

    # Keys

    def handle_key(self, key: str) -> Optional[str]:
        """Apply a key. Returns "start" or "quit" when the screen is done."""
        self.message = None
        if key in ("q", "Q"):
            return "quit"
        if key in ("up", "down", "pageup", "pagedown", "home", "end"):
            self._move(key)
        elif key in ("esc", "backspace", "left", "shift-tab"):
            self.focus = IMAGES
        elif key == "enter" and self.focus == IMAGES:
            if self.image is None:
                self.message = f"No .img or .iso file in {display_path(self.directory)}"
            else:
                self.focus = PORTS
        elif key == "enter":
            if self.port is None:
                self.message = "No USB hub found - connect one, it is selected automatically"
            else:
                return "start"
        return None

    def _move(self, key: str) -> None:
        items = self.images if self.focus == IMAGES else self.hubs
        if not items:
            return
        current = self.image if self.focus == IMAGES else self.port
        index = items.index(current) if current in items else -1
        steps = {"up": -1, "down": 1, "pageup": -PAGE_STEP, "pagedown": PAGE_STEP,
                 "home": -len(items), "end": len(items)}
        index = max(0, min(len(items) - 1, index + steps[key]))
        if self.focus == IMAGES:
            self.image = items[index]
        else:
            self.port = items[index]
            self._port_chosen = True

    # Rendering

    def _images_list(self) -> Table:
        table = Table.grid(padding=(0, 2))
        table.add_column(no_wrap=True)
        table.add_column(justify="right", style="dim")
        table.add_column(style="dim")
        for path in self.images:
            selected = path == self.image
            try:
                stat = path.stat()
                size = _format_size(stat.st_size)
                modified = time.strftime("%Y-%m-%d %H:%M", time.localtime(stat.st_mtime))
            except OSError:
                size, modified = "?", "?"
            name = Text((IMAGE_MARKER if selected else "  ") + path.name,
                        style="bold yellow" if selected else "")
            table.add_row(name, size, modified)
        return table

    def _panel(self, content, title: str, focus: str) -> Panel:
        focused = self.focus == focus
        done = focus == IMAGES and self.focus == PORTS
        return Panel(content, title=("✓ " if done else "") + title, title_align="left",
                     border_style="cyan" if focused else "dim",
                     subtitle="↑↓ to select, Enter to confirm" if focused else None,
                     subtitle_align="right")

    def render(self, settings: Dict[str, str], system_disks: Set[str],
               device_size: Callable[[str], Optional[int]]) -> Layout:
        images_title = f"1 Image  ·  {display_path(self.directory)}"
        if self.images:
            images = Window(self._images_list(), IMAGE_MARKER)
        else:
            images = Text("No .img or .iso files here. Start tsflash in the image's "
                          "directory or pass the image: tsflash tui IMAGE", style="dim")

        if self.ports:
            trees = build_tree(self.ports, system_disks, device_size, self.port,
                               marker=PORT_MARKER)
            ports = Window(Group(*trees), f"◀ {PORT_MARKER}")
        else:
            ports = Text("No USB ports found", style="dim")
        ports_title = "2 USB hub to monitor"

        summary = Text.assemble(
            (" Image ", "bold"), (self.image.name if self.image else "–", "yellow"),
            ("   Port ", "bold"), (self.port or "–", "yellow"),
            *[part for key, value in settings.items()
              for part in ((f"   {key} ", "bold"), (value, "cyan"))])

        if self.focus == IMAGES:
            keys = " ↑↓ select   Enter confirm image   q quit"
        else:
            keys = " ↑↓ select   Enter start   Esc back to image   q quit"
        footer = (Text(f" {self.message}", style="bold black on yellow") if self.message
                  else Text(keys, style="dim"))

        layout = Layout()
        layout.split_column(
            Layout(Text(" tsflash setup", style="bold cyan"), size=1),
            Layout(self._panel(images, images_title, IMAGES),
                   size=min(max(len(self.images), 1), 10) + 2),
            Layout(self._panel(ports, ports_title, PORTS)),
            Layout(summary, size=1),
            Layout(footer, size=1),
        )
        return layout
