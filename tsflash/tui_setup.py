"""Interactive setup page for `tsflash tui` started without an image.

Everything is shown on one page: the images in the current directory, the USB
port tree and the settings. The page is redrawn after each answer, so the
choices made so far are always visible.
"""

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

from rich.console import Console, Group
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from .usb import find_first_usb_hub
from .usb_view import _format_size, build_tree

IMAGE_SUFFIXES = (".img", ".iso")


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


@dataclass
class SetupState:
    directory: Path
    images: List[Path]
    ports: Dict[str, dict]
    image: Optional[Path] = None
    port: Optional[str] = None
    message: Optional[str] = None


def _display_path(path: Path) -> str:
    """Path with the (invoking user's) home directory shortened to ~."""
    home = os.path.expanduser(f"~{os.environ['SUDO_USER']}" if os.environ.get("SUDO_USER")
                              else "~")
    text = str(path)
    return "~" + text[len(home):] if text == home or text.startswith(home + "/") else text


class SetupAborted(Exception):
    """The user left the setup page without starting."""


def _images_table(state: SetupState) -> Table:
    table = Table(box=None, pad_edge=False, show_header=False)
    table.add_column(justify="right", style="bold cyan")
    table.add_column()
    table.add_column(justify="right", style="dim")
    table.add_column(style="dim")
    for number, path in enumerate(state.images, 1):
        selected = state.image is not None and path.resolve() == state.image.resolve()
        try:
            stat = path.stat()
            size, modified = _format_size(stat.st_size), time.strftime(
                "%Y-%m-%d %H:%M", time.localtime(stat.st_mtime))
        except OSError:
            size, modified = "?", "?"
        name = Text(("▶ " if selected else "  ") + path.name,
                    style="bold yellow" if selected else "")
        table.add_row(str(number), name, size, modified)
    return table


def render_page(state: SetupState, settings: Dict[str, str], system_disks: Set[str],
                device_size: Callable[[str], Optional[int]]) -> Group:
    parts = [Rule("tsflash setup", align="left", style="cyan"), Text()]

    parts.append(Text(f"Images in {_display_path(state.directory)}", style="bold"))
    if state.images:
        parts.append(_images_table(state))
    else:
        parts.append(Text("  no .img or .iso files - enter a path", style="dim"))
    if state.image is not None and state.image not in state.images:
        parts.append(Text(f"  ▶ {state.image}", style="bold yellow"))
    parts.append(Text())

    parts.append(Text("USB ports", style="bold"))
    if state.ports:
        parts.extend(build_tree(state.ports, system_disks, device_size, state.port,
                                marker="selected"))
    else:
        parts.append(Text("  no USB ports found", style="dim"))
    parts.append(Text())

    parts.append(Text("Settings", style="bold"))
    for key, value in settings.items():
        parts.append(Text.assemble(f"  {key}: ", (value, "cyan")))
    parts.append(Text())

    if state.message:
        parts.append(Text(state.message, style="bold red"))
    return Group(*parts)


def _resolve_image(answer: str, images: List[Path]) -> Path:
    if answer.isdigit():
        index = int(answer)
        if 1 <= index <= len(images):
            return images[index - 1]
        raise ValueError(f"No image number {answer}")
    path = Path(os.path.expanduser(answer))
    if not path.is_file():
        raise ValueError(f"Not a file: {answer}")
    return path


def run_setup(
    port: Optional[str],
    settings: Dict[str, str],
    enumerate_ports: Callable[[], Dict[str, dict]],
    system_disks: Set[str],
    device_size: Callable[[str], Optional[int]],
    ask: Callable[[str, Optional[str]], str],
    console: Optional[Console] = None,
    directory: Optional[Path] = None,
):
    """
    Ask for the image and the port on one page, then for confirmation.

    `ask(prompt, default)` reads one answer; it returns the default for an
    empty answer. A port given on the command line is used without asking.

    Returns:
        (image_path, port)

    Raises:
        SetupAborted: if the user does not confirm
    """
    console = console or Console()
    directory = Path(directory or Path.cwd())
    state = SetupState(directory=directory, images=find_images(directory),
                       ports=enumerate_ports())
    state.port = port or find_first_usb_hub(state.ports)

    def show():
        console.clear()
        console.print(render_page(state, settings, system_disks, device_size))
        state.message = None

    # Image
    while state.image is None:
        show()
        default = "1" if state.images else None
        answer = ask("Image (number or path)", default).strip()
        if not answer:
            state.message = "Select an image"
            continue
        try:
            state.image = _resolve_image(answer, state.images)
        except ValueError as e:
            state.message = str(e)

    # Port
    if port is None:
        while True:
            if state.port is None and not state.message:
                state.message = "No USB hub found - connect one and enter r, or enter a port"
            show()
            answer = ask("Port to monitor (r to rescan)", state.port).strip()
            if answer.lower() == "r":
                state.ports = enumerate_ports()
                if state.port not in state.ports:
                    state.port = find_first_usb_hub(state.ports)
                continue
            if answer in state.ports:
                state.port = answer
                break
            state.message = f"Unknown port: {answer}" if answer else "Select a port"

    show()
    if ask("Start flashing? [y/n]", "y").strip().lower() not in ("y", "yes"):
        raise SetupAborted()
    return str(state.image), state.port
