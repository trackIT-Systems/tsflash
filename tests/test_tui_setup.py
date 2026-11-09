import argparse
import os
import select
import subprocess
import sys
import time

import pytest
from rich.console import Console

from tsflash import cli
from tsflash.keys import decode
from tsflash.tui_setup import IMAGES, PORTS, SetupScreen, find_images
from tsflash.usb import find_first_usb_hub

EMPTY = {"vendor_id": None, "product_id": None, "manufacturer": None, "product": None,
         "serial": None, "block_devices": []}


def dev(name, *block_devices):
    return dict(EMPTY, vendor_id="0x2109", product_id="0x2817", product=name,
                block_devices=list(block_devices))


# Built-in hub 1-1 with an external hub on 1-1.2, and a second hub on 1-2
HUBS = {
    "1-1": dev("Internal Hub"),
    "1-1.1": EMPTY,
    "1-1.2": dev("External Hub"),
    "1-1.2.1": dev("Reader", "/dev/sda"),
    "1-1.2.2": EMPTY,
    "1-2": dev("Other Hub"),
    "1-2.1": EMPTY,
}


@pytest.fixture
def images(tmp_path):
    for name, mtime in (("old.img", 1000), ("new.ISO", 3000), ("mid.img", 2000),
                        ("notes.txt", 4000)):
        path = tmp_path / name
        path.write_bytes(b"x" * 1024)
        os.utime(path, (mtime, mtime))
    (tmp_path / "dir.img").mkdir()
    return tmp_path


def render(screen, width=100, height=30):
    console = Console(width=width, height=height, record=True, force_terminal=False)
    console.print(screen.render({"Block size": "4M"}, set(), lambda d: 8 << 30))
    return console.export_text()


def press(screen, *keys):
    return [screen.handle_key(k) for k in keys][-1]


# Keys

def test_decode_keys():
    assert decode("a\x1b[A\x1b[B\r\t\x1b[Z", final=False) == (
        ["a", "up", "down", "enter", "tab", "shift-tab"], "")
    assert decode("\x1bOA\x1b[5~", final=False) == (["up", "pageup"], "")


def test_decode_waits_for_incomplete_escape_sequences():
    assert decode("q\x1b[", final=False) == (["q"], "\x1b[")
    assert decode("\x1b", final=False) == ([], "\x1b")
    assert decode("\x1b", final=True) == (["esc"], "")


# Lists and defaults

def test_find_images_newest_first(images):
    assert [p.name for p in find_images(images)] == ["new.ISO", "mid.img", "old.img"]


def test_default_hub_is_first_hub_without_hubs_below():
    assert find_first_usb_hub(HUBS) == "1-1.2"
    assert find_first_usb_hub({"1-1": EMPTY, "1-2": EMPTY}) is None


def test_defaults_are_newest_image_and_default_hub(images):
    screen = SetupScreen(images, HUBS)
    assert screen.image.name == "new.ISO" and screen.port == "1-1.2"
    assert press(screen, "enter") == "start"


# Navigation

def test_arrows_move_in_the_focused_list(images):
    screen = SetupScreen(images, HUBS)
    press(screen, "down", "down", "down")          # stops at the last image
    assert screen.image.name == "old.img"
    press(screen, "tab", "up")
    assert screen.focus == PORTS and screen.port == "1-1"
    press(screen, "end")
    assert screen.port == "1-2"
    press(screen, "shift-tab", "home")
    assert screen.focus == IMAGES and screen.image.name == "new.ISO"


def test_only_hubs_are_selectable(images):
    screen = SetupScreen(images, HUBS)
    press(screen, "tab")
    seen = {screen.port}
    for _ in range(5):
        press(screen, "down")
        seen.add(screen.port)
    assert seen == {"1-1.2", "1-2"}


def test_quit(images):
    assert press(SetupScreen(images, HUBS), "q") == "quit"


def test_fixed_port_cannot_be_changed(images):
    screen = SetupScreen(images, HUBS, port="1-2")
    press(screen, "tab", "down")
    assert screen.focus == IMAGES and screen.port == "1-2"
    assert "set by --port" in render(screen)


# Rescans

def test_hub_plugged_in_later_is_selected(images):
    screen = SetupScreen(images, {"1-1": EMPTY})
    assert screen.port is None
    assert press(screen, "enter") is None and "No USB hub found" in render(screen)
    screen.update_ports(HUBS)
    assert screen.port == "1-1.2"


def test_hub_chosen_with_keys_is_kept_on_rescan(images):
    screen = SetupScreen(images, HUBS)
    press(screen, "tab", "end")
    screen.update_ports(dict(HUBS, **{"1-3": dev("New Hub"), "1-3.1": EMPTY}))
    assert screen.port == "1-2"
    screen.update_ports({k: v for k, v in HUBS.items() if not k.startswith("1-2")})
    assert screen.port == "1-1.2"                  # gone: back to the default


def test_selected_image_is_kept_on_rescan(images):
    screen = SetupScreen(images, HUBS)
    press(screen, "down")
    newest = images / "newest.img"
    newest.write_bytes(b"x")
    screen.update_images(find_images(images))
    assert screen.image.name == "mid.img"


def test_no_images(tmp_path):
    screen = SetupScreen(tmp_path, HUBS)
    assert press(screen, "enter") is None
    assert "No .img or .iso file" in render(screen)


# Rendering

def test_page_shows_lists_and_summary(images):
    text = render(SetupScreen(images, HUBS))
    assert "▶ new.ISO" in text and "mid.img" in text
    assert "1-1.2  External Hub" in text and "◀ selected" in text
    assert "Port 1-1.2" in text and "Block size 4M" in text
    assert "Enter start" in text


def test_long_port_tree_scrolls_to_the_selection(images):
    ports = {f"1-{i}": EMPTY for i in range(1, 40)}
    ports.update({"1-30": dev("Far Hub"), "1-30.1": EMPTY})
    text = render(SetupScreen(images, ports), height=20)
    assert "1-30  Far Hub" in text and "◀ selected" in text
    assert "1-1  " not in text


# End to end

DRIVER = """
import sys
import tsflash.monitor as monitor, tsflash.tui as tui, tsflash.tui_setup as setup
ports = {'1-1': {'vendor_id': '1', 'product_id': '2', 'product': 'Hub', 'block_devices': []},
         '1-1.1': {'vendor_id': None, 'product_id': None, 'block_devices': []}}
fake = lambda: ports
tui.enumerate_all_usb_ports = fake
tui.get_system_disks = lambda: set()
init = monitor.PortMonitor.__init__
def patched(self, *a, **k):
    k.setdefault('enumerate_ports', fake)
    init(self, *a, **k)
monitor.PortMonitor.__init__ = patched
sys.exit(tui.run_tui(None, directory=sys.argv[1]))
"""


def run_on_pty(directory, keys):
    import pty

    master, slave = pty.openpty()
    proc = subprocess.Popen([sys.executable, "-c", DRIVER, str(directory)],
                            stdin=slave, stdout=slave, stderr=slave, close_fds=True)
    os.close(slave)
    output = b""

    def drain(seconds):
        nonlocal output
        end = time.time() + seconds
        while time.time() < end:
            if select.select([master], [], [], 0.05)[0]:
                try:
                    output += os.read(master, 65536)
                except OSError:
                    return

    try:
        drain(1.5)
        for key in keys:
            os.write(master, key)
            drain(0.5)
        drain(1.0)
        exit_code = proc.poll()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        os.close(master)
    return output.decode(errors="replace"), exit_code


@pytest.mark.skipif(sys.platform == "win32", reason="needs a pty")
def test_setup_screen_starts_monitor(image_file):
    output, exit_code = run_on_pty(image_file.parent, [b"\x1b[B", b"\r"])
    assert "tsflash setup" in output
    assert "q / Ctrl-C: quit" in output            # the monitor view is shown
    assert exit_code is None                       # still running


@pytest.mark.skipif(sys.platform == "win32", reason="needs a pty")
def test_setup_screen_quits_with_q(image_file):
    output, exit_code = run_on_pty(image_file.parent, [b"q"])
    assert "tsflash setup" in output and exit_code == 0


def test_tui_without_image_needs_a_terminal(monkeypatch):
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: False, raising=False)
    monkeypatch.setattr(cli, "check_devmon_and_exit", lambda: 0)
    args = argparse.Namespace(image_path=None, port=None, block_size="4M", stable_delay=3.0,
                              verbose=False, quiet=False)
    assert cli.cmd_tui(args) == 1
