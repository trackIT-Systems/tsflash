import os
import select
import subprocess
import sys
import time

import pytest
from rich.console import Console

from tsflash.tui import QuitController, _create_footer, _create_layout


def test_quit_requires_second_press_within_window():
    q = QuitController(window=3)
    q.request(now=0)
    assert q.prompt_active(now=1) and not q.confirmed
    q.request(now=2)
    assert q.confirmed


def test_quit_prompt_expires():
    q = QuitController(window=3)
    q.request(now=0)
    assert not q.prompt_active(now=3.5)
    q.request(now=4)              # starts a new prompt
    assert not q.confirmed


def test_quit_prompt_dismissed_by_other_key():
    q = QuitController(window=3)
    q.request(now=0)
    q.dismiss()
    q.request(now=1)
    assert not q.confirmed
    q.request(now=2)
    assert q.confirmed


def test_force_quits_immediately():
    q = QuitController()
    q.force()
    assert q.confirmed


def test_footer_warns_about_active_flashes():
    assert "ABORTED" in _create_footer(True, 2).plain
    assert "ABORTED" not in _create_footer(True, 0).plain
    assert "quit" in _create_footer(False, 0).plain


class Config:
    image_path = "/data/image.img"


def test_layout_renders_all_states():
    empty = {"vendor_id": None, "product_id": None, "block_devices": []}
    dev = lambda *b: {"vendor_id": "1", "product_id": "2", "block_devices": list(b)}
    ports = {"1-1": dev(), "1-1.1": dev("/dev/sda"), "1-1.2": dev("/dev/sdb"),
             "1-1.3": dev("/dev/sdc"), "1-1.4": empty}
    states = {
        "1-1.1": {"state": "flashing", "block_devices": ["/dev/sda"],
                  "progress": {"percent": 42.0, "bytes_written": 2 << 30,
                               "total_bytes": 5 << 30, "start_time": time.time() - 60}},
        "1-1.2": {"state": "ignored", "block_devices": ["/dev/sdb"],
                  "error": "System disk (/dev/sdb) - never flashed"},
        "1-1.3": {"state": "unknown", "block_devices": ["/dev/sdc"], "error": "No medium"},
    }
    console = Console(width=120, height=20, record=True, force_terminal=False)
    console.print(_create_layout(Config(), "1-1", ports, states, ["a [log] line"], 120, True, 1))
    text = console.export_text()

    for expected in ["FLASHING", "42.0%", "IGNORED", "System disk", "No medium",
                     "NOT_CONNECTED", "a [log] line", "ABORTED"]:
        assert expected in text


DRIVER = """
import sys
import tsflash.monitor as monitor, tsflash.tui as tui
fake = lambda: {'1-1': {'vendor_id': '1', 'product_id': '2', 'block_devices': []},
                '1-1.1': {'vendor_id': None, 'product_id': None, 'block_devices': []}}
tui.enumerate_all_usb_ports = fake
init = monitor.PortMonitor.__init__
def patched(self, *a, **k):
    k.setdefault('enumerate_ports', fake)
    init(self, *a, **k)
monitor.PortMonitor.__init__ = patched
sys.exit(tui.run_tui(sys.argv[1]))
"""


CTRL_C = object()  # sentinel: deliver SIGINT, as the terminal does for Ctrl-C


@pytest.mark.skipif(sys.platform == "win32", reason="needs a pty")
@pytest.mark.parametrize("keys, quits", [
    ([b"q"], False),
    ([b"q", b"q"], True),
    ([CTRL_C, CTRL_C], True),
    ([b"q", CTRL_C], True),
    ([b"q", b"x", b"q"], False),
])
def test_tui_quit_keys(image_file, keys, quits):
    """Run the real TUI on a pty (with faked USB) and press keys."""
    import pty
    import signal

    master, slave = pty.openpty()
    proc = subprocess.Popen([sys.executable, "-c", DRIVER, str(image_file)],
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
            if key is CTRL_C:
                proc.send_signal(signal.SIGINT)
            else:
                os.write(master, key)
            drain(0.3)
        drain(1.0)
        exit_code = proc.poll()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        os.close(master)

    assert b"again to quit" in output
    if quits:
        assert exit_code == 0
    else:
        assert exit_code is None
