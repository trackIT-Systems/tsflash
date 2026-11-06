"""Scenario tests for the PortMonitor state machine (USB, rpiboot and flashing faked)."""

import threading
import time

import pytest

from tsflash.flash import FlashAborted
from tsflash.monitor import (
    BOOTING, COMPLETED, FAILED, FLASHING, IGNORED, UNKNOWN, WAITING, PortMonitor,
)

GiB = 1 << 30
HUB = {"vendor_id": "0x2109", "product_id": "0x0817", "block_devices": []}
EMPTY = {"vendor_id": None, "product_id": None, "block_devices": []}
RPI = {"vendor_id": "0x0a5c", "product_id": "0x2711", "block_devices": []}


def device(*block_devices):
    return {"vendor_id": "0x0bda", "product_id": "0x0158", "block_devices": list(block_devices)}


class Config:
    stable_delay = 0
    block_size = "4M"
    image_path = "image.img"


class World:
    """Fake environment: one hub "1" with a single downstream port "1.1"."""

    def __init__(self):
        self.port = EMPTY
        self.sizes = {}
        self.system = set()
        self.flashed = []
        self.flash_error = None
        self.rpiboot_result = True
        self.rpiboots = 0

    def enumerate(self):
        return {"1": HUB, "1.1": self.port}

    def flash(self, image, device, block_size, cancel_event=None, progress_callback=None, **kw):
        if self.flash_error:
            raise self.flash_error
        progress_callback(10, 10, 100.0)
        self.flashed.append(device)

    def boot(self, port, **kwargs):
        self.rpiboots += 1
        return self.rpiboot_result


@pytest.fixture
def world():
    return World()


@pytest.fixture
def monitor(world):
    m = PortMonitor("1", Config(), b"image", enumerate_ports=world.enumerate,
                    boot_device=world.boot, flash=world.flash,
                    system_disks=lambda: world.system,
                    device_size=lambda d: world.sizes.get(d, 0))
    yield m
    m.shutdown(timeout=2)


def settle(monitor, polls=4):
    """Poll a few times, giving background jobs time to finish."""
    for _ in range(polls):
        monitor.poll()
        time.sleep(0.05)
    return monitor.snapshot()[0].get("1.1", {})


def test_empty_port_has_no_state(world, monitor):
    assert settle(monitor) == {}


def test_card_reader_no_medium_then_cards(world, monitor):
    world.port = device("/dev/sda")
    world.sizes = {"/dev/sda": 0}
    state = settle(monitor)
    assert state["state"] == UNKNOWN and state["error"] == "No medium"

    world.sizes["/dev/sda"] = 8 * GiB
    state = settle(monitor)
    assert state["state"] == COMPLETED and world.flashed == ["/dev/sda"]

    world.sizes["/dev/sda"] = 0                   # card pulled, reader stays
    assert settle(monitor)["state"] == UNKNOWN

    world.sizes["/dev/sda"] = 8 * GiB             # next card
    assert settle(monitor)["state"] == COMPLETED
    assert world.flashed == ["/dev/sda", "/dev/sda"]


def test_waits_for_stable_delay(world, monitor):
    monitor.config.stable_delay = 60
    world.port = device("/dev/sda")
    world.sizes = {"/dev/sda": GiB}
    assert settle(monitor)["state"] == WAITING
    assert world.flashed == []


def test_completed_device_is_not_reflashed(world, monitor):
    world.port = device("/dev/sda")
    world.sizes = {"/dev/sda": GiB}
    settle(monitor)
    settle(monitor)
    assert world.flashed == ["/dev/sda"]


def test_flash_failure_sticks_until_medium_removed(world, monitor):
    world.flash_error = IOError("write error")
    world.port = device("/dev/sda")
    world.sizes = {"/dev/sda": GiB}
    state = settle(monitor)
    assert state["state"] == FAILED and state["error"] == "write error"

    world.flash_error = None
    assert settle(monitor)["state"] == FAILED   # no automatic retry

    world.sizes["/dev/sda"] = 0
    assert settle(monitor)["state"] == UNKNOWN


def test_rpiboot_failure_sticks_until_unplugged(world, monitor):
    world.rpiboot_result = False
    world.port = RPI
    state = settle(monitor, polls=6)
    assert state["state"] == FAILED and state["error"] == "rpiboot failed"
    assert world.rpiboots == 1                   # no retry loop

    world.port = EMPTY
    assert settle(monitor) == {}

    world.port = RPI
    settle(monitor)
    assert world.rpiboots == 2


def test_rpiboot_then_flash(world, monitor):
    world.port = RPI
    state = settle(monitor)
    assert state["state"] == BOOTING and state["rpiboot_done"]

    world.port = EMPTY                           # re-enumeration gap
    settle(monitor, polls=1)

    world.port = device("/dev/sdb")
    world.sizes = {"/dev/sdb": GiB}
    assert settle(monitor)["state"] == COMPLETED
    assert world.flashed == ["/dev/sdb"]


def test_unplug_after_rpiboot_resets_port(world, monitor):
    world.port = RPI
    settle(monitor)
    world.port = EMPTY
    assert settle(monitor) == {}
    world.port = RPI
    settle(monitor)
    assert world.rpiboots == 2


def test_system_disk_is_ignored(world, monitor):
    world.system = {"sda"}
    world.port = device("/dev/sda")
    world.sizes = {"/dev/sda": GiB}
    state = settle(monitor)
    assert state["state"] == IGNORED and "System disk" in state["error"]
    assert world.flashed == []


def test_multiple_block_devices_are_ignored(world, monitor):
    world.port = device("/dev/sda", "/dev/sdb")
    world.sizes = {"/dev/sda": GiB, "/dev/sdb": GiB}
    state = settle(monitor)
    assert state["state"] == IGNORED and "Multiple block devices" in state["error"]
    assert world.flashed == []

    world.sizes["/dev/sdb"] = 0                  # only one card left
    assert settle(monitor)["state"] == COMPLETED
    assert world.flashed == ["/dev/sda"]


def test_device_removed_while_waiting(world, monitor):
    monitor.config.stable_delay = 60
    world.port = device("/dev/sda")
    world.sizes = {"/dev/sda": GiB}
    assert settle(monitor)["state"] == WAITING
    world.port = EMPTY
    assert settle(monitor) == {}


def test_shutdown_aborts_running_flash(world, monitor):
    started = threading.Event()

    def slow_flash(image, device, block_size, cancel_event=None, **kwargs):
        started.set()
        while not cancel_event.is_set():
            time.sleep(0.01)
        raise FlashAborted("aborted")

    world.flash = slow_flash
    monitor._flash = slow_flash
    world.port = device("/dev/sda")
    world.sizes = {"/dev/sda": GiB}
    settle(monitor, polls=2)
    assert started.wait(2)
    assert monitor.active_flash_count() == 1
    assert monitor.snapshot()[0]["1.1"]["state"] == FLASHING

    monitor.shutdown(timeout=2)

    state = monitor.snapshot()[0]["1.1"]
    assert state["state"] == FAILED and state["error"].startswith("Aborted")
    assert monitor.active_flash_count() == 0


def test_snapshot_is_a_copy(world, monitor):
    world.port = device("/dev/sda")
    world.sizes = {"/dev/sda": 0}
    settle(monitor)
    states, ports = monitor.snapshot()
    states["1.1"]["state"] = "tampered"
    assert monitor.snapshot()[0]["1.1"]["state"] == UNKNOWN
    assert "1.1" in ports
