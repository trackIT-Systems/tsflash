import argparse
import json
import logging
import subprocess
import sys

import pytest

from tsflash import cli


def test_module_entry_point_propagates_exit_code(tmp_path):
    # run_tui returns 1 for a missing image; `python -m tsflash` must pass it on
    result = subprocess.run(
        [sys.executable, "-m", "tsflash", "tui", str(tmp_path / "missing.img")],
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    assert "does not exist" in result.stderr


def _flash_args(image_file, target):
    return argparse.Namespace(image_path=str(image_file), target=target,
                              block_size="4M", non_interactive=True)


def test_flash_refuses_system_disk(image_file, monkeypatch):
    monkeypatch.setattr(cli, "validate_block_device", lambda path: (True, None))
    monkeypatch.setattr(cli, "disk_for_device", lambda path: {"mmcblk0"})
    monkeypatch.setattr(cli, "get_system_disks", lambda: {"mmcblk0"})

    def must_not_flash(*args, **kwargs):
        raise AssertionError("system disk must not be flashed")

    monkeypatch.setattr(cli, "flash_image", must_not_flash)

    assert cli.cmd_flash(_flash_args(image_file, "/dev/mmcblk0p2")) == 1


def test_flash_allows_other_disk(image_file, monkeypatch):
    monkeypatch.setattr(cli, "validate_block_device", lambda path: (True, None))
    monkeypatch.setattr(cli, "disk_for_device", lambda path: {"sda"})
    monkeypatch.setattr(cli, "get_system_disks", lambda: {"mmcblk0"})
    flashed = []
    monkeypatch.setattr(cli, "flash_image", lambda image, target, *a, **k: flashed.append(target))

    assert cli.cmd_flash(_flash_args(image_file, "/dev/sda")) == 0
    assert flashed == ["/dev/sda"]


# --- main(): argument parsing and dispatch ---------------------------------

@pytest.fixture
def dispatched(monkeypatch):
    """Replace the command handlers; returns the (command, args) calls."""
    calls = []
    for name in ("flash", "usb", "daemon", "rpiboot", "tui"):
        monkeypatch.setattr(cli, f"cmd_{name}", lambda args, name=name: calls.append((name, args)) or 7)
    monkeypatch.setattr(cli, "setup_logging", lambda verbose, quiet: calls.append(("logging", verbose, quiet)))
    return calls


def run_main(monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["tsflash", *argv])
    return cli.main()


def test_main_dispatches_flash(monkeypatch, dispatched):
    assert run_main(monkeypatch, "-v", "flash", "a.img", "/dev/sda", "--block-size", "1M") == 7
    assert dispatched[0] == ("logging", True, False)
    command, args = dispatched[1]
    assert command == "flash"
    assert (args.image_path, args.target, args.block_size, args.non_interactive) == ("a.img", "/dev/sda", "1M", False)


def test_main_dispatches_usb(monkeypatch, dispatched):
    run_main(monkeypatch, "-q", "usb", "--json", "--port", "1-2")
    assert dispatched[0] == ("logging", False, True)
    command, args = dispatched[1]
    assert (command, args.json, args.all, args.port) == ("usb", True, False, "1-2")


def test_main_dispatches_daemon(monkeypatch, dispatched):
    run_main(monkeypatch, "daemon", "--config", "/etc/tsflash.yml")
    command, args = dispatched[1]
    assert (command, args.config) == ("daemon", "/etc/tsflash.yml")


def test_main_dispatches_rpiboot(monkeypatch, dispatched):
    run_main(monkeypatch, "rpiboot", "-p", "1-2.3")
    command, args = dispatched[1]
    assert (command, args.port) == ("rpiboot", "1-2.3")


def test_main_dispatches_tui_with_defaults(monkeypatch, dispatched):
    run_main(monkeypatch, "tui")
    command, args = dispatched[1]
    assert command == "tui"
    assert (args.image_path, args.port, args.block_size, args.stable_delay) == (None, None, "4M", 3.0)


@pytest.mark.parametrize("argv", [[], ["unknown"], ["flash", "only-image.img"], ["tui", "--stable-delay", "x"]])
def test_main_rejects_bad_arguments(monkeypatch, dispatched, argv):
    with pytest.raises(SystemExit) as exit_info:
        run_main(monkeypatch, *argv)
    assert exit_info.value.code == 2
    assert not any(call[0] in ("flash", "usb", "daemon", "rpiboot", "tui") for call in dispatched)


def test_main_version(monkeypatch, capsys):
    with pytest.raises(SystemExit) as exit_info:
        run_main(monkeypatch, "--version")
    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == f"tsflash {cli.__version__}"


@pytest.mark.parametrize(("verbose", "quiet", "level"), [
    (False, False, logging.INFO), (True, False, logging.DEBUG), (False, True, logging.WARNING)])
def test_setup_logging_levels(monkeypatch, verbose, quiet, level):
    calls = []
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kwargs: calls.append(kwargs))
    cli.setup_logging(verbose=verbose, quiet=quiet)
    assert calls[0]["level"] == level


# --- flash -----------------------------------------------------------------

def test_flash_rejects_invalid_image(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(cli, "flash_image", lambda *a, **k: pytest.fail("must not flash"))
    assert cli.cmd_flash(_flash_args(tmp_path / "missing.img", "/dev/sda")) == 1
    assert "does not exist" in caplog.text


def test_flash_rejects_invalid_target(image_file, monkeypatch, caplog):
    monkeypatch.setattr(cli, "validate_block_device", lambda path: (False, "not a block device"))
    monkeypatch.setattr(cli, "flash_image", lambda *a, **k: pytest.fail("must not flash"))
    assert cli.cmd_flash(_flash_args(image_file, "/tmp/file")) == 1
    assert "not a block device" in caplog.text


def test_flash_passes_options(image_file, monkeypatch):
    monkeypatch.setattr(cli, "validate_block_device", lambda path: (True, None))
    monkeypatch.setattr(cli, "disk_for_device", lambda path: {"sda"})
    monkeypatch.setattr(cli, "get_system_disks", lambda: set())
    calls = []
    monkeypatch.setattr(cli, "flash_image", lambda image, target, block_size, **kwargs:
                        calls.append((target, block_size, kwargs)))

    assert cli.cmd_flash(_flash_args(image_file, "/dev/sda")) == 0
    assert calls == [("/dev/sda", "4M", {"non_interactive": True, "image_path": str(image_file)})]


@pytest.mark.parametrize(("error", "message"), [
    (ValueError("bad block size"), "Invalid parameter: bad block size"),
    (PermissionError("denied"), "run this command with sudo"),
    (OSError("disk gone"), "I/O error: disk gone"),
    (RuntimeError("weird"), "Unexpected error: weird"),
])
def test_flash_reports_errors(image_file, monkeypatch, caplog, error, message):
    monkeypatch.setattr(cli, "validate_block_device", lambda path: (True, None))
    monkeypatch.setattr(cli, "disk_for_device", lambda path: {"sda"})
    monkeypatch.setattr(cli, "get_system_disks", lambda: set())

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(cli, "flash_image", fail)
    assert cli.cmd_flash(_flash_args(image_file, "/dev/sda")) == 1
    assert message in caplog.text


# --- usb -------------------------------------------------------------------

def _port(vendor=None):
    return {"vendor_id": vendor, "product_id": "0x0001" if vendor else None, "manufacturer": None,
            "product": None, "serial": None, "block_devices": []}


# A dock hub (1-1) with the flashing hub (1-1.2) below it, and a second bus.
USB_PORTS = {"1-1": _port("0x2109"), "1-1.1": _port(), "1-1.2": _port("0x05e3"),
             "1-1.2.1": _port(), "1-1.2.2": _port(), "3-1": _port()}


@pytest.fixture
def usb_tree(monkeypatch):
    """Fake USB ports; returns the print_usb_tree calls (ports, default_port)."""
    calls = []
    monkeypatch.setattr(cli, "enumerate_all_usb_ports", lambda: USB_PORTS)
    monkeypatch.setattr(cli, "get_system_disks", lambda: set())
    monkeypatch.setattr(cli, "print_usb_tree", lambda ports, disks, size, default:
                        calls.append((sorted(ports), default)))
    return calls


def _usb_args(json=False, all=False, port=None):
    return argparse.Namespace(json=json, all=all, port=port)


def test_usb_shows_default_hub(usb_tree):
    assert cli.cmd_usb(_usb_args()) == 0
    assert usb_tree == [(["1-1.2", "1-1.2.1", "1-1.2.2"], "1-1.2")]


def test_usb_all(usb_tree):
    assert cli.cmd_usb(_usb_args(all=True)) == 0
    assert usb_tree == [(sorted(USB_PORTS), "1-1.2")]


def test_usb_port(usb_tree):
    assert cli.cmd_usb(_usb_args(port="1-1")) == 0
    assert usb_tree[0][0] == ["1-1", "1-1.1", "1-1.2", "1-1.2.1", "1-1.2.2"]


def test_usb_unknown_port(usb_tree, caplog):
    assert cli.cmd_usb(_usb_args(port="9-9")) == 1
    assert usb_tree == []
    assert "No ports found matching port '9-9'" in caplog.text


def test_usb_without_hub_shows_all(usb_tree, monkeypatch):
    monkeypatch.setattr(cli, "enumerate_all_usb_ports", lambda: {"1-1": _port(), "2-1": _port()})
    assert cli.cmd_usb(_usb_args()) == 0
    assert usb_tree == [(["1-1", "2-1"], None)]


def test_usb_json(usb_tree, capsys):
    assert cli.cmd_usb(_usb_args(json=True)) == 0
    assert sorted(json.loads(capsys.readouterr().out)) == ["1-1.2", "1-1.2.1", "1-1.2.2"]
    assert usb_tree == []


@pytest.mark.parametrize(("error", "message"), [
    (RuntimeError("no sysfs"), "USB enumeration error: no sysfs"),
    (KeyError("x"), "Unexpected error"),
])
def test_usb_reports_errors(monkeypatch, caplog, error, message):
    def fail():
        raise error

    monkeypatch.setattr(cli, "enumerate_all_usb_ports", fail)
    assert cli.cmd_usb(_usb_args()) == 1
    assert message in caplog.text


# --- devmon detection --------------------------------------------------------

def test_devmon_is_ignored_off_linux(monkeypatch):
    monkeypatch.setattr(cli.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(cli.subprocess, "run", lambda *a, **k: pytest.fail("must not run"))
    assert cli.is_devmon_running() is False


def _fake_run(monkeypatch, results):
    """results maps the program name to a return code, or None if it is missing."""
    calls = []

    def run(command, **kwargs):
        calls.append(command[0])
        if results[command[0]] is None:
            raise FileNotFoundError(command[0])
        return subprocess.CompletedProcess(command, results[command[0]])

    monkeypatch.setattr(cli.platform, "system", lambda: "Linux")
    monkeypatch.setattr(cli.subprocess, "run", run)
    return calls


@pytest.mark.parametrize(("results", "running", "programs"), [
    ({"pgrep": 0}, True, ["pgrep"]),
    ({"pgrep": 1}, False, ["pgrep"]),
    ({"pgrep": None, "ps": 0}, True, ["pgrep", "ps"]),
    ({"pgrep": None, "ps": None}, False, ["pgrep", "ps"]),
])
def test_devmon_detection(monkeypatch, results, running, programs):
    calls = _fake_run(monkeypatch, results)
    assert cli.is_devmon_running() is running
    assert calls == programs


def test_check_devmon_explains_how_to_stop_it(monkeypatch, caplog):
    monkeypatch.setattr(cli, "is_devmon_running", lambda: True)
    assert cli.check_devmon_and_exit() == 1
    assert "sudo systemctl stop devmon" in caplog.text


# --- daemon, rpiboot, tui ------------------------------------------------------

def _flags(verbose=False, quiet=False, **kwargs):
    return argparse.Namespace(verbose=verbose, quiet=quiet, **kwargs)


@pytest.mark.parametrize(("verbose", "quiet", "level"), [
    (False, False, None), (True, False, "DEBUG"), (False, True, "WARNING")])
def test_daemon_passes_config_and_log_level(monkeypatch, verbose, quiet, level):
    calls = []
    monkeypatch.setattr(cli, "check_devmon_and_exit", lambda: 0)
    monkeypatch.setattr(cli, "run_daemon", lambda config, log_level: calls.append((config, log_level)) or 0)
    assert cli.cmd_daemon(_flags(verbose, quiet, config="c.yml")) == 0
    assert calls == [("c.yml", level)]


@pytest.mark.parametrize("command", ["daemon", "tui"])
def test_devmon_blocks_daemon_and_tui(monkeypatch, command):
    monkeypatch.setattr(cli, "check_devmon_and_exit", lambda: 1)
    monkeypatch.setattr(cli, "run_daemon", lambda *a, **k: pytest.fail("must not start"))
    monkeypatch.setattr(cli, "run_tui", lambda *a, **k: pytest.fail("must not start"))
    args = _flags(config=None, image_path="a.img", port=None, block_size="4M", stable_delay=3.0)
    assert getattr(cli, f"cmd_{command}")(args) == 1


def test_rpiboot_returns_exit_code(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "run_rpiboot", lambda port, verbose: calls.append((port, verbose)) or (False, 4))
    assert cli.cmd_rpiboot(_flags(verbose=True, port="1-2.3")) == 4
    assert calls == [("1-2.3", True)]


@pytest.mark.parametrize(("verbose", "quiet", "level"), [
    (False, False, "INFO"), (True, False, "DEBUG"), (False, True, "WARNING")])
def test_tui_passes_options(monkeypatch, verbose, quiet, level):
    calls = []
    monkeypatch.setattr(cli, "check_devmon_and_exit", lambda: 0)
    monkeypatch.setattr(cli, "run_tui", lambda **kwargs: calls.append(kwargs) or 0)
    args = _flags(verbose, quiet, image_path="a.img", port="1-2", block_size="1M", stable_delay=0.5)
    assert cli.cmd_tui(args) == 0
    assert calls == [{"image_path": "a.img", "port": "1-2", "block_size": "1M",
                      "stable_delay": 0.5, "log_level": level}]


def test_tui_without_image_starts_setup_in_a_terminal(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "check_devmon_and_exit", lambda: 0)
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: True, raising=False)
    monkeypatch.setattr(cli, "run_tui", lambda **kwargs: calls.append(kwargs) or 0)
    args = _flags(image_path=None, port=None, block_size="4M", stable_delay=3.0)
    assert cli.cmd_tui(args) == 0
    assert calls[0]["image_path"] is None
