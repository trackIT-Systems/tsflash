"""Tests for tsflashd: config loading, port selection, monitor lifecycle and
shutdown, with USB enumeration, the port monitor and signals faked."""

import logging
import signal
import sys

import pytest

from tsflash import daemon

HUB = {"vendor_id": "0x2109", "product_id": "0x2817", "block_devices": []}
EMPTY = {"vendor_id": None, "product_id": None, "block_devices": []}
PORTS = {"1-1": HUB, "1-1.1": EMPTY, "1-1.2": EMPTY}


class FakeMonitor:
    instances = []

    def __init__(self, monitor_port, config, image):
        self.monitor_port, self.config, self.image = monitor_port, config, image
        self.calls = []
        self.run_effect = None
        FakeMonitor.instances.append(self)

    def run(self):
        self.calls.append("run")
        if self.run_effect:
            raise self.run_effect

    def stop(self):
        self.calls.append("stop")

    def shutdown(self):
        self.calls.append("shutdown")


class FakeImage:
    def __init__(self, path):
        self.path = path
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def env(tmp_path, image_file, monkeypatch):
    """A config file next to the image, and faked hardware and signals."""
    FakeMonitor.instances = []
    images, handlers = [], {}

    def make_image(path):
        images.append(FakeImage(path))
        return images[-1]

    monkeypatch.setattr(daemon, "PortMonitor", FakeMonitor)
    monkeypatch.setattr(daemon, "create_image_mmap", make_image)
    monkeypatch.setattr(daemon, "enumerate_all_usb_ports", lambda: PORTS)
    monkeypatch.setattr(daemon.signal, "signal", lambda signum, handler: handlers.update({signum: handler}))

    root = logging.getLogger()
    level = root.level
    yield Env(tmp_path / "tsflash.yml", image_file, images, handlers)
    root.setLevel(level)


class Env:
    def __init__(self, config_path, image_file, images, handlers):
        self.config_path, self.image_file = config_path, image_file
        self.images, self.handlers = images, handlers

    def write_config(self, **settings):
        lines = [f"image_path: {self.image_file}"]
        lines += [f"{key}: {value}" for key, value in settings.items()]
        self.config_path.write_text("\n".join(lines) + "\n")
        return str(self.config_path)

    @property
    def monitor(self):
        assert len(FakeMonitor.instances) == 1
        return FakeMonitor.instances[0]


def test_runs_monitor_on_auto_detected_hub(env):
    assert daemon.run_daemon(env.write_config()) == 0

    assert env.monitor.monitor_port == "1-1"
    assert env.monitor.calls == ["run", "shutdown"]
    assert env.monitor.image is env.images[0]
    assert env.images[0].path == str(env.image_file)
    assert env.images[0].closed


def test_uses_configured_port_and_settings(env):
    assert daemon.run_daemon(env.write_config(port='"1-1.2"', block_size="1M", stable_delay=5)) == 0

    assert env.monitor.monitor_port == "1-1.2"
    assert env.monitor.config.block_size == "1M"
    assert env.monitor.config.stable_delay == 5.0


def test_unknown_configured_port_fails(env, caplog):
    assert daemon.run_daemon(env.write_config(port='"9-9"')) == 1

    assert FakeMonitor.instances == []
    assert env.images[0].closed
    assert "specified port '9-9' not found" in caplog.text


def test_no_hub_fails(env, monkeypatch, caplog):
    monkeypatch.setattr(daemon, "enumerate_all_usb_ports", lambda: {"1-1": EMPTY})

    assert daemon.run_daemon(env.write_config()) == 1
    assert FakeMonitor.instances == []
    assert "no USB hub found" in caplog.text


def test_missing_config_file_fails(env, caplog):
    assert daemon.run_daemon(str(env.config_path)) == 1
    assert "Configuration file not found" in caplog.text
    assert env.images == []


def test_invalid_config_fails(env, caplog):
    env.config_path.write_text("block_size: 4M\n")

    assert daemon.run_daemon(str(env.config_path)) == 1
    assert "Missing required configuration: image_path" in caplog.text


def test_unreadable_image_fails(env, monkeypatch, caplog):
    def fail(path):
        raise OSError("mmap failed")

    monkeypatch.setattr(daemon, "create_image_mmap", fail)

    assert daemon.run_daemon(env.write_config()) == 1
    assert FakeMonitor.instances == []
    assert "mmap failed" in caplog.text


def test_signals_stop_the_monitor(env):
    assert daemon.run_daemon(env.write_config()) == 0

    assert set(env.handlers) == {signal.SIGTERM, signal.SIGINT}
    env.handlers[signal.SIGTERM](signal.SIGTERM, None)
    assert env.monitor.calls[-1] == "stop"


@pytest.mark.parametrize(("error", "exit_code"), [(KeyboardInterrupt(), 0), (RuntimeError("boom"), 1)])
def test_monitor_errors_still_shut_down_and_close_image(env, monkeypatch, error, exit_code):
    def make_failing(*args):
        monitor = FakeMonitor(*args)
        monitor.run_effect = error
        return monitor

    monkeypatch.setattr(daemon, "PortMonitor", make_failing)

    assert daemon.run_daemon(env.write_config()) == exit_code
    assert env.monitor.calls == ["run", "shutdown"]
    assert env.images[0].closed


@pytest.mark.parametrize(("config_level", "override", "expected"), [
    ("WARNING", None, logging.WARNING),
    ("WARNING", "DEBUG", logging.DEBUG),
    ("debug", None, logging.DEBUG),
])
def test_log_level_from_config_or_override(env, config_level, override, expected):
    assert daemon.run_daemon(env.write_config(log_level=config_level), log_level=override) == 0
    assert logging.getLogger().level == expected


@pytest.mark.parametrize(("level", "expected"), [
    ("DEBUG", logging.DEBUG), ("ERROR", logging.ERROR), ("bogus", logging.INFO)])
def test_setup_logging_levels(monkeypatch, level, expected):
    calls = []
    monkeypatch.setattr(daemon.logging, "basicConfig", lambda **kwargs: calls.append(kwargs))

    daemon.setup_logging(level)
    assert calls[0]["level"] == expected


@pytest.mark.parametrize(("argv", "config", "log_level"), [
    ([], None, None),
    (["--config", "/etc/tsflash.yml"], "/etc/tsflash.yml", None),
    (["-v"], None, "DEBUG"),
    (["-q"], None, "WARNING"),
])
def test_main_passes_arguments(monkeypatch, argv, config, log_level):
    calls = []
    monkeypatch.setattr(sys, "argv", ["tsflashd", *argv])
    monkeypatch.setattr(daemon, "run_daemon", lambda path, log_level: calls.append((path, log_level)) or 3)

    with pytest.raises(SystemExit) as exit_info:
        daemon.main()
    assert exit_info.value.code == 3
    assert calls == [(config, log_level)]
