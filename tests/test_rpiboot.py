import os
import shutil
import stat
import threading
import time

import pytest

from tsflash.rpiboot import EXIT_INTERRUPTED, EXIT_TIMEOUT, run_rpiboot


@pytest.fixture
def fake_rpiboot(tmp_path, monkeypatch):
    """Install a fake `rpiboot` shell script first in PATH."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")

    def install(script):
        path = bin_dir / "rpiboot"
        path.write_text("#!/bin/sh\n" + script)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
        assert shutil.which("rpiboot") == str(path)
        return path

    return install


def test_success_is_based_on_exit_code(fake_rpiboot):
    # rpiboot prints "Failed" for transient errors it recovers from
    fake_rpiboot('echo "Failed : 0x5"\necho "Second stage boot server done"\nexit 0\n')
    stages = []
    assert run_rpiboot(port="1-1", stage_callback=stages.append) == (True, 0)
    assert stages == ["Boot complete"]


def test_nonzero_exit_is_failure(fake_rpiboot):
    fake_rpiboot('echo "Second stage boot server done"\nexit 3\n')
    assert run_rpiboot(port="1-1") == (False, 3)


def test_passes_port(fake_rpiboot, tmp_path):
    args_file = tmp_path / "args"
    fake_rpiboot(f'echo "$@" > {args_file}\n')
    run_rpiboot(port="1-2.3", verbose=True)
    assert args_file.read_text().split() == ["-v", "-p", "1-2.3"]


def test_timeout_kills_process(fake_rpiboot):
    fake_rpiboot('echo "Waiting for BCM2835/6/7/2711/2712..."\nexec sleep 30\n')
    start = time.monotonic()
    assert run_rpiboot(port="1-1", timeout=0.5) == (False, EXIT_TIMEOUT)
    assert time.monotonic() - start < 5


def test_stop_event_kills_process(fake_rpiboot):
    fake_rpiboot('exec sleep 30\n')
    stop = threading.Event()
    threading.Timer(0.3, stop.set).start()
    start = time.monotonic()
    assert run_rpiboot(port="1-1", stop_event=stop) == (False, EXIT_INTERRUPTED)
    assert time.monotonic() - start < 5


def test_missing_binary(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))
    assert run_rpiboot(port="1-1") == (False, 1)
