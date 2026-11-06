import subprocess
import sys


def test_module_entry_point_propagates_exit_code(tmp_path):
    # run_tui returns 1 for a missing image; `python -m tsflash` must pass it on
    result = subprocess.run(
        [sys.executable, "-m", "tsflash", "tui", str(tmp_path / "missing.img")],
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    assert "does not exist" in result.stderr
