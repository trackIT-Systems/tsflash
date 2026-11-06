import io
import threading

import pytest

from tsflash import flash
from tsflash.flash import FlashAborted, flash_image, parse_block_size


@pytest.mark.parametrize("value, expected", [
    ("4M", 4 * 1024 * 1024),
    ("4m", 4 * 1024 * 1024),
    ("512K", 512 * 1024),
    ("1G", 1024 ** 3),
    ("1000", 1000),
    (" 1M ", 1024 * 1024),
])
def test_parse_block_size(value, expected):
    assert parse_block_size(value) == expected


@pytest.mark.parametrize("value", ["", "0", "0M", "-1M", "4MB", "abc", "M"])
def test_parse_block_size_rejects_invalid(value):
    with pytest.raises(ValueError):
        parse_block_size(value)


@pytest.mark.parametrize("non_interactive", [True, False])
def test_flash_image_writes_exact_copy(tmp_path, image_file, mapped_image, non_interactive):
    target = tmp_path / "target"
    target.touch()
    calls = []

    flash_image(mapped_image, str(target), "300K", non_interactive=non_interactive,
                progress_callback=lambda *args: calls.append(args))

    assert target.read_bytes() == image_file.read_bytes()
    # Same callback signature in both modes: (bytes_written, total_bytes, percent)
    assert all(len(c) == 3 for c in calls)
    assert calls[-1] == (len(mapped_image), len(mapped_image), 100.0)


def test_flash_image_handles_short_writes(tmp_path, image_file, mapped_image, monkeypatch):
    """Raw writes may write less than requested - the rest must not be lost."""
    real_open = open

    class ShortWriter(io.FileIO):
        def write(self, b):
            return super().write(bytes(b)[:1000])

    def fake_open(path, mode="r", buffering=-1):
        if mode == "wb":
            return ShortWriter(path, "wb")
        return real_open(path, mode, buffering)

    monkeypatch.setattr(flash, "open", fake_open, raising=False)
    target = tmp_path / "target"
    target.touch()

    flash_image(mapped_image, str(target), "64K", non_interactive=True)

    assert target.read_bytes() == image_file.read_bytes()


def test_flash_image_zero_length_write_fails(tmp_path, mapped_image, monkeypatch):
    class NoProgressWriter(io.FileIO):
        def write(self, b):
            return 0

    monkeypatch.setattr(flash, "open", lambda p, m="r", buffering=-1: NoProgressWriter(p, "wb"),
                        raising=False)
    target = tmp_path / "target"
    target.touch()

    with pytest.raises(IOError, match="Short write"):
        flash_image(mapped_image, str(target), "64K", non_interactive=True)


def test_flash_image_refuses_image_larger_than_target(tmp_path, mapped_image):
    target = tmp_path / "target"
    target.write_bytes(b"\0" * 1024)  # "device" of 1 KiB

    with pytest.raises(IOError, match="larger than target"):
        flash_image(mapped_image, str(target), "4M", non_interactive=True)
    assert target.read_bytes() == b"\0" * 1024


def test_flash_image_cancel(tmp_path, mapped_image):
    cancel = threading.Event()
    seen = []

    def progress(bytes_written, total, percent):
        seen.append(bytes_written)
        cancel.set()  # cancel after the first chunk

    target = tmp_path / "target"
    target.touch()
    with pytest.raises(FlashAborted):
        flash_image(mapped_image, str(target), "64K", non_interactive=True,
                    progress_callback=progress, cancel_event=cancel)
    assert seen == [64 * 1024]
    assert target.stat().st_size == 64 * 1024


def test_flash_image_does_not_create_missing_target(tmp_path, mapped_image):
    target = tmp_path / "vanished-device"
    with pytest.raises(IOError, match="Cannot open block device"):
        flash_image(mapped_image, str(target), "4M", non_interactive=True)
    assert not target.exists()


def test_flash_image_invalid_block_size(tmp_path, mapped_image):
    with pytest.raises(ValueError):
        flash_image(mapped_image, str(tmp_path / "target"), "0M", non_interactive=True)
