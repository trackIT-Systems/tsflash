"""Shared test helpers."""

import os

import pytest

from tsflash.flash import create_image_mmap


@pytest.fixture
def image_file(tmp_path):
    """A 1 MiB + 1 byte image with non-repeating content."""
    path = tmp_path / "image.img"
    path.write_bytes(os.urandom(1024 * 1024 + 1))
    return path


@pytest.fixture
def mapped_image(image_file):
    image = create_image_mmap(str(image_file))
    yield image
    image.close()
