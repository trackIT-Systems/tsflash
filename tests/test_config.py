import pytest

from tsflash.config import DaemonConfig, load_config


def make(image_file, **kwargs):
    return DaemonConfig({"image_path": str(image_file), **kwargs})


def test_defaults(image_file):
    config = make(image_file)
    assert config.block_size == "4M"
    assert config.stable_delay == 3.0
    assert config.log_level == "INFO"
    assert config.port is None


def test_negative_stable_delay_message(image_file):
    with pytest.raises(ValueError, match="non-negative"):
        make(image_file, stable_delay=-1)


def test_non_numeric_stable_delay(image_file):
    with pytest.raises(ValueError, match="must be a number"):
        make(image_file, stable_delay="soon")


@pytest.mark.parametrize("block_size", ["0", "-1M", "4MB", "x"])
def test_invalid_block_size_rejected_at_load(image_file, block_size):
    with pytest.raises(ValueError):
        make(image_file, block_size=block_size)


def test_missing_image(tmp_path):
    with pytest.raises(ValueError, match="does not exist"):
        DaemonConfig({"image_path": str(tmp_path / "missing.img")})


def test_load_config(tmp_path, image_file):
    path = tmp_path / "tsflash.yml"
    path.write_text(f"image_path: {image_file}\nlog_level: debug\nstable_delay: 1\n")
    config = load_config(str(path))
    assert config.log_level == "DEBUG"
    assert config.stable_delay == 1.0
