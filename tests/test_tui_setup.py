import argparse
import os

import pytest
from rich.console import Console

from tsflash import cli
from tsflash.tui_setup import SetupAborted, find_images, run_setup
from tsflash.usb import find_first_usb_hub

EMPTY = {"vendor_id": None, "product_id": None, "manufacturer": None, "product": None,
         "serial": None, "block_devices": []}


def dev(name, *block_devices):
    return dict(EMPTY, vendor_id="0x2109", product_id="0x2817", product=name,
                block_devices=list(block_devices))


# Built-in hub 1-1 with an external hub on 1-1.2, and a second hub on 1-2
PORTS = {
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


class Answers:
    """Scripted answers for the prompts; records what was asked."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.prompts = []

    def __call__(self, prompt, default):
        self.prompts.append((prompt, default))
        answer = self.answers.pop(0)
        return default if answer == "" else answer


def setup(answers, directory, port=None, ports=PORTS):
    console = Console(width=120, record=True, force_terminal=False)
    result = run_setup(port=port, settings={"block size": "4M"},
                       enumerate_ports=lambda: ports, system_disks=set(),
                       device_size=lambda d: 8 << 30, ask=answers, console=console,
                       directory=directory)
    return result, console.export_text()


def test_find_images_newest_first(images):
    assert [p.name for p in find_images(images)] == ["new.ISO", "mid.img", "old.img"]


def test_default_hub_is_first_hub_without_hubs_below():
    assert find_first_usb_hub(PORTS) == "1-1.2"
    assert find_first_usb_hub({"1-1": EMPTY, "1-2": EMPTY}) is None


def test_defaults_select_newest_image_and_default_hub(images):
    answers = Answers("", "", "")
    (image, port), page = setup(answers, images)
    assert image == str(images / "new.ISO") and port == "1-1.2"
    assert [p for p, _ in answers.prompts] == [
        "Image (number or path)", "Port to monitor (r to rescan)", "Start flashing? [y/n]"]
    # The final page shows all choices together
    assert "▶ new.ISO" in page and "1-1.2  External Hub" in page and "◀ selected" in page
    assert "block size: 4M" in page


def test_select_by_number_and_port_name(images):
    (image, port), page = setup(Answers("3", "1-2", "y"), images)
    assert image == str(images / "old.img") and port == "1-2"


def test_invalid_answers_are_asked_again(images):
    answers = Answers("9", "1", "9-9", "1-1", "y")
    (image, port), page = setup(answers, images)
    assert len(answers.prompts) == 5 and port == "1-1"


def test_image_by_path_when_directory_has_none(tmp_path, images):
    empty = tmp_path / "empty"
    empty.mkdir()
    answers = Answers(str(images / "mid.img"), "", "y")
    (image, port), page = setup(answers, empty)
    assert answers.prompts[0][1] is None           # no default without images
    assert image == str(images / "mid.img")
    assert "no .img or .iso files" in page


def test_port_from_command_line_is_not_asked(images):
    answers = Answers("", "y")
    (image, port), _ = setup(answers, images, port="1-2")
    assert port == "1-2" and len(answers.prompts) == 2


def test_rescan(images):
    scans = iter([{}, PORTS])
    console = Console(record=True, force_terminal=False)
    answers = Answers("1", "r", "", "y")
    image, port = run_setup(None, {}, lambda: next(scans), set(), lambda d: None, answers,
                            console=console, directory=images)
    assert answers.prompts[1][1] is None and port == "1-1.2"


def test_declining_aborts(images):
    with pytest.raises(SetupAborted):
        setup(Answers("", "", "n"), images)


def test_tui_without_image_needs_a_terminal(monkeypatch):
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: False, raising=False)
    monkeypatch.setattr(cli, "check_devmon_and_exit", lambda: 0)
    args = argparse.Namespace(image_path=None, port=None, block_size="4M", stable_delay=3.0,
                              verbose=False, quiet=False)
    assert cli.cmd_tui(args) == 1


def test_hint_when_no_hub_found(images):
    console = Console(width=120, record=True, force_terminal=False)
    answers = Answers("1", "1-1", "y")
    run_setup(None, {}, lambda: {"1-1": EMPTY}, set(), lambda d: None, answers,
              console=console, directory=images)
    assert "No USB hub found" in console.export_text()
