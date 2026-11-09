"""Keyboard input for the TUI: decodes raw terminal input into key names.

Special keys arrive as escape sequences (e.g. "\\x1b[A" for up). A lone ESC
is only reported once no further bytes followed within ESC_TIMEOUT.
"""

import codecs
import os
import re
import select
import time
from typing import List, Tuple

ESC_TIMEOUT = 0.05

SEQUENCES = {
    "\x1b[A": "up", "\x1bOA": "up",
    "\x1b[B": "down", "\x1bOB": "down",
    "\x1b[C": "right", "\x1bOC": "right",
    "\x1b[D": "left", "\x1bOD": "left",
    "\x1b[H": "home", "\x1bOH": "home", "\x1b[1~": "home",
    "\x1b[F": "end", "\x1bOF": "end", "\x1b[4~": "end",
    "\x1b[5~": "pageup", "\x1b[6~": "pagedown",
    "\x1b[Z": "shift-tab",
}

CHARACTERS = {"\r": "enter", "\n": "enter", "\t": "tab", "\x7f": "backspace"}

_COMPLETE = re.compile(r"\x1b(\[[0-9;]*[A-Za-z~]|O[A-Za-z])")
_INCOMPLETE = re.compile(r"\x1b(\[[0-9;]*|O)?")


def decode(data: str, final: bool) -> Tuple[List[str], str]:
    """
    Split terminal input into key names.

    Returns the keys and the undecoded rest (the start of an escape sequence
    that may still be completed). With `final`, nothing is held back.
    """
    keys = []
    i = 0
    while i < len(data):
        if data[i] == "\x1b":
            match = _COMPLETE.match(data, i)
            if match:
                keys.append(SEQUENCES.get(match.group(0), "unknown"))
                i = match.end()
                continue
            if not final and _INCOMPLETE.fullmatch(data, i):
                break
            keys.append("esc")
            i += 1
            continue
        keys.append(CHARACTERS.get(data[i], data[i]))
        i += 1
    return keys, data[i:]


class KeyReader:
    """Non-blocking reader of key names from a terminal in cbreak mode."""

    def __init__(self, fd: int):
        self.fd = fd
        self._pending = ""
        self._since = 0.0
        self._decoder = codecs.getincrementaldecoder("utf-8")("ignore")

    def read(self) -> List[str]:
        while select.select([self.fd], [], [], 0)[0]:
            chunk = os.read(self.fd, 1024)
            if not chunk:
                break
            self._pending += self._decoder.decode(chunk)
            self._since = time.monotonic()
        final = time.monotonic() - self._since >= ESC_TIMEOUT
        keys, self._pending = decode(self._pending, final)
        return keys
