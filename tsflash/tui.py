"""Text-based UI for monitoring tsflash operations."""

import logging
import os
import signal
import sys
import termios
import threading
import time
import tty
from collections import deque
from pathlib import Path
from typing import Callable, Deque, Dict, List, Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table, box
from rich.text import Text

from .config import DaemonConfig
from .flash import create_image_mmap, parse_block_size
from .keys import KeyReader
from .monitor import (
    BOOTING,
    COMPLETED,
    FAILED,
    FLASHING,
    IGNORED,
    NOT_CONNECTED,
    UNKNOWN,
    WAITING,
    PortMonitor,
    find_monitor_port,
)
from .tui_setup import SetupScreen, find_images
from .usb import block_device_size, enumerate_all_usb_ports, filter_ports_by_limit
from .validators import get_system_disks

logger = logging.getLogger(__name__)

# Seconds within which a second Ctrl-C / q confirms quitting
QUIT_CONFIRM_WINDOW = 3.0

# Seconds between USB and image rescans on the setup screen
SETUP_RESCAN_INTERVAL = 1.0


class TUIHandler(logging.Handler):
    """Logging handler that captures log messages for TUI display."""

    def __init__(self, maxlen: int = 50):
        super().__init__()
        self.buffer: Deque[str] = deque(maxlen=maxlen)
        self._buffer_lock = threading.Lock()

    def emit(self, record):
        """Emit a log record to the buffer."""
        try:
            msg = self.format(record)
            with self._buffer_lock:
                self.buffer.append(msg)
        except Exception:
            self.handleError(record)

    def lines(self):
        with self._buffer_lock:
            return list(self.buffer)


class QuitController:
    """
    Double-press quit confirmation (as in many TUIs): the first Ctrl-C or 'q'
    shows a prompt, a second one within QUIT_CONFIRM_WINDOW seconds quits.
    Any other key dismisses the prompt.
    """

    def __init__(self, window: float = QUIT_CONFIRM_WINDOW):
        self.window = window
        self._prompt_until = 0.0
        self.confirmed = False

    def request(self, now: Optional[float] = None) -> None:
        now = time.monotonic() if now is None else now
        if now < self._prompt_until:
            self.confirmed = True
        else:
            self._prompt_until = now + self.window

    def force(self) -> None:
        self.confirmed = True

    def dismiss(self) -> None:
        self._prompt_until = 0.0

    def prompt_active(self, now: Optional[float] = None) -> bool:
        now = time.monotonic() if now is None else now
        return not self.confirmed and now < self._prompt_until


def _report_error(msg: str) -> None:
    """
    Log an error and also print it to stderr if logging only goes to the TUI
    buffer (which is never shown when the TUI fails to start).
    """
    logger.error(msg)
    handlers = logging.getLogger().handlers
    if handlers and all(isinstance(h, TUIHandler) for h in handlers):
        print(f"ERROR: {msg}", file=sys.stderr)


def _format_bytes(bytes_value: float) -> str:
    """Format bytes as human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def _get_state_color(state: str) -> str:
    """Get color for port state."""
    colors = {
        NOT_CONNECTED: "dim white",
        WAITING: "yellow",
        BOOTING: "cyan",
        FLASHING: "blue",
        COMPLETED: "green",
        FAILED: "red",
        UNKNOWN: "magenta",
        IGNORED: "dim yellow",
    }
    return colors.get(state, "white")


def _build_title(monitor_port: Optional[str], ports_data: Dict, config: DaemonConfig) -> str:
    """<program name> @ USB Port <port> (manufacturer / product), <image path>"""
    title_parts = ["tsflash"]
    if monitor_port:
        title_parts.append("@ USB Port")
        title_parts.append(monitor_port)
        port_info = ports_data.get(monitor_port, {})
        device_parts = [p for p in (port_info.get('manufacturer'), port_info.get('product')) if p]
        if device_parts:
            title_parts.append(f"({' / '.join(device_parts)})")
    title_parts.append(config.image_path)
    return " ".join(title_parts)


def _downstream_ports(ports_data: Dict, monitor_port: Optional[str]) -> Dict:
    """Ports below the monitored hub, excluding the hub itself."""
    if not monitor_port:
        return ports_data
    downstream = filter_ports_by_limit(ports_data, monitor_port)
    return {k: v for k, v in downstream.items() if k != monitor_port}


def _create_ports_table(port_states: Dict[str, dict], ports_data: Dict, monitor_port: Optional[str],
                        config: DaemonConfig) -> Table:
    """Create table showing port states."""
    table = Table(
        show_header=True,
        header_style="bold magenta",
        expand=True,
        box=box.ROUNDED,
        title=_build_title(monitor_port, ports_data, config),
        title_style="bold yellow"
    )
    table.add_column("Port", style="cyan", no_wrap=True)
    table.add_column("State", style="bold")
    table.add_column("Progress", ratio=2, no_wrap=True)
    table.add_column("Device", style="yellow")
    table.add_column("Info", style="dim", ratio=3, no_wrap=True)

    all_ports = _downstream_ports(ports_data, monitor_port)
    sorted_ports = sorted(all_ports.keys())

    if not sorted_ports:
        table.add_row("(none)", "[dim]No ports available[/dim]", "", "", "")

    for port_str in sorted_ports:
        port_info = all_ports.get(port_str, {})
        port_state = port_states.get(port_str, {})
        if port_state:
            state = port_state.get('state', NOT_CONNECTED)
        elif port_info.get('vendor_id') is not None or port_info.get('product_id') is not None:
            state = UNKNOWN  # device connected but not yet tracked
        else:
            state = NOT_CONNECTED

        state_color = _get_state_color(state)
        state_display = f"[{state_color}]{state.upper()}[/{state_color}]"

        progress_info = port_state.get('progress', {})
        progress_str = ""
        info_text = port_state.get('error') or ""

        if state == FLASHING and progress_info:
            percent = progress_info.get('percent', 0.0)
            bytes_written = progress_info.get('bytes_written', 0)
            total_bytes = progress_info.get('total_bytes', 0)

            # Bar fills the cell; the percentage gets a fixed-width column
            progress_str = Table.grid(expand=True)
            progress_str.add_column(ratio=1)
            progress_str.add_column(width=7, justify="right")
            progress_str.add_row(
                ProgressBar(total=100.0, completed=percent, complete_style="blue",
                            finished_style="blue"),
                f"{percent:.1f}%",
            )

            elapsed_time = time.time() - progress_info.get('start_time', time.time())
            if total_bytes > 0 and elapsed_time > 0:
                speed_str = _format_bytes(bytes_written / elapsed_time) + "/s"
                info_text = f"{_format_bytes(bytes_written)} / {_format_bytes(total_bytes)} @ {speed_str}"
            else:
                info_text = "Initializing..."
        elif state == WAITING:
            wait_time = time.time() - port_state.get('detected_time', time.time())
            progress_str = f"[yellow]Waiting ({wait_time:.1f}s)[/yellow]"
        elif state == BOOTING:
            progress_str = f"[cyan]{port_state.get('boot_stage', 'Booting...')}[/cyan]"
        elif state == COMPLETED:
            progress_str = "[green]✓ Completed[/green]"
        elif state == FAILED:
            progress_str = "[red]✗ Failed[/red]"
            info_text = info_text or "Unknown error"
        elif state == IGNORED:
            progress_str = "[dim yellow]– Ignored[/dim yellow]"

        # Block devices - from port state if available, otherwise from the scan
        block_devices = port_state.get('block_devices') or port_info.get('block_devices', [])
        block_devices_str = ", ".join(block_devices) if block_devices else "-"

        table.add_row(port_str, state_display, progress_str, block_devices_str, Text(info_text))

    return table


def _create_log_panel(log_lines) -> Panel:
    """Create log display panel."""
    if not log_lines:
        content = Text("No log messages yet...", style="dim")
    else:
        content = Text("\n".join(log_lines[-30:]))  # Show last 30 lines
    return Panel(content, title="Logs", border_style="green")


def _create_footer(quit_prompt: bool, active_flashes: int) -> Text:
    """Key hints, or the quit confirmation prompt."""
    if not quit_prompt:
        return Text(" q / Ctrl-C: quit", style="dim")
    msg = " Press Ctrl-C (or q) again to quit"
    if active_flashes:
        msg += f" - {active_flashes} flash operation(s) in progress will be ABORTED"
    msg += ", any other key to cancel "
    return Text(msg, style="bold white on red" if active_flashes else "bold black on yellow")


def _create_layout(config: DaemonConfig, monitor_port: Optional[str], ports_data: Dict,
                   port_states: Dict[str, dict], log_lines, console_width: int,
                   quit_prompt: bool, active_flashes: int) -> Layout:
    """Create the main TUI layout."""
    layout = Layout()

    # Rows the title occupies (table borders take ~4 chars)
    title = _build_title(monitor_port, ports_data, config)
    available_title_width = max(20, console_width - 4)
    title_lines = max(1, (len(title) + available_title_width - 1) // available_title_width)

    # Ports table: one row per port (at least 1 for "(none)") + borders (2) +
    # separators (2) + header (1), plus extra title lines
    num_ports = len(_downstream_ports(ports_data, monitor_port)) or 1
    ports_size = num_ports + 5 + (title_lines - 1)

    layout.split_column(
        Layout(name="ports", size=ports_size),
        Layout(name="logs"),  # Takes remaining space
        Layout(name="footer", size=1),
    )
    layout["ports"].update(_create_ports_table(port_states, ports_data, monitor_port, config))
    layout["logs"].update(_create_log_panel(log_lines))
    layout["footer"].update(_create_footer(quit_prompt, active_flashes))
    return layout


def _setup_logging(log_level: str) -> TUIHandler:
    """Route all logging into a TUIHandler (no stdout/stderr output while the TUI runs)."""
    level = getattr(logging, log_level, logging.INFO)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Also clear handlers from package loggers that might have been configured
    for logger_name in ['tsflash', 'tsflash.daemon', 'tsflash.monitor', 'tsflash.usb',
                        'tsflash.flash', 'tsflash.rpiboot']:
        child_logger = logging.getLogger(logger_name)
        child_logger.handlers.clear()
        child_logger.propagate = True

    handler = TUIHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S'))
    handler.setLevel(level)
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
    return handler


class TerminalSession:
    """
    The full-screen terminal for the setup screen and the monitor: cbreak
    keyboard input, the rich Live display and the signal handlers.

    cbreak keeps ISIG, so Ctrl-C still arrives as SIGINT; on_sigint and
    on_sigterm are called for SIGINT and SIGTERM.
    """

    def __init__(self):
        self.console = Console()
        self.on_sigint: Callable[[], None] = lambda: None
        self.on_sigterm: Callable[[], None] = lambda: None
        self.live: Optional[Live] = None
        self._keys: Optional[KeyReader] = None
        self._fd: Optional[int] = None
        self._term_settings = None
        self._old_handlers = {}

    def __enter__(self):
        for signum, handler in ((signal.SIGINT, lambda *_: self.on_sigint()),
                                (signal.SIGTERM, lambda *_: self.on_sigterm())):
            self._old_handlers[signum] = signal.signal(signum, handler)
        if sys.stdin.isatty():
            try:
                self._fd = sys.stdin.fileno()
                self._term_settings = termios.tcgetattr(self._fd)
                tty.setcbreak(self._fd)
                self._keys = KeyReader(self._fd)
            except (termios.error, OSError) as e:
                logger.debug(f"Could not setup keyboard input: {e}")
                self._fd = None
        self.live = Live(console=self.console, screen=True, auto_refresh=False)
        self.live.__enter__()
        return self

    def __exit__(self, *exc):
        try:
            self.live.__exit__(*exc)
        finally:
            if self._fd is not None and self._term_settings is not None:
                try:
                    termios.tcsetattr(self._fd, termios.TCSADRAIN, self._term_settings)
                except (termios.error, OSError):
                    pass
            for signum, handler in self._old_handlers.items():
                signal.signal(signum, handler)
        return False

    def keys(self) -> List[str]:
        if self._keys is None:
            return []
        try:
            return self._keys.read()
        except OSError:
            return []

    def show(self, renderable) -> None:
        self.live.update(renderable, refresh=True)


def _start_from_setup(session: TerminalSession, port: Optional[str], settings: Dict[str, str],
                      make_config: Callable[[str, str], DaemonConfig],
                      directory: Optional[str] = None):
    """
    Run the setup screen until the user starts or quits.

    Returns:
        (config, image, ports_data, monitor_port), or None if the user quit
    """
    screen = SetupScreen(Path(directory or os.getcwd()), enumerate_all_usb_ports(), port)
    system_disks = get_system_disks()
    quit_requested = threading.Event()
    session.on_sigint = quit_requested.set
    session.on_sigterm = quit_requested.set
    last_scan = time.monotonic()

    while not quit_requested.is_set():
        for key in session.keys():
            action = screen.handle_key(key)
            if action == "quit":
                return None
            if action != "start":
                continue
            try:
                config = make_config(str(screen.image), screen.port)
                image = create_image_mmap(config.image_path)
            except (ValueError, IOError) as e:
                screen.message = str(e)
                continue
            ports_data = enumerate_all_usb_ports()
            if screen.port not in ports_data:
                image.close()
                screen.update_ports(ports_data)
                screen.message = f"Port {config.port} is gone"
                continue
            logger.info("Image file memory-mapped for efficient parallel flashing")
            return config, image, ports_data, screen.port

        if time.monotonic() - last_scan >= SETUP_RESCAN_INTERVAL:
            last_scan = time.monotonic()
            try:
                screen.update_ports(enumerate_all_usb_ports())
            except RuntimeError as e:
                screen.message = str(e)
            screen.update_images(find_images(screen.directory))

        session.show(screen.render(settings, system_disks, block_device_size))
        time.sleep(0.05)
    return None


def _run_live(session: TerminalSession, monitor: PortMonitor, monitor_thread: threading.Thread,
              config: DaemonConfig, initial_ports_data: Dict, log_handler: TUIHandler) -> None:
    """Run the monitor display until the user confirms quitting."""
    quit_ctl = QuitController()
    session.on_sigint = quit_ctl.request

    def _on_sigterm():
        logger.info("Received SIGTERM, shutting down...")
        quit_ctl.force()

    session.on_sigterm = _on_sigterm

    while not quit_ctl.confirmed:
        for key in session.keys():
            if key.lower() == 'q':
                quit_ctl.request()
            else:
                quit_ctl.dismiss()

        if quit_ctl.confirmed:
            break

        port_states, ports_data = monitor.snapshot()
        session.show(_create_layout(
            config, monitor.monitor_port, ports_data or initial_ports_data,
            port_states, log_handler.lines(), session.console.width,
            quit_ctl.prompt_active(), monitor.active_flash_count()))

        if not monitor_thread.is_alive():
            break

        time.sleep(0.1)


def run_tui(
    image_path: Optional[str] = None,
    port: Optional[str] = None,
    block_size: str = "4M",
    stable_delay: float = 3.0,
    log_level: str = "INFO",
    directory: Optional[str] = None,
) -> int:
    """
    Run the text-based UI for monitoring tsflash operations.

    Args:
        image_path: Path to the image file to flash; None shows the setup
            screen to select the image (and the port, unless given)
        port: USB port to monitor (optional, e.g., "1-2")
        block_size: Block size for flashing (default: "4M")
        stable_delay: Seconds to wait after device appears before flashing (default: 3.0)
        log_level: Logging level (default: "INFO")
        directory: Directory listed on the setup screen (default: current directory)

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    def make_config(image: str, monitor_port: Optional[str]) -> DaemonConfig:
        return DaemonConfig({
            'image_path': image,
            'port': monitor_port,
            'block_size': block_size,
            'stable_delay': stable_delay,
            'log_level': log_level
        })

    image = None
    try:
        parse_block_size(block_size)  # fail before the setup screen
        log_handler = _setup_logging(log_level)

        if image_path is not None:
            config = make_config(image_path, port)
            try:
                image = create_image_mmap(config.image_path)
                logger.info("Image file memory-mapped for efficient parallel flashing")
            except IOError as e:
                _report_error(f"Could not create memory-mapped image: {e}")
                return 1

            ports_data = enumerate_all_usb_ports()
            monitor_port = find_monitor_port(ports_data, config.port)
            if monitor_port is None:
                error_msg = "Could not determine port to monitor"
                if config.port:
                    error_msg += f" (specified port '{config.port}' not found)"
                else:
                    error_msg += " (no USB hub found for auto-detection)"
                _report_error(error_msg)
                return 1

        with TerminalSession() as session:
            if image is None:
                settings = {"Block size": block_size, "Stable delay": f"{stable_delay:g} s"}
                started = _start_from_setup(session, port, settings, make_config, directory)
                if started is None:
                    return 0
                config, image, ports_data, monitor_port = started

            monitor = PortMonitor(monitor_port, config, image)
            monitor_thread = threading.Thread(target=monitor.run, daemon=True, name="monitor")
            monitor_thread.start()
            try:
                _run_live(session, monitor, monitor_thread, config, ports_data, log_handler)
            finally:
                monitor.shutdown()
                monitor_thread.join(timeout=2.0)

        # Tell the user which devices were left incomplete
        port_states, _ = monitor.snapshot()
        for port_str, state in sorted(port_states.items()):
            if state.get('state') == FAILED and (state.get('error') or '').startswith('Aborted'):
                devices = ", ".join(state.get('block_devices') or []) or "?"
                print(f"WARNING: flashing {devices} on port {port_str} was aborted - "
                      f"the device is incomplete", file=sys.stderr)

        logger.info("TUI shutting down")
        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except ValueError as e:
        _report_error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.debug("Unexpected error", exc_info=True)
        _report_error(f"Unexpected error: {e}")
        return 1
    finally:
        if image is not None:
            image.close()
