"""Text-based UI for monitoring tsflash daemon operations."""

import logging
import signal
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Deque, Dict, Optional

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table, box

from .config import DaemonConfig, load_config
from .daemon import (
    COMPLETED,
    FAILED,
    FLASHING,
    NOT_CONNECTED,
    WAITING,
    BOOTING,
    UNKNOWN,
    boot_rpiboot_device,
    find_monitor_port,
    flash_device,
)
from .flash import create_image_mmap
from .usb import enumerate_all_usb_ports, filter_ports_by_limit, is_rpiboot_device

logger = logging.getLogger(__name__)

# Global state for TUI
_tui_shutdown_requested = False
_tui_port_states: Dict[str, dict] = {}
_tui_log_buffer: Deque[str] = deque(maxlen=50)
_tui_lock = threading.Lock()
_tui_flash_executor: Optional[ThreadPoolExecutor] = None
_tui_image_mmap = None


class TUIHandler(logging.Handler):
    """Logging handler that captures log messages for TUI display."""

    def emit(self, record):
        """Emit a log record to the buffer."""
        try:
            msg = self.format(record)
            with _tui_lock:
                _tui_log_buffer.append(msg)
        except Exception:
            self.handleError(record)


def _signal_handler(signum, frame):
    """Handle shutdown signals."""
    global _tui_shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    _tui_shutdown_requested = True


def _format_bytes(bytes_value: int) -> str:
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
    }
    return colors.get(state, "white")


def _create_ports_table(port_states: Dict[str, dict], ports_data: Dict, monitor_port: Optional[str], config: DaemonConfig, console_width: Optional[int] = None) -> Table:
    """Create table showing port states."""
    # Build title: <program name> @ USB Port <port location> (manufacturer / product name), <image path>
    title_parts = ["tsflash"]
    
    if monitor_port:
        title_parts.append("@ USB Port")
        title_parts.append(monitor_port)
        
        # Get USB device name for the monitor port
        port_info = ports_data.get(monitor_port, {})
        device_parts = []
        if port_info.get('manufacturer'):
            device_parts.append(port_info['manufacturer'])
        if port_info.get('product'):
            device_parts.append(port_info['product'])
        
        if device_parts:
            device_name = " / ".join(device_parts)
            title_parts.append(f"({device_name})")
    
    title_parts.append(config.image_path)
    
    title = " ".join(title_parts)
    
    table = Table(
        show_header=True,
        header_style="bold magenta",
        expand=True,
        box=box.ROUNDED,
        title=title,
        title_style="bold yellow"
    )
    table.add_column("Port", style="cyan", no_wrap=True, ratio=1)
    table.add_column("State", style="bold", ratio=1)
    table.add_column("Progress", ratio=3)
    table.add_column("Device", style="yellow", ratio=1)
    table.add_column("Info", style="dim", ratio=2)
    
    # Get all downstream ports (excluding the monitor port itself)
    if monitor_port:
        downstream_ports = filter_ports_by_limit(ports_data, monitor_port)
        # Remove the monitor port itself from the list
        all_port_strings = {k: v for k, v in downstream_ports.items() if k != monitor_port}
    else:
        all_port_strings = ports_data
    
    # Sort ports for consistent display
    sorted_ports = sorted(all_port_strings.keys())
    
    if not sorted_ports:
        table.add_row("(none)", "[dim]No ports available[/dim]", "", "", "")
    
    for port_str in sorted_ports:
        # Get port state if it exists, otherwise default to NOT_CONNECTED
        port_state = port_states.get(port_str, {})
        state = port_state.get('state', NOT_CONNECTED)
        
        # If port is not in port_states, check if it's actually empty
        if port_str not in port_states:
            port_info = all_port_strings.get(port_str, {})
            # Check if port has any device connected
            if port_info.get('vendor_id') is not None or port_info.get('product_id') is not None:
                # Device connected but not yet tracked - show as UNKNOWN
                state = UNKNOWN
            else:
                # Port is empty
                state = NOT_CONNECTED
        
        state_color = _get_state_color(state)
        
        # Format state name
        state_display = f"[{state_color}]{state.upper()}[/{state_color}]"
        
        # Create progress bar or status
        progress_info = port_state.get('progress', {})
        if state == FLASHING and progress_info:
            percent = progress_info.get('percent', 0.0)
            bytes_written = progress_info.get('bytes_written', 0)
            total_bytes = progress_info.get('total_bytes', 0)
            
            # Create dynamic text-based progress bar
            # Calculate progress column width based on terminal width and column ratios
            # Column ratios: Port(1) + State(1) + Progress(3) + Block Devices(1) + Info(2) = 8 total
            # Progress gets 3/8 = 37.5% of width, minus table borders (~4 chars) and padding
            if console_width:
                # Estimate: table borders ~4 chars, column padding ~2 chars per column
                available_width = console_width - 4 - (5 * 2)  # borders + padding
                progress_col_width = max(10, int(available_width * 3 / 8))  # Progress ratio is 3/8
                # Reserve space for percentage text " 100.0%" (~7 chars)
                bar_width = max(10, progress_col_width - 7)
            else:
                bar_width = 20  # Fallback to fixed width
            
            filled = int(bar_width * percent / 100.0)
            bar = "█" * filled + "░" * (bar_width - filled)
            progress_str = f"[blue]{bar}[/blue] {percent:.1f}%"
            
            # Calculate and show speed and bytes info
            start_time = progress_info.get('start_time', time.time())
            elapsed_time = time.time() - start_time
            
            if total_bytes > 0 and elapsed_time > 0:
                # Calculate speed (bytes per second)
                speed_bps = bytes_written / elapsed_time
                speed_str = _format_bytes(speed_bps) + "/s"
                info_text = f"{_format_bytes(bytes_written)} / {_format_bytes(total_bytes)} @ {speed_str}"
            else:
                info_text = "Initializing..."
        elif state == WAITING:
            detected_time = port_state.get('detected_time', time.time())
            wait_time = time.time() - detected_time
            progress_str = f"[yellow]Waiting ({wait_time:.1f}s)[/yellow]"
            info_text = ""
        elif state == BOOTING:
            # Show rpiboot stage if available
            boot_stage = port_state.get('boot_stage', 'Booting...')
            progress_str = f"[cyan]{boot_stage}[/cyan]"
            info_text = ""
        elif state == COMPLETED:
            progress_str = "[green]✓ Completed[/green]"
            info_text = ""
        elif state == FAILED:
            error = port_state.get('error', 'Unknown error')
            progress_str = f"[red]✗ Failed[/red]"
            info_text = error[:40] + "..." if len(error) > 40 else error
        else:
            progress_str = ""
            info_text = ""
        
        # Block devices - get from port_state if available, otherwise from ports_data
        block_devices = port_state.get('block_devices', [])
        if not block_devices:
            port_info = all_port_strings.get(port_str, {})
            block_devices = port_info.get('block_devices', [])
        block_devices_str = ", ".join(block_devices) if block_devices else "-"
        
        table.add_row(port_str, state_display, progress_str, block_devices_str, info_text)
    
    return table


def _create_log_panel() -> Panel:
    """Create log display panel."""
    with _tui_lock:
        log_lines = list(_tui_log_buffer)
    
    if not log_lines:
        log_content = "[dim]No log messages yet...[/dim]"
    else:
        log_content = "\n".join(log_lines[-30:])  # Show last 30 lines
    
    return Panel(log_content, title="Logs", border_style="green")


def _create_layout(config: DaemonConfig, monitor_port: Optional[str], ports_data: Dict) -> Layout:
    """Create the main TUI layout."""
    layout = Layout()
    
    # Get console width for calculations
    try:
        console = Console()
        console_width = console.width
    except Exception:
        console_width = 80  # Fallback to reasonable default
    
    # Build title to check if it wraps
    title_parts = ["tsflash"]
    if monitor_port:
        title_parts.append("@ USB Port")
        title_parts.append(monitor_port)
        port_info = ports_data.get(monitor_port, {})
        device_parts = []
        if port_info.get('manufacturer'):
            device_parts.append(port_info['manufacturer'])
        if port_info.get('product'):
            device_parts.append(port_info['product'])
        if device_parts:
            device_name = " / ".join(device_parts)
            title_parts.append(f"({device_name})")
    title_parts.append(config.image_path)
    title = " ".join(title_parts)
    
    # Calculate if title wraps (table borders take ~4 chars)
    available_title_width = max(20, console_width - 4)
    title_lines = max(1, (len(title) + available_title_width - 1) // available_title_width)
    
    # Calculate sizes dynamically
    # Ports table: number of ports + table overhead (header row + box borders + title lines)
    # Get all downstream ports (excluding the monitor port itself)
    if monitor_port:
        downstream_ports = filter_ports_by_limit(ports_data, monitor_port)
        all_port_strings = {k: v for k, v in downstream_ports.items() if k != monitor_port}
    else:
        all_port_strings = ports_data
    
    num_ports = len(all_port_strings) if all_port_strings else 1  # At least 1 row for "(none)"
    # Base overhead: borders (2) + separators (2) + header (1) = 5, plus title lines
    ports_size = num_ports + 5 + (title_lines - 1)  # Subtract 1 because base overhead already includes 1 title line
    
    # Split into ports and logs (no header anymore)
    layout.split_column(
        Layout(name="ports", size=ports_size),
        Layout(name="logs"),  # Takes remaining space
    )
    
    # Ports table - use console width for dynamic progress bar sizing
    ports_table = _create_ports_table(_tui_port_states, ports_data, monitor_port, config, console_width)
    layout["ports"].update(ports_table)
    
    # Logs panel
    log_panel = _create_log_panel()
    layout["logs"].update(log_panel)
    
    return layout


def _monitor_devices_tui(monitor_port: str, config: DaemonConfig) -> None:
    """
    Main monitoring loop that watches for devices and flashes them.
    Adapted from daemon.monitor_devices() for TUI use.
    
    Args:
        monitor_port: USB port to monitor (e.g., "1-2")
        config: Daemon configuration
    """
    global _tui_shutdown_requested, _tui_port_states, _tui_flash_executor, _tui_image_mmap
    
    # Create thread pool for parallel flashing
    _tui_flash_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="flash")
    
    logger.info(f"Starting device monitoring on port {monitor_port}")
    logger.info(f"Monitoring for block devices, stable_delay={config.stable_delay}s")
    
    poll_interval = 1.0  # Poll every second
    
    try:
        while not _tui_shutdown_requested:
            try:
                current_time = time.time()
                
                # Enumerate USB ports
                ports_data = enumerate_all_usb_ports()
                
                # Filter to downstream ports
                downstream_ports = filter_ports_by_limit(ports_data, monitor_port)
                current_port_strings = set(downstream_ports.keys())
                
                # Process each downstream port
                for port_str, port_info in downstream_ports.items():
                    # Skip the monitor port itself (it's the hub, not a device)
                    if port_str == monitor_port:
                        continue
                    
                    # Get current state for this port (default to not_connected)
                    port_state = _tui_port_states.get(port_str, {})
                    current_state = port_state.get('state', NOT_CONNECTED)
                    block_devices = port_info.get('block_devices', [])
                    is_rpiboot = is_rpiboot_device(port_info)
                    
                    # State transition logic (same as daemon)
                    if current_state == NOT_CONNECTED:
                        # Port was empty or newly detected
                        if block_devices:
                            # Block device detected directly
                            logger.info(f"Block device detected at port {port_str}: {block_devices}")
                            _tui_port_states[port_str] = {
                                'state': WAITING,
                                'block_devices': block_devices.copy(),
                                'detected_time': current_time,
                                'progress': {},
                                'error': None
                            }
                        elif is_rpiboot:
                            # rpiboot-compatible device detected (no block devices yet)
                            logger.info(f"rpiboot-compatible device detected at port {port_str}, starting rpiboot")
                            _tui_port_states[port_str] = {
                                'state': BOOTING,
                                'block_devices': [],
                                'detected_time': current_time,
                                'progress': {},
                                'error': None,
                                'boot_stage': 'Starting rpiboot...'
                            }
                            
                            # Execute rpiboot in background thread with stage tracking
                            def handle_rpiboot_completion(port: str, states: Dict[str, dict]):
                                # Create stage callback to update boot_stage in port state
                                def stage_callback(stage: str):
                                    if port in states:
                                        states[port]['boot_stage'] = stage
                                
                                try:
                                    success = boot_rpiboot_device(port, timeout=60.0, stage_callback=stage_callback)
                                    if port in states:
                                        if success:
                                            logger.info(f"rpiboot completed successfully for port {port}, waiting for block device")
                                        else:
                                            states[port]['state'] = FAILED
                                            states[port]['error'] = 'rpiboot failed'
                                            logger.error(f"rpiboot failed for port {port}")
                                except Exception as e:
                                    if port in states:
                                        states[port]['state'] = FAILED
                                        states[port]['error'] = str(e)
                                    logger.error(f"Unexpected error during rpiboot for port {port}: {e}")
                            
                            # Start rpiboot in background thread
                            threading.Thread(
                                target=handle_rpiboot_completion,
                                args=(port_str, _tui_port_states),
                                daemon=True
                            ).start()
                        elif port_info.get('vendor_id') is not None or port_info.get('product_id') is not None:
                            # Device connected but not rpiboot and no block devices
                            _tui_port_states[port_str] = {
                                'state': UNKNOWN,
                                'block_devices': [],
                                'detected_time': current_time,
                                'progress': {},
                                'error': None
                            }
                    
                    elif current_state == BOOTING:
                        # rpiboot is running - check if block device appeared
                        if block_devices:
                            # Block device appeared after rpiboot
                            logger.info(f"Block device appeared at port {port_str} after rpiboot: {block_devices}")
                            _tui_port_states[port_str]['state'] = WAITING
                            _tui_port_states[port_str]['block_devices'] = block_devices.copy()
                            _tui_port_states[port_str]['detected_time'] = current_time
                    
                    elif current_state == WAITING:
                        # Block device detected, waiting for stable_delay
                        _tui_port_states[port_str]['block_devices'] = block_devices.copy()
                        
                        if not block_devices:
                            # Block device disappeared while waiting
                            logger.warning(f"Block device disappeared at port {port_str} while waiting")
                            _tui_port_states[port_str]['state'] = NOT_CONNECTED
                        else:
                            # Check if stable_delay has elapsed
                            detected_time = _tui_port_states[port_str].get('detected_time', current_time)
                            time_since_detection = current_time - detected_time
                            
                            if time_since_detection >= config.stable_delay:
                                # Device is stable, start flashing
                                logger.info(f"Device at port {port_str} is stable, starting flash operation")
                                
                                if not _tui_image_mmap:
                                    logger.error("Memory-mapped image not available, cannot flash device")
                                    _tui_port_states[port_str]['state'] = FAILED
                                    _tui_port_states[port_str]['error'] = 'Image not available'
                                    continue
                                
                                # Update state to FLASHING before submitting jobs
                                _tui_port_states[port_str]['state'] = FLASHING
                                _tui_port_states[port_str]['progress'] = {
                                    'bytes_written': 0,
                                    'total_bytes': 0,
                                    'percent': 0.0,
                                    'start_time': current_time
                                }
                                
                                # Flash all block devices on this port
                                for device in block_devices:
                                    future = _tui_flash_executor.submit(
                                        flash_device,
                                        _tui_image_mmap,
                                        device,
                                        config.block_size,
                                        config.image_path,
                                        port_str,
                                        _tui_port_states
                                    )
                    
                    elif current_state == FLASHING:
                        # Flashing in progress - state updated by flash_device callback
                        _tui_port_states[port_str]['block_devices'] = block_devices.copy()
                    
                    elif current_state == COMPLETED:
                        # Flash completed - wait for device removal
                        _tui_port_states[port_str]['block_devices'] = block_devices.copy()
                        if not block_devices:
                            # Device removed after completion
                            logger.info(f"Flashed device at port {port_str} has been removed")
                            _tui_port_states.pop(port_str, None)
                    
                    elif current_state == FAILED:
                        # Failed state - keep tracking until device is removed
                        _tui_port_states[port_str]['block_devices'] = block_devices.copy()
                        if not block_devices:
                            # Device removed after failure
                            logger.debug(f"Failed device at port {port_str} has been removed")
                            _tui_port_states.pop(port_str, None)
                    
                    elif current_state == UNKNOWN:
                        # Unknown device - check if it got block devices or was removed
                        if block_devices:
                            # Block device appeared - transition to waiting
                            logger.info(f"Block device appeared at port {port_str}: {block_devices}")
                            _tui_port_states[port_str]['state'] = WAITING
                            _tui_port_states[port_str]['block_devices'] = block_devices.copy()
                            _tui_port_states[port_str]['detected_time'] = current_time
                        elif port_info.get('vendor_id') is None and port_info.get('product_id') is None:
                            # Device removed
                            _tui_port_states.pop(port_str, None)
                
                # Clean up ports that no longer exist
                for port_str in list(_tui_port_states.keys()):
                    if port_str not in current_port_strings:
                        logger.debug(f"Port {port_str} no longer present, cleaning up")
                        _tui_port_states.pop(port_str, None)
                
            except Exception as e:
                logger.error(f"Error during monitoring cycle: {e}", exc_info=True)
            
            # Sleep until next poll
            time.sleep(poll_interval)
    
    finally:
        # Shutdown requested
        flashing_count = sum(1 for state in _tui_port_states.values() if state.get('state') == FLASHING)
        if flashing_count > 0:
            logger.info(f"Shutdown requested, interrupting {flashing_count} in-progress flash operation(s)...")
        else:
            logger.info("Shutdown requested")
        
        if _tui_flash_executor:
            _tui_flash_executor.shutdown(wait=False)
        
        # Clean up memory-mapped image
        if _tui_image_mmap:
            try:
                _tui_image_mmap.close()
            except Exception:
                pass
            _tui_image_mmap = None


def run_tui(config_path: Optional[str] = None) -> int:
    """
    Run the text-based UI for monitoring tsflash operations.
    
    Args:
        config_path: Path to config file (optional)
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    global _tui_shutdown_requested, _tui_image_mmap, _tui_flash_executor
    
    try:
        # Load configuration
        config = load_config(config_path)
        
        # Setup logging with TUI handler only (disable stdout/stderr)
        # Remove all existing handlers to prevent stdout/stderr output
        root_logger = logging.getLogger()
        
        # Clear all handlers from root logger and all child loggers
        root_logger.handlers.clear()
        root_logger.propagate = False  # Disable propagation to prevent any parent loggers
        
        # Also clear handlers from common child loggers that might have been configured
        for logger_name in ['tsflash', 'tsflash.daemon', 'tsflash.usb', 'tsflash.flash', 'tsflash.rpiboot']:
            child_logger = logging.getLogger(logger_name)
            child_logger.handlers.clear()
            child_logger.propagate = True  # Allow propagation to root so TUI handler catches it
        
        # Create and configure TUI handler
        tui_handler = TUIHandler()
        tui_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', 
                                                   datefmt='%Y-%m-%d %H:%M:%S'))
        
        # Set log level from config
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL,
        }
        tui_handler.setLevel(level_map.get(config.log_level, logging.INFO))
        
        # Add only the TUI handler to root logger
        root_logger.addHandler(tui_handler)
        root_logger.setLevel(level_map.get(config.log_level, logging.INFO))
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        
        # Create memory-mapped image
        try:
            _tui_image_mmap = create_image_mmap(config.image_path)
            logger.info("Image file memory-mapped for efficient parallel flashing")
        except IOError as e:
            logger.warning(f"Could not create memory-mapped image: {e}")
            logger.warning("Falling back to file-based access (may be slower for parallel flashing)")
            _tui_image_mmap = None
        
        # Find monitor port
        ports_data = enumerate_all_usb_ports()
        monitor_port = find_monitor_port(ports_data, config.port)
        
        if monitor_port is None:
            logger.error("Could not determine port to monitor")
            if _tui_image_mmap:
                try:
                    _tui_image_mmap.close()
                except Exception:
                    pass
                _tui_image_mmap = None
            return 1
        
        # Start monitoring in background thread
        monitor_thread = threading.Thread(
            target=_monitor_devices_tui,
            args=(monitor_port, config),
            daemon=True
        )
        monitor_thread.start()
        
        # Give monitoring thread a moment to start
        time.sleep(0.5)
        
        # Create console and run live display
        console = Console()
        
        try:
            with Live(console=console, screen=True, refresh_per_second=2) as live:
                while not _tui_shutdown_requested:
                    # Get current ports data for display
                    try:
                        ports_data = enumerate_all_usb_ports()
                    except Exception:
                        ports_data = {}
                    
                    # Update layout
                    layout = _create_layout(config, monitor_port, ports_data)
                    live.update(layout)
                    
                    # Check if monitor thread is still alive
                    if not monitor_thread.is_alive():
                        break
                    
                    time.sleep(0.5)  # Update every 0.5 seconds
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            _tui_shutdown_requested = True
            # Wait a bit for cleanup
            monitor_thread.join(timeout=2.0)
        
        # Cleanup
        if _tui_image_mmap:
            try:
                _tui_image_mmap.close()
            except Exception:
                pass
            _tui_image_mmap = None
        
        logger.info("TUI shutting down")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1
