"""USB port monitoring state machine shared by the daemon and the TUI.

A PortMonitor watches all ports downstream of one USB hub port and drives each
port through these states:

    NOT_CONNECTED -> WAITING -> FLASHING -> COMPLETED / FAILED
                  -> BOOTING (rpiboot) -> WAITING -> ...
                  -> UNKNOWN  (device without usable block device, e.g. empty card reader)
                  -> IGNORED  (device tsflash refuses to handle)

Rules:
- Block devices with size 0 (no medium) are not considered flashable.
- Disks holding /, /boot or /boot/firmware are never flashed (IGNORED).
- Ports exposing more than one flashable block device are not handled (IGNORED).
- A failed flash stays FAILED until the flashed block device disappears
  (card removed); a failed rpiboot stays FAILED until the device is unplugged.
"""

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Callable, Dict, List, Optional, Set

from .flash import FlashAborted, flash_image
from .rpiboot import run_rpiboot
from .usb import (
    block_device_size,
    enumerate_all_usb_ports,
    filter_ports_by_limit,
    find_first_usb_hub,
    is_rpiboot_device,
)
from .validators import get_system_disks

logger = logging.getLogger(__name__)

# Port state constants
NOT_CONNECTED = 'not_connected'
UNKNOWN = 'unknown'
BOOTING = 'booting'
WAITING = 'waiting'
FLASHING = 'flashing'
COMPLETED = 'completed'
FAILED = 'failed'
IGNORED = 'ignored'


def find_monitor_port(ports_data: Dict, config_port: Optional[str]) -> Optional[str]:
    """
    Find the USB port to monitor.

    Args:
        ports_data: Dictionary of all USB ports
        config_port: Port specified in config, or None for auto-detection

    Returns:
        Port string to monitor, or None if not found
    """
    if config_port:
        if config_port in ports_data:
            logger.info(f"Using specified port: {config_port}")
            return config_port
        logger.warning(f"Specified port '{config_port}' not found")
        return None

    first_hub = find_first_usb_hub(ports_data)
    if first_hub:
        logger.info(f"Auto-detected hub: {first_hub}")
        return first_hub
    logger.warning("No USB hub found for auto-detection")
    return None


def boot_rpiboot_device(port: str, timeout: float = 60.0, stage_callback=None,
                        stop_event=None) -> bool:
    """
    Boot a rpiboot-compatible device into mass storage mode.

    Args:
        port: USB port pathname to target (e.g., "1-2.3")
        timeout: Timeout in seconds for rpiboot operation (default: 60.0)
        stage_callback: Optional callback function(stage: str) -> None
        stop_event: Optional threading.Event to abort rpiboot

    Returns:
        True if rpiboot completed successfully, False otherwise
    """
    logger.info(f"Starting rpiboot for device at port {port}")
    try:
        success, exit_code = run_rpiboot(port=port, verbose=False, stage_callback=stage_callback,
                                         timeout=timeout, stop_event=stop_event)
    except Exception as e:
        logger.error(f"Unexpected error during rpiboot for port {port}: {e}")
        return False

    if success:
        logger.info(f"rpiboot completed successfully for port {port}")
    else:
        logger.error(f"rpiboot failed for port {port} (exit code: {exit_code})")
    return success


def _is_present(port_info: dict) -> bool:
    """Whether any USB device is connected at this port."""
    return port_info.get('vendor_id') is not None or port_info.get('product_id') is not None


class PortMonitor:
    """Watches downstream USB ports and flashes connected devices."""

    def __init__(self, monitor_port: str, config, image, *,
                 enumerate_ports: Callable[[], Dict] = enumerate_all_usb_ports,
                 boot_device: Callable[..., bool] = boot_rpiboot_device,
                 flash: Callable[..., None] = flash_image,
                 system_disks: Callable[[], Set[str]] = get_system_disks,
                 device_size: Callable[[str], Optional[int]] = block_device_size,
                 poll_interval: float = 1.0,
                 rpiboot_timeout: float = 60.0,
                 max_workers: int = 10):
        """
        Args:
            monitor_port: USB hub port to monitor (e.g., "1-2")
            config: DaemonConfig (uses image_path, block_size, stable_delay)
            image: MappedImage to flash (owned by the caller)
            enumerate_ports, boot_device, flash, system_disks, device_size:
                Injectable dependencies (for testing)
            poll_interval: Seconds between USB scans
            rpiboot_timeout: Seconds before an rpiboot attempt is aborted
            max_workers: Maximum number of parallel flash operations
        """
        self.monitor_port = monitor_port
        self.config = config
        self.image = image
        self.poll_interval = poll_interval
        self.rpiboot_timeout = rpiboot_timeout

        self._enumerate_ports = enumerate_ports
        self._boot_device = boot_device
        self._flash = flash
        self._system_disks = system_disks
        self._device_size = device_size

        self._states: Dict[str, dict] = {}
        self._ports_data: Dict = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()      # stops the monitoring loop and rpiboot
        self._cancel = threading.Event()    # aborts running flash operations
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="flash")
        self._futures = []

    # ------------------------------------------------------------------ public API

    def run(self) -> None:
        """Run the monitoring loop until stop() is called."""
        logger.info(f"Starting device monitoring on port {self.monitor_port}")
        logger.info(f"Monitoring for block devices, stable_delay={self.config.stable_delay}s")
        while not self._stop.is_set():
            try:
                self.poll()
            except Exception as e:
                logger.error(f"Error during monitoring cycle: {e}", exc_info=True)
            self._stop.wait(self.poll_interval)

    def stop(self) -> None:
        """Request the monitoring loop (and running rpiboot processes) to stop."""
        self._stop.set()

    def shutdown(self, timeout: float = 10.0) -> None:
        """
        Stop monitoring and abort running flash operations.

        Aborted devices are left incomplete and marked FAILED. Waits up to
        `timeout` seconds for flash threads to finish their current chunk.
        """
        self._stop.set()
        active = self.active_flash_count()
        if active:
            logger.warning(f"Aborting {active} in-progress flash operation(s)...")
        self._cancel.set()
        with self._lock:
            futures = list(self._futures)
        if futures:
            wait(futures, timeout=timeout)
        self._executor.shutdown(wait=False)

    def snapshot(self):
        """
        Get a consistent copy of the current state for display.

        Returns:
            tuple: (port_states, ports_data) - port_states maps port strings to
            copies of their state dicts, ports_data is the last USB scan.
        """
        with self._lock:
            states = {}
            for port_str, state in self._states.items():
                copied = dict(state)
                copied['progress'] = dict(state.get('progress') or {})
                copied['block_devices'] = list(state.get('block_devices') or [])
                states[port_str] = copied
            return states, self._ports_data

    def active_flash_count(self) -> int:
        """Number of ports currently being flashed."""
        with self._lock:
            return sum(1 for s in self._states.values() if s.get('state') == FLASHING)

    def poll(self) -> Dict:
        """Run one monitoring cycle. Returns the USB scan used."""
        ports_data = self._enumerate_ports()
        downstream = filter_ports_by_limit(ports_data, self.monitor_port)
        system_disks = self._system_disks()
        now = time.time()

        with self._lock:
            self._ports_data = ports_data
            for port_str, port_info in downstream.items():
                if port_str == self.monitor_port:
                    continue  # the hub itself
                self._update_port(port_str, port_info, system_disks, now)

            for port_str in list(self._states):
                if port_str not in downstream:
                    logger.debug(f"Port {port_str} no longer present, cleaning up")
                    self._states.pop(port_str, None)

        return ports_data

    # ------------------------------------------------------------ state machine

    def _flashable(self, block_devices: List[str], system_disks: Set[str]):
        """Split block devices into (flashable, system) lists."""
        system = [d for d in block_devices if os.path.basename(d) in system_disks]
        flashable = [d for d in block_devices
                     if d not in system and (self._device_size(d) or 0) > 0]
        return flashable, system

    def _set(self, port_str: str, state: str, now: float, **fields) -> dict:
        """Replace a port's state with a fresh state dict."""
        new = {
            'state': state,
            'block_devices': [],
            'detected_time': now,
            'progress': {},
            'error': None,
        }
        new.update(fields)
        self._states[port_str] = new
        return new

    def _evaluate(self, port_str: str, port_info: dict, system_disks: Set[str], now: float,
                  current: Optional[dict] = None) -> None:
        """Decide the state of a port that has no operation in progress."""
        block_devices = port_info.get('block_devices', [])
        flashable, system = self._flashable(block_devices, system_disks)
        current = current or {}

        if system:
            reason = f"System disk ({', '.join(system)}) - never flashed"
            if current.get('state') != IGNORED or current.get('error') != reason:
                logger.warning(f"Ignoring port {port_str}: {reason}")
            self._set(port_str, IGNORED, now, block_devices=block_devices, error=reason)
        elif len(flashable) > 1:
            reason = f"Multiple block devices ({', '.join(flashable)}) - not supported"
            if current.get('state') != IGNORED or current.get('error') != reason:
                logger.warning(f"Ignoring port {port_str}: {reason}")
            self._set(port_str, IGNORED, now, block_devices=block_devices, error=reason)
        elif flashable:
            logger.info(f"Block device detected at port {port_str}: {flashable}")
            self._set(port_str, WAITING, now, block_devices=flashable,
                      rpiboot=current.get('rpiboot', False))
        elif is_rpiboot_device(port_info):
            logger.info(f"rpiboot-compatible device detected at port {port_str}, starting rpiboot")
            state = self._set(port_str, BOOTING, now, rpiboot=True, rpiboot_done=False,
                              boot_stage='Starting rpiboot...')
            threading.Thread(target=self._rpiboot_job, args=(port_str, state),
                             daemon=True, name=f"rpiboot-{port_str}").start()
        elif _is_present(port_info):
            error = "No medium" if block_devices else None
            if current.get('state') == UNKNOWN:
                current['block_devices'] = list(block_devices)
                current['error'] = error
            else:
                self._set(port_str, UNKNOWN, now, block_devices=block_devices, error=error)
        else:
            self._states.pop(port_str, None)

    def _update_port(self, port_str: str, port_info: dict, system_disks: Set[str],
                     now: float) -> None:
        current = self._states.get(port_str)
        state = current.get('state', NOT_CONNECTED) if current else NOT_CONNECTED
        block_devices = port_info.get('block_devices', [])
        present = _is_present(port_info)

        if state in (NOT_CONNECTED, UNKNOWN, IGNORED):
            # Stateless - re-evaluate every poll
            self._evaluate(port_str, port_info, system_disks, now, current)

        elif state == BOOTING:
            flashable, system = self._flashable(block_devices, system_disks)
            if flashable or system:
                logger.info(f"Block device appeared at port {port_str} after rpiboot: {block_devices}")
                self._evaluate(port_str, port_info, system_disks, now, current)
            elif current.get('rpiboot_done') and not present:
                # rpiboot finished, port is empty (unplugged or re-enumerating)
                logger.debug(f"Port {port_str} empty after rpiboot, resetting state")
                self._states.pop(port_str, None)

        elif state == WAITING:
            flashable, _ = self._flashable(block_devices, system_disks)
            if flashable != current['block_devices']:
                if not flashable:
                    logger.warning(f"Block device disappeared at port {port_str} while waiting")
                # Device set changed - start over
                self._evaluate(port_str, port_info, system_disks, now, current)
            elif now - current['detected_time'] >= self.config.stable_delay:
                self._start_flash(port_str, current, now)

        elif state == FLASHING:
            pass  # result is set by the flash job

        elif state == COMPLETED:
            # Removed = no longer a flashable device (a card reader keeps its
            # block device, but with size 0, when the card is pulled)
            flashable, _ = self._flashable(block_devices, system_disks)
            if not set(current['block_devices']) & set(flashable):
                logger.info(f"Flashed device at port {port_str} has been removed")
                self._states.pop(port_str, None)

        elif state == FAILED:
            if current.get('rpiboot') and not current['block_devices']:
                # rpiboot failure: keep FAILED until the device is unplugged
                cleared = not present
            else:
                # Flash failure: keep FAILED until the medium is removed
                flashable, _ = self._flashable(block_devices, system_disks)
                cleared = not set(current['block_devices']) & set(flashable)
            if cleared:
                logger.debug(f"Failed device at port {port_str} has been removed")
                self._states.pop(port_str, None)

    # ------------------------------------------------------------- background jobs

    def _rpiboot_job(self, port_str: str, state: dict) -> None:
        # Updates this device's own state dict (not looked up by port), so a
        # removed/replaced port entry is never clobbered.
        def stage_callback(stage: str):
            with self._lock:
                state['boot_stage'] = stage

        try:
            success = self._boot_device(port_str, timeout=self.rpiboot_timeout,
                                        stage_callback=stage_callback, stop_event=self._stop)
        except Exception as e:
            logger.error(f"Unexpected error during rpiboot for port {port_str}: {e}")
            success = False

        with self._lock:
            if success:
                logger.info(f"rpiboot completed for port {port_str}, waiting for block device")
                state['rpiboot_done'] = True
            else:
                state['state'] = FAILED
                state['error'] = 'rpiboot failed'

    def _start_flash(self, port_str: str, state: dict, now: float) -> None:
        device = state['block_devices'][0]
        logger.info(f"Device at port {port_str} is stable, flashing {device}")
        state['state'] = FLASHING
        state['progress'] = {
            'bytes_written': 0,
            'total_bytes': 0,  # set by the first progress update
            'percent': 0.0,
            'start_time': now,
        }
        future = self._executor.submit(self._flash_job, port_str, device, state)
        self._futures = [f for f in self._futures if not f.done()] + [future]

    def _flash_job(self, port_str: str, device: str, state: dict) -> bool:
        def progress_callback(bytes_written: int, total_bytes: int, percent: float):
            with self._lock:
                state['progress'].update({
                    'bytes_written': bytes_written,
                    'total_bytes': total_bytes,
                    'percent': percent,
                })

        logger.info(f"Starting flash operation for {device} on port {port_str}")
        try:
            self._flash(self.image, device, self.config.block_size, non_interactive=True,
                        image_path=self.config.image_path, progress_callback=progress_callback,
                        cancel_event=self._cancel)
        except FlashAborted as e:
            with self._lock:
                state['state'] = FAILED
                state['error'] = 'Aborted - device is incomplete'
            logger.warning(str(e))
            return False
        except Exception as e:
            with self._lock:
                state['state'] = FAILED
                state['error'] = str(e)
            logger.error(f"Failed to flash {device} on port {port_str}: {e}")
            return False

        with self._lock:
            state['state'] = COMPLETED
            state['progress']['percent'] = 100.0
            state['error'] = None
        logger.info(f"Successfully flashed {device} on port {port_str}")
        return True
