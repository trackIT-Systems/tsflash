"""Daemon for automatically flashing devices on USB downstream ports."""

import logging
import mmap
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set

from .config import DaemonConfig, load_config
from .flash import flash_image, create_image_mmap
from .usb import enumerate_all_usb_ports, filter_ports_by_limit, find_first_usb_hub, is_rpiboot_device
from .rpiboot import run_rpiboot

logger = logging.getLogger(__name__)

# Port state constants
NOT_CONNECTED = 'not_connected'
UNKNOWN = 'unknown'
BOOTING = 'booting'
WAITING = 'waiting'
FLASHING = 'flashing'
COMPLETED = 'completed'
FAILED = 'failed'

# Global flag for graceful shutdown
_shutdown_requested = False
_flash_executor: Optional[ThreadPoolExecutor] = None
_image_mmap = None  # MappedImage wrapper


def _signal_handler(signum, frame):
    """Handle shutdown signals."""
    global _shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    _shutdown_requested = True


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
        # Use specified port
        if config_port in ports_data:
            logger.info(f"Using specified port: {config_port}")
            return config_port
        else:
            logger.warning(f"Specified port '{config_port}' not found")
            return None
    else:
        # Auto-detect first hub
        first_hub = find_first_usb_hub(ports_data)
        if first_hub:
            logger.info(f"Auto-detected first hub: {first_hub}")
            return first_hub
        else:
            logger.warning("No USB hub found for auto-detection")
            return None


def get_downstream_block_devices(ports_data: Dict, monitor_port: str) -> List[str]:
    """
    Get all block devices on downstream ports of the monitor port.
    
    Args:
        ports_data: Dictionary of all USB ports
        monitor_port: Port to monitor (e.g., "1-2")
        
    Returns:
        List of block device paths (e.g., ["/dev/sda", "/dev/sdb"])
    """
    # Filter to downstream ports
    downstream_ports = filter_ports_by_limit(ports_data, monitor_port)
    
    # Collect all block devices
    block_devices = []
    for port_str, port_info in downstream_ports.items():
        # Skip the monitor port itself (it's the hub, not a device)
        if port_str == monitor_port:
            continue
        
        # Collect block devices from this port
        for block_device in port_info.get('block_devices', []):
            if block_device not in block_devices:
                block_devices.append(block_device)
    
    return sorted(block_devices)


def find_port_for_block_device(ports_data: Dict, block_device: str, monitor_port: str) -> Optional[str]:
    """
    Find the USB port that owns a given block device.
    
    Args:
        ports_data: Dictionary of all USB ports
        block_device: Block device path (e.g., "/dev/sda")
        monitor_port: Port to monitor (e.g., "1-2")
        
    Returns:
        Port string if found, None otherwise
    """
    # Filter to downstream ports
    downstream_ports = filter_ports_by_limit(ports_data, monitor_port)
    
    # Search for the block device
    for port_str, port_info in downstream_ports.items():
        # Skip the monitor port itself (it's the hub, not a device)
        if port_str == monitor_port:
            continue
        
        # Check if this port has the block device
        if block_device in port_info.get('block_devices', []):
            return port_str
    
    return None


def flash_device(mapped_image, device: str, block_size: str, image_path: str, 
                 port_str: str, port_states: Dict[str, dict]) -> bool:
    """
    Flash a device with the configured image.
    
    Args:
        mapped_image: MappedImage wrapper containing the memory-mapped image
        device: Block device path (e.g., "/dev/sda")
        block_size: Block size for flashing (e.g., "4M")
        image_path: Path string for logging
        port_str: USB port string for this device
        port_states: Shared dictionary to update with port state information
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting flash operation for {device} on port {port_str}")
    
    # Initialize progress tracking in port state
    if port_str not in port_states:
        port_states[port_str] = {
            'state': FLASHING,
            'block_devices': [],
            'detected_time': time.time(),
            'progress': {},
            'error': None
        }
    
    port_states[port_str]['state'] = FLASHING
    port_states[port_str]['progress'] = {
        'bytes_written': 0,
        'total_bytes': 0,
        'percent': 0.0,
        'start_time': time.time()
    }
    
    def progress_callback(bytes_written: int, total_bytes: int, percent: float):
        """Callback to update progress in port state."""
        if port_str in port_states:
            port_states[port_str]['progress'].update({
                'bytes_written': bytes_written,
                'total_bytes': total_bytes,
                'percent': percent
            })
            port_states[port_str]['state'] = FLASHING
    
    try:
        # Use non-interactive mode to avoid progress bar interfering with logs
        flash_image(mapped_image, device, block_size, non_interactive=True, 
                   image_path=image_path, progress_callback=progress_callback)
        
        # Mark as completed
        if port_str in port_states:
            port_states[port_str]['state'] = COMPLETED
            port_states[port_str]['progress']['percent'] = 100.0
            port_states[port_str]['error'] = None
        
        logger.info(f"Successfully flashed {device} on port {port_str}")
        return True
    except Exception as e:
        # Mark as failed
        if port_str in port_states:
            port_states[port_str]['state'] = FAILED
            port_states[port_str]['error'] = str(e)
        logger.error(f"Failed to flash {device} on port {port_str}: {e}")
        return False


def cleanup_image_mmap() -> None:
    """Clean up the memory-mapped image file."""
    global _image_mmap
    
    if _image_mmap:
        try:
            _image_mmap.close()
            logger.debug("Closed memory-mapped image file")
        except Exception as e:
            logger.warning(f"Error closing memory-mapped image: {e}")
        _image_mmap = None


def boot_rpiboot_device(port: str, timeout: float = 60.0, stage_callback=None) -> bool:
    """
    Boot a rpiboot-compatible device into mass storage mode.
    
    Args:
        port: USB port pathname to target (e.g., "1-2.3")
        timeout: Timeout in seconds for rpiboot operation (default: 60.0)
        stage_callback: Optional callback function(stage: str) -> None called with
                        stage updates during boot process.
        
    Returns:
        True if rpiboot completed successfully, False otherwise
    """
    logger.info(f"Starting rpiboot for device at port {port}")
    
    try:
        # Use threading to implement timeout since run_rpiboot uses subprocess.Popen
        # We'll wrap run_rpiboot in a thread and use a timeout
        import threading
        
        result_container = {'success': False, 'completed': False, 'exit_code': 1}
        exception_container = {'exception': None}
        
        def run_rpiboot_thread():
            try:
                # We need to modify run_rpiboot to return the process, but for now
                # we'll use a simpler approach: call run_rpiboot and handle timeout
                # by checking completion status
                success, exit_code = run_rpiboot(port=port, verbose=False, stage_callback=stage_callback)
                result_container['success'] = success
                result_container['exit_code'] = exit_code
            except Exception as e:
                exception_container['exception'] = e
            finally:
                result_container['completed'] = True
        
        thread = threading.Thread(target=run_rpiboot_thread, daemon=True)
        thread.start()
        thread.join(timeout=timeout)
        
        if not result_container['completed']:
            logger.error(f"rpiboot timed out after {timeout} seconds for port {port}")
            # Note: The rpiboot process may still be running, but we've timed out
            # The process will continue in the background and may complete later
            return False
        
        if exception_container['exception']:
            logger.error(f"rpiboot raised exception for port {port}: {exception_container['exception']}")
            return False
        
        if result_container['success']:
            logger.info(f"rpiboot completed successfully for port {port}")
            return True
        else:
            exit_code = result_container.get('exit_code', 1)
            logger.error(f"rpiboot failed for port {port} (exit code: {exit_code})")
            return False
            
    except Exception as e:
        logger.error(f"Unexpected error during rpiboot for port {port}: {e}")
        return False


def monitor_devices(monitor_port: str, config: DaemonConfig) -> None:
    """
    Main monitoring loop that watches for devices and flashes them.
    
    Args:
        monitor_port: USB port to monitor (e.g., "1-2")
        config: Daemon configuration
    """
    global _shutdown_requested, _flash_executor, _image_mmap
    
    # Unified port state tracking
    # Each port has: state, block_devices, detected_time, progress, error
    port_states: Dict[str, dict] = {}
    
    # Create thread pool for parallel flashing
    _flash_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="flash")
    
    logger.info(f"Starting device monitoring on port {monitor_port}")
    logger.info(f"Monitoring for block devices, stable_delay={config.stable_delay}s")
    
    poll_interval = 1.0  # Poll every second
    
    try:
        while not _shutdown_requested:
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
                    port_state = port_states.get(port_str, {})
                    current_state = port_state.get('state', NOT_CONNECTED)
                    block_devices = port_info.get('block_devices', [])
                    is_rpiboot = is_rpiboot_device(port_info)
                    
                    # State transition logic
                    if current_state == NOT_CONNECTED:
                        # Port was empty or newly detected
                        if block_devices:
                            # Block device detected directly
                            logger.info(f"Block device detected at port {port_str}: {block_devices}")
                            port_states[port_str] = {
                                'state': WAITING,
                                'block_devices': block_devices.copy(),
                                'detected_time': current_time,
                                'progress': {},
                                'error': None
                            }
                        elif is_rpiboot:
                            # rpiboot-compatible device detected (no block devices yet)
                            logger.info(f"rpiboot-compatible device detected at port {port_str}, starting rpiboot")
                            port_states[port_str] = {
                                'state': BOOTING,
                                'block_devices': [],
                                'detected_time': current_time,
                                'progress': {},
                                'error': None
                            }
                            
                            # Execute rpiboot in background thread
                            def handle_rpiboot_completion(port: str, states: Dict[str, dict]):
                                try:
                                    success = boot_rpiboot_device(port, timeout=60.0)
                                    if port in states:
                                        if success:
                                            # rpiboot completed - state will transition to WAITING when block device appears
                                            logger.info(f"rpiboot completed successfully for port {port}, waiting for block device")
                                            # Don't change state here - let the next poll detect the block device
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
                                args=(port_str, port_states),
                                daemon=True
                            ).start()
                        elif port_info.get('vendor_id') is not None or port_info.get('product_id') is not None:
                            # Device connected but not rpiboot and no block devices
                            port_states[port_str] = {
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
                            port_states[port_str]['state'] = WAITING
                            port_states[port_str]['block_devices'] = block_devices.copy()
                            port_states[port_str]['detected_time'] = current_time
                        # If still booting and no block devices, keep waiting
                    
                    elif current_state == WAITING:
                        # Block device detected, waiting for stable_delay
                        # Update block devices list in case it changed
                        port_states[port_str]['block_devices'] = block_devices.copy()
                        
                        if not block_devices:
                            # Block device disappeared while waiting
                            logger.warning(f"Block device disappeared at port {port_str} while waiting")
                            port_states[port_str]['state'] = NOT_CONNECTED
                        else:
                            # Check if stable_delay has elapsed
                            detected_time = port_states[port_str].get('detected_time', current_time)
                            time_since_detection = current_time - detected_time
                            
                            if time_since_detection >= config.stable_delay:
                                # Device is stable, start flashing
                                logger.info(f"Device at port {port_str} is stable, starting flash operation")
                                
                                if not _image_mmap:
                                    logger.error("Memory-mapped image not available, cannot flash device")
                                    port_states[port_str]['state'] = FAILED
                                    port_states[port_str]['error'] = 'Image not available'
                                    continue
                                
                                # Update state to FLASHING before submitting jobs
                                port_states[port_str]['state'] = FLASHING
                                port_states[port_str]['progress'] = {
                                    'bytes_written': 0,
                                    'total_bytes': 0,
                                    'percent': 0.0,
                                    'start_time': current_time
                                }
                                
                                # Flash all block devices on this port
                                for device in block_devices:
                                    future = _flash_executor.submit(
                                        flash_device,
                                        _image_mmap,
                                        device,
                                        config.block_size,
                                        config.image_path,
                                        port_str,
                                        port_states
                                    )
                    
                    elif current_state == FLASHING:
                        # Flashing in progress - state updated by flash_device callback
                        # Update block devices list in case it changed
                        port_states[port_str]['block_devices'] = block_devices.copy()
                    
                    elif current_state == COMPLETED:
                        # Flash completed - wait for device removal
                        port_states[port_str]['block_devices'] = block_devices.copy()
                        if not block_devices:
                            # Device removed after completion
                            logger.info(f"Flashed device at port {port_str} has been removed")
                            port_states.pop(port_str, None)
                    
                    elif current_state == FAILED:
                        # Failed state - keep tracking until device is removed
                        port_states[port_str]['block_devices'] = block_devices.copy()
                        if not block_devices:
                            # Device removed after failure
                            logger.debug(f"Failed device at port {port_str} has been removed")
                            port_states.pop(port_str, None)
                    
                    elif current_state == UNKNOWN:
                        # Unknown device - check if it got block devices or was removed
                        if block_devices:
                            # Block device appeared - transition to waiting
                            logger.info(f"Block device appeared at port {port_str}: {block_devices}")
                            port_states[port_str]['state'] = WAITING
                            port_states[port_str]['block_devices'] = block_devices.copy()
                            port_states[port_str]['detected_time'] = current_time
                        elif port_info.get('vendor_id') is None and port_info.get('product_id') is None:
                            # Device removed
                            port_states.pop(port_str, None)
                
                # Clean up ports that no longer exist
                for port_str in list(port_states.keys()):
                    if port_str not in current_port_strings:
                        logger.debug(f"Port {port_str} no longer present, cleaning up")
                        port_states.pop(port_str, None)
                
            except Exception as e:
                logger.error(f"Error during monitoring cycle: {e}", exc_info=True)
            
            # Sleep until next poll
            time.sleep(poll_interval)
    
    finally:
        # Shutdown requested - interrupt in-progress flashes
        flashing_count = sum(1 for state in port_states.values() if state.get('state') == FLASHING)
        if flashing_count > 0:
            logger.info(f"Shutdown requested, interrupting {flashing_count} in-progress flash operation(s)...")
        else:
            logger.info("Shutdown requested")
        
        if _flash_executor:
            # Shutdown executor without waiting - this prevents new tasks and
            # allows the daemon to exit immediately. Running flash operations
            # will be interrupted when the process exits (they're daemon threads).
            _flash_executor.shutdown(wait=False)
        
        # Clean up memory-mapped image
        cleanup_image_mmap()


def setup_logging(log_level: str) -> None:
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Map string level to logging constant
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
    }
    
    level = level_map.get(log_level, logging.INFO)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def run_daemon(config_path: Optional[str] = None) -> int:
    """
    Main daemon entry point.
    
    Args:
        config_path: Path to config file (optional)
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    global _shutdown_requested
    
    try:
        # Load configuration
        config = load_config(config_path)
        
        # Setup logging (only if not already configured)
        if not logging.getLogger().handlers:
            setup_logging(config.log_level)
        else:
            # Logging already configured (e.g., by CLI flags), just set level
            level_map = {
                'DEBUG': logging.DEBUG,
                'INFO': logging.INFO,
                'WARNING': logging.WARNING,
                'ERROR': logging.ERROR,
                'CRITICAL': logging.CRITICAL,
            }
            level = level_map.get(config.log_level, logging.INFO)
            logging.getLogger().setLevel(level)
        
        logger.info("tsflashd starting...")
        logger.info(f"Image: {config.image_path}")
        logger.info(f"Block size: {config.block_size}")
        logger.info(f"Stable delay: {config.stable_delay}s")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        
        # Create memory-mapped image for efficient parallel access
        global _image_mmap
        try:
            _image_mmap = create_image_mmap(config.image_path)
            logger.info("Image file memory-mapped for efficient parallel flashing")
        except IOError as e:
            logger.warning(f"Could not create memory-mapped image: {e}")
            logger.warning("Falling back to file-based access (may be slower for parallel flashing)")
            _image_mmap = None
        
        # Find monitor port
        ports_data = enumerate_all_usb_ports()
        monitor_port = find_monitor_port(ports_data, config.port)
        
        if monitor_port is None:
            error_msg = "Could not determine port to monitor"
            if config.port:
                error_msg += f" (specified port '{config.port}' not found)"
            else:
                error_msg += " (no USB hub found for auto-detection)"
            logger.error(error_msg)
            cleanup_image_mmap()
            return 1
        
        try:
            # Start monitoring
            monitor_devices(monitor_port, config)
        finally:
            # Ensure cleanup on exit
            cleanup_image_mmap()
        
        logger.info("tsflashd shutting down")
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


def main():
    """CLI entry point for tsflashd."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='tsflashd - Automatic device flashing daemon',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--config',
        metavar='PATH',
        help='Path to configuration file (default: /boot/firmware/tsflash.yml)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output (DEBUG level logging) - overrides config log_level'
    )
    
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Enable quiet output (WARNING level logging) - overrides config log_level'
    )
    
    args = parser.parse_args()
    
    # If verbose/quiet flags are set, setup logging first
    # Otherwise, logging will be set up in run_daemon based on config
    if args.verbose:
        setup_logging('DEBUG')
    elif args.quiet:
        setup_logging('WARNING')
    
    # Run daemon (will setup logging from config if not already set)
    sys.exit(run_daemon(args.config))
