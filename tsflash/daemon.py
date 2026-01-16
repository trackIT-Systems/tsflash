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


def flash_device(mapped_image, device: str, block_size: str, image_path: str, 
                 progress_dict: Dict[str, dict]) -> bool:
    """
    Flash a device with the configured image.
    
    Args:
        mapped_image: MappedImage wrapper containing the memory-mapped image
        device: Block device path (e.g., "/dev/sda")
        block_size: Block size for flashing (e.g., "4M")
        image_path: Path string for logging
        progress_dict: Shared dictionary to update with progress information
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting flash operation for {device}")
    
    # Initialize progress tracking
    progress_dict[device] = {
        'status': 'flashing',
        'bytes_written': 0,
        'total_bytes': 0,
        'percent': 0.0,
        'start_time': time.time()
    }
    
    def progress_callback(bytes_written: int, total_bytes: int, percent: float):
        """Callback to update progress dictionary."""
        if device in progress_dict:
            progress_dict[device].update({
                'bytes_written': bytes_written,
                'total_bytes': total_bytes,
                'percent': percent,
                'status': 'flashing'
            })
    
    try:
        # Use non-interactive mode to avoid progress bar interfering with logs
        flash_image(mapped_image, device, block_size, non_interactive=True, 
                   image_path=image_path, progress_callback=progress_callback)
        
        # Mark as completed
        if device in progress_dict:
            progress_dict[device].update({
                'status': 'completed',
                'percent': 100.0
            })
        
        logger.info(f"Successfully flashed {device}")
        return True
    except Exception as e:
        # Mark as failed
        if device in progress_dict:
            progress_dict[device].update({
                'status': 'failed',
                'error': str(e)
            })
        logger.error(f"Failed to flash {device}: {e}")
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


def boot_rpiboot_device(port: str, timeout: float = 60.0) -> bool:
    """
    Boot a rpiboot-compatible device into mass storage mode.
    
    Args:
        port: USB port pathname to target (e.g., "1-2.3")
        timeout: Timeout in seconds for rpiboot operation (default: 60.0)
        
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
                success, exit_code = run_rpiboot(port=port, verbose=False)
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
    
    # Track devices we've seen and their timestamps
    device_timestamps: Dict[str, float] = {}
    # Track devices currently being flashed
    flashing_devices: Set[str] = set()
    # Track devices that have been successfully flashed
    flashed_devices: Set[str] = set()
    # Track progress of flashing devices (shared across threads)
    device_progress: Dict[str, dict] = {}
    # Track ports currently being booted via rpiboot
    rpibooting_devices: Set[str] = set()
    # Track ports that have completed rpiboot successfully
    rpibooted_ports: Set[str] = set()
    # Track ports where rpiboot failed
    rpiboot_failed_ports: Set[str] = set()
    
    # Create thread pool for parallel flashing
    _flash_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="flash")
    
    logger.info(f"Starting device monitoring on port {monitor_port}")
    logger.info(f"Monitoring for block devices, stable_delay={config.stable_delay}s")
    
    poll_interval = 1.0  # Poll every second
    
    try:
        while not _shutdown_requested:
            try:
                # Enumerate USB ports
                ports_data = enumerate_all_usb_ports()
                
                # Filter to downstream ports
                downstream_ports = filter_ports_by_limit(ports_data, monitor_port)
                
                # Check for rpiboot-compatible devices that need booting
                for port_str, port_info in downstream_ports.items():
                    # Skip the monitor port itself (it's the hub, not a device)
                    if port_str == monitor_port:
                        continue
                    
                    # Check if this is a rpiboot-compatible device
                    if not is_rpiboot_device(port_info):
                        continue
                    
                    # Skip if already being booted
                    if port_str in rpibooting_devices:
                        continue
                    
                    # Skip if already booted successfully
                    if port_str in rpibooted_ports:
                        continue
                    
                    # Skip if rpiboot previously failed
                    if port_str in rpiboot_failed_ports:
                        continue
                    
                    # Skip if device already has block devices (already in mass storage mode)
                    if port_info.get('block_devices'):
                        continue
                    
                    # This device needs rpiboot - start booting it
                    logger.info(f"Detected rpiboot-compatible device at port {port_str}, starting rpiboot")
                    rpibooting_devices.add(port_str)
                    
                    # Execute rpiboot in background thread
                    def handle_rpiboot_completion(port: str):
                        try:
                            success = boot_rpiboot_device(port, timeout=60.0)
                            rpibooting_devices.discard(port)
                            if success:
                                rpibooted_ports.add(port)
                                logger.info(f"rpiboot completed successfully for port {port}, device should appear as block device")
                            else:
                                rpiboot_failed_ports.add(port)
                                logger.error(f"rpiboot failed for port {port}, skipping device")
                        except Exception as e:
                            logger.error(f"Unexpected error during rpiboot for port {port}: {e}")
                            rpibooting_devices.discard(port)
                            rpiboot_failed_ports.add(port)
                    
                    # Start rpiboot in background thread
                    threading.Thread(
                        target=handle_rpiboot_completion,
                        args=(port_str,),
                        daemon=True
                    ).start()
                
                # Get current block devices on downstream ports
                current_devices = get_downstream_block_devices(ports_data, monitor_port)
                
                current_time = time.time()
                
                # Check for new devices
                for device in current_devices:
                    if device in flashing_devices:
                        # Already flashing this device
                        continue
                    
                    if device in flashed_devices:
                        # Already flashed this device, wait for removal
                        continue
                    
                    if device not in device_timestamps:
                        # New device detected
                        logger.info(f"New device detected: {device}")
                        device_timestamps[device] = current_time
                    
                    # Check if device has been stable long enough
                    time_since_detection = current_time - device_timestamps[device]
                    if time_since_detection >= config.stable_delay:
                        # Device is stable, start flashing
                        logger.info(f"Device {device} is stable, starting flash operation")
                        flashing_devices.add(device)
                        
                        # Submit flash job to thread pool
                        # Use memory-mapped image (should always be available)
                        if not _image_mmap:
                            logger.error("Memory-mapped image not available, cannot flash device")
                            flashing_devices.discard(device)
                            continue
                        
                        future = _flash_executor.submit(
                            flash_device,
                            _image_mmap,
                            device,
                            config.block_size,
                            config.image_path,  # Pass path for logging
                            device_progress  # Pass shared progress dictionary
                        )
                        
                        # Track completion in background
                        def handle_completion(dev: str, fut):
                            try:
                                success = fut.result()
                                flashing_devices.discard(dev)
                                if success:
                                    flashed_devices.add(dev)
                                    # Update progress to completed
                                    if dev in device_progress:
                                        device_progress[dev]['status'] = 'completed'
                                        device_progress[dev]['percent'] = 100.0
                                    logger.info(f"Flash completed for {dev}, waiting for removal")
                                else:
                                    # Failed flash, allow retry
                                    device_timestamps.pop(dev, None)
                                    device_progress.pop(dev, None)
                            except Exception as e:
                                logger.error(f"Unexpected error during flash of {dev}: {e}")
                                flashing_devices.discard(dev)
                                device_timestamps.pop(dev, None)
                                device_progress.pop(dev, None)
                        
                        # Use a separate thread to wait for completion
                        threading.Thread(
                            target=handle_completion,
                            args=(device, future),
                            daemon=True
                        ).start()
                
                # Check for removed devices (that were flashed)
                devices_to_remove = []
                for device in flashed_devices:
                    if device not in current_devices:
                        # Device was removed
                        logger.info(f"Flashed device {device} has been removed")
                        devices_to_remove.append(device)
                
                for device in devices_to_remove:
                    flashed_devices.remove(device)
                    device_timestamps.pop(device, None)
                    device_progress.pop(device, None)  # Clean up progress
                
                # Clean up timestamps and progress for devices that are no longer present
                # (but weren't flashed - they may have been removed before flashing)
                for device in list(device_timestamps.keys()):
                    if device not in current_devices and device not in flashing_devices:
                        device_timestamps.pop(device)
                        device_progress.pop(device, None)  # Clean up progress
                
                # Clean up progress for completed/failed devices that are no longer being tracked
                for device in list(device_progress.keys()):
                    if device not in flashing_devices and device not in flashed_devices:
                        device_progress.pop(device)
                
                # Clean up rpiboot state for ports that are no longer present
                # Check all downstream ports to see which ones still exist
                current_port_strings = set(downstream_ports.keys())
                for port_str in list(rpibooting_devices):
                    if port_str not in current_port_strings:
                        logger.debug(f"rpibooting port {port_str} no longer present, cleaning up")
                        rpibooting_devices.discard(port_str)
                for port_str in list(rpibooted_ports):
                    if port_str not in current_port_strings:
                        logger.debug(f"rpibooted port {port_str} no longer present, cleaning up")
                        rpibooted_ports.discard(port_str)
                for port_str in list(rpiboot_failed_ports):
                    if port_str not in current_port_strings:
                        logger.debug(f"rpiboot-failed port {port_str} no longer present, cleaning up")
                        rpiboot_failed_ports.discard(port_str)
                
            except Exception as e:
                logger.error(f"Error during monitoring cycle: {e}", exc_info=True)
            
            # Sleep until next poll
            time.sleep(poll_interval)
    
    finally:
        # Shutdown requested - interrupt in-progress flashes
        if flashing_devices:
            logger.info(f"Shutdown requested, interrupting {len(flashing_devices)} in-progress flash operation(s)...")
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
            logger.error("Could not determine port to monitor")
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
