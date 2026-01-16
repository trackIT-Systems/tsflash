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
from .usb import enumerate_all_usb_ports, filter_ports_by_limit, find_first_usb_hub

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
    # Track last progress log time for each device (to throttle logging)
    last_progress_log: Dict[str, float] = {}
    
    # Create thread pool for parallel flashing
    _flash_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="flash")
    
    logger.info(f"Starting device monitoring on port {monitor_port}")
    logger.info(f"Monitoring for block devices, stable_delay={config.stable_delay}s")
    
    poll_interval = 1.0  # Poll every second
    progress_log_interval = 5.0  # Log progress updates every 5 seconds per device
    
    try:
        while not _shutdown_requested:
            try:
                # Enumerate USB ports
                ports_data = enumerate_all_usb_ports()
                
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
                
                # Report progress for currently flashing devices (throttled)
                for device in flashing_devices:
                    if device in device_progress:
                        progress = device_progress[device]
                        if progress.get('status') == 'flashing':
                            percent = progress.get('percent', 0)
                            bytes_written = progress.get('bytes_written', 0)
                            total_bytes = progress.get('total_bytes', 0)
                            
                            # Throttle progress logging (log every 5 seconds per device)
                            should_log = False
                            if device not in last_progress_log:
                                should_log = True
                                last_progress_log[device] = current_time
                            elif (current_time - last_progress_log[device]) >= progress_log_interval:
                                should_log = True
                                last_progress_log[device] = current_time
                            
                            if should_log and total_bytes > 0:
                                logger.info(
                                    f"{device}: {percent:.1f}% complete "
                                    f"({bytes_written / (1024*1024):.2f} MB / {total_bytes / (1024*1024):.2f} MB)"
                                )
                
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
                    last_progress_log.pop(device, None)  # Clean up log timing
                
                # Clean up timestamps and progress for devices that are no longer present
                # (but weren't flashed - they may have been removed before flashing)
                for device in list(device_timestamps.keys()):
                    if device not in current_devices and device not in flashing_devices:
                        device_timestamps.pop(device)
                        device_progress.pop(device, None)  # Clean up progress
                        last_progress_log.pop(device, None)  # Clean up log timing
                
                # Clean up progress for completed/failed devices that are no longer being tracked
                for device in list(device_progress.keys()):
                    if device not in flashing_devices and device not in flashed_devices:
                        device_progress.pop(device)
                        last_progress_log.pop(device, None)
                
            except Exception as e:
                logger.error(f"Error during monitoring cycle: {e}", exc_info=True)
            
            # Sleep until next poll
            time.sleep(poll_interval)
    
    finally:
        # Shutdown requested - wait for in-progress flashes to complete
        logger.info("Shutdown requested, waiting for in-progress flashes to complete...")
        
        if _flash_executor:
            # Wait for all flashes to complete
            # Note: shutdown(wait=True) will wait indefinitely, but we're in a daemon
            # so we want to wait for completion. If we need timeout, we'd need to
            # track futures and use as_completed with timeout instead.
            _flash_executor.shutdown(wait=True)
            logger.info("All flash operations completed")
        
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
