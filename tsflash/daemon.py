"""Daemon for automatically flashing devices on USB downstream ports."""

import logging
import signal
import sys
from typing import Optional

from .config import load_config
from .flash import create_image_mmap
from .monitor import PortMonitor, find_monitor_port
from .usb import enumerate_all_usb_ports

logger = logging.getLogger(__name__)


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


def run_daemon(config_path: Optional[str] = None, log_level: Optional[str] = None) -> int:
    """
    Main daemon entry point.
    
    Args:
        config_path: Path to config file (optional)
        log_level: Log level override (e.g. from -v/-q); takes precedence over
                   the config file's log_level
        
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        # Load configuration
        config = load_config(config_path)
        
        # Setup logging; a command line override wins over the config file
        effective_level = log_level or config.log_level
        if not logging.getLogger().handlers:
            setup_logging(effective_level)
        else:
            # Logging already configured (e.g., by the CLI), just set level
            logging.getLogger().setLevel(getattr(logging, effective_level, logging.INFO))
        
        logger.info("tsflashd starting...")
        logger.info(f"Image: {config.image_path}")
        logger.info(f"Block size: {config.block_size}")
        logger.info(f"Stable delay: {config.stable_delay}s")
        
        # Create memory-mapped image for efficient parallel access
        try:
            image = create_image_mmap(config.image_path)
            logger.info("Image file memory-mapped for efficient parallel flashing")
        except IOError as e:
            logger.error(f"Could not create memory-mapped image: {e}")
            return 1
        
        try:
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
                return 1
            
            monitor = PortMonitor(monitor_port, config, image)
            
            # Stop on SIGTERM/SIGINT; in-progress flashes are aborted
            def _signal_handler(signum, frame):
                logger.info(f"Received signal {signum}, initiating graceful shutdown...")
                monitor.stop()
            
            signal.signal(signal.SIGTERM, _signal_handler)
            signal.signal(signal.SIGINT, _signal_handler)
            
            try:
                monitor.run()
            finally:
                monitor.shutdown()
        finally:
            image.close()
        
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
    
    # verbose/quiet flags override the config file's log_level
    if args.verbose:
        log_level = 'DEBUG'
    elif args.quiet:
        log_level = 'WARNING'
    else:
        log_level = None
    
    sys.exit(run_daemon(args.config, log_level=log_level))
