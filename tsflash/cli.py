"""Command-line interface for tsflash."""

import argparse
import logging
import subprocess
import sys

from . import __version__
from .flash import flash_image, create_image_mmap
from .validators import validate_image_file, validate_block_device
from .usb import enumerate_all_usb_ports, format_usb_output, filter_ports_by_limit, find_first_usb_hub
from .daemon import run_daemon
from .rpiboot import run_rpiboot
from .tui import run_tui


def setup_logging(verbose=False, quiet=False):
    """
    Configure logging based on verbosity flags.
    
    Args:
        verbose: If True, set logging level to DEBUG
        quiet: If True, set logging level to WARNING
    """
    if verbose:
        level = logging.DEBUG
    elif quiet:
        level = logging.WARNING
    else:
        level = logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(levelname)s: %(message)s',
        stream=sys.stderr
    )


def is_devmon_running():
    """
    Check if devmon process is running.
    
    Returns:
        bool: True if devmon is running, False otherwise
    """
    try:
        # Use pgrep to check if devmon is running
        result = subprocess.run(
            ['pgrep', '-x', 'devmon'],
            capture_output=True,
            text=True,
            check=False
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError):
        # If pgrep is not available, try using ps
        try:
            result = subprocess.run(
                ['ps', '-C', 'devmon'],
                capture_output=True,
                text=True,
                check=False
            )
            return result.returncode == 0
        except (subprocess.SubprocessError, FileNotFoundError):
            # If both fail, assume devmon is not running
            return False


def check_devmon_and_exit():
    """
    Check if devmon is running and exit with error if it is.
    
    Returns:
        int: Exit code (1 if devmon is running, 0 otherwise)
    """
    if is_devmon_running():
        logger = logging.getLogger(__name__)
        logger.error("devmon is currently running.")
        logger.error("")
        logger.error("devmon automatically mounts devices, which interferes with tsflash:")
        logger.error("  - Devices cannot be flashed while mounted")
        logger.error("  - devmon may re-mount devices during flash operations")
        logger.error("  - Newly flashed cards may be mounted before completion")
        logger.error("")
        logger.error("Please stop devmon before running tsflash:")
        logger.error("  sudo systemctl stop devmon")
        return 1
    return 0


def cmd_flash(args):
    """Handle the flash command."""
    logger = logging.getLogger(__name__)
    
    # Validate image file
    logger.debug(f"Validating image file: {args.image_path}")
    is_valid, error_msg = validate_image_file(args.image_path)
    if not is_valid:
        logger.error(error_msg)
        return 1
    
    # Validate block device
    logger.debug(f"Validating block device: {args.target}")
    is_valid, error_msg = validate_block_device(args.target)
    if not is_valid:
        logger.error(error_msg)
        return 1
    
    # Flash the image
    mapped_image = None
    try:
        # Create memory-mapped image for efficient access
        logger.debug(f"Creating memory-mapped image: {args.image_path}")
        mapped_image = create_image_mmap(args.image_path)
        
        # Flash using mmap
        flash_image(mapped_image, args.target, args.block_size, 
                   non_interactive=args.non_interactive, image_path=args.image_path)
        logger.info("Flash operation completed successfully")
        return 0
    except ValueError as e:
        logger.error(f"Invalid parameter: {e}")
        return 1
    except PermissionError as e:
        logger.error(f"Permission denied: {e}")
        logger.error("You may need to run this command with sudo")
        return 1
    except IOError as e:
        logger.error(f"I/O error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def cmd_usb(args):
    """Handle the usb command."""
    logger = logging.getLogger(__name__)
    
    try:
        # Enumerate all USB ports
        logger.debug("Enumerating USB ports...")
        ports_data = enumerate_all_usb_ports()
        
        # Determine which ports to show
        if args.all:
            # Show all ports
            logger.debug("Showing all USB ports")
        elif args.port:
            # Use specified port
            logger.debug(f"Limiting output to port and downstream ports: {args.port}")
            ports_data = filter_ports_by_limit(ports_data, args.port)
            if not ports_data:
                logger.warning(f"No ports found matching port '{args.port}'")
                return 1
        else:
            # Default: find first hub and limit to it
            first_hub = find_first_usb_hub(ports_data)
            if first_hub:
                logger.debug(f"Limiting output to first hub: {first_hub}")
                ports_data = filter_ports_by_limit(ports_data, first_hub)
            else:
                # No hub found, show all ports
                logger.debug("No USB hub found, showing all ports")
        
        # Format and print output
        output = format_usb_output(ports_data, json_output=args.json)
        print(output)
        
        return 0
    except RuntimeError as e:
        logger.error(f"USB enumeration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def cmd_daemon(args):
    """Handle the daemon command."""
    # Check if devmon is running
    exit_code = check_devmon_and_exit()
    if exit_code != 0:
        return exit_code
    
    # The daemon module handles its own logging setup, but we can override
    # with CLI flags if specified
    if args.verbose or args.quiet:
        import logging
        if args.verbose:
            logging.basicConfig(level=logging.DEBUG)
        elif args.quiet:
            logging.basicConfig(level=logging.WARNING)
    
    return run_daemon(args.config)


def cmd_rpiboot(args):
    """Handle the rpiboot command."""
    success, exit_code = run_rpiboot(port=args.port, verbose=args.verbose)
    return exit_code


def cmd_tui(args):
    """Handle the tui command."""
    # Check if devmon is running
    exit_code = check_devmon_and_exit()
    if exit_code != 0:
        return exit_code
    
    # Derive log level from verbose/quiet flags
    if args.verbose:
        log_level = 'DEBUG'
    elif args.quiet:
        log_level = 'WARNING'
    else:
        log_level = 'INFO'
    
    return run_tui(
        image_path=args.image_path,
        port=args.port,
        block_size=args.block_size,
        stable_delay=args.stable_delay,
        log_level=log_level
    )


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description='tsflash - SD Card Flashing Tool for Raspberry Pi',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Global flags
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output (DEBUG level logging)'
    )
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Enable quiet output (WARNING level logging)'
    )
    parser.add_argument(
        '--version',
        action='version',
        version=f'tsflash {__version__}'
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=False)
    
    # Flash subcommand
    flash_parser = subparsers.add_parser(
        'flash',
        help='Flash an OS image to a block device'
    )
    flash_parser.add_argument(
        'image_path',
        metavar='IMAGE-PATH',
        help='Path to the OS image file (.img or .iso)'
    )
    flash_parser.add_argument(
        'target',
        help='Path to the target block device (e.g., /dev/sda, /dev/mmcblk0)'
    )
    flash_parser.add_argument(
        '--block-size',
        default='4M',
        help='Block size for reading/writing (default: 4M). Examples: 4M, 1M, 512K'
    )
    flash_parser.add_argument(
        '--non-interactive',
        action='store_true',
        help='Use logging instead of progress bar (useful for scripts/daemons)'
    )
    
    # USB subcommand
    usb_parser = subparsers.add_parser(
        'usb',
        help='List USB ports and connected devices (default: first hub and downstream ports)'
    )
    usb_parser.add_argument(
        '--json',
        action='store_true',
        help='Output in JSON format'
    )
    usb_parser.add_argument(
        '--all',
        action='store_true',
        help='Show all USB ports (default: show only first hub and downstream ports)'
    )
    usb_parser.add_argument(
        '--port',
        metavar='PORT',
        help='Limit output to a specific port and downstream ports (e.g., 1-2)'
    )
    
    # Daemon subcommand
    daemon_parser = subparsers.add_parser(
        'daemon',
        help='Run the automatic device flashing daemon'
    )
    daemon_parser.add_argument(
        '--config',
        metavar='PATH',
        help='Path to configuration file (default: /boot/firmware/tsflash.yml)'
    )
    
    # rpiboot subcommand
    rpiboot_parser = subparsers.add_parser(
        'rpiboot',
        help='Boot a Raspberry Pi into mass storage device mode for flashing'
    )
    rpiboot_parser.add_argument(
        '-p', '--port',
        metavar='PORT',
        help='USB port pathname to target (e.g., 1-2.3)'
    )
    
    # TUI subcommand
    tui_parser = subparsers.add_parser(
        'tui',
        help='Run the TUI (Text User Interface) for interactive flashing'
    )
    tui_parser.add_argument(
        'image_path',
        metavar='IMAGE-PATH',
        help='Path to the image file to flash'
    )
    tui_parser.add_argument(
        '--port',
        metavar='PORT',
        help='USB port to monitor (e.g., "1-2"). Auto-detects first hub if not specified'
    )
    tui_parser.add_argument(
        '--block-size',
        default='4M',
        help='Block size for flashing (default: 4M). Examples: 4M, 1M, 512K'
    )
    tui_parser.add_argument(
        '--stable-delay',
        type=float,
        default=3.0,
        help='Seconds to wait after device appears before flashing (default: 3)'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose, quiet=args.quiet)
    
    # Handle commands
    if args.command == 'flash':
        return cmd_flash(args)
    elif args.command == 'usb':
        return cmd_usb(args)
    elif args.command == 'daemon':
        return cmd_daemon(args)
    elif args.command == 'rpiboot':
        return cmd_rpiboot(args)
    elif args.command == 'tui':
        return cmd_tui(args)
    else:
        # No command provided
        parser.error("A command is required. Use 'tui', 'flash', 'usb', 'daemon', or 'rpiboot'.")


if __name__ == '__main__':
    sys.exit(main())
