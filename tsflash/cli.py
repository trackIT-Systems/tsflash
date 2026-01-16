"""Command-line interface for tsflash."""

import argparse
import logging
import sys

from . import __version__
from .flash import flash_image, create_image_mmap
from .validators import validate_image_file, validate_block_device
from .usb import enumerate_all_usb_ports, format_usb_output, filter_ports_by_limit, find_first_usb_hub
from .daemon import run_daemon


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


def cmd_flash(args):
    """Handle the flash command."""
    logger = logging.getLogger(__name__)
    
    # Validate image file
    logger.debug(f"Validating image file: {args.file}")
    is_valid, error_msg = validate_image_file(args.file)
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
        logger.debug(f"Creating memory-mapped image: {args.file}")
        mapped_image = create_image_mmap(args.file)
        
        # Flash using mmap
        flash_image(mapped_image, args.target, args.block_size, 
                   non_interactive=args.non_interactive, image_path=args.file)
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
    # The daemon module handles its own logging setup, but we can override
    # with CLI flags if specified
    if args.verbose or args.quiet:
        import logging
        if args.verbose:
            logging.basicConfig(level=logging.DEBUG)
        elif args.quiet:
            logging.basicConfig(level=logging.WARNING)
    
    return run_daemon(args.config)


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
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Flash subcommand
    flash_parser = subparsers.add_parser(
        'flash',
        help='Flash an OS image to a block device'
    )
    flash_parser.add_argument(
        'file',
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
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
