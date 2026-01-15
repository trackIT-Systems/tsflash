"""Core flashing functionality."""

import os
import platform
import subprocess
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def parse_block_size(block_size_str):
    """
    Parse block size string (e.g., "4M", "1M", "512K") to bytes.
    
    Args:
        block_size_str: String like "4M", "1M", "512K", etc.
        
    Returns:
        int: Size in bytes
        
    Raises:
        ValueError: If the format is invalid
    """
    block_size_str = block_size_str.strip().upper()
    
    if not block_size_str:
        raise ValueError("Block size cannot be empty")
    
    # Extract number and unit
    multipliers = {
        'K': 1024,
        'M': 1024 * 1024,
        'G': 1024 * 1024 * 1024,
    }
    
    # Find the unit
    unit = None
    for u in multipliers:
        if block_size_str.endswith(u):
            unit = u
            break
    
    if unit is None:
        # Assume bytes if no unit
        return int(block_size_str)
    
    # Extract number
    number_str = block_size_str[:-len(unit)]
    try:
        number = int(number_str)
    except ValueError:
        raise ValueError(f"Invalid block size format: {block_size_str}")
    
    return number * multipliers[unit]


def unmount_device(device):
    """
    Unmount all partitions of a device.
    
    Args:
        device: Path to the block device
        
    Returns:
        bool: True if successful, False otherwise
    """
    from .validators import is_mounted
    
    mounted = is_mounted(device)
    
    if not mounted:
        logger.debug(f"No mounted partitions found for {device}")
        return True
    
    logger.info(f"Found {len(mounted)} mounted partition(s) for {device}")
    
    # Use diskutil on macOS, umount on Linux
    is_macos = platform.system() == 'Darwin'
    unmount_cmd = ['diskutil', 'unmount'] if is_macos else ['umount']
    
    for mount_point in mounted:
        logger.debug(f"Unmounting {mount_point}")
        try:
            result = subprocess.run(
                unmount_cmd + [mount_point],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode != 0:
                logger.error(f"Failed to unmount {mount_point}: {result.stderr}")
                return False
            else:
                logger.info(f"Unmounted {mount_point}")
        except Exception as e:
            logger.error(f"Error unmounting {mount_point}: {e}")
            return False
    
    return True


def flash_image(image_path, target_device, block_size='4M'):
    """
    Flash an image file to a block device using pure Python.
    
    Args:
        image_path: Path to the image file
        target_device: Path to the target block device
        block_size: Block size for reading/writing (default: '4M')
        
    Raises:
        IOError: If there's an I/O error during flashing
        PermissionError: If there are permission issues
        ValueError: If block_size format is invalid
    """
    # Parse block size
    try:
        chunk_size = parse_block_size(block_size)
        logger.debug(f"Using chunk size: {chunk_size} bytes ({block_size})")
    except ValueError as e:
        raise ValueError(f"Invalid block size '{block_size}': {e}")
    
    # Get file size for progress bar
    try:
        file_size = os.path.getsize(image_path)
        logger.info(f"Image file size: {file_size} bytes ({file_size / (1024*1024):.2f} MB)")
    except OSError as e:
        raise IOError(f"Cannot get file size: {e}")
    
    # Unmount device first
    if not unmount_device(target_device):
        raise IOError(f"Failed to unmount device {target_device}")
    
    logger.info(f"Flashing {image_path} to {target_device}...")
    
    image_file = None
    device_file = None
    
    try:
        # Open image file for reading
        try:
            image_file = open(image_path, 'rb')
            logger.debug(f"Opened image file: {image_path}")
        except IOError as e:
            raise IOError(f"Cannot open image file: {e}")
        
        # Open block device for writing
        try:
            # Use O_SYNC flag for synchronous writes to ensure data integrity
            device_file = open(target_device, 'wb', buffering=0)
            logger.debug(f"Opened block device: {target_device}")
        except PermissionError as e:
            raise PermissionError(
                f"Cannot open block device (may need root privileges): {e}"
            )
        except IOError as e:
            raise IOError(f"Cannot open block device: {e}")
        
        # Create progress bar
        with tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            bytes_written = 0
            
            while True:
                # Read chunk from image
                chunk = image_file.read(chunk_size)
                
                if not chunk:
                    # End of file
                    break
                
                # Write chunk to device
                try:
                    device_file.write(chunk)
                    bytes_written += len(chunk)
                    
                    # Update progress bar
                    pbar.update(len(chunk))
                    
                except IOError as e:
                    raise IOError(f"Error writing to device: {e}")
                except PermissionError as e:
                    raise PermissionError(f"Permission denied writing to device: {e}")
        
        # Sync to ensure all data is written to disk
        logger.info("Syncing data to disk...")
        try:
            # Sync the device file descriptor
            os.fsync(device_file.fileno())
            logger.debug("Synced device file descriptor")
        except OSError as e:
            logger.warning(f"Could not sync device file descriptor: {e}")
        
        # Close the device file before syncing filesystem
        if device_file:
            try:
                device_file.close()
                device_file = None
            except Exception as e:
                logger.warning(f"Error closing device file: {e}")
        
        # Sync the entire filesystem to ensure all buffers are flushed
        try:
            result = subprocess.run(
                ['sync'],
                capture_output=True,
                text=True,
                check=False,
                timeout=30
            )
            if result.returncode == 0:
                logger.info("Filesystem synced - it is now safe to remove the device")
            else:
                logger.warning(f"sync command returned non-zero exit code: {result.stderr}")
        except subprocess.TimeoutExpired:
            logger.warning("sync command timed out")
        except FileNotFoundError:
            logger.warning("sync command not found - data may not be fully written to disk")
        except Exception as e:
            logger.warning(f"Error running sync command: {e}")
        
        logger.info(f"Successfully flashed {bytes_written} bytes to {target_device}")
        
    except Exception as e:
        logger.error(f"Error during flashing: {e}")
        raise
    finally:
        # Cleanup
        if image_file:
            try:
                image_file.close()
            except Exception as e:
                logger.warning(f"Error closing image file: {e}")
        
        if device_file:
            try:
                device_file.close()
            except Exception as e:
                logger.warning(f"Error closing device file: {e}")
