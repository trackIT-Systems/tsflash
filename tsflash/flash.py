"""Core flashing functionality."""

import os
import platform
import subprocess
import logging
import time
import mmap
from typing import Union
from tqdm import tqdm


class MappedImage:
    """Wrapper for memory-mapped image file that keeps file handle alive."""
    
    def __init__(self, mmap_obj: mmap.mmap, file_handle):
        self.mmap = mmap_obj
        self._file_handle = file_handle
    
    def __len__(self):
        return len(self.mmap)
    
    def __getitem__(self, key):
        return self.mmap[key]
    
    def close(self):
        """Close both the mmap and file handle."""
        try:
            self.mmap.close()
        except Exception:
            pass
        try:
            self._file_handle.close()
        except Exception:
            pass

logger = logging.getLogger(__name__)


class FlashAborted(Exception):
    """Raised when a flash operation is cancelled via its cancel_event."""


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
    
    # Extract number (no unit means bytes)
    number_str = block_size_str[:-len(unit)] if unit else block_size_str
    try:
        number = int(number_str)
    except ValueError:
        raise ValueError(f"Invalid block size format: {block_size_str}")
    
    size = number * (multipliers[unit] if unit else 1)
    if size <= 0:
        raise ValueError(f"Block size must be positive: {block_size_str}")
    return size


def create_image_mmap(image_path: str) -> MappedImage:
    """
    Create a memory-mapped view of the image file for efficient access.
    
    Args:
        image_path: Path to the image file
        
    Returns:
        MappedImage wrapper containing the mmap and file handle
        
    Raises:
        IOError: If the file cannot be opened or mapped
    """
    try:
        # Open file in read-only mode
        image_file = open(image_path, 'rb')
        
        # Get file size
        file_size = image_file.seek(0, 2)  # Seek to end
        image_file.seek(0)  # Reset to beginning
        
        if file_size == 0:
            image_file.close()
            raise IOError(f"Image file is empty: {image_path}")
        
        # Create memory-mapped file (read-only, shared)
        # Note: We don't close image_file here - it needs to stay open while mmap is active
        image_mmap = mmap.mmap(image_file.fileno(), 0, access=mmap.ACCESS_READ)
        
        logger.debug(f"Memory-mapped image file: {file_size} bytes ({file_size / (1024*1024):.2f} MB)")
        return MappedImage(image_mmap, image_file)
        
    except IOError as e:
        raise IOError(f"Cannot create memory-mapped image: {e}")


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


def flash_image(image_source: Union[MappedImage, mmap.mmap], target_device: str, block_size: str = '4M', 
                non_interactive: bool = False, image_path: str = None, progress_callback=None,
                cancel_event=None):
    """
    Flash an image file to a block device using pure Python.
    
    Args:
        image_source: MappedImage wrapper or memory-mapped file object (mmap.mmap) containing the image
        target_device: Path to the target block device
        block_size: Block size for reading/writing (default: '4M')
        non_interactive: If True, use logging instead of progress bar (default: False)
        image_path: Optional path string for logging
        progress_callback: Optional callback function(bytes_written, total_bytes, percent) for progress updates
        cancel_event: Optional threading.Event; when set, flashing stops before the next
                      chunk and FlashAborted is raised (the device is left incomplete)
        
    Raises:
        IOError: If there's an I/O error during flashing
        PermissionError: If there are permission issues
        ValueError: If block_size format is invalid
        FlashAborted: If cancel_event was set during flashing
    """
    # Parse block size
    try:
        chunk_size = parse_block_size(block_size)
        logger.debug(f"Using chunk size: {chunk_size} bytes ({block_size})")
    except ValueError as e:
        raise ValueError(f"Invalid block size '{block_size}': {e}")
    
    # Handle both MappedImage wrapper and raw mmap objects
    if isinstance(image_source, MappedImage):
        image_mmap = image_source.mmap
    else:
        image_mmap = image_source
    
    # Get file size from mmap
    file_size = len(image_mmap)
    display_path = image_path or "memory-mapped image"
    logger.info(f"Image file size: {file_size} bytes ({file_size / (1024*1024):.2f} MB)")
    
    # Unmount device first
    if not unmount_device(target_device):
        raise IOError(f"Failed to unmount device {target_device}")
    
    logger.info(f"Flashing {display_path} to {target_device}...")
    
    device_file = None
    
    try:
        # Open block device for writing (unbuffered; data is fsync'ed at the end).
        # No O_CREAT/O_TRUNC: if the device vanished, open('wb') would silently
        # create a regular file of that name (e.g. in /dev) instead of failing.
        try:
            device_file = open(os.open(target_device, os.O_WRONLY), 'wb', buffering=0)
            logger.debug(f"Opened block device: {target_device}")
        except PermissionError as e:
            raise PermissionError(
                f"Cannot open block device (may need root privileges): {e}"
            )
        except IOError as e:
            raise IOError(f"Cannot open block device: {e}")
        
        # Refuse to write an image that does not fit on the target device.
        # (Seeking to the end of a block device yields its size on Linux; on
        # macOS raw devices this may return 0, in which case we skip the check.)
        try:
            device_size = os.lseek(device_file.fileno(), 0, os.SEEK_END)
            os.lseek(device_file.fileno(), 0, os.SEEK_SET)
        except OSError:
            device_size = 0
        if device_size and file_size > device_size:
            raise IOError(
                f"Image ({file_size} bytes) is larger than target device "
                f"{target_device} ({device_size} bytes)"
            )
        
        bytes_written = 0
        log_interval = 5.0  # Log progress every 5 seconds in non-interactive mode
        
        if non_interactive:
            logger.info(f"Writing {file_size} bytes ({file_size / (1024*1024):.2f} MB) to {target_device}")
            last_log_time = time.time()  # Initialize for first progress log
            pbar = None
        else:
            # Interactive mode: use progress bar
            pbar = tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024)
        
        try:
            offset = 0
            while offset < file_size:
                if cancel_event is not None and cancel_event.is_set():
                    raise FlashAborted(
                        f"Flashing {target_device} aborted after {bytes_written} bytes"
                    )
                
                end_offset = min(offset + chunk_size, file_size)
                chunk = memoryview(image_mmap[offset:end_offset])
                
                try:
                    # Unbuffered (raw) writes may be short - loop until the
                    # whole chunk has been written.
                    chunk_written = 0
                    while chunk_written < len(chunk):
                        n = device_file.write(chunk[chunk_written:])
                        if not n:
                            raise IOError(
                                f"Short write at offset {offset + chunk_written} "
                                f"(device full or removed?)"
                            )
                        chunk_written += n
                except PermissionError as e:
                    raise PermissionError(f"Permission denied writing to device: {e}")
                except IOError as e:
                    raise IOError(f"Error writing to device: {e}")
                
                bytes_written += chunk_written
                offset += chunk_written
                
                # Calculate progress
                percent = (bytes_written / file_size * 100) if file_size > 0 else 0
                
                # Call progress callback if provided
                if progress_callback:
                    try:
                        progress_callback(bytes_written, file_size, percent)
                    except Exception as e:
                        logger.debug(f"Error in progress callback: {e}")
                
                if pbar is not None:
                    pbar.update(chunk_written)
                else:
                    # Log progress periodically
                    current_time = time.time()
                    if (current_time - last_log_time) >= log_interval:
                        logger.info(
                            f"{target_device}: Progress: {bytes_written / (1024*1024):.2f} MB / "
                            f"{file_size / (1024*1024):.2f} MB ({percent:.1f}%)"
                        )
                        last_log_time = current_time
        finally:
            if pbar is not None:
                pbar.close()
        
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
        # Note: Don't close mmap here - it's managed by the caller
        if device_file:
            try:
                device_file.close()
            except Exception as e:
                logger.warning(f"Error closing device file: {e}")
