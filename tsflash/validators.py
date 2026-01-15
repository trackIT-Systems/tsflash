"""Validation functions for image files and block devices."""

import os
import stat
import logging

logger = logging.getLogger(__name__)


def validate_image_file(path):
    """
    Validate that a file exists and is an uncompressed OS image.
    
    Args:
        path: Path to the image file
        
    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    if not os.path.exists(path):
        return False, f"File does not exist: {path}"
    
    if not os.path.isfile(path):
        return False, f"Path is not a file: {path}"
    
    # Check file extension
    ext = os.path.splitext(path)[1].lower()
    valid_extensions = {'.img', '.iso'}
    
    if ext not in valid_extensions:
        return False, f"Unsupported file extension: {ext}. Only .img and .iso files are supported."
    
    # Check magic bytes
    try:
        with open(path, 'rb') as f:
            magic = f.read(8)
            
        # ISO 9660 magic bytes (for .iso files)
        # ISO 9660 files start with specific sector structure
        if ext == '.iso':
            # Check for ISO 9660 signature at offset 32768 (0x8000) or check first sector
            # For simplicity, we'll check if it's a valid ISO by looking at common patterns
            # Most ISO files have recognizable structure, but we'll be lenient
            # and just verify it's not obviously wrong
            if len(magic) < 8:
                return False, "File appears to be empty or too small"
        # For .img files, we'll accept any binary file as valid
        # Raw disk images don't have a standard magic byte signature
        elif ext == '.img':
            if len(magic) < 1:
                return False, "File appears to be empty"
                
    except IOError as e:
        return False, f"Cannot read file: {e}"
    
    logger.debug(f"Image file validation passed: {path}")
    return True, None


def validate_block_device(path):
    """
    Validate that a path is a block device.
    
    Args:
        path: Path to check (e.g., /dev/sdX, /dev/mmcblkX)
        
    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    if not os.path.exists(path):
        return False, f"Device does not exist: {path}"
    
    # Check if it's a block device
    try:
        mode = os.stat(path).st_mode
        if not stat.S_ISBLK(mode):
            return False, f"Path is not a block device: {path}"
    except OSError as e:
        return False, f"Cannot access device: {e}"
    
    # Check if it's a valid device path
    if not path.startswith('/dev/'):
        return False, f"Device path must start with /dev/: {path}"
    
    # Check for common device patterns
    basename = os.path.basename(path)
    valid_patterns = ['sd', 'mmcblk', 'nvme', 'hd']
    if not any(basename.startswith(pattern) for pattern in valid_patterns):
        logger.warning(f"Device path doesn't match common patterns: {path}")
        # Don't fail, but warn - might be valid custom device
    
    logger.debug(f"Block device validation passed: {path}")
    return True, None


def is_mounted(device):
    """
    Check if a device or any of its partitions are mounted.
    
    Args:
        device: Path to the block device (e.g., /dev/sda)
        
    Returns:
        list: List of mounted mount points for this device
    """
    mounted = []
    
    try:
        # Read /proc/mounts to find mounted filesystems
        with open('/proc/mounts', 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 1:
                    mount_device = parts[0]
                    mount_point = parts[1] if len(parts) > 1 else None
                    
                    # Check if this mount point uses our device or a partition
                    device_basename = os.path.basename(device)
                    mount_basename = os.path.basename(mount_device)
                    
                    # Match device or partition (e.g., /dev/sda matches /dev/sda1)
                    if mount_basename.startswith(device_basename):
                        if mount_point:
                            mounted.append(mount_point)
                        else:
                            mounted.append(mount_device)
    except IOError as e:
        logger.warning(f"Could not read /proc/mounts: {e}")
    
    return mounted
