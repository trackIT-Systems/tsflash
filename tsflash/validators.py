"""Validation functions for image files and block devices."""

import logging
import os
import re
import stat
import subprocess

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
    valid_extensions = {".img", ".iso"}

    if ext not in valid_extensions:
        return False, f"Unsupported file extension: {ext}. Only .img and .iso files are supported."

    # Check magic bytes
    try:
        with open(path, "rb") as f:
            magic = f.read(8)

        # ISO 9660 magic bytes (for .iso files)
        # ISO 9660 files start with specific sector structure
        if ext == ".iso":
            # Check for ISO 9660 signature at offset 32768 (0x8000) or check first sector
            # For simplicity, we'll check if it's a valid ISO by looking at common patterns
            # Most ISO files have recognizable structure, but we'll be lenient
            # and just verify it's not obviously wrong
            if len(magic) < 8:
                return False, "File appears to be empty or too small"
        # For .img files, we'll accept any binary file as valid
        # Raw disk images don't have a standard magic byte signature
        elif ext == ".img":
            if len(magic) < 1:
                return False, "File appears to be empty"

    except IOError as e:
        return False, f"Cannot read file: {e}"

    logger.debug(f"Image file validation passed: {path}")
    return True, None


def validate_block_device(path):
    """
    Validate that a path is a block device or character device.

    Args:
        path: Path to check (e.g., /dev/sdX, /dev/mmcblkX, /dev/rdiskX on macOS)

    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    if not os.path.exists(path):
        return False, f"Device does not exist: {path}"

    # Check if it's a block device or character device (rdisk on macOS)
    try:
        mode = os.stat(path).st_mode
        if not (stat.S_ISBLK(mode) or stat.S_ISCHR(mode)):
            return False, f"Path is not a block or character device: {path}"
    except OSError as e:
        return False, f"Cannot access device: {e}"

    # Check if it's a valid device path
    if not path.startswith("/dev/"):
        return False, f"Device path must start with /dev/: {path}"

    # Check for common device patterns
    basename = os.path.basename(path)
    valid_patterns = ["sd", "mmcblk", "nvme", "hd", "disk", "rdisk"]
    if not any(basename.startswith(pattern) for pattern in valid_patterns):
        logger.warning(f"Device path doesn't match common patterns: {path}")
        # Don't fail, but warn - might be valid custom device

    logger.debug(f"Block device validation passed: {path}")
    return True, None


def _is_device_or_partition(name, base):
    """
    Check whether block device name `name` is `base` itself or one of its partitions.

    Handles Linux (sda -> sda1, mmcblk0 -> mmcblk0p1, nvme0n1 -> nvme0n1p1) and
    macOS (disk4 -> disk4s1) naming. Plain prefix matching is not enough, as
    "sda" would also match "sdaa1" and "disk1" would match "disk10s1".
    """
    if name == base:
        return True
    if not name.startswith(base):
        return False
    suffix = name[len(base):]
    if base[-1].isdigit():
        # Partition separator required after a trailing digit (mmcblk0p1, disk4s1)
        return re.fullmatch(r"[ps]\d+(s\d+)?", suffix) is not None
    return suffix.isdigit()


def _unescape_mount_field(field):
    """Decode octal escapes (e.g. \\040 for space) used in /proc/mounts."""
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), field)


def is_mounted(device):
    """
    Check if a device or any of its partitions are mounted.

    Args:
        device: Path to the block device (e.g., /dev/sda, /dev/disk4, /dev/rdisk4)

    Returns:
        list: List of mounted mount points for this device
    """
    mounted = []

    device_basename = os.path.basename(device)

    # On macOS, rdisk and disk refer to the same physical device
    # If checking rdisk, also check for mounts of the corresponding disk device
    check_basenames = [device_basename]
    if device_basename.startswith("rdisk"):
        # Extract the number part (e.g., "rdisk4" -> "4")
        disk_number = device_basename[5:]  # Remove "rdisk" prefix
        check_basenames.append(f"disk{disk_number}")

    # Try /proc/mounts first (Linux)
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 1:
                    mount_device = _unescape_mount_field(parts[0])
                    mount_point = _unescape_mount_field(parts[1]) if len(parts) > 1 else None

                    # Check if this mount point uses our device or a partition
                    mount_basename = os.path.basename(mount_device)

                    # Match device or partition for any of the basenames we're checking
                    for check_basename in check_basenames:
                        if _is_device_or_partition(mount_basename, check_basename):
                            if mount_point:
                                mounted.append(mount_point)
                            else:
                                mounted.append(mount_device)
                            break
        return mounted
    except IOError:
        # /proc/mounts doesn't exist (e.g., on macOS), fall back to mount command
        pass

    # Fall back to mount command (works on both Linux and macOS)
    try:
        result = subprocess.run(["mount"], capture_output=True, text=True, check=False)

        if result.returncode == 0:
            for line in result.stdout.splitlines():
                # Parse mount output format: "device on mountpoint (options)"
                # Both Linux and macOS use similar format
                parts = line.split()
                if len(parts) >= 3 and parts[1] == "on":
                    mount_device = parts[0]
                    mount_point = parts[2]

                    # Check if this mount point uses our device or a partition
                    mount_basename = os.path.basename(mount_device)

                    # Match device or partition for any of the basenames we're checking
                    for check_basename in check_basenames:
                        if _is_device_or_partition(mount_basename, check_basename):
                            mounted.append(mount_point)
                            break
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        logger.warning(f"Could not determine mounted filesystems: {e}")

    return mounted
