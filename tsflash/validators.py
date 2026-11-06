"""Validation functions for image files and block devices."""

import logging
import os
import platform
import plistlib
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


SYSFS_CLASS_BLOCK = "/sys/class/block"
SYSFS_DEV_BLOCK = "/sys/dev/block"


def _sysfs_disks(sysfs_path, _depth=0):
    """
    Resolve a sysfs block device directory to the physical disk name(s) backing it.

    Partitions resolve to their parent disk; device-mapper/RAID devices resolve
    (recursively) through their slaves.
    """
    if _depth > 8:
        return set()
    sysfs_path = os.path.realpath(sysfs_path)
    if not os.path.isdir(sysfs_path):
        return set()

    slaves_dir = os.path.join(sysfs_path, "slaves")
    try:
        slaves = os.listdir(slaves_dir)
    except OSError:
        slaves = []
    if slaves:
        disks = set()
        for slave in slaves:
            disks |= _sysfs_disks(os.path.join(SYSFS_CLASS_BLOCK, slave), _depth + 1)
        return disks

    if os.path.exists(os.path.join(sysfs_path, "partition")):
        return {os.path.basename(os.path.dirname(sysfs_path))}
    return {os.path.basename(sysfs_path)}


def _diskutil_info(device, run=subprocess.run):
    """`diskutil info -plist` as a dict, or None if diskutil fails."""
    try:
        result = run(["diskutil", "info", "-plist", device], capture_output=True, timeout=10)
        if result.returncode != 0:
            return None
        return plistlib.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, plistlib.InvalidFileException, ValueError):
        return None


def _macos_physical_disks(device, run=subprocess.run, _depth=0):
    """
    Resolve a macOS disk, partition or mounted path to its physical disk(s).

    APFS volumes live on synthesized container disks; those resolve through
    their physical stores (e.g. disk3s3s1 -> disk3 -> disk0s2 -> disk0).
    """
    if _depth > 4:
        return set()
    info = _diskutil_info(device, run)
    if not info:
        return set()
    whole = info.get("ParentWholeDisk") or info.get("DeviceIdentifier")
    if not whole:
        return set()
    whole_info = info if info.get("WholeDisk") else _diskutil_info(whole, run) or {}
    stores = whole_info.get("APFSPhysicalStores") or []
    if stores:
        disks = set()
        for store in stores:
            name = store.get("APFSPhysicalStore")
            if name:
                disks |= _macos_physical_disks(name, run, _depth + 1)
        return disks
    return {whole}


def disk_for_device(device):
    """
    Get the physical disk name(s) for a block device or partition path.

    Example: "/dev/mmcblk0p2" -> {"mmcblk0"}, "/dev/sda" -> {"sda"},
    "/dev/rdisk4s1" -> {"disk4"} (macOS).
    Returns an empty set if this cannot be determined (e.g. no sysfs).
    """
    if platform.system() == "Darwin":
        return _macos_physical_disks(device)
    name = os.path.basename(os.path.realpath(device))
    return _sysfs_disks(os.path.join(SYSFS_CLASS_BLOCK, name))


def _mount_source(mount_point):
    """Return the device path mounted at mount_point according to /proc/mounts."""
    source = None
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                # Last match wins (later mounts shadow earlier ones)
                if len(parts) >= 2 and _unescape_mount_field(parts[1]) == mount_point:
                    source = _unescape_mount_field(parts[0])
    except IOError:
        return None
    return source if source and source.startswith("/dev/") else None


LINUX_SYSTEM_MOUNT_POINTS = ("/", "/boot", "/boot/firmware")
MACOS_SYSTEM_MOUNT_POINTS = ("/", "/System/Volumes/Data")

# The boot disk cannot change while tsflash runs; resolving it on macOS takes
# several diskutil calls, so the result is cached
_macos_system_disks = None


def get_system_disks(mount_points=None):
    """
    Get the names of all physical disks that hold the running system.

    These disks (and therefore all of their partitions) must never be flashed.

    Args:
        mount_points: Mount points whose backing disks count as system disks
            (default: /, /boot, /boot/firmware on Linux; / and the data
            volume on macOS)

    Returns:
        set: Disk names, e.g. {"mmcblk0"}, {"sda"} or {"disk0"}. Empty if
        undeterminable.
    """
    global _macos_system_disks
    if platform.system() == "Darwin":
        if mount_points is None:
            if _macos_system_disks is None:
                disks = set()
                for mount_point in MACOS_SYSTEM_MOUNT_POINTS:
                    disks |= _macos_physical_disks(mount_point)
                if disks:
                    _macos_system_disks = disks
                return disks
            return set(_macos_system_disks)
        disks = set()
        for mount_point in mount_points:
            disks |= _macos_physical_disks(mount_point)
        return disks

    if mount_points is None:
        mount_points = LINUX_SYSTEM_MOUNT_POINTS
    disks = set()
    for mount_point in mount_points:
        try:
            st = os.stat(mount_point)
        except OSError:
            continue

        # Preferred: map the filesystem's device number to sysfs
        # (works for /dev/root and other non-obvious mount sources)
        found = _sysfs_disks(os.path.join(SYSFS_DEV_BLOCK, f"{os.major(st.st_dev)}:{os.minor(st.st_dev)}"))

        # Fallback (e.g. btrfs/overlay with anonymous device numbers):
        # use the mount source from /proc/mounts
        if not found:
            source = _mount_source(mount_point)
            if source:
                found = disk_for_device(source)

        disks |= found

    return disks


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
