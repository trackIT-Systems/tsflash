"""USB device enumeration functionality."""

import json
import logging
import os
import platform
from pathlib import Path

logger = logging.getLogger(__name__)

# Path to USB sysfs
USB_SYSFS_PATH = Path("/sys/bus/usb/devices")


def _read_file_safe(path, default=None):
    """
    Safely read a file from sysfs, returning default if file doesn't exist.
    
    Args:
        path: Path to the file
        default: Default value to return if file doesn't exist
        
    Returns:
        str: File contents stripped, or default value
    """
    try:
        if path.exists():
            return path.read_text().strip()
    except (IOError, OSError, PermissionError) as e:
        logger.debug(f"Could not read {path}: {e}")
    return default


def _get_maxchild(device_path):
    """
    Get the maximum number of child ports for a USB device (hub).
    
    Args:
        device_path: Path to the USB device in sysfs
        
    Returns:
        int: Number of ports, or 0 if not a hub
    """
    maxchild_file = device_path / "maxchild"
    maxchild_str = _read_file_safe(maxchild_file, "0")
    try:
        return int(maxchild_str)
    except ValueError:
        return 0


def _enumerate_ports_recursive(bus_num, parent_path, parent_port_str=""):
    """
    Recursively enumerate all USB ports starting from a parent device.
    
    Args:
        bus_num: USB bus number
        parent_path: Path to the parent USB device in sysfs
        parent_port_str: Port string of parent (e.g., "1-2")
        
    Returns:
        dict: Dictionary mapping port strings to device info dictionaries
    """
    ports = {}
    maxchild = _get_maxchild(parent_path)
    
    if maxchild == 0:
        # Not a hub, no child ports
        return ports
    
    # Check each port on this hub
    for port_num in range(1, maxchild + 1):
        # Construct port string (e.g., "1-2.3" for port 3 on hub 1-2)
        if parent_port_str:
            port_str = f"{parent_port_str}.{port_num}"
        else:
            port_str = f"{bus_num}-{port_num}"
        
        # Check if device exists at this port
        device_name = port_str
        device_path = USB_SYSFS_PATH / device_name
        
        if device_path.exists() and device_path.is_dir():
            # Device is connected at this port
            device_info = _get_usb_device_info(device_path, port_str)
            ports[port_str] = device_info
            
            # Recursively enumerate ports on this device (if it's a hub)
            child_ports = _enumerate_ports_recursive(bus_num, device_path, port_str)
            ports.update(child_ports)
        else:
            # Port is empty
            ports[port_str] = {
                "vendor_id": None,
                "product_id": None,
                "manufacturer": None,
                "product": None,
                "serial": None,
                "block_devices": []
            }
    
    return ports


def _get_usb_device_info(device_path, port_str):
    """
    Get USB device information from sysfs.
    
    Args:
        device_path: Path to the USB device in sysfs
        port_str: Port string identifier (e.g., "1-2.3")
        
    Returns:
        dict: Dictionary with device information
    """
    # Read vendor and product IDs
    vendor_id = _read_file_safe(device_path / "idVendor")
    product_id = _read_file_safe(device_path / "idProduct")
    
    # Format vendor/product IDs with 0x prefix
    if vendor_id:
        try:
            vendor_id = f"0x{vendor_id.lower()}"
        except ValueError:
            pass
    
    if product_id:
        try:
            product_id = f"0x{product_id.lower()}"
        except ValueError:
            pass
    
    # Read manufacturer, product, and serial strings
    manufacturer = _read_file_safe(device_path / "manufacturer")
    product = _read_file_safe(device_path / "product")
    serial = _read_file_safe(device_path / "serial")
    
    # Find block devices
    block_devices = _find_block_devices(device_path, port_str)
    
    return {
        "vendor_id": vendor_id if vendor_id else None,
        "product_id": product_id if product_id else None,
        "manufacturer": manufacturer if manufacturer else None,
        "product": product if product else None,
        "serial": serial if serial else None,
        "block_devices": block_devices
    }


def _find_block_devices(device_path, port_str):
    """
    Find block devices associated with a USB device.
    
    Args:
        device_path: Path to the USB device in sysfs
        port_str: Port string identifier
        
    Returns:
        list: List of block device paths (e.g., ["/dev/sda"])
    """
    block_devices = []
    
    # Check if this device has a block/ subdirectory
    block_dir = device_path / "block"
    if block_dir.exists() and block_dir.is_dir():
        # Get all block device names
        for block_name in block_dir.iterdir():
            if block_name.is_dir():
                dev_path = Path(f"/dev/{block_name.name}")
                if dev_path.exists():
                    block_devices.append(str(dev_path))
    
    # Alternative: scan /sys/block and match via USB device path
    # USB storage devices go through SCSI, so we need to follow the path up to find USB device
    if not block_devices:
        try:
            sys_block = Path("/sys/block")
            if sys_block.exists():
                for block_name in sys_block.iterdir():
                    # Skip partitions (they have numbers after the base name like sda1, sda2)
                    # Only check base devices (sda, sdb, etc.)
                    if len(block_name.name) > 3 and block_name.name[3:].isdigit():
                        continue
                    
                    # Check if this block device is associated with our USB device
                    device_link = sys_block / block_name.name / "device"
                    if device_link.exists():
                        try:
                            # Follow symlink and check if port string appears in path
                            resolved = device_link.resolve()
                            resolved_str = str(resolved)
                            
                            # Check if the port string appears in the resolved path
                            # USB devices are at paths like: .../usb1/1-2/1-2.3/...
                            # Match port string as a complete component (not substring)
                            # Use path components to avoid false matches (e.g., "1-2" matching "1-2.3")
                            path_parts = resolved_str.split('/')
                            
                            # Find USB device components in the path
                            # We need to match the exact port_str, not a parent or child
                            port_found = False
                            for part in path_parts:
                                # Check if this part exactly matches our port string
                                if part == port_str:
                                    # Make sure no other part is a child of this port
                                    # (e.g., if port_str is "1-2", we don't want to match if "1-2.3" exists)
                                    is_parent = False
                                    for other_part in path_parts:
                                        if other_part.startswith(port_str + '.'):
                                            is_parent = True
                                            break
                                    if not is_parent:
                                        port_found = True
                                        break
                            
                            if port_found:
                                dev_path = Path(f"/dev/{block_name.name}")
                                if dev_path.exists():
                                    block_devices.append(str(dev_path))
                        except (OSError, RuntimeError) as e:
                            logger.debug(f"Error resolving device link for {block_name.name}: {e}")
                            pass
        except (IOError, OSError, PermissionError) as e:
            logger.debug(f"Error scanning /sys/block: {e}")
    
    return sorted(block_devices)


def enumerate_all_usb_ports():
    """
    Enumerate all physical USB ports on the system, including empty ports.
    
    Returns:
        dict: Dictionary mapping port strings to device info dictionaries
        
    Raises:
        RuntimeError: If not running on Linux or sysfs is not available
    """
    # Check if we're on Linux
    if platform.system() != 'Linux':
        raise RuntimeError("USB enumeration is only supported on Linux")
    
    # Check if sysfs is available
    if not USB_SYSFS_PATH.exists():
        raise RuntimeError(f"USB sysfs not found at {USB_SYSFS_PATH}. Is this a Linux system?")
    
    all_ports = {}
    
    # Enumerate all USB buses
    try:
        for bus_entry in USB_SYSFS_PATH.iterdir():
            if bus_entry.name.startswith("usb") and bus_entry.is_dir():
                # Extract bus number from name (e.g., "usb1" -> 1)
                try:
                    bus_num = int(bus_entry.name[3:])
                except ValueError:
                    logger.debug(f"Could not parse bus number from {bus_entry.name}")
                    continue
                
                # Enumerate ports on this bus
                bus_ports = _enumerate_ports_recursive(bus_num, bus_entry, "")
                all_ports.update(bus_ports)
    except (IOError, OSError, PermissionError) as e:
        raise RuntimeError(f"Error reading USB sysfs: {e}")
    
    return all_ports


def find_first_usb_hub(ports_data):
    """
    Find the first USB hub in the ports data.
    
    A hub is identified by having child ports (ports starting with its name + ".")
    
    Args:
        ports_data: Dictionary mapping port strings to device info
        
    Returns:
        str: Port string of the first hub found, or None if no hub found
    """
    # Sort ports to get consistent ordering
    sorted_ports = sorted(ports_data.keys())
    
    for port_str in sorted_ports:
        # Check if this port has children (indicating it's a hub)
        port_prefix = port_str + "."
        has_children = any(p.startswith(port_prefix) for p in ports_data.keys())
        
        if has_children:
            return port_str
    
    return None


def filter_ports_by_limit(ports_data, limit_port):
    """
    Filter ports data to only include the specified port and its downstream ports.
    
    Args:
        ports_data: Dictionary mapping port strings to device info
        limit_port: Port string to limit output to (e.g., "1-2")
        
    Returns:
        dict: Filtered dictionary containing only the limit port and its downstream ports
    """
    filtered = {}
    
    # Include the limit port itself if it exists
    if limit_port in ports_data:
        filtered[limit_port] = ports_data[limit_port]
    
    # Include all ports that are children of the limit port
    # Children have the format: limit_port.port_num (e.g., "1-2.3")
    limit_prefix = limit_port + "."
    for port_str, port_info in ports_data.items():
        if port_str.startswith(limit_prefix):
            filtered[port_str] = port_info
    
    return filtered


def format_usb_output(ports_data, json_output=False):
    """
    Format USB port enumeration data for output.
    
    Args:
        ports_data: Dictionary mapping port strings to device info
        json_output: If True, return JSON string; otherwise return human-readable text
        
    Returns:
        str: Formatted output string
    """
    if json_output:
        return json.dumps(ports_data, indent=2, sort_keys=True)
    
    # Human-readable format
    lines = []
    for port_str in sorted(ports_data.keys()):
        info = ports_data[port_str]
        
        # Check if port is empty
        if info["vendor_id"] is None and info["product_id"] is None:
            lines.append(f"{port_str}: (empty)")
        else:
            # Build device description
            parts = []
            
            # Vendor:Product IDs
            if info["vendor_id"] and info["product_id"]:
                parts.append(f"{info['vendor_id']}:{info['product_id']}")
            
            # Manufacturer and product names
            name_parts = []
            if info["manufacturer"]:
                name_parts.append(info["manufacturer"])
            if info["product"]:
                name_parts.append(info["product"])
            
            if name_parts:
                parts.append(" ".join(name_parts))
            
            # Block devices
            if info["block_devices"]:
                parts.append(f"[{', '.join(info['block_devices'])}]")
            
            line = f"{port_str}: {' '.join(parts)}"
            lines.append(line)
    
    return "\n".join(lines)
