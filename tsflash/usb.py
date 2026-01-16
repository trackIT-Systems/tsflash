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


def _get_related_bus_ports(port_str, raw_ports=None):
    """
    Get all bus port representations for a unified port.
    
    For a unified port (e.g., "3-1.4"), returns both USB 2.0 and USB 3.0
    port strings if they exist (e.g., ["3-1.4", "4-1.4"]).
    
    Args:
        port_str: Unified port string (USB 2.0 representation)
        raw_ports: Optional raw ports dictionary to check for USB 3.0 counterpart
        
    Returns:
        List of port strings on both buses
    """
    ports = [port_str]
    
    if raw_ports is None:
        return ports
    
    # Try to find USB 3.0 counterpart
    # Extract the hub part to check hub relations
    parts = port_str.split('.', 1)
    hub_port = parts[0]
    
    # Build hub relations if not already available
    hub_relations = _build_hub_relations(raw_ports)
    
    # Find USB 3.0 counterpart
    usb3_port = _find_usb3_counterpart(port_str, hub_relations)
    if usb3_port and usb3_port in raw_ports:
        ports.append(usb3_port)
    
    return ports


def _find_block_devices(device_path, port_str, raw_ports=None):
    """
    Find block devices associated with a USB device.
    
    For unified ports, also checks the USB 3.0 counterpart to find
    block devices that may be connected via USB 3.0.
    
    Args:
        device_path: Path to the USB device in sysfs
        port_str: Port string identifier (unified representation)
        raw_ports: Optional raw ports dictionary for finding USB 3.0 counterpart
        
    Returns:
        list: List of block device paths (e.g., ["/dev/sda"])
    """
    block_devices = []
    ports_to_check = [port_str]
    
    # If raw_ports provided, also check USB 3.0 counterpart
    if raw_ports is not None:
        related_ports = _get_related_bus_ports(port_str, raw_ports)
        ports_to_check = related_ports
    
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
                            
                            # Check if any of our port strings appears in the resolved path
                            # USB devices are at paths like: .../usb1/1-2/1-2.3/...
                            # Match port string as a complete component (not substring)
                            # Use path components to avoid false matches (e.g., "1-2" matching "1-2.3")
                            path_parts = resolved_str.split('/')
                            
                            # Check all port representations (USB 2.0 and USB 3.0)
                            port_found = False
                            for check_port in ports_to_check:
                                for part in path_parts:
                                    # Check if this part exactly matches our port string
                                    if part == check_port:
                                        # Make sure no other part is a child of this port
                                        # (e.g., if port_str is "1-2", we don't want to match if "1-2.3" exists)
                                        is_parent = False
                                        for other_part in path_parts:
                                            if other_part.startswith(check_port + '.'):
                                                is_parent = True
                                                break
                                        if not is_parent:
                                            port_found = True
                                            break
                                if port_found:
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


def _is_hub(port_str, ports_data):
    """
    Check if a port is a hub (has child ports).
    
    Args:
        port_str: Port string identifier
        ports_data: Dictionary mapping port strings to device info
        
    Returns:
        bool: True if port is a hub, False otherwise
    """
    port_prefix = port_str + "."
    return any(p.startswith(port_prefix) for p in ports_data.keys())


def _build_hub_relations(raw_ports):
    """
    Build mapping of USB 2.0 hub -> USB 3.0 hub.
    
    Identifies related hubs on consecutive buses (USB 2.0/3.0 pairs)
    that represent the same physical hub.
    
    Args:
        raw_ports: Raw ports dictionary from enumerate_all_usb_ports()
        
    Returns:
        Dictionary mapping USB 2.0 hub port to USB 3.0 hub port
    """
    relations = {}
    
    for port_str, port_info in raw_ports.items():
        try:
            bus_num = int(port_str.split('-')[0])
        except (ValueError, IndexError):
            continue
        
        # Only process USB 2.0 buses (odd numbers: 1, 3, 5, ...)
        # Note: This assumes USB 2.0 buses are odd and USB 3.0 are even
        # This may need adjustment based on actual system behavior
        if bus_num % 2 == 0:
            continue
        
        # Check if this is a hub
        if not _is_hub(port_str, raw_ports):
            continue
        
        # Check for USB 3.0 counterpart on next bus
        usb3_bus = bus_num + 1
        usb3_port = port_str.replace(f"{bus_num}-", f"{usb3_bus}-", 1)
        
        if usb3_port in raw_ports:
            usb3_info = raw_ports[usb3_port]
            # Check if same vendor (same physical hub)
            if (usb3_info.get('vendor_id') == port_info.get('vendor_id') and
                usb3_info.get('vendor_id') is not None and
                _is_hub(usb3_port, raw_ports)):
                relations[port_str] = usb3_port
    
    return relations


def _find_usb2_counterpart(usb3_port_str, hub_relations):
    """
    Find the USB 2.0 counterpart of a USB 3.0 port.
    
    Args:
        usb3_port_str: USB 3.0 port string (e.g., "4-1.4")
        hub_relations: Dictionary mapping USB 2.0 hub -> USB 3.0 hub
        
    Returns:
        USB 2.0 port string if found, None otherwise
    """
    try:
        bus_num = int(usb3_port_str.split('-')[0])
    except (ValueError, IndexError):
        return None
    
    # USB 3.0 buses are even, USB 2.0 are odd
    if bus_num % 2 != 0:
        return None
    
    # Extract the hub part (e.g., "4-1" from "4-1.4")
    parts = usb3_port_str.split('.', 1)
    usb3_hub = parts[0]
    
    # Find USB 2.0 hub counterpart
    usb2_hub = None
    for usb2_hub_port, usb3_hub_port in hub_relations.items():
        if usb3_hub_port == usb3_hub:
            usb2_hub = usb2_hub_port
            break
    
    if usb2_hub is None:
        return None
    
    # Reconstruct USB 2.0 port string
    if len(parts) > 1:
        # Has sub-ports (e.g., "4-1.4" -> "3-1.4")
        return usb3_port_str.replace(usb3_hub, usb2_hub, 1)
    else:
        # Just the hub itself
        return usb2_hub


def _find_usb3_counterpart(usb2_port_str, hub_relations):
    """
    Find the USB 3.0 counterpart of a USB 2.0 port.
    
    Args:
        usb2_port_str: USB 2.0 port string (e.g., "3-1.4")
        hub_relations: Dictionary mapping USB 2.0 hub -> USB 3.0 hub
        
    Returns:
        USB 3.0 port string if found, None otherwise
    """
    try:
        bus_num = int(usb2_port_str.split('-')[0])
    except (ValueError, IndexError):
        return None
    
    # USB 2.0 buses are odd, USB 3.0 are even
    if bus_num % 2 == 0:
        return None
    
    # Extract the hub part (e.g., "3-1" from "3-1.4")
    parts = usb2_port_str.split('.', 1)
    usb2_hub = parts[0]
    
    # Find USB 3.0 hub counterpart
    usb3_hub = hub_relations.get(usb2_hub)
    if usb3_hub is None:
        return None
    
    # Reconstruct USB 3.0 port string
    if len(parts) > 1:
        # Has sub-ports (e.g., "3-1.4" -> "4-1.4")
        return usb2_port_str.replace(usb2_hub, usb3_hub, 1)
    else:
        # Just the hub itself
        return usb3_hub


def _merge_port_info(usb2_info, usb3_info):
    """
    Merge port information from USB 2.0 and USB 3.0 buses.
    Prefers USB 3.0 device info when present, falls back to USB 2.0.
    
    Args:
        usb2_info: Port info dictionary from USB 2.0 bus
        usb3_info: Port info dictionary from USB 3.0 bus
        
    Returns:
        Merged port info dictionary
    """
    merged = {}
    
    # Prefer USB 3.0 values, fall back to USB 2.0
    merged['vendor_id'] = usb3_info.get('vendor_id') or usb2_info.get('vendor_id')
    merged['product_id'] = usb3_info.get('product_id') or usb2_info.get('product_id')
    merged['manufacturer'] = usb3_info.get('manufacturer') or usb2_info.get('manufacturer')
    merged['product'] = usb3_info.get('product') or usb2_info.get('product')
    merged['serial'] = usb3_info.get('serial') or usb2_info.get('serial')
    
    # Combine block devices from both buses
    block_devices = set(usb2_info.get('block_devices', []))
    block_devices.update(usb3_info.get('block_devices', []))
    merged['block_devices'] = sorted(block_devices)
    
    return merged


def unify_ports(raw_ports):
    """
    Merge USB 2.0/3.0 port pairs into unified port representation.
    
    Each physical port appears exactly once in the result.
    Uses USB 2.0 port string as canonical identifier.
    
    Args:
        raw_ports: Raw ports dictionary from enumerate_all_usb_ports()
        
    Returns:
        Unified ports dictionary with merged entries
    """
    unified = {}
    processed_usb3_ports = set()
    
    # Build hub relationship map
    hub_relations = _build_hub_relations(raw_ports)
    
    for port_str, port_info in raw_ports.items():
        # Skip USB 3.0 ports that will be merged
        try:
            bus_num = int(port_str.split('-')[0])
        except (ValueError, IndexError):
            # Invalid port format, include as-is
            unified[port_str] = port_info
            continue
        
        # Check if this is a USB 3.0 port (even bus number)
        # Note: This assumes USB 2.0 buses are odd and USB 3.0 are even
        if bus_num % 2 == 0:
            # Check if this port has a USB 2.0 counterpart
            usb2_port = _find_usb2_counterpart(port_str, hub_relations)
            if usb2_port:
                processed_usb3_ports.add(port_str)
                continue
        
        # Use USB 2.0 port as-is or merge if USB 3.0 counterpart exists
        if port_str not in processed_usb3_ports:
            usb3_port = _find_usb3_counterpart(port_str, hub_relations)
            if usb3_port and usb3_port in raw_ports:
                # Merge USB 2.0 and USB 3.0 port info
                merged_info = _merge_port_info(port_info, raw_ports[usb3_port])
                unified[port_str] = merged_info
                processed_usb3_ports.add(usb3_port)
            else:
                unified[port_str] = port_info
    
    return unified


def enumerate_all_usb_ports():
    """
    Enumerate all physical USB ports on the system, including empty ports.
    
    Returns unified ports where USB 2.0/3.0 port pairs are merged into
    a single entry (each physical port appears exactly once).
    
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
    
    # Unify USB 2.0/3.0 port pairs
    unified_ports = unify_ports(all_ports)
    
    return unified_ports


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


def is_rpiboot_device(port_info: dict) -> bool:
    """
    Check if a USB device is rpiboot-compatible.
    
    Args:
        port_info: Device info dictionary from USB enumeration
        
    Returns:
        True if device matches rpiboot-compatible vendor:product IDs, False otherwise
    """
    vendor_id = port_info.get("vendor_id")
    product_id = port_info.get("product_id")
    
    # Check for rpiboot-compatible devices: 0x0a5c:0x2712 or 0x0a5c:0x2711
    if vendor_id == "0x0a5c":
        if product_id == "0x2712" or product_id == "0x2711":
            return True
    
    return False


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
