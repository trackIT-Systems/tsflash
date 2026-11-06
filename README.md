# tsflash

**SD Card Flashing Tool for Raspberry Pi**

tsflash is a powerful Python tool for flashing OS images to SD cards and other block devices. It features an interactive TUI for real-time monitoring and automatic device flashing, along with a comprehensive CLI for manual operations.

## TUI - Interactive Monitoring

The `tui` command provides a terminal-based interface for monitoring USB ports and automatically flashing connected devices:

![tsflash TUI](tsflash_action.png)

The TUI displays:
- **Real-time port status** - Monitor all USB downstream ports with color-coded states
- **Live progress tracking** - Watch flash operations with progress bars, transfer speeds, and completion status
- **Automatic device detection** - Automatically detects and flashes devices as they're connected
- **Parallel flashing** - Flash multiple devices simultaneously
- **Log panel** - View detailed operation logs in real-time

Run `tsflash tui <image>` to launch the TUI.

## Features

- **Interactive TUI** - Real-time monitoring and progress visualization (`tsflash tui`)
- **CLI Commands** - Manual control with `flash`, `usb`, `daemon`, and `rpiboot` subcommands
- **Safety** - Never flashes the disk the running system boots from
- **Automatic Device Detection** - Monitors USB hubs and automatically flashes connected devices
- **USB 2.0/3.0 Dual-Mode Hub Support** - Automatically detects and merges USB 2.0/3.0 port pairs, ensuring each physical port appears exactly once
- **Raspberry Pi rpiboot Support** - Boot Raspberry Pi devices into mass storage mode for flashing
- **Parallel Flashing** - Flash multiple devices simultaneously using memory-mapped images
- **Efficient I/O** - Uses memory-mapped files for fast, parallel access to image files

## Installation

tsflash requires Python 3.11 or higher.

Install dependencies:
```bash
pip install pyyaml rich
```

Or install the package:
```bash
pip install .
```

## Quick Start

### TUI Mode

Launch the interactive TUI with the image to flash:

```bash
sudo tsflash tui /path/to/image.img
```

The TUI will:
1. Auto-detect the first USB hub to monitor (or use `--port`)
2. Display all downstream ports in real-time
3. Automatically flash devices as they're connected

### Manual Flash Command

Flash an image to a specific device:

```bash
sudo tsflash flash /path/to/image.img /dev/sda
```

### Daemon Configuration

The daemon (`tsflashd` / `tsflash daemon`) reads its settings from a configuration file at `/boot/firmware/tsflash.yml` (use `tsflash.example.yml` as a template). The TUI does not use this file; it takes its settings as command line options.

```yaml
# Path to the image file to flash
image_path: /data/images/raspberry-pi-os.img

# USB port to monitor (optional - auto-detects first hub if not specified)
# port: 1-2

# Block size for flashing (default: 4M)
block_size: 4M

# Seconds to wait after device appears before flashing (default: 3)
stable_delay: 1

# Logging level (default: INFO)
log_level: INFO
```

## Usage

### TUI Mode

```bash
tsflash [-v | -q] tui <image_file> [--port 1-2] [--block-size 4M] [--stable-delay 3]
```

- `--port` - USB hub port to monitor (default: first hub found)
- `--block-size` - Block size for writing (default: 4M)
- `--stable-delay` - Seconds to wait after a device appears before flashing (default: 3)
- `-v` / `-q` - Debug / warning log level (global options, placed before `tui`)

The TUI automatically:
- Monitors USB ports for connected devices
- Detects block devices and rpiboot-compatible devices
- Flashes devices using the given image
- Displays real-time progress and logs

To quit, press `q` or `Ctrl+C` twice within 3 seconds. The first press shows a confirmation prompt, which also warns if flash operations are in progress; any other key cancels it. Flash operations still running when you quit are aborted, and the affected devices are listed as incomplete on exit.

#### Port States

| State | Meaning |
|-------|---------|
| `NOT_CONNECTED` | Port is empty |
| `UNKNOWN` | Device connected, but nothing to flash (e.g. a card reader without a card - shown as "No medium") |
| `BOOTING` | rpiboot is booting a Raspberry Pi into mass storage mode |
| `WAITING` | Block device detected, waiting `stable_delay` seconds before flashing |
| `FLASHING` | Image is being written |
| `COMPLETED` | Flash succeeded; remove the card/device to reset the port |
| `FAILED` | Flash or rpiboot failed; stays until the card (flash failure) or the device (rpiboot failure) is removed |
| `IGNORED` | Device is not handled: it holds the running system, or it exposes more than one block device |

### CLI Commands

#### Flash

Manually flash an image to a block device:

```bash
tsflash flash <image_file> <block_device> [--block-size 4M] [--non-interactive]
```

Examples:
```bash
sudo tsflash flash raspberry-pi-os.img /dev/sda
sudo tsflash flash image.img /dev/mmcblk0 --block-size 1M
```

#### USB

List USB ports and connected devices:

```bash
tsflash usb [--all] [--port 1-2] [--json]
```

- `--all` - Show all USB ports (default: first hub and downstream ports)
- `--port` - Limit output to a specific port and downstream ports
- `--json` - Output in JSON format

#### Daemon

Run the automatic flashing daemon (background service):

```bash
tsflashd [--config /path/to/config.yml]
```

The daemon runs continuously, monitoring USB ports and automatically flashing devices (same behaviour as the TUI). Useful for production environments where the TUI isn't needed. `-v` / `-q` override the `log_level` from the configuration file. `SIGTERM` / `SIGINT` stop the daemon and abort running flash operations.

#### rpiboot

Boot a Raspberry Pi device into mass storage mode:

```bash
tsflash [-v] rpiboot [-p 1-2.3]
```

This is useful for flashing Raspberry Pi devices that don't have an SD card inserted.

## USB 2.0/3.0 Dual-Mode Hub Support

USB 3.0 hubs present themselves to the Linux kernel as two separate hubs on different buses:
- A USB 2.0 hub on one bus (e.g., `3-1` on bus 3)
- A USB 3.0 hub on another bus (e.g., `4-1` on bus 4)

Devices that support USB 3.0 connect to the USB 3.0 bus, while USB 2.0-only devices connect to the USB 2.0 bus. tsflash automatically detects related USB 2.0/3.0 hub pairs and merges them into a unified port representation.

**Key Benefits:**
- Each physical port appears exactly once in `tsflash usb` output and the TUI
- Devices connected via USB 3.0 are automatically detected and shown on the unified port
- Devices connected via USB 2.0 are also detected and shown on the unified port
- Block devices from both buses are combined in the unified port entry

tsflash identifies related hubs by matching vendor IDs and port numbers on consecutive buses. The unified port uses the USB 2.0 port string as the canonical identifier (e.g., `3-1.4`) and merges device information from both buses, preferring USB 3.0 device information when available.

## Safety

- **System disk protection** - Disks holding `/`, `/boot` or `/boot/firmware` (including all of their partitions) are never flashed. In the TUI and daemon they are shown as `IGNORED`; `tsflash flash` refuses them.
- **One block device per port** - Devices exposing more than one flashable block device (e.g. multi-slot card readers with several cards) are not handled and shown as `IGNORED`.
- **No medium** - Block devices with size 0 (e.g. card readers without a card) are not flashed.
- **devmon** - `tsflash tui` and `tsflash daemon` refuse to start while `devmon` is running, as it automounts devices during flashing.

## Configuration

Configuration (daemon only) is stored in a YAML file (default: `/boot/firmware/tsflash.yml`). See `tsflash.example.yml` for a complete example.

**Key Settings:**

- `image_path` (required) - Path to the OS image file (.img or .iso)
- `port` (optional) - USB port to monitor (e.g., "1-2"). Auto-detects first hub if not specified
- `block_size` (optional) - Block size for reading/writing (default: "4M"). Examples: "4M", "1M", "512K"
- `stable_delay` (optional) - Seconds to wait after device detection before flashing (default: 3)
- `log_level` (optional) - Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)

## Commands Reference

| Command | Description |
|---------|-------------|
| `tsflash tui <file>` | Launch interactive TUI |
| `tsflash flash <file> <device>` | Manually flash an image to a block device |
| `tsflash usb` | List USB ports and connected devices |
| `tsflash daemon` | Run automatic flashing daemon (background service) |
| `tsflash rpiboot` | Boot Raspberry Pi into mass storage mode |

## Requirements

- Python 3.11+
- pyyaml >= 6.0
- rich >= 13.0.0

## License

[Add license information if applicable]
