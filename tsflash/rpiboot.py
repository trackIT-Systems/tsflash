"""rpiboot wrapper functionality for booting Raspberry Pi into mass storage mode."""

import logging
import subprocess
import threading
import time

logger = logging.getLogger(__name__)

# Exit codes reported by run_rpiboot() for conditions detected on our side
EXIT_TIMEOUT = 124
EXIT_INTERRUPTED = 130


def _parse_stage(line):
    """Map an rpiboot output line to a short stage description (or None)."""
    line_lower = line.lower()
    if 'second stage boot server done' in line_lower:
        return "Boot complete"
    if 'waiting for' in line_lower:
        return "Waiting for device..."
    if 'sending' in line_lower:
        return "Sending boot files..."
    if 'file read:' in line_lower or 'loading:' in line_lower:
        file_part = line.split(':', 1)[1].strip()
        if len(file_part) > 30:
            file_part = file_part[:27] + "..."
        return f"Loading: {file_part}"
    return None


def run_rpiboot(port=None, verbose=False, stage_callback=None, timeout=None, stop_event=None):
    """
    Execute rpiboot to boot a Raspberry Pi into mass storage device mode.

    Success is determined solely by rpiboot's exit code: rpiboot prints
    "Failed ..." lines for transient errors it recovers from by retrying,
    while real failures make it exit with a non-zero code.

    Args:
        port: USB port pathname to target (e.g., "1-2.3"). If None, rpiboot
              will search for any compatible device.
        verbose: If True, pass -v flag to rpiboot and enable verbose logging.
        stage_callback: Optional callback function(stage: str) -> None called with
                        stage updates during boot process.
        timeout: Optional timeout in seconds. If rpiboot has not finished by then,
                 the process is killed and (False, EXIT_TIMEOUT) is returned.
        stop_event: Optional threading.Event; when set, the process is killed and
                    (False, EXIT_INTERRUPTED) is returned.

    Returns:
        tuple: (success: bool, exit_code: int)
            - success: True if rpiboot completed successfully, False otherwise
            - exit_code: Process exit code (0 for success, non-zero for errors,
              124 for timeout, 130 for interruption)
    """
    # Build the rpiboot command
    cmd = ['rpiboot']

    # Add verbose flag if set
    if verbose:
        cmd.append('-v')

    # Add port flag if specified
    if port:
        cmd.extend(['-p', port])

    logger.debug(f"Executing: {' '.join(cmd)}")

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line buffering
        )
    except FileNotFoundError:
        logger.error("rpiboot command not found. Please ensure rpiboot is installed and in PATH.")
        return (False, 1)
    except Exception as e:
        logger.error(f"Unexpected error executing rpiboot: {e}")
        return (False, 1)

    # Watchdog: kill rpiboot on timeout or stop request (otherwise it would wait
    # for a device forever and leak a process)
    kill_reason = []

    def _watchdog():
        deadline = time.monotonic() + timeout if timeout is not None else None
        while process.poll() is None:
            if stop_event is not None and stop_event.is_set():
                kill_reason.append('stopped')
            elif deadline is not None and time.monotonic() >= deadline:
                kill_reason.append('timeout')
            if kill_reason:
                try:
                    process.kill()
                except OSError:
                    pass
                return
            time.sleep(0.2)

    if timeout is not None or stop_event is not None:
        threading.Thread(target=_watchdog, daemon=True, name="rpiboot-watchdog").start()

    try:
        # Stream output line-by-line
        for line in process.stdout:
            line = line.rstrip('\n\r')
            if not line:
                continue

            stage = _parse_stage(line)
            if stage is None and verbose:
                logger.debug(line)
            else:
                logger.info(line)

            if stage is not None and stage_callback:
                stage_callback(stage)

        return_code = process.wait()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, terminating rpiboot...")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning("Process did not terminate gracefully, killing...")
            process.kill()
            process.wait()
        return (False, EXIT_INTERRUPTED)

    if 'timeout' in kill_reason:
        logger.error(f"rpiboot timed out after {timeout} seconds")
        if stage_callback:
            stage_callback("Timed out")
        return (False, EXIT_TIMEOUT)

    if 'stopped' in kill_reason:
        logger.info("rpiboot stopped")
        return (False, EXIT_INTERRUPTED)

    if return_code != 0:
        logger.error(f"rpiboot exited with code {return_code}")
        if stage_callback:
            stage_callback("Failed")
        return (False, return_code)

    logger.info("rpiboot completed successfully")
    return (True, 0)
