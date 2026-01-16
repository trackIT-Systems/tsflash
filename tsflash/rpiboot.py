"""rpiboot wrapper functionality for booting Raspberry Pi into mass storage mode."""

import logging
import subprocess

logger = logging.getLogger(__name__)


def run_rpiboot(port=None, verbose=False, stage_callback=None):
    """
    Execute rpiboot to boot a Raspberry Pi into mass storage device mode.
    
    Args:
        port: USB port pathname to target (e.g., "1-2.3"). If None, rpiboot
              will search for any compatible device.
        verbose: If True, pass -v flag to rpiboot and enable verbose logging.
        stage_callback: Optional callback function(stage: str) -> None called with
                        stage updates during boot process.
    
    Returns:
        tuple: (success: bool, exit_code: int)
            - success: True if rpiboot completed successfully, False otherwise
            - exit_code: Process exit code (0 for success, non-zero for errors, 130 for interruption)
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
        # Start the rpiboot process
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line buffering
        )
        
        # Track state for parsing
        success_detected = False
        error_detected = False
        
        # Stream output line-by-line
        try:
            for line in process.stdout:
                line = line.rstrip('\n\r')
                if not line:
                    continue
                
                # Parse important messages
                line_lower = line.lower()
                
                # Detect errors (check for specific error patterns)
                if 'failed' in line_lower:
                    logger.error(line)
                    error_detected = True
                    if stage_callback:
                        stage_callback("Failed")
                # Detect success
                elif 'second stage boot server done' in line_lower:
                    logger.info(line)
                    success_detected = True
                    if stage_callback:
                        stage_callback("Boot complete")
                # Detect waiting stage
                elif 'waiting for' in line_lower:
                    logger.info(line)
                    if stage_callback:
                        stage_callback("Waiting for device...")
                # Detect sending stage
                elif 'sending' in line_lower:
                    logger.info(line)
                    if stage_callback:
                        stage_callback("Sending boot files...")
                # Detect loading stage
                elif 'file read:' in line_lower or 'loading:' in line_lower:
                    logger.info(line)
                    if stage_callback:
                        # Try to extract filename
                        if ':' in line:
                            file_part = line.split(':', 1)[1].strip()
                            # Truncate if too long
                            if len(file_part) > 30:
                                file_part = file_part[:27] + "..."
                            stage_callback(f"Loading: {file_part}")
                        else:
                            stage_callback("Loading files...")
                # Other messages - use DEBUG level for verbose details, INFO otherwise
                else:
                    if verbose:
                        logger.debug(line)
                    else:
                        logger.info(line)
            
            # Wait for process to complete
            return_code = process.wait()
            
            # Determine final status
            if return_code != 0:
                logger.error(f"rpiboot exited with code {return_code}")
                return (False, return_code)
            
            if error_detected:
                logger.error("rpiboot completed but errors were detected")
                return (False, 1)
            
            if success_detected:
                logger.info("rpiboot completed successfully")
                return (True, 0)
            
            # If we get here, process completed but no clear success/error
            logger.info("rpiboot process completed")
            return (True, 0)
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user, terminating rpiboot...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("Process did not terminate gracefully, killing...")
                process.kill()
                process.wait()
            return (False, 130)  # Standard SIGINT exit code
            
    except FileNotFoundError:
        logger.error("rpiboot command not found. Please ensure rpiboot is installed and in PATH.")
        return (False, 1)
    except Exception as e:
        logger.error(f"Unexpected error executing rpiboot: {e}")
        return (False, 1)
