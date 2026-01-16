"""Configuration loading and validation for tsflashd."""

import logging
import os
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


class DaemonConfig:
    """Configuration for tsflashd daemon."""
    
    def __init__(self, config_dict: dict):
        """
        Initialize configuration from dictionary.
        
        Args:
            config_dict: Dictionary containing configuration values
            
        Raises:
            ValueError: If required fields are missing or invalid
        """
        # Required: image_path
        if 'image_path' not in config_dict:
            raise ValueError("Missing required configuration: image_path")
        
        self.image_path = config_dict['image_path']
        if not isinstance(self.image_path, str):
            raise ValueError("image_path must be a string")
        
        # Validate image file exists
        if not os.path.exists(self.image_path):
            raise ValueError(f"Image file does not exist: {self.image_path}")
        
        if not os.path.isfile(self.image_path):
            raise ValueError(f"Image path is not a file: {self.image_path}")
        
        # Optional: port (USB port to monitor)
        self.port = config_dict.get('port')
        if self.port is not None and not isinstance(self.port, str):
            raise ValueError("port must be a string (e.g., '1-2')")
        
        # Optional: block_size (default: "4M")
        self.block_size = config_dict.get('block_size', '4M')
        if not isinstance(self.block_size, str):
            raise ValueError("block_size must be a string (e.g., '4M')")
        
        # Optional: stable_delay (default: 3)
        self.stable_delay = config_dict.get('stable_delay', 3)
        try:
            self.stable_delay = float(self.stable_delay)
            if self.stable_delay < 0:
                raise ValueError("stable_delay must be non-negative")
        except (ValueError, TypeError):
            raise ValueError("stable_delay must be a number")
        
        # Optional: log_level (default: "INFO")
        self.log_level = config_dict.get('log_level', 'INFO')
        if not isinstance(self.log_level, str):
            raise ValueError("log_level must be a string")
        
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(f"log_level must be one of: {', '.join(valid_log_levels)}")
        
        self.log_level = self.log_level.upper()


def load_config(config_path: Optional[str] = None) -> DaemonConfig:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, uses default: /boot/firmware/tsflash.yml
        
    Returns:
        DaemonConfig: Loaded configuration object
        
    Raises:
        FileNotFoundError: If config file is not found
        ValueError: If config file is invalid
        yaml.YAMLError: If YAML parsing fails
    """
    # Determine config file path
    if config_path is None:
        config_path = '/boot/firmware/tsflash.yml'
    else:
        config_path = os.path.expanduser(config_path)
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Load YAML file
    try:
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in config file: {e}")
    except IOError as e:
        raise IOError(f"Cannot read config file: {e}")
    
    if config_dict is None:
        raise ValueError("Configuration file is empty")
    
    if not isinstance(config_dict, dict):
        raise ValueError("Configuration file must contain a YAML dictionary")
    
    # Create and validate config object
    try:
        config = DaemonConfig(config_dict)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except ValueError as e:
        raise ValueError(f"Invalid configuration: {e}")
