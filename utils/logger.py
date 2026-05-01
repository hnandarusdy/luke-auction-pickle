"""
Reusable Logger Class

This module provides a centralized logging utility that can be used
across different modules in the project.

Author: GitHub Copilot
Date: October 3, 2025
"""

import logging
import os
from datetime import datetime
from typing import Optional


class Logger:
    """
    A reusable logger class that provides consistent logging across the application.
    
    Features:
    - Console and file logging
    - Customizable log levels
    - Automatic log file rotation by date
    - Thread-safe logging
    """
    
    _instances = {}
    
    def __new__(cls, name: str = "default", *args, **kwargs):
        """Ensure singleton pattern per logger name."""
        if name not in cls._instances:
            cls._instances[name] = super(Logger, cls).__new__(cls)
        return cls._instances[name]
    
    def __init__(
        self, 
        name: str = "default",
        level: int = logging.INFO,
        log_to_file: bool = True,
        log_file_dir: str = "logs",
        log_format: Optional[str] = None
    ):
        """
        Initialize the Logger.
        
        Args:
            name (str): Logger name (used for log file naming)
            level (int): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_to_file (bool): Whether to log to file in addition to console
            log_file_dir (str): Directory to store log files
            log_format (str): Custom log format string
        """
        # Prevent re-initialization
        if hasattr(self, '_initialized'):
            return
        
        self.name = name
        self.level = level
        self.log_to_file = log_to_file
        self.log_file_dir = log_file_dir
        
        # Default log format
        if log_format is None:
            self.log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        else:
            self.log_format = log_format
        
        # Create the logger
        self.logger = logging.getLogger(f"pickles_{name}")
        self.logger.setLevel(level)
        
        # Clear any existing handlers to prevent duplicates
        self.logger.handlers.clear()
        
        # Setup console handler
        self._setup_console_handler()
        
        # Setup file handler if requested
        if self.log_to_file:
            self._setup_file_handler()
        
        self._initialized = True
        self.info(f"Logger '{name}' initialized successfully")
    
    def _setup_console_handler(self) -> None:
        """Setup console logging handler."""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        
        formatter = logging.Formatter(self.log_format)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(console_handler)
    
    def _setup_file_handler(self) -> None:
        """Setup file logging handler with date-based naming."""
        try:
            # Create logs directory if it doesn't exist
            os.makedirs(self.log_file_dir, exist_ok=True)
            
            # Generate log filename with current date
            current_date = datetime.now().strftime("%Y-%m-%d")
            log_filename = f"{self.name}_{current_date}.log"
            log_filepath = os.path.join(self.log_file_dir, log_filename)
            
            # Create file handler
            file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
            file_handler.setLevel(self.level)
            
            formatter = logging.Formatter(self.log_format)
            file_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            
        except Exception as e:
            print(f"Warning: Could not setup file logging: {e}")
    
    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)
    
    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)
    
    def warning(self, message: str) -> None:
        """Log warning message."""
        self.logger.warning(message)
    
    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)
    
    def critical(self, message: str) -> None:
        """Log critical message."""
        self.logger.critical(message)
    
    def exception(self, message: str) -> None:
        """Log exception with traceback."""
        self.logger.exception(message)
    
    def set_level(self, level: int) -> None:
        """
        Change the logging level.
        
        Args:
            level (int): New logging level
        """
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)
    
    def get_logger(self) -> logging.Logger:
        """
        Get the underlying Python logger object.
        
        Returns:
            logging.Logger: The logger instance
        """
        return self.logger


# Convenience functions for quick logging
def get_logger(
    name: str = "default", 
    level: int = logging.INFO,
    log_to_file: bool = True
) -> Logger:
    """
    Get or create a logger instance.
    
    Args:
        name (str): Logger name
        level (int): Logging level
        log_to_file (bool): Whether to log to file
        
    Returns:
        Logger: Logger instance
    """
    return Logger(name=name, level=level, log_to_file=log_to_file)


# Example usage
if __name__ == "__main__":
    # Test the logger
    logger = get_logger("test", logging.DEBUG)
    
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    
    print("Logger test completed. Check console and logs/ directory.")