"""
Logger Utility Module

This module provides comprehensive logging functionality for the Canvas Downloader
application. It uses the loguru library for enhanced logging capabilities with
automatic file rotation, colored console output, and structured logging.

Features:
- Automatic log file rotation based on size
- Colored console output with different levels
- Structured logging with context information
- Performance timing decorators
- Error tracking and reporting
- Configuration-based setup
- Thread-safe logging

Usage:
    from src.utils.logger import get_logger

    logger = get_logger(__name__)
    logger.info("Application started")
    logger.error("Something went wrong", extra={"user_id": 123})

    # Use timing decorator
    @log_execution_time
    def download_file():
        pass
"""

import sys
import functools
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Optional, Callable
from datetime import datetime

try:
    from loguru import logger as loguru_logger
except ImportError:
    raise ImportError("Loguru not found. Please install with: pip install loguru")

from ..config.settings import get_config


class CanvasDownloaderLogger:
    """
    Canvas Downloader Logger Class

    This class provides a centralized logging system for the entire application.
    It configures loguru with appropriate handlers, formatters, and rotation
    policies based on the application configuration.

    The logger supports:
    - Multiple output targets (console, file)
    - Automatic log rotation and cleanup
    - Contextual logging with extra fields
    - Performance monitoring
    - Error tracking with stack traces
    - Different log levels for different components
    """

    def __init__(self):
        """Initialize the logger with configuration from settings."""
        self.config = get_config().logging
        self._initialized = False
        self._loggers = {}
        self._setup_logging()

    def _setup_logging(self):
        """
        Set up the logging configuration based on application settings.

        This method configures loguru with console and file handlers,
        sets up appropriate formatting, and establishes log rotation policies.
        """
        if self._initialized:
            return

        # Remove default handler
        loguru_logger.remove()

        # Add console handler with colored output
        console_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )

        loguru_logger.add(
            sys.stdout,
            format=console_format,
            level=self.config.level,
            colorize=True,
            backtrace=True,
            diagnose=True
        )

        # Add file handler with rotation
        log_file_path = Path(self.config.file)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        file_format = (
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
            "{level: <8} | "
            "{name}:{function}:{line} | "
            "{message}"
        )

        # Parse max_size to bytes
        max_size = self._parse_size(self.config.max_size)

        loguru_logger.add(
            str(log_file_path),
            format=file_format,
            level=self.config.level,
            rotation=max_size,
            retention=self.config.backup_count,
            compression="gz",
            backtrace=True,
            diagnose=True,
            enqueue=True  # Thread-safe logging
        )

        # Add error-only handler for critical issues
        error_log_path = log_file_path.parent / "errors.log"
        loguru_logger.add(
            str(error_log_path),
            format=file_format,
            level="ERROR",
            rotation="10 MB",
            retention=10,
            compression="gz",
            backtrace=True,
            diagnose=True,
            enqueue=True
        )

        self._initialized = True
        loguru_logger.info("Logging system initialized",
                           extra={"log_file": str(log_file_path), "level": self.config.level})

    def _parse_size(self, size_str: str) -> str:
        """
        Parse size string to loguru-compatible format.

        Args:
            size_str: Size string like "10MB", "1GB", etc.

        Returns:
            str: Loguru-compatible size string
        """
        size_str = size_str.upper().replace('B', ' B')
        return size_str

    def get_logger(self, name: str) -> "LoggerAdapter":
        """
        Get a logger instance for a specific module or component.

        Args:
            name: Logger name (usually __name__ from the calling module)

        Returns:
            LoggerAdapter: Configured logger instance
        """
        if name not in self._loggers:
            self._loggers[name] = LoggerAdapter(loguru_logger, name)

        return self._loggers[name]

    def set_level(self, level: str):
        """
        Change the logging level for all handlers.

        Args:
            level: New logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        # This would require reconfiguring loguru handlers
        # For now, we'll log the level change
        loguru_logger.info(f"Logging level change requested: {level}")
        # Note: Loguru doesn't have a simple way to change levels dynamically
        # This would require removing and re-adding handlers

    def add_context(self, **kwargs):
        """
        Add contextual information to all log messages.

        Args:
            **kwargs: Context key-value pairs
        """
        return loguru_logger.bind(**kwargs)


class LoggerAdapter:
    """
    Logger Adapter Class

    This class provides a convenient interface to the loguru logger with
    additional features specific to the Canvas Downloader application.
    It maintains context information and provides helper methods for
    common logging patterns.
    """

    def __init__(self, logger, name: str):
        """
        Initialize the logger adapter.

        Args:
            logger: Loguru logger instance
            name: Name for this logger (usually module name)
        """
        self.logger = logger.bind(name=name)
        self.name = name
        self._context = {}

    def bind(self, **kwargs):
        """
        Bind additional context to this logger.

        Args:
            **kwargs: Context key-value pairs

        Returns:
            LoggerAdapter: New logger instance with bound context
        """
        new_context = {**self._context, **kwargs}
        adapter = LoggerAdapter(self.logger, self.name)
        adapter._context = new_context
        adapter.logger = self.logger.bind(**new_context)
        return adapter

    def debug(self, message: str, **kwargs):
        """Log a debug message."""
        self.logger.debug(message, **kwargs)

    def info(self, message: str, **kwargs):
        """Log an info message."""
        self.logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log a warning message."""
        self.logger.warning(message, **kwargs)

    def error(self, message: str, exception: Exception = None, **kwargs):
        """
        Log an error message with optional exception details.

        Args:
            message: Error message
            exception: Optional exception object
            **kwargs: Additional context
        """
        if exception:
            kwargs.update({
                "exception_type": type(exception).__name__,
                "exception_message": str(exception),
                "traceback": traceback.format_exc()
            })

        self.logger.error(message, **kwargs)

    def critical(self, message: str, exception: Exception = None, **kwargs):
        """
        Log a critical message with optional exception details.

        Args:
            message: Critical message
            exception: Optional exception object
            **kwargs: Additional context
        """
        if exception:
            kwargs.update({
                "exception_type": type(exception).__name__,
                "exception_message": str(exception),
                "traceback": traceback.format_exc()
            })

        self.logger.critical(message, **kwargs)

    def exception(self, message: str, **kwargs):
        """
        Log an exception with full traceback.

        Args:
            message: Exception message
            **kwargs: Additional context
        """
        self.logger.exception(message, **kwargs)

    def log_download_start(self, item_type: str, item_name: str, course_name: str):
        """
        Log the start of a download operation.

        Args:
            item_type: Type of content being downloaded
            item_name: Name of the item
            course_name: Name of the course
        """
        self.info(
            f"Starting download: {item_type}",
            item_type=item_type,
            item_name=item_name,
            course_name=course_name,
            operation="download_start"
        )

    def log_download_complete(self, item_type: str, item_name: str,
                              file_path: str, file_size: int = None, duration: float = None):
        """
        Log the completion of a download operation.

        Args:
            item_type: Type of content downloaded
            item_name: Name of the item
            file_path: Path where file was saved
            file_size: Size of downloaded file in bytes
            duration: Download duration in seconds
        """
        extra_data = {
            "item_type": item_type,
            "item_name": item_name,
            "file_path": file_path,
            "operation": "download_complete"
        }

        if file_size is not None:
            extra_data["file_size_bytes"] = file_size
            extra_data["file_size_mb"] = round(file_size / 1024 / 1024, 2)

        if duration is not None:
            extra_data["duration_seconds"] = round(duration, 2)

        self.info(f"Download completed: {item_type}", **extra_data)

    def log_download_skip(self, item_type: str, item_name: str, reason: str):
        """
        Log when a download is skipped.

        Args:
            item_type: Type of content
            item_name: Name of the item
            reason: Reason for skipping
        """
        self.info(
            f"Download skipped: {item_type}",
            item_type=item_type,
            item_name=item_name,
            reason=reason,
            operation="download_skip"
        )

    def log_api_call(self, endpoint: str, method: str = "GET", duration: float = None):
        """
        Log an API call.

        Args:
            endpoint: API endpoint
            method: HTTP method
            duration: Call duration in seconds
        """
        extra_data = {
            "endpoint": endpoint,
            "method": method,
            "operation": "api_call"
        }

        if duration is not None:
            extra_data["duration_seconds"] = round(duration, 3)

        self.debug(f"API call: {method} {endpoint}", **extra_data)

    def log_course_processing(self, course_name: str, course_id: str, operation: str):
        """
        Log course processing operations.

        Args:
            course_name: Name of the course
            course_id: Course ID
            operation: Type of operation (start, complete, error)
        """
        self.info(
            f"Course {operation}: {course_name}",
            course_name=course_name,
            course_id=course_id,
            operation=f"course_{operation}"
        )


def log_execution_time(func: Callable) -> Callable:
    """
    Decorator to log function execution time.

    Args:
        func: Function to decorate

    Returns:
        Callable: Decorated function
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time

            logger.debug(
                f"Function executed: {func.__name__}",
                function_name=func.__name__,
                duration_seconds=round(duration, 3),
                operation="function_execution"
            )

            return result

        except Exception as e:
            duration = time.time() - start_time

            logger.error(
                f"Function failed: {func.__name__}",
                function_name=func.__name__,
                duration_seconds=round(duration, 3),
                operation="function_error",
                exception=e
            )

            raise

    return wrapper


def log_api_call(func: Callable) -> Callable:
    """
    Decorator to log API calls with timing.

    Args:
        func: Function to decorate (should be an API call function)

    Returns:
        Callable: Decorated function
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = time.time()

        # Try to extract endpoint information
        endpoint = "unknown"
        if args and hasattr(args[0], '__class__'):
            endpoint = f"{args[0].__class__.__name__}.{func.__name__}"

        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time

            logger.log_api_call(endpoint, duration=duration)
            return result

        except Exception as e:
            duration = time.time() - start_time

            logger.error(
                f"API call failed: {endpoint}",
                endpoint=endpoint,
                duration_seconds=round(duration, 3),
                operation="api_error",
                exception=e
            )

            raise

    return wrapper


# Global logger instance
_logger_instance = None


def get_logger(name: str = None) -> LoggerAdapter:
    """
    Get a logger instance for the specified name.

    Args:
        name: Logger name (usually __name__ from calling module)

    Returns:
        LoggerAdapter: Configured logger instance
    """
    global _logger_instance

    if _logger_instance is None:
        _logger_instance = CanvasDownloaderLogger()

    if name is None:
        name = "canvas_downloader"

    return _logger_instance.get_logger(name)


def setup_logging():
    """
    Initialize the logging system.

    This function should be called once at application startup.
    """
    global _logger_instance

    if _logger_instance is None:
        _logger_instance = CanvasDownloaderLogger()

    logger = get_logger("setup")
    logger.info("Canvas Downloader logging system ready")


def shutdown_logging():
    """
    Shutdown the logging system gracefully.

    This function should be called at application exit.
    """
    logger = get_logger("shutdown")
    logger.info("Canvas Downloader shutting down")

    # Loguru handles cleanup automatically
    loguru_logger.remove()


# Initialize logging when module is imported
setup_logging()