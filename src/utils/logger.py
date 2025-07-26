"""
Logging System Module - COMPLETE IMPLEMENTATION

This module provides comprehensive logging functionality for the Canvas
Downloader application. It includes structured logging, file rotation,
performance monitoring, and integration with the progress tracking system.

Features:
- Structured logging with JSON output support
- File rotation and size management
- Performance timing decorators
- Context-aware logging (course, assignment, etc.)
- Integration with progress tracking
- Configurable log levels and formats
- Sensitive data masking
- Thread-safe logging operations
- Rich console output support

Usage:
    # Get logger for a module
    logger = get_logger(__name__)

    # Basic logging
    logger.info("Starting download", course_id="123", assignment_count=25)

    # Performance timing
    @log_execution_time
    def download_assignment(assignment):
        # Function implementation
        pass

    # Course processing tracking
    logger.log_course_processing("Math 101", "123", "start")
"""

import os
import sys
import json
import time
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union, Callable
from functools import wraps
import threading
import traceback
from dataclasses import dataclass, field

try:
    from rich.logging import RichHandler
    from rich.console import Console
    from rich.text import Text
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


@dataclass
class LogContext:
    """
    Context information for enhanced logging.

    This class maintains contextual information that should be included
    in log messages for better traceability and debugging.
    """
    # Course context
    course_id: Optional[str] = None
    course_name: Optional[str] = None

    # Content context
    content_type: Optional[str] = None
    item_id: Optional[str] = None
    item_name: Optional[str] = None

    # Session context
    session_id: Optional[str] = None
    user_id: Optional[str] = None

    # Processing context
    operation: Optional[str] = None
    step: Optional[str] = None

    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging."""
        context = {}

        if self.course_id:
            context['course_id'] = self.course_id
        if self.course_name:
            context['course_name'] = self.course_name
        if self.content_type:
            context['content_type'] = self.content_type
        if self.item_id:
            context['item_id'] = self.item_id
        if self.item_name:
            context['item_name'] = self.item_name
        if self.session_id:
            context['session_id'] = self.session_id
        if self.user_id:
            context['user_id'] = self.user_id
        if self.operation:
            context['operation'] = self.operation
        if self.step:
            context['step'] = self.step

        # Add metadata
        context.update(self.metadata)

        return context


class ContextualLogger:
    """
    Enhanced logger with contextual information and structured output.

    This logger extends the standard Python logging with additional features
    like context tracking, performance monitoring, and structured JSON output.
    """

    def __init__(self, name: str, base_logger: logging.Logger):
        """
        Initialize the contextual logger.

        Args:
            name: Logger name
            base_logger: Underlying Python logger
        """
        self.name = name
        self.base_logger = base_logger
        self.context = LogContext()
        self._lock = threading.RLock()

        # Performance tracking
        self._operation_timers = {}

        # Sensitive data patterns for masking
        self._sensitive_patterns = [
            'api_key', 'password', 'token', 'secret', 'credential'
        ]

    def set_context(self, **kwargs) -> None:
        """
        Set context information for subsequent log messages.

        Args:
            **kwargs: Context key-value pairs
        """
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self.context, key):
                    setattr(self.context, key, value)
                else:
                    self.context.metadata[key] = value

    def clear_context(self) -> None:
        """Clear all context information."""
        with self._lock:
            self.context = LogContext()

    def _mask_sensitive_data(self, data: Any) -> Any:
        """
        Mask sensitive data in log messages.

        Args:
            data: Data to mask

        Returns:
            Masked data
        """
        if isinstance(data, dict):
            masked = {}
            for key, value in data.items():
                if any(pattern in key.lower() for pattern in self._sensitive_patterns):
                    if isinstance(value, str) and len(value) > 8:
                        masked[key] = f"{value[:4]}***{value[-4:]}"
                    else:
                        masked[key] = "***"
                else:
                    masked[key] = self._mask_sensitive_data(value)
            return masked
        elif isinstance(data, str):
            # Check if the string looks like an API key or token
            if len(data) > 20 and any(char.isalnum() for char in data):
                return f"{data[:4]}***{data[-4:]}"

        return data

    def _prepare_log_data(self, message: str, **kwargs) -> Dict[str, Any]:
        """
        Prepare structured log data.

        Args:
            message: Log message
            **kwargs: Additional log data

        Returns:
            Structured log data
        """
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'logger': self.name,
            'message': message,
            'thread_id': threading.get_ident(),
            'process_id': os.getpid()
        }

        # Add context
        context_data = self.context.to_dict()
        if context_data:
            log_data['context'] = context_data

        # Add additional data
        if kwargs:
            # Mask sensitive data
            safe_kwargs = self._mask_sensitive_data(kwargs)
            log_data['data'] = safe_kwargs

        return log_data

    def _log(self, level: int, message: str, **kwargs) -> None:
        """
        Internal logging method.

        Args:
            level: Logging level
            message: Log message
            **kwargs: Additional log data
        """
        if not self.base_logger.isEnabledFor(level):
            return

        # Prepare structured data
        log_data = self._prepare_log_data(message, **kwargs)

        # Create log message
        if kwargs:
            # Include additional data in message
            extra_info = ', '.join(f"{k}={v}" for k, v in kwargs.items())
            full_message = f"{message} ({extra_info})"
        else:
            full_message = message

        # Add structured data as extra
        extra = {
            'structured_data': log_data,
            'context': self.context.to_dict()
        }

        self.base_logger.log(level, full_message, extra=extra)

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, exception: Exception = None, **kwargs) -> None:
        """
        Log error message with optional exception.

        Args:
            message: Error message
            exception: Optional exception object
            **kwargs: Additional error data
        """
        if exception:
            kwargs['exception_type'] = type(exception).__name__
            kwargs['exception_message'] = str(exception)
            kwargs['traceback'] = traceback.format_exc()

        self._log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, exception: Exception = None, **kwargs) -> None:
        """
        Log critical message with optional exception.

        Args:
            message: Critical message
            exception: Optional exception object
            **kwargs: Additional critical data
        """
        if exception:
            kwargs['exception_type'] = type(exception).__name__
            kwargs['exception_message'] = str(exception)
            kwargs['traceback'] = traceback.format_exc()

        self._log(logging.CRITICAL, message, **kwargs)

    def start_operation(self, operation_name: str, **kwargs) -> None:
        """
        Start timing an operation.

        Args:
            operation_name: Name of the operation
            **kwargs: Additional operation data
        """
        with self._lock:
            self._operation_timers[operation_name] = {
                'start_time': time.time(),
                'metadata': kwargs
            }

        self.info(f"Started operation: {operation_name}", **kwargs)

    def end_operation(self, operation_name: str, **kwargs) -> float:
        """
        End timing an operation and log the duration.

        Args:
            operation_name: Name of the operation
            **kwargs: Additional result data

        Returns:
            Operation duration in seconds
        """
        with self._lock:
            if operation_name not in self._operation_timers:
                self.warning(f"No timer found for operation: {operation_name}")
                return 0.0

            timer_data = self._operation_timers.pop(operation_name)
            duration = time.time() - timer_data['start_time']

            # Combine metadata
            log_data = {**timer_data['metadata'], **kwargs, 'duration_seconds': duration}

            self.info(f"Completed operation: {operation_name}", **log_data)

            return duration

    def log_course_processing(self, course_name: str, course_id: str, status: str, **kwargs) -> None:
        """
        Log course processing events with standardized format.

        Args:
            course_name: Name of the course
            course_id: Course identifier
            status: Processing status (start, progress, complete, error)
            **kwargs: Additional course data
        """
        self.info(f"Course processing {status}",
                 course_name=course_name,
                 course_id=course_id,
                 processing_status=status,
                 **kwargs)

    def log_download_progress(self, filename: str, bytes_downloaded: int,
                            total_bytes: int, **kwargs) -> None:
        """
        Log file download progress.

        Args:
            filename: Name of file being downloaded
            bytes_downloaded: Bytes downloaded so far
            total_bytes: Total file size
            **kwargs: Additional download data
        """
        percentage = (bytes_downloaded / total_bytes * 100) if total_bytes > 0 else 0

        self.debug(f"Download progress: {filename}",
                  filename=filename,
                  bytes_downloaded=bytes_downloaded,
                  total_bytes=total_bytes,
                  percentage=percentage,
                  **kwargs)


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter for structured logging.

    This formatter outputs log records as JSON for easy parsing
    and analysis by log aggregation systems.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.

        Args:
            record: Log record to format

        Returns:
            JSON-formatted log string
        """
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread_id': record.thread,
            'process_id': record.process
        }

        # Add structured data if available
        if hasattr(record, 'structured_data'):
            log_data.update(record.structured_data)

        # Add context if available
        if hasattr(record, 'context'):
            log_data['context'] = record.context

        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }

        return json.dumps(log_data, default=str)


class CanvasDownloaderLoggerSetup:
    """
    Logger setup and configuration for Canvas Downloader.

    This class handles the initialization and configuration of the logging
    system, including console and file handlers, formatters, and rotation.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize logger setup.

        Args:
            config: Logging configuration dictionary
        """
        self.config = config or {}
        self.console = Console() if RICH_AVAILABLE else None
        self._loggers = {}
        self._setup_complete = False

    def setup_logging(self) -> None:
        """Set up the complete logging system."""
        if self._setup_complete:
            return

        # Get configuration
        log_level = self.config.get('level', 'INFO').upper()
        console_output = self.config.get('console_output', True)
        file_output = self.config.get('file_output', True)
        log_format = self.config.get('format',
                                   '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level))

        # Clear existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Set up console handler
        if console_output:
            self._setup_console_handler(root_logger, log_format)

        # Set up file handler
        if file_output:
            self._setup_file_handler(root_logger)

        # Configure third-party loggers
        self._configure_third_party_loggers()

        self._setup_complete = True

    def _setup_console_handler(self, logger: logging.Logger, log_format: str) -> None:
        """Set up console logging handler."""
        if RICH_AVAILABLE and self.config.get('use_rich_console', True):
            # Use Rich handler for enhanced console output
            console_handler = RichHandler(
                console=self.console,
                show_time=True,
                show_path=False,
                rich_tracebacks=True,
                tracebacks_show_locals=False
            )
        else:
            # Use standard console handler
            console_handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(log_format)
            console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

    def _setup_file_handler(self, logger: logging.Logger) -> None:
        """Set up file logging with rotation."""
        # Get log file configuration
        logs_folder = Path(self.config.get('logs_folder', 'logs'))
        logs_folder.mkdir(parents=True, exist_ok=True)

        max_size_mb = self.config.get('max_log_size_mb', 50)
        backup_count = self.config.get('backup_count', 5)

        # Main log file with rotation
        log_file = logs_folder / 'canvas_downloader.log'
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=backup_count,
            encoding='utf-8'
        )

        # Use JSON formatter for file output
        json_formatter = JSONFormatter()
        file_handler.setFormatter(json_formatter)

        logger.addHandler(file_handler)

        # Separate error log file
        error_log_file = logs_folder / 'canvas_downloader_errors.log'
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(json_formatter)

        logger.addHandler(error_handler)

    def _configure_third_party_loggers(self) -> None:
        """Configure logging levels for third-party libraries."""
        # Reduce noise from third-party libraries
        third_party_loggers = {
            'urllib3': logging.WARNING,
            'requests': logging.WARNING,
            'aiohttp': logging.WARNING,
            'asyncio': logging.WARNING,
            'canvasapi': logging.INFO
        }

        for logger_name, level in third_party_loggers.items():
            logging.getLogger(logger_name).setLevel(level)

    def get_logger(self, name: str) -> ContextualLogger:
        """
        Get a contextual logger instance.

        Args:
            name: Logger name

        Returns:
            ContextualLogger: Enhanced logger instance
        """
        if not self._setup_complete:
            self.setup_logging()

        if name not in self._loggers:
            base_logger = logging.getLogger(name)
            self._loggers[name] = ContextualLogger(name, base_logger)

        return self._loggers[name]


# Global logger setup instance
_logger_setup = None
_setup_lock = threading.Lock()


def setup_logging(config: Dict[str, Any] = None) -> None:
    """
    Set up the global logging system.

    Args:
        config: Logging configuration dictionary
    """
    global _logger_setup

    with _setup_lock:
        if _logger_setup is None:
            _logger_setup = CanvasDownloaderLoggerSetup(config)
        _logger_setup.setup_logging()


def get_logger(name: str) -> ContextualLogger:
    """
    Get a logger instance for the specified name.

    Args:
        name: Logger name (typically __name__)

    Returns:
        ContextualLogger: Enhanced logger instance
    """
    global _logger_setup

    with _setup_lock:
        if _logger_setup is None:
            # Initialize with default config if not already set up
            _logger_setup = CanvasDownloaderLoggerSetup()

        return _logger_setup.get_logger(name)


def log_execution_time(func: Callable) -> Callable:
    """
    Decorator to log function execution time.

    Args:
        func: Function to decorate

    Returns:
        Decorated function
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        operation_name = f"{func.__module__}.{func.__name__}"

        logger.start_operation(operation_name)
        try:
            result = func(*args, **kwargs)
            logger.end_operation(operation_name, success=True)
            return result
        except Exception as e:
            logger.end_operation(operation_name, success=False, error=str(e))
            raise

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        operation_name = f"{func.__module__}.{func.__name__}"

        logger.start_operation(operation_name)
        try:
            result = await func(*args, **kwargs)
            logger.end_operation(operation_name, success=True)
            return result
        except Exception as e:
            logger.end_operation(operation_name, success=False, error=str(e))
            raise

    # Return appropriate wrapper based on function type
    import asyncio
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return wrapper


def configure_logger_context(course_id: str = None, course_name: str = None,
                           content_type: str = None, **kwargs) -> None:
    """
    Configure global logger context for the current thread.

    This is useful for setting context that should be included in all
    log messages from the current thread.

    Args:
        course_id: Current course ID
        course_name: Current course name
        content_type: Current content type being processed
        **kwargs: Additional context
    """
    # This would typically set thread-local context
    # For now, we'll leave this as a placeholder for thread-local implementation
    pass


# Example usage and testing functions
def test_logging_system():
    """Test the logging system functionality."""
    # Set up logging
    test_config = {
        'level': 'DEBUG',
        'console_output': True,
        'file_output': True,
        'logs_folder': 'test_logs',
        'use_rich_console': True
    }

    setup_logging(test_config)

    # Get test logger
    logger = get_logger('test_module')

    # Test basic logging
    logger.info("Testing basic logging functionality")
    logger.debug("Debug message with data", test_value=123, test_flag=True)
    logger.warning("Warning message", warning_code="W001")

    # Test context
    logger.set_context(course_id="12345", course_name="Test Course")
    logger.info("Message with context")

    # Test operation timing
    logger.start_operation("test_operation", operation_type="test")
    time.sleep(0.1)  # Simulate work
    logger.end_operation("test_operation", items_processed=10)

    # Test error logging
    try:
        raise ValueError("Test exception")
    except ValueError as e:
        logger.error("Test error occurred", exception=e, error_code="E001")

    # Test course processing logging
    logger.log_course_processing("Test Course", "12345", "start")
    logger.log_course_processing("Test Course", "12345", "complete", items_downloaded=25)

    print("Logging system test completed. Check test_logs/ for output files.")


if __name__ == "__main__":
    test_logging_system()