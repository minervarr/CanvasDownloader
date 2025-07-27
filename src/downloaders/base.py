"""
Enhanced Base Downloader Module - COMPATIBLE IMPLEMENTATION

This module provides the enhanced base downloader class that works seamlessly
with the new bulletproof configuration system while preserving the existing
ContentDownloaderFactory class that other parts of the codebase depend on.

Key Features:
- Seamless integration with bulletproof configuration system
- Preserves existing ContentDownloaderFactory functionality
- Multiple ways to access configuration settings
- Comprehensive error handling that never crashes
- Intelligent fallback mechanisms for all settings
- Enhanced logging and debugging support
- Robust download statistics and progress tracking
- Full backward compatibility with existing downloaders

The Enhanced Base Downloader ensures:
- Configuration access never fails
- All settings have safe defaults
- Comprehensive validation of download parameters
- Robust error handling and recovery
- Detailed logging for troubleshooting
- Performance optimization with smart defaults
"""

import os
import re
import json
import hashlib
import asyncio
import aiohttp
import aiofiles
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from urllib.parse import urlparse, unquote
import mimetypes
import platform
import time

from ..config.settings import get_config
from ..utils.logger import get_logger, log_execution_time
from ..utils.progress import ProgressTracker


class DownloadError(Exception):
    """Custom exception for download-related errors."""
    pass


class ConfigurationAccessMixin:
    """
    Mixin to provide bulletproof configuration access.

    This mixin provides multiple ways to access configuration values
    with comprehensive fallback mechanisms.
    """

    def safe_config_get(self, key_path: str, default: Any = None, expected_type: type = None) -> Any:
        """
        Safely get configuration value with multiple access methods.

        Args:
            key_path: Configuration key path (e.g., 'download_settings.max_retries')
            default: Default value if not found
            expected_type: Expected type for validation

        Returns:
            Configuration value or default
        """
        if not hasattr(self, 'config'):
            return default

        try:
            # Method 1: Try attribute access (new system)
            keys = key_path.split('.')
            if len(keys) == 2:
                section, key = keys
                try:
                    section_obj = getattr(self.config, section)
                    value = getattr(section_obj, key)
                    if value is not None:
                        if expected_type and not isinstance(value, expected_type):
                            value = expected_type(value)
                        return value
                except (AttributeError, ValueError, TypeError):
                    pass

            # Method 2: Try safe_get method
            try:
                value = self.config.safe_get(key_path, default, expected_type)
                if value != default:
                    return value
            except Exception:
                pass

            # Method 3: Try regular get method
            try:
                value = self.config.get(key_path, default)
                if value is not None and expected_type:
                    value = expected_type(value)
                return value
            except Exception:
                pass

            # Method 4: Return default
            return default

        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.debug(f"Config access failed for {key_path}, using default", exception=e)
            return default


class BaseDownloader(ABC, ConfigurationAccessMixin):
    """
    Enhanced Abstract Base Downloader Class with Bulletproof Configuration

    This class provides a robust foundation for all content-specific downloaders
    with comprehensive error handling, multiple configuration access patterns,
    and intelligent fallback mechanisms.

    The Enhanced Base Downloader is designed to NEVER crash due to configuration
    issues or missing settings. Instead, it provides graceful fallbacks and
    detailed logging for troubleshooting.
    """

    def __init__(self, canvas_client, progress_tracker: ProgressTracker = None):
        """
        Initialize the enhanced base downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        self.canvas_client = canvas_client
        self.progress_tracker = progress_tracker
        self.config = get_config()
        self.logger = get_logger(__name__)

        # Initialize download statistics
        self.stats = {
            'total_items': 0,
            'downloaded_items': 0,
            'skipped_items': 0,
            'failed_items': 0,
            'total_size_bytes': 0,
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'errors': [],
            'warnings': []
        }

        # Initialize download configuration with bulletproof access
        self._initialize_download_config()

        # File management
        self.course_folder = None
        self.metadata_file = None
        self.processed_items = set()

        # HTTP session for downloads
        self._session = None

        self.logger.info(f"Initialized {self.__class__.__name__}",
                        max_retries=self.max_retries,
                        chunk_size=self.chunk_size,
                        verify_downloads=self.verify_downloads,
                        timeout=self.timeout)

    def _initialize_download_config(self) -> None:
        """Initialize download configuration with bulletproof access and safe defaults."""
        try:
            # Download settings with multiple access methods and safe defaults
            self.max_retries = self.safe_config_get('download_settings.max_retries', 3, int)
            self.retry_delay = self.safe_config_get('download_settings.retry_delay', 1.0, float)
            self.chunk_size = self.safe_config_get('download_settings.chunk_size', 8192, int)
            self.timeout = self.safe_config_get('download_settings.timeout', 30, int)
            self.verify_downloads = self.safe_config_get('download_settings.verify_downloads', True, bool)
            self.skip_existing = self.safe_config_get('download_settings.skip_existing', True, bool)
            self.parallel_downloads = self.safe_config_get('download_settings.parallel_downloads', 4, int)
            self.max_file_size_mb = self.safe_config_get('download_settings.max_file_size_mb', 500, int)

            # File type restrictions
            self.allowed_extensions = self.safe_config_get('download_settings.allowed_extensions', [], list)
            self.blocked_extensions = self.safe_config_get('download_settings.blocked_extensions',
                                                         ['.exe', '.bat', '.cmd', '.scr'], list)

            # Validate settings
            self._validate_download_settings()

            self.logger.debug("Download configuration initialized successfully",
                            max_retries=self.max_retries,
                            chunk_size=self.chunk_size,
                            timeout=self.timeout)

        except Exception as e:
            self.logger.error("Failed to initialize download config, using safe defaults", exception=e)
            self._set_safe_defaults()

    def _set_safe_defaults(self) -> None:
        """Set safe default values for all configuration options."""
        self.max_retries = 3
        self.retry_delay = 1.0
        self.chunk_size = 8192
        self.timeout = 30
        self.verify_downloads = True
        self.skip_existing = True
        self.parallel_downloads = 4
        self.max_file_size_mb = 500
        self.allowed_extensions = []
        self.blocked_extensions = ['.exe', '.bat', '.cmd', '.scr']

        self.logger.warning("Using safe default configuration values")

    def _validate_download_settings(self) -> None:
        """Validate and fix download settings."""
        # Ensure positive values
        if self.max_retries < 0:
            self.max_retries = 3
            self.logger.warning("Fixed negative max_retries value")

        if self.chunk_size <= 0:
            self.chunk_size = 8192
            self.logger.warning("Fixed invalid chunk_size value")

        if self.timeout <= 0:
            self.timeout = 30
            self.logger.warning("Fixed invalid timeout value")

        if self.parallel_downloads <= 0:
            self.parallel_downloads = 1
            self.logger.warning("Fixed invalid parallel_downloads value")
        elif self.parallel_downloads > 20:
            self.parallel_downloads = 20
            self.logger.warning("Limited parallel_downloads to maximum of 20")

        if self.max_file_size_mb <= 0:
            self.max_file_size_mb = 500
            self.logger.warning("Fixed invalid max_file_size_mb value")

        # Ensure lists are actually lists
        if not isinstance(self.allowed_extensions, list):
            self.allowed_extensions = []
            self.logger.warning("Fixed invalid allowed_extensions type")

        if not isinstance(self.blocked_extensions, list):
            self.blocked_extensions = ['.exe', '.bat', '.cmd', '.scr']
            self.logger.warning("Fixed invalid blocked_extensions type")

    @abstractmethod
    def get_content_type_name(self) -> str:
        """
        Get the content type name for this downloader.

        Returns:
            str: The content type identifier (e.g., 'assignments', 'modules')
        """
        pass

    @abstractmethod
    def fetch_content_list(self, course) -> List[Any]:
        """
        Fetch the list of content items from the course.

        Args:
            course: Canvas course object

        Returns:
            List[Any]: List of content items to process
        """
        pass

    @abstractmethod
    def extract_metadata(self, item: Any) -> Dict[str, Any]:
        """
        Extract metadata from a content item.

        Args:
            item: Canvas content item object

        Returns:
            Dict[str, Any]: Metadata dictionary
        """
        pass

    @abstractmethod
    async def process_content_item(self, item: Any, course_folder: Path,
                                 metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single content item and download associated files.

        Args:
            item: Canvas content item object
            course_folder: Base folder for the course
            metadata: Pre-extracted item metadata

        Returns:
            Optional[Dict[str, Any]]: Download information or None if skipped
        """
        pass

    def setup_course_folder(self, course_info: Dict[str, Any]) -> Path:
        """
        Set up and create the course folder structure.

        Args:
            course_info: Dictionary containing course information

        Returns:
            Path: The course folder path
        """
        try:
            # Get base download directory with multiple access methods
            downloads_base = None

            # Try different ways to get downloads folder
            try:
                downloads_base = self.config.paths.downloads_folder
            except AttributeError:
                downloads_base = self.safe_config_get('paths.downloads_folder', 'downloads', str)

            downloads_base = Path(downloads_base)

            # Create course folder based on organization preferences
            course_folder = self._build_course_path(downloads_base, course_info)

            # Create the folder structure
            course_folder.mkdir(parents=True, exist_ok=True)

            # Create content type subfolder
            content_folder = course_folder / self.get_content_type_name()
            content_folder.mkdir(exist_ok=True)

            # Set up metadata file path
            self.metadata_file = content_folder / f"{self.get_content_type_name()}_metadata.json"

            # Store for later use
            self.course_folder = content_folder

            self.logger.info(f"Course folder setup complete",
                           course_folder=str(course_folder),
                           content_folder=str(content_folder))

            return content_folder

        except Exception as e:
            self.logger.error(f"Failed to setup course folder", exception=e)
            # Emergency fallback
            fallback_path = Path('downloads') / 'emergency_fallback' / self.get_content_type_name()
            fallback_path.mkdir(parents=True, exist_ok=True)
            self.course_folder = fallback_path
            return fallback_path

    def _build_course_path(self, base_path: Path, course_info: Dict[str, Any]) -> Path:
        """
        Build the course folder path based on configuration.

        Args:
            base_path: Base downloads directory
            course_info: Course information

        Returns:
            Path: Course folder path
        """
        try:
            # Get folder organization preferences
            organize_by_semester = self.safe_config_get('folder_structure.organize_by_semester', True, bool)
            folder_template = self.safe_config_get('folder_structure.folder_name_template',
                                                 '{course_code}-{course_name}', str)

            path_parts = []

            # Add semester organization if enabled
            if organize_by_semester:
                current_year = datetime.now().year
                semester = self._determine_semester(course_info)
                path_parts.extend([str(current_year), semester])

            # Build course folder name
            course_name = self._build_course_folder_name(course_info, folder_template)
            path_parts.append(course_name)

            # Construct full path
            course_path = base_path
            for part in path_parts:
                course_path = course_path / self._sanitize_folder_name(part)

            return course_path

        except Exception as e:
            self.logger.error(f"Failed to build course path", exception=e)
            # Simple fallback
            safe_name = self._sanitize_folder_name(course_info.get('name', 'Unknown_Course'))
            return base_path / safe_name

    def _determine_semester(self, course_info: Dict[str, Any]) -> str:
        """Determine semester from course information."""
        course_name = course_info.get('name', '').lower()

        if any(term in course_name for term in ['fall', 'autumn']):
            return 'Fall'
        elif 'spring' in course_name:
            return 'Spring'
        elif 'summer' in course_name:
            return 'Summer'
        elif 'winter' in course_name:
            return 'Winter'
        else:
            # Determine by current date
            month = datetime.now().month
            if month in [8, 9, 10, 11, 12]:
                return 'Fall'
            elif month in [1, 2, 3, 4, 5]:
                return 'Spring'
            else:
                return 'Summer'

    def _build_course_folder_name(self, course_info: Dict[str, Any], template: str) -> str:
        """Build course folder name from template."""
        try:
            course_code = course_info.get('course_code', 'UNKNOWN')
            course_name = course_info.get('name', 'Unknown Course')

            # Clean up course name (remove code if already present)
            if course_code and course_code in course_name:
                course_name = course_name.replace(course_code, '').strip(' -_')

            folder_name = template.format(
                course_code=course_code,
                course_name=course_name,
                course_id=course_info.get('id', ''),
                full_name=course_info.get('name', '')
            )

            return folder_name

        except Exception as e:
            self.logger.warning(f"Failed to build folder name from template", exception=e)
            # Simple fallback
            return f"{course_info.get('course_code', 'UNKNOWN')}-{course_info.get('name', 'Unknown')}"

    def _sanitize_folder_name(self, name: str) -> str:
        """Sanitize folder name for filesystem compatibility."""
        # Remove invalid characters
        invalid_chars = '<>:"/\\|?*'
        sanitized = name

        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')

        # Remove leading/trailing dots and spaces
        sanitized = sanitized.strip('. ')

        # Limit length
        if len(sanitized) > 100:
            sanitized = sanitized[:97] + "..."

        # Ensure not empty
        if not sanitized:
            sanitized = "Unnamed"

        return sanitized

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all content for a course (maintained for backward compatibility).

        Args:
            course: Canvas course object
            course_info: Course information dictionary

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        self.stats['start_time'] = datetime.now()

        try:
            self.logger.info(f"Starting {self.get_content_type_name()} download for course",
                           course_name=course_info.get('name', 'Unknown'),
                           course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if content type is enabled
            if not self._is_content_type_enabled():
                self.logger.info(f"{self.get_content_type_name()} download is disabled")
                return self.stats

            # Fetch content items
            items = self.fetch_content_list(course)

            if not items:
                self.logger.info(f"No {self.get_content_type_name()} found in course")
                return self.stats

            self.stats['total_items'] = len(items)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(items))

            # Process each item
            items_metadata = []

            for index, item in enumerate(items, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(item)
                    metadata['index'] = index
                    metadata['content_type'] = self.get_content_type_name()

                    # Update progress
                    if self.progress_tracker:
                        item_name = metadata.get('title', f'Item {index}')
                        self.progress_tracker.update_current_item(item_name)

                    # Process the item
                    download_info = await self.process_content_item(item, course_folder, metadata)

                    if download_info:
                        metadata.update(download_info)
                        self.stats['downloaded_items'] += 1

                        # Add to file size tracking
                        if 'file_size' in download_info:
                            self.stats['total_size_bytes'] += download_info['file_size']
                    else:
                        self.stats['skipped_items'] += 1

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                except Exception as e:
                    self.logger.error(f"Failed to process {self.get_content_type_name()} item",
                                    exception=e,
                                    item_index=index)
                    self.stats['failed_items'] += 1

                    # Add error metadata
                    error_metadata = {
                        'item_number': index,
                        'error': str(e),
                        'processing_timestamp': datetime.now().isoformat(),
                        'failed': True
                    }
                    items_metadata.append(error_metadata)

            # Save metadata
            self.save_metadata(items_metadata)

            # Calculate final statistics
            self.stats['end_time'] = datetime.now()
            self.stats['duration_seconds'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

            self.logger.info(f"Download completed for {self.get_content_type_name()}",
                           **{k: v for k, v in self.stats.items() if k not in ['start_time', 'end_time']})

            return self.stats

        except Exception as e:
            self.logger.error(f"Failed to download {self.get_content_type_name()}",
                            exception=e,
                            course_id=str(course.id))
            raise DownloadError(f"Download failed for {self.get_content_type_name()}: {e}")

    def _is_content_type_enabled(self) -> bool:
        """Check if this content type is enabled for download."""
        try:
            return self.config.is_content_type_enabled(self.get_content_type_name())
        except Exception:
            # Safe fallback - assume enabled if we can't check
            return True

    def save_metadata(self, items_metadata: List[Dict[str, Any]]) -> None:
        """
        Save metadata file with error handling.

        Args:
            items_metadata: List of metadata dictionaries
        """
        if not self.metadata_file or not items_metadata:
            return

        try:
            metadata_content = {
                'content_type': self.get_content_type_name(),
                'download_date': datetime.now().isoformat(),
                'total_items': len(items_metadata),
                'items': items_metadata,
                'statistics': self.stats.copy()
            }

            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata_content, f, indent=2, ensure_ascii=False, default=str)

            self.logger.debug(f"Saved metadata file", file_path=str(self.metadata_file))

        except Exception as e:
            self.logger.error(f"Failed to save metadata", exception=e)

    def is_file_allowed(self, filename: str) -> bool:
        """
        Check if file is allowed based on extension filters.

        Args:
            filename: Name of the file to check

        Returns:
            bool: True if file is allowed
        """
        try:
            file_ext = Path(filename).suffix.lower()

            # Check blocked extensions
            if file_ext in self.blocked_extensions:
                self.logger.debug(f"File blocked by extension", filename=filename, extension=file_ext)
                return False

            # Check allowed extensions (if specified)
            if self.allowed_extensions and file_ext not in self.allowed_extensions:
                self.logger.debug(f"File not in allowed extensions", filename=filename, extension=file_ext)
                return False

            return True

        except Exception as e:
            self.logger.warning(f"Error checking file extension", filename=filename, exception=e)
            # Default to allowed if we can't check
            return True

    def is_file_size_allowed(self, size_bytes: int) -> bool:
        """
        Check if file size is within allowed limits.

        Args:
            size_bytes: File size in bytes

        Returns:
            bool: True if size is allowed
        """
        max_size_bytes = self.max_file_size_mb * 1024 * 1024

        if size_bytes > max_size_bytes:
            self.logger.debug(f"File too large",
                            size_mb=round(size_bytes / (1024 * 1024), 2),
                            max_mb=self.max_file_size_mb)
            return False

        return True

    def sanitize_filename(self, filename: str) -> str:
        """
        Sanitize a filename for the current filesystem (maintained for compatibility).

        Args:
            filename: Original filename

        Returns:
            str: Sanitized filename
        """
        # Remove or replace invalid characters
        invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        sanitized = filename

        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')

        # Remove leading/trailing spaces and dots
        sanitized = sanitized.strip(' .')

        # Ensure filename is not empty
        if not sanitized:
            sanitized = "unnamed_file"

        # Truncate if too long (keeping extension)
        if len(sanitized) > 255:
            name, ext = os.path.splitext(sanitized)
            max_name_len = 255 - len(ext)
            sanitized = name[:max_name_len] + ext

        return sanitized


class ContentDownloaderFactory:
    """
    Factory class for creating content downloaders.

    This factory provides a centralized way to create and manage
    different types of content downloaders with proper registration
    and initialization.
    """

    _downloaders = {}

    @classmethod
    def register_downloader(cls, content_type: str, downloader_class):
        """
        Register a downloader class for a specific content type.

        Args:
            content_type: Name of the content type
            downloader_class: Downloader class to register
        """
        cls._downloaders[content_type] = downloader_class

    @classmethod
    def create_downloader(cls, content_type: str, canvas_client,
                         progress_tracker: ProgressTracker = None) -> BaseDownloader:
        """
        Create a downloader instance for the specified content type.

        Args:
            content_type: Type of content downloader to create
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker

        Returns:
            BaseDownloader: Configured downloader instance

        Raises:
            ValueError: If content type is not registered
        """
        if content_type not in cls._downloaders:
            available = ', '.join(cls._downloaders.keys())
            raise ValueError(f"Unknown content type '{content_type}'. Available: {available}")

        downloader_class = cls._downloaders[content_type]
        return downloader_class(canvas_client, progress_tracker)

    @classmethod
    def get_available_content_types(cls) -> List[str]:
        """
        Get list of available content types.

        Returns:
            List[str]: List of registered content type names
        """
        return list(cls._downloaders.keys())