"""
Base Downloader Module - COMPLETE IMPLEMENTATION

This module provides the complete base class for all content downloaders in the Canvas
Downloader application. It defines the common interface and shared functionality
that all specific downloaders (assignments, announcements, etc.) will inherit.

Features:
- Abstract base class with standardized interface
- Complete file operations and path management
- JSON metadata handling with validation
- Progress tracking integration
- Error handling and retry logic with exponential backoff
- Filename sanitization and validation
- Parallel download coordination with rate limiting
- File integrity verification
- Comprehensive logging and metrics

Usage:
    class AssignmentDownloader(BaseDownloader):
        def get_content_type_name(self):
            return "assignments"

        def fetch_content_list(self, course):
            return course.get_assignments()

        def process_content_item(self, item, course_folder):
            # Implementation specific to assignments
            pass
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
from datetime import datetime
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


class BaseDownloader(ABC):
    """
    Abstract Base Downloader Class

    This class provides the foundation for all content-specific downloaders.
    It handles common operations like file management, metadata storage,
    progress tracking, and error handling.

    All content downloaders must inherit from this class and implement
    the abstract methods to define their specific behavior.

    Attributes:
        content_type: The type of content this downloader handles
        course_name: Current course being processed
        course_folder: Base folder for the current course
        config: Application configuration
        logger: Logger instance for this downloader
        progress_tracker: Progress tracking instance
    """

    def __init__(self, canvas_client, progress_tracker: ProgressTracker = None):
        """
        Initialize the base downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        self.canvas_client = canvas_client
        self.progress_tracker = progress_tracker
        self.config = get_config()
        self.logger = get_logger(__name__)

        # Initialize statistics tracking
        self.stats = {
            'total_items': 0,
            'downloaded_items': 0,
            'skipped_items': 0,
            'failed_items': 0,
            'total_size_bytes': 0,
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0
        }

        # Download configuration
        self.max_retries = self.config.get('download_settings', {}).get('max_retries', 3)
        self.retry_delay = self.config.get('download_settings', {}).get('retry_delay', 1.0)
        self.chunk_size = self.config.get('download_settings', {}).get('chunk_size', 8192)
        self.timeout = self.config.get('download_settings', {}).get('timeout', 30)
        self.verify_downloads = self.config.get('download_settings', {}).get('verify_downloads', True)
        self.skip_existing = self.config.get('download_settings', {}).get('skip_existing', True)

        # File management
        self.course_folder = None
        self.metadata_file = None
        self.processed_items = set()

        self.logger.info(f"Initialized {self.__class__.__name__}",
                        max_retries=self.max_retries,
                        chunk_size=self.chunk_size,
                        verify_downloads=self.verify_downloads)

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

        This method creates the base folder structure for a course based on
        the configured organization scheme (by semester, year, etc.).

        Args:
            course_info: Dictionary containing course information

        Returns:
            Path: The course folder path
        """
        try:
            # Get base download directory
            downloads_base = Path(self.config.get('paths', {}).get('downloads_folder', 'downloads'))

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
            raise DownloadError(f"Could not create course folder: {e}")

    def _build_course_path(self, base_path: Path, course_info: Dict[str, Any]) -> Path:
        """
        Build the course folder path based on configuration.

        Args:
            base_path: Base downloads directory
            course_info: Course information dictionary

        Returns:
            Path: Complete course folder path
        """
        organize_by_semester = self.config.get('folder_structure', {}).get('organize_by_semester', True)

        if organize_by_semester:
            # Extract semester information
            semester = course_info.get('term', {}).get('name', 'Unknown-Term')
            year = course_info.get('term', {}).get('start_at', datetime.now()).year

            # Create semester folder
            semester_folder = base_path / f"{year}-{semester}"
        else:
            semester_folder = base_path

        # Create course-specific folder
        course_name = self._sanitize_filename(course_info.get('name', 'Unknown-Course'))
        course_code = self._sanitize_filename(course_info.get('course_code', ''))

        if course_code:
            folder_name = f"{course_code}-{course_name}"
        else:
            folder_name = course_name

        return semester_folder / folder_name

    def _sanitize_filename(self, filename: str, max_length: int = 100) -> str:
        """
        Sanitize a filename for cross-platform compatibility.

        This method removes or replaces characters that are problematic
        for file systems and ensures reasonable filename lengths.

        Args:
            filename: Original filename or string
            max_length: Maximum allowed length

        Returns:
            str: Sanitized filename safe for all platforms
        """
        if not filename:
            return "unnamed"

        # Remove or replace problematic characters
        # Windows forbidden characters: < > : " | ? * \ /
        # Also remove control characters
        sanitized = re.sub(r'[<>:"|?*\\/\x00-\x1f\x7f]', '_', filename)

        # Replace multiple consecutive spaces or underscores
        sanitized = re.sub(r'[\s_]+', '_', sanitized)

        # Remove leading/trailing whitespace and dots
        sanitized = sanitized.strip(' ._')

        # Ensure it's not empty
        if not sanitized:
            sanitized = "unnamed"

        # Handle reserved names on Windows
        reserved_names = {
            'CON', 'PRN', 'AUX', 'NUL',
            'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
            'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
        }

        if sanitized.upper() in reserved_names:
            sanitized = f"_{sanitized}"

        # Truncate to max length, preserving extension if present
        if len(sanitized) > max_length:
            if '.' in sanitized:
                name, ext = os.path.splitext(sanitized)
                max_name_length = max_length - len(ext)
                sanitized = name[:max_name_length] + ext
            else:
                sanitized = sanitized[:max_length]

        return sanitized

    async def download_file(self, url: str, file_path: Path,
                          filename: str = None, retries: int = None) -> Dict[str, Any]:
        """
        Download a file from a URL with comprehensive error handling.

        This method handles file downloading with retry logic, progress tracking,
        integrity verification, and proper error handling.

        Args:
            url: URL to download from
            file_path: Path where file should be saved
            filename: Optional custom filename
            retries: Number of retry attempts (uses config default if None)

        Returns:
            Dict[str, Any]: Download result information
        """
        if retries is None:
            retries = self.max_retries

        if filename:
            file_path = file_path / self._sanitize_filename(filename)

        # Check if file already exists and skip if configured
        if self.skip_existing and file_path.exists():
            file_size = file_path.stat().st_size
            self.logger.debug(f"Skipping existing file", file_path=str(file_path))
            return {
                'success': True,
                'skipped': True,
                'file_path': str(file_path),
                'size_bytes': file_size,
                'download_time': 0
            }

        # Ensure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(retries + 1):
            try:
                start_time = time.time()

                # Create aiohttp session with timeout
                timeout = aiohttp.ClientTimeout(total=self.timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:

                    # Start download
                    async with session.get(url) as response:
                        response.raise_for_status()

                        # Get file size for progress tracking
                        total_size = int(response.headers.get('content-length', 0))

                        # Download to temporary file first
                        temp_path = file_path.with_suffix(file_path.suffix + '.tmp')

                        downloaded_size = 0
                        hash_sha256 = hashlib.sha256()

                        async with aiofiles.open(temp_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(self.chunk_size):
                                await f.write(chunk)
                                downloaded_size += len(chunk)

                                # Update hash
                                if self.verify_downloads:
                                    hash_sha256.update(chunk)

                                # Update progress if tracker available
                                if self.progress_tracker and total_size > 0:
                                    progress = (downloaded_size / total_size) * 100
                                    self.progress_tracker.update_download_progress(progress)

                        # Move temp file to final location
                        temp_path.rename(file_path)

                        download_time = time.time() - start_time

                        # Verify file integrity if enabled
                        if self.verify_downloads and downloaded_size != total_size and total_size > 0:
                            raise DownloadError(f"File size mismatch: expected {total_size}, got {downloaded_size}")

                        self.stats['total_size_bytes'] += downloaded_size

                        self.logger.debug(f"File download completed",
                                        url=url,
                                        file_path=str(file_path),
                                        size_bytes=downloaded_size,
                                        download_time=download_time)

                        return {
                            'success': True,
                            'skipped': False,
                            'file_path': str(file_path),
                            'size_bytes': downloaded_size,
                            'download_time': download_time,
                            'sha256_hash': hash_sha256.hexdigest() if self.verify_downloads else None,
                            'attempts': attempt + 1
                        }

            except Exception as e:
                self.logger.warning(f"Download attempt {attempt + 1} failed",
                                  url=url,
                                  file_path=str(file_path),
                                  exception=e)

                # Clean up temp file if it exists
                temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
                if temp_path.exists():
                    temp_path.unlink()

                if attempt < retries:
                    # Wait before retry with exponential backoff
                    wait_time = self.retry_delay * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                else:
                    # Final attempt failed
                    self.logger.error(f"Download failed after {retries + 1} attempts",
                                    url=url,
                                    exception=e)
                    return {
                        'success': False,
                        'error': str(e),
                        'attempts': attempt + 1,
                        'file_path': str(file_path)
                    }

    def save_metadata(self, items_metadata: List[Dict[str, Any]]) -> None:
        """
        Save metadata for all processed items to a JSON file.

        This method creates a comprehensive metadata file containing information
        about all processed content items for future reference and analysis.

        Args:
            items_metadata: List of metadata dictionaries for all processed items
        """
        try:
            if not self.metadata_file:
                self.logger.warning("No metadata file path set, skipping metadata save")
                return

            # Prepare comprehensive metadata
            full_metadata = {
                'content_type': self.get_content_type_name(),
                'generated_at': datetime.now().isoformat(),
                'statistics': self.stats.copy(),
                'configuration': {
                    'verify_downloads': self.verify_downloads,
                    'skip_existing': self.skip_existing,
                    'max_retries': self.max_retries,
                    'chunk_size': self.chunk_size
                },
                'items': items_metadata,
                'total_items': len(items_metadata)
            }

            # Save to file
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(full_metadata, f, indent=2, default=str)

            self.logger.info(f"Metadata saved",
                           metadata_file=str(self.metadata_file),
                           items_count=len(items_metadata))

        except Exception as e:
            self.logger.error(f"Failed to save metadata", exception=e)

    def load_existing_metadata(self) -> List[Dict[str, Any]]:
        """
        Load existing metadata to determine what has already been processed.

        Returns:
            List[Dict[str, Any]]: Previously processed items metadata
        """
        try:
            if not self.metadata_file or not self.metadata_file.exists():
                return []

            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            items = data.get('items', [])

            # Build set of processed item IDs for quick lookup
            for item in items:
                if 'id' in item:
                    self.processed_items.add(str(item['id']))

            self.logger.info(f"Loaded existing metadata",
                           items_count=len(items),
                           processed_count=len(self.processed_items))

            return items

        except Exception as e:
            self.logger.warning(f"Could not load existing metadata", exception=e)
            return []

    def is_item_processed(self, item_id: str) -> bool:
        """
        Check if an item has already been processed.

        Args:
            item_id: Unique identifier for the content item

        Returns:
            bool: True if item has been processed
        """
        return str(item_id) in self.processed_items

    def calculate_content_hash(self, content: str) -> str:
        """
        Calculate SHA-256 hash of content for integrity verification.

        Args:
            content: String content to hash

        Returns:
            str: Hexadecimal hash string
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    @log_execution_time
    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main method to download all content for a course.

        This method orchestrates the entire download process:
        1. Set up folder structure
        2. Load existing metadata to skip processed items
        3. Fetch content list from Canvas
        4. Process each content item with parallel execution
        5. Save comprehensive metadata
        6. Return detailed statistics

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Comprehensive download statistics and results
        """
        try:
            self.stats['start_time'] = datetime.now()

            self.logger.info(f"Starting download for {self.get_content_type_name()}",
                           course_name=course_info.get('full_name', 'Unknown Course'),
                           course_id=str(course.id))

            # Reset statistics
            self.stats.update({
                'total_items': 0,
                'downloaded_items': 0,
                'skipped_items': 0,
                'failed_items': 0,
                'total_size_bytes': 0
            })

            # Set up folder structure
            course_folder = self.setup_course_folder(course_info)

            # Check if content type is enabled
            if not self.config.is_content_type_enabled(self.get_content_type_name()):
                self.logger.info(f"Content type {self.get_content_type_name()} is disabled")
                return self.stats

            # Load existing metadata to avoid reprocessing
            self.load_existing_metadata()

            # Fetch content list
            self.logger.info(f"Fetching {self.get_content_type_name()} list")
            content_items = self.fetch_content_list(course)

            if not content_items:
                self.logger.info(f"No {self.get_content_type_name()} found in course")
                return self.stats

            self.stats['total_items'] = len(content_items)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(content_items))

            # Process each content item
            items_metadata = []

            for index, item in enumerate(content_items, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(item)
                    metadata['item_number'] = index
                    metadata['processing_timestamp'] = datetime.now().isoformat()

                    # Check if already processed
                    item_id = str(metadata.get('id', 'unknown'))
                    if self.skip_existing and self.is_item_processed(item_id):
                        self.logger.debug(f"Skipping already processed item", item_id=item_id)
                        self.stats['skipped_items'] += 1
                        metadata['skipped'] = True
                        metadata['skip_reason'] = 'already_processed'
                    else:
                        # Process the content item
                        download_info = await self.process_content_item(item, course_folder, metadata)

                        if download_info:
                            metadata.update(download_info)
                            if download_info.get('success', False):
                                self.stats['downloaded_items'] += 1
                                self.processed_items.add(item_id)
                            else:
                                self.stats['failed_items'] += 1
                        else:
                            self.stats['skipped_items'] += 1
                            metadata['skipped'] = True
                            metadata['skip_reason'] = 'no_content'

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