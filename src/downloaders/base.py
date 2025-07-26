"""
Base Downloader Module

This module provides the base class for all content downloaders in the Canvas
Downloader application. It defines the common interface and shared functionality
that all specific downloaders (assignments, announcements, etc.) will inherit.

Features:
- Abstract base class with standardized interface
- Common file operations and path management
- JSON metadata handling
- Progress tracking integration
- Error handling and retry logic
- Filename sanitization and validation
- Parallel download coordination

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
import json
import hashlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime
import asyncio
import aiohttp
import aiofiles
from urllib.parse import urlparse, unquote

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
        self.config = get_config()
        self.logger = get_logger(self.__class__.__name__)
        self.progress_tracker = progress_tracker

        # Current processing context
        self.course_name = ""
        self.course_folder = None
        self.content_folder = None

        # Download statistics
        self.stats = {
            'total_items': 0,
            'downloaded_items': 0,
            'skipped_items': 0,
            'failed_items': 0,
            'total_size_bytes': 0
        }

    @abstractmethod
    def get_content_type_name(self) -> str:
        """
        Get the name of the content type this downloader handles.

        Returns:
            str: Content type name (e.g., 'assignments', 'announcements')
        """
        pass

    @abstractmethod
    def fetch_content_list(self, course) -> List[Any]:
        """
        Fetch the list of content items from Canvas for the given course.

        Args:
            course: Canvas course object

        Returns:
            List[Any]: List of content items from Canvas API
        """
        pass

    @abstractmethod
    def extract_metadata(self, item: Any) -> Dict[str, Any]:
        """
        Extract metadata from a Canvas content item.

        Args:
            item: Canvas content item object

        Returns:
            Dict[str, Any]: Metadata dictionary for JSON storage
        """
        pass

    @abstractmethod
    def get_download_info(self, item: Any) -> Optional[Dict[str, str]]:
        """
        Get download information for a content item.

        Args:
            item: Canvas content item object

        Returns:
            Optional[Dict[str, str]]: Download info with 'url' and 'filename' keys,
                                    or None if no download is needed
        """
        pass

    def setup_course_folder(self, course_info: Dict[str, Any]) -> Path:
        """
        Set up the folder structure for a course.

        Creates the directory structure based on the configured pattern:
        {year}/{semester} Semester/{course_name}/

        Args:
            course_info: Dictionary containing parsed course information

        Returns:
            Path: Path to the course folder
        """
        try:
            # Get base download path
            base_path = self.config.get_download_path()

            # Parse course information
            year = course_info.get('year', 'Unknown_Year')
            semester = course_info.get('semester', 'Unknown_Semester')
            subject_name = course_info.get('subject_name', 'Unknown_Subject')
            subject_code = course_info.get('subject_code', '')
            subsection = course_info.get('subsection', '')

            # Create course name: "Subject (CODE) Subsection"
            course_name_parts = [subject_name]
            if subject_code:
                course_name_parts[0] += f" ({subject_code})"
            if subsection:
                course_name_parts.append(subsection)

            course_name = " ".join(course_name_parts)
            self.course_name = course_name

            # Build folder path: year/semester/course_name
            folder_path = base_path / year / f"{semester} Semester" / self.sanitize_filename(course_name)

            # Create course folder
            folder_path.mkdir(parents=True, exist_ok=True)
            self.course_folder = folder_path

            # Create content-specific subfolder
            content_folder = folder_path / self.get_content_type_name()
            content_folder.mkdir(exist_ok=True)
            self.content_folder = content_folder

            self.logger.info(
                f"Set up course folder: {folder_path}",
                course_name=course_name,
                content_type=self.get_content_type_name(),
                folder_path=str(folder_path)
            )

            return folder_path

        except Exception as e:
            self.logger.error(f"Failed to set up course folder", exception=e)
            raise DownloadError(f"Could not create course folder: {e}")

    def sanitize_filename(self, filename: str) -> str:
        """
        Sanitize a filename to be safe for the file system.

        Removes or replaces characters that are invalid in filenames
        and ensures the filename doesn't exceed system limits.

        Args:
            filename: Original filename

        Returns:
            str: Sanitized filename safe for file system use
        """
        if not self.config.file_naming.sanitize_filenames:
            return filename

        # Characters to remove or replace
        invalid_chars = {
            '<': '',
            '>': '',
            ':': '-',
            '"': "'",
            '/': '-',
            '\\': '-',
            '|': '-',
            '?': '',
            '*': '',
            '\0': '',
            '\r': '',
            '\n': ' ',
            '\t': ' '
        }

        # Replace invalid characters
        sanitized = filename
        for char, replacement in invalid_chars.items():
            sanitized = sanitized.replace(char, replacement)

        # Remove multiple spaces and trim
        sanitized = ' '.join(sanitized.split())
        sanitized = sanitized.strip(' .')

        # Ensure filename isn't empty
        if not sanitized:
            sanitized = "unnamed_file"

        # Respect maximum filename length
        max_length = self.config.file_naming.max_filename_length
        if len(sanitized) > max_length:
            # Try to preserve file extension
            if '.' in sanitized:
                name, ext = sanitized.rsplit('.', 1)
                max_name_length = max_length - len(ext) - 1
                if max_name_length > 0:
                    sanitized = name[:max_name_length] + '.' + ext
                else:
                    sanitized = sanitized[:max_length]
            else:
                sanitized = sanitized[:max_length]

        return sanitized

    def generate_filename(self, item_number: int, item_name: str,
                         file_extension: str = "") -> str:
        """
        Generate a filename based on the configured pattern.

        Args:
            item_number: Sequential number for the item
            item_name: Name of the content item
            file_extension: File extension (with or without dot)

        Returns:
            str: Generated filename
        """
        # Ensure extension starts with dot
        if file_extension and not file_extension.startswith('.'):
            file_extension = '.' + file_extension

        # Use configured naming pattern
        pattern = self.config.file_naming.pattern

        try:
            filename = pattern.format(
                type=self.get_content_type_name(),
                number=item_number,
                name=self.sanitize_filename(item_name)
            )

            # Add extension
            filename += file_extension

            return self.sanitize_filename(filename)

        except Exception as e:
            # Fallback to simple naming if pattern fails
            self.logger.warning(f"Filename pattern failed, using fallback: {e}")
            sanitized_name = self.sanitize_filename(item_name)
            return f"{self.get_content_type_name()}_{item_number:03d}_{sanitized_name}{file_extension}"

    def save_metadata(self, items_metadata: List[Dict[str, Any]]) -> Path:
        """
        Save metadata for all items to a JSON file.

        Args:
            items_metadata: List of metadata dictionaries

        Returns:
            Path: Path to the saved metadata file
        """
        try:
            metadata_filename = f"{self.get_content_type_name()}_metadata.json"
            metadata_path = self.content_folder / metadata_filename

            # Prepare metadata with additional information
            metadata_document = {
                'content_type': self.get_content_type_name(),
                'course_name': self.course_name,
                'download_date': datetime.now().isoformat(),
                'total_items': len(items_metadata),
                'items': items_metadata
            }

            # Save to JSON file with pretty formatting
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata_document, f, indent=2, ensure_ascii=False, default=str)

            self.logger.info(
                f"Saved metadata for {len(items_metadata)} items",
                content_type=self.get_content_type_name(),
                metadata_file=str(metadata_path),
                item_count=len(items_metadata)
            )

            return metadata_path

        except Exception as e:
            self.logger.error(f"Failed to save metadata", exception=e)
            raise DownloadError(f"Could not save metadata: {e}")

    def file_exists_and_valid(self, file_path: Path, expected_size: int = None) -> bool:
        """
        Check if a file exists and optionally validate its size.

        Args:
            file_path: Path to the file to check
            expected_size: Optional expected file size in bytes

        Returns:
            bool: True if file exists and is valid
        """
        if not file_path.exists():
            return False

        if not file_path.is_file():
            return False

        # Check file size if provided
        if expected_size is not None:
            actual_size = file_path.stat().st_size
            if actual_size != expected_size:
                self.logger.warning(
                    f"File size mismatch: {file_path}",
                    expected_size=expected_size,
                    actual_size=actual_size
                )
                return False

        return True

    async def download_file(self, url: str, file_path: Path,
                          expected_size: int = None, session: aiohttp.ClientSession = None) -> bool:
        """
        Download a file from the given URL to the specified path.

        Args:
            url: URL to download from
            file_path: Local path to save the file
            expected_size: Optional expected file size for validation
            session: Optional aiohttp session for connection reuse

        Returns:
            bool: True if download was successful
        """
        try:
            # Skip if file exists and skip_existing is enabled
            if (self.config.download_settings.skip_existing and
                self.file_exists_and_valid(file_path, expected_size)):

                self.logger.log_download_skip(
                    self.get_content_type_name(),
                    file_path.name,
                    "File already exists"
                )
                self.stats['skipped_items'] += 1
                return True

            # Create directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Use provided session or create a new one
            close_session = False
            if session is None:
                timeout = aiohttp.ClientTimeout(total=self.config.download_settings.timeout)
                session = aiohttp.ClientSession(timeout=timeout)
                close_session = True

            try:
                start_time = asyncio.get_event_loop().time()

                async with session.get(url) as response:
                    response.raise_for_status()

                    # Get file size from headers
                    content_length = response.headers.get('Content-Length')
                    file_size = int(content_length) if content_length else None

                    # Download file in chunks
                    async with aiofiles.open(file_path, 'wb') as f:
                        downloaded = 0
                        chunk_size = self.config.download_settings.chunk_size

                        async for chunk in response.content.iter_chunked(chunk_size):
                            await f.write(chunk)
                            downloaded += len(chunk)

                            # Update progress if tracker is available
                            if self.progress_tracker:
                                self.progress_tracker.update_download_progress(
                                    downloaded, file_size
                                )

                end_time = asyncio.get_event_loop().time()
                duration = end_time - start_time

                # Validate downloaded file
                actual_size = file_path.stat().st_size
                if expected_size and actual_size != expected_size:
                    self.logger.warning(
                        f"Downloaded file size mismatch",
                        expected_size=expected_size,
                        actual_size=actual_size,
                        file_path=str(file_path)
                    )

                # Log successful download
                self.logger.log_download_complete(
                    self.get_content_type_name(),
                    file_path.name,
                    str(file_path),
                    actual_size,
                    duration
                )

                self.stats['downloaded_items'] += 1
                self.stats['total_size_bytes'] += actual_size

                return True

            finally:
                if close_session:
                    await session.close()

        except Exception as e:
            self.logger.error(
                f"Failed to download file: {url}",
                exception=e,
                file_path=str(file_path),
                url=url
            )

            # Clean up partial download
            if file_path.exists():
                try:
                    file_path.unlink()
                except:
                    pass

            self.stats['failed_items'] += 1
            return False

    def extract_file_extension_from_url(self, url: str) -> str:
        """
        Extract file extension from a URL.

        Args:
            url: URL to extract extension from

        Returns:
            str: File extension (without dot) or empty string
        """
        try:
            parsed_url = urlparse(url)
            path = unquote(parsed_url.path)

            if '.' in path:
                extension = path.split('.')[-1].lower()
                # Validate extension (simple check for common file extensions)
                valid_extensions = {
                    'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',
                    'txt', 'rtf', 'zip', 'rar', '7z', 'tar', 'gz',
                    'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg',
                    'mp3', 'mp4', 'avi', 'mov', 'wav', 'wmv',
                    'html', 'htm', 'css', 'js', 'json', 'xml',
                    'py', 'java', 'cpp', 'c', 'h', 'cs', 'php'
                }

                if extension in valid_extensions:
                    return extension

            return ""

        except Exception:
            return ""

    def calculate_content_hash(self, content: str) -> str:
        """
        Calculate SHA-256 hash of content for change detection.

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
        2. Fetch content list from Canvas
        3. Process each content item
        4. Save metadata
        5. Return statistics

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.log_course_processing(
                course_info.get('full_name', 'Unknown Course'),
                str(course.id),
                'start'
            )

            # Reset statistics
            self.stats = {
                'total_items': 0,
                'downloaded_items': 0,
                'skipped_items': 0,
                'failed_items': 0,
                'total_size_bytes': 0
            }

            # Set up folder structure
            course_folder = self.setup_course_folder(course_info)

            # Check if content type is enabled
            if not self.config.is_content_type_enabled(self.get_content_type_name()):
                self.logger.info(f"Content type {self.get_content_type_name()} is disabled")
                return self.stats

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

            # Create aiohttp session for downloads
            timeout = aiohttp.ClientTimeout(total=self.config.download_settings.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:

                for index, item in enumerate(content_items, 1):
                    try:
                        # Extract metadata
                        metadata = self.extract_metadata(item)
                        metadata['item_number'] = index
                        items_metadata.append(metadata)

                        # Get download information
                        download_info = self.get_download_info(item)

                        if download_info:
                            # Generate filename
                            extension = self.extract_file_extension_from_url(download_info['url'])
                            filename = self.generate_filename(
                                index,
                                download_info['filename'],
                                extension
                            )

                            file_path = self.content_folder / filename

                            # Download the file
                            success = await self.download_file(
                                download_info['url'],
                                file_path,
                                session=session
                            )

                            # Update metadata with download result
                            metadata['downloaded'] = success
                            metadata['local_filename'] = filename if success else None
                            metadata['local_path'] = str(file_path) if success else None
                        else:
                            # No file to download (metadata only)
                            metadata['downloaded'] = False
                            metadata['local_filename'] = None
                            metadata['local_path'] = None

                        # Update progress
                        if self.progress_tracker:
                            self.progress_tracker.update_item_progress(index)

                    except Exception as e:
                        self.logger.error(
                            f"Failed to process {self.get_content_type_name()} item",
                            exception=e,
                            item_index=index
                        )
                        self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            # Log completion
            self.logger.log_course_processing(
                course_info.get('full_name', 'Unknown Course'),
                str(course.id),
                'complete'
            )

            self.logger.info(
                f"Download completed for {self.get_content_type_name()}",
                **self.stats
            )

            return self.stats

        except Exception as e:
            self.logger.error(
                f"Failed to download {self.get_content_type_name()}",
                exception=e,
                course_id=str(course.id)
            )

            self.logger.log_course_processing(
                course_info.get('full_name', 'Unknown Course'),
                str(course.id),
                'error'
            )

            raise DownloadError(f"Download failed for {self.get_content_type_name()}: {e}")


class ContentDownloaderFactory:
    """
    Factory class for creating content downloaders.

    This factory provides a centralized way to create and manage
    different types of content downloaders.
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
            content_type: Name of the content type
            canvas_client: Canvas API client
            progress_tracker: Optional progress tracker

        Returns:
            BaseDownloader: Configured downloader instance

        Raises:
            ValueError: If content type is not registered
        """
        if content_type not in cls._downloaders:
            raise ValueError(f"Unknown content type: {content_type}")

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