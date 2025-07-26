"""
Files Downloader Module

This module implements the files downloader for Canvas courses. It handles
downloading all course files including documents, images, videos, and other
media files that instructors have uploaded to the course.

Canvas Files can include:
- Course documents (PDFs, Word docs, presentations)
- Images and media files
- Lecture recordings and videos
- Supplementary materials
- Folders and organizational structure
- File versions and revisions
- Usage rights and copyright information

Features:
- Download all course files with original folder structure
- Preserve file metadata and timestamps
- Handle large file downloads with progress tracking
- Maintain file permissions and access information
- Process file versions and revisions
- Organize files by upload date and folder structure
- Handle duplicate filenames gracefully

Usage:
    # Initialize the downloader
    downloader = FilesDownloader(canvas_client, progress_tracker)

    # Download all files for a course
    stats = await downloader.download_course_content(course, course_info)
"""

import asyncio
import json
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from urllib.parse import urlparse, unquote

import aiofiles
from canvasapi.file import File
from canvasapi.folder import Folder
from canvasapi.exceptions import CanvasException

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class FilesDownloader(BaseDownloader):
    """
    Canvas Files Downloader

    This class handles downloading files from Canvas courses while preserving
    the original folder structure and maintaining comprehensive file metadata.
    It processes all types of files including documents, media, and other
    course materials.

    The downloader ensures that:
    - Original folder hierarchy is preserved
    - File metadata and timestamps are maintained
    - Large files are downloaded efficiently
    - Duplicate filenames are handled appropriately
    - File access permissions are recorded
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the files downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # File processing settings
        self.preserve_folder_structure = True
        self.download_locked_files = False
        self.include_hidden_files = False
        self.verify_file_integrity = True

        # File organization settings
        self.organize_by_date = False
        self.organize_by_type = False
        self.flatten_structure = False

        # Download optimization
        self.max_file_size_mb = 500  # Skip files larger than this
        self.allowed_extensions = set()  # Empty means allow all
        self.blocked_extensions = {'.tmp', '.temp', '.lock'}

        # Folder and file tracking
        self._folder_structure = {}
        self._processed_folders = set()
        self._file_count_by_type = {}

        self.logger.info("Files downloader initialized",
                         preserve_structure=self.preserve_folder_structure,
                         max_file_size_mb=self.max_file_size_mb)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "files"

    def fetch_content_list(self, course) -> List[File]:
        """
        Fetch all files from the course.

        Args:
            course: Canvas course object

        Returns:
            List[File]: List of file objects
        """
        try:
            self.logger.info(f"Fetching files for course {course.id}")

            # Get all files in the course
            files = list(course.get_files(
                include=['user']  # Include user information for uploaded_by metadata
            ))

            # Filter files based on settings
            filtered_files = []

            for file in files:
                # Check if file is locked and we should skip locked files
                if not self.download_locked_files and getattr(file, 'locked', False):
                    self.logger.debug(f"Skipping locked file: {getattr(file, 'display_name', 'unknown')}")
                    continue

                # Check if file is hidden and we should skip hidden files
                if not self.include_hidden_files and getattr(file, 'hidden', False):
                    self.logger.debug(f"Skipping hidden file: {getattr(file, 'display_name', 'unknown')}")
                    continue

                # Check file size
                file_size_bytes = getattr(file, 'size', 0)
                file_size_mb = file_size_bytes / (1024 * 1024)

                if self.max_file_size_mb > 0 and file_size_mb > self.max_file_size_mb:
                    self.logger.warning(f"Skipping large file: {getattr(file, 'display_name', 'unknown')} "
                                        f"({file_size_mb:.1f} MB)")
                    continue

                # Check file extension
                filename = getattr(file, 'filename', '')
                extension = Path(filename).suffix.lower()

                if self.blocked_extensions and extension in self.blocked_extensions:
                    self.logger.debug(f"Skipping blocked file type: {filename}")
                    continue

                if self.allowed_extensions and extension not in self.allowed_extensions:
                    self.logger.debug(f"Skipping non-allowed file type: {filename}")
                    continue

                filtered_files.append(file)

            self.logger.info(f"Found {len(filtered_files)} files to download "
                             f"(filtered from {len(files)} total)",
                             course_id=course.id,
                             total_files=len(files),
                             downloadable_files=len(filtered_files))

            return filtered_files

        except CanvasException as e:
            self.logger.error(f"Failed to fetch files", exception=e)
            raise DownloadError(f"Could not fetch files: {e}")

    def extract_metadata(self, file: File) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from a file.

        Args:
            file: Canvas file object

        Returns:
            Dict[str, Any]: File metadata
        """
        try:
            # Basic file information
            filename = getattr(file, 'filename', '')
            display_name = getattr(file, 'display_name', filename)

            metadata = {
                'id': file.id,
                'filename': filename,
                'display_name': display_name,
                'content_type': getattr(file, 'content_type', ''),
                'url': getattr(file, 'url', ''),
                'size': getattr(file, 'size', 0),
                'folder_id': getattr(file, 'folder_id', None),
                'uuid': getattr(file, 'uuid', ''),
                'locked': getattr(file, 'locked', False),
                'hidden': getattr(file, 'hidden', False),
                'locked_for_user': getattr(file, 'locked_for_user', False),
                'thumbnail_url': getattr(file, 'thumbnail_url', ''),
                'preview_url': getattr(file, 'preview_url', ''),
                'mime_class': getattr(file, 'mime_class', ''),
                'media_entry_id': getattr(file, 'media_entry_id', ''),
                'category': getattr(file, 'category', ''),
                'visibility_level': getattr(file, 'visibility_level', ''),
                'canvadocs_session_url': getattr(file, 'canvadocs_session_url', ''),
                'crocodoc_session_url': getattr(file, 'crocodoc_session_url', ''),
                'usage_rights': getattr(file, 'usage_rights', {}),
                'workflow_state': getattr(file, 'workflow_state', ''),
                'lock_explanation': getattr(file, 'lock_explanation', ''),
                'lock_at': self._format_date(getattr(file, 'lock_at', None)),
                'unlock_at': self._format_date(getattr(file, 'unlock_at', None))
            }

            # Date information
            metadata.update({
                'created_at': self._format_date(getattr(file, 'created_at', None)),
                'updated_at': self._format_date(getattr(file, 'updated_at', None)),
                'modified_at': self._format_date(getattr(file, 'modified_at', None))
            })

            # User information (who uploaded the file)
            user = getattr(file, 'user', {})
            if user:
                metadata['uploaded_by'] = {
                    'id': user.get('id'),
                    'name': user.get('name', ''),
                    'display_name': user.get('display_name', ''),
                    'avatar_image_url': user.get('avatar_image_url', '')
                }

            # File analysis
            metadata.update(self._analyze_file_properties(filename, metadata))

            # Canvas URLs
            html_url = getattr(file, 'html_url', '')
            if html_url:
                metadata['html_url'] = html_url
                metadata['canvas_url'] = html_url

            # File path information (will be updated during folder processing)
            metadata['folder_path'] = ''
            metadata['relative_path'] = filename

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract file metadata",
                              file_id=getattr(file, 'id', 'unknown'),
                              exception=e)

            # Return minimal metadata on error
            return {
                'id': getattr(file, 'id', None),
                'filename': getattr(file, 'filename', 'unknown_file'),
                'display_name': getattr(file, 'display_name', 'Unknown File'),
                'size': getattr(file, 'size', 0),
                'error': f"Metadata extraction failed: {e}"
            }

    def _analyze_file_properties(self, filename: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze file properties and classification.

        Args:
            filename: File name
            metadata: Existing metadata

        Returns:
            Dict[str, Any]: Additional file properties
        """
        if not filename:
            return {}

        file_path = Path(filename)
        extension = file_path.suffix.lower()

        # Determine MIME type if not already set
        mime_type = metadata.get('content_type', '')
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = 'application/octet-stream'

        # Classify file type
        file_category = self._classify_file_type(extension, mime_type)

        # Calculate file size in human readable format
        size_bytes = metadata.get('size', 0)
        size_human = self._format_file_size(size_bytes)

        return {
            'extension': extension,
            'mime_type_detected': mime_type,
            'file_category': file_category,
            'size_human': size_human,
            'size_mb': round(size_bytes / (1024 * 1024), 2),
            'is_media': file_category in ['image', 'video', 'audio'],
            'is_document': file_category in ['document', 'text', 'presentation', 'spreadsheet'],
            'is_archive': file_category == 'archive',
            'is_code': file_category == 'code'
        }

    def _classify_file_type(self, extension: str, mime_type: str) -> str:
        """
        Classify file type based on extension and MIME type.

        Args:
            extension: File extension
            mime_type: MIME type

        Returns:
            str: File category
        """
        # Document types
        document_extensions = {'.pdf', '.doc', '.docx', '.odt', '.rtf'}
        presentation_extensions = {'.ppt', '.pptx', '.odp', '.key'}
        spreadsheet_extensions = {'.xls', '.xlsx', '.ods', '.csv'}
        text_extensions = {'.txt', '.md', '.rst', '.tex'}

        # Media types
        image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.tiff'}
        video_extensions = {'.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v'}
        audio_extensions = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a'}

        # Archive types
        archive_extensions = {'.zip', '.rar', '.7z', '.tar', '.gz', '.bz2'}

        # Code types
        code_extensions = {'.py', '.js', '.html', '.css', '.java', '.cpp', '.c', '.php', '.rb'}

        if extension in document_extensions:
            return 'document'
        elif extension in presentation_extensions:
            return 'presentation'
        elif extension in spreadsheet_extensions:
            return 'spreadsheet'
        elif extension in text_extensions:
            return 'text'
        elif extension in image_extensions:
            return 'image'
        elif extension in video_extensions:
            return 'video'
        elif extension in audio_extensions:
            return 'audio'
        elif extension in archive_extensions:
            return 'archive'
        elif extension in code_extensions:
            return 'code'
        else:
            # Try to classify by MIME type
            if mime_type.startswith('image/'):
                return 'image'
            elif mime_type.startswith('video/'):
                return 'video'
            elif mime_type.startswith('audio/'):
                return 'audio'
            elif mime_type.startswith('text/'):
                return 'text'
            elif 'pdf' in mime_type:
                return 'document'
            else:
                return 'other'

    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        if size_bytes == 0:
            return "0 B"

        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0

        return f"{size_bytes:.1f} PB"

    def get_download_info(self, file: File) -> Optional[Dict[str, str]]:
        """
        Get download information for a file.

        Args:
            file: Canvas file object

        Returns:
            Optional[Dict[str, str]]: Download information with URL and filename
        """
        try:
            download_url = getattr(file, 'url', '')
            filename = getattr(file, 'filename', '') or getattr(file, 'display_name', '')

            if not download_url:
                self.logger.warning(f"No download URL available for file",
                                    file_id=file.id,
                                    filename=filename)
                return None

            if not filename:
                filename = f"file_{file.id}"

            return {
                'url': download_url,
                'filename': filename
            }

        except Exception as e:
            self.logger.error(f"Failed to get download info for file",
                              file_id=getattr(file, 'id', 'unknown'),
                              exception=e)
            return None

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all files for a course.

        This method overrides the base implementation to handle folder
        structure preservation and file organization.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting files download for course",
                             course_name=course_info.get('full_name', 'Unknown'),
                             course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if files download is enabled
            if not self.config.is_content_type_enabled('files'):
                self.logger.info("Files download is disabled")
                return self.stats

            # Build folder structure map if preserving structure
            if self.preserve_folder_structure:
                await self._build_folder_structure(course)

            # Fetch files
            files = self.fetch_content_list(course)

            if not files:
                self.logger.info("No files found in course")
                return self.stats

            self.stats['total_items'] = len(files)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(files))

            # Process each file
            items_metadata = []

            for index, file in enumerate(files, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(file)
                    metadata['item_number'] = index

                    # Determine file path based on folder structure
                    file_path = await self._determine_file_path(file, metadata, index)

                    # Get download information
                    download_info = self.get_download_info(file)

                    if download_info:
                        # Download the file
                        success = await self.download_file(
                            download_info['url'],
                            file_path,
                            expected_size=metadata.get('size')
                        )

                        # Update metadata with download result
                        metadata['downloaded'] = success
                        metadata['local_filename'] = file_path.name if success else None
                        metadata['local_path'] = str(file_path) if success else None

                        if success:
                            # Update file type statistics
                            file_category = metadata.get('file_category', 'other')
                            self._file_count_by_type[file_category] = \
                                self._file_count_by_type.get(file_category, 0) + 1
                    else:
                        # No download URL available
                        metadata['downloaded'] = False
                        metadata['local_filename'] = None
                        metadata['local_path'] = None
                        metadata['download_error'] = 'No download URL available'
                        self.stats['failed_items'] += 1

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                except Exception as e:
                    self.logger.error(f"Failed to process file",
                                      file_id=getattr(file, 'id', 'unknown'),
                                      filename=getattr(file, 'filename', 'unknown'),
                                      exception=e)
                    self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            # Save additional statistics
            await self._save_file_statistics()

            self.logger.info(f"Files download completed",
                             course_id=str(course.id),
                             file_types=dict(self._file_count_by_type),
                             **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Files download failed", exception=e)
            raise DownloadError(f"Files download failed: {e}")

    async def _build_folder_structure(self, course):
        """Build a map of folder structure for file organization."""
        try:
            self.logger.debug("Building folder structure map")

            # Get all folders in the course
            folders = list(course.get_folders())

            # Build folder hierarchy
            for folder in folders:
                folder_id = getattr(folder, 'id', None)
                folder_name = getattr(folder, 'name', '')
                parent_folder_id = getattr(folder, 'parent_folder_id', None)

                if folder_id:
                    self._folder_structure[folder_id] = {
                        'name': folder_name,
                        'parent_id': parent_folder_id,
                        'path': ''  # Will be calculated
                    }

            # Calculate full paths for each folder
            for folder_id in self._folder_structure:
                self._folder_structure[folder_id]['path'] = self._calculate_folder_path(folder_id)

            self.logger.debug(f"Built folder structure with {len(self._folder_structure)} folders")

        except Exception as e:
            self.logger.warning(f"Failed to build folder structure", exception=e)
            self._folder_structure = {}

    def _calculate_folder_path(self, folder_id: int, visited: set = None) -> str:
        """
        Calculate the full path for a folder by traversing up the hierarchy.

        Args:
            folder_id: ID of the folder
            visited: Set of visited folders (for cycle detection)

        Returns:
            str: Full folder path
        """
        if visited is None:
            visited = set()

        if folder_id in visited:
            # Cycle detected, return just the folder name
            return self._folder_structure.get(folder_id, {}).get('name', str(folder_id))

        visited.add(folder_id)

        folder_info = self._folder_structure.get(folder_id, {})
        folder_name = folder_info.get('name', str(folder_id))
        parent_id = folder_info.get('parent_id')

        if parent_id and parent_id in self._folder_structure:
            parent_path = self._calculate_folder_path(parent_id, visited)
            if parent_path:
                return f"{parent_path}/{folder_name}"

        return folder_name

    async def _determine_file_path(self, file: File, metadata: Dict[str, Any], index: int) -> Path:
        """
        Determine the local file path for a downloaded file.

        Args:
            file: Canvas file object
            metadata: File metadata
            index: File index number

        Returns:
            Path: Local file path for the downloaded file
        """
        filename = metadata.get('filename', f'file_{index}')
        sanitized_filename = self.sanitize_filename(filename)

        if self.preserve_folder_structure and metadata.get('folder_id'):
            # Use Canvas folder structure
            folder_id = metadata['folder_id']
            folder_path = self._folder_structure.get(folder_id, {}).get('path', '')

            if folder_path:
                # Create folder path and ensure it exists
                local_folder_path = self.content_folder / folder_path
                local_folder_path.mkdir(parents=True, exist_ok=True)
                file_path = local_folder_path / sanitized_filename
            else:
                file_path = self.content_folder / sanitized_filename

        elif self.organize_by_type:
            # Organize by file type
            file_category = metadata.get('file_category', 'other')
            type_folder = self.content_folder / file_category
            type_folder.mkdir(exist_ok=True)
            file_path = type_folder / sanitized_filename

        elif self.organize_by_date:
            # Organize by upload date
            created_at = metadata.get('created_at', '')
            if created_at:
                try:
                    date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    date_folder = date_obj.strftime('%Y-%m')
                    date_path = self.content_folder / date_folder
                    date_path.mkdir(exist_ok=True)
                    file_path = date_path / sanitized_filename
                except:
                    file_path = self.content_folder / sanitized_filename
            else:
                file_path = self.content_folder / sanitized_filename

        else:
            # Flat structure - all files in content folder
            file_path = self.content_folder / sanitized_filename

        # Ensure filename is unique
        if file_path.exists():
            unique_filename = self.generate_unique_filename(file_path.parent, file_path.name)
            file_path = file_path.parent / unique_filename

        return file_path

    def generate_unique_filename(self, directory: Path, filename: str) -> str:
        """Generate a unique filename in the given directory."""
        if not (directory / filename).exists():
            return filename

        # Extract name and extension
        file_path = Path(filename)
        name = file_path.stem
        extension = file_path.suffix

        # Try numbered variants
        counter = 1
        while True:
            unique_filename = f"{name}_{counter:03d}{extension}"
            if not (directory / unique_filename).exists():
                return unique_filename
            counter += 1

            # Prevent infinite loop
            if counter > 9999:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                return f"{name}_{timestamp}{extension}"

    async def _save_file_statistics(self):
        """Save additional file statistics."""
        try:
            stats_filename = "file_statistics.json"
            stats_path = self.content_folder / stats_filename

            # Calculate additional statistics
            total_size_mb = self.stats.get('total_size_bytes', 0) / (1024 * 1024)

            statistics = {
                'download_summary': self.stats,
                'file_types': dict(self._file_count_by_type),
                'total_size_mb': round(total_size_mb, 2),
                'folder_structure_preserved': self.preserve_folder_structure,
                'folders_processed': len(self._folder_structure),
                'download_settings': {
                    'max_file_size_mb': self.max_file_size_mb,
                    'download_locked_files': self.download_locked_files,
                    'include_hidden_files': self.include_hidden_files,
                    'preserve_folder_structure': self.preserve_folder_structure
                },
                'generated_at': datetime.now().isoformat()
            }

            async with aiofiles.open(stats_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(statistics, indent=2, ensure_ascii=False))

            self.logger.debug(f"Saved file statistics",
                              file_path=str(stats_path))

        except Exception as e:
            self.logger.error(f"Failed to save file statistics", exception=e)

    def _format_date(self, date_obj) -> str:
        """Format a date object to ISO string."""
        if not date_obj:
            return ""

        try:
            if hasattr(date_obj, 'isoformat'):
                return date_obj.isoformat()
            elif isinstance(date_obj, str):
                return date_obj
            else:
                return str(date_obj)
        except:
            return ""


# Register the downloader with the factory
from .base import ContentDownloaderFactory

ContentDownloaderFactory.register_downloader('files', FilesDownloader)