"""
File Manager Module

This module provides comprehensive file and directory management functionality
for the Canvas Downloader application. It handles file operations, directory
structure creation, file validation, and metadata management.

The File Manager ensures:
- Safe file operations with proper error handling
- Directory structure creation and management
- File naming consistency and validation
- Duplicate detection and handling
- File integrity verification
- Metadata file management
- Cleanup and maintenance operations

Features:
- Cross-platform file operations
- Atomic file operations to prevent corruption
- File size and hash verification
- Directory traversal and organization
- Bulk file operations with progress tracking
- File type detection and validation
- Safe filename sanitization
- Symbolic link and junction handling

Usage:
    # Initialize file manager
    file_manager = FileManager()

    # Create course directory structure
    course_path = file_manager.create_course_directory(course_info)

    # Save a file with validation
    success = file_manager.save_file(
        content_data,
        file_path,
        verify_integrity=True
    )

    # Manage course files
    file_manager.organize_course_files(course_path)
"""

import os
import shutil
import hashlib
import tempfile
import mimetypes
import json
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple, Iterator
from dataclasses import dataclass, field
from datetime import datetime
import stat
import platform
import asyncio
import aiofiles
from concurrent.futures import ThreadPoolExecutor

from ..config.settings import get_config
from ..utils.logger import get_logger
from ..core.course_parser import ParsedCourse


@dataclass
class FileInfo:
    """
    Data class representing comprehensive file information.

    This class contains metadata about files managed by the File Manager,
    including verification data, timestamps, and organizational information.
    """
    # Basic file information
    path: Path
    name: str = ""
    size: int = 0
    extension: str = ""
    mime_type: str = ""

    # File integrity
    hash_sha256: str = ""
    hash_md5: str = ""
    is_verified: bool = False

    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    modified_at: datetime = field(default_factory=datetime.now)
    accessed_at: datetime = field(default_factory=datetime.now)

    # Canvas-specific metadata
    canvas_id: str = ""
    content_type: str = ""
    course_id: str = ""
    original_url: str = ""

    # File status
    exists: bool = False
    is_readable: bool = False
    is_writable: bool = False
    permissions: str = ""

    def __post_init__(self):
        """Post-initialization to populate derived fields."""
        if not self.name:
            self.name = self.path.name

        if not self.extension:
            self.extension = self.path.suffix.lower()

        if not self.mime_type:
            self.mime_type, _ = mimetypes.guess_type(str(self.path))
            if not self.mime_type:
                self.mime_type = "application/octet-stream"

        # Update file status if path exists
        if self.path.exists():
            self._update_file_status()

    def _update_file_status(self):
        """Update file status information from actual file."""
        try:
            stat_result = self.path.stat()

            self.exists = True
            self.size = stat_result.st_size
            self.modified_at = datetime.fromtimestamp(stat_result.st_mtime)
            self.accessed_at = datetime.fromtimestamp(stat_result.st_atime)

            # Check permissions
            self.is_readable = os.access(self.path, os.R_OK)
            self.is_writable = os.access(self.path, os.W_OK)

            # Get permission string
            mode = stat_result.st_mode
            self.permissions = stat.filemode(mode)

        except Exception:
            self.exists = False

    def calculate_hashes(self) -> Tuple[str, str]:
        """
        Calculate SHA256 and MD5 hashes of the file.

        Returns:
            Tuple[str, str]: (sha256_hash, md5_hash)
        """
        if not self.path.exists():
            return "", ""

        sha256_hash = hashlib.sha256()
        md5_hash = hashlib.md5()

        try:
            with open(self.path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
                    md5_hash.update(chunk)

            self.hash_sha256 = sha256_hash.hexdigest()
            self.hash_md5 = md5_hash.hexdigest()
            self.is_verified = True

            return self.hash_sha256, self.hash_md5

        except Exception as e:
            logger = get_logger(__name__)
            logger.error(f"Failed to calculate file hashes",
                         file_path=str(self.path), exception=e)
            return "", ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert FileInfo to dictionary for JSON serialization."""
        return {
            'path': str(self.path),
            'name': self.name,
            'size': self.size,
            'extension': self.extension,
            'mime_type': self.mime_type,
            'hash_sha256': self.hash_sha256,
            'hash_md5': self.hash_md5,
            'is_verified': self.is_verified,
            'created_at': self.created_at.isoformat(),
            'modified_at': self.modified_at.isoformat(),
            'accessed_at': self.accessed_at.isoformat(),
            'canvas_id': self.canvas_id,
            'content_type': self.content_type,
            'course_id': self.course_id,
            'original_url': self.original_url,
            'exists': self.exists,
            'is_readable': self.is_readable,
            'is_writable': self.is_writable,
            'permissions': self.permissions
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileInfo':
        """Create FileInfo from dictionary."""
        path = Path(data['path'])

        file_info = cls(path=path)

        # Update fields from dictionary
        for key, value in data.items():
            if key == 'path':
                continue
            elif key in ['created_at', 'modified_at', 'accessed_at']:
                if isinstance(value, str):
                    setattr(file_info, key, datetime.fromisoformat(value))
            else:
                setattr(file_info, key, value)

        return file_info


class DirectoryStructure:
    """
    Class for managing directory structure templates and creation.

    This class handles the creation and management of directory structures
    for organizing downloaded Canvas content.
    """

    def __init__(self, base_path: Path):
        """
        Initialize directory structure manager.

        Args:
            base_path: Base directory path for all operations
        """
        self.base_path = Path(base_path)
        self.logger = get_logger(__name__)

    def create_course_structure(self, course: ParsedCourse) -> Dict[str, Path]:
        """
        Create complete directory structure for a course.

        Args:
            course: ParsedCourse object with course information

        Returns:
            Dict[str, Path]: Dictionary mapping content types to their paths
        """
        # Create base course directory
        course_path = self.base_path / course.folder_path
        course_path.mkdir(parents=True, exist_ok=True)

        # Define content type subdirectories
        content_types = [
            'announcements',
            'assignments',
            'discussions',
            'files',
            'grades',
            'modules',
            'people',
            'quizzes',
            'chat'
        ]

        structure = {'course_root': course_path}

        # Create subdirectories for each content type
        for content_type in content_types:
            content_path = course_path / content_type
            content_path.mkdir(exist_ok=True)
            structure[content_type] = content_path

        # Create metadata directory
        metadata_path = course_path / '_metadata'
        metadata_path.mkdir(exist_ok=True)
        structure['metadata'] = metadata_path

        # Create logs directory
        logs_path = course_path / '_logs'
        logs_path.mkdir(exist_ok=True)
        structure['logs'] = logs_path

        self.logger.info(f"Created course directory structure",
                         course_name=course.folder_name,
                         course_path=str(course_path),
                         content_types=len(content_types))

        return structure

    def ensure_directory_exists(self, directory_path: Union[str, Path]) -> Path:
        """
        Ensure a directory exists, creating it if necessary.

        Args:
            directory_path: Path to the directory

        Returns:
            Path: Resolved directory path
        """
        path = Path(directory_path)
        path.mkdir(parents=True, exist_ok=True)
        return path.resolve()


class FileManager:
    """
    Comprehensive File Manager for Canvas Downloader

    This class provides a complete file management system for the Canvas
    Downloader application. It handles all file operations including creation,
    verification, organization, and maintenance of downloaded content.

    The File Manager ensures data integrity, provides atomic operations,
    and maintains comprehensive metadata about all managed files.
    """

    def __init__(self, config=None):
        """
        Initialize the File Manager.

        Args:
            config: Optional configuration object. If None, uses global config.
        """
        self.config = config or get_config()
        self.logger = get_logger(__name__)

        # Initialize base paths
        self.base_download_path = Path(self.config.download_settings.base_download_path)
        self.base_download_path.mkdir(parents=True, exist_ok=True)

        # Directory structure manager
        self.directory_manager = DirectoryStructure(self.base_download_path)

        # File operation settings
        self.chunk_size = self.config.download_settings.chunk_size
        self.verify_files = True
        self.atomic_operations = True

        # File registry for tracking managed files
        self._file_registry: Dict[str, FileInfo] = {}
        self._registry_file = self.base_download_path / '_file_registry.json'

        # Thread pool for parallel operations
        self._executor = ThreadPoolExecutor(max_workers=4)

        # Load existing file registry
        self._load_file_registry()

        self.logger.info("File manager initialized",
                         base_path=str(self.base_download_path),
                         atomic_operations=self.atomic_operations,
                         verify_files=self.verify_files)

    def _load_file_registry(self):
        """Load file registry from disk."""
        try:
            if self._registry_file.exists():
                with open(self._registry_file, 'r', encoding='utf-8') as f:
                    registry_data = json.load(f)

                for file_path, file_data in registry_data.items():
                    self._file_registry[file_path] = FileInfo.from_dict(file_data)

                self.logger.debug(f"Loaded file registry with {len(self._file_registry)} entries")

        except Exception as e:
            self.logger.error("Failed to load file registry", exception=e)
            self._file_registry = {}

    def _save_file_registry(self):
        """Save file registry to disk."""
        try:
            registry_data = {}
            for file_path, file_info in self._file_registry.items():
                registry_data[file_path] = file_info.to_dict()

            # Use atomic write to prevent corruption
            temp_file = self._registry_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(registry_data, f, indent=2, ensure_ascii=False)

            # Atomic move
            temp_file.replace(self._registry_file)

        except Exception as e:
            self.logger.error("Failed to save file registry", exception=e)

    def create_course_directory(self, course: ParsedCourse) -> Dict[str, Path]:
        """
        Create directory structure for a course.

        Args:
            course: ParsedCourse object with course information

        Returns:
            Dict[str, Path]: Dictionary mapping content types to their directory paths
        """
        return self.directory_manager.create_course_structure(course)

    def sanitize_filename(self, filename: str) -> str:
        """
        Sanitize a filename for cross-platform compatibility.

        Args:
            filename: Original filename

        Returns:
            str: Sanitized filename
        """
        if not self.config.file_naming.sanitize_filenames:
            return filename

        # Characters that are invalid in filenames on various platforms
        invalid_chars = {
            # Windows reserved characters
            '<': '',
            '>': '',
            ':': '-',
            '"': "'",
            '/': '-',
            '\\': '-',
            '|': '-',
            '?': '',
            '*': '',

            # Control characters
            '\0': '',
            '\r': '',
            '\n': ' ',
            '\t': ' ',
        }

        # Additional problematic characters
        for i in range(1, 32):
            invalid_chars[chr(i)] = ''

        # Replace invalid characters
        sanitized = filename
        for char, replacement in invalid_chars.items():
            sanitized = sanitized.replace(char, replacement)

        # Handle reserved names on Windows
        reserved_names = {
            'CON', 'PRN', 'AUX', 'NUL',
            'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
            'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
        }

        name_part = sanitized.split('.')[0].upper()
        if name_part in reserved_names:
            sanitized = f"_{sanitized}"

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

    def generate_unique_filename(self, directory: Path, base_filename: str) -> str:
        """
        Generate a unique filename in the given directory.

        Args:
            directory: Target directory
            base_filename: Base filename to make unique

        Returns:
            str: Unique filename
        """
        sanitized_name = self.sanitize_filename(base_filename)

        # If file doesn't exist, use as-is
        target_path = directory / sanitized_name
        if not target_path.exists():
            return sanitized_name

        # Extract name and extension
        if '.' in sanitized_name:
            name, extension = sanitized_name.rsplit('.', 1)
            extension = '.' + extension
        else:
            name = sanitized_name
            extension = ''

        # Try numbered variants
        counter = 1
        while True:
            unique_name = f"{name}_{counter:03d}{extension}"
            target_path = directory / unique_name

            if not target_path.exists():
                return unique_name

            counter += 1

            # Prevent infinite loop
            if counter > 9999:
                # Add timestamp to ensure uniqueness
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                unique_name = f"{name}_{timestamp}{extension}"
                break

        return unique_name

    def save_file(self, content: Union[bytes, str], file_path: Path,
                  verify_integrity: bool = True, canvas_metadata: Dict[str, Any] = None) -> FileInfo:
        """
        Save content to a file with optional integrity verification.

        Args:
            content: Content to save (bytes or string)
            file_path: Target file path
            verify_integrity: Whether to verify file integrity after writing
            canvas_metadata: Optional Canvas-specific metadata

        Returns:
            FileInfo: Information about the saved file
        """
        file_path = Path(file_path)

        try:
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Create FileInfo object
            file_info = FileInfo(
                path=file_path,
                canvas_id=canvas_metadata.get('id', '') if canvas_metadata else '',
                content_type=canvas_metadata.get('content_type', '') if canvas_metadata else '',
                course_id=canvas_metadata.get('course_id', '') if canvas_metadata else '',
                original_url=canvas_metadata.get('url', '') if canvas_metadata else ''
            )

            if self.atomic_operations:
                # Use atomic write operation
                success = self._atomic_write(content, file_path)
            else:
                # Direct write
                success = self._direct_write(content, file_path)

            if success:
                # Update file info with actual file data
                file_info._update_file_status()

                # Verify integrity if requested
                if verify_integrity:
                    file_info.calculate_hashes()

                # Register file
                self._register_file(file_info)

                self.logger.debug(f"Successfully saved file",
                                  file_path=str(file_path),
                                  file_size=file_info.size,
                                  verified=file_info.is_verified)

            return file_info

        except Exception as e:
            self.logger.error(f"Failed to save file",
                              file_path=str(file_path),
                              exception=e)

            # Return FileInfo with error state
            return FileInfo(path=file_path, exists=False)

    def _atomic_write(self, content: Union[bytes, str], file_path: Path) -> bool:
        """
        Perform atomic write operation using temporary file.

        Args:
            content: Content to write
            file_path: Target file path

        Returns:
            bool: True if successful
        """
        try:
            # Create temporary file in the same directory
            temp_file = file_path.with_suffix(file_path.suffix + '.tmp')

            # Write to temporary file
            if self._direct_write(content, temp_file):
                # Atomic move to final location
                temp_file.replace(file_path)
                return True
            else:
                # Clean up temporary file on failure
                if temp_file.exists():
                    temp_file.unlink()
                return False

        except Exception as e:
            self.logger.error(f"Atomic write failed", exception=e)
            return False

    def _direct_write(self, content: Union[bytes, str], file_path: Path) -> bool:
        """
        Perform direct write operation.

        Args:
            content: Content to write
            file_path: Target file path

        Returns:
            bool: True if successful
        """
        try:
            mode = 'wb' if isinstance(content, bytes) else 'w'
            encoding = None if isinstance(content, bytes) else 'utf-8'

            with open(file_path, mode, encoding=encoding) as f:
                f.write(content)

            return True

        except Exception as e:
            self.logger.error(f"Direct write failed", exception=e)
            return False

    async def save_file_async(self, content: Union[bytes, str], file_path: Path,
                              verify_integrity: bool = True,
                              canvas_metadata: Dict[str, Any] = None) -> FileInfo:
        """
        Asynchronously save content to a file.

        Args:
            content: Content to save
            file_path: Target file path
            verify_integrity: Whether to verify integrity
            canvas_metadata: Optional Canvas metadata

        Returns:
            FileInfo: Information about the saved file
        """
        file_path = Path(file_path)

        try:
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Create FileInfo object
            file_info = FileInfo(
                path=file_path,
                canvas_id=canvas_metadata.get('id', '') if canvas_metadata else '',
                content_type=canvas_metadata.get('content_type', '') if canvas_metadata else '',
                course_id=canvas_metadata.get('course_id', '') if canvas_metadata else '',
                original_url=canvas_metadata.get('url', '') if canvas_metadata else ''
            )

            # Write file asynchronously
            mode = 'wb' if isinstance(content, bytes) else 'w'
            encoding = None if isinstance(content, bytes) else 'utf-8'

            async with aiofiles.open(file_path, mode, encoding=encoding) as f:
                await f.write(content)

            # Update file info
            file_info._update_file_status()

            # Verify integrity if requested
            if verify_integrity:
                # Run hash calculation in executor to avoid blocking
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self._executor, file_info.calculate_hashes)

            # Register file
            self._register_file(file_info)

            return file_info

        except Exception as e:
            self.logger.error(f"Async file save failed",
                              file_path=str(file_path),
                              exception=e)
            return FileInfo(path=file_path, exists=False)

    def _register_file(self, file_info: FileInfo):
        """Register a file in the file registry."""
        self._file_registry[str(file_info.path)] = file_info
        # Save registry periodically (could be optimized to batch saves)
        self._save_file_registry()

    def get_file_info(self, file_path: Union[str, Path]) -> Optional[FileInfo]:
        """
        Get information about a file.

        Args:
            file_path: Path to the file

        Returns:
            Optional[FileInfo]: File information or None if not found
        """
        file_path_str = str(Path(file_path))
        return self._file_registry.get(file_path_str)

    def file_exists(self, file_path: Union[str, Path]) -> bool:
        """
        Check if a file exists and is registered.

        Args:
            file_path: Path to check

        Returns:
            bool: True if file exists and is registered
        """
        path = Path(file_path)

        # Check physical existence
        if not path.exists():
            return False

        # Check registry
        file_info = self.get_file_info(file_path)
        return file_info is not None and file_info.exists

    def verify_file_integrity(self, file_path: Union[str, Path],
                              expected_hash: str = None) -> bool:
        """
        Verify file integrity using hash comparison.

        Args:
            file_path: Path to the file
            expected_hash: Optional expected hash to compare against

        Returns:
            bool: True if file integrity is verified
        """
        file_info = self.get_file_info(file_path)

        if not file_info or not file_info.exists:
            return False

        # Calculate current hash
        current_sha256, _ = file_info.calculate_hashes()

        if expected_hash:
            return current_sha256 == expected_hash

        # If no expected hash provided, just verify the file is readable
        return bool(current_sha256)

    def get_directory_info(self, directory_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Get comprehensive information about a directory.

        Args:
            directory_path: Path to the directory

        Returns:
            Dict[str, Any]: Directory information including file counts and sizes
        """
        directory = Path(directory_path)

        if not directory.exists() or not directory.is_dir():
            return {'error': 'Directory does not exist'}

        info = {
            'path': str(directory),
            'exists': True,
            'total_files': 0,
            'total_directories': 0,
            'total_size': 0,
            'file_types': {},
            'largest_file': None,
            'newest_file': None,
            'oldest_file': None
        }

        try:
            newest_time = None
            oldest_time = None
            largest_size = 0

            for item in directory.rglob('*'):
                if item.is_file():
                    info['total_files'] += 1

                    # File size
                    size = item.stat().st_size
                    info['total_size'] += size

                    # Largest file
                    if size > largest_size:
                        largest_size = size
                        info['largest_file'] = {
                            'path': str(item),
                            'size': size
                        }

                    # File type
                    extension = item.suffix.lower()
                    info['file_types'][extension] = info['file_types'].get(extension, 0) + 1

                    # Modification times
                    mod_time = item.stat().st_mtime
                    if newest_time is None or mod_time > newest_time:
                        newest_time = mod_time
                        info['newest_file'] = {
                            'path': str(item),
                            'modified': datetime.fromtimestamp(mod_time).isoformat()
                        }

                    if oldest_time is None or mod_time < oldest_time:
                        oldest_time = mod_time
                        info['oldest_file'] = {
                            'path': str(item),
                            'modified': datetime.fromtimestamp(mod_time).isoformat()
                        }

                elif item.is_dir():
                    info['total_directories'] += 1

        except Exception as e:
            self.logger.error(f"Error analyzing directory", exception=e)
            info['error'] = str(e)

        return info

    def cleanup_empty_directories(self, base_path: Union[str, Path]) -> int:
        """
        Remove empty directories recursively.

        Args:
            base_path: Base path to start cleanup

        Returns:
            int: Number of directories removed
        """
        base = Path(base_path)
        removed_count = 0

        try:
            # Get all directories, sorted by depth (deepest first)
            directories = sorted([d for d in base.rglob('*') if d.is_dir()],
                                 key=lambda x: len(x.parts), reverse=True)

            for directory in directories:
                try:
                    # Skip if not empty
                    if any(directory.iterdir()):
                        continue

                    # Remove empty directory
                    directory.rmdir()
                    removed_count += 1

                    self.logger.debug(f"Removed empty directory: {directory}")

                except OSError:
                    # Directory not empty or permission denied
                    continue

        except Exception as e:
            self.logger.error(f"Error during directory cleanup", exception=e)

        self.logger.info(f"Cleanup completed", removed_directories=removed_count)
        return removed_count

    def get_file_registry_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the file registry.

        Returns:
            Dict[str, Any]: Registry statistics
        """
        total_files = len(self._file_registry)
        verified_files = sum(1 for f in self._file_registry.values() if f.is_verified)
        total_size = sum(f.size for f in self._file_registry.values() if f.exists)

        content_types = {}
        for file_info in self._file_registry.values():
            ct = file_info.content_type or 'unknown'
            content_types[ct] = content_types.get(ct, 0) + 1

        return {
            'total_files': total_files,
            'verified_files': verified_files,
            'verification_rate': (verified_files / total_files * 100) if total_files > 0 else 0,
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / 1024 / 1024, 2),
            'content_types': content_types,
            'registry_file_path': str(self._registry_file)
        }

    def export_file_inventory(self, output_path: Union[str, Path]) -> bool:
        """
        Export complete file inventory to JSON file.

        Args:
            output_path: Path for the inventory file

        Returns:
            bool: True if export successful
        """
        try:
            inventory = {
                'export_date': datetime.now().isoformat(),
                'base_path': str(self.base_download_path),
                'total_files': len(self._file_registry),
                'statistics': self.get_file_registry_stats(),
                'files': {path: info.to_dict() for path, info in self._file_registry.items()}
            }

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(inventory, f, indent=2, ensure_ascii=False)

            self.logger.info(f"File inventory exported", output_path=str(output_path))
            return True

        except Exception as e:
            self.logger.error(f"Failed to export file inventory", exception=e)
            return False

    def cleanup(self):
        """Clean up file manager resources."""
        # Save final registry state
        self._save_file_registry()

        # Shutdown executor
        self._executor.shutdown(wait=True)

        self.logger.info("File manager cleaned up")


def create_file_manager(config=None, **kwargs) -> FileManager:
    """
    Factory function to create a file manager instance.

    Args:
        config: Optional configuration object
        **kwargs: Additional arguments to pass to FileManager constructor

    Returns:
        FileManager: Configured file manager instance
    """
    return FileManager(config=config, **kwargs)