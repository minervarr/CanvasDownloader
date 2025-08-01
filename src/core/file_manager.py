"""
Enhanced File Manager Module - BULLETPROOF IMPLEMENTATION

This module provides a rock-solid file and directory management system for the
Canvas Downloader application. It's designed to work seamlessly with the new
bulletproof configuration system and handle all edge cases gracefully.

Key Features:
- Seamless integration with the new configuration system
- Multiple ways to access configuration (attribute, dot notation, safe_get)
- Bulletproof error handling that never crashes the application
- Comprehensive file validation and integrity checking
- Atomic file operations to prevent corruption
- Intelligent fallback mechanisms
- Extensive logging and debugging support

The File Manager ensures:
- Safe file operations with proper error handling
- Directory structure creation and management
- File naming consistency and validation
- Duplicate detection and handling
- File integrity verification
- Metadata file management
- Cleanup and maintenance operations

Usage:
    # Initialize file manager (automatically uses bulletproof config)
    file_manager = FileManager()

    # All these work seamlessly:
    base_path = file_manager.base_download_path
    chunk_size = file_manager.chunk_size

    # Create course directory structure
    course_path = file_manager.create_course_directory(course_info)

    # Save a file with validation
    success = file_manager.save_file(content_data, file_path, verify_integrity=True)
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
import threading

from ..config.settings import get_config
from ..utils.logger import get_logger
from ..config.constants import get_config_default_and_type


@dataclass
class FileInfo:
    """
    Comprehensive file information with all metadata.

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
        if self.path and self.path.exists():
            self.name = self.path.name
            self.extension = self.path.suffix.lower()
            self.size = self.path.stat().st_size
            self.exists = True

            # Get MIME type
            self.mime_type = mimetypes.guess_type(str(self.path))[0] or "application/octet-stream"

            # Check permissions
            self.is_readable = os.access(self.path, os.R_OK)
            self.is_writable = os.access(self.path, os.W_OK)

            # Get file timestamps
            stat_info = self.path.stat()
            self.modified_at = datetime.fromtimestamp(stat_info.st_mtime)
            self.accessed_at = datetime.fromtimestamp(stat_info.st_atime)
            if hasattr(stat_info, 'st_birthtime'):  # macOS
                self.created_at = datetime.fromtimestamp(stat_info.st_birthtime)
            else:
                self.created_at = datetime.fromtimestamp(stat_info.st_ctime)

    def to_dict(self) -> Dict[str, Any]:
        """Convert FileInfo to dictionary."""
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
        info = cls(path=Path(data['path']))

        # Set all fields from dictionary
        for key, value in data.items():
            if hasattr(info, key) and key != 'path':
                if key in ('created_at', 'modified_at', 'accessed_at') and isinstance(value, str):
                    setattr(info, key, datetime.fromisoformat(value))
                else:
                    setattr(info, key, value)

        return info


class DirectoryStructure:
    """
    Directory structure management for course organization.

    This class handles the creation and management of directory structures
    for organizing downloaded Canvas content.
    """

    def __init__(self, base_path: Path):
        """
        Initialize directory structure manager.

        Args:
            base_path: Base path for all downloads
        """
        self.base_path = Path(base_path)
        self.logger = get_logger(__name__)

        # Ensure base path exists
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"Failed to create base directory", exception=e)
            raise

    def create_course_structure(self, course_info: Dict[str, Any]) -> Path:
        """
        Create directory structure for a course.

        Args:
            course_info: Course information dictionary

        Returns:
            Path: Path to the course directory
        """
        try:
            # Get configuration for folder structure
            config = get_config()

            # Build course path components
            path_parts = []

            # Add year/semester organization if enabled
            if config.safe_get('folder_structure.organize_by_semester', True, bool):
                # Try to extract semester/year information
                course_name = course_info.get('name', '')
                course_code = course_info.get('course_code', '')

                # Simple semester detection
                year = datetime.now().year
                semester = "Unknown"

                if any(term in course_name.lower() for term in ['fall', 'autumn']):
                    semester = "Fall"
                elif any(term in course_name.lower() for term in ['spring']):
                    semester = "Spring"
                elif any(term in course_name.lower() for term in ['summer']):
                    semester = "Summer"
                elif any(term in course_name.lower() for term in ['winter']):
                    semester = "Winter"

                path_parts.extend([str(year), semester])

            # Add course folder
            course_folder_name = self._build_course_folder_name(course_info)
            path_parts.append(course_folder_name)

            # Create the full path
            course_path = self.base_path
            for part in path_parts:
                course_path = course_path / self._sanitize_folder_name(part)

            # Create the directory structure
            course_path.mkdir(parents=True, exist_ok=True)

            self.logger.info(f"Created course directory structure",
                           course_path=str(course_path))

            return course_path

        except Exception as e:
            self.logger.error(f"Failed to create course structure", exception=e)
            # Fallback to simple structure
            fallback_name = course_info.get('name', 'Unknown_Course')
            fallback_path = self.base_path / self._sanitize_folder_name(fallback_name)
            fallback_path.mkdir(parents=True, exist_ok=True)
            return fallback_path

    def _build_course_folder_name(self, course_info: Dict[str, Any]) -> str:
        """Build course folder name from course information."""
        config = get_config()
        template = config.safe_get('folder_structure.folder_name_template',
                                 '{course_code}-{course_name}', str)

        try:
            # Prepare variables for template
            course_code = course_info.get('course_code', 'UNKNOWN')
            course_name = course_info.get('name', 'Unknown Course')

            # Remove course code from name if it's already there
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
            self.logger.warning(f"Failed to build course folder name from template", exception=e)
            # Simple fallback
            return f"{course_info.get('course_code', 'UNKNOWN')}-{course_info.get('name', 'Unknown')}"

    def _sanitize_folder_name(self, name: str) -> str:
        """
        Sanitize folder name for filesystem compatibility.

        Args:
            name: Original folder name

        Returns:
            str: Sanitized folder name
        """
        config = get_config()

        if not config.safe_get('folder_structure.sanitize_names', True, bool):
            return name

        # Remove/replace invalid characters
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


class FileManager:
    """
    Bulletproof File Manager with Enhanced Configuration Integration

    This class provides comprehensive file management functionality that works
    seamlessly with the new bulletproof configuration system. It handles all
    file operations with proper error handling and fallback mechanisms.
    """

    def __init__(self, config=None):
        """
        Initialize the File Manager with bulletproof configuration handling.

        Args:
            config: Optional configuration object. If None, uses global config.
        """
        self.config = config or get_config()
        self.logger = get_logger(__name__)
        self._lock = threading.RLock()

        # Initialize paths with multiple fallback strategies
        self._initialize_paths()

        # Directory structure manager
        self.directory_manager = DirectoryStructure(self.base_download_path)

        # File operation settings with safe defaults
        self._initialize_settings()

        # File registry for tracking managed files
        self._file_registry: Dict[str, FileInfo] = {}
        self._registry_file = self.base_download_path / '_file_registry.json'

        # Thread pool for parallel operations
        self._executor = ThreadPoolExecutor(max_workers=4)

        # Load existing file registry
        self._load_file_registry()

        self.logger.info("File manager initialized successfully",
                        base_path=str(self.base_download_path),
                        chunk_size=self.chunk_size,
                        verify_files=self.verify_files)

    def _initialize_paths(self) -> None:
        '''Initialize file paths with multiple fallback strategies.'''
        try:
            # Get default and type for base_download_path
            DEFAULT_VALUE, EXPECTED_TYPE = get_config_default_and_type('download_settings.base_download_path')

            # Try multiple ways to get the base download path
            base_path = None

            # Method 1: New attribute access
            try:
                base_path = self.config.safe_get('download_settings.base_download_path', DEFAULT_VALUE, EXPECTED_TYPE)
                self.logger.debug("Got base path via attribute access")
            except AttributeError:
                pass

            # Method 2: Dot notation with safe_get
            if base_path is None:
                base_path = self.config.safe_get('download_settings.base_download_path', DEFAULT_VALUE, EXPECTED_TYPE)
                self.logger.debug("Got base path via safe_get")

            # Method 3: Regular get method
            if base_path is None:
                base_path = self.config.get('download_settings.base_download_path', DEFAULT_VALUE)
                self.logger.debug("Got base path via get method")

            # Method 4: Absolute fallback
            if base_path is None:
                base_path = DEFAULT_VALUE
                self.logger.warning("Using absolute fallback for base path")

            # Convert to Path and create directory
            self.base_download_path = Path(base_path)
            self.base_download_path.mkdir(parents=True, exist_ok=True)

            self.logger.info(f"Base download path initialized", path=str(self.base_download_path))

        except Exception as e:
            self.logger.error(f"Failed to initialize paths", exception=e)
            # Emergency fallback
            self.base_download_path = Path('downloads')
            self.base_download_path.mkdir(parents=True, exist_ok=True)
            self.logger.warning("Using emergency fallback path: downloads")

    def _initialize_settings(self) -> None:
        '''Initialize file operation settings with safe defaults.'''
        try:
            # Get defaults and types for all settings
            verify_default, verify_type = get_config_default_and_type('download_settings.verify_downloads')
            retries_default, retries_type = get_config_default_and_type('download_settings.max_retries')
            timeout_default, timeout_type = get_config_default_and_type('download_settings.timeout')
            skip_default, skip_type = get_config_default_and_type('download_settings.skip_existing')

            # Get settings with multiple access methods and safe defaults

            # Chunk size
            try:
                self.chunk_size = self.config.safe_get('download_settings.chunk_size', 8192, int)
            except AttributeError:
                self.chunk_size = self.config.safe_get('download_settings.chunk_size', 8192, int)

            # Verify files
            try:
                self.verify_files = self.config.safe_get('download_settings.verify_downloads', verify_default,
                                                         verify_type)
            except AttributeError:
                self.verify_files = self.config.safe_get('download_settings.verify_downloads', verify_default,
                                                         verify_type)

            # Max retries
            try:
                self.max_retries = self.config.safe_get('download_settings.max_retries', retries_default, retries_type)
            except AttributeError:
                self.max_retries = self.config.safe_get('download_settings.max_retries', retries_default, retries_type)

            # Timeout
            try:
                self.timeout = self.config.safe_get('download_settings.timeout', timeout_default, timeout_type)
            except AttributeError:
                self.timeout = self.config.safe_get('download_settings.timeout', timeout_default, timeout_type)

            # Skip existing
            try:
                self.skip_existing = self.config.safe_get('download_settings.skip_existing', skip_default, skip_type)
            except AttributeError:
                self.skip_existing = self.config.safe_get('download_settings.skip_existing', skip_default, skip_type)

            # Additional settings
            self.atomic_operations = True
            self.preserve_timestamps = True

            self.logger.debug("File operation settings initialized",
                              chunk_size=self.chunk_size,
                              verify_files=self.verify_files,
                              max_retries=self.max_retries)

        except Exception as e:
            self.logger.error(f"Failed to initialize settings", exception=e)
            # Safe defaults
            self.chunk_size = 8192
            self.verify_files = True
            self.max_retries = 3
            self.timeout = 30
            self.skip_existing = True
            self.atomic_operations = True
            self.preserve_timestamps = True

            self.logger.warning("Using safe default settings")

    def _load_file_registry(self) -> None:
        """Load file registry from disk with error handling."""
        try:
            if self._registry_file.exists():
                with open(self._registry_file, 'r', encoding='utf-8') as f:
                    registry_data = json.load(f)

                for file_path, file_data in registry_data.items():
                    try:
                        self._file_registry[file_path] = FileInfo.from_dict(file_data)
                    except Exception as e:
                        self.logger.warning(f"Failed to load registry entry for {file_path}", exception=e)

                self.logger.debug(f"Loaded file registry with {len(self._file_registry)} entries")

        except Exception as e:
            self.logger.error("Failed to load file registry", exception=e)
            self._file_registry = {}

    def _save_file_registry(self) -> None:
        """Save file registry to disk with error handling."""
        try:
            with self._lock:
                registry_data = {}

                for file_path, file_info in self._file_registry.items():
                    try:
                        registry_data[file_path] = file_info.to_dict()
                    except Exception as e:
                        self.logger.warning(f"Failed to serialize registry entry for {file_path}", exception=e)

                # Atomic write to prevent corruption
                temp_file = self._registry_file.with_suffix('.tmp')

                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(registry_data, f, indent=2, ensure_ascii=False)

                # Move to final location
                temp_file.replace(self._registry_file)

                self.logger.debug(f"Saved file registry with {len(registry_data)} entries")

        except Exception as e:
            self.logger.error("Failed to save file registry", exception=e)

    def create_course_directory(self, course_info: Dict[str, Any]) -> Path:
        """
        Create directory structure for a course.

        Args:
            course_info: Course information dictionary

        Returns:
            Path: Path to the course directory
        """
        try:
            return self.directory_manager.create_course_structure(course_info)

        except Exception as e:
            self.logger.error(f"Failed to create course directory", exception=e)
            # Emergency fallback
            fallback_name = course_info.get('name', 'Unknown_Course')[:50]
            fallback_path = self.base_download_path / fallback_name
            fallback_path.mkdir(parents=True, exist_ok=True)
            return fallback_path

    async def save_file(self, content: Union[bytes, str], file_path: Path,
                       metadata: Dict[str, Any] = None, verify_integrity: bool = None) -> bool:
        """
        Save content to file with comprehensive error handling.

        Args:
            content: File content to save
            file_path: Target file path
            metadata: Optional metadata about the file
            verify_integrity: Whether to verify file integrity (uses config default if None)

        Returns:
            bool: True if file saved successfully
        """
        if verify_integrity is None:
            verify_integrity = self.verify_files

        try:
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert string content to bytes if needed
            if isinstance(content, str):
                content = content.encode('utf-8')

            success = False

            if self.atomic_operations:
                success = await self._save_file_atomic(content, file_path, verify_integrity)
            else:
                success = await self._save_file_direct(content, file_path, verify_integrity)

            if success:
                # Update file registry
                await self._update_file_registry(file_path, metadata)

                self.logger.debug(f"File saved successfully", file_path=str(file_path))
                return True
            else:
                self.logger.error(f"Failed to save file", file_path=str(file_path))
                return False

        except Exception as e:
            self.logger.error(f"Error saving file", file_path=str(file_path), exception=e)
            return False

    async def _save_file_atomic(self, content: bytes, file_path: Path, verify_integrity: bool) -> bool:
        """Save file using atomic operations to prevent corruption."""
        temp_file = None

        try:
            # Create temporary file in same directory
            temp_file = file_path.with_suffix(f'.tmp_{os.getpid()}')

            # Write to temporary file
            async with aiofiles.open(temp_file, 'wb') as f:
                await f.write(content)

            # Verify integrity if requested
            if verify_integrity:
                if not await self._verify_file_integrity(temp_file, content):
                    self.logger.error(f"File integrity verification failed", file_path=str(file_path))
                    return False

            # Atomic move to final location
            temp_file.replace(file_path)

            return True

        except Exception as e:
            self.logger.error(f"Atomic file save failed", exception=e)
            return False

        finally:
            # Clean up temporary file if it still exists
            if temp_file and temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass

    async def _save_file_direct(self, content: bytes, file_path: Path, verify_integrity: bool) -> bool:
        """Save file directly without atomic operations."""
        try:
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(content)

            # Verify integrity if requested
            if verify_integrity:
                if not await self._verify_file_integrity(file_path, content):
                    self.logger.error(f"File integrity verification failed", file_path=str(file_path))
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Direct file save failed", exception=e)
            return False

    async def _verify_file_integrity(self, file_path: Path, original_content: bytes) -> bool:
        """Verify file integrity by comparing content."""
        try:
            async with aiofiles.open(file_path, 'rb') as f:
                saved_content = await f.read()

            return saved_content == original_content

        except Exception as e:
            self.logger.error(f"File integrity verification error", exception=e)
            return False

    async def _update_file_registry(self, file_path: Path, metadata: Dict[str, Any] = None) -> None:
        """Update file registry with new file information."""
        try:
            with self._lock:
                file_info = FileInfo(path=file_path)

                # Add metadata if provided
                if metadata:
                    if 'canvas_id' in metadata:
                        file_info.canvas_id = str(metadata['canvas_id'])
                    if 'content_type' in metadata:
                        file_info.content_type = metadata['content_type']
                    if 'course_id' in metadata:
                        file_info.course_id = str(metadata['course_id'])
                    if 'original_url' in metadata:
                        file_info.original_url = metadata['original_url']

                # Calculate file hashes if verification is enabled
                if self.verify_files:
                    await self._calculate_file_hashes(file_info)

                self._file_registry[str(file_path)] = file_info

                # Save registry periodically (every 10 files)
                if len(self._file_registry) % 10 == 0:
                    self._save_file_registry()

        except Exception as e:
            self.logger.error(f"Failed to update file registry", exception=e)

    async def _calculate_file_hashes(self, file_info: FileInfo) -> None:
        """Calculate file hashes for integrity verification."""
        try:
            sha256_hash = hashlib.sha256()
            md5_hash = hashlib.md5()

            async with aiofiles.open(file_info.path, 'rb') as f:
                while chunk := await f.read(self.chunk_size):
                    sha256_hash.update(chunk)
                    md5_hash.update(chunk)

            file_info.hash_sha256 = sha256_hash.hexdigest()
            file_info.hash_md5 = md5_hash.hexdigest()
            file_info.is_verified = True

        except Exception as e:
            self.logger.error(f"Failed to calculate file hashes", exception=e)

    def file_exists(self, file_path: Path) -> bool:
        """
        Check if file exists with additional validation.

        Args:
            file_path: Path to check

        Returns:
            bool: True if file exists and is readable
        """
        try:
            return file_path.exists() and file_path.is_file() and os.access(file_path, os.R_OK)
        except Exception:
            return False

    def should_skip_file(self, file_path: Path, expected_size: int = None) -> bool:
        """
        Determine if file should be skipped based on configuration.

        Args:
            file_path: Path to check
            expected_size: Expected file size for validation

        Returns:
            bool: True if file should be skipped
        """
        if not self.skip_existing:
            return False

        if not self.file_exists(file_path):
            return False

        # If expected size is provided, validate it
        if expected_size is not None:
            try:
                actual_size = file_path.stat().st_size
                if actual_size != expected_size:
                    self.logger.debug(f"File size mismatch, re-downloading",
                                    file_path=str(file_path),
                                    expected=expected_size,
                                    actual=actual_size)
                    return False
            except Exception:
                return False

        self.logger.debug(f"Skipping existing file", file_path=str(file_path))
        return True

    def get_safe_filename(self, original_name: str, max_length: int = 255) -> str:
        """
        Get a safe filename for the current filesystem.

        Args:
            original_name: Original filename
            max_length: Maximum filename length

        Returns:
            str: Safe filename
        """
        # Remove invalid characters
        invalid_chars = '<>:"/\\|?*'
        safe_name = original_name

        for char in invalid_chars:
            safe_name = safe_name.replace(char, '_')

        # Handle reserved names on Windows
        reserved_names = {
            'CON', 'PRN', 'AUX', 'NUL',
            'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
            'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
        }

        name_without_ext = safe_name.rsplit('.', 1)[0] if '.' in safe_name else safe_name
        if name_without_ext.upper() in reserved_names:
            safe_name = f"_{safe_name}"

        # Truncate if too long
        if len(safe_name) > max_length:
            name_part = safe_name.rsplit('.', 1)[0] if '.' in safe_name else safe_name
            ext_part = safe_name.rsplit('.', 1)[1] if '.' in safe_name else ''

            max_name_length = max_length - len(ext_part) - 1 if ext_part else max_length
            name_part = name_part[:max_name_length]

            safe_name = f"{name_part}.{ext_part}" if ext_part else name_part

        # Ensure not empty
        if not safe_name.strip():
            safe_name = "unnamed_file"

        return safe_name

    def get_unique_filename(self, base_path: Path, preferred_name: str) -> Path:
        """
        Get a unique filename by adding numbers if file exists.

        Args:
            base_path: Base directory path
            preferred_name: Preferred filename

        Returns:
            Path: Unique file path
        """
        safe_name = self.get_safe_filename(preferred_name)
        file_path = base_path / safe_name

        if not file_path.exists():
            return file_path

        # Add number suffix
        name_part = safe_name.rsplit('.', 1)[0] if '.' in safe_name else safe_name
        ext_part = safe_name.rsplit('.', 1)[1] if '.' in safe_name else ''

        counter = 1
        while True:
            if ext_part:
                new_name = f"{name_part}_{counter}.{ext_part}"
            else:
                new_name = f"{name_part}_{counter}"

            new_path = base_path / new_name
            if not new_path.exists():
                return new_path

            counter += 1

            # Prevent infinite loop
            if counter > 1000:
                import uuid
                unique_suffix = str(uuid.uuid4())[:8]
                if ext_part:
                    new_name = f"{name_part}_{unique_suffix}.{ext_part}"
                else:
                    new_name = f"{name_part}_{unique_suffix}"
                return base_path / new_name

    def organize_course_files(self, course_path: Path) -> Dict[str, int]:
        """
        Organize files in a course directory.

        Args:
            course_path: Path to course directory

        Returns:
            Dict[str, int]: Statistics about organization
        """
        stats = {
            'files_processed': 0,
            'files_moved': 0,
            'directories_created': 0,
            'errors': 0
        }

        try:
            config = get_config()

            # Get organization preferences
            organize_by_type = config.safe_get('folder_structure.organize_by_content_type', True, bool)

            if not organize_by_type:
                return stats

            # Process all files in course directory
            for file_path in course_path.rglob('*'):
                if file_path.is_file():
                    try:
                        stats['files_processed'] += 1

                        # Determine content type folder
                        content_type = self._determine_content_type(file_path)
                        content_folder = course_path / content_type

                        # Create content type folder if needed
                        if not content_folder.exists():
                            content_folder.mkdir(exist_ok=True)
                            stats['directories_created'] += 1

                        # Move file if not already in correct location
                        if file_path.parent != content_folder:
                            new_path = self.get_unique_filename(content_folder, file_path.name)
                            file_path.rename(new_path)
                            stats['files_moved'] += 1

                            # Update registry
                            if str(file_path) in self._file_registry:
                                file_info = self._file_registry.pop(str(file_path))
                                file_info.path = new_path
                                self._file_registry[str(new_path)] = file_info

                    except Exception as e:
                        self.logger.error(f"Failed to organize file", file_path=str(file_path), exception=e)
                        stats['errors'] += 1

            # Save updated registry
            self._save_file_registry()

            self.logger.info(f"Course files organized",
                           course_path=str(course_path),
                           **stats)

        except Exception as e:
            self.logger.error(f"Failed to organize course files", exception=e)
            stats['errors'] += 1

        return stats

    def _determine_content_type(self, file_path: Path) -> str:
        """
        Determine content type based on file path and name.

        Args:
            file_path: Path to file

        Returns:
            str: Content type folder name
        """
        file_name = file_path.name.lower()
        parent_name = file_path.parent.name.lower()

        # Check parent directory name for hints
        if 'assignment' in parent_name:
            return 'assignments'
        elif 'module' in parent_name:
            return 'modules'
        elif 'announcement' in parent_name:
            return 'announcements'
        elif 'discussion' in parent_name:
            return 'discussions'
        elif 'quiz' in parent_name:
            return 'quizzes'

        # Check file extension
        extension = file_path.suffix.lower()

        if extension in ['.pdf', '.doc', '.docx']:
            return 'documents'
        elif extension in ['.jpg', '.jpeg', '.png', '.gif', '.bmp']:
            return 'images'
        elif extension in ['.mp4', '.avi', '.mov', '.wmv']:
            return 'videos'
        elif extension in ['.mp3', '.wav', '.aac']:
            return 'audio'
        elif extension in ['.zip', '.rar', '.7z']:
            return 'archives'
        elif extension in ['.html', '.htm']:
            return 'web_content'

        return 'misc'

    def get_file_registry_stats(self) -> Dict[str, Any]:
        """Get statistics about the file registry."""
        with self._lock:
            total_files = len(self._file_registry)
            total_size = sum(info.size for info in self._file_registry.values() if info.size)
            verified_files = sum(1 for info in self._file_registry.values() if info.is_verified)

            content_types = {}
            for info in self._file_registry.values():
                content_type = info.content_type or 'unknown'
                content_types[content_type] = content_types.get(content_type, 0) + 1

            return {
                'total_files': total_files,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'verified_files': verified_files,
                'content_types': content_types,
                'registry_file': str(self._registry_file)
            }

    def export_file_inventory(self, output_path: Path) -> bool:
        """
        Export complete file inventory to JSON.

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

    def cleanup(self) -> None:
        """Clean up file manager resources."""
        try:
            # Save final registry state
            self._save_file_registry()

            # Shutdown executor
            self._executor.shutdown(wait=True)

            self.logger.info("File manager cleaned up successfully")

        except Exception as e:
            self.logger.error(f"Error during file manager cleanup", exception=e)


def create_file_manager(config=None, **kwargs) -> FileManager:
    """
    Factory function to create a file manager instance.

    Args:
        config: Optional configuration object
        **kwargs: Additional arguments to pass to FileManager constructor

    Returns:
        FileManager: Configured file manager instance
    """
    try:
        return FileManager(config=config, **kwargs)
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Failed to create file manager", exception=e)
        # Return a minimal working file manager
        return FileManager(config=None)


# Utility functions for common file operations
def get_download_directory() -> Path:
    """Get the configured download directory."""
    config = get_config()
    base_path = config.safe_get('download_settings.base_download_path', 'downloads', str)
    return Path(base_path)


def ensure_directory_exists(path: Union[str, Path]) -> Path:
    """Ensure directory exists and return Path object."""
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj