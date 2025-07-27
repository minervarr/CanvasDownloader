"""
Main Orchestrator Module

This module provides the central orchestration system for the Canvas Downloader
application. It coordinates all components including course selection, content
downloading, progress tracking, and error handling.

The Orchestrator manages:
- Canvas API client initialization and authentication
- Course selection and filtering
- Content type configuration and selection
- Download process coordination across multiple courses
- Progress tracking and reporting
- Error handling and recovery
- Resource management and cleanup
- Download statistics and reporting

Features:
- Parallel course processing with configurable concurrency
- Intelligent retry mechanisms for failed downloads
- Comprehensive error reporting and logging
- Resource-aware download management
- Real-time progress tracking and UI updates
- Flexible content filtering and selection
- Download resumption and incremental updates

Usage:
    # Initialize orchestrator
    orchestrator = CanvasOrchestrator()

    # Set up Canvas session
    await orchestrator.initialize_session("session_name")

    # Configure download options
    orchestrator.configure_content_types(['assignments', 'announcements', 'files'])

    # Start download process
    results = await orchestrator.download_courses(selected_course_ids)
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set, Tuple
from dataclasses import dataclass, field
import traceback

import aiofiles

from ..api.client import CanvasAPIClient, create_canvas_client
from ..config.sessions import SessionManager, get_session_manager
from ..config.settings import get_config
from ..core.course_parser import CourseParser, create_course_parser
from ..core.file_manager import FileManager, create_file_manager
from ..utils.progress import ProgressTracker, create_progress_tracker
from ..utils.logger import get_logger
from ..downloaders.base import ContentDownloaderFactory
from ..utils.json_utils import DateTimeJSONEncoder

# Import all downloader modules to trigger their registration
from ..downloaders import (
    assignments,
    announcements,
    discussions,
    files,
    modules,
    # Add other downloaders as they become available
)

@dataclass
class DownloadResults:
    """
    Data class representing the results of a download operation.

    This class contains comprehensive information about the download
    process including statistics, errors, and file information.
    """
    # Overall statistics
    total_courses: int = 0
    processed_courses: int = 0
    successful_courses: int = 0
    failed_courses: int = 0

    # Content statistics
    total_content_items: int = 0
    downloaded_items: int = 0
    skipped_items: int = 0
    failed_items: int = 0

    # File statistics
    total_files_downloaded: int = 0
    total_bytes_downloaded: int = 0

    # Timing information
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    # Course-specific results
    course_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Error tracking
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)

    # Download paths and metadata
    download_paths: List[str] = field(default_factory=list)
    metadata_files: List[str] = field(default_factory=list)

    @property
    def duration(self) -> float:
        """Get total download duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now() - self.start_time).total_seconds()

    @property
    def success_rate(self) -> float:
        """Get success rate as percentage."""
        if self.processed_courses == 0:
            return 0.0
        return (self.successful_courses / self.processed_courses) * 100

    @property
    def download_speed_mb_per_sec(self) -> float:
        """Get download speed in MB/s."""
        duration = self.duration
        if duration == 0:
            return 0.0
        return (self.total_bytes_downloaded / (1024 * 1024)) / duration

    def to_dict(self) -> Dict[str, Any]:
        """Convert results to dictionary for serialization."""
        return {
            'summary': {
                'total_courses': self.total_courses,
                'processed_courses': self.processed_courses,
                'successful_courses': self.successful_courses,
                'failed_courses': self.failed_courses,
                'success_rate_percent': round(self.success_rate, 2)
            },
            'content_statistics': {
                'total_content_items': self.total_content_items,
                'downloaded_items': self.downloaded_items,
                'skipped_items': self.skipped_items,
                'failed_items': self.failed_items
            },
            'file_statistics': {
                'total_files_downloaded': self.total_files_downloaded,
                'total_bytes_downloaded': self.total_bytes_downloaded,
                'total_mb_downloaded': round(self.total_bytes_downloaded / (1024 * 1024), 2),
                'download_speed_mb_per_sec': round(self.download_speed_mb_per_sec, 2)
            },
            'timing': {
                'start_time': self.start_time.isoformat(),
                'end_time': self.end_time.isoformat() if self.end_time else None,
                'duration_seconds': round(self.duration, 2),
                'duration_formatted': self._format_duration(self.duration)
            },
            'course_results': self.course_results,
            'errors': self.errors,
            'warnings': self.warnings,
            'download_paths': self.download_paths,
            'metadata_files': self.metadata_files
        }

    def _format_duration(self, duration_seconds: float) -> str:
        """Format duration in human-readable format."""
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        seconds = int(duration_seconds % 60)

        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"


class CanvasOrchestrator:
    """
    Canvas Downloader Main Orchestrator

    This class provides the central coordination system for downloading content
    from Canvas courses. It manages the entire download process from authentication
    to completion, coordinating multiple downloaders and providing comprehensive
    progress tracking and error handling.

    The orchestrator ensures:
    - Proper resource management and cleanup
    - Coordinated progress tracking across all operations
    - Comprehensive error handling and recovery
    - Efficient parallel processing of multiple courses
    - Consistent data organization and metadata management
    """

    def __init__(self, config=None, progress_tracker=None):
        """
        Initialize the Canvas Orchestrator.

        Args:
            config: Optional configuration object. If None, uses global config.
            progress_tracker: Optional progress tracker. If None, creates default.
        """
        self.config = config or get_config()
        self.logger = get_logger(__name__)

        # Core components
        self.session_manager = get_session_manager()
        self.course_parser = create_course_parser(self.config)
        self.file_manager = create_file_manager(self.config)
        self.progress_tracker = progress_tracker or create_progress_tracker()

        # Canvas API client (initialized during session setup)
        self.canvas_client: Optional[CanvasAPIClient] = None
        self.current_session_name: Optional[str] = None

        # Download configuration
        self.enabled_content_types: Set[str] = set()
        self.selected_courses: List[str] = []
        self.download_options = {}

        # Processing state
        self._is_downloading = False
        self._download_task: Optional[asyncio.Task] = None
        self._should_stop = False

        # Results tracking
        self.current_results: Optional[DownloadResults] = None

        # Initialize with default content types
        self._initialize_default_content_types()

        self.logger.info("Canvas Orchestrator initialized")

    def _initialize_default_content_types(self):
        """Initialize with default enabled content types from config."""
        # ✅ One line that handles everything safely
        for content_type, priority in self.config.get_enabled_content_types():
            self.enabled_content_types.add(content_type)

    async def initialize_session(self, session_name: str) -> bool:
        """
        Initialize Canvas session with the specified credentials.

        Args:
            session_name: Name of the saved session to load

        Returns:
            bool: True if session initialized successfully
        """
        try:
            self.logger.info(f"Initializing Canvas session: {session_name}")

            # Load session credentials
            credentials = self.session_manager.select_session(session_name)
            if not credentials:
                self.logger.error(f"Failed to load session credentials")
                return False

            # Create Canvas API client
            self.canvas_client = create_canvas_client(
                api_url=credentials['api_url'],
                api_key=credentials['api_key'],
                timeout=self.config.download_settings.timeout,
                max_retries=self.config.safe_get('download_settings.max_retries', 3, int)
            )

            # Test connection
            success, message, user_info = self.canvas_client.test_connection()
            if not success:
                self.logger.error(f"Canvas connection test failed: {message}")
                return False

            self.current_session_name = session_name
            self.logger.info(f"Canvas session initialized successfully",
                             session_name=session_name,
                             user_name=user_info.get('name', 'Unknown'),
                             api_url=credentials['api_url'])

            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize Canvas session",
                              session_name=session_name, exception=e)
            return False

    def configure_content_types(self, content_types: List[str]):
        """
        Configure which content types should be downloaded.

        Args:
            content_types: List of content type names to enable
        """
        self.enabled_content_types = set(content_types)
        self.logger.info(f"Configured content types",
                         enabled_types=list(self.enabled_content_types))

    def configure_download_options(self, **options):
        """
        Configure additional download options.

        Args:
            **options: Download option key-value pairs
        """
        self.download_options.update(options)
        self.logger.info(f"Configured download options", options=options)

    async def get_available_courses(self) -> List[Dict[str, Any]]:
        """
        Get list of available courses for the current user.

        Returns:
            List[Dict[str, Any]]: List of course information dictionaries
        """
        if not self.canvas_client:
            raise RuntimeError("Canvas session not initialized")

        try:
            self.logger.info("Fetching available courses")

            # Get courses from Canvas
            courses = self.canvas_client.get_user_courses(
                include_concluded=True,
                enrollment_state="active"
            )

            # Parse course names and add organizational information
            parsed_courses = []
            for course in courses:
                try:
                    # Parse course name
                    parsed_course = self.course_parser.parse_course_name(
                        course['name'],
                        str(course['id'])
                    )

                    # Add Canvas course information
                    course_info = {
                        'id': course['id'],
                        'name': course['name'],
                        'course_code': course.get('course_code', ''),
                        'workflow_state': course.get('workflow_state', ''),
                        'start_at': course.get('start_at', ''),
                        'end_at': course.get('end_at', ''),
                        'enrollment_term_id': course.get('enrollment_term_id'),
                        'parsed': parsed_course,
                        'folder_path': parsed_course.folder_path if parsed_course.is_parsed_successfully else course[
                            'name']
                    }

                    parsed_courses.append(course_info)

                except Exception as e:
                    self.logger.warning(f"Failed to parse course name",
                                        course_id=course['id'],
                                        course_name=course['name'],
                                        exception=e)

                    # Add course with minimal parsing
                    course_info = {
                        'id': course['id'],
                        'name': course['name'],
                        'course_code': course.get('course_code', ''),
                        'workflow_state': course.get('workflow_state', ''),
                        'parsed': None,
                        'folder_path': course['name']
                    }
                    parsed_courses.append(course_info)

            # Sort courses by semester and name
            parsed_courses.sort(key=lambda x: (
                x.get('parsed').year if x.get('parsed') and x.get('parsed').is_parsed_successfully else '9999',
                x.get('parsed').semester if x.get('parsed') and x.get('parsed').is_parsed_successfully else '9',
                x['name']
            ))

            self.logger.info(f"Found {len(parsed_courses)} available courses")
            return parsed_courses

        except Exception as e:
            self.logger.error(f"Failed to get available courses", exception=e)
            raise

    async def check_course_content_availability(self, course_ids: List[str]) -> Dict[str, Dict[str, bool]]:
        """
        Check content availability for selected courses.

        Args:
            course_ids: List of course IDs to check

        Returns:
            Dict[str, Dict[str, bool]]: Content availability by course
        """
        if not self.canvas_client:
            raise RuntimeError("Canvas session not initialized")

        availability = {}

        for course_id in course_ids:
            try:
                content_availability = self.canvas_client.check_content_availability(course_id)
                availability[course_id] = content_availability

            except Exception as e:
                self.logger.warning(f"Failed to check content availability",
                                    course_id=course_id, exception=e)
                # Default to assuming content is available
                availability[course_id] = {
                    'announcements': True,
                    'assignments': True,
                    'discussions': True,
                    'files': True,
                    'modules': True,
                    'quizzes': True,
                    'grades': True,
                    'people': True,
                    'pages': True
                }

        return availability

    async def download_courses(self, course_ids: List[str],
                               progress_callback=None) -> DownloadResults:
        """
        Download content from selected courses.

        Args:
            course_ids: List of course IDs to download
            progress_callback: Optional callback for progress updates

        Returns:
            DownloadResults: Comprehensive download results
        """
        if not self.canvas_client:
            raise RuntimeError("Canvas session not initialized")

        if self._is_downloading:
            raise RuntimeError("Download already in progress")

        try:
            self._is_downloading = True
            self._should_stop = False

            # Initialize results
            self.current_results = DownloadResults()
            self.current_results.total_courses = len(course_ids)
            self.current_results.start_time = datetime.now()

            # Set up progress tracking
            self.progress_tracker.set_total_courses(len(course_ids))

            if progress_callback:
                self.progress_tracker.add_progress_callback(progress_callback)

            self.logger.info(f"Starting download for {len(course_ids)} courses",
                             course_ids=course_ids,
                             enabled_content_types=list(self.enabled_content_types))

            # Process each course
            for course_id in course_ids:
                if self._should_stop:
                    self.logger.info("Download stopped by user request")
                    break

                try:
                    await self._download_single_course(course_id)
                    self.current_results.successful_courses += 1

                except Exception as e:
                    self.logger.error(f"Failed to download course",
                                      course_id=course_id,
                                      exception=e)

                    # Record error
                    error_info = {
                        'course_id': course_id,
                        'error_type': type(e).__name__,
                        'error_message': str(e),
                        'timestamp': datetime.now().isoformat(),
                        'traceback': traceback.format_exc()
                    }
                    self.current_results.errors.append(error_info)
                    self.current_results.failed_courses += 1

                finally:
                    self.current_results.processed_courses += 1
                    self.progress_tracker.update_application_progress()

            # Finalize results
            self.current_results.end_time = datetime.now()

            # Save download summary
            await self._save_download_summary()

            self.logger.info(f"Download completed",
                             successful_courses=self.current_results.successful_courses,
                             failed_courses=self.current_results.failed_courses,
                             duration=self.current_results.duration)

            return self.current_results

        except Exception as e:
            self.logger.error(f"Download process failed", exception=e)

            if self.current_results:
                self.current_results.end_time = datetime.now()
                error_info = {
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'timestamp': datetime.now().isoformat(),
                    'traceback': traceback.format_exc()
                }
                self.current_results.errors.append(error_info)

            raise

        finally:
            self._is_downloading = False

            if progress_callback:
                self.progress_tracker.remove_progress_callback(progress_callback)

    async def _download_single_course(self, course_id: str):
        """
        Download content from a single course.

        Args:
            course_id: Course ID to download
        """
        try:
            # Get course information
            course = self.canvas_client.get_course(course_id)
            course_info_raw = self.canvas_client.get_course_info(course_id)

            # Parse course name
            parsed_course = self.course_parser.parse_course_name(
                course_info_raw['name'],
                course_id
            )

            # Create course info dictionary
            course_info = {
                'id': course_id,
                'name': course_info_raw['name'],
                'full_name': course_info_raw['name'],
                'subject_name': parsed_course.subject_name,
                'subject_code': parsed_course.subject_code,
                'subsection': parsed_course.subsection,
                'year': parsed_course.year,
                'semester': parsed_course.semester,
                'folder_name': parsed_course.folder_name,
                'folder_path': parsed_course.folder_path,
                'workflow_state': course_info_raw.get('workflow_state', ''),
                'parsed_successfully': parsed_course.is_parsed_successfully
            }

            self.logger.info(f"Starting course download",
                             course_id=course_id,
                             course_name=course_info['name'],
                             folder_path=course_info['folder_path'])

            # Start course progress tracking
            enabled_types = [ct for ct in self.enabled_content_types
                             if self.config.is_content_type_enabled(ct)]

            self.progress_tracker.start_course(
                course_info['name'],
                course_id
            )
            # Set total content types separately
            self.progress_tracker.set_total_content_types(len(enabled_types))

            # Initialize course results
            course_results = {
                'course_info': course_info,
                'content_results': {},
                'start_time': datetime.now().isoformat(),
                'end_time': None,
                'success': False,
                'errors': [],
                'warnings': []
            }

            # Download each content type
            total_items = 0

            for content_type in enabled_types:
                if self._should_stop:
                    break

                try:
                    self.logger.info(f"Downloading {content_type}",
                                     course_id=course_id,
                                     content_type=content_type)

                    # Start content type progress
                    self.progress_tracker.start_content_type(content_type, 0)

                    # Get appropriate downloader
                    downloader = ContentDownloaderFactory.create_downloader(
                        content_type,
                        self.canvas_client,
                        self.progress_tracker
                    )

                    # Download content
                    content_stats = await downloader.download_course_content(course, course_info)

                    # Record results
                    course_results['content_results'][content_type] = content_stats

                    # Update overall statistics
                    total_items += content_stats.get('total_items', 0)
                    self.current_results.total_content_items += content_stats.get('total_items', 0)
                    self.current_results.downloaded_items += content_stats.get('downloaded_items', 0)
                    self.current_results.skipped_items += content_stats.get('skipped_items', 0)
                    self.current_results.failed_items += content_stats.get('failed_items', 0)
                    self.current_results.total_bytes_downloaded += content_stats.get('total_size_bytes', 0)

                    if content_type == 'files':
                        self.current_results.total_files_downloaded += content_stats.get('downloaded_items', 0)

                    # Complete content type
                    self.progress_tracker.complete_content_type(content_type)

                    self.logger.info(f"Completed {content_type} download",
                                     course_id=course_id,
                                     content_type=content_type,
                                     **content_stats)

                except Exception as e:
                    self.logger.error(f"Failed to download {content_type}",
                                      course_id=course_id,
                                      content_type=content_type,
                                      exception=e)

                    # Record content type error
                    error_info = {
                        'content_type': content_type,
                        'error_type': type(e).__name__,
                        'error_message': str(e),
                        'timestamp': datetime.now().isoformat()
                    }
                    course_results['errors'].append(error_info)

                    # Still complete the content type to maintain progress
                    self.progress_tracker.complete_content_type(content_type)

            # Finalize course results
            course_results['end_time'] = datetime.now().isoformat()
            course_results['success'] = len(course_results['errors']) == 0
            course_results['total_items'] = total_items

            # Store course results
            self.current_results.course_results[course_id] = course_results

            # Complete course progress
            self.progress_tracker.complete_course(course_id)

            # Record download path
            if parsed_course.is_parsed_successfully:
                course_folder_path = self.file_manager.base_download_path / parsed_course.folder_path
                self.current_results.download_paths.append(str(course_folder_path))

        except Exception as e:
            self.logger.error(f"Failed to process course {course_id}", exception=e)
            raise

    async def _save_download_summary(self):
        """Save comprehensive download summary."""
        try:
            if not self.current_results:
                return

            # Create summary document
            summary = {
                'session_info': {
                    'session_name': self.current_session_name,
                    'canvas_url': getattr(self.canvas_client, 'api_url', ''),
                    'user_info': self.canvas_client.get_current_user() if self.canvas_client else {}
                },
                'download_configuration': {
                    'enabled_content_types': list(self.enabled_content_types),
                    'download_options': self.download_options,
                    'parallel_downloads': self.config.safe_get('download_settings.parallel_downloads', 4, int),
                    'skip_existing': self.config.safe_get('download_settings.skip_existing', True, bool)
                },
                'results': self.current_results.to_dict(),
                'generated_at': datetime.now().isoformat(),
                'version': '1.0'
            }

            # Save to base download directory
            summary_path = self.file_manager.base_download_path / 'download_summary.json'

            async with aiofiles.open(summary_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(summary, indent=2, ensure_ascii=False, cls=DateTimeJSONEncoder))

            self.current_results.metadata_files.append(str(summary_path))

            self.logger.info(f"Saved download summary",
                             summary_path=str(summary_path))

        except Exception as e:
            self.logger.error(f"Failed to save download summary", exception=e)

    def stop_download(self):
        """Request download process to stop gracefully."""
        self._should_stop = True
        self.logger.info("Download stop requested")

    def is_downloading(self) -> bool:
        """Check if download is currently in progress."""
        return self._is_downloading

    def get_current_progress(self) -> Dict[str, Any]:
        """Get current download progress information."""
        if not self.current_results:
            return {}

        progress_info = {
            'is_downloading': self._is_downloading,
            'overall_progress': self.progress_tracker.get_overall_statistics(),
            'current_results': self.current_results.to_dict(),
            'enabled_content_types': list(self.enabled_content_types)
        }

        return progress_info

    def get_download_statistics(self) -> Optional[Dict[str, Any]]:
        """Get comprehensive download statistics."""
        if not self.current_results:
            return None

        stats = self.current_results.to_dict()

        # Add parser statistics
        parser_stats = self.course_parser.get_parsing_statistics()
        stats['parsing_statistics'] = parser_stats

        # Add file manager statistics
        file_stats = self.file_manager.get_file_registry_stats()
        stats['file_management_statistics'] = file_stats

        return stats

    async def cleanup(self):
        """Clean up orchestrator resources."""
        try:
            # Stop any ongoing downloads
            if self._is_downloading:
                self.stop_download()

                # Wait for download task to complete
                if self._download_task and not self._download_task.done():
                    try:
                        await asyncio.wait_for(self._download_task, timeout=10.0)
                    except asyncio.TimeoutError:
                        self._download_task.cancel()

            # Clean up components
            if self.progress_tracker:
                self.progress_tracker.cleanup()

            if self.file_manager:
                self.file_manager.cleanup()

            # Clear Canvas client
            self.canvas_client = None
            self.current_session_name = None

            self.logger.info("Canvas Orchestrator cleaned up")

        except Exception as e:
            self.logger.error(f"Error during orchestrator cleanup", exception=e)


def create_orchestrator(**kwargs) -> CanvasOrchestrator:
    """
    Factory function to create a Canvas Orchestrator instance.

    Args:
        **kwargs: Arguments to pass to CanvasOrchestrator constructor

    Returns:
        CanvasOrchestrator: Configured orchestrator instance
    """
    return CanvasOrchestrator(**kwargs)