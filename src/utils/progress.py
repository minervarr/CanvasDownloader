"""
Progress Tracking System Module

This module provides comprehensive progress tracking functionality for the Canvas
Downloader application. It manages progress reporting across multiple levels:
- Overall application progress
- Course-level progress
- Content type progress (assignments, announcements, etc.)
- Individual file download progress

The system supports both console-based progress bars and potential GUI integration,
with thread-safe operations for parallel downloads.

Features:
- Multi-level progress tracking (application -> course -> content -> file)
- Thread-safe progress updates for parallel downloads
- Configurable progress display (console, file, or custom callbacks)
- Detailed progress statistics and timing information
- Integration with the logging system
- Memory-efficient progress state management

Usage:
    # Initialize progress tracker
    tracker = ProgressTracker()

    # Set up overall progress
    tracker.set_total_courses(5)

    # Start processing a course
    tracker.start_course("Math 101", course_id="12345")
    tracker.set_total_content_types(8)  # assignments, announcements, etc.

    # Start downloading a content type
    tracker.start_content_type("assignments", total_items=25)

    # Update individual item progress
    for i in range(25):
        tracker.update_item_progress(i + 1)

    # Complete content type
    tracker.complete_content_type("assignments")
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timedelta
from enum import Enum
import sys

try:
    from tqdm import tqdm
    from rich.console import Console
    from rich.progress import (
        Progress, TaskID, SpinnerColumn, TextColumn,
        BarColumn, TimeElapsedColumn, TimeRemainingColumn,
        MofNCompleteColumn, FileSizeColumn, TransferSpeedColumn
    )
    from rich.table import Table
    from rich.live import Live
    from rich.panel import Panel
    from rich.text import Text

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Rich library not available. Progress will use basic console output.")

from ..utils.logger import get_logger


class ProgressLevel(Enum):
    """Enumeration of different progress tracking levels."""
    APPLICATION = "application"
    COURSE = "course"
    CONTENT_TYPE = "content_type"
    ITEM = "item"
    DOWNLOAD = "download"


@dataclass
class ProgressState:
    """
    Data class representing the state of a progress operation.

    This class maintains all the information needed to track progress
    at any level of the application hierarchy.
    """
    # Basic progress information
    current: int = 0
    total: int = 0
    completed: bool = False

    # Timing information
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    # Descriptive information
    name: str = ""
    description: str = ""

    # Statistics
    success_count: int = 0
    error_count: int = 0
    skip_count: int = 0

    # Download-specific information
    bytes_downloaded: int = 0
    total_bytes: Optional[int] = None

    @property
    def percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total == 0:
            return 0.0
        return (self.current / self.total) * 100

    @property
    def elapsed_time(self) -> timedelta:
        """Calculate elapsed time."""
        end_time = self.end_time or datetime.now()
        return end_time - self.start_time

    @property
    def estimated_total_time(self) -> Optional[timedelta]:
        """Estimate total time based on current progress."""
        if self.current == 0 or self.total == 0:
            return None

        elapsed = self.elapsed_time
        estimated_total_seconds = (elapsed.total_seconds() / self.current) * self.total
        return timedelta(seconds=estimated_total_seconds)

    @property
    def eta(self) -> Optional[timedelta]:
        """Calculate estimated time to completion."""
        estimated_total = self.estimated_total_time
        if estimated_total is None:
            return None

        elapsed = self.elapsed_time
        return estimated_total - elapsed

    @property
    def speed(self) -> Optional[float]:
        """Calculate processing speed (items per second)."""
        elapsed_seconds = self.elapsed_time.total_seconds()
        if elapsed_seconds == 0:
            return None
        return self.current / elapsed_seconds

    @property
    def download_speed(self) -> Optional[float]:
        """Calculate download speed (bytes per second)."""
        elapsed_seconds = self.elapsed_time.total_seconds()
        if elapsed_seconds == 0 or self.bytes_downloaded == 0:
            return None
        return self.bytes_downloaded / elapsed_seconds


class ProgressTracker:
    """
    Multi-Level Progress Tracker

    This class provides comprehensive progress tracking for the Canvas Downloader
    application. It manages progress at multiple hierarchical levels and provides
    thread-safe updates for parallel operations.

    The tracker maintains state for:
    - Overall application progress (multiple courses)
    - Individual course progress (multiple content types)
    - Content type progress (multiple items)
    - Individual item progress (file downloads)

    Progress can be displayed using Rich library (if available) or fallback
    to basic console output.
    """

    def __init__(self, use_rich: bool = None, console_output: bool = True):
        """
        Initialize the progress tracker.

        Args:
            use_rich: Whether to use Rich library for enhanced display.
                     If None, auto-detect based on availability.
            console_output: Whether to display progress to console
        """
        self.use_rich = use_rich if use_rich is not None else RICH_AVAILABLE
        self.console_output = console_output
        self.logger = get_logger(__name__)

        # Thread safety
        self._lock = threading.RLock()

        # Progress state storage
        self._application_progress = ProgressState(name="Canvas Downloader")
        self._course_progress: Dict[str, ProgressState] = {}
        self._content_progress: Dict[str, ProgressState] = {}
        self._item_progress: Dict[str, ProgressState] = {}

        # Current operation context
        self._current_course_id: Optional[str] = None
        self._current_content_type: Optional[str] = None

        # Display components
        self._console = Console() if self.use_rich else None
        self._progress_display = None
        self._live_display = None

        # Callback functions for custom progress handling
        self._progress_callbacks: List[Callable] = []

        # Statistics
        self._start_time = datetime.now()
        self._statistics = {
            'total_files_downloaded': 0,
            'total_bytes_downloaded': 0,
            'total_errors': 0,
            'total_skipped': 0
        }

        self.logger.info("Progress tracker initialized",
                         use_rich=self.use_rich,
                         console_output=console_output)

    def add_progress_callback(self, callback: Callable[[str, ProgressState], None]):
        """
        Add a callback function to receive progress updates.

        Args:
            callback: Function that takes (level, progress_state) parameters
        """
        with self._lock:
            self._progress_callbacks.append(callback)

    def remove_progress_callback(self, callback: Callable):
        """Remove a progress callback function."""
        with self._lock:
            if callback in self._progress_callbacks:
                self._progress_callbacks.remove(callback)

    def _notify_callbacks(self, level: str, progress_state: ProgressState):
        """Notify all registered callbacks of progress updates."""
        for callback in self._progress_callbacks:
            try:
                callback(level, progress_state)
            except Exception as e:
                self.logger.error(f"Progress callback error: {e}")

    # =============================================================================
    # Application-Level Progress Methods
    # =============================================================================

    def set_total_courses(self, total: int):
        """
        Set the total number of courses to process.

        Args:
            total: Total number of courses
        """
        with self._lock:
            self._application_progress.total = total
            self._application_progress.current = 0
            self._application_progress.start_time = datetime.now()

            self.logger.info(f"Starting application progress tracking for {total} courses")
            self._notify_callbacks(ProgressLevel.APPLICATION.value, self._application_progress)

            if self.console_output:
                self._initialize_display()

    def update_application_progress(self, increment: int = 1):
        """
        Update application-level progress.

        Args:
            increment: Number to increment progress by
        """
        with self._lock:
            self._application_progress.current = min(
                self._application_progress.current + increment,
                self._application_progress.total
            )

            if self._application_progress.current >= self._application_progress.total:
                self._application_progress.completed = True
                self._application_progress.end_time = datetime.now()

            self._notify_callbacks(ProgressLevel.APPLICATION.value, self._application_progress)
            self._update_display()

    # =============================================================================
    # Course-Level Progress Methods
    # =============================================================================

    def start_course(self, course_name: str, course_id: str, total_content_types: int = 0):
        """
        Start tracking progress for a course.

        Args:
            course_name: Human-readable course name
            course_id: Unique course identifier
            total_content_types: Number of content types to download
        """
        with self._lock:
            self._current_course_id = course_id

            course_progress = ProgressState(
                name=course_name,
                description=f"Course ID: {course_id}",
                total=total_content_types,
                start_time=datetime.now()
            )

            self._course_progress[course_id] = course_progress

            self.logger.info(f"Started course progress tracking",
                             course_name=course_name,
                             course_id=course_id,
                             total_content_types=total_content_types)

            self._notify_callbacks(ProgressLevel.COURSE.value, course_progress)
            self._update_display()

    def set_total_content_types(self, total: int):
        """
        Set the total number of content types for the current course.

        Args:
            total: Total number of content types
        """
        with self._lock:
            if self._current_course_id and self._current_course_id in self._course_progress:
                self._course_progress[self._current_course_id].total = total
                self._notify_callbacks(ProgressLevel.COURSE.value,
                                       self._course_progress[self._current_course_id])
                self._update_display()

    def complete_course(self, course_id: str = None):
        """
        Mark a course as completed.

        Args:
            course_id: Course ID to complete. If None, uses current course.
        """
        with self._lock:
            course_id = course_id or self._current_course_id

            if course_id and course_id in self._course_progress:
                course_progress = self._course_progress[course_id]
                course_progress.completed = True
                course_progress.end_time = datetime.now()
                course_progress.current = course_progress.total

                self.logger.info(f"Completed course progress tracking",
                                 course_id=course_id,
                                 elapsed_time=course_progress.elapsed_time)

                self._notify_callbacks(ProgressLevel.COURSE.value, course_progress)
                self.update_application_progress()

            if course_id == self._current_course_id:
                self._current_course_id = None

    # =============================================================================
    # Content Type Progress Methods
    # =============================================================================

    def start_content_type(self, content_type: str, total_items: int):
        """
        Start tracking progress for a content type (e.g., assignments, announcements).

        Args:
            content_type: Name of the content type
            total_items: Total number of items to process
        """
        with self._lock:
            self._current_content_type = content_type

            progress_key = f"{self._current_course_id}:{content_type}"
            content_progress = ProgressState(
                name=content_type.title(),
                description=f"Downloading {content_type}",
                total=total_items,
                start_time=datetime.now()
            )

            self._content_progress[progress_key] = content_progress

            self.logger.info(f"Started content type progress tracking",
                             content_type=content_type,
                             total_items=total_items,
                             course_id=self._current_course_id)

            self._notify_callbacks(ProgressLevel.CONTENT_TYPE.value, content_progress)
            self._update_display()

    def update_item_progress(self, current_item: int):
        """
        Update progress for the current content type.

        Args:
            current_item: Current item number being processed
        """
        with self._lock:
            if not self._current_content_type or not self._current_course_id:
                return

            progress_key = f"{self._current_course_id}:{self._current_content_type}"

            if progress_key in self._content_progress:
                content_progress = self._content_progress[progress_key]
                content_progress.current = min(current_item, content_progress.total)

                self._notify_callbacks(ProgressLevel.CONTENT_TYPE.value, content_progress)
                self._update_display()

    def update_content_statistics(self, success: int = 0, error: int = 0, skip: int = 0):
        """
        Update statistics for the current content type.

        Args:
            success: Number of successful operations
            error: Number of errors
            skip: Number of skipped operations
        """
        with self._lock:
            if not self._current_content_type or not self._current_course_id:
                return

            progress_key = f"{self._current_course_id}:{self._current_content_type}"

            if progress_key in self._content_progress:
                content_progress = self._content_progress[progress_key]
                content_progress.success_count += success
                content_progress.error_count += error
                content_progress.skip_count += skip

                # Update global statistics
                self._statistics['total_errors'] += error
                self._statistics['total_skipped'] += skip

                self._notify_callbacks(ProgressLevel.CONTENT_TYPE.value, content_progress)

    def complete_content_type(self, content_type: str = None):
        """
        Mark a content type as completed.

        Args:
            content_type: Content type to complete. If None, uses current content type.
        """
        with self._lock:
            content_type = content_type or self._current_content_type

            if content_type and self._current_course_id:
                progress_key = f"{self._current_course_id}:{content_type}"

                if progress_key in self._content_progress:
                    content_progress = self._content_progress[progress_key]
                    content_progress.completed = True
                    content_progress.end_time = datetime.now()
                    content_progress.current = content_progress.total

                    self.logger.info(f"Completed content type progress tracking",
                                     content_type=content_type,
                                     course_id=self._current_course_id,
                                     elapsed_time=content_progress.elapsed_time,
                                     success_count=content_progress.success_count,
                                     error_count=content_progress.error_count,
                                     skip_count=content_progress.skip_count)

                    self._notify_callbacks(ProgressLevel.CONTENT_TYPE.value, content_progress)

                    # Update course progress
                    if self._current_course_id in self._course_progress:
                        self._course_progress[self._current_course_id].current += 1
                        self._notify_callbacks(ProgressLevel.COURSE.value,
                                               self._course_progress[self._current_course_id])

            if content_type == self._current_content_type:
                self._current_content_type = None

    # =============================================================================
    # Download Progress Methods
    # =============================================================================

    def update_download_progress(self, bytes_downloaded: int, total_bytes: int = None):
        """
        Update download progress for individual files.

        Args:
            bytes_downloaded: Number of bytes downloaded so far
            total_bytes: Total bytes to download (if known)
        """
        with self._lock:
            # Update current content progress
            if (self._current_content_type and self._current_course_id and
                    f"{self._current_course_id}:{self._current_content_type}" in self._content_progress):

                content_progress = self._content_progress[f"{self._current_course_id}:{self._current_content_type}"]
                content_progress.bytes_downloaded += bytes_downloaded

                if total_bytes:
                    content_progress.total_bytes = (content_progress.total_bytes or 0) + total_bytes

            # Update global statistics
            self._statistics['total_bytes_downloaded'] += bytes_downloaded

            self._update_display()

    def record_file_download(self, success: bool = True):
        """
        Record the completion of a file download.

        Args:
            success: Whether the download was successful
        """
        with self._lock:
            if success:
                self._statistics['total_files_downloaded'] += 1
                self.update_content_statistics(success=1)
            else:
                self.update_content_statistics(error=1)

    # =============================================================================
    # Display Methods
    # =============================================================================

    def _initialize_display(self):
        """Initialize the progress display system."""
        if not self.console_output:
            return

        if self.use_rich and self._console:
            # Initialize Rich progress display
            self._progress_display = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=self._console
            )

            # Start live display
            self._live_display = Live(self._generate_rich_display(),
                                      console=self._console,
                                      refresh_per_second=2)
            self._live_display.start()
        else:
            # Fallback to basic console output
            print("Canvas Downloader - Progress Tracking Initialized")
            print("=" * 60)

    def _update_display(self):
        """Update the progress display."""
        if not self.console_output:
            return

        if self.use_rich and self._live_display:
            # Update Rich display
            self._live_display.update(self._generate_rich_display())
        else:
            # Update basic console display
            self._update_basic_display()

    def _generate_rich_display(self):
        """Generate Rich display layout."""
        if not self.use_rich:
            return ""

        # Create main table
        table = Table(title="Canvas Downloader Progress", show_header=True, header_style="bold blue")
        table.add_column("Level", style="cyan", width=15)
        table.add_column("Name", style="white", width=30)
        table.add_column("Progress", style="green", width=20)
        table.add_column("Status", style="yellow", width=15)
        table.add_column("Time", style="magenta", width=10)

        # Add application progress
        app_progress = self._application_progress
        table.add_row(
            "Application",
            app_progress.name,
            f"{app_progress.current}/{app_progress.total}",
            f"{app_progress.percentage:.1f}%",
            str(app_progress.elapsed_time).split('.')[0]
        )

        # Add current course progress
        if self._current_course_id and self._current_course_id in self._course_progress:
            course_progress = self._course_progress[self._current_course_id]
            table.add_row(
                "Course",
                course_progress.name,
                f"{course_progress.current}/{course_progress.total}",
                f"{course_progress.percentage:.1f}%",
                str(course_progress.elapsed_time).split('.')[0]
            )

        # Add current content type progress
        if self._current_content_type and self._current_course_id:
            progress_key = f"{self._current_course_id}:{self._current_content_type}"
            if progress_key in self._content_progress:
                content_progress = self._content_progress[progress_key]
                table.add_row(
                    "Content",
                    content_progress.name,
                    f"{content_progress.current}/{content_progress.total}",
                    f"{content_progress.percentage:.1f}%",
                    str(content_progress.elapsed_time).split('.')[0]
                )

        # Add statistics panel
        stats_text = f"""
Files Downloaded: {self._statistics['total_files_downloaded']}
Bytes Downloaded: {self._format_bytes(self._statistics['total_bytes_downloaded'])}
Errors: {self._statistics['total_errors']}
Skipped: {self._statistics['total_skipped']}
        """.strip()

        stats_panel = Panel(stats_text, title="Statistics", border_style="blue")

        # Combine table and stats
        from rich.columns import Columns
        return Columns([table, stats_panel])

    def _update_basic_display(self):
        """Update basic console display without Rich."""
        # Simple progress display for fallback
        app = self._application_progress
        print(f"\rOverall Progress: {app.current}/{app.total} "
              f"({app.percentage:.1f}%) - "
              f"Files: {self._statistics['total_files_downloaded']} - "
              f"Errors: {self._statistics['total_errors']}", end="", flush=True)

    def _format_bytes(self, bytes_count: int) -> str:
        """Format byte count in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"

    # =============================================================================
    # Utility and Information Methods
    # =============================================================================

    def get_overall_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the download process.

        Returns:
            Dict[str, Any]: Dictionary containing detailed statistics
        """
        with self._lock:
            elapsed_time = datetime.now() - self._start_time

            return {
                'start_time': self._start_time.isoformat(),
                'elapsed_time': str(elapsed_time).split('.')[0],
                'application_progress': {
                    'current': self._application_progress.current,
                    'total': self._application_progress.total,
                    'percentage': self._application_progress.percentage,
                    'completed': self._application_progress.completed
                },
                'download_statistics': self._statistics.copy(),
                'active_course': self._current_course_id,
                'active_content_type': self._current_content_type,
                'courses_processed': len([c for c in self._course_progress.values() if c.completed]),
                'total_courses': len(self._course_progress)
            }

    def get_course_statistics(self, course_id: str) -> Optional[Dict[str, Any]]:
        """
        Get statistics for a specific course.

        Args:
            course_id: Course ID to get statistics for

        Returns:
            Optional[Dict[str, Any]]: Course statistics or None if not found
        """
        with self._lock:
            if course_id not in self._course_progress:
                return None

            course_progress = self._course_progress[course_id]

            # Get content type statistics for this course
            content_stats = {}
            for key, content_progress in self._content_progress.items():
                if key.startswith(f"{course_id}:"):
                    content_type = key.split(":", 1)[1]
                    content_stats[content_type] = {
                        'current': content_progress.current,
                        'total': content_progress.total,
                        'percentage': content_progress.percentage,
                        'success_count': content_progress.success_count,
                        'error_count': content_progress.error_count,
                        'skip_count': content_progress.skip_count,
                        'completed': content_progress.completed
                    }

            return {
                'course_id': course_id,
                'name': course_progress.name,
                'current': course_progress.current,
                'total': course_progress.total,
                'percentage': course_progress.percentage,
                'completed': course_progress.completed,
                'elapsed_time': str(course_progress.elapsed_time).split('.')[0],
                'content_types': content_stats
            }

    def reset(self):
        """Reset all progress tracking state."""
        with self._lock:
            self._application_progress = ProgressState(name="Canvas Downloader")
            self._course_progress.clear()
            self._content_progress.clear()
            self._item_progress.clear()

            self._current_course_id = None
            self._current_content_type = None

            self._statistics = {
                'total_files_downloaded': 0,
                'total_bytes_downloaded': 0,
                'total_errors': 0,
                'total_skipped': 0
            }

            self._start_time = datetime.now()

            self.logger.info("Progress tracker reset")

    def cleanup(self):
        """Clean up display resources."""
        if self._live_display:
            self._live_display.stop()
            self._live_display = None

        if self.console_output and not self.use_rich:
            print()  # New line after progress output

        self.logger.info("Progress tracker cleaned up")


def create_progress_tracker(**kwargs) -> ProgressTracker:
    """
    Factory function to create a progress tracker instance.

    Args:
        **kwargs: Arguments to pass to ProgressTracker constructor

    Returns:
        ProgressTracker: Configured progress tracker instance
    """
    return ProgressTracker(**kwargs)