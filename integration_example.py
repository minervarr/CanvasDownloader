#!/usr/bin/env python3
"""
Complete Canvas Downloader Integration Example

This example demonstrates how to use all the components of the Canvas Downloader
system together. It shows proper initialization, configuration, and usage of:

- Configuration management
- Logging system
- Progress tracking
- Canvas API client
- Course parsing
- Content downloaders (assignments, modules, etc.)
- Error handling and recovery

This serves as both a working example and a template for building your own
Canvas downloader applications.

Usage:
    python integration_example.py
"""

import asyncio
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

# Add the src directory to the path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Core imports
from src.config.settings import get_config
from src.utils.logger import setup_logging, get_logger
from src.utils.progress import ProgressTracker
from src.core.course_parser import CourseParser

# Downloader imports
from src.downloaders.base import ContentDownloaderFactory
from src.downloaders.assignments import AssignmentsDownloader
from src.downloaders.modules import ModulesDownloader

# External dependencies
try:
    from canvasapi import Canvas

    CANVASAPI_AVAILABLE = True
except ImportError:
    Canvas = None
    CANVASAPI_AVAILABLE = False
    print("⚠️  canvasapi library not installed. Run: pip install canvasapi")


class CanvasDownloaderApp:
    """
    Complete Canvas Downloader Application

    This class demonstrates how to integrate all components of the Canvas
    downloader system into a working application.
    """

    def __init__(self, config_file: str = None):
        """
        Initialize the Canvas downloader application.

        Args:
            config_file: Optional path to configuration file
        """
        # Initialize configuration
        self.config = get_config(config_file)

        # Set up logging
        logging_config = self.config.get('logging', {})
        setup_logging(logging_config)
        self.logger = get_logger(__name__)

        # Initialize progress tracking
        ui_config = self.config.get('ui', {})
        self.progress_tracker = ProgressTracker(
            console_output=True,
            use_rich=ui_config.get('use_rich_progress', True)
        )

        # Initialize course parser
        self.course_parser = CourseParser()

        # Canvas client (will be set when session is selected)
        self.canvas_client = None
        self.current_session = None

        # Register downloaders
        self._register_downloaders()

        self.logger.info("Canvas Downloader Application initialized")

    def _register_downloaders(self) -> None:
        """Register all available content downloaders."""
        ContentDownloaderFactory.register_downloader('assignments', AssignmentsDownloader)
        ContentDownloaderFactory.register_downloader('modules', ModulesDownloader)
        # Add other downloaders as they are implemented

        available_types = ContentDownloaderFactory.get_available_content_types()
        self.logger.info(f"Registered downloaders", content_types=available_types)

    def list_canvas_sessions(self) -> Dict[str, Dict[str, Any]]:
        """
        List all configured Canvas sessions.

        Returns:
            Dict[str, Dict[str, Any]]: Session information
        """
        sessions = self.config.get_all_sessions()

        if not sessions:
            self.logger.info("No Canvas sessions configured")
            return {}

        self.logger.info(f"Found {len(sessions)} Canvas sessions")
        for session_id, session_data in sessions.items():
            self.logger.info(f"Session: {session_data['name']}",
                             session_id=session_id,
                             canvas_url=session_data['canvas_url'])

        return sessions

    def add_canvas_session(self, name: str, canvas_url: str, api_key: str) -> str:
        """
        Add a new Canvas session.

        Args:
            name: Human-readable name for the session
            canvas_url: Canvas instance URL
            api_key: Canvas API key

        Returns:
            str: Session ID
        """
        session_id = self.config.add_canvas_session(name, canvas_url, api_key)
        self.logger.info(f"Added Canvas session", name=name, session_id=session_id)
        return session_id

    def connect_to_canvas(self, session_id: str = None) -> bool:
        """
        Connect to Canvas using a specific session.

        Args:
            session_id: Session ID to use (prompts user if None)

        Returns:
            bool: True if connection successful
        """
        if not CANVASAPI_AVAILABLE:
            self.logger.error("canvasapi library not available")
            return False

        # Get session information
        if session_id is None:
            sessions = self.list_canvas_sessions()
            if not sessions:
                self.logger.error("No Canvas sessions available")
                return False

            # Use first session for demo (in real app, prompt user)
            session_id = list(sessions.keys())[0]

        session_data = self.config.get_canvas_session(session_id)
        if not session_data:
            self.logger.error(f"Session not found", session_id=session_id)
            return False

        try:
            # Create Canvas client
            self.canvas_client = Canvas(
                session_data['canvas_url'],
                session_data['api_key']
            )

            # Test connection by getting user info
            user = self.canvas_client.get_current_user()
            self.current_session = session_data

            # Update last used timestamp
            self.config.update_session_last_used(session_id)

            self.logger.info(f"Connected to Canvas",
                             canvas_url=session_data['canvas_url'],
                             user_name=getattr(user, 'name', 'Unknown'))

            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to Canvas", exception=e)
            return False

    def get_available_courses(self) -> List[Dict[str, Any]]:
        """
        Get list of available courses from Canvas.

        Returns:
            List[Dict[str, Any]]: Course information
        """
        if not self.canvas_client:
            self.logger.error("Not connected to Canvas")
            return []

        try:
            self.logger.info("Fetching available courses")

            # Get courses with enrollments
            canvas_courses = list(self.canvas_client.get_courses(
                enrollment_state='active',
                include=['term', 'course_image', 'public_description']
            ))

            # Parse course information
            parsed_courses = self.course_parser.parse_multiple_courses(canvas_courses)

            # Convert to list of dictionaries
            course_list = [course.to_dict() for course in parsed_courses]

            self.logger.info(f"Found {len(course_list)} available courses")

            return course_list

        except Exception as e:
            self.logger.error("Failed to fetch courses", exception=e)
            return []

    async def download_course_content(self, course_id: str,
                                      content_types: List[str] = None) -> Dict[str, Any]:
        """
        Download content for a specific course.

        Args:
            course_id: Canvas course ID
            content_types: List of content types to download (None for all enabled)

        Returns:
            Dict[str, Any]: Download results
        """
        if not self.canvas_client:
            self.logger.error("Not connected to Canvas")
            return {'error': 'Not connected to Canvas'}

        try:
            # Get course object
            course = self.canvas_client.get_course(course_id)

            # Parse course information
            parsed_course = self.course_parser.parse_course(course)

            self.logger.info(f"Starting download for course",
                             course_name=parsed_course.full_name,
                             course_id=course_id)

            # Determine content types to download
            if content_types is None:
                enabled_types = self.config.get_enabled_content_types()
                content_types = [content_type for content_type, _ in enabled_types]

            # Set up progress tracking
            self.progress_tracker.start_course(parsed_course.full_name, course_id)
            self.progress_tracker.set_total_content_types(len(content_types))

            # Download each content type
            results = {}

            for content_type in content_types:
                try:
                    self.logger.info(f"Starting {content_type} download")

                    # Create downloader
                    downloader = ContentDownloaderFactory.create_downloader(
                        content_type,
                        self.canvas_client,
                        self.progress_tracker
                    )

                    # Start content type tracking
                    self.progress_tracker.start_content_type(content_type)

                    # Download content (this is the key fix - await the async method)
                    stats = await downloader.download_course_content(course, parsed_course.to_dict())
                    results[content_type] = stats

                    # Complete content type
                    self.progress_tracker.complete_content_type(content_type)

                    self.logger.info(f"Completed {content_type} download", **stats)

                except Exception as e:
                    error_msg = f"Failed to download {content_type}: {e}"
                    self.logger.error(error_msg, exception=e)
                    results[content_type] = {'error': error_msg}
                    self.progress_tracker.report_error(error_msg)

            # Complete course
            self.progress_tracker.complete_course(course_id)

            self.logger.info(f"Course download completed",
                             course_id=course_id,
                             results_summary={k: 'success' if 'error' not in v else 'failed'
                                              for k, v in results.items()})

            return results

        except Exception as e:
            self.logger.error(f"Course download failed", exception=e)
            return {'error': str(e)}

    async def download_multiple_courses(self, course_ids: List[str],
                                        content_types: List[str] = None) -> Dict[str, Any]:
        """
        Download content for multiple courses.

        Args:
            course_ids: List of Canvas course IDs
            content_types: List of content types to download

        Returns:
            Dict[str, Any]: Combined download results
        """
        self.progress_tracker.set_total_courses(len(course_ids))

        all_results = {}

        for course_id in course_ids:
            try:
                results = await self.download_course_content(course_id, content_types)
                all_results[course_id] = results
            except Exception as e:
                error_msg = f"Failed to download course {course_id}: {e}"
                self.logger.error(error_msg, exception=e)
                all_results[course_id] = {'error': error_msg}

        return all_results

    @staticmethod
    def generate_download_report(results: Dict[str, Any]) -> str:
        """
        Generate a summary report of download results.

        Args:
            results: Download results from download operations

        Returns:
            str: Formatted report
        """
        report_lines = [
            "=" * 60,
            "CANVAS DOWNLOADER SUMMARY REPORT",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 60,
            ""
        ]

        # Overall statistics
        total_courses = len(results)
        successful_courses = sum(1 for r in results.values() if 'error' not in r)
        failed_courses = total_courses - successful_courses

        report_lines.extend([
            "OVERALL STATISTICS:",
            f"  Total Courses: {total_courses}",
            f"  Successful: {successful_courses}",
            f"  Failed: {failed_courses}",
            ""
        ])

        # Course-by-course breakdown
        report_lines.append("COURSE BREAKDOWN:")
        for course_id, course_results in results.items():
            if 'error' in course_results:
                report_lines.append(f"  Course {course_id}: FAILED - {course_results['error']}")
            else:
                # Summarize content type results
                content_summary = []
                for content_type, stats in course_results.items():
                    if isinstance(stats, dict) and 'downloaded_items' in stats:
                        content_summary.append(f"{content_type}({stats['downloaded_items']})")

                summary_str = ", ".join(content_summary) if content_summary else "No content"
                report_lines.append(f"  Course {course_id}: SUCCESS - {summary_str}")

        report_lines.extend([
            "",
            "=" * 60
        ])

        return "\n".join(report_lines)


async def demo_basic_usage():
    """Demonstrate basic usage of the Canvas Downloader."""
    print("🚀 Canvas Downloader - Basic Usage Demo")
    print("=" * 50)

    # Initialize application
    app = CanvasDownloaderApp()

    # Check for existing sessions
    sessions = app.list_canvas_sessions()

    if not sessions:
        print("\n📝 No Canvas sessions found. Let's add one!")
        print("You'll need:")
        print("1. Your Canvas URL (e.g., https://yourschool.instructure.com)")
        print("2. Your Canvas API key (generate from Canvas Account Settings)")
        print("\nFor this demo, we'll use example values...")

        # Demo session (replace with real values)
        session_id = app.add_canvas_session(
            name="Demo University",
            canvas_url="https://demo.instructure.com",
            api_key="your_api_key_here"
        )
        print(f"✅ Added demo session: {session_id}")

    # Connect to Canvas
    print("\n🔌 Connecting to Canvas...")
    if not app.connect_to_canvas():
        print("❌ Failed to connect to Canvas. Please check your credentials.")
        return

    print("✅ Connected to Canvas successfully!")

    # Get available courses
    print("\n📚 Fetching available courses...")
    courses = app.get_available_courses()

    if not courses:
        print("❌ No courses found or failed to fetch courses.")
        return

    print(f"✅ Found {len(courses)} courses:")
    for course in courses[:5]:  # Show first 5 courses
        print(f"  - {course['full_name']} (ID: {course['id']})")

    if len(courses) > 5:
        print(f"  ... and {len(courses) - 5} more")

    # Download content for first course
    if courses:
        first_course = courses[0]
        print(f"\n⬇️  Downloading content for: {first_course['full_name']}")

        # Download specific content types
        content_types = ['assignments', 'modules']  # Start with these

        results = await app.download_course_content(
            first_course['id'],
            content_types
        )

        # Generate and display report
        print("\n📊 Download Results:")
        if 'error' in results:
            print(f"❌ Download failed: {results['error']}")
        else:
            for content_type, stats in results.items():
                if 'error' in stats:
                    print(f"  {content_type}: ❌ {stats['error']}")
                else:
                    downloaded = stats.get('downloaded_items', 0)
                    total = stats.get('total_items', 0)
                    print(f"  {content_type}: ✅ {downloaded}/{total} items")

    print("\n🎉 Demo completed!")


async def demo_advanced_usage():
    """Demonstrate advanced usage with multiple courses and configuration."""
    print("🚀 Canvas Downloader - Advanced Usage Demo")
    print("=" * 50)

    # Initialize with custom configuration
    config = get_config()

    # Customize download settings
    config.set('download_settings.max_retries', 5)
    config.set('download_settings.parallel_downloads', 6)
    config.set('content_types.assignments.organize_by_groups', True)
    config.set('content_types.assignments.download_rubrics', True)

    app = CanvasDownloaderApp()

    # Connect to Canvas
    if not app.connect_to_canvas():
        print("❌ Failed to connect to Canvas")
        return

    # Get courses
    courses = app.get_available_courses()
    if not courses:
        print("❌ No courses available")
        return

    # Select courses for download (first 3 for demo)
    selected_courses = courses[:3]
    course_ids = [course['id'] for course in selected_courses]

    print(f"\n⬇️  Downloading content for {len(selected_courses)} courses:")
    for course in selected_courses:
        print(f"  - {course['full_name']}")

    # Download all enabled content types
    results = await app.download_multiple_courses(course_ids)

    # Generate detailed report
    report = CanvasDownloaderApp.generate_download_report(results)
    print("\n" + report)

    # Save report to file
    report_file = Path('downloads') / 'download_report.txt'
    report_file.parent.mkdir(exist_ok=True)
    report_file.write_text(report)
    print(f"\n💾 Report saved to: {report_file}")


def main():
    """Main entry point for the integration example."""
    print("Canvas Downloader Integration Example")
    print("Choose a demo to run:")
    print("1. Basic Usage Demo")
    print("2. Advanced Usage Demo")
    print("3. Configuration Demo")

    choice = input("\nEnter your choice (1-3): ").strip()

    if choice == "1":
        asyncio.run(demo_basic_usage())
    elif choice == "2":
        asyncio.run(demo_advanced_usage())
    elif choice == "3":
        demo_configuration()
    else:
        print("Invalid choice. Running basic demo...")
        asyncio.run(demo_basic_usage())


def demo_configuration():
    """Demonstrate configuration management."""
    print("🔧 Canvas Downloader - Configuration Demo")
    print("=" * 50)

    # Get configuration instance
    config = get_config()

    # Show current configuration
    print("\n📋 Current Configuration:")

    # Content types
    print("\nEnabled Content Types:")
    for content_type, priority in config.get_enabled_content_types():
        print(f"  {priority}. {content_type}")

    # Download settings
    print(f"\nDownload Settings:")
    print(f"  Max Retries: {config.get('download_settings.max_retries')}")
    print(f"  Timeout: {config.get('download_settings.timeout')}s")
    print(f"  Parallel Downloads: {config.get('download_settings.parallel_downloads')}")
    print(f"  Skip Existing: {config.get('download_settings.skip_existing')}")

    # Folder structure
    print(f"\nFolder Organization:")
    print(f"  By Semester: {config.get('folder_structure.organize_by_semester')}")
    print(f"  Assignment Groups: {config.get('folder_structure.create_assignment_groups')}")
    print(f"  Include Due Dates: {config.get('folder_structure.include_due_dates')}")

    # Canvas sessions
    sessions = config.get_all_sessions()
    print(f"\nCanvas Sessions: {len(sessions)}")
    for session_id, session_data in sessions.items():
        print(f"  - {session_data['name']} ({session_data['canvas_url']})")

    # Validation
    print("\n🔍 Configuration Validation:")
    errors = config.validate_config()
    if errors:
        print("❌ Configuration errors found:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✅ Configuration is valid")

    # Create template
    template_file = Path('config_template.json')
    config.create_config_template(template_file)
    print(f"\n📄 Configuration template created: {template_file}")


if __name__ == "__main__":
    main()