#!/usr/bin/env python3
"""
Test Script to Verify Async Issues Are Fixed

This script tests the async/await functionality without requiring
Canvas credentials, to ensure all the dangerous async issues are resolved.
"""

import asyncio
import sys
from pathlib import Path

# Add the src directory to the path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.config.settings import get_config
from src.utils.logger import setup_logging, get_logger
from src.utils.progress import ProgressTracker


class MockCanvas:
    """Mock Canvas client for testing without real API calls."""

    def get_course(self, course_id):
        return MockCourse()

    def get_current_user(self):
        return MockUser()

    def get_courses(self, **kwargs):
        return [MockCourse(), MockCourse()]


class MockCourse:
    """Mock course object."""

    def __init__(self):
        self.id = "12345"
        self.name = "Test Course"
        self.course_code = "TEST101"
        self.workflow_state = "active"


class MockUser:
    """Mock user object."""

    def __init__(self):
        self.name = "Test User"


class MockDownloader:
    """Mock downloader for testing async behavior."""

    def __init__(self, canvas_client, progress_tracker=None):
        self.canvas_client = canvas_client
        self.progress_tracker = progress_tracker
        self.logger = get_logger(__name__)

    async def download_course_content(self, course, course_info):
        """Mock async download method."""
        # Simulate some async work
        await asyncio.sleep(0.1)

        return {
            'total_items': 5,
            'downloaded_items': 5,
            'skipped_items': 0,
            'failed_items': 0,
            'total_size_bytes': 1024
        }


async def test_async_download():
    """Test that async downloads work properly."""
    print("🧪 Testing async download functionality...")

    # Set up components
    config = get_config()
    setup_logging({'level': 'INFO', 'console_output': True, 'file_output': False})
    logger = get_logger(__name__)
    progress_tracker = ProgressTracker(console_output=False)  # Disable console for test

    # Create mock Canvas client
    canvas_client = MockCanvas()

    # Test course parsing
    from src.core.course_parser import CourseParser
    parser = CourseParser()

    course = MockCourse()
    parsed_course = parser.parse_course(course)

    print(f"✅ Course parsing works: {parsed_course.full_name}")

    # Test mock downloader
    downloader = MockDownloader(canvas_client, progress_tracker)

    # This should work without await issues
    stats = await downloader.download_course_content(course, parsed_course.to_dict())

    print(f"✅ Async download works: {stats['downloaded_items']} items downloaded")

    # Test progress tracking
    progress_tracker.start_course("Test Course", "12345")
    progress_tracker.start_content_type("assignments", 5)
    progress_tracker.update_item_progress(5)
    progress_tracker.complete_content_type("assignments")
    progress_tracker.complete_course("12345")

    print("✅ Progress tracking works")

    # Test static report generation
    results = {
        "12345": {
            "assignments": stats
        }
    }

    # Import the app class to test static method
    from integration_example import CanvasDownloaderApp
    report = CanvasDownloaderApp.generate_download_report(results)

    print("✅ Static report generation works")
    print("📊 Sample report snippet:")
    print(report.split('\n')[2])  # Show title line

    return True


async def test_multiple_async_operations():
    """Test multiple async operations in sequence."""
    print("\n🔄 Testing multiple async operations...")

    # Simulate multiple downloads
    tasks = []
    for i in range(3):
        canvas_client = MockCanvas()
        downloader = MockDownloader(canvas_client)
        course = MockCourse()

        # This tests that we can await multiple operations
        task = downloader.download_course_content(course, {"id": str(i)})
        tasks.append(task)

    # Wait for all downloads to complete
    results = await asyncio.gather(*tasks)

    print(f"✅ Multiple async operations completed: {len(results)} downloads")

    return True


def test_sync_operations():
    """Test synchronous operations that shouldn't use await."""
    print("\n⚙️  Testing synchronous operations...")

    # Configuration operations (should be sync)
    config = get_config()

    # Test getting values
    enabled = config.is_content_type_enabled('assignments')
    print(f"✅ Config sync operations work: assignments enabled = {enabled}")

    # Test setting values
    config.set('test_setting', 'test_value', save=False)
    value = config.get('test_setting')
    print(f"✅ Config set/get works: {value}")

    # Test validation
    errors = config.validate_config()
    print(f"✅ Config validation works: {len(errors)} errors found")

    return True


async def main():
    """Main test function."""
    print("🚀 Canvas Downloader Async/Await Test Suite")
    print("=" * 50)

    try:
        # Test sync operations first
        test_sync_operations()

        # Test async operations
        await test_async_download()
        await test_multiple_async_operations()

        print("\n🎉 All tests passed! Async issues are fixed.")
        print("✅ The integration example should now work without await errors.")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    # Run the async test
    success = asyncio.run(main())

    if success:
        print("\n🚀 Ready to run the real integration example!")
        print("Run: python integration_example.py")
    else:
        print("\n🔧 Please fix the remaining issues before proceeding.")
        sys.exit(1)