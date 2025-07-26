"""
Canvas API Client Module

This module provides a wrapper around the Canvas API library with additional
functionality for the Canvas Downloader application. It handles authentication,
provides convenient methods for accessing Canvas data, and includes error
handling and retry logic.

Features:
- Wrapper around canvasapi library
- Automatic retry on API failures
- Comprehensive error handling
- Rate limiting and request optimization
- Course filtering and selection
- User information retrieval

Usage:
    # Initialize client with credentials
    client = CanvasAPIClient(api_url, api_key)

    # Get user's courses
    courses = client.get_user_courses()

    # Get specific course
    course = client.get_course(course_id)

    # Get course content
    assignments = client.get_course_assignments(course)
"""

import time
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime

try:
    from canvasapi import Canvas
    from canvasapi.course import Course
    from canvasapi.user import User
    from canvasapi.exceptions import CanvasException, Unauthorized, ResourceDoesNotExist
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError as import_error:
    raise ImportError(
        "Required libraries not found. Please install with: pip install canvasapi requests") from import_error


class CanvasAPIError(Exception):
    """Custom exception for Canvas API related errors."""
    pass


class RateLimitExceeded(CanvasAPIError):
    """Exception raised when Canvas API rate limit is exceeded."""
    pass


class CanvasAPIClient:
    """
    Canvas API Client Wrapper

    This class provides a high-level interface to the Canvas API with enhanced
    error handling, retry logic, and convenience methods specifically designed
    for the Canvas Downloader application.

    The client handles:
    - Authentication and session management
    - Automatic retry on transient failures
    - Rate limiting compliance
    - Response caching for repeated requests
    - Comprehensive error reporting
    """

    def __init__(self, api_url: str, api_key: str, timeout: int = 30, max_retries: int = 3):
        """
        Initialize the Canvas API client.

        Args:
            api_url: Canvas instance URL (e.g., https://canvas.school.edu)
            api_key: Canvas API access token
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts for failed requests
        """
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries

        # Initialize Canvas API client
        try:
            self.canvas = Canvas(self.api_url, self.api_key)

            # Configure requests session with retry strategy
            self._setup_session()

            # Cache for frequently accessed data
            self._cache = {}
            self._cache_timestamps = {}
            self._cache_duration = 300  # 5 minutes

            # Rate limiting
            self._last_request_time = 0
            self._min_request_interval = 0.1  # Minimum 100ms between requests

        except Exception as canvas_init_error:
            raise CanvasAPIError(f"Failed to initialize Canvas API client: {canvas_init_error}") from canvas_init_error

    def _setup_session(self):
        """
        Configure the requests session with retry strategy and timeouts.

        This method sets up automatic retries for transient network errors
        and HTTP status codes that indicate temporary issues.
        """
        # Define retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry
            allowed_methods=["HEAD", "GET", "OPTIONS"],  # Only retry safe methods
            backoff_factor=1  # Exponential backoff: 1, 2, 4, 8 seconds
        )

        # Create HTTP adapter with retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)

        # Apply adapter to the Canvas client's session
        if hasattr(self.canvas, '_Canvas__requester') and hasattr(self.canvas._Canvas__requester,
                                                                  '_session'):  # noqa: SLF001
            session = self.canvas._Canvas__requester._session  # noqa: SLF001
            session.mount("https://", adapter)
            session.timeout = self.timeout

    def _rate_limit(self):
        """
        Implement basic rate limiting to avoid overwhelming the Canvas API.

        This method ensures a minimum interval between API requests to comply
        with Canvas API rate limiting policies.
        """
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time

        if time_since_last_request < self._min_request_interval:
            sleep_time = self._min_request_interval - time_since_last_request
            time.sleep(sleep_time)

        self._last_request_time = time.time()

    def _get_cached_data(self, cache_key: str) -> Optional[Any]:
        """
        Retrieve data from cache if it's still valid.

        Args:
            cache_key: Unique identifier for the cached data

        Returns:
            Cached data if valid, None otherwise
        """
        if cache_key not in self._cache:
            return None

        # Check if cache is still valid
        cache_time = self._cache_timestamps.get(cache_key, 0)
        if time.time() - cache_time > self._cache_duration:
            # Cache expired, remove it
            del self._cache[cache_key]
            del self._cache_timestamps[cache_key]
            return None

        return self._cache[cache_key]

    def _set_cached_data(self, cache_key: str, data: Any):
        """
        Store data in cache with timestamp.

        Args:
            cache_key: Unique identifier for the data
            data: Data to cache
        """
        self._cache[cache_key] = data
        self._cache_timestamps[cache_key] = time.time()

    @staticmethod
    def _handle_canvas_exception(exception: Exception, operation: str) -> None:
        """
        Handle Canvas API exceptions with appropriate error messages.

        Args:
            exception: The exception that occurred
            operation: Description of the operation that failed

        Raises:
            Appropriate CanvasAPIError subclass
        """
        if isinstance(exception, Unauthorized):
            raise CanvasAPIError(f"Authentication failed during {operation}. Please check your API key.") from exception

        elif isinstance(exception, ResourceDoesNotExist):
            raise CanvasAPIError(
                f"Resource not found during {operation}. It may have been deleted or you may not have access.") from exception

        elif isinstance(exception, CanvasException):
            if "429" in str(exception) or "rate limit" in str(exception).lower():
                raise RateLimitExceeded(
                    f"Rate limit exceeded during {operation}. Please wait before retrying.") from exception
            else:
                raise CanvasAPIError(f"Canvas API error during {operation}: {exception}") from exception

        else:
            raise CanvasAPIError(f"Unexpected error during {operation}: {exception}") from exception

    def test_connection(self) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Test the Canvas API connection and return user information.

        Returns:
            Tuple[bool, str, Optional[Dict]]:
                - Success status
                - Status message
                - User information dictionary (if successful)
        """
        try:
            self._rate_limit()

            # Get current user profile
            user = self.canvas.get_current_user()

            user_info = {
                'id': user.id,
                'name': user.name,
                'email': getattr(user, 'email', 'Not available'),
                'login_id': getattr(user, 'login_id', 'Not available'),
                'avatar_url': getattr(user, 'avatar_url', '')
            }

            return True, f"Successfully connected as {user.name}", user_info

        except Exception as canvas_exception:
            self._handle_canvas_exception(canvas_exception, "connection test")

    def get_current_user(self) -> Dict[str, Any]:
        """
        Get information about the current user.

        Returns:
            Dict[str, Any]: User information dictionary
        """
        cache_key = "current_user"
        cached_user = self._get_cached_data(cache_key)

        if cached_user:
            return cached_user

        try:
            self._rate_limit()
            user = self.canvas.get_current_user()

            user_info = {
                'id': user.id,
                'name': user.name,
                'email': getattr(user, 'email', ''),
                'login_id': getattr(user, 'login_id', ''),
                'avatar_url': getattr(user, 'avatar_url', ''),
                'bio': getattr(user, 'bio', ''),
                'title': getattr(user, 'title', ''),
                'time_zone': getattr(user, 'time_zone', '')
            }

            self._set_cached_data(cache_key, user_info)
            return user_info

        except Exception as canvas_exception:
            self._handle_canvas_exception(canvas_exception, "getting current user")

    def get_user_courses(self, include_concluded: bool = True,
                         enrollment_state: str = "active") -> List[Dict[str, Any]]:
        """
        Get courses for the current user.

        Args:
            include_concluded: Whether to include concluded courses
            enrollment_state: Filter by enrollment state ('active', 'invited', 'completed')

        Returns:
            List[Dict[str, Any]]: List of course information dictionaries
        """
        cache_key = f"user_courses_{include_concluded}_{enrollment_state}"
        cached_courses = self._get_cached_data(cache_key)

        if cached_courses:
            return cached_courses

        try:
            self._rate_limit()

            # Get courses with specified parameters
            courses = self.canvas.get_courses(
                enrollment_state=enrollment_state,
                include=['term', 'course_image', 'total_students']
            )

            course_list = []
            for course in courses:
                # Skip concluded courses if not requested
                if not include_concluded and getattr(course, 'workflow_state', '') == 'completed':
                    continue

                course_info = {
                    'id': course.id,
                    'name': course.name,
                    'course_code': getattr(course, 'course_code', ''),
                    'workflow_state': getattr(course, 'workflow_state', ''),
                    'start_at': getattr(course, 'start_at', ''),
                    'end_at': getattr(course, 'end_at', ''),
                    'enrollment_term_id': getattr(course, 'enrollment_term_id', None),
                    'is_public': getattr(course, 'is_public', False),
                    'course_format': getattr(course, 'course_format', ''),
                    'total_students': getattr(course, 'total_students', 0),
                    'storage_quota_mb': getattr(course, 'storage_quota_mb', 0),
                    'hide_final_grades': getattr(course, 'hide_final_grades', False),
                    'term': self._extract_term_info(getattr(course, 'term', {})),
                    'enrollments': self._extract_enrollment_info(getattr(course, 'enrollments', []))
                }

                course_list.append(course_info)

            # Sort courses by name
            course_list.sort(key=lambda x: x['name'])

            self._set_cached_data(cache_key, course_list)
            return course_list

        except Exception as canvas_exception:
            self._handle_canvas_exception(canvas_exception, "getting user courses")

    @staticmethod
    def _extract_term_info(term_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract term information from Canvas term data.

        Args:
            term_data: Raw term data from Canvas API

        Returns:
            Dict[str, Any]: Processed term information
        """
        if not term_data:
            return {}

        return {
            'id': term_data.get('id'),
            'name': term_data.get('name', ''),
            'start_at': term_data.get('start_at', ''),
            'end_at': term_data.get('end_at', ''),
            'workflow_state': term_data.get('workflow_state', '')
        }

    @staticmethod
    def _extract_enrollment_info(enrollments_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract enrollment information from Canvas enrollment data.

        Args:
            enrollments_data: Raw enrollment data from Canvas API

        Returns:
            List[Dict[str, Any]]: Processed enrollment information
        """
        enrollments = []

        for enrollment in enrollments_data:
            enrollment_info = {
                'id': enrollment.get('id'),
                'type': enrollment.get('type', ''),
                'role': enrollment.get('role', ''),
                'enrollment_state': enrollment.get('enrollment_state', ''),
                'user_id': enrollment.get('user_id'),
                'course_id': enrollment.get('course_id'),
                'course_section_id': enrollment.get('course_section_id')
            }
            enrollments.append(enrollment_info)

        return enrollments

    def get_course(self, course_id: Union[int, str]) -> Course:
        """
        Get a specific course object.

        Args:
            course_id: Canvas course ID

        Returns:
            Course: Canvas course object
        """
        try:
            self._rate_limit()
            course = self.canvas.get_course(course_id)
            return course

        except Exception as canvas_exception:
            self._handle_canvas_exception(canvas_exception, f"getting course {course_id}")

    def get_course_info(self, course_id: Union[int, str]) -> Dict[str, Any]:
        """
        Get detailed information about a specific course.

        Args:
            course_id: Canvas course ID

        Returns:
            Dict[str, Any]: Detailed course information
        """
        cache_key = f"course_info_{course_id}"
        cached_info = self._get_cached_data(cache_key)

        if cached_info:
            return cached_info

        try:
            course = self.get_course(course_id)

            course_info = {
                'id': course.id,
                'name': course.name,
                'course_code': getattr(course, 'course_code', ''),
                'workflow_state': getattr(course, 'workflow_state', ''),
                'account_id': getattr(course, 'account_id', None),
                'root_account_id': getattr(course, 'root_account_id', None),
                'enrollment_term_id': getattr(course, 'enrollment_term_id', None),
                'start_at': getattr(course, 'start_at', ''),
                'end_at': getattr(course, 'end_at', ''),
                'public_syllabus': getattr(course, 'public_syllabus', False),
                'storage_quota_mb': getattr(course, 'storage_quota_mb', 0),
                'is_public': getattr(course, 'is_public', False),
                'course_format': getattr(course, 'course_format', ''),
                'restrict_enrollments_to_course_dates': getattr(course, 'restrict_enrollments_to_course_dates', False),
                'hide_final_grades': getattr(course, 'hide_final_grades', False),
                'apply_assignment_group_weights': getattr(course, 'apply_assignment_group_weights', False),
                'time_zone': getattr(course, 'time_zone', ''),
                'license': getattr(course, 'license', ''),
                'default_view': getattr(course, 'default_view', ''),
                'syllabus_body': getattr(course, 'syllabus_body', ''),
                'total_students': getattr(course, 'total_students', 0)
            }

            self._set_cached_data(cache_key, course_info)
            return course_info

        except Exception as canvas_exception:
            self._handle_canvas_exception(canvas_exception, f"getting course info for {course_id}")

    @staticmethod
    def parse_course_name(course_name: str) -> Dict[str, str]:
        """
        Parse a Canvas course name to extract components.

        Expected format: "Subject name (CODE) - Subsection - Year - Semester"
        Example: "Álgebra Lineal (CC1103) - Teoría 2 - 2024 - 1"

        Args:
            course_name: Full course name from Canvas

        Returns:
            Dict[str, str]: Parsed course components
        """
        import re

        # Default values
        parsed = {
            'subject_name': course_name,
            'subject_code': '',
            'subsection': '',
            'year': '',
            'semester': '',
            'full_name': course_name
        }

        try:
            # Regular expression to match the expected format
            # Pattern: Subject (CODE) - Section - Year - Semester
            pattern = r'^(.+?)\s*\(([^)]+)\)\s*-\s*(.+?)\s*-\s*(\d{4})\s*-\s*(.+)$'
            match = re.match(pattern, course_name.strip())

            if match:
                parsed['subject_name'] = match.group(1).strip()
                parsed['subject_code'] = match.group(2).strip()
                parsed['subsection'] = match.group(3).strip()
                parsed['year'] = match.group(4).strip()
                parsed['semester'] = match.group(5).strip()
            else:
                # Try alternative patterns
                # Pattern for just subject and code: "Subject (CODE)"
                simple_pattern = r'^(.+?)\s*\(([^)]+)\)(.*)$'
                simple_match = re.match(simple_pattern, course_name.strip())

                if simple_match:
                    parsed['subject_name'] = simple_match.group(1).strip()
                    parsed['subject_code'] = simple_match.group(2).strip()
                    # Try to extract additional info from the remainder
                    remainder = simple_match.group(3).strip()
                    if remainder:
                        # Split by dashes and try to identify year/semester
                        parts = [part.strip() for part in remainder.split('-') if part.strip()]
                        for part in parts:
                            if re.match(r'^\d{4}$', part):  # Year
                                parsed['year'] = part
                            elif re.match(r'^[12]$', part):  # Semester (1 or 2)
                                parsed['semester'] = part
                            elif not parsed['subsection']:  # First non-year/semester part
                                parsed['subsection'] = part

        except Exception as parsing_exception:
            print(f"Warning: Could not parse course name '{course_name}': {parsing_exception}")

        return parsed

    def get_course_statistics(self, course_id: Union[int, str]) -> Dict[str, Any]:
        """
        Get statistics about course content.

        Args:
            course_id: Canvas course ID

        Returns:
            Dict[str, Any]: Course content statistics
        """
        try:
            course = self.get_course(course_id)

            stats = {
                'assignments_count': 0,
                'announcements_count': 0,
                'discussions_count': 0,
                'files_count': 0,
                'modules_count': 0,
                'quizzes_count': 0,
                'pages_count': 0,
                'students_count': 0
            }

            # Count assignments
            try:
                self._rate_limit()
                assignments = list(course.get_assignments())
                stats['assignments_count'] = len(assignments)
            except CanvasException:
                pass

            # Count announcements
            try:
                self._rate_limit()
                announcements = list(course.get_discussion_topics(only_announcements=True))
                stats['announcements_count'] = len(announcements)
            except CanvasException:
                pass

            # Count discussions
            try:
                self._rate_limit()
                discussions = list(course.get_discussion_topics())
                # Subtract announcements from discussions
                stats['discussions_count'] = max(0, len(discussions) - stats['announcements_count'])
            except CanvasException:
                pass

            # Count files
            try:
                self._rate_limit()
                files = list(course.get_files())
                stats['files_count'] = len(files)
            except CanvasException:
                pass

            # Count modules
            try:
                self._rate_limit()
                modules = list(course.get_modules())
                stats['modules_count'] = len(modules)
            except CanvasException:
                pass

            # Count quizzes
            try:
                self._rate_limit()
                quizzes = list(course.get_quizzes())
                stats['quizzes_count'] = len(quizzes)
            except CanvasException:
                pass

            # Count pages
            try:
                self._rate_limit()
                pages = list(course.get_pages())
                stats['pages_count'] = len(pages)
            except CanvasException:
                pass

            # Count students
            try:
                self._rate_limit()
                students = list(course.get_users(enrollment_type=['student']))
                stats['students_count'] = len(students)
            except CanvasException:
                pass

            return stats

        except Exception as canvas_exception:
            self._handle_canvas_exception(canvas_exception, f"getting course statistics for {course_id}")

    def get_course_tabs(self, course_id: Union[int, str]) -> List[Dict[str, Any]]:
        """
        Get available tabs/navigation items for a course.

        This helps determine what content types are available in the course.

        Args:
            course_id: Canvas course ID

        Returns:
            List[Dict[str, Any]]: List of available course tabs
        """
        try:
            self._rate_limit()
            course = self.get_course(course_id)
            tabs = list(course.get_tabs())

            tab_info = []
            for tab in tabs:
                tab_data = {
                    'id': getattr(tab, 'id', ''),
                    'html_url': getattr(tab, 'html_url', ''),
                    'full_url': getattr(tab, 'full_url', ''),
                    'label': getattr(tab, 'label', ''),
                    'type': getattr(tab, 'type', ''),
                    'hidden': getattr(tab, 'hidden', False),
                    'visibility': getattr(tab, 'visibility', ''),
                    'position': getattr(tab, 'position', 0)
                }
                tab_info.append(tab_data)

            return tab_info

        except Exception as canvas_exception:
            self._handle_canvas_exception(canvas_exception, f"getting course tabs for {course_id}")

    def check_content_availability(self, course_id: Union[int, str]) -> Dict[str, bool]:
        """
        Check which content types are available for a course.

        Args:
            course_id: Canvas course ID

        Returns:
            Dict[str, bool]: Content type availability mapping
        """
        availability = {
            'announcements': False,
            'assignments': False,
            'discussions': False,
            'files': False,
            'modules': False,
            'quizzes': False,
            'grades': False,
            'people': False,
            'pages': False
        }

        try:
            # Get course tabs to check availability
            tabs = self.get_course_tabs(course_id)

            # Map tab types to content types
            tab_mappings = {
                'announcements': 'announcements',
                'assignments': 'assignments',
                'discussions': 'discussions',
                'files': 'files',
                'modules': 'modules',
                'quizzes': 'quizzes',
                'grades': 'grades',
                'people': 'people',
                'wiki': 'pages',
                'pages': 'pages'
            }

            for tab in tabs:
                tab_type = tab.get('type', '').lower()
                if tab_type in tab_mappings and not tab.get('hidden', False):
                    content_type = tab_mappings[tab_type]
                    availability[content_type] = True

            # Some content types might not have tabs but still be available
            # Try to access them directly to verify
            course = self.get_course(course_id)

            # Check assignments
            if not availability['assignments']:
                try:
                    self._rate_limit()
                    list(course.get_assignments(per_page=1))
                    availability['assignments'] = True
                except CanvasException:
                    pass

            # Check announcements
            if not availability['announcements']:
                try:
                    self._rate_limit()
                    list(course.get_discussion_topics(only_announcements=True, per_page=1))
                    availability['announcements'] = True
                except CanvasException:
                    pass

            # Check discussions
            if not availability['discussions']:
                try:
                    self._rate_limit()
                    list(course.get_discussion_topics(per_page=1))
                    availability['discussions'] = True
                except CanvasException:
                    pass

            # People is usually always available if you have access to the course
            availability['people'] = True

            return availability

        except Exception as canvas_exception:
            print(f"Warning: Could not check content availability for course {course_id}: {canvas_exception}")
            # Return conservative availability (assume most content is available)
            return {
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

    def get_account_info(self) -> Dict[str, Any]:
        """
        Get information about the Canvas account/institution.

        Returns:
            Dict[str, Any]: Account information
        """
        try:
            self._rate_limit()

            # Get the user's accounts
            accounts = list(self.canvas.get_accounts())

            if accounts:
                # Usually the first account is the main institution account
                account = accounts[0]

                account_info = {
                    'id': account.id,
                    'name': getattr(account, 'name', ''),
                    'domain': getattr(account, 'domain', ''),
                    'workflow_state': getattr(account, 'workflow_state', ''),
                    'parent_account_id': getattr(account, 'parent_account_id', None),
                    'root_account_id': getattr(account, 'root_account_id', None)
                }

                return account_info

            return {}

        except Exception as canvas_exception:
            print(f"Warning: Could not get account info: {canvas_exception}")
            return {}

    def clear_cache(self):
        """Clear the internal cache."""
        self._cache.clear()
        self._cache_timestamps.clear()

    def get_cache_info(self) -> Dict[str, Any]:
        """
        Get information about the current cache state.

        Returns:
            Dict[str, Any]: Cache statistics
        """
        current_time = time.time()
        valid_entries = 0
        expired_entries = 0

        for cache_key, timestamp in self._cache_timestamps.items():
            if current_time - timestamp <= self._cache_duration:
                valid_entries += 1
            else:
                expired_entries += 1

        return {
            'total_entries': len(self._cache),
            'valid_entries': valid_entries,
            'expired_entries': expired_entries,
            'cache_duration_seconds': self._cache_duration
        }


def create_canvas_client(api_url: str, api_key: str, **kwargs) -> CanvasAPIClient:
    """
    Factory function to create a Canvas API client.

    Args:
        api_url: Canvas instance URL
        api_key: Canvas API access token
        **kwargs: Additional arguments for CanvasAPIClient

    Returns:
        CanvasAPIClient: Configured Canvas API client
    """
    return CanvasAPIClient(api_url, api_key, **kwargs)