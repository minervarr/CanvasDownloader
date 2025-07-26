"""
Course Parser Utility Module

This module provides utilities for parsing and processing Canvas course information.
It extracts and normalizes course data, handles term information, and creates
standardized course representations for the downloader system.

Features:
- Course information extraction and normalization
- Term and semester parsing
- Course code and name standardization
- Date handling and timezone conversion
- Enrollment status processing
- Course metadata extraction

Usage:
    parser = CourseParser()
    course_info = parser.parse_course(canvas_course)
    folder_name = parser.generate_folder_name(course_info)
"""

import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from pathlib import Path

from ..utils.logger import get_logger


@dataclass
class ParsedCourse:
    """
    Standardized course information structure.

    This class represents a Canvas course with normalized and processed
    information for use throughout the downloader system.
    """
    # Basic course information
    id: str
    name: str
    course_code: str

    # Display information
    full_name: str = ""
    short_name: str = ""

    # Term information
    term: Dict[str, Any] = field(default_factory=dict)
    semester: str = ""
    year: int = 0

    # Status information
    workflow_state: str = "unknown"
    enrollment_status: str = "unknown"

    # Dates
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    created_date: Optional[datetime] = None

    # Metadata
    description: str = ""
    syllabus_body: str = ""
    default_view: str = "modules"

    # Statistics
    total_students: int = 0
    total_teachers: int = 0

    # Storage
    storage_quota_mb: int = 0
    storage_used_mb: int = 0

    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'id': self.id,
            'name': self.name,
            'course_code': self.course_code,
            'full_name': self.full_name,
            'short_name': self.short_name,
            'term': self.term,
            'semester': self.semester,
            'year': self.year,
            'workflow_state': self.workflow_state,
            'enrollment_status': self.enrollment_status,
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'created_date': self.created_date.isoformat() if self.created_date else None,
            'description': self.description,
            'syllabus_body': self.syllabus_body,
            'default_view': self.default_view,
            'total_students': self.total_students,
            'total_teachers': self.total_teachers,
            'storage_quota_mb': self.storage_quota_mb,
            'storage_used_mb': self.storage_used_mb,
            'metadata': self.metadata
        }


class CourseParser:
    """
    Canvas Course Information Parser

    This class handles parsing and normalizing Canvas course objects into
    standardized course information structures for use by the downloader system.
    """

    def __init__(self):
        """Initialize the course parser."""
        self.logger = get_logger(__name__)

        # Semester detection patterns
        self.semester_patterns = {
            r'spring|spr': 'Spring',
            r'summer|sum': 'Summer',
            r'fall|fal|autumn|aut': 'Fall',
            r'winter|win': 'Winter',
            r'intersession|inter': 'Intersession',
            r'session.*1': 'Session 1',
            r'session.*2': 'Session 2'
        }

        # Year extraction pattern
        self.year_pattern = r'20\d{2}'

        # Course code cleaning patterns
        self.course_code_patterns = {
            r'[^\w\-\_\.]': '_',  # Replace non-alphanumeric chars with underscore
            r'_{2,}': '_',        # Replace multiple underscores with single
            r'^_|_$': ''          # Remove leading/trailing underscores
        }

    def parse_course(self, canvas_course) -> ParsedCourse:
        """
        Parse a Canvas course object into standardized course information.

        Args:
            canvas_course: Canvas course object from canvasapi

        Returns:
            ParsedCourse: Standardized course information
        """
        try:
            # Extract basic information
            course_id = str(getattr(canvas_course, 'id', 'unknown'))
            name = getattr(canvas_course, 'name', 'Unknown Course')
            course_code = getattr(canvas_course, 'course_code', '')

            # Create parsed course object
            parsed_course = ParsedCourse(
                id=course_id,
                name=name,
                course_code=course_code
            )

            # Generate display names
            parsed_course.full_name = self._generate_full_name(name, course_code)
            parsed_course.short_name = self._generate_short_name(name, course_code)

            # Parse term information
            parsed_course.term = self._parse_term_info(canvas_course)
            parsed_course.semester = parsed_course.term.get('semester', '')
            parsed_course.year = parsed_course.term.get('year', 0)

            # Extract status information
            parsed_course.workflow_state = getattr(canvas_course, 'workflow_state', 'unknown')

            # Parse dates
            parsed_course.start_date = self._parse_date(getattr(canvas_course, 'start_at', None))
            parsed_course.end_date = self._parse_date(getattr(canvas_course, 'end_at', None))
            parsed_course.created_date = self._parse_date(getattr(canvas_course, 'created_at', None))

            # Extract additional information
            parsed_course.description = getattr(canvas_course, 'public_description', '') or ''
            parsed_course.syllabus_body = getattr(canvas_course, 'syllabus_body', '') or ''
            parsed_course.default_view = getattr(canvas_course, 'default_view', 'modules')

            # Extract storage information
            parsed_course.storage_quota_mb = self._bytes_to_mb(
                getattr(canvas_course, 'storage_quota', 0)
            )

            # Store additional metadata
            parsed_course.metadata = self._extract_metadata(canvas_course)

            self.logger.debug("Parsed course information",
                            course_id=course_id,
                            course_name=name,
                            semester=parsed_course.semester,
                            year=parsed_course.year)

            return parsed_course

        except Exception as e:
            self.logger.error("Failed to parse course", exception=e)

            # Return minimal course info on error
            return ParsedCourse(
                id=str(getattr(canvas_course, 'id', 'unknown')),
                name=getattr(canvas_course, 'name', 'Unknown Course'),
                course_code=getattr(canvas_course, 'course_code', ''),
                full_name='Unknown Course'
            )

    def _generate_full_name(self, name: str, course_code: str) -> str:
        """
        Generate a full display name for the course.

        Args:
            name: Course name
            course_code: Course code

        Returns:
            str: Full course name
        """
        if course_code and course_code not in name:
            return f"{course_code} - {name}"
        return name

    def _generate_short_name(self, name: str, course_code: str) -> str:
        """
        Generate a short display name for the course.

        Args:
            name: Course name
            course_code: Course code

        Returns:
            str: Short course name
        """
        if course_code:
            return course_code

        # Extract first few words if no course code
        words = name.split()
        if len(words) <= 3:
            return name
        else:
            return ' '.join(words[:3]) + '...'

    def _parse_term_info(self, canvas_course) -> Dict[str, Any]:
        """
        Parse term information from the course.

        Args:
            canvas_course: Canvas course object

        Returns:
            Dict[str, Any]: Term information
        """
        term_info = {
            'id': None,
            'name': '',
            'semester': '',
            'year': 0,
            'start_at': None,
            'end_at': None
        }

        try:
            # Try to get term object
            if hasattr(canvas_course, 'term') and canvas_course.term:
                term = canvas_course.term
                term_info['id'] = getattr(term, 'id', None)
                term_info['name'] = getattr(term, 'name', '')
                term_info['start_at'] = self._parse_date(getattr(term, 'start_at', None))
                term_info['end_at'] = self._parse_date(getattr(term, 'end_at', None))

            # Try to get enrollment term
            elif hasattr(canvas_course, 'enrollment_term_id'):
                # This would require additional API call to get term details
                term_info['id'] = canvas_course.enrollment_term_id

            # Parse semester and year from term name or course name
            term_name = term_info['name']
            if not term_name:
                term_name = getattr(canvas_course, 'name', '')

            semester, year = self._extract_semester_year(term_name)
            term_info['semester'] = semester
            term_info['year'] = year

        except Exception as e:
            self.logger.warning("Could not parse term information", exception=e)

        return term_info

    def _extract_semester_year(self, text: str) -> tuple[str, int]:
        """
        Extract semester and year from text.

        Args:
            text: Text to parse

        Returns:
            Tuple of (semester, year)
        """
        text_lower = text.lower()

        # Extract semester
        semester = ''
        for pattern, sem_name in self.semester_patterns.items():
            if re.search(pattern, text_lower):
                semester = sem_name
                break

        # Extract year
        year = 0
        year_match = re.search(self.year_pattern, text)
        if year_match:
            year = int(year_match.group())
        else:
            # Default to current year if not found
            year = datetime.now().year

        return semester, year

    def _parse_date(self, date_str: Union[str, datetime, None]) -> Optional[datetime]:
        """
        Parse date string or datetime object.

        Args:
            date_str: Date string or datetime object

        Returns:
            Optional[datetime]: Parsed datetime or None
        """
        if not date_str:
            return None

        if isinstance(date_str, datetime):
            return date_str

        try:
            # Try ISO format first
            if 'T' in str(date_str):
                return datetime.fromisoformat(str(date_str).replace('Z', '+00:00'))
            else:
                # Try other common formats
                for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y']:
                    try:
                        return datetime.strptime(str(date_str), fmt)
                    except ValueError:
                        continue
        except Exception as e:
            self.logger.warning(f"Could not parse date: {date_str}", exception=e)

        return None

    def _bytes_to_mb(self, bytes_value: Union[int, str, None]) -> int:
        """
        Convert bytes to megabytes.

        Args:
            bytes_value: Value in bytes

        Returns:
            int: Value in megabytes
        """
        if not bytes_value:
            return 0

        try:
            return int(int(bytes_value) / (1024 * 1024))
        except (ValueError, TypeError):
            return 0

    def _extract_metadata(self, canvas_course) -> Dict[str, Any]:
        """
        Extract additional metadata from course object.

        Args:
            canvas_course: Canvas course object

        Returns:
            Dict[str, Any]: Additional metadata
        """
        metadata = {}

        # Extract various course attributes
        attributes = [
            'account_id', 'root_account_id', 'enrollment_term_id',
            'grading_standard_id', 'grade_passback_setting',
            'public_syllabus', 'public_syllabus_to_auth',
            'apply_assignment_group_weights', 'license',
            'is_public', 'is_public_to_auth_users',
            'restrict_enrollments_to_course_dates',
            'friendly_name', 'course_format', 'time_zone'
        ]

        for attr in attributes:
            if hasattr(canvas_course, attr):
                value = getattr(canvas_course, attr)
                if value is not None:
                    metadata[attr] = value

        return metadata

    def generate_folder_name(self, parsed_course: ParsedCourse,
                           template: str = "{course_code}-{course_name}") -> str:
        """
        Generate a folder name for the course.

        Args:
            parsed_course: Parsed course information
            template: Folder name template

        Returns:
            str: Generated folder name
        """
        # Prepare template variables
        variables = {
            'course_code': self._sanitize_folder_name(parsed_course.course_code),
            'course_name': self._sanitize_folder_name(parsed_course.name),
            'short_name': self._sanitize_folder_name(parsed_course.short_name),
            'semester': parsed_course.semester,
            'year': str(parsed_course.year),
            'term_name': self._sanitize_folder_name(parsed_course.term.get('name', '')),
            'course_id': parsed_course.id
        }

        try:
            # Generate folder name from template
            folder_name = template.format(**variables)

            # Clean up the result
            folder_name = self._sanitize_folder_name(folder_name)

            # Ensure it's not empty
            if not folder_name.strip():
                folder_name = f"Course_{parsed_course.id}"

            return folder_name

        except Exception as e:
            self.logger.warning("Could not generate folder name from template",
                              template=template, exception=e)

            # Fallback to simple name
            if parsed_course.course_code:
                return self._sanitize_folder_name(f"{parsed_course.course_code}-{parsed_course.name}")
            else:
                return self._sanitize_folder_name(parsed_course.name)

    def _sanitize_folder_name(self, name: str, max_length: int = 100) -> str:
        """
        Sanitize a string for use as a folder name.

        Args:
            name: Original name
            max_length: Maximum allowed length

        Returns:
            str: Sanitized folder name
        """
        if not name:
            return ""

        # Apply cleaning patterns
        sanitized = name
        for pattern, replacement in self.course_code_patterns.items():
            sanitized = re.sub(pattern, replacement, sanitized)

        # Remove excessive whitespace
        sanitized = re.sub(r'\s+', ' ', sanitized)
        sanitized = sanitized.strip()

        # Truncate if too long
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length].rstrip()

        return sanitized

    def parse_multiple_courses(self, canvas_courses: List) -> List[ParsedCourse]:
        """
        Parse multiple Canvas courses.

        Args:
            canvas_courses: List of Canvas course objects

        Returns:
            List[ParsedCourse]: List of parsed course information
        """
        parsed_courses = []

        for course in canvas_courses:
            try:
                parsed_course = self.parse_course(course)
                parsed_courses.append(parsed_course)
            except Exception as e:
                self.logger.error("Failed to parse course in batch",
                                course_id=getattr(course, 'id', 'unknown'),
                                exception=e)

        self.logger.info(f"Parsed {len(parsed_courses)} courses from {len(canvas_courses)} total")

        return parsed_courses


def create_course_parser(config=None) -> CourseParser:
    """
    Factory function to create a CourseParser instance.

    This factory function provides a consistent interface for creating
    CourseParser instances throughout the application, following the
    same pattern used by other modules.

    Args:
        config: Optional configuration object (currently unused but
                provided for future extensibility)

    Returns:
        CourseParser: A new CourseParser instance

    Example:
        # Create a parser using the factory function
        parser = create_course_parser()

        # Parse a Canvas course object
        parsed_course = parser.parse_course(canvas_course)
    """
    return CourseParser()