"""
Course Parser Module

This module handles parsing of Canvas course names and organizing them into
a structured folder hierarchy. It supports various course naming formats and
provides flexible folder organization patterns.

The parser handles course names in the format:
"Subject name (CODE) - Subsection - Year - Semester"

Examples:
- "Álgebra Lineal (CC1103) - Teoría 2 - 2024 - 1"
- "Cálculo Vectorial (CC1104) - Teoría 2.04 - 2025 - 1"
- "Programming Fundamentals (CS101) - Lab A - 2024 - 2"

And organizes them into folder structures like:
- "2025/1 Semester/Cálculo Vectorial (CC1104) Teoría 2.04/"
- "2024/2 Semester/Programming Fundamentals (CS101) Lab A/"

Features:
- Flexible course name parsing with multiple fallback patterns
- Configurable folder structure templates
- International character support (accents, special characters)
- Semester name localization support
- Validation and sanitization of folder names
- Support for edge cases and malformed course names
- Course grouping and organization utilities

Usage:
    # Initialize course parser
    parser = CourseParser()

    # Parse a course name
    course_info = parser.parse_course_name("Álgebra Lineal (CC1103) - Teoría 2 - 2024 - 1")

    # Generate folder path
    folder_path = parser.generate_folder_path(course_info)
    # Result: "2024/1 Semester/Álgebra Lineal (CC1103) Teoría 2"

    # Parse multiple courses and organize them
    courses = parser.organize_courses_by_semester(course_list)
"""

import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass, field
from datetime import datetime
import locale

from ..config.settings import get_config
from ..utils.logger import get_logger


@dataclass
class ParsedCourse:
    """
    Data class representing a parsed Canvas course.

    This class contains all the extracted information from a course name
    along with metadata and organizational information.
    """
    # Original course information
    original_name: str = ""
    course_id: str = ""

    # Parsed components
    subject_name: str = ""
    subject_code: str = ""
    subsection: str = ""
    year: str = ""
    semester: str = ""

    # Additional metadata
    is_parsed_successfully: bool = False
    parsing_confidence: float = 0.0  # 0.0 to 1.0
    fallback_used: bool = False

    # Organizational information
    folder_name: str = ""
    folder_path: str = ""
    semester_folder: str = ""
    year_folder: str = ""

    # Canvas-specific information
    workflow_state: str = ""
    enrollment_term_id: Optional[int] = None
    start_date: str = ""
    end_date: str = ""

    def __post_init__(self):
        """Post-initialization processing."""
        if not self.folder_name and self.is_parsed_successfully:
            self.folder_name = self._generate_folder_name()

    def _generate_folder_name(self) -> str:
        """Generate a folder name from parsed components."""
        parts = []

        # Add subject name and code
        if self.subject_name:
            if self.subject_code:
                parts.append(f"{self.subject_name} ({self.subject_code})")
            else:
                parts.append(self.subject_name)

        # Add subsection if available
        if self.subsection:
            parts.append(self.subsection)

        return " ".join(parts) if parts else self.original_name

    @property
    def academic_period(self) -> str:
        """Get academic period in format 'YYYY-S' (e.g., '2024-1')."""
        if self.year and self.semester:
            return f"{self.year}-{self.semester}"
        return ""

    @property
    def sort_key(self) -> Tuple[str, str, str]:
        """Get sort key for organizing courses."""
        return (self.year or "9999", self.semester or "9", self.subject_code or "ZZZZ")


class CourseNamePattern:
    """
    Class representing a course name parsing pattern.

    This class defines a regex pattern and extraction logic for parsing
    course names in various formats.
    """

    def __init__(self, name: str, pattern: str, groups: Dict[str, int],
                 confidence: float = 1.0, description: str = ""):
        """
        Initialize a course name pattern.

        Args:
            name: Pattern name identifier
            pattern: Regular expression pattern
            groups: Mapping of component names to regex group numbers
            confidence: Confidence score for this pattern (0.0 to 1.0)
            description: Human-readable description of the pattern
        """
        self.name = name
        self.pattern = re.compile(pattern, re.IGNORECASE | re.UNICODE)
        self.groups = groups
        self.confidence = confidence
        self.description = description

    def match(self, course_name: str) -> Optional[Dict[str, str]]:
        """
        Try to match the course name against this pattern.

        Args:
            course_name: Course name to match

        Returns:
            Optional[Dict[str, str]]: Extracted components or None if no match
        """
        match = self.pattern.match(course_name.strip())
        if not match:
            return None

        components = {}
        for component, group_num in self.groups.items():
            try:
                value = match.group(group_num)
                components[component] = value.strip() if value else ""
            except (IndexError, AttributeError):
                components[component] = ""

        # Add confidence and pattern information
        components['_confidence'] = self.confidence
        components['_pattern_name'] = self.name

        return components


class CourseParser:
    """
    Canvas Course Name Parser

    This class provides comprehensive course name parsing functionality,
    extracting structured information from Canvas course names and organizing
    them into a logical folder hierarchy.

    The parser supports multiple naming patterns and provides fallback mechanisms
    for handling edge cases and malformed course names.
    """

    def __init__(self, config=None):
        """
        Initialize the course parser.

        Args:
            config: Optional configuration object. If None, uses global config.
        """
        self.config = config or get_config()
        self.logger = get_logger(__name__)

        # Initialize parsing patterns
        self._patterns = self._initialize_patterns()

        # Semester name mappings for localization
        self._semester_names = self._initialize_semester_names()

        # Statistics
        self._parsing_stats = {
            'total_parsed': 0,
            'successful_parses': 0,
            'fallback_parses': 0,
            'failed_parses': 0,
            'pattern_usage': {}
        }

        self.logger.info("Course parser initialized",
                         pattern_count=len(self._patterns),
                         locale=self.config.date_format.locale)

    def _initialize_patterns(self) -> List[CourseNamePattern]:
        """
        Initialize course name parsing patterns.

        Returns:
            List[CourseNamePattern]: List of parsing patterns ordered by confidence
        """
        patterns = [
            # Standard format: "Subject (CODE) - Section - Year - Semester"
            CourseNamePattern(
                name="standard_full",
                pattern=r'^(.+?)\s*\(([^)]+)\)\s*-\s*(.+?)\s*-\s*(\d{4})\s*-\s*(.+)$',
                groups={
                    'subject_name': 1,
                    'subject_code': 2,
                    'subsection': 3,
                    'year': 4,
                    'semester': 5
                },
                confidence=1.0,
                description="Full standard format with all components"
            ),

            # Without year: "Subject (CODE) - Section - Semester"
            CourseNamePattern(
                name="no_year",
                pattern=r'^(.+?)\s*\(([^)]+)\)\s*-\s*(.+?)\s*-\s*([12])$',
                groups={
                    'subject_name': 1,
                    'subject_code': 2,
                    'subsection': 3,
                    'semester': 4
                },
                confidence=0.8,
                description="Standard format without year"
            ),

            # Without section: "Subject (CODE) - Year - Semester"
            CourseNamePattern(
                name="no_section",
                pattern=r'^(.+?)\s*\(([^)]+)\)\s*-\s*(\d{4})\s*-\s*(.+)$',
                groups={
                    'subject_name': 1,
                    'subject_code': 2,
                    'year': 3,
                    'semester': 4
                },
                confidence=0.9,
                description="Standard format without section"
            ),

            # Only subject and code: "Subject (CODE)"
            CourseNamePattern(
                name="subject_code_only",
                pattern=r'^(.+?)\s*\(([^)]+)\)\s*(.*)$',
                groups={
                    'subject_name': 1,
                    'subject_code': 2,
                    'remainder': 3
                },
                confidence=0.6,
                description="Subject and code with optional remainder"
            ),

            # Year and semester at the end: "Subject - Year - Semester"
            CourseNamePattern(
                name="year_semester_end",
                pattern=r'^(.+?)\s*-\s*(\d{4})\s*-\s*([12])$',
                groups={
                    'subject_name': 1,
                    'year': 2,
                    'semester': 3
                },
                confidence=0.7,
                description="Subject with year and semester at end"
            ),

            # With semester name: "Subject (CODE) - Section - Year - Spring/Fall"
            CourseNamePattern(
                name="semester_name",
                pattern=r'^(.+?)\s*\(([^)]+)\)\s*-\s*(.+?)\s*-\s*(\d{4})\s*-\s*(Spring|Fall|Summer|Winter|Primavera|Otoño|Verano|Invierno)$',
                groups={
                    'subject_name': 1,
                    'subject_code': 2,
                    'subsection': 3,
                    'year': 4,
                    'semester_name': 5
                },
                confidence=0.9,
                description="Format with semester names instead of numbers"
            ),

            # Alternative separators (using | or ;)
            CourseNamePattern(
                name="alternative_separators",
                pattern=r'^(.+?)\s*\(([^)]+)\)\s*[|;]\s*(.+?)\s*[|;]\s*(\d{4})\s*[|;]\s*(.+)$',
                groups={
                    'subject_name': 1,
                    'subject_code': 2,
                    'subsection': 3,
                    'year': 4,
                    'semester': 5
                },
                confidence=0.8,
                description="Alternative separators (| or ;)"
            )
        ]

        return sorted(patterns, key=lambda p: p.confidence, reverse=True)

    def _initialize_semester_names(self) -> Dict[str, str]:
        """
        Initialize semester name mappings for localization.

        Returns:
            Dict[str, str]: Mapping of semester names to numbers
        """
        return {
            # English
            'spring': '1',
            'fall': '2',
            'autumn': '2',
            'summer': '3',
            'winter': '4',

            # Spanish
            'primavera': '1',
            'otoño': '2',
            'verano': '3',
            'invierno': '4',

            # Numbers
            '1': '1',
            '2': '2',
            '3': '3',
            '4': '4',

            # Full names
            'primer semestre': '1',
            'segundo semestre': '2',
            'first semester': '1',
            'second semester': '2'
        }

    def parse_course_name(self, course_name: str, course_id: str = "") -> ParsedCourse:
        """
        Parse a Canvas course name into structured components.

        This method attempts to extract subject name, code, section, year, and
        semester information from a course name using multiple parsing patterns.

        Args:
            course_name: The full course name from Canvas
            course_id: Optional course ID for reference

        Returns:
            ParsedCourse: Parsed course information
        """
        self._parsing_stats['total_parsed'] += 1

        # Create result object
        result = ParsedCourse(
            original_name=course_name,
            course_id=course_id
        )

        if not course_name or not course_name.strip():
            self.logger.warning("Empty course name provided")
            self._parsing_stats['failed_parses'] += 1
            return result

        # Clean the course name
        clean_name = self._clean_course_name(course_name)

        # Try each pattern in order of confidence
        for pattern in self._patterns:
            components = pattern.match(clean_name)
            if components:
                # Successfully matched pattern
                result = self._extract_components(components, result)
                result.is_parsed_successfully = True
                result.parsing_confidence = components.get('_confidence', 0.0)

                # Update statistics
                self._parsing_stats['successful_parses'] += 1
                pattern_name = components.get('_pattern_name', 'unknown')
                self._parsing_stats['pattern_usage'][pattern_name] = \
                    self._parsing_stats['pattern_usage'].get(pattern_name, 0) + 1

                self.logger.debug(f"Successfully parsed course name",
                                  course_name=course_name,
                                  pattern=pattern_name,
                                  confidence=result.parsing_confidence)

                break

        # If no pattern matched, try fallback parsing
        if not result.is_parsed_successfully:
            result = self._fallback_parse(clean_name, result)
            self._parsing_stats['fallback_parses'] += 1

        # Post-process and validate
        result = self._post_process_course(result)

        return result

    def _clean_course_name(self, course_name: str) -> str:
        """
        Clean and normalize a course name for parsing.

        Args:
            course_name: Raw course name

        Returns:
            str: Cleaned course name
        """
        # Remove extra whitespace
        cleaned = ' '.join(course_name.split())

        # Normalize Unicode characters (handle accents, etc.)
        cleaned = unicodedata.normalize('NFKC', cleaned)

        # Remove common prefixes/suffixes that might interfere with parsing
        prefixes_to_remove = [
            'Course:', 'Curso:', 'Class:', 'Clase:',
            '[ONLINE]', '[PRESENCIAL]', '[HYBRID]'
        ]

        for prefix in prefixes_to_remove:
            if cleaned.upper().startswith(prefix.upper()):
                cleaned = cleaned[len(prefix):].strip()

        return cleaned

    def _extract_components(self, components: Dict[str, str], result: ParsedCourse) -> ParsedCourse:
        """
        Extract components from regex match groups into ParsedCourse object.

        Args:
            components: Dictionary of matched components
            result: ParsedCourse object to populate

        Returns:
            ParsedCourse: Updated course object
        """
        # Extract basic components
        result.subject_name = components.get('subject_name', '').strip()
        result.subject_code = components.get('subject_code', '').strip()
        result.subsection = components.get('subsection', '').strip()
        result.year = components.get('year', '').strip()
        result.semester = components.get('semester', '').strip()

        # Handle semester name to number conversion
        semester_name = components.get('semester_name', '').strip().lower()
        if semester_name and semester_name in self._semester_names:
            result.semester = self._semester_names[semester_name]

        # Handle remainder field (from subject_code_only pattern)
        remainder = components.get('remainder', '').strip()
        if remainder and not result.subsection:
            # Try to extract additional info from remainder
            remainder_parts = [part.strip() for part in remainder.split('-') if part.strip()]

            for part in remainder_parts:
                # Check if it's a year
                if re.match(r'^\d{4}$', part) and not result.year:
                    result.year = part
                # Check if it's a semester number
                elif re.match(r'^[1-4]$', part) and not result.semester:
                    result.semester = part
                # Otherwise, it might be a section
                elif not result.subsection:
                    result.subsection = part

        # Infer missing year if possible (use current year as fallback)
        if not result.year:
            current_year = datetime.now().year
            result.year = str(current_year)
            self.logger.debug(f"Inferred year as {current_year} for course",
                              course_name=result.original_name)

        return result

    def _fallback_parse(self, course_name: str, result: ParsedCourse) -> ParsedCourse:
        """
        Attempt fallback parsing for courses that don't match standard patterns.

        Args:
            course_name: Course name to parse
            result: ParsedCourse object to populate

        Returns:
            ParsedCourse: Updated course object
        """
        result.fallback_used = True
        result.parsing_confidence = 0.3

        # Try to extract any parenthetical content as subject code
        code_match = re.search(r'\(([^)]+)\)', course_name)
        if code_match:
            result.subject_code = code_match.group(1).strip()
            # Remove the code from the name to get subject name
            subject_name = course_name.replace(code_match.group(0), '').strip()
            result.subject_name = subject_name
        else:
            # Use the entire name as subject name
            result.subject_name = course_name

        # Try to find year anywhere in the string
        year_match = re.search(r'\b(20\d{2})\b', course_name)
        if year_match:
            result.year = year_match.group(1)
        else:
            result.year = str(datetime.now().year)

        # Try to find semester number
        semester_match = re.search(r'\b([1-2])\b', course_name)
        if semester_match:
            result.semester = semester_match.group(1)
        else:
            result.semester = "1"  # Default to first semester

        self.logger.warning(f"Used fallback parsing for course",
                            course_name=course_name,
                            extracted_code=result.subject_code,
                            extracted_year=result.year)

        result.is_parsed_successfully = True  # Mark as successful even if fallback
        return result

    def _post_process_course(self, result: ParsedCourse) -> ParsedCourse:
        """
        Post-process a parsed course to clean and validate components.

        Args:
            result: ParsedCourse object to process

        Returns:
            ParsedCourse: Processed course object
        """
        # Clean and validate subject name
        if result.subject_name:
            result.subject_name = self._clean_subject_name(result.subject_name)

        # Clean and validate subject code
        if result.subject_code:
            result.subject_code = self._clean_subject_code(result.subject_code)

        # Validate year
        if result.year:
            result.year = self._validate_year(result.year)

        # Validate semester
        if result.semester:
            result.semester = self._validate_semester(result.semester)

        # Generate folder names
        result.folder_name = result._generate_folder_name()
        result.folder_path = self.generate_folder_path(result)

        return result

    def _clean_subject_name(self, subject_name: str) -> str:
        """Clean and normalize subject name."""
        # Remove extra whitespace
        cleaned = ' '.join(subject_name.split())

        # Capitalize properly (title case but preserve known abbreviations)
        # This is a simple implementation - could be enhanced with a dictionary
        # of known terms that should remain uppercase
        if cleaned.isupper():
            cleaned = cleaned.title()

        return cleaned

    def _clean_subject_code(self, subject_code: str) -> str:
        """Clean and normalize subject code."""
        # Remove whitespace and convert to uppercase
        return subject_code.strip().upper()

    def _validate_year(self, year: str) -> str:
        """Validate and normalize year."""
        try:
            year_int = int(year)
            current_year = datetime.now().year

            # Reasonable range check (Canvas has been around since ~2008)
            if 2008 <= year_int <= current_year + 5:
                return str(year_int)
            else:
                self.logger.warning(f"Year {year} seems unreasonable, using current year")
                return str(current_year)
        except ValueError:
            self.logger.warning(f"Invalid year format: {year}")
            return str(datetime.now().year)

    def _validate_semester(self, semester: str) -> str:
        """Validate and normalize semester."""
        # Convert to string and check if it's a valid semester number
        semester = str(semester).strip()

        if semester in ['1', '2', '3', '4']:
            return semester

        # Try to convert semester names
        semester_lower = semester.lower()
        if semester_lower in self._semester_names:
            return self._semester_names[semester_lower]

        # Default to semester 1 if invalid
        self.logger.warning(f"Invalid semester: {semester}, defaulting to 1")
        return "1"

    def generate_folder_path(self, course: ParsedCourse) -> str:
        """
        Generate a folder path for a parsed course.

        Args:
            course: ParsedCourse object

        Returns:
            str: Generated folder path
        """
        # Get the folder structure template from config
        template = self.config.date_format.folder_structure

        # Generate semester folder name
        semester_name = self._get_semester_name(course.semester)

        # Prepare template variables
        template_vars = {
            'year': course.year,
            'semester': semester_name,
            'course_name': course.folder_name,
            'subject_code': course.subject_code,
            'subject_name': course.subject_name,
            'subsection': course.subsection
        }

        try:
            # Apply the template
            folder_path = template.format(**template_vars)

            # Clean the path for file system compatibility
            folder_path = self._sanitize_path(folder_path)

            return folder_path

        except KeyError as e:
            self.logger.error(f"Template variable not found: {e}")
            # Fallback to simple structure
            return f"{course.year}/{semester_name}/{course.folder_name}"

    def _get_semester_name(self, semester_number: str) -> str:
        """
        Get localized semester name from semester number.

        Args:
            semester_number: Semester number (1, 2, 3, 4)

        Returns:
            str: Localized semester name
        """
        semester_names = {
            '1': '1 Semester',
            '2': '2 Semester',
            '3': '3 Semester',
            '4': '4 Semester'
        }

        # Could be enhanced with locale-specific names
        locale_code = self.config.date_format.locale.lower()

        if locale_code.startswith('es'):  # Spanish
            semester_names = {
                '1': '1 Semestre',
                '2': '2 Semestre',
                '3': '3 Semestre',
                '4': '4 Semestre'
            }

        return semester_names.get(semester_number, f"{semester_number} Semester")

    def _sanitize_path(self, path: str) -> str:
        """
        Sanitize a path for file system compatibility.

        Args:
            path: Path to sanitize

        Returns:
            str: Sanitized path
        """
        # Characters that are problematic in file paths
        invalid_chars = {
            '<': '',
            '>': '',
            ':': '-',
            '"': "'",
            '/': '-',  # Will be handled by Path
            '\\': '-',  # Will be handled by Path
            '|': '-',
            '?': '',
            '*': '',
            '\0': ''
        }

        # Replace invalid characters
        for char, replacement in invalid_chars.items():
            path = path.replace(char, replacement)

        # Remove multiple spaces and normalize
        path = ' '.join(path.split())

        # Use Path to handle OS-specific path separators
        path_obj = Path(path)
        return str(path_obj)

    def organize_courses_by_semester(self, courses: List[Union[Dict[str, Any], ParsedCourse]]) -> Dict[
        str, List[ParsedCourse]]:
        """
        Organize a list of courses by academic semester.

        Args:
            courses: List of course dictionaries or ParsedCourse objects

        Returns:
            Dict[str, List[ParsedCourse]]: Courses organized by semester key
        """
        organized = {}

        for course in courses:
            # Convert to ParsedCourse if needed
            if isinstance(course, dict):
                course_name = course.get('name', '')
                course_id = str(course.get('id', ''))
                parsed = self.parse_course_name(course_name, course_id)
            else:
                parsed = course

            # Create semester key
            semester_key = f"{parsed.year}-{parsed.semester}"

            if semester_key not in organized:
                organized[semester_key] = []

            organized[semester_key].append(parsed)

        # Sort courses within each semester
        for semester_key in organized:
            organized[semester_key].sort(key=lambda c: (c.subject_code, c.subsection))

        return organized

    def get_parsing_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about course name parsing performance.

        Returns:
            Dict[str, Any]: Parsing statistics
        """
        total = self._parsing_stats['total_parsed']

        if total == 0:
            return self._parsing_stats.copy()

        success_rate = (self._parsing_stats['successful_parses'] / total) * 100
        fallback_rate = (self._parsing_stats['fallback_parses'] / total) * 100
        failure_rate = (self._parsing_stats['failed_parses'] / total) * 100

        stats = self._parsing_stats.copy()
        stats.update({
            'success_rate_percent': round(success_rate, 2),
            'fallback_rate_percent': round(fallback_rate, 2),
            'failure_rate_percent': round(failure_rate, 2)
        })

        return stats

    def reset_statistics(self):
        """Reset parsing statistics."""
        self._parsing_stats = {
            'total_parsed': 0,
            'successful_parses': 0,
            'fallback_parses': 0,
            'failed_parses': 0,
            'pattern_usage': {}
        }

        self.logger.info("Course parser statistics reset")


def create_course_parser(**kwargs) -> CourseParser:
    """
    Factory function to create a course parser instance.

    Args:
        **kwargs: Arguments to pass to CourseParser constructor

    Returns:
        CourseParser: Configured course parser instance
    """
    return CourseParser(**kwargs)