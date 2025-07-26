"""
People Downloader Module

This module implements the people downloader for Canvas courses. It handles
downloading information about course participants including students, teachers,
TAs, and other enrolled users. This information is useful for understanding
course composition and contact information.

Canvas People include:
- Student enrollment information
- Instructor and TA details
- Course section assignments
- User profiles and contact information
- Enrollment status and dates
- User roles and permissions
- Avatar images and profile data

Features:
- Download course participant information
- Export enrollment data and statistics
- Handle different user roles and permissions
- Process profile information and avatars
- Create class rosters and contact lists
- Respect privacy settings and restrictions
- Generate enrollment analytics

Usage:
    # Initialize the downloader
    downloader = PeopleDownloader(canvas_client, progress_tracker)

    # Download all people information for a course
    stats = await downloader.download_course_content(course, course_info)
"""

import asyncio
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

import aiofiles
from canvasapi.user import User
from canvasapi.exceptions import CanvasException

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class PeopleDownloader(BaseDownloader):
    """
    Canvas People Downloader

    This class handles downloading information about course participants
    from Canvas courses. It processes enrollment data, user profiles,
    and generates useful reports about course composition.

    The downloader ensures that:
    - Participant information is preserved
    - Privacy settings are respected
    - Role-based organization is maintained
    - Contact information is available (when permitted)
    - Enrollment statistics are generated
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the people downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # People processing settings
        self.download_user_profiles = True
        self.download_avatars = False  # Privacy consideration
        self.include_contact_info = True
        self.create_class_roster = True
        self.export_enrollment_csv = True

        # Privacy and access settings
        self.respect_privacy_settings = True
        self.include_concluded_enrollments = False
        self.include_inactive_enrollments = False

        # Content processing options
        self.group_by_role = True
        self.group_by_section = True
        self.create_enrollment_analytics = True

        self.logger.info("People downloader initialized",
                         download_profiles=self.download_user_profiles,
                         respect_privacy=self.respect_privacy_settings)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "people"

    def fetch_content_list(self, course) -> List[Dict[str, Any]]:
        """
        Fetch all people (enrollments) from the course.

        Args:
            course: Canvas course object

        Returns:
            List[Dict[str, Any]]: List of enrollment/user information
        """
        try:
            self.logger.info(f"Fetching people for course {course.id}")

            # Define enrollment types to include
            enrollment_types = ['student', 'teacher', 'ta', 'observer', 'designer']

            # Define enrollment states
            enrollment_states = ['active']
            if self.include_concluded_enrollments:
                enrollment_states.append('completed')
            if self.include_inactive_enrollments:
                enrollment_states.append('inactive')

            people = []

            # Get enrollments with user information
            try:
                enrollments = list(course.get_enrollments(
                    type=enrollment_types,
                    state=enrollment_states,
                    include=['user', 'email', 'avatar_url', 'bio', 'course_section']
                ))

                for enrollment in enrollments:
                    try:
                        user = getattr(enrollment, 'user', None)
                        if user:
                            person_data = {
                                'type': 'enrollment',
                                'enrollment': enrollment,
                                'user': user,
                                'enrollment_id': getattr(enrollment, 'id', None),
                                'user_id': getattr(user, 'id', None)
                            }
                            people.append(person_data)

                    except Exception as e:
                        self.logger.warning(f"Could not process enrollment",
                                            enrollment_id=getattr(enrollment, 'id', 'unknown'),
                                            exception=e)

            except Exception as e:
                self.logger.warning(f"Could not fetch enrollments", exception=e)

            # Also try to get users directly (alternative approach)
            try:
                users = list(course.get_users(
                    enrollment_type=enrollment_types,
                    enrollment_state=enrollment_states,
                    include=['email', 'enrollments', 'avatar_url', 'bio']
                ))

                # Create a set of existing user IDs to avoid duplicates
                existing_user_ids = {p.get('user_id') for p in people if p.get('user_id')}

                for user in users:
                    user_id = getattr(user, 'id', None)
                    if user_id not in existing_user_ids:
                        person_data = {
                            'type': 'user',
                            'enrollment': None,
                            'user': user,
                            'enrollment_id': None,
                            'user_id': user_id
                        }
                        people.append(person_data)

            except Exception as e:
                self.logger.warning(f"Could not fetch users directly", exception=e)

            self.logger.info(f"Found {len(people)} people",
                             course_id=course.id,
                             people_count=len(people))

            return people

        except CanvasException as e:
            self.logger.error(f"Failed to fetch people", exception=e)
            raise DownloadError(f"Could not fetch people: {e}")

    def extract_metadata(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from a person/enrollment.

        Args:
            person_data: Person data dictionary

        Returns:
            Dict[str, Any]: Person metadata
        """
        try:
            enrollment = person_data.get('enrollment')
            user = person_data.get('user')

            if not user:
                return {'error': 'No user data available'}

            # Basic user information
            metadata = {
                'type': person_data.get('type', 'unknown'),
                'user_id': getattr(user, 'id', None),
                'name': getattr(user, 'name', ''),
                'sortable_name': getattr(user, 'sortable_name', ''),
                'short_name': getattr(user, 'short_name', ''),
                'sis_user_id': getattr(user, 'sis_user_id', ''),
                'integration_id': getattr(user, 'integration_id', ''),
                'login_id': getattr(user, 'login_id', ''),
                'email': getattr(user, 'email', ''),
                'locale': getattr(user, 'locale', ''),
                'effective_locale': getattr(user, 'effective_locale', ''),
                'bio': getattr(user, 'bio', ''),
                'pronouns': getattr(user, 'pronouns', ''),
                'avatar_url': getattr(user, 'avatar_url', ''),
                'calendar_url': getattr(user, 'calendar_url', ''),
                'time_zone': getattr(user, 'time_zone', ''),
                'title': getattr(user, 'title', '')
            }

            # Privacy considerations - remove sensitive info if needed
            if not self.include_contact_info:
                metadata['email'] = '[Privacy Protected]'
                metadata['login_id'] = '[Privacy Protected]'

            # Enrollment information (if available)
            if enrollment:
                metadata.update({
                    'enrollment_id': getattr(enrollment, 'id', None),
                    'enrollment_type': getattr(enrollment, 'type', ''),
                    'enrollment_role': getattr(enrollment, 'role', ''),
                    'enrollment_role_id': getattr(enrollment, 'role_id', None),
                    'enrollment_state': getattr(enrollment, 'enrollment_state', ''),
                    'course_section_id': getattr(enrollment, 'course_section_id', None),
                    'limit_privileges_to_course_section': getattr(enrollment, 'limit_privileges_to_course_section',
                                                                  False),
                    'sis_account_id': getattr(enrollment, 'sis_account_id', ''),
                    'sis_course_id': getattr(enrollment, 'sis_course_id', ''),
                    'sis_section_id': getattr(enrollment, 'sis_section_id', ''),
                    'sis_user_id_enrollment': getattr(enrollment, 'sis_user_id', ''),
                    'html_url': getattr(enrollment, 'html_url', ''),
                    'grades': getattr(enrollment, 'grades', {}),
                    'associated_user_id': getattr(enrollment, 'associated_user_id', None),
                    'last_activity_at': self._format_date(getattr(enrollment, 'last_activity_at', None)),
                    'total_activity_time': getattr(enrollment, 'total_activity_time', None)
                })

                # Date information
                metadata.update({
                    'created_at': self._format_date(getattr(enrollment, 'created_at', None)),
                    'updated_at': self._format_date(getattr(enrollment, 'updated_at', None)),
                    'start_at': self._format_date(getattr(enrollment, 'start_at', None)),
                    'end_at': self._format_date(getattr(enrollment, 'end_at', None)),
                })

                # Course section information
                course_section = getattr(enrollment, 'course_section', None)
                if course_section:
                    metadata['course_section'] = {
                        'id': getattr(course_section, 'id', None),
                        'name': getattr(course_section, 'name', ''),
                        'sis_section_id': getattr(course_section, 'sis_section_id', ''),
                        'integration_id': getattr(course_section, 'integration_id', ''),
                        'start_at': self._format_date(getattr(course_section, 'start_at', None)),
                        'end_at': self._format_date(getattr(course_section, 'end_at', None)),
                        'course_id': getattr(course_section, 'course_id', None),
                        'nonxlist_course_id': getattr(course_section, 'nonxlist_course_id', None),
                        'sis_course_id': getattr(course_section, 'sis_course_id', '')
                    }
            else:
                # Try to extract enrollment info from user object
                enrollments = getattr(user, 'enrollments', [])
                if enrollments:
                    # Use the first enrollment for this course
                    for enroll in enrollments:
                        if hasattr(enroll, 'course_id') and str(getattr(enroll, 'course_id', '')) == str(
                                person_data.get('course_id', '')):
                            metadata.update({
                                'enrollment_type': getattr(enroll, 'type', ''),
                                'enrollment_role': getattr(enroll, 'role', ''),
                                'enrollment_state': getattr(enroll, 'enrollment_state', '')
                            })
                            break

            # Categorize person type for organization
            metadata['person_category'] = self._categorize_person(metadata)

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract person metadata",
                              user_id=person_data.get('user_id', 'unknown'),
                              exception=e)

            # Return minimal metadata on error
            return {
                'user_id': person_data.get('user_id'),
                'name': 'Unknown Person',
                'enrollment_type': 'unknown',
                'error': f"Metadata extraction failed: {e}"
            }

    def _categorize_person(self, metadata: Dict[str, Any]) -> str:
        """
        Categorize a person based on their enrollment information.

        Args:
            metadata: Person metadata

        Returns:
            str: Person category
        """
        enrollment_type = metadata.get('enrollment_type', '').lower()
        enrollment_role = metadata.get('enrollment_role', '').lower()

        # Map enrollment types to categories
        if enrollment_type in ['teacher', 'instructor']:
            return 'instructor'
        elif enrollment_type in ['ta', 'teachingassistant', 'teaching_assistant']:
            return 'teaching_assistant'
        elif enrollment_type == 'student':
            return 'student'
        elif enrollment_type == 'observer':
            return 'observer'
        elif enrollment_type == 'designer':
            return 'designer'
        elif 'teacher' in enrollment_role or 'instructor' in enrollment_role:
            return 'instructor'
        elif 'ta' in enrollment_role or 'assistant' in enrollment_role:
            return 'teaching_assistant'
        else:
            return 'other'

    def get_download_info(self, person_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        Get download information for a person.

        People data is processed rather than directly downloaded.

        Args:
            person_data: Person data dictionary

        Returns:
            Optional[Dict[str, str]]: Download information or None
        """
        return None

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all people information for a course.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting people download for course",
                             course_name=course_info.get('full_name', 'Unknown'),
                             course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if people download is enabled
            if not self.config.is_content_type_enabled('people'):
                self.logger.info("People download is disabled")
                return self.stats

            # Fetch people
            people = self.fetch_content_list(course)

            if not people:
                self.logger.info("No people found in course")
                return self.stats

            self.stats['total_items'] = len(people)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(people))

            # Process each person
            items_metadata = []

            for index, person in enumerate(people, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(person)
                    metadata['item_number'] = index

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                    self.stats['downloaded_items'] += 1

                except Exception as e:
                    self.logger.error(f"Failed to process person",
                                      user_id=person.get('user_id', 'unknown'),
                                      exception=e)
                    self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            # Create organized reports
            await self._create_people_reports(items_metadata)

            # Export enrollment CSV
            if self.export_enrollment_csv:
                await self._export_enrollment_csv(items_metadata)

            # Create class roster
            if self.create_class_roster:
                await self._create_class_roster(items_metadata)

            # Create enrollment analytics
            if self.create_enrollment_analytics:
                await self._create_enrollment_analytics(items_metadata)

            self.logger.info(f"People download completed",
                             course_id=str(course.id),
                             **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"People download failed", exception=e)
            raise DownloadError(f"People download failed: {e}")

    async def _create_people_reports(self, people_metadata: List[Dict[str, Any]]):
        """Create organized people reports."""
        try:
            # Group by role
            if self.group_by_role:
                await self._create_role_based_reports(people_metadata)

            # Group by section
            if self.group_by_section:
                await self._create_section_based_reports(people_metadata)

        except Exception as e:
            self.logger.error(f"Failed to create people reports", exception=e)

    async def _create_role_based_reports(self, people_metadata: List[Dict[str, Any]]):
        """Create reports grouped by user roles."""
        try:
            # Group people by category
            by_category = {}
            for person in people_metadata:
                category = person.get('person_category', 'other')
                if category not in by_category:
                    by_category[category] = []
                by_category[category].append(person)

            # Create report for each category
            for category, people in by_category.items():
                report_filename = f"people_by_role_{category}.html"
                report_path = self.content_folder / report_filename

                # Sort people by name
                sorted_people = sorted(people, key=lambda x: x.get('sortable_name', x.get('name', '')))

                html_content = self._create_people_html_report(
                    f"{category.replace('_', ' ').title()} ({len(people)})",
                    sorted_people,
                    category
                )

                async with aiofiles.open(report_path, 'w', encoding='utf-8') as f:
                    await f.write(html_content)

                self.logger.debug(f"Created role-based report for {category}",
                                  file_path=str(report_path))

        except Exception as e:
            self.logger.error(f"Failed to create role-based reports", exception=e)

    async def _create_section_based_reports(self, people_metadata: List[Dict[str, Any]]):
        """Create reports grouped by course sections."""
        try:
            # Group people by section
            by_section = {}
            for person in people_metadata:
                section_info = person.get('course_section', {})
                section_name = section_info.get('name', 'No Section Assigned')

                if section_name not in by_section:
                    by_section[section_name] = []
                by_section[section_name].append(person)

            # Create report for each section
            for section_name, people in by_section.items():
                safe_section_name = self.sanitize_filename(section_name)
                report_filename = f"people_by_section_{safe_section_name}.html"
                report_path = self.content_folder / report_filename

                # Sort people by role then name
                sorted_people = sorted(people, key=lambda x: (
                    x.get('person_category', 'zz'),
                    x.get('sortable_name', x.get('name', ''))
                ))

                html_content = self._create_people_html_report(
                    f"Section: {section_name} ({len(people)})",
                    sorted_people,
                    'section'
                )

                async with aiofiles.open(report_path, 'w', encoding='utf-8') as f:
                    await f.write(html_content)

                self.logger.debug(f"Created section-based report for {section_name}",
                                  file_path=str(report_path))

        except Exception as e:
            self.logger.error(f"Failed to create section-based reports", exception=e)

    def _create_people_html_report(self, title: str, people: List[Dict[str, Any]],
                                   report_type: str) -> str:
        """Create HTML report for a group of people."""
        people_html = ""

        for person in people:
            name = person.get('name', 'Unknown')
            email = person.get('email', '') if self.include_contact_info else '[Privacy Protected]'
            role = person.get('enrollment_role', person.get('enrollment_type', 'Unknown'))
            status = person.get('enrollment_state', 'Unknown')
            last_activity = person.get('last_activity_at', '')[:10] if person.get('last_activity_at') else 'N/A'

            people_html += f"""
            <tr>
                <td>{name}</td>
                <td>{email}</td>
                <td>{role.replace('_', ' ').title()}</td>
                <td>{status.title()}</td>
                <td>{last_activity}</td>
            </tr>
            """

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Course People: {title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .summary {{
            background-color: #f0f8ff;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #f5f5f5;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        .privacy-note {{
            background-color: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 4px;
            padding: 10px;
            margin: 20px 0;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <h1>👥 {title}</h1>

    <div class="summary">
        <p><strong>Total People:</strong> {len(people)}</p>
        <p><strong>Report Type:</strong> {report_type.replace('_', ' ').title()}</p>
    </div>

    {'''<div class="privacy-note">
        <strong>Privacy Notice:</strong> Contact information has been protected per privacy settings.
    </div>''' if not self.include_contact_info else ''}

    <table>
        <thead>
            <tr>
                <th>Name</th>
                <th>Email</th>
                <th>Role</th>
                <th>Status</th>
                <th>Last Activity</th>
            </tr>
        </thead>
        <tbody>
            {people_html}
        </tbody>
    </table>

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    async def _export_enrollment_csv(self, people_metadata: List[Dict[str, Any]]):
        """Export enrollment data to CSV format."""
        try:
            csv_filename = "course_enrollments.csv"
            csv_path = self.content_folder / csv_filename

            # Prepare CSV data
            fieldnames = [
                'Name',
                'Sortable Name',
                'Email',
                'User ID',
                'SIS User ID',
                'Enrollment Type',
                'Role',
                'Enrollment State',
                'Section',
                'Created Date',
                'Last Activity',
                'Total Activity Time'
            ]

            if not self.include_contact_info:
                # Remove sensitive fields if privacy protection is enabled
                fieldnames = [f for f in fieldnames if f not in ['Email', 'SIS User ID']]

            async with aiofiles.open(csv_path, 'w', encoding='utf-8', newline='') as csvfile:
                # Write header
                await csvfile.write(','.join(fieldnames) + '\n')

                # Write data rows
                for person in people_metadata:
                    row_data = {
                        'Name': person.get('name', ''),
                        'Sortable Name': person.get('sortable_name', ''),
                        'Email': person.get('email', '') if self.include_contact_info else '[Protected]',
                        'User ID': str(person.get('user_id', '')),
                        'SIS User ID': person.get('sis_user_id', '') if self.include_contact_info else '[Protected]',
                        'Enrollment Type': person.get('enrollment_type', ''),
                        'Role': person.get('enrollment_role', ''),
                        'Enrollment State': person.get('enrollment_state', ''),
                        'Section': person.get('course_section', {}).get('name', ''),
                        'Created Date': person.get('created_at', '')[:10] if person.get('created_at') else '',
                        'Last Activity': person.get('last_activity_at', '')[:10] if person.get(
                            'last_activity_at') else '',
                        'Total Activity Time': str(person.get('total_activity_time', ''))
                    }

                    # Filter row data based on fieldnames
                    row = [row_data.get(field, '') for field in fieldnames]

                    # Escape commas and quotes in CSV
                    escaped_row = []
                    for field in row:
                        field_str = str(field)
                        if ',' in field_str or '"' in field_str:
                            field_str = '"' + field_str.replace('"', '""') + '"'
                        escaped_row.append(field_str)

                    await csvfile.write(','.join(escaped_row) + '\n')

            self.logger.debug(f"Exported enrollment CSV",
                              file_path=str(csv_path))

        except Exception as e:
            self.logger.error(f"Failed to export enrollment CSV", exception=e)

    async def _create_class_roster(self, people_metadata: List[Dict[str, Any]]):
        """Create a printable class roster."""
        try:
            roster_filename = "class_roster.html"
            roster_path = self.content_folder / roster_filename

            # Filter and sort students
            students = [p for p in people_metadata if p.get('person_category') == 'student']
            students.sort(key=lambda x: x.get('sortable_name', x.get('name', '')))

            # Group by section if multiple sections
            sections = {}
            for student in students:
                section_name = student.get('course_section', {}).get('name', 'Default Section')
                if section_name not in sections:
                    sections[section_name] = []
                sections[section_name].append(student)

            sections_html = ""
            for section_name, section_students in sections.items():
                students_html = ""
                for i, student in enumerate(section_students, 1):
                    name = student.get('name', 'Unknown')
                    email = student.get('email', '') if self.include_contact_info else '[Protected]'

                    students_html += f"""
                    <tr>
                        <td>{i}</td>
                        <td>{name}</td>
                        <td>{email}</td>
                        <td style="border: 1px solid #ccc; width: 100px;">&nbsp;</td>
                    </tr>
                    """

                sections_html += f"""
                <div class="section">
                    <h2>{section_name} ({len(section_students)} students)</h2>
                    <table class="roster-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>Student Name</th>
                                <th>Email</th>
                                <th>Signature</th>
                            </tr>
                        </thead>
                        <tbody>
                            {students_html}
                        </tbody>
                    </table>
                </div>
                <div class="page-break"></div>
                """

            html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Class Roster</title>
    <style>
        @media print {{
            body {{ margin: 0; font-size: 10pt; }}
            .page-break {{ page-break-before: always; }}
            .no-print {{ display: none; }}
        }}
        body {{
            font-family: 'Times New Roman', serif;
            max-width: 100%;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.4;
        }}
        .roster-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        .roster-table th, .roster-table td {{
            border: 1px solid #000;
            padding: 8px;
            text-align: left;
        }}
        .roster-table th {{
            background-color: #f0f0f0;
            font-weight: bold;
        }}
        .section {{
            margin-bottom: 30px;
        }}
        h1 {{ text-align: center; }}
        h2 {{ 
            border-bottom: 2px solid #000; 
            padding-bottom: 5px;
        }}
        .header-info {{
            margin-bottom: 30px;
            border: 1px solid #000;
            padding: 15px;
        }}
    </style>
</head>
<body>
    <h1>CLASS ROSTER</h1>

    <div class="header-info">
        <p><strong>Date:</strong> _______________</p>
        <p><strong>Instructor:</strong> _______________</p>
        <p><strong>Total Students:</strong> {len(students)}</p>
    </div>

    <div class="no-print">
        <p><em>This is a printable class roster. Use your browser's print function to print.</em></p>
    </div>

    {sections_html}

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #000; font-size: 0.8em;">
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

            async with aiofiles.open(roster_path, 'w', encoding='utf-8') as f:
                await f.write(html_template)

            self.logger.debug(f"Created class roster",
                              file_path=str(roster_path))

        except Exception as e:
            self.logger.error(f"Failed to create class roster", exception=e)

    async def _create_enrollment_analytics(self, people_metadata: List[Dict[str, Any]]):
        """Create enrollment analytics and statistics."""
        try:
            analytics_filename = "enrollment_analytics.json"
            analytics_path = self.content_folder / analytics_filename

            # Calculate statistics
            total_people = len(people_metadata)

            # Count by category
            by_category = {}
            by_role = {}
            by_state = {}
            by_section = {}

            for person in people_metadata:
                # Category counts
                category = person.get('person_category', 'other')
                by_category[category] = by_category.get(category, 0) + 1

                # Role counts
                role = person.get('enrollment_role', 'unknown')
                by_role[role] = by_role.get(role, 0) + 1

                # State counts
                state = person.get('enrollment_state', 'unknown')
                by_state[state] = by_state.get(state, 0) + 1

                # Section counts
                section = person.get('course_section', {}).get('name', 'No Section')
                by_section[section] = by_section.get(section, 0) + 1

            # Activity analysis
            active_users = sum(1 for p in people_metadata if p.get('last_activity_at'))
            never_accessed = total_people - active_users

            # Calculate recent activity (last 30 days)
            from datetime import datetime, timedelta
            thirty_days_ago = datetime.now() - timedelta(days=30)

            recent_activity = 0
            for person in people_metadata:
                last_activity = person.get('last_activity_at')
                if last_activity:
                    try:
                        activity_date = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                        if activity_date > thirty_days_ago:
                            recent_activity += 1
                    except:
                        pass

            # Create analytics document
            analytics = {
                'generated_at': datetime.now().isoformat(),
                'summary': {
                    'total_people': total_people,
                    'active_users': active_users,
                    'never_accessed': never_accessed,
                    'recent_activity_30_days': recent_activity,
                    'activity_rate': (active_users / total_people * 100) if total_people > 0 else 0,
                    'recent_activity_rate': (recent_activity / total_people * 100) if total_people > 0 else 0
                },
                'breakdown_by_category': by_category,
                'breakdown_by_role': by_role,
                'breakdown_by_state': by_state,
                'breakdown_by_section': by_section,
                'insights': self._generate_enrollment_insights(by_category, by_state, active_users, total_people)
            }

            async with aiofiles.open(analytics_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(analytics, indent=2, ensure_ascii=False))

            self.logger.debug(f"Created enrollment analytics",
                              file_path=str(analytics_path))

        except Exception as e:
            self.logger.error(f"Failed to create enrollment analytics", exception=e)

    def _generate_enrollment_insights(self, by_category: Dict[str, int],
                                      by_state: Dict[str, int],
                                      active_users: int, total_people: int) -> List[str]:
        """Generate insights from enrollment data."""
        insights = []

        # Student-teacher ratio
        students = by_category.get('student', 0)
        instructors = by_category.get('instructor', 0) + by_category.get('teaching_assistant', 0)

        if instructors > 0:
            ratio = students / instructors
            insights.append(f"Student-to-instructor ratio: {ratio:.1f}:1")

        # Activity insights
        if total_people > 0:
            activity_rate = active_users / total_people * 100
            if activity_rate < 50:
                insights.append(f"Low activity rate: {activity_rate:.1f}% of users have accessed the course")
            elif activity_rate > 90:
                insights.append(f"High engagement: {activity_rate:.1f}% of users have accessed the course")

        # Enrollment state insights
        active_enrollments = by_state.get('active', 0)
        completed_enrollments = by_state.get('completed', 0)

        if completed_enrollments > 0:
            insights.append(f"{completed_enrollments} users have completed enrollment")

        # Section distribution
        section_count = len([k for k in by_category.keys() if k != 'No Section'])
        if section_count > 1:
            insights.append(f"Course has {section_count} sections")

        return insights

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

ContentDownloaderFactory.register_downloader('people', PeopleDownloader)