"""
Grades Downloader Module

This module implements the grades downloader for Canvas courses. It handles
downloading grade information, assignment scores, rubric assessments, and
grade-related feedback. This downloader focuses on the user's own grades
and feedback rather than accessing other students' information.

Canvas Grades include:
- Individual assignment grades and scores
- Rubric assessments and feedback
- Grade comments and instructor feedback
- Overall course grade and progress
- Grade history and submission attempts
- Grading schemes and scales
- Grade statistics and analytics

Features:
- Download personal grade information
- Export assignment scores and feedback
- Process rubric assessments
- Handle grade comments and feedback
- Create grade summary reports
- Export gradebook data (if accessible)
- Process grade history and changes

Usage:
    # Initialize the downloader
    downloader = GradesDownloader(canvas_client, progress_tracker)

    # Download all grades for a course
    stats = await downloader.download_course_content(course, course_info)
"""

import asyncio
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

import aiofiles
from canvasapi.exceptions import CanvasException

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class GradesDownloader(BaseDownloader):
    """
    Canvas Grades Downloader

    This class handles downloading grade information from Canvas courses.
    It focuses on the current user's grades, feedback, and grade-related
    data while respecting privacy and access restrictions.

    The downloader ensures that:
    - Personal grade information is preserved
    - Feedback and comments are included
    - Rubric assessments are captured
    - Grade history is maintained
    - Privacy restrictions are respected
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the grades downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # Grade processing settings
        self.download_assignment_grades = True
        self.download_grade_comments = True
        self.download_rubric_assessments = True
        self.create_grade_summary = True
        self.export_gradebook_csv = True

        # Privacy and access settings
        self.respect_privacy_settings = True
        self.only_user_grades = True  # Only download current user's grades

        # Content processing options
        self.include_grade_history = True
        self.process_feedback_attachments = True
        self.create_grade_analytics = True

        self.logger.info("Grades downloader initialized",
                         download_comments=self.download_grade_comments,
                         download_rubrics=self.download_rubric_assessments)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "grades"

    def fetch_content_list(self, course) -> List[Dict[str, Any]]:
        """
        Fetch grade-related information from the course.

        Args:
            course: Canvas course object

        Returns:
            List[Dict[str, Any]]: List of grade-related items
        """
        try:
            self.logger.info(f"Fetching grades for course {course.id}")

            grade_items = []

            # Get current user
            current_user = self.canvas_client.get_current_user()
            user_id = current_user['id']

            # Get assignments with grades
            try:
                assignments = list(course.get_assignments(
                    include=['submission', 'rubric_assessment']
                ))

                for assignment in assignments:
                    try:
                        # Get user's submission for this assignment
                        submission = assignment.get_submission(user_id, include=[
                            'submission_comments',
                            'rubric_assessment',
                            'assignment',
                            'course',
                            'user'
                        ])

                        if submission:
                            grade_item = {
                                'type': 'assignment_grade',
                                'assignment': assignment,
                                'submission': submission,
                                'assignment_id': assignment.id,
                                'user_id': user_id
                            }
                            grade_items.append(grade_item)

                    except Exception as e:
                        self.logger.warning(f"Could not get submission for assignment {assignment.id}",
                                            exception=e)

                        # Still add assignment info even without submission
                        grade_item = {
                            'type': 'assignment_grade',
                            'assignment': assignment,
                            'submission': None,
                            'assignment_id': assignment.id,
                            'user_id': user_id
                        }
                        grade_items.append(grade_item)

            except Exception as e:
                self.logger.warning(f"Could not fetch assignments", exception=e)

            # Get course enrollments (for overall grade)
            try:
                enrollments = list(course.get_enrollments(
                    user_id=user_id,
                    include=['current_grading_period_scores', 'total_scores']
                ))

                for enrollment in enrollments:
                    grade_item = {
                        'type': 'course_grade',
                        'enrollment': enrollment,
                        'user_id': user_id
                    }
                    grade_items.append(grade_item)

            except Exception as e:
                self.logger.warning(f"Could not fetch enrollments", exception=e)

            self.logger.info(f"Found {len(grade_items)} grade-related items",
                             course_id=course.id,
                             grade_items=len(grade_items))

            return grade_items

        except CanvasException as e:
            self.logger.error(f"Failed to fetch grades", exception=e)
            raise DownloadError(f"Could not fetch grades: {e}")

    def extract_metadata(self, grade_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from a grade item.

        Args:
            grade_item: Grade item dictionary

        Returns:
            Dict[str, Any]: Grade metadata
        """
        try:
            item_type = grade_item.get('type', 'unknown')

            if item_type == 'assignment_grade':
                return self._extract_assignment_grade_metadata(grade_item)
            elif item_type == 'course_grade':
                return self._extract_course_grade_metadata(grade_item)
            else:
                return {'type': item_type, 'error': 'Unknown grade item type'}

        except Exception as e:
            self.logger.error(f"Failed to extract grade metadata",
                              item_type=grade_item.get('type', 'unknown'),
                              exception=e)

            return {
                'type': grade_item.get('type', 'unknown'),
                'error': f"Metadata extraction failed: {e}"
            }

    def _extract_assignment_grade_metadata(self, grade_item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from assignment grade item."""
        assignment = grade_item.get('assignment')
        submission = grade_item.get('submission')

        # Basic assignment information
        metadata = {
            'type': 'assignment_grade',
            'assignment_id': getattr(assignment, 'id', None),
            'assignment_name': getattr(assignment, 'name', ''),
            'assignment_points_possible': getattr(assignment, 'points_possible', None),
            'assignment_due_at': self._format_date(getattr(assignment, 'due_at', None)),
            'assignment_grading_type': getattr(assignment, 'grading_type', ''),
            'user_id': grade_item.get('user_id')
        }

        # Submission information
        if submission:
            metadata.update({
                'submission_id': getattr(submission, 'id', None),
                'submitted_at': self._format_date(getattr(submission, 'submitted_at', None)),
                'graded_at': self._format_date(getattr(submission, 'graded_at', None)),
                'score': getattr(submission, 'score', None),
                'grade': getattr(submission, 'grade', None),
                'points_deducted': getattr(submission, 'points_deducted', None),
                'excused': getattr(submission, 'excused', False),
                'missing': getattr(submission, 'missing', False),
                'late': getattr(submission, 'late', False),
                'workflow_state': getattr(submission, 'workflow_state', ''),
                'submission_type': getattr(submission, 'submission_type', ''),
                'attempt': getattr(submission, 'attempt', None),
                'cached_due_date': self._format_date(getattr(submission, 'cached_due_date', None)),
                'preview_url': getattr(submission, 'preview_url', ''),
                'grade_matches_current_submission': getattr(submission, 'grade_matches_current_submission', True),
                'grader_id': getattr(submission, 'grader_id', None),
                'entered_grade': getattr(submission, 'entered_grade', None),
                'entered_score': getattr(submission, 'entered_score', None)
            })

            # Submission comments
            submission_comments = getattr(submission, 'submission_comments', [])
            if submission_comments:
                metadata['submission_comments'] = []
                for comment in submission_comments:
                    comment_data = {
                        'id': getattr(comment, 'id', None),
                        'author_id': getattr(comment, 'author_id', None),
                        'author_name': getattr(comment, 'author_name', ''),
                        'comment': getattr(comment, 'comment', ''),
                        'created_at': self._format_date(getattr(comment, 'created_at', None)),
                        'avatar_path': getattr(comment, 'avatar_path', ''),
                        'media_comment_url': getattr(comment, 'media_comment_url', ''),
                        'media_comment_type': getattr(comment, 'media_comment_type', ''),
                        'attachments': getattr(comment, 'attachments', [])
                    }
                    metadata['submission_comments'].append(comment_data)

            # Rubric assessment
            rubric_assessment = getattr(submission, 'rubric_assessment', None)
            if rubric_assessment:
                metadata['rubric_assessment'] = self._process_rubric_assessment(rubric_assessment)
        else:
            # No submission found
            metadata.update({
                'submission_id': None,
                'submitted_at': None,
                'score': None,
                'grade': None,
                'workflow_state': 'unsubmitted',
                'note': 'No submission found for this assignment'
            })

        # Assignment URLs
        if assignment:
            html_url = getattr(assignment, 'html_url', '')
            if html_url:
                metadata['assignment_url'] = html_url

        return metadata

    def _extract_course_grade_metadata(self, grade_item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from course grade item."""
        enrollment = grade_item.get('enrollment')

        metadata = {
            'type': 'course_grade',
            'user_id': grade_item.get('user_id'),
            'enrollment_id': getattr(enrollment, 'id', None),
            'enrollment_type': getattr(enrollment, 'type', ''),
            'enrollment_state': getattr(enrollment, 'enrollment_state', ''),
            'course_section_id': getattr(enrollment, 'course_section_id', None),
            'limit_privileges_to_course_section': getattr(enrollment, 'limit_privileges_to_course_section', False)
        }

        if enrollment:
            # Current grades
            current_score = getattr(enrollment, 'current_score', None)
            current_grade = getattr(enrollment, 'current_grade', None)
            final_score = getattr(enrollment, 'final_score', None)
            final_grade = getattr(enrollment, 'final_grade', None)

            metadata.update({
                'current_score': current_score,
                'current_grade': current_grade,
                'final_score': final_score,
                'final_grade': final_grade,
                'unposted_current_score': getattr(enrollment, 'unposted_current_score', None),
                'unposted_current_grade': getattr(enrollment, 'unposted_current_grade', None),
                'unposted_final_score': getattr(enrollment, 'unposted_final_score', None),
                'unposted_final_grade': getattr(enrollment, 'unposted_final_grade', None)
            })

            # Current grading period scores
            current_grading_period_scores = getattr(enrollment, 'current_grading_period_scores', None)
            if current_grading_period_scores:
                metadata['current_grading_period_scores'] = current_grading_period_scores

            # Total scores for all grading periods
            total_scores = getattr(enrollment, 'total_scores', None)
            if total_scores:
                metadata['total_scores'] = total_scores

            # Compute grade percentage if possible
            if current_score is not None:
                metadata['current_percentage'] = current_score
            if final_score is not None:
                metadata['final_percentage'] = final_score

        return metadata

    def _process_rubric_assessment(self, rubric_assessment) -> Dict[str, Any]:
        """Process rubric assessment data."""
        try:
            assessment_data = {}

            # Convert rubric assessment to dictionary if needed
            if hasattr(rubric_assessment, 'items'):
                # It's a dictionary-like object
                for criterion_id, assessment in rubric_assessment.items():
                    criterion_data = {
                        'points': getattr(assessment, 'points', None),
                        'rating_id': getattr(assessment, 'rating_id', None),
                        'comments': getattr(assessment, 'comments', ''),
                        'comments_html': getattr(assessment, 'comments_html', ''),
                        'description': getattr(assessment, 'description', ''),
                        'long_description': getattr(assessment, 'long_description', '')
                    }
                    assessment_data[criterion_id] = criterion_data

            return assessment_data

        except Exception as e:
            self.logger.warning(f"Failed to process rubric assessment", exception=e)
            return {}

    def get_download_info(self, grade_item: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        Get download information for a grade item.

        Grades are processed rather than directly downloaded.

        Args:
            grade_item: Grade item dictionary

        Returns:
            Optional[Dict[str, str]]: Download information or None
        """
        return None

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all grade information for a course.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting grades download for course",
                             course_name=course_info.get('full_name', 'Unknown'),
                             course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if grades are enabled
            if not self.config.is_content_type_enabled('grades'):
                self.logger.info("Grades download is disabled")
                return self.stats

            # Fetch grade items
            grade_items = self.fetch_content_list(course)

            if not grade_items:
                self.logger.info("No grade information found in course")
                return self.stats

            self.stats['total_items'] = len(grade_items)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(grade_items))

            # Process each grade item
            items_metadata = []
            assignment_grades = []
            course_grades = []

            for index, grade_item in enumerate(grade_items, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(grade_item)
                    metadata['item_number'] = index

                    # Organize by type
                    if metadata.get('type') == 'assignment_grade':
                        assignment_grades.append(metadata)
                    elif metadata.get('type') == 'course_grade':
                        course_grades.append(metadata)

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                    self.stats['downloaded_items'] += 1

                except Exception as e:
                    self.logger.error(f"Failed to process grade item",
                                      grade_item_type=grade_item.get('type', 'unknown'),
                                      exception=e)
                    self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            # Create grade reports
            await self._create_grade_reports(assignment_grades, course_grades)

            # Export gradebook CSV
            if self.export_gradebook_csv and assignment_grades:
                await self._export_gradebook_csv(assignment_grades)

            # Create grade summary
            if self.create_grade_summary:
                await self._create_grade_summary(assignment_grades, course_grades)

            self.logger.info(f"Grades download completed",
                             course_id=str(course.id),
                             **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Grades download failed", exception=e)
            raise DownloadError(f"Grades download failed: {e}")

    async def _create_grade_reports(self, assignment_grades: List[Dict[str, Any]],
                                    course_grades: List[Dict[str, Any]]):
        """Create detailed grade reports."""
        try:
            # Create assignment grades report
            if assignment_grades:
                await self._create_assignment_grades_report(assignment_grades)

            # Create course grades report
            if course_grades:
                await self._create_course_grades_report(course_grades)

        except Exception as e:
            self.logger.error(f"Failed to create grade reports", exception=e)

    async def _create_assignment_grades_report(self, assignment_grades: List[Dict[str, Any]]):
        """Create detailed assignment grades report."""
        try:
            report_filename = "assignment_grades_report.html"
            report_path = self.content_folder / report_filename

            # Sort assignments by due date or name
            sorted_assignments = sorted(assignment_grades,
                                        key=lambda x: x.get('assignment_due_at', '') or x.get('assignment_name', ''))

            # Create HTML report
            assignments_html = ""
            total_points_possible = 0
            total_points_earned = 0

            for assignment in sorted_assignments:
                assignment_name = assignment.get('assignment_name', 'Unknown Assignment')
                points_possible = assignment.get('assignment_points_possible', 0) or 0
                score = assignment.get('score')
                grade = assignment.get('grade', 'No Grade')
                due_at = assignment.get('assignment_due_at', '')

                total_points_possible += points_possible
                if score is not None:
                    total_points_earned += score

                # Grade status
                status_class = "graded" if score is not None else "ungraded"
                if assignment.get('missing'):
                    status_class = "missing"
                elif assignment.get('late'):
                    status_class = "late"
                elif assignment.get('excused'):
                    status_class = "excused"

                assignments_html += f"""
                <tr class="{status_class}">
                    <td>{assignment_name}</td>
                    <td>{score if score is not None else 'N/A'}</td>
                    <td>{points_possible}</td>
                    <td>{grade}</td>
                    <td>{due_at[:10] if due_at else 'N/A'}</td>
                    <td class="status">{status_class.title()}</td>
                </tr>
                """

            # Calculate overall percentage
            overall_percentage = (total_points_earned / total_points_possible * 100) if total_points_possible > 0 else 0

            html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Assignment Grades Report</title>
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
        .graded {{ background-color: #f0fff0; }}
        .ungraded {{ background-color: #fff8dc; }}
        .missing {{ background-color: #ffe4e1; }}
        .late {{ background-color: #ffeaa7; }}
        .excused {{ background-color: #e6f3ff; }}
        .status {{
            font-weight: bold;
            text-transform: uppercase;
            font-size: 0.8em;
        }}
    </style>
</head>
<body>
    <h1>📊 Assignment Grades Report</h1>

    <div class="summary">
        <h2>Grade Summary</h2>
        <p><strong>Total Points Earned:</strong> {total_points_earned:.1f}</p>
        <p><strong>Total Points Possible:</strong> {total_points_possible:.1f}</p>
        <p><strong>Overall Percentage:</strong> {overall_percentage:.2f}%</p>
        <p><strong>Total Assignments:</strong> {len(assignment_grades)}</p>
    </div>

    <table>
        <thead>
            <tr>
                <th>Assignment</th>
                <th>Score</th>
                <th>Points Possible</th>
                <th>Grade</th>
                <th>Due Date</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
            {assignments_html}
        </tbody>
    </table>

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

            async with aiofiles.open(report_path, 'w', encoding='utf-8') as f:
                await f.write(html_template)

            self.logger.debug(f"Created assignment grades report",
                              file_path=str(report_path))

        except Exception as e:
            self.logger.error(f"Failed to create assignment grades report", exception=e)

    async def _create_course_grades_report(self, course_grades: List[Dict[str, Any]]):
        """Create course overall grades report."""
        try:
            if not course_grades:
                return

            report_filename = "course_grades_report.html"
            report_path = self.content_folder / report_filename

            # Get the most recent course grade entry
            course_grade = course_grades[0]  # Usually there's only one

            html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Course Grades Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .grade-card {{
            background-color: #f0f8ff;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border-left: 4px solid #007bff;
        }}
        .grade-value {{
            font-size: 2em;
            font-weight: bold;
            color: #007bff;
        }}
        .grade-info {{
            margin-top: 15px;
        }}
        .grade-info p {{
            margin: 5px 0;
        }}
    </style>
</head>
<body>
    <h1>📈 Course Grade Summary</h1>

    <div class="grade-card">
        <h2>Current Grade</h2>
        <div class="grade-value">
            {course_grade.get('current_grade', 'N/A')} 
            ({course_grade.get('current_score', 'N/A')}%)
        </div>

        <div class="grade-info">
            <p><strong>Final Grade:</strong> {course_grade.get('final_grade', 'N/A')} ({course_grade.get('final_score', 'N/A')}%)</p>
            <p><strong>Enrollment Type:</strong> {course_grade.get('enrollment_type', 'Unknown')}</p>
            <p><strong>Enrollment State:</strong> {course_grade.get('enrollment_state', 'Unknown')}</p>
        </div>
    </div>

    {f'''<div class="grade-card">
        <h3>Unposted Grades</h3>
        <p><strong>Current:</strong> {course_grade.get('unposted_current_grade', 'N/A')} ({course_grade.get('unposted_current_score', 'N/A')}%)</p>
        <p><strong>Final:</strong> {course_grade.get('unposted_final_grade', 'N/A')} ({course_grade.get('unposted_final_score', 'N/A')}%)</p>
        <p><em>Note: These grades include unposted assignments</em></p>
    </div>''' if course_grade.get('unposted_current_score') is not None else ''}

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><em>Note: Grades shown are based on published assignments only unless otherwise noted.</em></p>
    </footer>
</body>
</html>"""

            async with aiofiles.open(report_path, 'w', encoding='utf-8') as f:
                await f.write(html_template)

            self.logger.debug(f"Created course grades report",
                              file_path=str(report_path))

        except Exception as e:
            self.logger.error(f"Failed to create course grades report", exception=e)

    async def _export_gradebook_csv(self, assignment_grades: List[Dict[str, Any]]):
        """Export grades to CSV format."""
        try:
            csv_filename = "gradebook_export.csv"
            csv_path = self.content_folder / csv_filename

            # Prepare CSV data
            fieldnames = [
                'Assignment Name',
                'Points Possible',
                'Score',
                'Grade',
                'Percentage',
                'Due Date',
                'Submitted Date',
                'Graded Date',
                'Status',
                'Late',
                'Missing',
                'Excused'
            ]

            async with aiofiles.open(csv_path, 'w', encoding='utf-8', newline='') as csvfile:
                # Write header
                await csvfile.write(','.join(fieldnames) + '\n')

                # Write data rows
                for assignment in assignment_grades:
                    score = assignment.get('score')
                    points_possible = assignment.get('assignment_points_possible', 0) or 0
                    percentage = (score / points_possible * 100) if score is not None and points_possible > 0 else ''

                    row = [
                        assignment.get('assignment_name', ''),
                        str(points_possible),
                        str(score) if score is not None else '',
                        assignment.get('grade', ''),
                        f"{percentage:.2f}" if percentage else '',
                        assignment.get('assignment_due_at', '')[:10] if assignment.get('assignment_due_at') else '',
                        assignment.get('submitted_at', '')[:10] if assignment.get('submitted_at') else '',
                        assignment.get('graded_at', '')[:10] if assignment.get('graded_at') else '',
                        assignment.get('workflow_state', ''),
                        'Yes' if assignment.get('late') else 'No',
                        'Yes' if assignment.get('missing') else 'No',
                        'Yes' if assignment.get('excused') else 'No'
                    ]

                    # Escape commas and quotes in CSV
                    escaped_row = []
                    for field in row:
                        if ',' in field or '"' in field:
                            field = '"' + field.replace('"', '""') + '"'
                        escaped_row.append(field)

                    await csvfile.write(','.join(escaped_row) + '\n')

            self.logger.debug(f"Exported gradebook CSV",
                              file_path=str(csv_path))

        except Exception as e:
            self.logger.error(f"Failed to export gradebook CSV", exception=e)

    async def _create_grade_summary(self, assignment_grades: List[Dict[str, Any]],
                                    course_grades: List[Dict[str, Any]]):
        """Create comprehensive grade summary."""
        try:
            summary_filename = "grade_summary.json"
            summary_path = self.content_folder / summary_filename

            # Calculate statistics
            graded_assignments = [a for a in assignment_grades if a.get('score') is not None]
            total_assignments = len(assignment_grades)
            graded_count = len(graded_assignments)

            total_points_possible = sum(a.get('assignment_points_possible', 0) or 0 for a in assignment_grades)
            total_points_earned = sum(a.get('score', 0) or 0 for a in graded_assignments)

            # Grade distribution
            grade_distribution = {}
            for assignment in graded_assignments:
                grade = assignment.get('grade', 'No Grade')
                grade_distribution[grade] = grade_distribution.get(grade, 0) + 1

            # Assignment status counts
            status_counts = {
                'graded': graded_count,
                'ungraded': total_assignments - graded_count,
                'missing': sum(1 for a in assignment_grades if a.get('missing')),
                'late': sum(1 for a in assignment_grades if a.get('late')),
                'excused': sum(1 for a in assignment_grades if a.get('excused'))
            }

            # Course grade info
            course_grade_info = {}
            if course_grades:
                course_grade = course_grades[0]
                course_grade_info = {
                    'current_grade': course_grade.get('current_grade'),
                    'current_score': course_grade.get('current_score'),
                    'final_grade': course_grade.get('final_grade'),
                    'final_score': course_grade.get('final_score')
                }

            # Create summary document
            summary = {
                'summary_generated': datetime.now().isoformat(),
                'course_grade': course_grade_info,
                'assignment_statistics': {
                    'total_assignments': total_assignments,
                    'graded_assignments': graded_count,
                    'completion_rate': (graded_count / total_assignments * 100) if total_assignments > 0 else 0,
                    'total_points_possible': total_points_possible,
                    'total_points_earned': total_points_earned,
                    'calculated_percentage': (
                                total_points_earned / total_points_possible * 100) if total_points_possible > 0 else 0
                },
                'status_breakdown': status_counts,
                'grade_distribution': grade_distribution,
                'performance_metrics': {
                    'average_score': total_points_earned / graded_count if graded_count > 0 else 0,
                    'highest_score': max((a.get('score', 0) for a in graded_assignments), default=0),
                    'lowest_score': min((a.get('score', 0) for a in graded_assignments), default=0)
                }
            }

            async with aiofiles.open(summary_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(summary, indent=2, ensure_ascii=False))

            self.logger.debug(f"Created grade summary",
                              file_path=str(summary_path))

        except Exception as e:
            self.logger.error(f"Failed to create grade summary", exception=e)

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

ContentDownloaderFactory.register_downloader('grades', GradesDownloader)