"""
Assignments Downloader Module

This module implements the assignments downloader for Canvas courses. It handles
downloading assignment descriptions, rubrics, submission files, and associated
attachments while maintaining comprehensive metadata.

Assignments are one of the most complex content types in Canvas as they can include:
- Assignment descriptions and instructions
- Rubrics and grading criteria
- File attachments
- Submission guidelines
- Due dates and availability windows
- Group assignment settings
- External tool integrations

Features:
- Download assignment descriptions as HTML and markdown
- Extract and download assignment attachments
- Save rubric information and criteria
- Handle external tool assignments (LTI)
- Process group assignment configurations
- Manage submission settings and restrictions
- Track assignment availability and due dates

Usage:
    # Initialize the downloader
    downloader = AssignmentsDownloader(canvas_client, progress_tracker)

    # Download all assignments for a course
    stats = await downloader.download_course_content(course, course_info)
"""

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from urllib.parse import urljoin, urlparse

import aiofiles

try:
    import markdownify
    MARKDOWNIFY_AVAILABLE = True
except ImportError:
    MARKDOWNIFY_AVAILABLE = False

from canvasapi.assignment import Assignment
from canvasapi.exceptions import CanvasException

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class AssignmentsDownloader(BaseDownloader):
    """
    Canvas Assignments Downloader

    This class handles downloading assignments from Canvas courses, including
    assignment descriptions, attachments, rubrics, and metadata. It processes
    various types of assignments including regular assignments, discussions,
    external tools, and group assignments.

    The downloader ensures that all assignment-related content is preserved
    with proper organization and comprehensive metadata tracking.
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the assignments downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # Assignment processing settings
        self.download_descriptions = True
        self.download_attachments = True
        self.download_rubrics = True
        self.convert_to_markdown = MARKDOWNIFY_AVAILABLE

        # External tool handling
        self.process_external_tools = True
        self.external_tool_info = {}

        self.logger.info("Assignments downloader initialized",
                        markdown_available=MARKDOWNIFY_AVAILABLE,
                        download_attachments=self.download_attachments)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "assignments"

    def fetch_content_list(self, course) -> List[Assignment]:
        """
        Fetch all assignments from the course.

        Args:
            course: Canvas course object

        Returns:
            List[Assignment]: List of assignment objects
        """
        try:
            self.logger.info(f"Fetching assignments for course {course.id}")

            # Get assignments with additional information
            assignments = list(course.get_assignments(
                include=[
                    'description',
                    'rubric_settings',
                    'rubric',
                    'submission',
                    'assignment_visibility',
                    'overrides',
                    'observed_users',
                    'assessment_requests'
                ]
            ))

            self.logger.info(f"Found {len(assignments)} assignments",
                           course_id=course.id,
                           assignment_count=len(assignments))

            return assignments

        except CanvasException as e:
            self.logger.error(f"Failed to fetch assignments", exception=e)
            raise DownloadError(f"Could not fetch assignments: {e}")

    def extract_metadata(self, assignment: Assignment) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from an assignment.

        Args:
            assignment: Canvas assignment object

        Returns:
            Dict[str, Any]: Assignment metadata
        """
        try:
            # Basic assignment information
            metadata = {
                'id': assignment.id,
                'name': assignment.name,
                'description': getattr(assignment, 'description', ''),
                'course_id': assignment.course_id,
                'assignment_group_id': getattr(assignment, 'assignment_group_id', None),
                'position': getattr(assignment, 'position', None),
                'submission_types': getattr(assignment, 'submission_types', []),
                'allowed_extensions': getattr(assignment, 'allowed_extensions', []),
                'turnitin_enabled': getattr(assignment, 'turnitin_enabled', False),
                'vericite_enabled': getattr(assignment, 'vericite_enabled', False),
                'grading_type': getattr(assignment, 'grading_type', ''),
                'points_possible': getattr(assignment, 'points_possible', None),
                'grading_standard_id': getattr(assignment, 'grading_standard_id', None),
                'external_tool_tag_attributes': getattr(assignment, 'external_tool_tag_attributes', {}),
                'peer_reviews': getattr(assignment, 'peer_reviews', False),
                'automatic_peer_reviews': getattr(assignment, 'automatic_peer_reviews', False),
                'peer_review_count': getattr(assignment, 'peer_review_count', 0),
                'peer_reviews_assign_at': self._format_date(getattr(assignment, 'peer_reviews_assign_at', None)),
                'anonymous_peer_reviews': getattr(assignment, 'anonymous_peer_reviews', False),
                'group_category_id': getattr(assignment, 'group_category_id', None),
                'grade_group_students_individually': getattr(assignment, 'grade_group_students_individually', False),
                'anonymous_instructor_annotations': getattr(assignment, 'anonymous_instructor_annotations', False),
                'anonymous_grading': getattr(assignment, 'anonymous_grading', False),
                'anonymous_submissions': getattr(assignment, 'anonymous_submissions', False),
                'require_lockdown_browser': getattr(assignment, 'require_lockdown_browser', False),
                'require_lockdown_browser_for_results': getattr(assignment, 'require_lockdown_browser_for_results', False),
                'require_lockdown_browser_monitor': getattr(assignment, 'require_lockdown_browser_monitor', False),
                'lockdown_browser_monitor_data': getattr(assignment, 'lockdown_browser_monitor_data', ''),
                'published': getattr(assignment, 'published', False),
                'unpublishable': getattr(assignment, 'unpublishable', True),
                'only_visible_to_overrides': getattr(assignment, 'only_visible_to_overrides', False),
                'locked_for_user': getattr(assignment, 'locked_for_user', False),
                'submissions_download_url': getattr(assignment, 'submissions_download_url', ''),
                'post_manually': getattr(assignment, 'post_manually', False),
                'moderated_grading': getattr(assignment, 'moderated_grading', False),
                'grader_count': getattr(assignment, 'grader_count', None),
                'final_grader_id': getattr(assignment, 'final_grader_id', None),
                'grader_comments_visible_to_graders': getattr(assignment, 'grader_comments_visible_to_graders', True),
                'graders_anonymous_to_graders': getattr(assignment, 'graders_anonymous_to_graders', False),
                'grader_names_visible_to_final_grader': getattr(assignment, 'grader_names_visible_to_final_grader', True),
                'anonymous_grader_identities_hidden_from_students': getattr(assignment, 'anonymous_grader_identities_hidden_from_students', False)
            }

            # Date information
            metadata.update({
                'created_at': self._format_date(getattr(assignment, 'created_at', None)),
                'updated_at': self._format_date(getattr(assignment, 'updated_at', None)),
                'due_at': self._format_date(getattr(assignment, 'due_at', None)),
                'lock_at': self._format_date(getattr(assignment, 'lock_at', None)),
                'unlock_at': self._format_date(getattr(assignment, 'unlock_at', None)),
            })

            # Assignment overrides (different due dates for different students/groups)
            overrides = getattr(assignment, 'overrides', [])
            if overrides:
                metadata['overrides'] = []
                for override in overrides:
                    override_data = {
                        'id': getattr(override, 'id', None),
                        'assignment_id': getattr(override, 'assignment_id', None),
                        'student_ids': getattr(override, 'student_ids', []),
                        'group_id': getattr(override, 'group_id', None),
                        'course_section_id': getattr(override, 'course_section_id', None),
                        'title': getattr(override, 'title', ''),
                        'due_at': self._format_date(getattr(override, 'due_at', None)),
                        'unlock_at': self._format_date(getattr(override, 'unlock_at', None)),
                        'lock_at': self._format_date(getattr(override, 'lock_at', None))
                    }
                    metadata['overrides'].append(override_data)

            # Rubric information
            rubric = getattr(assignment, 'rubric', None)
            if rubric:
                metadata['rubric'] = self._extract_rubric_metadata(rubric)

            # Rubric settings
            rubric_settings = getattr(assignment, 'rubric_settings', {})
            if rubric_settings:
                metadata['rubric_settings'] = {
                    'id': rubric_settings.get('id'),
                    'title': rubric_settings.get('title', ''),
                    'points_possible': rubric_settings.get('points_possible'),
                    'free_form_criterion_comments': rubric_settings.get('free_form_criterion_comments', False),
                    'hide_score_total': rubric_settings.get('hide_score_total', False),
                    'hide_points': rubric_settings.get('hide_points', False)
                }

            # Submission information
            submission = getattr(assignment, 'submission', None)
            if submission:
                metadata['submission'] = {
                    'id': getattr(submission, 'id', None),
                    'user_id': getattr(submission, 'user_id', None),
                    'assignment_id': getattr(submission, 'assignment_id', None),
                    'body': getattr(submission, 'body', ''),
                    'url': getattr(submission, 'url', ''),
                    'grade': getattr(submission, 'grade', None),
                    'score': getattr(submission, 'score', None),
                    'submitted_at': self._format_date(getattr(submission, 'submitted_at', None)),
                    'graded_at': self._format_date(getattr(submission, 'graded_at', None)),
                    'grade_matches_current_submission': getattr(submission, 'grade_matches_current_submission', True),
                    'workflow_state': getattr(submission, 'workflow_state', ''),
                    'submission_type': getattr(submission, 'submission_type', ''),
                    'preview_url': getattr(submission, 'preview_url', ''),
                    'attempt': getattr(submission, 'attempt', None),
                    'cached_due_date': self._format_date(getattr(submission, 'cached_due_date', None)),
                    'excused': getattr(submission, 'excused', False),
                    'late_policy_status': getattr(submission, 'late_policy_status', None),
                    'points_deducted': getattr(submission, 'points_deducted', None),
                    'grading_period_id': getattr(submission, 'grading_period_id', None),
                    'extra_attempts': getattr(submission, 'extra_attempts', None),
                    'posted_at': self._format_date(getattr(submission, 'posted_at', None)),
                    'redo_request': getattr(submission, 'redo_request', False)
                }

            # HTML content information
            if metadata['description']:
                metadata['description_stats'] = self._analyze_html_content(metadata['description'])

            # External tool information
            if metadata['external_tool_tag_attributes']:
                metadata['is_external_tool'] = True
                metadata['external_tool_info'] = self._extract_external_tool_info(metadata['external_tool_tag_attributes'])
            else:
                metadata['is_external_tool'] = False

            # Assignment URLs
            html_url = getattr(assignment, 'html_url', '')
            if html_url:
                metadata['html_url'] = html_url
                metadata['canvas_url'] = html_url

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract assignment metadata",
                            assignment_id=getattr(assignment, 'id', 'unknown'),
                            exception=e)

            # Return minimal metadata on error
            return {
                'id': getattr(assignment, 'id', None),
                'name': getattr(assignment, 'name', 'Unknown Assignment'),
                'description': '',
                'error': f"Metadata extraction failed: {e}"
            }

    def _extract_rubric_metadata(self, rubric: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract metadata from assignment rubric.

        Args:
            rubric: Rubric data from Canvas

        Returns:
            List[Dict[str, Any]]: Processed rubric metadata
        """
        processed_rubric = []

        for criterion in rubric:
            criterion_data = {
                'id': criterion.get('id'),
                'description': criterion.get('description', ''),
                'long_description': criterion.get('long_description', ''),
                'points': criterion.get('points', 0),
                'criterion_use_range': criterion.get('criterion_use_range', False),
                'ratings': []
            }

            # Process rubric ratings
            ratings = criterion.get('ratings', [])
            for rating in ratings:
                rating_data = {
                    'id': rating.get('id'),
                    'description': rating.get('description', ''),
                    'long_description': rating.get('long_description', ''),
                    'points': rating.get('points', 0)
                }
                criterion_data['ratings'].append(rating_data)

            processed_rubric.append(criterion_data)

        return processed_rubric

    def _extract_external_tool_info(self, tool_attributes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract information about external tool assignments.

        Args:
            tool_attributes: External tool attributes from Canvas

        Returns:
            Dict[str, Any]: External tool information
        """
        return {
            'url': tool_attributes.get('url', ''),
            'new_tab': tool_attributes.get('new_tab', False),
            'resource_link_id': tool_attributes.get('resource_link_id', ''),
            'external_data': tool_attributes.get('external_data', ''),
            'content_type': tool_attributes.get('content_type', ''),
            'content_id': tool_attributes.get('content_id', '')
        }

    def _analyze_html_content(self, html_content: str) -> Dict[str, Any]:
        """
        Analyze HTML content to extract useful statistics.

        Args:
            html_content: HTML content to analyze

        Returns:
            Dict[str, Any]: Content analysis results
        """
        if not html_content:
            return {'word_count': 0, 'character_count': 0, 'has_images': False, 'has_links': False}

        # Count words (simple approach)
        # Remove HTML tags for word counting
        import re
        text_content = re.sub(r'<[^>]+>', ' ', html_content)
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        word_count = len(text_content.split()) if text_content else 0

        # Check for images and links
        has_images = bool(re.search(r'<img[^>]*>', html_content, re.IGNORECASE))
        has_links = bool(re.search(r'<a[^>]*href[^>]*>', html_content, re.IGNORECASE))

        # Extract embedded files/attachments
        file_links = re.findall(r'/files/(\d+)/', html_content)

        return {
            'word_count': word_count,
            'character_count': len(html_content),
            'has_images': has_images,
            'has_links': has_links,
            'embedded_file_ids': file_links,
            'html_length': len(html_content)
        }

    def get_download_info(self, assignment: Assignment) -> Optional[Dict[str, str]]:
        """
        Get download information for an assignment.

        For assignments, we don't download a single file but rather process
        the assignment content and create multiple files (HTML, markdown, etc.).

        Args:
            assignment: Canvas assignment object

        Returns:
            Optional[Dict[str, str]]: Download information or None
        """
        # Assignments don't have a single download URL like files
        # Instead, we process them to create local files
        return None

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all assignments for a course.

        This method overrides the base implementation to handle the specific
        requirements of assignment processing.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting assignment download for course",
                           course_name=course_info.get('full_name', 'Unknown'),
                           course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if assignments are enabled
            if not self.config.is_content_type_enabled('assignments'):
                self.logger.info("Assignments download is disabled")
                return self.stats

            # Fetch assignments
            assignments = self.fetch_content_list(course)

            if not assignments:
                self.logger.info("No assignments found in course")
                return self.stats

            self.stats['total_items'] = len(assignments)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(assignments))

            # Process each assignment
            items_metadata = []

            for index, assignment in enumerate(assignments, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(assignment)
                    metadata['item_number'] = index

                    # Process assignment content
                    await self._process_assignment(assignment, metadata, index)

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                    self.stats['downloaded_items'] += 1

                except Exception as e:
                    self.logger.error(f"Failed to process assignment",
                                    assignment_id=getattr(assignment, 'id', 'unknown'),
                                    assignment_name=getattr(assignment, 'name', 'unknown'),
                                    exception=e)
                    self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            self.logger.info(f"Assignment download completed",
                           course_id=str(course.id),
                           **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Assignment download failed", exception=e)
            raise DownloadError(f"Assignment download failed: {e}")

    async def _process_assignment(self, assignment: Assignment, metadata: Dict[str, Any], index: int):
        """
        Process a single assignment and create associated files.

        Args:
            assignment: Canvas assignment object
            metadata: Assignment metadata
            index: Assignment index number
        """
        try:
            assignment_name = self.sanitize_filename(assignment.name)

            # Create assignment-specific folder
            assignment_folder = self.content_folder / f"assignment_{index:03d}_{assignment_name}"
            assignment_folder.mkdir(exist_ok=True)

            # Save assignment description as HTML
            if self.download_descriptions and metadata.get('description'):
                await self._save_assignment_description(
                    assignment, metadata, assignment_folder, index
                )

            # Save rubric if available
            if self.download_rubrics and metadata.get('rubric'):
                await self._save_assignment_rubric(
                    assignment, metadata, assignment_folder, index
                )

            # Process external tool assignments
            if metadata.get('is_external_tool') and self.process_external_tools:
                await self._save_external_tool_info(
                    assignment, metadata, assignment_folder, index
                )

            # Download embedded files from description
            if self.download_attachments and metadata.get('description_stats', {}).get('embedded_file_ids'):
                await self._download_embedded_files(
                    assignment, metadata, assignment_folder
                )

            # Save individual assignment metadata
            await self._save_assignment_metadata(metadata, assignment_folder)

        except Exception as e:
            self.logger.error(f"Failed to process assignment content",
                            assignment_id=assignment.id,
                            exception=e)
            raise

    async def _save_assignment_description(self, assignment: Assignment,
                                         metadata: Dict[str, Any],
                                         assignment_folder: Path, index: int):
        """Save assignment description in multiple formats."""
        try:
            description = metadata['description']
            if not description:
                return

            assignment_name = self.sanitize_filename(assignment.name)

            # Save as HTML
            html_filename = f"description_{index:03d}_{assignment_name}.html"
            html_path = assignment_folder / html_filename

            # Create a complete HTML document
            html_content = self._create_html_document(assignment.name, description, metadata)

            async with aiofiles.open(html_path, 'w', encoding='utf-8') as f:
                await f.write(html_content)

            self.logger.debug(f"Saved assignment description as HTML",
                            file_path=str(html_path))

            # Convert to Markdown if available
            if self.convert_to_markdown and MARKDOWNIFY_AVAILABLE:
                try:
                    markdown_content = markdownify.markdownify(description)

                    markdown_filename = f"description_{index:03d}_{assignment_name}.md"
                    markdown_path = assignment_folder / markdown_filename

                    async with aiofiles.open(markdown_path, 'w', encoding='utf-8') as f:
                        await f.write(markdown_content)

                    self.logger.debug(f"Saved assignment description as Markdown",
                                    file_path=str(markdown_path))

                except Exception as e:
                    self.logger.warning(f"Failed to convert description to Markdown", exception=e)

        except Exception as e:
            self.logger.error(f"Failed to save assignment description", exception=e)

    def _create_html_document(self, title: str, content: str, metadata: Dict[str, Any]) -> str:
        """
        Create a complete HTML document with assignment content.

        Args:
            title: Assignment title
            content: Assignment description HTML
            metadata: Assignment metadata

        Returns:
            str: Complete HTML document
        """
        # Basic assignment information for the header
        info_html = ""

        if metadata.get('due_at'):
            info_html += f"<p><strong>Due Date:</strong> {metadata['due_at']}</p>"

        if metadata.get('points_possible'):
            info_html += f"<p><strong>Points:</strong> {metadata['points_possible']}</p>"

        if metadata.get('submission_types'):
            submission_types = ', '.join(metadata['submission_types'])
            info_html += f"<p><strong>Submission Types:</strong> {submission_types}</p>"

        if metadata.get('html_url'):
            info_html += f"<p><strong>Canvas URL:</strong> <a href=\"{metadata['html_url']}\">{metadata['html_url']}</a></p>"

        # Create complete HTML document
        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .assignment-header {{
            background-color: #f5f5f5;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        .assignment-content {{
            margin-top: 20px;
        }}
        h1 {{
            color: #333;
            border-bottom: 2px solid #ddd;
            padding-bottom: 10px;
        }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    
    <div class="assignment-header">
        <h2>Assignment Information</h2>
        {info_html}
    </div>
    
    <div class="assignment-content">
        <h2>Description</h2>
        {content}
    </div>
    
    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    async def _save_assignment_rubric(self, assignment: Assignment,
                                    metadata: Dict[str, Any],
                                    assignment_folder: Path, index: int):
        """Save assignment rubric information."""
        try:
            rubric_data = metadata.get('rubric', [])
            if not rubric_data:
                return

            assignment_name = self.sanitize_filename(assignment.name)
            rubric_filename = f"rubric_{index:03d}_{assignment_name}.json"
            rubric_path = assignment_folder / rubric_filename

            # Create comprehensive rubric document
            rubric_document = {
                'assignment_id': assignment.id,
                'assignment_name': assignment.name,
                'rubric_settings': metadata.get('rubric_settings', {}),
                'rubric_criteria': rubric_data,
                'total_points': sum(criterion.get('points', 0) for criterion in rubric_data),
                'created_at': datetime.now().isoformat()
            }

            async with aiofiles.open(rubric_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(rubric_document, indent=2, ensure_ascii=False))

            self.logger.debug(f"Saved assignment rubric",
                            file_path=str(rubric_path),
                            criteria_count=len(rubric_data))

        except Exception as e:
            self.logger.error(f"Failed to save assignment rubric", exception=e)

    async def _save_external_tool_info(self, assignment: Assignment,
                                     metadata: Dict[str, Any],
                                     assignment_folder: Path, index: int):
        """Save external tool assignment information."""
        try:
            tool_info = metadata.get('external_tool_info', {})
            if not tool_info:
                return

            assignment_name = self.sanitize_filename(assignment.name)
            tool_filename = f"external_tool_{index:03d}_{assignment_name}.json"
            tool_path = assignment_folder / tool_filename

            # Create external tool document
            tool_document = {
                'assignment_id': assignment.id,
                'assignment_name': assignment.name,
                'tool_info': tool_info,
                'tool_url': tool_info.get('url', ''),
                'canvas_url': metadata.get('html_url', ''),
                'note': 'This assignment uses an external tool. The actual assignment content is hosted externally.',
                'created_at': datetime.now().isoformat()
            }

            async with aiofiles.open(tool_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(tool_document, indent=2, ensure_ascii=False))

            self.logger.debug(f"Saved external tool info",
                            file_path=str(tool_path),
                            tool_url=tool_info.get('url', 'unknown'))

        except Exception as e:
            self.logger.error(f"Failed to save external tool info", exception=e)

    async def _download_embedded_files(self, assignment: Assignment,
                                     metadata: Dict[str, Any],
                                     assignment_folder: Path):
        """Download files embedded in assignment descriptions."""
        try:
            embedded_file_ids = metadata.get('description_stats', {}).get('embedded_file_ids', [])
            if not embedded_file_ids:
                return

            # Create attachments subfolder
            attachments_folder = assignment_folder / 'attachments'
            attachments_folder.mkdir(exist_ok=True)

            course = self.canvas_client.get_course(assignment.course_id)

            for file_id in embedded_file_ids:
                try:
                    # Get file information from Canvas
                    canvas_file = course.get_file(file_id)

                    # Generate local filename
                    original_filename = getattr(canvas_file, 'filename', f'file_{file_id}')
                    sanitized_filename = self.sanitize_filename(original_filename)

                    file_path = attachments_folder / sanitized_filename

                    # Download the file
                    download_url = getattr(canvas_file, 'url', '')
                    if download_url:
                        success = await self.download_file(download_url, file_path)

                        if success:
                            self.logger.debug(f"Downloaded embedded file",
                                            file_id=file_id,
                                            filename=sanitized_filename)
                        else:
                            self.logger.warning(f"Failed to download embedded file",
                                              file_id=file_id)

                except Exception as e:
                    self.logger.warning(f"Could not download embedded file",
                                      file_id=file_id,
                                      exception=e)

        except Exception as e:
            self.logger.error(f"Failed to download embedded files", exception=e)

    async def _save_assignment_metadata(self, metadata: Dict[str, Any], assignment_folder: Path):
        """Save individual assignment metadata."""
        try:
            metadata_path = assignment_folder / 'assignment_metadata.json'

            async with aiofiles.open(metadata_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False, default=str))

            self.logger.debug(f"Saved assignment metadata",
                            file_path=str(metadata_path))

        except Exception as e:
            self.logger.error(f"Failed to save assignment metadata", exception=e)

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
ContentDownloaderFactory.register_downloader('assignments', AssignmentsDownloader)