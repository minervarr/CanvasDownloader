"""
Assignments Downloader Module

This module implements the assignments downloader for Canvas courses. Canvas assignments
are one of the most critical content types for students, representing graded work,
submissions, and assessment criteria that form the core of academic coursework.

Canvas Assignments include:
- Assignment instructions and descriptions
- Due dates, availability windows, and submission types
- Rubrics and grading criteria
- Student submissions and feedback
- Attached files and external tool integrations
- Group assignments and peer reviews
- Multiple submission attempts and versioning

Features:
- Download complete assignment details and instructions
- Process assignment attachments and linked files
- Handle different submission types (online text, file uploads, external tools)
- Extract rubric information and grading criteria
- Download student submissions (with appropriate permissions)
- Process assignment groups and weighting
- Handle assignment overrides and special conditions
- Create organized folder structure by assignment groups
- Generate assignment summary reports and indexes

Security Considerations:
- Respects Canvas permissions for submission access
- Handles confidential rubric criteria appropriately
- Protects student privacy in submission downloads
- Validates file types and sizes for safety

Usage:
    # Initialize the downloader
    downloader = AssignmentsDownloader(canvas_client, progress_tracker)

    # Download all assignments for a course
    stats = await downloader.download_course_content(course, course_info)

    # Configure download options
    downloader.download_submissions = True  # Requires appropriate permissions
    downloader.download_rubrics = True
    downloader.create_assignment_index = True
"""

import asyncio
import json
import html
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
from urllib.parse import urlparse, unquote

import aiofiles

try:
    import markdownify
    MARKDOWNIFY_AVAILABLE = True
except ImportError:
    MARKDOWNIFY_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    BEAUTIFULSOUP_AVAILABLE = True
except ImportError:
    BEAUTIFULSOUP_AVAILABLE = False

from canvasapi.assignment import Assignment
from canvasapi.exceptions import CanvasException, Unauthorized

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class AssignmentsDownloader(BaseDownloader):
    """
    Canvas Assignments Downloader

    This class handles downloading assignment content from Canvas courses.
    Assignments represent the core academic work and assessment materials
    that students need access to for completing coursework.

    The downloader ensures that:
    - Complete assignment instructions are preserved
    - All attachment files are downloaded
    - Rubrics and grading criteria are saved
    - Assignment metadata is comprehensively captured
    - Student submissions are handled appropriately (with permissions)
    - Assignment groups and organization are maintained
    - Due dates and availability windows are documented

    Download Organization:
    - assignments/
    ├── assignment_groups/           # Organized by Canvas assignment groups
    │   ├── homework/
    │   ├── projects/
    │   └── exams/
    ├── individual_assignments/      # All assignments in one folder (alternative)
    ├── submissions/                 # Student submissions (if permissions allow)
    ├── rubrics/                     # Rubric files and criteria
    ├── attachments/                 # Assignment attachment files
    └── assignment_index.json        # Complete assignment catalog
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the assignments downloader.

        Args:
            canvas_client: Canvas API client instance for making requests
            progress_tracker: Optional progress tracker for UI updates and monitoring
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # Assignment download configuration
        self.download_instructions = True      # Download assignment descriptions and instructions
        self.download_attachments = True       # Download files attached to assignments
        self.download_rubrics = True          # Download rubric information and criteria
        self.download_submissions = False      # Download student submissions (requires permissions)
        self.create_assignment_index = True    # Create comprehensive assignment catalog
        self.organize_by_groups = True        # Organize assignments by Canvas assignment groups

        # Content processing options
        self.convert_html_to_markdown = MARKDOWNIFY_AVAILABLE  # Convert HTML descriptions to Markdown
        self.extract_embedded_images = BEAUTIFULSOUP_AVAILABLE # Extract images from HTML descriptions
        self.download_external_links = False  # Download content from external links (be cautious)
        self.process_due_date_overrides = True # Handle individual student due date overrides

        # File organization preferences
        self.create_group_folders = True      # Create separate folders for assignment groups
        self.include_due_dates_in_filenames = True  # Add due dates to filenames for sorting
        self.sanitize_assignment_names = True # Clean assignment names for file system compatibility

        # Submission handling (requires instructor/admin permissions)
        self.download_submission_comments = False  # Download grading comments and feedback
        self.download_submission_attachments = False  # Download student-submitted files
        self.respect_submission_privacy = True # Ensure student privacy protection

        self.logger.info("Assignments downloader initialized",
                        markdown_available=MARKDOWNIFY_AVAILABLE,
                        beautifulsoup_available=BEAUTIFULSOUP_AVAILABLE,
                        download_attachments=self.download_attachments,
                        organize_by_groups=self.organize_by_groups)

    def get_content_type_name(self) -> str:
        """
        Get the content type name for this downloader.

        Returns:
            str: The content type identifier used in configuration and logging
        """
        return "assignments"

    def fetch_content_list(self, course) -> List[Assignment]:
        """
        Fetch all assignments from the course with comprehensive details.

        This method retrieves assignments with all available metadata including
        submission details, rubric information, and assignment group data.

        Args:
            course: Canvas course object

        Returns:
            List[Assignment]: Complete list of assignment objects with metadata

        Raises:
            DownloadError: If assignment fetching fails due to API or permission issues
        """
        try:
            self.logger.info(f"Fetching assignments for course {course.id}")

            # Define what additional data to include with assignments
            include_params = [
                'submission',           # Include submission information
                'assignment_visibility', # Include visibility settings
                'all_dates',            # Include all due dates and overrides
                'overrides',            # Include assignment overrides
                'observed_users',       # Include observed user information
                'can_edit',             # Include edit permissions
                'score_statistics'      # Include grade statistics
            ]

            # Fetch assignments with comprehensive metadata
            assignments = list(course.get_assignments(
                include=include_params,
                order_by='due_at',           # Order by due date for logical organization
                bucket='unsubmitted'         # Focus on assignments that need attention
            ))

            self.logger.info(f"Found {len(assignments)} assignments",
                           course_id=course.id,
                           assignment_count=len(assignments))

            # Log assignment types breakdown for debugging
            assignment_types = {}
            for assignment in assignments:
                submission_types = getattr(assignment, 'submission_types', ['unknown'])
                for sub_type in submission_types:
                    assignment_types[sub_type] = assignment_types.get(sub_type, 0) + 1

            self.logger.debug("Assignment submission types found", **assignment_types)

            return assignments

        except CanvasException as e:
            self.logger.error(f"Failed to fetch assignments", exception=e)
            raise DownloadError(f"Could not fetch assignments: {e}")

    def extract_metadata(self, assignment: Assignment) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from an assignment object.

        This method captures all relevant assignment information including
        submission requirements, grading criteria, due dates, and accessibility settings.

        Args:
            assignment: Canvas assignment object

        Returns:
            Dict[str, Any]: Comprehensive assignment metadata dictionary
        """
        try:
            # Basic assignment information
            metadata = {
                'id': assignment.id,
                'name': assignment.name,
                'description': getattr(assignment, 'description', ''),
                'created_at': getattr(assignment, 'created_at', None),
                'updated_at': getattr(assignment, 'updated_at', None),
                'points_possible': getattr(assignment, 'points_possible', None),
                'grading_type': getattr(assignment, 'grading_type', 'points'),
                'submission_types': getattr(assignment, 'submission_types', []),
                'workflow_state': getattr(assignment, 'workflow_state', 'unknown'),

                # Due date and availability information
                'due_at': getattr(assignment, 'due_at', None),
                'unlock_at': getattr(assignment, 'unlock_at', None),
                'lock_at': getattr(assignment, 'lock_at', None),

                # Assignment grouping and organization
                'assignment_group_id': getattr(assignment, 'assignment_group_id', None),
                'position': getattr(assignment, 'position', None),

                # Submission and grading settings
                'allowed_extensions': getattr(assignment, 'allowed_extensions', []),
                'turnitin_enabled': getattr(assignment, 'turnitin_enabled', False),
                'peer_reviews': getattr(assignment, 'peer_reviews', False),
                'automatic_peer_reviews': getattr(assignment, 'automatic_peer_reviews', False),
                'grade_group_students_individually': getattr(assignment, 'grade_group_students_individually', False),
                'group_category_id': getattr(assignment, 'group_category_id', None),

                # Accessibility and display options
                'muted': getattr(assignment, 'muted', False),
                'html_url': getattr(assignment, 'html_url', ''),
                'external_tool_tag_attributes': getattr(assignment, 'external_tool_tag_attributes', {}),

                # File and attachment information
                'has_attachments': False,  # Will be determined during processing
                'attachment_count': 0,     # Will be counted during download

                # Processing metadata
                'download_timestamp': datetime.now().isoformat(),
                'content_type': 'assignment',
                'processing_notes': []
            }

            # Handle submission information if available
            if hasattr(assignment, 'submission') and assignment.submission:
                submission = assignment.submission
                metadata['submission_info'] = {
                    'submitted_at': getattr(submission, 'submitted_at', None),
                    'grade': getattr(submission, 'grade', None),
                    'score': getattr(submission, 'score', None),
                    'submission_type': getattr(submission, 'submission_type', None),
                    'workflow_state': getattr(submission, 'workflow_state', None),
                    'graded_at': getattr(submission, 'graded_at', None),
                    'grader_id': getattr(submission, 'grader_id', None)
                }

            # Handle multiple due dates (overrides) if available
            if hasattr(assignment, 'all_dates') and assignment.all_dates:
                metadata['all_due_dates'] = []
                for date_info in assignment.all_dates:
                    date_entry = {
                        'due_at': getattr(date_info, 'due_at', None),
                        'unlock_at': getattr(date_info, 'unlock_at', None),
                        'lock_at': getattr(date_info, 'lock_at', None),
                        'base': getattr(date_info, 'base', False)
                    }

                    # Add specific student/section information if available
                    if hasattr(date_info, 'student_ids'):
                        date_entry['student_ids'] = date_info.student_ids
                    if hasattr(date_info, 'section_id'):
                        date_entry['section_id'] = date_info.section_id

                    metadata['all_due_dates'].append(date_entry)

            # Clean up description HTML for better readability
            if metadata['description']:
                # Remove excessive whitespace and clean HTML
                description = metadata['description'].strip()
                if BEAUTIFULSOUP_AVAILABLE:
                    soup = BeautifulSoup(description, 'html.parser')
                    # Extract plain text for length calculation
                    text_content = soup.get_text().strip()
                    metadata['description_length'] = len(text_content)
                    metadata['has_rich_content'] = len(soup.find_all()) > 0
                else:
                    metadata['description_length'] = len(description)
                    metadata['has_rich_content'] = '<' in description and '>' in description

            self.logger.debug(f"Extracted metadata for assignment",
                            assignment_id=assignment.id,
                            name=assignment.name,
                            submission_types=metadata['submission_types'],
                            points_possible=metadata['points_possible'])

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract assignment metadata",
                            assignment_id=getattr(assignment, 'id', 'unknown'),
                            exception=e)
            # Return minimal metadata on error
            return {
                'id': getattr(assignment, 'id', 'unknown'),
                'name': getattr(assignment, 'name', 'Unknown Assignment'),
                'error': str(e),
                'download_timestamp': datetime.now().isoformat(),
                'content_type': 'assignment'
            }

    async def process_content_item(self, assignment: Assignment, course_folder: Path,
                                 metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single assignment and download all associated content.

        This method handles the complete processing pipeline for an assignment:
        1. Create appropriate folder structure
        2. Download assignment instructions and description
        3. Process and download attachment files
        4. Extract rubric information if available
        5. Handle submission information (with permissions)
        6. Create assignment summary files

        Args:
            assignment: Canvas assignment object to process
            course_folder: Base folder path for the course
            metadata: Pre-extracted assignment metadata

        Returns:
            Optional[Dict[str, Any]]: Download result information or None if skipped
        """
        try:
            # Set current assignment ID for logging context
            self._current_assignment_id = assignment.id

            assignment_name = self._sanitize_filename(assignment.name)

            # Determine assignment folder structure
            if self.organize_by_groups:
                assignment_folder = await self._create_assignment_group_folder(
                    assignment, course_folder, metadata
                )
            else:
                assignment_folder = course_folder / 'assignments' / assignment_name

            # Ensure assignment folder exists
            assignment_folder.mkdir(parents=True, exist_ok=True)

            # Initialize download results
            download_info = {
                'assignment_id': assignment.id,
                'assignment_name': assignment.name,
                'folder_path': str(assignment_folder),
                'files_downloaded': [],
                'attachments_downloaded': 0,
                'errors': [],
                'processing_time': None,
                'success': True
            }

            start_time = datetime.now()

            # 1. Download assignment instructions/description
            if self.download_instructions and metadata.get('description'):
                await self._download_assignment_instructions(
                    assignment, assignment_folder, metadata, download_info
                )

            # 2. Process assignment attachments
            if self.download_attachments:
                await self._download_assignment_attachments(
                    assignment, assignment_folder, download_info
                )

            # 3. Download rubric information
            if self.download_rubrics:
                await self._download_assignment_rubric(
                    assignment, assignment_folder, download_info
                )

            # 4. Handle submissions (if permissions allow)
            if self.download_submissions:
                await self._download_assignment_submissions(
                    assignment, assignment_folder, download_info
                )

            # 5. Create assignment summary file
            await self._create_assignment_summary(
                assignment, assignment_folder, metadata, download_info
            )

            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            download_info['processing_time'] = processing_time

            # Determine if processing was successful
            download_info['success'] = len(download_info['errors']) == 0

            self.logger.info(f"Assignment processing completed",
                           assignment_id=assignment.id,
                           assignment_name=assignment.name,
                           files_downloaded=len(download_info['files_downloaded']),
                           processing_time=processing_time,
                           success=download_info['success'])

            return download_info

        except Exception as e:
            self.logger.error(f"Failed to process assignment",
                            assignment_id=getattr(assignment, 'id', 'unknown'),
                            assignment_name=getattr(assignment, 'name', 'unknown'),
                            exception=e)

            return {
                'assignment_id': getattr(assignment, 'id', 'unknown'),
                'assignment_name': getattr(assignment, 'name', 'unknown'),
                'error': str(e),
                'files_downloaded': [],
                'processing_failed': True,
                'success': False
            }
        finally:
            # Clear assignment ID context
            self._current_assignment_id = None

    async def _create_assignment_group_folder(self, assignment: Assignment,
                                            course_folder: Path,
                                            metadata: Dict[str, Any]) -> Path:
        """
        Create folder structure organized by assignment groups.

        This method creates a hierarchical folder structure that mirrors Canvas
        assignment groups, making it easier to navigate assignments by category.

        Args:
            assignment: Canvas assignment object
            course_folder: Base course folder path
            metadata: Assignment metadata containing group information

        Returns:
            Path: The specific folder path for this assignment
        """
        base_assignments_folder = course_folder / 'assignments'

        # Get assignment group information
        group_id = metadata.get('assignment_group_id')

        if group_id and self.create_group_folders:
            try:
                # Fetch assignment group details
                group = assignment.course.get_assignment_group(group_id)
                group_name = self._sanitize_filename(group.name)

                # Create group folder
                group_folder = base_assignments_folder / 'assignment_groups' / group_name

                self.logger.debug(f"Using assignment group folder",
                                assignment_id=assignment.id,
                                group_name=group_name,
                                group_id=group_id)

                return group_folder

            except Exception as e:
                self.logger.warning(f"Could not fetch assignment group, using default folder",
                                  group_id=group_id,
                                  exception=e)

        # Fallback to individual assignments folder
        return base_assignments_folder / 'individual_assignments'

    async def _download_assignment_instructions(self, assignment: Assignment,
                                              assignment_folder: Path,
                                              metadata: Dict[str, Any],
                                              download_info: Dict[str, Any]) -> None:
        """
        Download and process assignment instructions and descriptions.

        This method handles the assignment description content, converting HTML
        to readable formats and extracting embedded images where possible.

        Args:
            assignment: Canvas assignment object
            assignment_folder: Folder to save instruction files
            metadata: Assignment metadata
            download_info: Download tracking information
        """
        try:
            description = metadata.get('description', '').strip()
            if not description:
                return

            assignment_name = self._sanitize_filename(assignment.name)

            # Save original HTML description
            html_file = assignment_folder / f"{assignment_name}_instructions.html"
            async with aiofiles.open(html_file, 'w', encoding='utf-8') as f:
                await f.write(description)
            download_info['files_downloaded'].append(str(html_file))

            # Convert to Markdown if available
            if self.convert_html_to_markdown and MARKDOWNIFY_AVAILABLE:
                try:
                    markdown_content = markdownify.markdownify(description,
                                                             heading_style="ATX")
                    markdown_file = assignment_folder / f"{assignment_name}_instructions.md"
                    async with aiofiles.open(markdown_file, 'w', encoding='utf-8') as f:
                        await f.write(markdown_content)
                    download_info['files_downloaded'].append(str(markdown_file))

                except Exception as e:
                    self.logger.warning(f"Could not convert assignment to Markdown",
                                      assignment_id=assignment.id,
                                      exception=e)

            # Extract images from HTML content
            if self.extract_embedded_images and BEAUTIFULSOUP_AVAILABLE:
                await self._extract_embedded_images(description, assignment_folder,
                                                   assignment_name, download_info)

            self.logger.debug(f"Downloaded assignment instructions",
                            assignment_id=assignment.id,
                            files_created=2 if MARKDOWNIFY_AVAILABLE else 1)

        except Exception as e:
            error_msg = f"Failed to download instructions: {e}"
            download_info['errors'].append(error_msg)
            self.logger.error(error_msg, assignment_id=assignment.id, exception=e)

    async def _download_assignment_attachments(self, assignment: Assignment,
                                             assignment_folder: Path,
                                             download_info: Dict[str, Any]) -> None:
        """
        Download files attached to the assignment.

        Canvas assignments can have file attachments that provide additional
        resources, templates, or supporting materials for students.

        Args:
            assignment: Canvas assignment object
            assignment_folder: Folder to save attachment files
            download_info: Download tracking information
        """
        try:
            attachments_found = 0
            attachments_folder = assignment_folder / 'attachments'

            # Method 1: Check for file attachments through Canvas API
            if hasattr(assignment, 'attachments') and assignment.attachments:
                attachments_folder.mkdir(exist_ok=True)

                for attachment in assignment.attachments:
                    try:
                        # Get file information
                        file_url = getattr(attachment, 'url', None)
                        file_name = getattr(attachment, 'filename', 'unknown_file')
                        file_size = getattr(attachment, 'size', 0)

                        if file_url:
                            # Download the attachment
                            download_result = await self.download_file(
                                file_url,
                                attachments_folder,
                                filename=file_name
                            )

                            if download_result.get('success'):
                                download_info['files_downloaded'].append(download_result['file_path'])
                                attachments_found += 1

                                self.logger.debug(f"Downloaded assignment attachment",
                                                assignment_id=assignment.id,
                                                filename=file_name,
                                                size_bytes=file_size)
                            else:
                                error_msg = f"Failed to download attachment {file_name}: {download_result.get('error', 'Unknown error')}"
                                download_info['errors'].append(error_msg)

                    except Exception as e:
                        self.logger.warning(f"Could not download attachment",
                                          assignment_id=assignment.id,
                                          filename=getattr(attachment, 'filename', 'unknown'),
                                          exception=e)

            # Method 2: Parse HTML description for embedded file links
            if hasattr(assignment, 'description') and assignment.description:
                embedded_files = await self._extract_file_links_from_html(
                    assignment.description, attachments_folder, download_info
                )
                attachments_found += embedded_files

            # Method 3: Check for Canvas file references in description
            if hasattr(assignment, 'description') and assignment.description and 'canvas.instructure.com' in assignment.description:
                canvas_files = await self._download_canvas_file_references(
                    assignment, assignment.description, attachments_folder, download_info
                )
                attachments_found += canvas_files

            download_info['attachments_downloaded'] = attachments_found

            self.logger.debug(f"Attachment processing completed for assignment",
                            assignment_id=assignment.id,
                            attachments_found=attachments_found)

        except Exception as e:
            error_msg = f"Failed to download attachments: {e}"
            download_info['errors'].append(error_msg)
            self.logger.error(error_msg, assignment_id=assignment.id, exception=e)

    async def _download_assignment_rubric(self, assignment: Assignment,
                                        assignment_folder: Path,
                                        download_info: Dict[str, Any]) -> None:
        """
        Download rubric information for the assignment.

        Rubrics provide detailed grading criteria and help students understand
        how their work will be evaluated.

        Args:
            assignment: Canvas assignment object
            assignment_folder: Folder to save rubric files
            download_info: Download tracking information
        """
        try:
            if not hasattr(assignment, 'rubric') or not assignment.rubric:
                return

            rubric_folder = assignment_folder / 'rubric'
            rubric_folder.mkdir(exist_ok=True)

            # Extract rubric information
            rubric_data = {
                'assignment_id': assignment.id,
                'assignment_name': assignment.name,
                'rubric_id': assignment.rubric.get('id'),
                'title': assignment.rubric.get('title'),
                'points_possible': assignment.rubric.get('points_possible'),
                'criteria': assignment.rubric.get('criteria', []),
                'extracted_at': datetime.now().isoformat()
            }

            # Save rubric as JSON
            assignment_name = self._sanitize_filename(assignment.name)
            rubric_file = rubric_folder / f"{assignment_name}_rubric.json"
            async with aiofiles.open(rubric_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(rubric_data, indent=2))

            download_info['files_downloaded'].append(str(rubric_file))

            # Create human-readable rubric summary
            summary_file = rubric_folder / f"{assignment_name}_rubric_summary.txt"
            await self._create_rubric_summary(rubric_data, summary_file)
            download_info['files_downloaded'].append(str(summary_file))

            self.logger.debug(f"Downloaded rubric information",
                            assignment_id=assignment.id,
                            rubric_id=rubric_data['rubric_id'],
                            criteria_count=len(rubric_data['criteria']))

        except Exception as e:
            error_msg = f"Failed to download rubric: {e}"
            download_info['errors'].append(error_msg)
            self.logger.error(error_msg, assignment_id=assignment.id, exception=e)

    async def _download_assignment_submissions(self, assignment: Assignment,
                                             assignment_folder: Path,
                                             download_info: Dict[str, Any]) -> None:
        """
        Download submission information (requires appropriate permissions).

        This method handles student submissions, but only when the user has
        appropriate instructor or admin permissions.

        Args:
            assignment: Canvas assignment object
            assignment_folder: Folder to save submission files
            download_info: Download tracking information
        """
        try:
            if not self.respect_submission_privacy:
                self.logger.warning("Submission privacy protection is disabled")
                return

            # Only proceed if user has appropriate permissions
            submissions_folder = assignment_folder / 'submissions'

            # This is a placeholder for submission handling
            # Actual implementation would require checking user permissions
            # and handling student privacy appropriately

            self.logger.debug(f"Submission processing completed for assignment",
                            assignment_id=assignment.id,
                            note="Requires instructor permissions")

        except Unauthorized:
            self.logger.info(f"No permission to access submissions",
                           assignment_id=assignment.id)
        except Exception as e:
            error_msg = f"Failed to process submissions: {e}"
            download_info['errors'].append(error_msg)
            self.logger.error(error_msg, assignment_id=assignment.id, exception=e)

    async def _create_assignment_summary(self, assignment: Assignment,
                                       assignment_folder: Path,
                                       metadata: Dict[str, Any],
                                       download_info: Dict[str, Any]) -> None:
        """
        Create a comprehensive summary file for the assignment.

        This summary provides all key assignment information in a single,
        easily readable file for quick reference.

        Args:
            assignment: Canvas assignment object
            assignment_folder: Assignment folder path
            metadata: Assignment metadata
            download_info: Download tracking information
        """
        try:
            assignment_name = self._sanitize_filename(assignment.name)
            summary_file = assignment_folder / f"{assignment_name}_summary.json"

            # Combine metadata with download information
            summary_data = {
                **metadata,
                'download_info': download_info,
                'folder_structure': {
                    'base_folder': str(assignment_folder),
                    'files_created': download_info['files_downloaded'],
                    'organization_method': 'by_group' if self.organize_by_groups else 'individual'
                }
            }

            # Save comprehensive summary
            async with aiofiles.open(summary_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(summary_data, indent=2, default=str))

            download_info['files_downloaded'].append(str(summary_file))

            self.logger.debug(f"Created assignment summary",
                            assignment_id=assignment.id,
                            summary_file=str(summary_file))

        except Exception as e:
            error_msg = f"Failed to create assignment summary: {e}"
            download_info['errors'].append(error_msg)
            self.logger.error(error_msg, assignment_id=assignment.id, exception=e)

    async def _extract_embedded_images(self, html_content: str, assignment_folder: Path,
                                     assignment_name: str, download_info: Dict[str, Any]) -> None:
        """
        Extract and download images embedded in assignment descriptions.

        Args:
            html_content: HTML content containing embedded images
            assignment_folder: Folder to save extracted images
            assignment_name: Assignment name for file naming
            download_info: Download tracking information
        """
        if not BEAUTIFULSOUP_AVAILABLE:
            return

        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            images = soup.find_all('img')

            if not images:
                return

            images_folder = assignment_folder / 'images'
            images_folder.mkdir(exist_ok=True)

            for idx, img in enumerate(images):
                src = img.get('src')
                if src and src.startswith('http'):
                    try:
                        # Determine file extension from URL or content type
                        parsed_url = urlparse(src)
                        original_name = Path(unquote(parsed_url.path)).name

                        if not original_name or '.' not in original_name:
                            original_name = f"{assignment_name}_image_{idx + 1}.jpg"
                        else:
                            # Sanitize the original filename
                            original_name = self._sanitize_filename(original_name)

                        # Download the image
                        download_result = await self.download_file(
                            src, images_folder, filename=original_name
                        )

                        if download_result.get('success'):
                            download_info['files_downloaded'].append(download_result['file_path'])
                            self.logger.debug(f"Downloaded embedded image",
                                            assignment_id=getattr(self, '_current_assignment_id', 'unknown'),
                                            image_src=src,
                                            saved_as=original_name)
                        else:
                            self.logger.warning(f"Could not download embedded image",
                                              src=src,
                                              error=download_result.get('error'))

                    except Exception as e:
                        self.logger.warning(f"Could not download embedded image",
                                          src=src, exception=e)

        except Exception as e:
            self.logger.warning(f"Could not extract embedded images", exception=e)

    async def _extract_file_links_from_html(self, html_content: str,
                                           attachments_folder: Path,
                                           download_info: Dict[str, Any]) -> int:
        """
        Extract and download files linked in HTML content.

        Args:
            html_content: HTML content to parse for file links
            attachments_folder: Folder to save downloaded files
            download_info: Download tracking information

        Returns:
            int: Number of files downloaded
        """
        if not BEAUTIFULSOUP_AVAILABLE:
            return 0

        files_downloaded = 0

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find all links that might be files
            links = soup.find_all('a', href=True)

            for link in links:
                href = link.get('href')
                if not href or not href.startswith('http'):
                    continue

                # Check if this looks like a file URL
                parsed_url = urlparse(href)
                path = unquote(parsed_url.path)

                # Look for file extensions
                if '.' in path:
                    extension = Path(path).suffix.lower()
                    # Common file extensions for educational content
                    file_extensions = {'.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
                                     '.txt', '.zip', '.rar', '.mp3', '.mp4', '.avi', '.mov'}

                    if extension in file_extensions:
                        try:
                            filename = Path(path).name
                            if filename:
                                filename = self._sanitize_filename(filename)

                                download_result = await self.download_file(
                                    href, attachments_folder, filename=filename
                                )

                                if download_result.get('success'):
                                    download_info['files_downloaded'].append(download_result['file_path'])
                                    files_downloaded += 1

                                    self.logger.debug(f"Downloaded linked file",
                                                    url=href,
                                                    filename=filename)

                        except Exception as e:
                            self.logger.warning(f"Could not download linked file",
                                              url=href, exception=e)

        except Exception as e:
            self.logger.warning(f"Could not parse HTML for file links", exception=e)

        return files_downloaded

    async def _download_canvas_file_references(self, assignment: Assignment,
                                             html_content: str,
                                             attachments_folder: Path,
                                             download_info: Dict[str, Any]) -> int:
        """
        Download files referenced through Canvas file URLs in the content.

        Args:
            assignment: Canvas assignment object
            html_content: HTML content containing Canvas file references
            attachments_folder: Folder to save downloaded files
            download_info: Download tracking information

        Returns:
            int: Number of files downloaded
        """
        files_downloaded = 0

        try:
            # Look for Canvas file URLs in the format:
            # https://institution.instructure.com/courses/COURSE_ID/files/FILE_ID
            import re

            canvas_file_pattern = r'https?://[^/]+/courses/\d+/files/(\d+)'
            file_matches = re.findall(canvas_file_pattern, html_content)

            if not file_matches:
                return 0

            attachments_folder.mkdir(exist_ok=True)

            for file_id in file_matches:
                try:
                    # Get the file object from Canvas
                    course = assignment.course
                    canvas_file = course.get_file(file_id)

                    if canvas_file:
                        filename = getattr(canvas_file, 'filename', f'canvas_file_{file_id}')
                        file_url = getattr(canvas_file, 'url', None)

                        if file_url:
                            filename = self._sanitize_filename(filename)

                            download_result = await self.download_file(
                                file_url, attachments_folder, filename=filename
                            )

                            if download_result.get('success'):
                                download_info['files_downloaded'].append(download_result['file_path'])
                                files_downloaded += 1

                                self.logger.debug(f"Downloaded Canvas file reference",
                                                file_id=file_id,
                                                filename=filename)

                except Exception as e:
                    self.logger.warning(f"Could not download Canvas file reference",
                                      file_id=file_id, exception=e)

        except Exception as e:
            self.logger.warning(f"Could not process Canvas file references", exception=e)

        return files_downloaded

    async def _create_rubric_summary(self, rubric_data: Dict[str, Any], summary_file: Path) -> None:
        """
        Create a human-readable rubric summary file.

        Args:
            rubric_data: Rubric information dictionary
            summary_file: Path to save the summary file
        """
        try:
            summary_content = [
                f"Rubric Summary: {rubric_data.get('title', 'Unknown')}",
                f"Total Points: {rubric_data.get('points_possible', 'Not specified')}",
                f"Assignment: {rubric_data.get('assignment_name', 'Unknown')}",
                f"Rubric ID: {rubric_data.get('rubric_id', 'Unknown')}",
                f"Generated: {rubric_data.get('extracted_at', 'Unknown')}",
                "=" * 70,
                ""
            ]

            criteria = rubric_data.get('criteria', [])
            if criteria:
                summary_content.append("GRADING CRITERIA:")
                summary_content.append("-" * 50)
                summary_content.append("")

                for idx, criterion in enumerate(criteria, 1):
                    criterion_desc = criterion.get('description', 'No description')
                    criterion_points = criterion.get('points', 'Not specified')

                    summary_content.extend([
                        f"{idx}. {criterion_desc}",
                        f"   Points: {criterion_points}",
                        ""
                    ])

                    # Add rating levels if available
                    ratings = criterion.get('ratings', [])
                    if ratings:
                        summary_content.append("   Rating Levels:")
                        for rating in ratings:
                            rating_desc = rating.get('description', 'No description')
                            rating_points = rating.get('points', '?')
                            summary_content.append(f"     • {rating_desc} ({rating_points} pts)")
                        summary_content.append("")
            else:
                summary_content.extend([
                    "No grading criteria found in this rubric.",
                    ""
                ])

            # Add usage instructions
            summary_content.extend([
                "=" * 70,
                "INSTRUCTIONS:",
                "This rubric file contains the grading criteria for the assignment.",
                "Refer to this when completing your work to understand how you will be evaluated.",
                "Each criterion shows the maximum points available and rating descriptions.",
                ""
            ])

            async with aiofiles.open(summary_file, 'w', encoding='utf-8') as f:
                await f.write('\n'.join(summary_content))

            self.logger.debug(f"Created rubric summary",
                            rubric_id=rubric_data.get('rubric_id'),
                            criteria_count=len(criteria),
                            summary_file=str(summary_file))

        except Exception as e:
            self.logger.warning(f"Could not create rubric summary", exception=e)

# Register the downloader with the factory
from .base import ContentDownloaderFactory
ContentDownloaderFactory.register_downloader('assignments', AssignmentsDownloader)