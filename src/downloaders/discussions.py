"""
Discussions Downloader Module

This module implements the discussions downloader for Canvas courses.
Discussions are interactive forum-style conversations where students and
instructors can participate in threaded conversations about course topics.

Canvas Discussions include:
- Discussion topics and prompts
- Student and instructor replies
- Threaded conversation structure
- Assignment-linked discussions
- Group discussions
- Graded discussions
- File attachments and media
- Reply-to-reply conversations

Features:
- Download discussion topics and all replies
- Preserve threaded conversation structure
- Handle graded discussions and assignment links
- Process group discussions separately
- Download attached files and media
- Convert discussions to multiple formats (HTML, JSON, text)
- Maintain user information and timestamps

Usage:
    # Initialize the downloader
    downloader = DiscussionsDownloader(canvas_client, progress_tracker)

    # Download all discussions for a course
    stats = await downloader.download_course_content(course, course_info)
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

import aiofiles

try:
    import markdownify
    MARKDOWNIFY_AVAILABLE = True
except ImportError:
    MARKDOWNIFY_AVAILABLE = False

from canvasapi.discussion_topic import DiscussionTopic
from canvasapi.exceptions import CanvasException

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class DiscussionsDownloader(BaseDownloader):
    """
    Canvas Discussions Downloader

    This class handles downloading discussion topics and their replies from
    Canvas courses. It processes regular discussions (excluding announcements)
    and maintains the threaded conversation structure while preserving all
    user interactions and metadata.

    The downloader ensures that:
    - Complete discussion threads are preserved
    - User information and timestamps are maintained
    - Attached files and media are downloaded
    - Different discussion types are properly handled
    - Conversation hierarchy is maintained
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the discussions downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # Discussion processing settings
        self.download_content = True
        self.download_replies = True
        self.download_attachments = True
        self.convert_to_markdown = MARKDOWNIFY_AVAILABLE
        self.include_graded_discussions = True
        self.include_group_discussions = True

        # Reply processing options
        self.max_reply_depth = 10  # Maximum depth for nested replies
        self.include_deleted_replies = False
        self.sort_replies_by_date = True

        # Content formatting
        self.create_threaded_view = True
        self.create_flat_view = True
        self.include_user_avatars = False

        self.logger.info("Discussions downloader initialized",
                        markdown_available=MARKDOWNIFY_AVAILABLE,
                        max_reply_depth=self.max_reply_depth)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "discussions"

    def fetch_content_list(self, course) -> List[DiscussionTopic]:
        """
        Fetch all discussion topics from the course (excluding announcements).

        Args:
            course: Canvas course object

        Returns:
            List[DiscussionTopic]: List of discussion topic objects
        """
        try:
            self.logger.info(f"Fetching discussions for course {course.id}")

            # Get discussions (excluding announcements)
            discussions = list(course.get_discussion_topics(
                only_announcements=False,
                include=['author', 'summary', 'user_name', 'sections', 'group_category', 'assignment']
            ))

            # Filter out announcements (they should be handled by AnnouncementsDownloader)
            regular_discussions = []
            for discussion in discussions:
                # Check if it's an announcement
                discussion_type = getattr(discussion, 'discussion_type', '')
                if discussion_type != 'announcement':
                    regular_discussions.append(discussion)

            self.logger.info(f"Found {len(regular_discussions)} discussions "
                           f"(filtered from {len(discussions)} total)",
                           course_id=course.id,
                           discussion_count=len(regular_discussions))

            return regular_discussions

        except CanvasException as e:
            self.logger.error(f"Failed to fetch discussions", exception=e)
            raise DownloadError(f"Could not fetch discussions: {e}")

    def extract_metadata(self, discussion: DiscussionTopic) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from a discussion topic.

        Args:
            discussion: Canvas discussion topic object

        Returns:
            Dict[str, Any]: Discussion metadata
        """
        try:
            # Basic discussion information
            metadata = {
                'id': discussion.id,
                'title': discussion.title,
                'message': getattr(discussion, 'message', ''),
                'course_id': getattr(discussion, 'course_id', None),
                'author_id': getattr(discussion, 'author_id', None),
                'discussion_type': getattr(discussion, 'discussion_type', ''),
                'position': getattr(discussion, 'position', None),
                'podcast_enabled': getattr(discussion, 'podcast_enabled', False),
                'podcast_has_student_posts': getattr(discussion, 'podcast_has_student_posts', False),
                'require_initial_post': getattr(discussion, 'require_initial_post', False),
                'user_can_see_posts': getattr(discussion, 'user_can_see_posts', True),
                'discussion_subentry_count': getattr(discussion, 'discussion_subentry_count', 0),
                'read_state': getattr(discussion, 'read_state', 'unread'),
                'unread_count': getattr(discussion, 'unread_count', 0),
                'subscribed': getattr(discussion, 'subscribed', False),
                'subscription_hold': getattr(discussion, 'subscription_hold', None),
                'assignment_id': getattr(discussion, 'assignment_id', None),
                'delayed_post_at': self._format_date(getattr(discussion, 'delayed_post_at', None)),
                'published': getattr(discussion, 'published', True),
                'locked': getattr(discussion, 'locked', False),
                'pinned': getattr(discussion, 'pinned', False),
                'locked_for_user': getattr(discussion, 'locked_for_user', False),
                'user_name': getattr(discussion, 'user_name', ''),
                'topic_children': getattr(discussion, 'topic_children', []),
                'group_category_id': getattr(discussion, 'group_category_id', None),
                'allow_rating': getattr(discussion, 'allow_rating', False),
                'only_graders_can_rate': getattr(discussion, 'only_graders_can_rate', False),
                'sort_by_rating': getattr(discussion, 'sort_by_rating', False),
                'is_announcement': False  # This distinguishes it from announcements
            }

            # Date information
            metadata.update({
                'created_at': self._format_date(getattr(discussion, 'created_at', None)),
                'updated_at': self._format_date(getattr(discussion, 'updated_at', None)),
                'posted_at': self._format_date(getattr(discussion, 'posted_at', None)),
                'last_reply_at': self._format_date(getattr(discussion, 'last_reply_at', None)),
            })

            # Author information
            author = getattr(discussion, 'author', {})
            if author:
                metadata['author'] = {
                    'id': author.get('id'),
                    'name': author.get('name', ''),
                    'display_name': author.get('display_name', ''),
                    'avatar_image_url': author.get('avatar_image_url', ''),
                    'html_url': author.get('html_url', '')
                }

            # Assignment information (for graded discussions)
            assignment = getattr(discussion, 'assignment', None)
            if assignment:
                metadata['assignment'] = {
                    'id': getattr(assignment, 'id', None),
                    'name': getattr(assignment, 'name', ''),
                    'points_possible': getattr(assignment, 'points_possible', None),
                    'due_at': self._format_date(getattr(assignment, 'due_at', None)),
                    'submission_types': getattr(assignment, 'submission_types', [])
                }
                metadata['is_graded'] = True
            else:
                metadata['is_graded'] = False

            # Group information
            group_category = getattr(discussion, 'group_category', None)
            if group_category:
                metadata['group_category'] = {
                    'id': getattr(group_category, 'id', None),
                    'name': getattr(group_category, 'name', ''),
                    'role': getattr(group_category, 'role', ''),
                    'self_signup': getattr(group_category, 'self_signup', None),
                    'group_limit': getattr(group_category, 'group_limit', None)
                }
                metadata['is_group_discussion'] = True
            else:
                metadata['is_group_discussion'] = False

            # Section information
            sections = getattr(discussion, 'sections', [])
            if sections:
                metadata['sections'] = []
                for section in sections:
                    section_data = {
                        'id': getattr(section, 'id', None),
                        'name': getattr(section, 'name', ''),
                        'course_id': getattr(section, 'course_id', None)
                    }
                    metadata['sections'].append(section_data)

            # Attachment information
            attachments = getattr(discussion, 'attachments', [])
            if attachments:
                metadata['attachments'] = []
                for attachment in attachments:
                    attachment_data = {
                        'id': getattr(attachment, 'id', None),
                        'filename': getattr(attachment, 'filename', ''),
                        'display_name': getattr(attachment, 'display_name', ''),
                        'content_type': getattr(attachment, 'content_type', ''),
                        'size': getattr(attachment, 'size', 0),
                        'url': getattr(attachment, 'url', ''),
                        'created_at': self._format_date(getattr(attachment, 'created_at', None))
                    }
                    metadata['attachments'].append(attachment_data)

            # Content analysis
            if metadata['message']:
                metadata['content_stats'] = self._analyze_html_content(metadata['message'])

            # Canvas URLs
            html_url = getattr(discussion, 'html_url', '')
            if html_url:
                metadata['html_url'] = html_url
                metadata['canvas_url'] = html_url

            # Permissions
            permissions = getattr(discussion, 'permissions', {})
            if permissions:
                metadata['permissions'] = {
                    'attach': permissions.get('attach', False),
                    'update': permissions.get('update', False),
                    'reply': permissions.get('reply', False),
                    'delete': permissions.get('delete', False)
                }

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract discussion metadata",
                            discussion_id=getattr(discussion, 'id', 'unknown'),
                            exception=e)

            # Return minimal metadata on error
            return {
                'id': getattr(discussion, 'id', None),
                'title': getattr(discussion, 'title', 'Unknown Discussion'),
                'message': '',
                'error': f"Metadata extraction failed: {e}"
            }

    def _analyze_html_content(self, html_content: str) -> Dict[str, Any]:
        """Analyze HTML content to extract useful statistics."""
        if not html_content:
            return {'word_count': 0, 'character_count': 0, 'has_images': False, 'has_links': False}

        import re

        # Count words (remove HTML tags for counting)
        text_content = re.sub(r'<[^>]+>', ' ', html_content)
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        word_count = len(text_content.split()) if text_content else 0

        # Check for media content
        has_images = bool(re.search(r'<img[^>]*>', html_content, re.IGNORECASE))
        has_links = bool(re.search(r'<a[^>]*href[^>]*>', html_content, re.IGNORECASE))
        has_videos = bool(re.search(r'<video[^>]*>', html_content, re.IGNORECASE))

        # Extract embedded file links
        file_links = re.findall(r'/files/(\d+)/', html_content)

        return {
            'word_count': word_count,
            'character_count': len(html_content),
            'text_length': len(text_content),
            'has_images': has_images,
            'has_links': has_links,
            'has_videos': has_videos,
            'has_canvas_files': bool(file_links),
            'embedded_file_ids': file_links,
            'html_length': len(html_content)
        }

    def get_download_info(self, discussion: DiscussionTopic) -> Optional[Dict[str, str]]:
        """
        Get download information for a discussion.

        Discussions are processed rather than directly downloaded.

        Args:
            discussion: Canvas discussion topic object

        Returns:
            Optional[Dict[str, str]]: Download information or None
        """
        return None

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all discussions for a course.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting discussions download for course",
                           course_name=course_info.get('full_name', 'Unknown'),
                           course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if discussions are enabled
            if not self.config.is_content_type_enabled('discussions'):
                self.logger.info("Discussions download is disabled")
                return self.stats

            # Fetch discussions
            discussions = self.fetch_content_list(course)

            if not discussions:
                self.logger.info("No discussions found in course")
                return self.stats

            self.stats['total_items'] = len(discussions)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(discussions))

            # Process each discussion
            items_metadata = []

            for index, discussion in enumerate(discussions, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(discussion)
                    metadata['item_number'] = index

                    # Process discussion content
                    await self._process_discussion(discussion, metadata, index)

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                    self.stats['downloaded_items'] += 1

                except Exception as e:
                    self.logger.error(f"Failed to process discussion",
                                    discussion_id=getattr(discussion, 'id', 'unknown'),
                                    discussion_title=getattr(discussion, 'title', 'unknown'),
                                    exception=e)
                    self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            self.logger.info(f"Discussions download completed",
                           course_id=str(course.id),
                           **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Discussions download failed", exception=e)
            raise DownloadError(f"Discussions download failed: {e}")

    async def _process_discussion(self, discussion: DiscussionTopic,
                                metadata: Dict[str, Any], index: int):
        """
        Process a single discussion and create associated files.

        Args:
            discussion: Canvas discussion object
            metadata: Discussion metadata
            index: Discussion index number
        """
        try:
            discussion_title = self.sanitize_filename(discussion.title)

            # Create discussion-specific folder
            discussion_folder = self.content_folder / f"discussion_{index:03d}_{discussion_title}"
            discussion_folder.mkdir(exist_ok=True)

            # Save discussion content
            if self.download_content:
                await self._save_discussion_content(discussion, metadata, discussion_folder, index)

            # Download attachments
            if self.download_attachments and metadata.get('attachments'):
                await self._download_discussion_attachments(discussion, metadata, discussion_folder)

            # Download embedded files
            if self.download_attachments and metadata.get('content_stats', {}).get('embedded_file_ids'):
                await self._download_embedded_files(discussion, metadata, discussion_folder)

            # Download discussion replies
            if self.download_replies and metadata.get('discussion_subentry_count', 0) > 0:
                await self._download_discussion_replies(discussion, metadata, discussion_folder)

            # Save individual discussion metadata
            await self._save_discussion_metadata(metadata, discussion_folder)

        except Exception as e:
            self.logger.error(f"Failed to process discussion content",
                            discussion_id=discussion.id,
                            exception=e)
            raise

    async def _save_discussion_content(self, discussion: DiscussionTopic,
                                     metadata: Dict[str, Any],
                                     discussion_folder: Path, index: int):
        """Save discussion content in multiple formats."""
        try:
            message = metadata.get('message', '')
            if not message:
                return

            discussion_title = self.sanitize_filename(discussion.title)

            # Save as HTML
            html_filename = f"discussion_{index:03d}_{discussion_title}.html"
            html_path = discussion_folder / html_filename

            html_content = self._create_html_document(discussion.title, message, metadata)

            async with aiofiles.open(html_path, 'w', encoding='utf-8') as f:
                await f.write(html_content)

            self.logger.debug(f"Saved discussion as HTML", file_path=str(html_path))

            # Save as plain text
            text_filename = f"discussion_{index:03d}_{discussion_title}.txt"
            text_path = discussion_folder / text_filename

            text_content = self._html_to_text(message)
            text_document = self._create_text_document(discussion.title, text_content, metadata)

            async with aiofiles.open(text_path, 'w', encoding='utf-8') as f:
                await f.write(text_document)

            self.logger.debug(f"Saved discussion as text", file_path=str(text_path))

        except Exception as e:
            self.logger.error(f"Failed to save discussion content", exception=e)

    def _create_html_document(self, title: str, content: str, metadata: Dict[str, Any]) -> str:
        """Create a complete HTML document with discussion content."""
        # Build information header
        info_html = ""

        if metadata.get('posted_at'):
            info_html += f"<p><strong>Posted:</strong> {metadata['posted_at']}</p>"

        author = metadata.get('author', {})
        if author.get('name'):
            info_html += f"<p><strong>Author:</strong> {author['name']}</p>"

        if metadata.get('is_graded'):
            assignment = metadata.get('assignment', {})
            points = assignment.get('points_possible')
            due_date = assignment.get('due_at')
            if points:
                info_html += f"<p><strong>Points Possible:</strong> {points}</p>"
            if due_date:
                info_html += f"<p><strong>Due Date:</strong> {due_date}</p>"

        if metadata.get('discussion_subentry_count', 0) > 0:
            info_html += f"<p><strong>Replies:</strong> {metadata['discussion_subentry_count']}</p>"

        if metadata.get('html_url'):
            info_html += f"<p><strong>Canvas URL:</strong> <a href=\"{metadata['html_url']}\">{metadata['html_url']}</a></p>"

        # Create HTML document
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
        .discussion-header {{
            background-color: #f0f8ff;
            padding: 15px;
            border-left: 4px solid #2196f3;
            margin-bottom: 20px;
            border-radius: 4px;
        }}
        .discussion-content {{
            margin-top: 20px;
        }}
        h1 {{
            color: #2196f3;
            border-bottom: 2px solid #ddd;
            padding-bottom: 10px;
        }}
        .discussion-badge {{
            background-color: #2196f3;
            color: white;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
        }}
        .graded-badge {{
            background-color: #ff5722;
            color: white;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
            margin-left: 8px;
        }}
    </style>
</head>
<body>
    <div>
        <span class="discussion-badge">DISCUSSION</span>
        {f'<span class="graded-badge">GRADED</span>' if metadata.get('is_graded') else ''}
    </div>
    <h1>{title}</h1>
    
    <div class="discussion-header">
        <h2>Discussion Information</h2>
        {info_html}
    </div>
    
    <div class="discussion-content">
        <h2>Prompt</h2>
        {content}
    </div>
    
    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    def _create_text_document(self, title: str, content: str, metadata: Dict[str, Any]) -> str:
        """Create a plain text document with discussion content."""
        header_lines = [
            "=" * 60,
            f"DISCUSSION: {title}",
            "=" * 60,
            ""
        ]

        if metadata.get('is_graded'):
            header_lines.append("[GRADED DISCUSSION]")
            header_lines.append("")

        if metadata.get('posted_at'):
            header_lines.append(f"Posted: {metadata['posted_at']}")

        author = metadata.get('author', {})
        if author.get('name'):
            header_lines.append(f"Author: {author['name']}")

        if metadata.get('discussion_subentry_count', 0) > 0:
            header_lines.append(f"Replies: {metadata['discussion_subentry_count']}")

        if metadata.get('html_url'):
            header_lines.append(f"Canvas URL: {metadata['html_url']}")

        header_lines.extend(["", "-" * 60, "PROMPT:", ""])

        footer_lines = [
            "",
            "-" * 60,
            f"Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ]

        return "\n".join(header_lines) + content + "\n".join(footer_lines)

    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML content to plain text."""
        if not html_content:
            return ""

        import re

        text = html_content

        # Replace HTML elements with text equivalents
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</h[1-6]>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<li[^>]*>', '• ', text, flags=re.IGNORECASE)
        text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Clean up whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = text.strip()

        return text

    async def _download_discussion_attachments(self, discussion: DiscussionTopic,
                                             metadata: Dict[str, Any],
                                             discussion_folder: Path):
        """Download files attached to discussions."""
        try:
            attachments = metadata.get('attachments', [])
            if not attachments:
                return

            attachments_folder = discussion_folder / 'attachments'
            attachments_folder.mkdir(exist_ok=True)

            for attachment in attachments:
                try:
                    download_url = attachment.get('url', '')
                    if not download_url:
                        continue

                    original_filename = attachment.get('filename', f"attachment_{attachment.get('id', 'unknown')}")
                    sanitized_filename = self.sanitize_filename(original_filename)
                    file_path = attachments_folder / sanitized_filename

                    success = await self.download_file(download_url, file_path)

                    if success:
                        self.logger.debug(f"Downloaded discussion attachment",
                                        attachment_id=attachment.get('id'),
                                        filename=sanitized_filename)

                except Exception as e:
                    self.logger.warning(f"Could not download attachment",
                                      attachment_id=attachment.get('id', 'unknown'),
                                      exception=e)

        except Exception as e:
            self.logger.error(f"Failed to download discussion attachments", exception=e)

    async def _download_embedded_files(self, discussion: DiscussionTopic,
                                     metadata: Dict[str, Any],
                                     discussion_folder: Path):
        """Download files embedded in discussion content."""
        try:
            embedded_file_ids = metadata.get('content_stats', {}).get('embedded_file_ids', [])
            if not embedded_file_ids:
                return

            embedded_folder = discussion_folder / 'embedded_files'
            embedded_folder.mkdir(exist_ok=True)

            course = self.canvas_client.get_course(discussion.course_id)

            for file_id in embedded_file_ids:
                try:
                    canvas_file = course.get_file(file_id)

                    original_filename = getattr(canvas_file, 'filename', f'file_{file_id}')
                    sanitized_filename = self.sanitize_filename(original_filename)
                    file_path = embedded_folder / sanitized_filename

                    download_url = getattr(canvas_file, 'url', '')
                    if download_url:
                        success = await self.download_file(download_url, file_path)

                        if success:
                            self.logger.debug(f"Downloaded embedded file",
                                            file_id=file_id,
                                            filename=sanitized_filename)

                except Exception as e:
                    self.logger.warning(f"Could not download embedded file",
                                      file_id=file_id,
                                      exception=e)

        except Exception as e:
            self.logger.error(f"Failed to download embedded files", exception=e)

    async def _download_discussion_replies(self, discussion: DiscussionTopic,
                                         metadata: Dict[str, Any],
                                         discussion_folder: Path):
        """Download and process discussion replies."""
        try:
            if metadata.get('discussion_subentry_count', 0) == 0:
                return

            # Get discussion entries (replies)
            entries = list(discussion.get_topic_entries())

            if not entries:
                return

            # Create comprehensive reply structure
            replies_data = await self._process_reply_tree(entries)

            # Save replies in multiple formats
            await self._save_replies_json(replies_data, discussion, discussion_folder)

            if self.create_threaded_view:
                await self._save_replies_threaded_html(replies_data, discussion, discussion_folder)

            if self.create_flat_view:
                await self._save_replies_flat_html(replies_data, discussion, discussion_folder)

        except Exception as e:
            self.logger.error(f"Failed to download discussion replies", exception=e)

    async def _process_reply_tree(self, entries: List) -> List[Dict[str, Any]]:
        """Process discussion entries into a structured reply tree."""
        try:
            replies_data = []

            for entry in entries:
                entry_data = {
                    'id': getattr(entry, 'id', None),
                    'user_id': getattr(entry, 'user_id', None),
                    'user_name': getattr(entry, 'user_name', ''),
                    'message': getattr(entry, 'message', ''),
                    'created_at': self._format_date(getattr(entry, 'created_at', None)),
                    'updated_at': self._format_date(getattr(entry, 'updated_at', None)),
                    'parent_id': getattr(entry, 'parent_id', None),
                    'rating_count': getattr(entry, 'rating_count', 0),
                    'rating_sum': getattr(entry, 'rating_sum', 0),
                    'read_state': getattr(entry, 'read_state', 'unread'),
                    'forced_read_state': getattr(entry, 'forced_read_state', False),
                    'replies': []  # Will be populated with child replies
                }

                # Analyze reply content
                if entry_data['message']:
                    entry_data['content_stats'] = self._analyze_html_content(entry_data['message'])

                replies_data.append(entry_data)

            # Sort by creation date if requested
            if self.sort_replies_by_date:
                replies_data.sort(key=lambda x: x.get('created_at', ''))

            # Build reply hierarchy
            reply_tree = self._build_reply_hierarchy(replies_data)

            return reply_tree

        except Exception as e:
            self.logger.error(f"Failed to process reply tree", exception=e)
            return []

    def _build_reply_hierarchy(self, replies_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build hierarchical reply structure."""
        # Create lookup map
        replies_map = {reply['id']: reply for reply in replies_data}

        # Find top-level replies and build hierarchy
        top_level_replies = []

        for reply in replies_data:
            parent_id = reply.get('parent_id')

            if parent_id and parent_id in replies_map:
                # This is a reply to another reply
                parent_reply = replies_map[parent_id]
                parent_reply['replies'].append(reply)
            else:
                # This is a top-level reply to the discussion
                top_level_replies.append(reply)

        return top_level_replies

    async def _save_replies_json(self, replies_data: List[Dict[str, Any]],
                               discussion: DiscussionTopic,
                               discussion_folder: Path):
        """Save replies data as JSON."""
        try:
            replies_filename = f"replies.json"
            replies_path = discussion_folder / replies_filename

            replies_document = {
                'discussion_id': discussion.id,
                'discussion_title': discussion.title,
                'total_replies': len(replies_data),
                'reply_structure': 'hierarchical',
                'replies': replies_data,
                'downloaded_at': datetime.now().isoformat()
            }

            async with aiofiles.open(replies_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(replies_document, indent=2, ensure_ascii=False))

            self.logger.debug(f"Saved discussion replies as JSON",
                            file_path=str(replies_path),
                            reply_count=len(replies_data))

        except Exception as e:
            self.logger.error(f"Failed to save replies JSON", exception=e)

    async def _save_replies_threaded_html(self, replies_data: List[Dict[str, Any]],
                                        discussion: DiscussionTopic,
                                        discussion_folder: Path):
        """Save replies as threaded HTML view."""
        try:
            replies_filename = f"replies_threaded.html"
            replies_path = discussion_folder / replies_filename

            html_content = self._create_threaded_html(discussion.title, replies_data)

            async with aiofiles.open(replies_path, 'w', encoding='utf-8') as f:
                await f.write(html_content)

            self.logger.debug(f"Saved threaded replies HTML",
                            file_path=str(replies_path))

        except Exception as e:
            self.logger.error(f"Failed to save threaded replies HTML", exception=e)

    def _create_threaded_html(self, title: str, replies_data: List[Dict[str, Any]]) -> str:
        """Create threaded HTML view of replies."""

        def render_reply(reply: Dict[str, Any], depth: int = 0) -> str:
            indent = depth * 40

            reply_html = f"""
            <div class="reply" style="margin-left: {indent}px; border-left: 2px solid #ddd; padding-left: 15px; margin-bottom: 20px;">
                <div class="reply-header">
                    <strong>{reply.get('user_name', 'Unknown User')}</strong>
                    <span class="reply-date">{reply.get('created_at', '')}</span>
                </div>
                <div class="reply-content">
                    {reply.get('message', '')}
                </div>
            """

            # Add nested replies
            for nested_reply in reply.get('replies', []):
                if depth < self.max_reply_depth:
                    reply_html += render_reply(nested_reply, depth + 1)

            reply_html += "</div>"
            return reply_html

        replies_html = ""
        for reply in replies_data:
            replies_html += render_reply(reply)

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Discussion Replies: {title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .reply {{
            background-color: #f9f9f9;
            border-radius: 5px;
            padding: 15px;
        }}
        .reply-header {{
            color: #666;
            font-size: 0.9em;
            margin-bottom: 10px;
        }}
        .reply-date {{
            float: right;
        }}
        .reply-content {{
            clear: both;
        }}
    </style>
</head>
<body>
    <h1>Discussion Replies: {title}</h1>
    <p><strong>Total Replies:</strong> {len(replies_data)}</p>
    <hr>
    {replies_html}
    
    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    async def _save_replies_flat_html(self, replies_data: List[Dict[str, Any]],
                                    discussion: DiscussionTopic,
                                    discussion_folder: Path):
        """Save replies as flat HTML view (chronological order)."""
        try:
            replies_filename = f"replies_flat.html"
            replies_path = discussion_folder / replies_filename

            # Flatten reply structure
            flat_replies = self._flatten_replies(replies_data)

            # Sort by date
            flat_replies.sort(key=lambda x: x.get('created_at', ''))

            html_content = self._create_flat_html(discussion.title, flat_replies)

            async with aiofiles.open(replies_path, 'w', encoding='utf-8') as f:
                await f.write(html_content)

            self.logger.debug(f"Saved flat replies HTML",
                            file_path=str(replies_path))

        except Exception as e:
            self.logger.error(f"Failed to save flat replies HTML", exception=e)

    def _flatten_replies(self, replies_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten hierarchical reply structure."""
        flat_replies = []

        def flatten_recursive(reply: Dict[str, Any]):
            flat_replies.append(reply)
            for nested_reply in reply.get('replies', []):
                flatten_recursive(nested_reply)

        for reply in replies_data:
            flatten_recursive(reply)

        return flat_replies

    def _create_flat_html(self, title: str, flat_replies: List[Dict[str, Any]]) -> str:
        """Create flat HTML view of replies."""
        replies_html = ""

        for reply in flat_replies:
            replies_html += f"""
            <div class="reply">
                <div class="reply-header">
                    <strong>{reply.get('user_name', 'Unknown User')}</strong>
                    <span class="reply-date">{reply.get('created_at', '')}</span>
                </div>
                <div class="reply-content">
                    {reply.get('message', '')}
                </div>
            </div>
            """

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Discussion Replies (Flat): {title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .reply {{
            background-color: #f9f9f9;
            border-radius: 5px;
            padding: 15px;
            margin-bottom: 15px;
        }}
        .reply-header {{
            color: #666;
            font-size: 0.9em;
            margin-bottom: 10px;
            border-bottom: 1px solid #ddd;
            padding-bottom: 5px;
        }}
        .reply-date {{
            float: right;
        }}
        .reply-content {{
            clear: both;
        }}
    </style>
</head>
<body>
    <h1>Discussion Replies (Chronological): {title}</h1>
    <p><strong>Total Replies:</strong> {len(flat_replies)}</p>
    <hr>
    {replies_html}
    
    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    async def _save_discussion_metadata(self, metadata: Dict[str, Any], discussion_folder: Path):
        """Save individual discussion metadata."""
        try:
            metadata_path = discussion_folder / 'discussion_metadata.json'

            async with aiofiles.open(metadata_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False, default=str))

            self.logger.debug(f"Saved discussion metadata",
                            file_path=str(metadata_path))

        except Exception as e:
            self.logger.error(f"Failed to save discussion metadata", exception=e)

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
ContentDownloaderFactory.register_downloader('discussions', DiscussionsDownloader)