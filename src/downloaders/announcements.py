"""
Announcements Downloader Module

This module implements the announcements downloader for Canvas courses.
Announcements in Canvas are essentially discussion topics that are marked
as announcements. They contain important course information, updates,
and communications from instructors.

Announcements typically include:
- Title and content body (HTML)
- Publication and posting dates
- Author information
- Attachment files
- Read/unread status
- Comments and replies (if enabled)
- Delayed posting settings

Features:
- Download announcement content as HTML and text
- Extract and download announcement attachments
- Save author and timing information
- Handle delayed posting announcements
- Process announcement replies and comments
- Convert HTML content to readable formats

Usage:
    # Initialize the downloader
    downloader = AnnouncementsDownloader(canvas_client, progress_tracker)

    # Download all announcements for a course
    stats = await downloader.download_course_content(course, course_info)
"""

import asyncio
import json
import re
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


class AnnouncementsDownloader(BaseDownloader):
    """
    Canvas Announcements Downloader

    This class handles downloading announcements from Canvas courses.
    Announcements are special discussion topics that provide important
    course information and updates. This downloader ensures all
    announcement content is preserved with proper formatting and metadata.

    The downloader processes announcement content, attachments, and
    maintains comprehensive metadata about posting dates, authors,
    and read status.
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the announcements downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # Announcement processing settings
        self.download_content = True
        self.download_attachments = True
        self.download_replies = True
        self.convert_to_markdown = MARKDOWNIFY_AVAILABLE
        self.include_html_styling = True

        # Content processing options
        self.clean_html = True
        self.preserve_formatting = True

        self.logger.info("Announcements downloader initialized",
                        markdown_available=MARKDOWNIFY_AVAILABLE,
                        download_replies=self.download_replies)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "announcements"

    def fetch_content_list(self, course) -> List[DiscussionTopic]:
        """
        Fetch all announcements from the course.

        Announcements in Canvas are discussion topics with announcement=True.

        Args:
            course: Canvas course object

        Returns:
            List[DiscussionTopic]: List of announcement discussion topics
        """
        try:
            self.logger.info(f"Fetching announcements for course {course.id}")

            # Get announcements (which are discussion topics marked as announcements)
            announcements = list(course.get_discussion_topics(
                only_announcements=True,
                include=['author', 'summary', 'user_name', 'sections', 'group_category']
            ))

            self.logger.info(f"Found {len(announcements)} announcements",
                           course_id=course.id,
                           announcement_count=len(announcements))

            return announcements

        except CanvasException as e:
            self.logger.error(f"Failed to fetch announcements", exception=e)
            raise DownloadError(f"Could not fetch announcements: {e}")

    def extract_metadata(self, announcement: DiscussionTopic) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from an announcement.

        Args:
            announcement: Canvas announcement (DiscussionTopic) object

        Returns:
            Dict[str, Any]: Announcement metadata
        """
        try:
            # Basic announcement information
            metadata = {
                'id': announcement.id,
                'title': announcement.title,
                'message': getattr(announcement, 'message', ''),
                'course_id': getattr(announcement, 'course_id', None),
                'author_id': getattr(announcement, 'author_id', None),
                'discussion_type': getattr(announcement, 'discussion_type', ''),
                'position': getattr(announcement, 'position', None),
                'podcast_enabled': getattr(announcement, 'podcast_enabled', False),
                'podcast_has_student_posts': getattr(announcement, 'podcast_has_student_posts', False),
                'require_initial_post': getattr(announcement, 'require_initial_post', False),
                'user_can_see_posts': getattr(announcement, 'user_can_see_posts', True),
                'discussion_subentry_count': getattr(announcement, 'discussion_subentry_count', 0),
                'read_state': getattr(announcement, 'read_state', 'unread'),
                'unread_count': getattr(announcement, 'unread_count', 0),
                'subscribed': getattr(announcement, 'subscribed', False),
                'subscription_hold': getattr(announcement, 'subscription_hold', None),
                'assignment_id': getattr(announcement, 'assignment_id', None),
                'delayed_post_at': self._format_date(getattr(announcement, 'delayed_post_at', None)),
                'published': getattr(announcement, 'published', True),
                'locked': getattr(announcement, 'locked', False),
                'pinned': getattr(announcement, 'pinned', False),
                'locked_for_user': getattr(announcement, 'locked_for_user', False),
                'user_name': getattr(announcement, 'user_name', ''),
                'topic_children': getattr(announcement, 'topic_children', []),
                'group_category_id': getattr(announcement, 'group_category_id', None),
                'allow_rating': getattr(announcement, 'allow_rating', False),
                'only_graders_can_rate': getattr(announcement, 'only_graders_can_rate', False),
                'sort_by_rating': getattr(announcement, 'sort_by_rating', False),
                'is_announcement': True  # This distinguishes it from regular discussions
            }

            # Date information
            metadata.update({
                'created_at': self._format_date(getattr(announcement, 'created_at', None)),
                'updated_at': self._format_date(getattr(announcement, 'updated_at', None)),
                'posted_at': self._format_date(getattr(announcement, 'posted_at', None)),
                'last_reply_at': self._format_date(getattr(announcement, 'last_reply_at', None)),
            })

            # Author information
            author = getattr(announcement, 'author', {})
            if author:
                metadata['author'] = {
                    'id': author.get('id'),
                    'name': author.get('name', ''),
                    'display_name': author.get('display_name', ''),
                    'avatar_image_url': author.get('avatar_image_url', ''),
                    'html_url': author.get('html_url', '')
                }

            # Summary information
            summary = getattr(announcement, 'summary', '')
            if summary:
                metadata['summary'] = summary

            # Section information
            sections = getattr(announcement, 'sections', [])
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
            attachments = getattr(announcement, 'attachments', [])
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
                        'created_at': self._format_date(getattr(attachment, 'created_at', None)),
                        'updated_at': self._format_date(getattr(attachment, 'updated_at', None))
                    }
                    metadata['attachments'].append(attachment_data)

            # HTML content analysis
            if metadata['message']:
                metadata['content_stats'] = self._analyze_html_content(metadata['message'])

            # Canvas URLs
            html_url = getattr(announcement, 'html_url', '')
            if html_url:
                metadata['html_url'] = html_url
                metadata['canvas_url'] = html_url

            # Permissions and access
            permissions = getattr(announcement, 'permissions', {})
            if permissions:
                metadata['permissions'] = {
                    'attach': permissions.get('attach', False),
                    'update': permissions.get('update', False),
                    'reply': permissions.get('reply', False),
                    'delete': permissions.get('delete', False)
                }

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract announcement metadata",
                            announcement_id=getattr(announcement, 'id', 'unknown'),
                            exception=e)

            # Return minimal metadata on error
            return {
                'id': getattr(announcement, 'id', None),
                'title': getattr(announcement, 'title', 'Unknown Announcement'),
                'message': '',
                'error': f"Metadata extraction failed: {e}"
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

        # Count words (remove HTML tags for counting)
        import re
        text_content = re.sub(r'<[^>]+>', ' ', html_content)
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        word_count = len(text_content.split()) if text_content else 0

        # Check for media content
        has_images = bool(re.search(r'<img[^>]*>', html_content, re.IGNORECASE))
        has_links = bool(re.search(r'<a[^>]*href[^>]*>', html_content, re.IGNORECASE))
        has_videos = bool(re.search(r'<video[^>]*>', html_content, re.IGNORECASE))
        has_audio = bool(re.search(r'<audio[^>]*>', html_content, re.IGNORECASE))

        # Extract embedded file links
        file_links = re.findall(r'/files/(\d+)/', html_content)

        # Check for common Canvas embedded content
        has_canvas_files = bool(file_links)
        has_equations = bool(re.search(r'class="math_equation_latex"', html_content, re.IGNORECASE))

        return {
            'word_count': word_count,
            'character_count': len(html_content),
            'text_length': len(text_content),
            'has_images': has_images,
            'has_links': has_links,
            'has_videos': has_videos,
            'has_audio': has_audio,
            'has_canvas_files': has_canvas_files,
            'has_equations': has_equations,
            'embedded_file_ids': file_links,
            'html_length': len(html_content)
        }

    def get_download_info(self, announcement: DiscussionTopic) -> Optional[Dict[str, str]]:
        """
        Get download information for an announcement.

        Announcements don't have direct download URLs like files.
        Instead, we process their content to create local files.

        Args:
            announcement: Canvas announcement (DiscussionTopic) object

        Returns:
            Optional[Dict[str, str]]: Download information or None
        """
        # Announcements are processed rather than directly downloaded
        return None

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all announcements for a course.

        This method overrides the base implementation to handle the specific
        requirements of announcement processing.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting announcements download for course",
                           course_name=course_info.get('full_name', 'Unknown'),
                           course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if announcements are enabled
            if not self.config.is_content_type_enabled('announcements'):
                self.logger.info("Announcements download is disabled")
                return self.stats

            # Fetch announcements
            announcements = self.fetch_content_list(course)

            if not announcements:
                self.logger.info("No announcements found in course")
                return self.stats

            self.stats['total_items'] = len(announcements)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(announcements))

            # Process each announcement
            items_metadata = []

            for index, announcement in enumerate(announcements, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(announcement)
                    metadata['item_number'] = index

                    # Process announcement content
                    await self._process_announcement(announcement, metadata, index)

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                    self.stats['downloaded_items'] += 1

                except Exception as e:
                    self.logger.error(f"Failed to process announcement",
                                    announcement_id=getattr(announcement, 'id', 'unknown'),
                                    announcement_title=getattr(announcement, 'title', 'unknown'),
                                    exception=e)
                    self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            self.logger.info(f"Announcements download completed",
                           course_id=str(course.id),
                           **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Announcements download failed", exception=e)
            raise DownloadError(f"Announcements download failed: {e}")

    async def _process_announcement(self, announcement: DiscussionTopic,
                                  metadata: Dict[str, Any], index: int):
        """
        Process a single announcement and create associated files.

        Args:
            announcement: Canvas announcement object
            metadata: Announcement metadata
            index: Announcement index number
        """
        try:
            announcement_title = self.sanitize_filename(announcement.title)

            # Save announcement content
            if self.download_content:
                await self._save_announcement_content(announcement, metadata, index)

            # Download attachments
            if self.download_attachments and metadata.get('attachments'):
                await self._download_announcement_attachments(announcement, metadata, index)

            # Download embedded files
            if self.download_attachments and metadata.get('content_stats', {}).get('embedded_file_ids'):
                await self._download_embedded_files(announcement, metadata, index)

            # Download replies/comments if enabled
            if self.download_replies and metadata.get('discussion_subentry_count', 0) > 0:
                await self._download_announcement_replies(announcement, metadata, index)

        except Exception as e:
            self.logger.error(f"Failed to process announcement content",
                            announcement_id=announcement.id,
                            exception=e)
            raise

    async def _save_announcement_content(self, announcement: DiscussionTopic,
                                       metadata: Dict[str, Any], index: int):
        """Save announcement content in multiple formats."""
        try:
            message = metadata.get('message', '')
            if not message:
                return

            announcement_title = self.sanitize_filename(announcement.title)

            # Save as HTML
            html_filename = self.generate_filename(index, announcement_title, 'html')
            html_path = self.content_folder / html_filename

            # Create a complete HTML document
            html_content = self._create_html_document(announcement.title, message, metadata)

            async with aiofiles.open(html_path, 'w', encoding='utf-8') as f:
                await f.write(html_content)

            self.logger.debug(f"Saved announcement as HTML",
                            file_path=str(html_path))

            # Save as plain text
            text_filename = self.generate_filename(index, announcement_title, 'txt')
            text_path = self.content_folder / text_filename

            # Convert HTML to plain text
            text_content = self._html_to_text(message)
            text_document = self._create_text_document(announcement.title, text_content, metadata)

            async with aiofiles.open(text_path, 'w', encoding='utf-8') as f:
                await f.write(text_document)

            self.logger.debug(f"Saved announcement as text",
                            file_path=str(text_path))

            # Convert to Markdown if available
            if self.convert_to_markdown and MARKDOWNIFY_AVAILABLE:
                try:
                    markdown_content = markdownify.markdownify(message)

                    markdown_filename = self.generate_filename(index, announcement_title, 'md')
                    markdown_path = self.content_folder / markdown_filename

                    # Create markdown document with header
                    markdown_document = self._create_markdown_document(
                        announcement.title, markdown_content, metadata
                    )

                    async with aiofiles.open(markdown_path, 'w', encoding='utf-8') as f:
                        await f.write(markdown_document)

                    self.logger.debug(f"Saved announcement as Markdown",
                                    file_path=str(markdown_path))

                except Exception as e:
                    self.logger.warning(f"Failed to convert announcement to Markdown", exception=e)

        except Exception as e:
            self.logger.error(f"Failed to save announcement content", exception=e)

    def _create_html_document(self, title: str, content: str, metadata: Dict[str, Any]) -> str:
        """Create a complete HTML document with announcement content."""
        # Basic announcement information for the header
        info_html = ""

        if metadata.get('posted_at'):
            info_html += f"<p><strong>Posted:</strong> {metadata['posted_at']}</p>"

        author = metadata.get('author', {})
        if author.get('name'):
            info_html += f"<p><strong>Author:</strong> {author['name']}</p>"

        if metadata.get('last_reply_at'):
            info_html += f"<p><strong>Last Reply:</strong> {metadata['last_reply_at']}</p>"

        if metadata.get('discussion_subentry_count', 0) > 0:
            info_html += f"<p><strong>Replies:</strong> {metadata['discussion_subentry_count']}</p>"

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
        .announcement-header {{
            background-color: #e8f4fd;
            padding: 15px;
            border-left: 4px solid #1976d2;
            margin-bottom: 20px;
            border-radius: 4px;
        }}
        .announcement-content {{
            margin-top: 20px;
        }}
        h1 {{
            color: #1976d2;
            border-bottom: 2px solid #ddd;
            padding-bottom: 10px;
        }}
        .announcement-badge {{
            background-color: #ff9800;
            color: white;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="announcement-badge">ANNOUNCEMENT</div>
    <h1>{title}</h1>
    
    <div class="announcement-header">
        <h2>Announcement Information</h2>
        {info_html}
    </div>
    
    <div class="announcement-content">
        {content}
    </div>
    
    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    def _create_text_document(self, title: str, content: str, metadata: Dict[str, Any]) -> str:
        """Create a plain text document with announcement content."""
        # Build header information
        header_lines = [
            "=" * 60,
            f"ANNOUNCEMENT: {title}",
            "=" * 60,
            ""
        ]

        if metadata.get('posted_at'):
            header_lines.append(f"Posted: {metadata['posted_at']}")

        author = metadata.get('author', {})
        if author.get('name'):
            header_lines.append(f"Author: {author['name']}")

        if metadata.get('last_reply_at'):
            header_lines.append(f"Last Reply: {metadata['last_reply_at']}")

        if metadata.get('discussion_subentry_count', 0) > 0:
            header_lines.append(f"Replies: {metadata['discussion_subentry_count']}")

        if metadata.get('html_url'):
            header_lines.append(f"Canvas URL: {metadata['html_url']}")

        header_lines.extend(["", "-" * 60, ""])

        # Footer
        footer_lines = [
            "",
            "-" * 60,
            f"Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ]

        return "\n".join(header_lines) + content + "\n".join(footer_lines)

    def _create_markdown_document(self, title: str, content: str, metadata: Dict[str, Any]) -> str:
        """Create a Markdown document with announcement content."""
        # Build header
        header_lines = [
            f"# 📢 {title}",
            "",
            "> **Announcement**",
            ""
        ]

        if metadata.get('posted_at'):
            header_lines.append(f"**Posted:** {metadata['posted_at']}")

        author = metadata.get('author', {})
        if author.get('name'):
            header_lines.append(f"**Author:** {author['name']}")

        if metadata.get('last_reply_at'):
            header_lines.append(f"**Last Reply:** {metadata['last_reply_at']}")

        if metadata.get('discussion_subentry_count', 0) > 0:
            header_lines.append(f"**Replies:** {metadata['discussion_subentry_count']}")

        if metadata.get('html_url'):
            header_lines.append(f"**Canvas URL:** [{metadata['html_url']}]({metadata['html_url']})")

        header_lines.extend(["", "---", ""])

        # Footer
        footer_lines = [
            "",
            "---",
            f"*Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
        ]

        return "\n".join(header_lines) + content + "\n".join(footer_lines)

    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML content to plain text."""
        if not html_content:
            return ""

        import re

        # Replace common HTML elements with text equivalents
        text = html_content

        # Replace line breaks
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</h[1-6]>', '\n\n', text, flags=re.IGNORECASE)

        # Replace lists
        text = re.sub(r'<li[^>]*>', '• ', text, flags=re.IGNORECASE)
        text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Clean up whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)  # Multiple newlines to double
        text = re.sub(r'[ \t]+', ' ', text)      # Multiple spaces to single
        text = text.strip()

        return text

    async def _download_announcement_attachments(self, announcement: DiscussionTopic,
                                               metadata: Dict[str, Any], index: int):
        """Download files attached to announcements."""
        try:
            attachments = metadata.get('attachments', [])
            if not attachments:
                return

            announcement_title = self.sanitize_filename(announcement.title)

            # Create attachments subfolder
            attachments_folder = self.content_folder / f"announcement_{index:03d}_{announcement_title}_attachments"
            attachments_folder.mkdir(exist_ok=True)

            for attachment in attachments:
                try:
                    download_url = attachment.get('url', '')
                    if not download_url:
                        continue

                    # Generate local filename
                    original_filename = attachment.get('filename', f"attachment_{attachment.get('id', 'unknown')}")
                    sanitized_filename = self.sanitize_filename(original_filename)
                    file_path = attachments_folder / sanitized_filename

                    # Download the file
                    success = await self.download_file(download_url, file_path)

                    if success:
                        self.logger.debug(f"Downloaded announcement attachment",
                                        attachment_id=attachment.get('id'),
                                        filename=sanitized_filename,
                                        size=attachment.get('size', 0))
                    else:
                        self.logger.warning(f"Failed to download announcement attachment",
                                          attachment_id=attachment.get('id'))

                except Exception as e:
                    self.logger.warning(f"Could not download attachment",
                                      attachment_id=attachment.get('id', 'unknown'),
                                      exception=e)

        except Exception as e:
            self.logger.error(f"Failed to download announcement attachments", exception=e)

    async def _download_embedded_files(self, announcement: DiscussionTopic,
                                     metadata: Dict[str, Any], index: int):
        """Download files embedded in announcement content."""
        try:
            embedded_file_ids = metadata.get('content_stats', {}).get('embedded_file_ids', [])
            if not embedded_file_ids:
                return

            announcement_title = self.sanitize_filename(announcement.title)

            # Create embedded files subfolder
            embedded_folder = self.content_folder / f"announcement_{index:03d}_{announcement_title}_embedded"
            embedded_folder.mkdir(exist_ok=True)

            course = self.canvas_client.get_course(announcement.course_id)

            for file_id in embedded_file_ids:
                try:
                    # Get file information from Canvas
                    canvas_file = course.get_file(file_id)

                    # Generate local filename
                    original_filename = getattr(canvas_file, 'filename', f'file_{file_id}')
                    sanitized_filename = self.sanitize_filename(original_filename)
                    file_path = embedded_folder / sanitized_filename

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

    async def _download_announcement_replies(self, announcement: DiscussionTopic,
                                           metadata: Dict[str, Any], index: int):
        """Download replies/comments to announcements."""
        try:
            if metadata.get('discussion_subentry_count', 0) == 0:
                return

            announcement_title = self.sanitize_filename(announcement.title)

            # Get discussion entries (replies)
            entries = list(announcement.get_topic_entries())

            if not entries:
                return

            # Create replies file
            replies_filename = f"announcement_{index:03d}_{announcement_title}_replies.json"
            replies_path = self.content_folder / replies_filename

            # Process replies
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
                    'rating_sum': getattr(entry, 'rating_sum', 0)
                }
                replies_data.append(entry_data)

            # Save replies data
            replies_document = {
                'announcement_id': announcement.id,
                'announcement_title': announcement.title,
                'total_replies': len(replies_data),
                'replies': replies_data,
                'downloaded_at': datetime.now().isoformat()
            }

            async with aiofiles.open(replies_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(replies_document, indent=2, ensure_ascii=False))

            self.logger.debug(f"Saved announcement replies",
                            file_path=str(replies_path),
                            reply_count=len(replies_data))

        except Exception as e:
            self.logger.error(f"Failed to download announcement replies", exception=e)

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
ContentDownloaderFactory.register_downloader('announcements', AnnouncementsDownloader)