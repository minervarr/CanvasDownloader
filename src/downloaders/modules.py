"""
Modules Downloader Module

This module implements the modules downloader for Canvas courses. Canvas modules
provide a way for instructors to organize course content into logical units or
lessons. Modules can contain various types of content including assignments,
discussions, quizzes, pages, external tools, and files.

Canvas Modules include:
- Module structure and organization
- Module items (content within modules)
- Prerequisites and unlock conditions
- Completion requirements
- Module descriptions and headers
- Associated content and files
- Sequential learning paths

Features:
- Download complete module structure and hierarchy
- Process all module items and their content
- Handle different item types (assignments, pages, files, etc.)
- Preserve module organization and prerequisites
- Download associated files and external content
- Create navigation-friendly module indexes
- Handle completion requirements and conditions

Usage:
    # Initialize the downloader
    downloader = ModulesDownloader(canvas_client, progress_tracker)

    # Download all modules for a course
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

from canvasapi.module import Module
from canvasapi.exceptions import CanvasException

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class ModulesDownloader(BaseDownloader):
    """
    Canvas Modules Downloader

    This class handles downloading module content from Canvas courses.
    Modules represent organized learning paths with various content types
    including pages, assignments, discussions, and external resources.

    The downloader ensures that:
    - Complete module hierarchy is preserved
    - All module items are processed appropriately
    - Prerequisites and unlock conditions are documented
    - Associated content is downloaded where possible
    - Navigation structure is maintained
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the modules downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # Module processing settings
        self.download_module_content = True
        self.download_module_items = True
        self.download_associated_files = True
        self.create_module_index = True
        self.convert_to_markdown = MARKDOWNIFY_AVAILABLE

        # Item processing options
        self.process_external_urls = True
        self.download_wiki_pages = True
        self.create_navigation_files = True

        # Content organization
        self.preserve_module_structure = True
        self.create_item_shortcuts = True

        self.logger.info("Modules downloader initialized",
                         markdown_available=MARKDOWNIFY_AVAILABLE,
                         download_items=self.download_module_items)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "modules"

    def fetch_content_list(self, course) -> List[Module]:
        """
        Fetch all modules from the course.

        Args:
            course: Canvas course object

        Returns:
            List[Module]: List of module objects
        """
        try:
            self.logger.info(f"Fetching modules for course {course.id}")

            # Get modules with items included
            modules = list(course.get_modules(
                include=['items', 'content_details']
            ))

            self.logger.info(f"Found {len(modules)} modules",
                             course_id=course.id,
                             module_count=len(modules))

            return modules

        except CanvasException as e:
            self.logger.error(f"Failed to fetch modules", exception=e)
            raise DownloadError(f"Could not fetch modules: {e}")

    def extract_metadata(self, module: Module) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from a module.

        Args:
            module: Canvas module object

        Returns:
            Dict[str, Any]: Module metadata
        """
        try:
            # Basic module information
            metadata = {
                'id': module.id,
                'name': module.name,
                'course_id': getattr(module, 'course_id', None),
                'position': getattr(module, 'position', None),
                'workflow_state': getattr(module, 'workflow_state', ''),
                'item_count': getattr(module, 'item_count', 0),
                'items_url': getattr(module, 'items_url', ''),
                'published': getattr(module, 'published', False),
                'publish_final_grade': getattr(module, 'publish_final_grade', False),
                'requirement_count': getattr(module, 'requirement_count', 0),
                'completed_at': self._format_date(getattr(module, 'completed_at', None)),
                'state': getattr(module, 'state', ''),
            }

            # Date information
            metadata.update({
                'created_at': self._format_date(getattr(module, 'created_at', None)),
                'updated_at': self._format_date(getattr(module, 'updated_at', None)),
                'unlock_at': self._format_date(getattr(module, 'unlock_at', None)),
            })

            # Prerequisites
            prerequisites = getattr(module, 'prerequisites', [])
            if prerequisites:
                metadata['prerequisites'] = []
                for prereq in prerequisites:
                    prereq_data = {
                        'id': getattr(prereq, 'id', None),
                        'name': getattr(prereq, 'name', ''),
                        'type': getattr(prereq, 'type', ''),
                    }
                    metadata['prerequisites'].append(prereq_data)

            # Completion requirements
            completion_requirements = getattr(module, 'completion_requirements', [])
            if completion_requirements:
                metadata['completion_requirements'] = []
                for req in completion_requirements:
                    req_data = {
                        'id': getattr(req, 'id', None),
                        'type': getattr(req, 'type', ''),
                        'min_score': getattr(req, 'min_score', None),
                        'completed': getattr(req, 'completed', False)
                    }
                    metadata['completion_requirements'].append(req_data)

            # Module items (content within the module)
            items = getattr(module, 'items', [])
            if items:
                metadata['items'] = []
                total_items = len(items)

                for item in items:
                    item_metadata = self._extract_module_item_metadata(item)
                    metadata['items'].append(item_metadata)

                metadata['total_items'] = total_items

                # Analyze item types
                item_types = {}
                for item in metadata['items']:
                    item_type = item.get('type', 'unknown')
                    item_types[item_type] = item_types.get(item_type, 0) + 1

                metadata['item_types_summary'] = item_types

            # Canvas URLs
            html_url = getattr(module, 'html_url', '')
            if html_url:
                metadata['html_url'] = html_url
                metadata['canvas_url'] = html_url

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract module metadata",
                              module_id=getattr(module, 'id', 'unknown'),
                              exception=e)

            # Return minimal metadata on error
            return {
                'id': getattr(module, 'id', None),
                'name': getattr(module, 'name', 'Unknown Module'),
                'item_count': 0,
                'error': f"Metadata extraction failed: {e}"
            }

    def _extract_module_item_metadata(self, item) -> Dict[str, Any]:
        """
        Extract metadata from a module item.

        Args:
            item: Canvas module item object

        Returns:
            Dict[str, Any]: Module item metadata
        """
        try:
            item_metadata = {
                'id': getattr(item, 'id', None),
                'title': getattr(item, 'title', ''),
                'type': getattr(item, 'type', ''),
                'content_id': getattr(item, 'content_id', None),
                'html_url': getattr(item, 'html_url', ''),
                'url': getattr(item, 'url', ''),
                'external_url': getattr(item, 'external_url', ''),
                'position': getattr(item, 'position', None),
                'indent': getattr(item, 'indent', 0),
                'page_url': getattr(item, 'page_url', ''),
                'workflow_state': getattr(item, 'workflow_state', ''),
                'published': getattr(item, 'published', False),
                'module_id': getattr(item, 'module_id', None),
                'completion_requirement': getattr(item, 'completion_requirement', {}),
                'content_details': getattr(item, 'content_details', {})
            }

            # Process content details if available
            content_details = item_metadata.get('content_details', {})
            if content_details:
                item_metadata['content_details'] = {
                    'points_possible': content_details.get('points_possible'),
                    'due_at': self._format_date(content_details.get('due_at')),
                    'unlock_at': self._format_date(content_details.get('unlock_at')),
                    'lock_at': self._format_date(content_details.get('lock_at')),
                    'locked_for_user': content_details.get('locked_for_user', False),
                    'lock_explanation': content_details.get('lock_explanation', ''),
                    'lock_info': content_details.get('lock_info', {})
                }

            # Determine item category
            item_metadata['category'] = self._categorize_module_item(item_metadata)

            return item_metadata

        except Exception as e:
            self.logger.warning(f"Failed to extract module item metadata",
                                item_id=getattr(item, 'id', 'unknown'),
                                exception=e)

            return {
                'id': getattr(item, 'id', None),
                'title': getattr(item, 'title', 'Unknown Item'),
                'type': getattr(item, 'type', 'unknown'),
                'error': f"Item metadata extraction failed: {e}"
            }

    def _categorize_module_item(self, item_metadata: Dict[str, Any]) -> str:
        """
        Categorize a module item based on its type and properties.

        Args:
            item_metadata: Module item metadata

        Returns:
            str: Item category
        """
        item_type = item_metadata.get('type', '').lower()

        # Map Canvas item types to categories
        type_mapping = {
            'assignment': 'assignment',
            'quiz': 'quiz',
            'discussion': 'discussion',
            'wikipage': 'page',
            'file': 'file',
            'page': 'page',
            'externalurl': 'external_link',
            'externaltool': 'external_tool',
            'subheader': 'header',
            'context_module_sub_header': 'header'
        }

        return type_mapping.get(item_type, 'other')

    def get_download_info(self, module: Module) -> Optional[Dict[str, str]]:
        """
        Get download information for a module.

        Modules are processed rather than directly downloaded.

        Args:
            module: Canvas module object

        Returns:
            Optional[Dict[str, str]]: Download information or None
        """
        return None

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all modules for a course.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting modules download for course",
                             course_name=course_info.get('full_name', 'Unknown'),
                             course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if modules are enabled
            if not self.config.is_content_type_enabled('modules'):
                self.logger.info("Modules download is disabled")
                return self.stats

            # Fetch modules
            modules = self.fetch_content_list(course)

            if not modules:
                self.logger.info("No modules found in course")
                return self.stats

            self.stats['total_items'] = len(modules)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(modules))

            # Process each module
            items_metadata = []

            for index, module in enumerate(modules, 1):
                try:
                    # Extract metadata
                    metadata = self.extract_metadata(module)
                    metadata['item_number'] = index

                    # Process module content
                    await self._process_module(module, metadata, index, course)

                    items_metadata.append(metadata)

                    # Update progress
                    if self.progress_tracker:
                        self.progress_tracker.update_item_progress(index)

                    self.stats['downloaded_items'] += 1

                except Exception as e:
                    self.logger.error(f"Failed to process module",
                                      module_id=getattr(module, 'id', 'unknown'),
                                      module_name=getattr(module, 'name', 'unknown'),
                                      exception=e)
                    self.stats['failed_items'] += 1

            # Save metadata
            self.save_metadata(items_metadata)

            # Create course-wide module index
            if self.create_module_index:
                await self._create_course_module_index(items_metadata)

            self.logger.info(f"Modules download completed",
                             course_id=str(course.id),
                             **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Modules download failed", exception=e)
            raise DownloadError(f"Modules download failed: {e}")

    async def _process_module(self, module: Module, metadata: Dict[str, Any],
                              index: int, course):
        """
        Process a single module and create associated files.

        Args:
            module: Canvas module object
            metadata: Module metadata
            index: Module index number
            course: Canvas course object
        """
        try:
            module_name = self.sanitize_filename(module.name)

            # Create module-specific folder
            module_folder = self.content_folder / f"module_{index:03d}_{module_name}"
            module_folder.mkdir(exist_ok=True)

            # Save module overview
            await self._save_module_overview(module, metadata, module_folder, index)

            # Process module items
            if self.download_module_items and metadata.get('items'):
                await self._process_module_items(module, metadata, module_folder, course)

            # Create module navigation
            if self.create_navigation_files:
                await self._create_module_navigation(module, metadata, module_folder)

            # Save individual module metadata
            await self._save_module_metadata(metadata, module_folder)

        except Exception as e:
            self.logger.error(f"Failed to process module content",
                              module_id=module.id,
                              exception=e)
            raise

    async def _save_module_overview(self, module: Module, metadata: Dict[str, Any],
                                    module_folder: Path, index: int):
        """Save module overview and information."""
        try:
            module_name = self.sanitize_filename(module.name)

            # Save as HTML
            html_filename = f"module_{index:03d}_{module_name}_overview.html"
            html_path = module_folder / html_filename

            html_content = self._create_module_html(module.name, metadata)

            async with aiofiles.open(html_path, 'w', encoding='utf-8') as f:
                await f.write(html_content)

            self.logger.debug(f"Saved module overview as HTML",
                              file_path=str(html_path))

            # Save as text summary
            text_filename = f"module_{index:03d}_{module_name}_summary.txt"
            text_path = module_folder / text_filename

            text_content = self._create_module_text_summary(module.name, metadata)

            async with aiofiles.open(text_path, 'w', encoding='utf-8') as f:
                await f.write(text_content)

            self.logger.debug(f"Saved module summary as text",
                              file_path=str(text_path))

        except Exception as e:
            self.logger.error(f"Failed to save module overview", exception=e)

    def _create_module_html(self, module_name: str, metadata: Dict[str, Any]) -> str:
        """Create HTML overview of module content."""
        # Build module information
        info_html = ""

        if metadata.get('workflow_state'):
            status = "Published" if metadata.get('published') else "Unpublished"
            info_html += f"<p><strong>Status:</strong> {status}</p>"

        if metadata.get('item_count'):
            info_html += f"<p><strong>Items:</strong> {metadata['item_count']}</p>"

        if metadata.get('requirement_count'):
            info_html += f"<p><strong>Requirements:</strong> {metadata['requirement_count']}</p>"

        if metadata.get('unlock_at'):
            info_html += f"<p><strong>Unlocks:</strong> {metadata['unlock_at']}</p>"

        if metadata.get('canvas_url'):
            info_html += f"<p><strong>Canvas URL:</strong> <a href=\"{metadata['canvas_url']}\">{metadata['canvas_url']}</a></p>"

        # Build prerequisites section
        prereq_html = ""
        if metadata.get('prerequisites'):
            prereq_html = "<h3>Prerequisites</h3><ul>"
            for prereq in metadata['prerequisites']:
                prereq_html += f"<li>{prereq.get('name', 'Unknown')} ({prereq.get('type', 'unknown')})</li>"
            prereq_html += "</ul>"

        # Build completion requirements section
        completion_html = ""
        if metadata.get('completion_requirements'):
            completion_html = "<h3>Completion Requirements</h3><ul>"
            for req in metadata['completion_requirements']:
                req_type = req.get('type', 'unknown')
                min_score = req.get('min_score')
                score_text = f" (min score: {min_score})" if min_score else ""
                completion_html += f"<li>{req_type.replace('_', ' ').title()}{score_text}</li>"
            completion_html += "</ul>"

        # Build items section
        items_html = ""
        if metadata.get('items'):
            items_html = "<h3>Module Items</h3><ol>"
            for item in metadata['items']:
                item_title = item.get('title', 'Untitled')
                item_type = item.get('type', 'unknown')
                indent = item.get('indent', 0)
                indent_style = f"margin-left: {indent * 20}px;" if indent > 0 else ""

                items_html += f'<li style="{indent_style}"><strong>[{item_type}]</strong> {item_title}'

                if item.get('external_url'):
                    items_html += f' <a href="{item["external_url"]}">(External Link)</a>'
                elif item.get('html_url'):
                    items_html += f' <a href="{item["html_url"]}">(Canvas Link)</a>'

                items_html += "</li>"
            items_html += "</ol>"

        # Create complete HTML document
        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Module: {module_name}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .module-header {{
            background-color: #e3f2fd;
            padding: 15px;
            border-left: 4px solid #1976d2;
            margin-bottom: 20px;
            border-radius: 4px;
        }}
        .module-content {{
            margin-top: 20px;
        }}
        h1 {{
            color: #1976d2;
            border-bottom: 2px solid #ddd;
            padding-bottom: 10px;
        }}
        .module-badge {{
            background-color: #1976d2;
            color: white;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
        }}
        ol li {{
            margin-bottom: 8px;
        }}
        ul li {{
            margin-bottom: 4px;
        }}
    </style>
</head>
<body>
    <div class="module-badge">MODULE</div>
    <h1>{module_name}</h1>

    <div class="module-header">
        <h2>Module Information</h2>
        {info_html}
    </div>

    <div class="module-content">
        {prereq_html}
        {completion_html}
        {items_html}
    </div>

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    def _create_module_text_summary(self, module_name: str, metadata: Dict[str, Any]) -> str:
        """Create text summary of module content."""
        lines = [
            "=" * 60,
            f"MODULE: {module_name}",
            "=" * 60,
            ""
        ]

        # Basic information
        if metadata.get('published'):
            lines.append("Status: Published")
        else:
            lines.append("Status: Unpublished")

        if metadata.get('item_count'):
            lines.append(f"Items: {metadata['item_count']}")

        if metadata.get('requirement_count'):
            lines.append(f"Requirements: {metadata['requirement_count']}")

        if metadata.get('unlock_at'):
            lines.append(f"Unlocks: {metadata['unlock_at']}")

        if metadata.get('canvas_url'):
            lines.append(f"Canvas URL: {metadata['canvas_url']}")

        lines.append("")

        # Prerequisites
        if metadata.get('prerequisites'):
            lines.append("Prerequisites:")
            for prereq in metadata['prerequisites']:
                lines.append(f"  - {prereq.get('name', 'Unknown')} ({prereq.get('type', 'unknown')})")
            lines.append("")

        # Completion requirements
        if metadata.get('completion_requirements'):
            lines.append("Completion Requirements:")
            for req in metadata['completion_requirements']:
                req_type = req.get('type', 'unknown').replace('_', ' ').title()
                min_score = req.get('min_score')
                score_text = f" (min score: {min_score})" if min_score else ""
                lines.append(f"  - {req_type}{score_text}")
            lines.append("")

        # Module items
        if metadata.get('items'):
            lines.append("Module Items:")
            for i, item in enumerate(metadata['items'], 1):
                item_title = item.get('title', 'Untitled')
                item_type = item.get('type', 'unknown')
                indent = "  " + ("  " * item.get('indent', 0))
                lines.append(f"{indent}{i}. [{item_type}] {item_title}")

                if item.get('external_url'):
                    lines.append(f"{indent}   External: {item['external_url']}")
            lines.append("")

        # Item type summary
        if metadata.get('item_types_summary'):
            lines.append("Item Types Summary:")
            for item_type, count in metadata['item_types_summary'].items():
                lines.append(f"  - {item_type}: {count}")
            lines.append("")

        # Footer
        lines.extend([
            "-" * 60,
            f"Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])

        return "\n".join(lines)

    async def _process_module_items(self, module: Module, metadata: Dict[str, Any],
                                    module_folder: Path, course):
        """Process individual module items."""
        try:
            items = metadata.get('items', [])
            if not items:
                return

            # Create items subfolder
            items_folder = module_folder / 'items'
            items_folder.mkdir(exist_ok=True)

            # Process each item
            for item in items:
                try:
                    await self._process_module_item(item, items_folder, course)

                except Exception as e:
                    self.logger.warning(f"Failed to process module item",
                                        item_id=item.get('id', 'unknown'),
                                        item_title=item.get('title', 'unknown'),
                                        exception=e)

        except Exception as e:
            self.logger.error(f"Failed to process module items", exception=e)

    async def _process_module_item(self, item: Dict[str, Any], items_folder: Path, course):
        """Process a single module item."""
        try:
            item_type = item.get('type', 'unknown')
            item_title = self.sanitize_filename(item.get('title', 'untitled'))
            item_id = item.get('id', 'unknown')

            # Create item-specific file
            item_filename = f"{item_type}_{item_id}_{item_title}.json"
            item_path = items_folder / item_filename

            # Save item metadata
            async with aiofiles.open(item_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(item, indent=2, ensure_ascii=False, default=str))

            # Handle specific item types
            if item_type == 'ExternalUrl' and item.get('external_url'):
                await self._save_external_url_info(item, items_folder)

            elif item_type == 'WikiPage' and item.get('page_url'):
                await self._download_wiki_page(item, items_folder, course)

            elif item_type == 'File' and item.get('url'):
                await self._download_module_file(item, items_folder)

            # Create shortcuts for referenced content
            if self.create_item_shortcuts:
                await self._create_item_shortcut(item, items_folder)

        except Exception as e:
            self.logger.warning(f"Failed to process individual module item",
                                item_id=item.get('id', 'unknown'),
                                exception=e)

    async def _save_external_url_info(self, item: Dict[str, Any], items_folder: Path):
        """Save information about external URL items."""
        try:
            item_title = self.sanitize_filename(item.get('title', 'external_link'))
            url_filename = f"external_url_{item_title}.txt"
            url_path = items_folder / url_filename

            content = f"""External URL: {item.get('title', 'Untitled')}
URL: {item.get('external_url', '')}
Type: External Link
Canvas Item ID: {item.get('id', '')}

Description:
This item links to external content outside of Canvas.

Direct URL: {item.get('external_url', '')}

Downloaded on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

            async with aiofiles.open(url_path, 'w', encoding='utf-8') as f:
                await f.write(content)

        except Exception as e:
            self.logger.warning(f"Failed to save external URL info", exception=e)

    async def _download_wiki_page(self, item: Dict[str, Any], items_folder: Path, course):
        """Download wiki page content."""
        try:
            if not self.download_wiki_pages:
                return

            page_url = item.get('page_url', '')
            if not page_url:
                return

            # Get page content from Canvas
            try:
                page = course.get_page(page_url)
                page_content = getattr(page, 'body', '')
                page_title = getattr(page, 'title', item.get('title', 'Untitled Page'))

                if page_content:
                    # Save as HTML
                    safe_title = self.sanitize_filename(page_title)
                    html_filename = f"wiki_page_{safe_title}.html"
                    html_path = items_folder / html_filename

                    html_document = self._create_wiki_page_html(page_title, page_content, item)

                    async with aiofiles.open(html_path, 'w', encoding='utf-8') as f:
                        await f.write(html_document)

                    self.logger.debug(f"Downloaded wiki page",
                                      page_title=page_title,
                                      file_path=str(html_path))

            except Exception as e:
                self.logger.warning(f"Could not download wiki page content",
                                    page_url=page_url,
                                    exception=e)

        except Exception as e:
            self.logger.warning(f"Failed to download wiki page", exception=e)

    def _create_wiki_page_html(self, title: str, content: str, item: Dict[str, Any]) -> str:
        """Create HTML document for wiki page."""
        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wiki Page: {title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .page-header {{
            background-color: #f0f8f0;
            padding: 15px;
            border-left: 4px solid #4caf50;
            margin-bottom: 20px;
            border-radius: 4px;
        }}
        h1 {{
            color: #4caf50;
            border-bottom: 2px solid #ddd;
            padding-bottom: 10px;
        }}
        .page-badge {{
            background-color: #4caf50;
            color: white;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 0.8em;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="page-badge">WIKI PAGE</div>
    <h1>{title}</h1>

    <div class="page-header">
        <p><strong>Module Item:</strong> {item.get('title', 'Unknown')}</p>
        <p><strong>Page URL:</strong> {item.get('page_url', '')}</p>
        {f"<p><strong>Canvas URL:</strong> <a href=\"{item['html_url']}\">{item['html_url']}</a></p>" if item.get('html_url') else ""}
    </div>

    <div class="page-content">
        {content}
    </div>

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    async def _download_module_file(self, item: Dict[str, Any], items_folder: Path):
        """Download file associated with module item."""
        try:
            if not self.download_associated_files:
                return

            file_url = item.get('url', '')
            if not file_url:
                return

            # Generate filename
            item_title = item.get('title', 'unknown_file')
            safe_title = self.sanitize_filename(item_title)

            # Try to get file extension from URL or content details
            extension = ""
            if '.' in item_title:
                extension = Path(item_title).suffix

            filename = f"file_{safe_title}{extension}"
            file_path = items_folder / filename

            # Download the file
            success = await self.download_file(file_url, file_path)

            if success:
                self.logger.debug(f"Downloaded module file",
                                  filename=filename,
                                  file_path=str(file_path))

        except Exception as e:
            self.logger.warning(f"Failed to download module file", exception=e)

    async def _create_item_shortcut(self, item: Dict[str, Any], items_folder: Path):
        """Create shortcut/reference file for module items."""
        try:
            item_type = item.get('type', 'unknown')
            item_title = self.sanitize_filename(item.get('title', 'untitled'))

            shortcut_filename = f"link_{item_type}_{item_title}.txt"
            shortcut_path = items_folder / shortcut_filename

            content_lines = [
                f"Module Item: {item.get('title', 'Untitled')}",
                f"Type: {item_type}",
                f"Item ID: {item.get('id', 'Unknown')}",
                ""
            ]

            if item.get('html_url'):
                content_lines.append(f"Canvas URL: {item['html_url']}")

            if item.get('external_url'):
                content_lines.append(f"External URL: {item['external_url']}")

            if item.get('page_url'):
                content_lines.append(f"Page URL: {item['page_url']}")

            content_lines.extend([
                "",
                f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "",
                "Note: This is a reference file pointing to the actual content in Canvas."
            ])

            async with aiofiles.open(shortcut_path, 'w', encoding='utf-8') as f:
                await f.write('\n'.join(content_lines))

        except Exception as e:
            self.logger.warning(f"Failed to create item shortcut", exception=e)

    async def _create_module_navigation(self, module: Module, metadata: Dict[str, Any],
                                        module_folder: Path):
        """Create module navigation file."""
        try:
            nav_filename = "module_navigation.html"
            nav_path = module_folder / nav_filename

            # Create navigation HTML
            nav_html = self._create_navigation_html(module.name, metadata)

            async with aiofiles.open(nav_path, 'w', encoding='utf-8') as f:
                await f.write(nav_html)

            self.logger.debug(f"Created module navigation",
                              file_path=str(nav_path))

        except Exception as e:
            self.logger.error(f"Failed to create module navigation", exception=e)

    def _create_navigation_html(self, module_name: str, metadata: Dict[str, Any]) -> str:
        """Create HTML navigation for module."""
        items_html = ""

        if metadata.get('items'):
            items_html = "<ul class='module-items'>"

            for item in metadata['items']:
                item_title = item.get('title', 'Untitled')
                item_type = item.get('type', 'unknown')
                indent_class = f"indent-{min(item.get('indent', 0), 3)}"

                items_html += f'<li class="item {indent_class}">'
                items_html += f'<span class="item-type">[{item_type}]</span> '
                items_html += f'<span class="item-title">{item_title}</span>'

                # Add links if available
                if item.get('external_url'):
                    items_html += f' <a href="{item["external_url"]}" class="external-link" target="_blank">↗</a>'
                elif item.get('html_url'):
                    items_html += f' <a href="{item["html_url"]}" class="canvas-link" target="_blank">🔗</a>'

                items_html += '</li>'

            items_html += "</ul>"

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Navigation: {module_name}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .module-items {{
            list-style: none;
            padding: 0;
        }}
        .item {{
            padding: 8px 0;
            border-bottom: 1px solid #eee;
        }}
        .indent-1 {{ margin-left: 20px; }}
        .indent-2 {{ margin-left: 40px; }}
        .indent-3 {{ margin-left: 60px; }}
        .item-type {{
            background-color: #e3f2fd;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 0.8em;
            color: #1976d2;
        }}
        .item-title {{
            font-weight: 500;
        }}
        .external-link, .canvas-link {{
            margin-left: 8px;
            text-decoration: none;
            font-size: 0.9em;
        }}
        .external-link {{ color: #ff5722; }}
        .canvas-link {{ color: #1976d2; }}
    </style>
</head>
<body>
    <h1>📚 {module_name}</h1>
    <p>Module navigation and quick links</p>
    {items_html}

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </footer>
</body>
</html>"""

        return html_template

    async def _create_course_module_index(self, modules_metadata: List[Dict[str, Any]]):
        """Create course-wide module index."""
        try:
            index_filename = "course_modules_index.html"
            index_path = self.content_folder / index_filename

            index_html = self._create_course_index_html(modules_metadata)

            async with aiofiles.open(index_path, 'w', encoding='utf-8') as f:
                await f.write(index_html)

            self.logger.debug(f"Created course module index",
                              file_path=str(index_path))

        except Exception as e:
            self.logger.error(f"Failed to create course module index", exception=e)

    def _create_course_index_html(self, modules_metadata: List[Dict[str, Any]]) -> str:
        """Create HTML index of all course modules."""
        modules_html = ""

        for module in modules_metadata:
            module_name = module.get('name', 'Unnamed Module')
            item_count = module.get('item_count', 0)
            status = "Published" if module.get('published') else "Unpublished"

            modules_html += f"""
            <div class="module-card">
                <h3>{module_name}</h3>
                <p><strong>Items:</strong> {item_count} | <strong>Status:</strong> {status}</p>

                {f"<p><strong>Prerequisites:</strong> {len(module.get('prerequisites', []))}</p>" if module.get('prerequisites') else ""}
                {f"<p><strong>Requirements:</strong> {module.get('requirement_count', 0)}</p>" if module.get('requirement_count') else ""}

                <div class="module-links">
                    <a href="module_{module.get('item_number', 1):03d}_{self.sanitize_filename(module_name)}/module_{module.get('item_number', 1):03d}_{self.sanitize_filename(module_name)}_overview.html">
                        📖 View Module
                    </a>
                    {f'<a href="{module["canvas_url"]}" target="_blank">🔗 Open in Canvas</a>' if module.get('canvas_url') else ''}
                </div>
            </div>
            """

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Course Modules Index</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .module-card {{
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
            background-color: #f9f9f9;
        }}
        .module-card h3 {{
            margin-top: 0;
            color: #1976d2;
        }}
        .module-links {{
            margin-top: 10px;
        }}
        .module-links a {{
            margin-right: 15px;
            text-decoration: none;
            color: #1976d2;
            font-weight: 500;
        }}
        .module-links a:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
    <h1>📚 Course Modules Index</h1>
    <p>Overview of all course modules and their content.</p>

    <div class="modules-container">
        {modules_html}
    </div>

    <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em;">
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Total Modules: {len(modules_metadata)}</p>
    </footer>
</body>
</html>"""

        return html_template

    async def _save_module_metadata(self, metadata: Dict[str, Any], module_folder: Path):
        """Save individual module metadata."""
        try:
            metadata_path = module_folder / 'module_metadata.json'

            async with aiofiles.open(metadata_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False, default=str))

            self.logger.debug(f"Saved module metadata",
                              file_path=str(metadata_path))

        except Exception as e:
            self.logger.error(f"Failed to save module metadata", exception=e)

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

ContentDownloaderFactory.register_downloader('modules', ModulesDownloader)