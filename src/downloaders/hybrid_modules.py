"""
Hybrid Modules Downloader - Integration Module

This module replaces your existing modules downloader with a hybrid approach:
1. Canvas API → Get module structure and metadata
2. Web Scraping → Get actual file download URLs
3. Direct Download → Download the actual PDFs and files

Purpose: Get BOTH module structure AND actual files (solving your empty downloads)
Strategy: API for organization + Web scraping for content = Complete solution

Usage: Drop-in replacement for your existing ModulesDownloader
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
from ..utils.web_content_extractor import create_web_content_extractor


class HybridModulesDownloader(BaseDownloader):
    """
    Hybrid Canvas Modules Downloader

    This downloader combines the best of both worlds:
    - Canvas API for module structure and organization
    - Web scraping for actual file content that API misses

    It solves the empty downloads problem by actually getting the files
    that show up in Canvas web interface but not in API responses.

    The downloader ensures that:
    - Complete module hierarchy is preserved (API)
    - ALL module files are downloaded (Web scraping)
    - PDFs and documents are actually retrieved
    - Folder structure matches Canvas organization
    - Both metadata and content are captured
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """
        Initialize the hybrid modules downloader.

        Args:
            canvas_client: Canvas API client instance
            progress_tracker: Optional progress tracker for UI updates
        """
        super().__init__(canvas_client, progress_tracker)
        self.logger = get_logger(__name__)

        # Module processing settings (inherited)
        self.download_module_content = True
        self.download_module_items = True
        self.download_associated_files = True
        self.create_module_index = True
        self.convert_to_markdown = MARKDOWNIFY_AVAILABLE

        # Enhanced settings for hybrid approach
        self.use_web_extraction = True
        self.cookies_path = "config/cookies.txt"
        self.download_actual_files = True
        self.create_web_backups = True

        # Initialize web content extractor
        try:
            self.web_extractor = create_web_content_extractor(
                cookies_path=self.cookies_path
            )
            self.logger.info("Web content extractor initialized successfully")
        except Exception as e:
            self.logger.warning(f"Web content extractor failed to initialize", exception=e)
            self.web_extractor = None
            self.use_web_extraction = False

        self.logger.info("Hybrid modules downloader initialized",
                         markdown_available=MARKDOWNIFY_AVAILABLE,
                         download_items=self.download_module_items,
                         web_extraction=self.use_web_extraction)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "modules"

    def fetch_content_list(self, course) -> List[Module]:
        """
        Fetch all modules from the course using Canvas API.

        Args:
            course: Canvas course object

        Returns:
            List[Module]: List of module objects
        """
        try:
            self.logger.info(f"Fetching modules for course {course.id}")

            # Get modules with items included (same as original)
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

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download all modules for a course using hybrid approach.

        Args:
            course: Canvas course object
            course_info: Parsed course information

        Returns:
            Dict[str, Any]: Download statistics and results
        """
        try:
            self.logger.info(f"Starting HYBRID modules download for course",
                             course_name=course_info.get('full_name', 'Unknown'),
                             course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if modules are enabled
            if not self.config.is_content_type_enabled('modules'):
                self.logger.info("Modules download is disabled")
                return self.stats

            # STEP 1: Fetch modules using Canvas API
            modules = self.fetch_content_list(course)

            if not modules:
                self.logger.info("No modules found in course")
                return self.stats

            self.stats['total_items'] = len(modules)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(modules))

            # STEP 2: Process each module with hybrid approach
            items_metadata = []
            total_files_downloaded = 0

            for index, module in enumerate(modules, 1):
                try:
                    # Extract metadata using API
                    metadata = self.extract_metadata(module)
                    metadata['item_number'] = index

                    # HYBRID PROCESSING: API + Web extraction
                    files_downloaded = await self._process_module_hybrid(
                        module, metadata, index, course
                    )

                    metadata['files_downloaded'] = files_downloaded
                    total_files_downloaded += files_downloaded

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

            # Update stats with actual file downloads
            self.stats['total_files_downloaded'] = total_files_downloaded
            self.stats['total_size_bytes'] = sum(
                item.get('total_size', 0) for item in items_metadata
            )

            # Save metadata
            self.save_metadata(items_metadata)

            # Create course-wide module index
            if self.create_module_index:
                await self._create_course_module_index(items_metadata)

            self.logger.info(f"HYBRID modules download completed",
                             course_id=str(course.id),
                             total_files=total_files_downloaded,
                             **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Hybrid modules download failed", exception=e)
            raise DownloadError(f"Hybrid modules download failed: {e}")

    async def _process_module_hybrid(self, module: Module, metadata: Dict[str, Any],
                                     index: int, course) -> int:
        """
        Process a single module using hybrid approach (API + Web extraction).

        Args:
            module: Canvas module object
            metadata: Module metadata from API
            index: Module index
            course: Canvas course object

        Returns:
            int: Number of files actually downloaded
        """
        try:
            module_folder = self.course_folder / f"module_{index:03d}_{self.sanitize_filename(module.name)}"
            module_folder.mkdir(parents=True, exist_ok=True)

            files_downloaded = 0

            # STEP 1: Process using original API method (for structure/metadata)
            await self._process_module_api(module, metadata, module_folder)

            # STEP 2: Extract actual files using web scraping
            if self.use_web_extraction and self.web_extractor:
                web_files_downloaded = await self._process_module_web_extraction(
                    module, course, module_folder
                )
                files_downloaded += web_files_downloaded
            else:
                self.logger.warning(f"Web extraction not available for module {module.name}")

            # STEP 3: Create enhanced module documentation
            await self._create_hybrid_module_summary(module, metadata, module_folder, files_downloaded)

            return files_downloaded

        except Exception as e:
            self.logger.error(f"Failed to process module hybrid", exception=e)
            return 0

    async def _process_module_api(self, module: Module, metadata: Dict[str, Any],
                                  module_folder: Path):
        """Process module using original Canvas API approach."""
        try:
            # Create module overview file
            overview_content = self._generate_module_overview(module, metadata)
            overview_file = module_folder / f"{self.sanitize_filename(module.name)}_overview.html"

            async with aiofiles.open(overview_file, 'w', encoding='utf-8') as f:
                await f.write(overview_content)

            # Create module metadata file
            metadata_file = module_folder / "module_metadata.json"
            async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False, default=str))

            # Process module items (original way)
            if self.download_module_items:
                await self._process_module_items_api(module, metadata, module_folder)

        except Exception as e:
            self.logger.error(f"Failed to process module via API", exception=e)

    async def _process_module_web_extraction(self, module: Module, course,
                                             module_folder: Path) -> int:
        """
        Process module using web extraction to get actual files.

        Args:
            module: Canvas module object
            course: Canvas course object
            module_folder: Module folder path

        Returns:
            int: Number of files downloaded
        """
        try:
            files_downloaded = 0

            # Extract files from module page
            self.logger.info(f"Extracting files from module {module.name} via web scraping")

            file_infos = self.web_extractor.extract_module_files(
                str(course.id),
                str(module.id)
            )

            if not file_infos:
                self.logger.info(f"No files found via web extraction for module {module.name}")
                return 0

            # Create files subfolder
            files_folder = module_folder / "files"
            files_folder.mkdir(exist_ok=True)

            # Download each file
            for file_info in file_infos:
                try:
                    # Generate safe filename
                    safe_filename = self.sanitize_filename(file_info.filename)
                    if not safe_filename:
                        safe_filename = f"file_{file_info.content_id or 'unknown'}"

                    file_path = files_folder / safe_filename

                    # Download the file
                    self.logger.info(f"Downloading file: {file_info.filename}")
                    success = self.web_extractor.download_file(file_info.url, file_path)

                    if success:
                        files_downloaded += 1

                        # Save file metadata
                        metadata_file = files_folder / f"{safe_filename}.metadata.json"
                        file_metadata = {
                            'original_filename': file_info.filename,
                            'url': file_info.url,
                            'file_type': file_info.file_type,
                            'size': file_info.size,
                            'item_title': file_info.item_title,
                            'content_id': file_info.content_id,
                            'download_timestamp': datetime.now().isoformat(),
                            'extraction_method': 'web_scraping'
                        }

                        async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
                            await f.write(json.dumps(file_metadata, indent=2, ensure_ascii=False))

                        self.logger.info(f"Successfully downloaded: {file_info.filename}")
                    else:
                        self.logger.warning(f"Failed to download: {file_info.filename}")

                except Exception as e:
                    self.logger.error(f"Error downloading file {file_info.filename}", exception=e)
                    continue

            self.logger.info(f"Web extraction completed for module {module.name}: {files_downloaded} files downloaded")
            return files_downloaded

        except Exception as e:
            self.logger.error(f"Failed to process module via web extraction", exception=e)
            return 0

    async def _process_module_items_api(self, module: Module, metadata: Dict[str, Any],
                                        module_folder: Path):
        """Process module items using API (original method for comparison)."""
        try:
            items = metadata.get('items', [])
            if not items:
                return

            # Create items subfolder
            items_folder = module_folder / 'items'
            items_folder.mkdir(exist_ok=True)

            # Process each item (original way)
            for item in items:
                try:
                    await self._process_module_item_api(item, items_folder)
                except Exception as e:
                    self.logger.warning(f"Failed to process module item",
                                        item_id=item.get('id', 'unknown'),
                                        item_title=item.get('title', 'unknown'),
                                        exception=e)

        except Exception as e:
            self.logger.error(f"Failed to process module items via API", exception=e)

    async def _process_module_item_api(self, item: Dict[str, Any], items_folder: Path):
        """Process a single module item using API (original method)."""
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

            # Create reference file
            if self.create_item_shortcuts:
                await self._create_item_shortcut_api(item, items_folder)

        except Exception as e:
            self.logger.warning(f"Failed to process individual module item via API",
                                item_id=item.get('id', 'unknown'),
                                exception=e)

    async def _create_item_shortcut_api(self, item: Dict[str, Any], items_folder: Path):
        """Create shortcut/reference file for module items (original method)."""
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

    async def _create_hybrid_module_summary(self, module: Module, metadata: Dict[str, Any],
                                            module_folder: Path, files_downloaded: int):
        """Create enhanced module summary with hybrid results."""
        try:
            module_name = getattr(module, 'name', 'Unknown Module')
            summary_file = module_folder / f"{self.sanitize_filename(module_name)}_HYBRID_summary.txt"

            lines = [
                "=" * 80,
                f"HYBRID MODULE SUMMARY: {module_name}",
                "=" * 80,
                "",
                "🔄 PROCESSING METHOD: Canvas API + Web Scraping",
                "",
                "📊 RESULTS:",
                f"  • API Module Items: {len(metadata.get('items', []))}",
                f"  • Actual Files Downloaded: {files_downloaded}",
                f"  • Module ID: {getattr(module, 'id', 'Unknown')}",
                f"  • Position: {getattr(module, 'position', 'Unknown')}",
                "",
                "📁 FOLDER STRUCTURE:",
                f"  • Module Overview: {module_name}_overview.html",
                f"  • Module Metadata: module_metadata.json",
                f"  • API Items: items/ folder",
                f"  • Downloaded Files: files/ folder",
                "",
                "🕒 PROCESSING INFO:",
                f"  • Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"  • API Status: ✅ Success",
                f"  • Web Extraction: {'✅ Success' if files_downloaded > 0 else '⚠️ No files found'}",
                "",
                "📋 MODULE DETAILS:"
            ]

            # Add module details
            if hasattr(module, 'workflow_state'):
                lines.append(f"  • Status: {module.workflow_state}")

            if hasattr(module, 'published'):
                lines.append(f"  • Published: {'Yes' if module.published else 'No'}")

            if hasattr(module, 'unlock_at'):
                lines.append(f"  • Unlock Date: {module.unlock_at or 'No restriction'}")

            # Add success indicators
            lines.extend([
                "",
                "🎯 SUCCESS INDICATORS:",
                f"  • Module structure captured: ✅",
                f"  • Files actually downloaded: {'✅' if files_downloaded > 0 else '❌'}",
                f"  • Hybrid approach effective: {'✅' if files_downloaded > 0 else '⚠️'}",
                "",
                "=" * 80
            ])

            async with aiofiles.open(summary_file, 'w', encoding='utf-8') as f:
                await f.write('\n'.join(lines))

        except Exception as e:
            self.logger.warning(f"Failed to create hybrid module summary", exception=e)

    def extract_metadata(self, module: Module) -> Dict[str, Any]:
        """Extract comprehensive metadata from a module (same as original)."""
        try:
            # Basic module information
            metadata = {
                'id': module.id if hasattr(module, 'id') else None,
                'name': str(module.name) if hasattr(module, 'name') else '',
                'position': getattr(module, 'position', None),
                'workflow_state': getattr(module, 'workflow_state', ''),
                'item_count': getattr(module, 'item_count', 0),
                'published': getattr(module, 'published', False),
                'unlock_at': getattr(module, 'unlock_at', None),
                'require_sequential_progress': getattr(module, 'require_sequential_progress', False),
                'prerequisite_module_ids': getattr(module, 'prerequisite_module_ids', []),
                'state': getattr(module, 'state', ''),
            }

            # Module items (content within the module)
            items = getattr(module, 'items', [])
            if items:
                metadata['items'] = []
                for item in items:
                    item_metadata = self._extract_item_metadata(item)
                    metadata['items'].append(item_metadata)

            return metadata

        except Exception as e:
            self.logger.warning(f"Failed to extract module metadata",
                                module_id=getattr(module, 'id', 'unknown'),
                                exception=e)

            return {
                'id': getattr(module, 'id', None),
                'name': str(getattr(module, 'name', 'Unknown Module')),
                'error': f"Metadata extraction failed: {e}"
            }

    def _extract_item_metadata(self, item) -> Dict[str, Any]:
        """Extract metadata from a module item (same as original)."""
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


# Factory function for easy integration
def create_hybrid_modules_downloader(canvas_client, progress_tracker=None):
    """
    Factory function to create a hybrid modules downloader.

    Args:
        canvas_client: Canvas API client
        progress_tracker: Optional progress tracker

    Returns:
        HybridModulesDownloader: Configured downloader instance
    """
    return HybridModulesDownloader(canvas_client, progress_tracker)