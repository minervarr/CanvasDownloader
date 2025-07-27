"""
Direct ModulesDownloader Replacement - Drop-in Fix

This directly replaces your existing src/downloaders/modules.py file
to add web scraping capabilities for actual file downloads.

INSTRUCTIONS:
1. Install dependencies: pip install beautifulsoup4
2. Place cookies at: config/cookies.txt
3. Replace your existing src/downloaders/modules.py with this code
4. Run your downloader - it will now download actual files!

NO OTHER CHANGES NEEDED - this is a drop-in replacement.
"""

import asyncio
import json
import time
import requests
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from urllib.parse import urljoin, urlparse, unquote

import aiofiles
from canvasapi import course

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

from canvasapi.module import Module
from canvasapi.exceptions import CanvasException

from .base import BaseDownloader, DownloadError
from ..utils.logger import get_logger


class ModulesDownloader(BaseDownloader):
    """
    ENHANCED Canvas Modules Downloader with Web Scraping

    This enhanced version combines Canvas API with web scraping to actually
    download the files that show in Canvas but are missing from API responses.

    NEW FEATURES:
    - Web scraping to extract actual file URLs
    - Direct file downloads (PDFs, documents, etc.)
    - Browser cookie authentication
    - Hybrid approach: API for structure + Web for content

    The downloader ensures that:
    - Complete module hierarchy is preserved (API)
    - ALL module files are actually downloaded (Web scraping)
    - PDFs and documents are retrieved
    - Both metadata and real content are captured
    """

    def __init__(self, canvas_client, progress_tracker=None):
        """Initialize the enhanced modules downloader."""
        super().__init__(canvas_client, progress_tracker)
        self.canvas_client = canvas_client  # Store Canvas client for URL resolution
        self.logger = get_logger(__name__)

        # Original settings
        self.download_module_content = True
        self.download_module_items = True
        self.download_associated_files = True
        self.create_module_index = True
        self.convert_to_markdown = MARKDOWNIFY_AVAILABLE

        # NEW: Web scraping settings
        self.use_web_extraction = BEAUTIFULSOUP_AVAILABLE
        self.cookies_path = Path("config/cookies.txt")
        self.download_actual_files = True

        # Initialize web session for scraping
        self.web_session = requests.Session()
        self.base_url = None
        self._setup_web_session()

        self.logger.info("ENHANCED Modules downloader initialized (WITH URL RESOLUTION)",
                       markdown_available=MARKDOWNIFY_AVAILABLE,
                       download_items=self.download_module_items,
                       web_extraction=self.use_web_extraction,
                       cookies_available=hasattr(self, 'session') and bool(self.session.cookies))

    def _setup_web_session(self):
        """Set up web session with cookies for scraping."""
        try:
            # Set headers
            self.web_session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'DNT': '1',
                'Connection': 'keep-alive',
            })

            # Load cookies if available
            if self.cookies_path.exists():
                self._load_cookies()
                self.logger.info("Web session initialized with cookies")
            else:
                self.logger.warning(f"No cookies file found at {self.cookies_path}")
                self.use_web_extraction = False

        except Exception as e:
            self.logger.warning("Failed to setup web session", exception=e)
            self.use_web_extraction = False

    def _load_cookies(self):
        """Load browser cookies from file."""
        try:
            with open(self.cookies_path, 'r', encoding='utf-8') as f:
                cookie_count = 0
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split('\t')
                        if len(parts) >= 7:
                            domain = parts[0]
                            name = parts[5]
                            value = parts[6]

                            self.web_session.cookies.set(name, value, domain=domain)
                            cookie_count += 1

                            # Auto-detect Canvas URL from cookies
                            if not self.base_url and 'instructure.com' in domain:
                                if domain.startswith('.'):
                                    domain = domain[1:]
                                self.base_url = f"https://{domain}"

                self.logger.info(f"Loaded {cookie_count} cookies, detected URL: {self.base_url}")

        except Exception as e:
            self.logger.error("Failed to load cookies", exception=e)

    def get_content_type_name(self) -> str:
        """Get the content type name for this downloader."""
        return "modules"

    def fetch_content_list(self, course) -> List[Module]:
        """Fetch all modules from the course using Canvas API."""
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

    async def download_course_content(self, course, course_info: Dict[str, Any]) -> Dict[str, Any]:
        """Download all modules using ENHANCED hybrid approach."""
        try:
            self.logger.info(f"Starting ENHANCED modules download for course",
                           course_name=course_info.get('full_name', 'Unknown'),
                           course_id=str(course.id))

            # Set up course folder
            course_folder = self.setup_course_folder(course_info)

            # Check if modules are enabled
            if not self.config.is_content_type_enabled('modules'):
                self.logger.info("Modules download is disabled")
                return self.stats

            # Fetch modules using Canvas API
            modules = self.fetch_content_list(course)

            if not modules:
                self.logger.info("No modules found in course")
                return self.stats

            self.stats['total_items'] = len(modules)

            # Update progress tracker
            if self.progress_tracker:
                self.progress_tracker.set_total_items(len(modules))

            # Process each module with ENHANCED approach
            items_metadata = []
            total_files_downloaded = 0

            for index, module in enumerate(modules, 1):
                try:
                    # Extract metadata using API
                    metadata = self.extract_metadata(module)
                    metadata['item_number'] = index

                    # ENHANCED PROCESSING: API + Web scraping
                    files_downloaded = await self._process_module_enhanced(
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

            # Save metadata
            self.save_metadata(items_metadata)

            # Create course-wide module index
            if self.create_module_index:
                await self._create_course_module_index(items_metadata)

            self.logger.info(f"ENHANCED modules download completed",
                           course_id=str(course.id),
                           total_files=total_files_downloaded,
                           **self.stats)

            return self.stats

        except Exception as e:
            self.logger.error(f"Enhanced modules download failed", exception=e)
            raise DownloadError(f"Enhanced modules download failed: {e}")

    def _resolve_canvas_file_url(self, url: str, course_id: str) -> str:
        """
        CORE FIX: Visit module item page and extract actual download URL.
        """
        try:
            # Check if this is a Canvas module item URL
            module_item_match = re.search(r'/courses/(\d+)/modules/items/(\d+)', url)
            if not module_item_match:
                return url

            self.logger.info(f"🔍 Visiting module item page: {url}")

            # Visit the module item page using the correct session
            response = self.web_session.get(url, timeout=30)
            response.raise_for_status()

            self.logger.info(f"📄 Response size: {len(response.content)} bytes")

            # Parse the HTML to find the download link
            if BEAUTIFULSOUP_AVAILABLE:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Look for the exact pattern: <a download="true" href="/courses/17926/files/3636002/download?download_frd=1">
                download_links = soup.find_all('a', {'download': 'true',
                                                     'href': re.compile(r'/courses/\d+/files/\d+/download')})
                self.logger.info(f"🔗 Found {len(download_links)} download links with download='true'")

                if download_links:
                    download_href = download_links[0].get('href')
                    if download_href.startswith('/'):
                        download_url = urljoin(self.base_url, download_href)
                    else:
                        download_url = download_href

                    self.logger.info(f"✅ FOUND REAL DOWNLOAD URL: {download_url}")
                    return download_url
                else:
                    # Fallback: look for any /files/*/download links
                    fallback_links = soup.find_all('a', href=re.compile(r'/files/\d+/download'))
                    self.logger.info(f"🔗 Fallback: Found {len(fallback_links)} /files/*/download links")
                    if fallback_links:
                        download_href = fallback_links[0].get('href')
                        download_url = urljoin(self.base_url, download_href) if download_href.startswith(
                            '/') else download_href
                        self.logger.info(f"✅ FOUND FALLBACK DOWNLOAD URL: {download_url}")
                        return download_url

            # If no download link found, return original URL
            self.logger.info(f"❌ No download link found in module item page")
            return url

        except Exception as e:
            self.logger.error(f"Error resolving Canvas file URL: {e}")
            return url

    async def _process_module_enhanced(self, module: Module, metadata: Dict[str, Any],
                                     index: int, course) -> int:
        """Process a single module using ENHANCED approach."""
        try:
            module_name = self.sanitize_filename(getattr(module, 'name', f'module_{index}'))
            module_folder = self.course_folder / f"module_{index:03d}_{module_name}"
            module_folder.mkdir(parents=True, exist_ok=True)

            files_downloaded = 0

            # STEP 1: Create module overview and metadata (original functionality)
            await self._create_module_overview(module, metadata, module_folder)

            # STEP 2: Process module items using API (original way)
            await self._process_module_items_api(module, metadata, module_folder)

            # STEP 3: NEW - Extract and download actual files using web scraping
            if self.use_web_extraction and self.base_url:
                web_files = await self._extract_files_from_web(module, course, module_folder)
                files_downloaded += web_files

                # Create success indicator
                if web_files > 0:
                    success_file = module_folder / "FILES_DOWNLOADED_SUCCESS.txt"
                    success_content = f"""✅ ENHANCED MODULES DOWNLOADER SUCCESS!

Module: {module.name}
Files Downloaded: {web_files}
Method: Web Scraping + Canvas API
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

This module now contains actual files, not just empty placeholders!
Check the 'files/' subfolder for downloaded content.
"""
                    async with aiofiles.open(success_file, 'w', encoding='utf-8') as f:
                        await f.write(success_content)
            else:
                # Create explanation file if web extraction not available
                explanation_file = module_folder / "NO_WEB_EXTRACTION.txt"
                explanation_content = f"""ℹ️  Web Extraction Not Available

Module: {module.name}
Reason: {'Missing BeautifulSoup' if not BEAUTIFULSOUP_AVAILABLE else 'No cookies or Canvas URL'}
Fallback: API-only processing

To enable file downloads:
1. Install: pip install beautifulsoup4
2. Export browser cookies to: config/cookies.txt
3. Re-run the downloader
"""
                async with aiofiles.open(explanation_file, 'w', encoding='utf-8') as f:
                    await f.write(explanation_content)

            return files_downloaded

        except Exception as e:
            self.logger.error(f"Failed to process module enhanced", exception=e)
            return 0

    async def _extract_files_from_web(self, module: Module, course, module_folder: Path) -> int:
        """ENHANCED: Extract and download actual files using web scraping with debugging."""
        if not self.use_web_extraction:
            return 0

        try:
            files_downloaded = 0
            module_url = f"{self.base_url}/courses/{course.id}/modules/{module.id}"

            self.logger.info(f"DEBUGGING: Extracting files from module web page: {module.name}")
            self.logger.info(f"DEBUGGING: Module URL: {module_url}")

            # Fetch module page
            response = self.web_session.get(module_url, timeout=30)
            response.raise_for_status()

            self.logger.info(f"DEBUGGING: HTTP Status: {response.status_code}")
            self.logger.info(f"DEBUGGING: Content-Type: {response.headers.get('content-type', 'unknown')}")
            self.logger.info(f"DEBUGGING: Response size: {len(response.content)} bytes")

            # DEBUGGING: Save actual HTML for inspection
            debug_folder = module_folder / "debug"
            debug_folder.mkdir(exist_ok=True)
            debug_html_file = debug_folder / f"module_{module.id}_page.html"

            async with aiofiles.open(debug_html_file, 'w', encoding='utf-8') as f:
                await f.write(response.text)

            self.logger.info(f"DEBUGGING: Saved module HTML to {debug_html_file}")

            soup = BeautifulSoup(response.content, 'html.parser')

            # Create files folder
            files_folder = module_folder / "files"
            files_folder.mkdir(exist_ok=True)

            # ENHANCED: Extract file links from the page with debugging
            file_links = self._find_file_links_enhanced(soup, module)

            self.logger.info(f"DEBUGGING: Found {len(file_links)} potential file links in module {module.name}")

            # DEBUGGING: Log what we found
            if file_links:
                for i, link in enumerate(file_links[:5]):  # Show first 5
                    self.logger.info(f"DEBUGGING: Link {i + 1}: {link.get('url', 'No URL')[:100]}...")
                    self.logger.info(f"DEBUGGING: Filename: {link.get('filename', 'No filename')}")
                    self.logger.info(f"DEBUGGING: Type: {link.get('type', 'No type')}")
            else:
                self.logger.warning(f"DEBUGGING: No file links found - will save diagnostic info")
                await self._save_debug_info(soup, debug_folder, module)

            # Download each file
            for file_info in file_links:
                try:
                    success = await self._download_file_from_web(file_info, files_folder)
                    if success:
                        files_downloaded += 1

                except Exception as e:
                    self.logger.warning(f"Failed to download file {file_info.get('filename', 'unknown')}",
                                        exception=e)
                    continue

            # Add small delay to be nice to the server
            if file_links:
                await asyncio.sleep(1)

            self.logger.info(f"FINAL RESULT: Downloaded {files_downloaded} files from module {module.name}")
            return files_downloaded

        except Exception as e:
            self.logger.error(f"Failed to extract files from web for module {module.name}", exception=e)
            return 0

    def _find_file_links_enhanced(self, soup: BeautifulSoup, module) -> List[Dict[str, Any]]:
        """ENHANCED: Find file download links with better detection and debugging."""
        file_links = []

        try:
            self.logger.info(f"DEBUGGING: Starting enhanced file link detection for module {module.name}")

            # METHOD 1: Look for ANY links with file-like patterns (very broad)
            all_links = soup.find_all('a', href=True)
            self.logger.info(f"DEBUGGING: Found {len(all_links)} total links on page")

            file_extensions = ['.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx', '.txt', '.zip']

            for link in all_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True)

                # Check if this looks like a file
                is_file_link = any([
                    any(ext in href.lower() for ext in file_extensions),
                    any(ext in link_text.lower() for ext in file_extensions),
                    '/files/' in href,
                    'download' in href.lower(),
                    'attachment' in href.lower(),
                ])

                if is_file_link:
                    # Make URL absolute
                    if href.startswith('/'):
                        href = urljoin(self.base_url, href)

                    # Extract filename
                    filename = self._extract_filename_from_link_enhanced(link, href)

                    if filename:
                        file_info = {
                            'url': href,
                            'filename': filename,
                            'type': 'detected_file',
                            'title': link_text[:100],
                            'method': 'broad_detection',
                            'course_id': str(getattr(module, 'course_id', '')) if hasattr(module, 'course_id') else ''
                        }
                        file_links.append(file_info)
                        self.logger.info(f"DEBUGGING: Found file link: {filename} -> {href[:100]}...")

            # METHOD 2: Look for Canvas-specific patterns
            canvas_patterns = [
                # Canvas file attachment patterns
                {'selector': 'a[href*="/courses/"][href*="/files/"]', 'method': 'canvas_files'},
                {'selector': 'a[data-api-endpoint*="files"]', 'method': 'api_files'},
                {'selector': '.attachment a', 'method': 'attachment_class'},
                {'selector': '.file a', 'method': 'file_class'},
                {'selector': 'a[title*=".pdf"]', 'method': 'pdf_title'},
                {'selector': 'a[title*=".doc"]', 'method': 'doc_title'},
                # Module item patterns
                {'selector': '.context_module_item a', 'method': 'module_item'},
                {'selector': '.ig-row a', 'method': 'ig_row'},
                {'selector': '[data-module-item-id] a', 'method': 'module_item_id'},
            ]

            for pattern in canvas_patterns:
                elements = soup.select(pattern['selector'])
                self.logger.info(f"DEBUGGING: {pattern['method']} found {len(elements)} elements")

                for element in elements:
                    href = element.get('href', '')
                    if not href:
                        continue

                    # Make URL absolute
                    if href.startswith('/'):
                        href = urljoin(self.base_url, href)

                    filename = self._extract_filename_from_link_enhanced(element, href)
                    if filename:
                        file_info = {
                            'url': href,
                            'filename': filename,
                            'type': pattern['method'],
                            'title': element.get_text(strip=True)[:100],
                            'method': pattern['method'],
                            'course_id': str(getattr(module, 'course_id', '')) if hasattr(module, 'course_id') else ''
                        }
                        file_links.append(file_info)
                        self.logger.info(f"DEBUGGING: {pattern['method']} found: {filename}")

            # METHOD 3: Look for any text that mentions files
            page_text = soup.get_text()
            mentioned_files = []
            for ext in file_extensions:
                import re
                pattern = rf'\b\w+{re.escape(ext)}\b'
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                mentioned_files.extend(matches)

            if mentioned_files:
                self.logger.info(f"DEBUGGING: Page mentions these files: {mentioned_files[:10]}")

            # Remove duplicates based on URL
            seen_urls = set()
            unique_links = []
            for link in file_links:
                if link['url'] not in seen_urls:
                    seen_urls.add(link['url'])
                    unique_links.append(link)

            self.logger.info(f"DEBUGGING: After deduplication: {len(unique_links)} unique file links")

            return unique_links

        except Exception as e:
            self.logger.error("Enhanced file link detection failed", exception=e)
            return []

    def _extract_filename_from_link_enhanced(self, link, href: str) -> str:
        """ENHANCED: Extract filename from link with better detection."""
        try:
            # Method 1: Get from URL path
            parsed = urlparse(href)
            path_filename = Path(unquote(parsed.path)).name

            if path_filename and '.' in path_filename and len(path_filename) > 1:
                return path_filename

            # Method 2: Get from query parameters
            from urllib.parse import parse_qs
            query_params = parse_qs(parsed.query)

            # Check for filename in various parameter names
            filename_params = ['filename', 'name', 'file', 'attachment']
            for param in filename_params:
                if param in query_params and query_params[param]:
                    filename = unquote(query_params[param][0])
                    if '.' in filename:
                        return filename

            # Method 3: Get from link text (more liberal)
            link_text = link.get_text(strip=True)
            if link_text:
                # Look for file-like patterns in text
                import re
                file_pattern = r'([^/\\:*?"<>|]+\.[a-zA-Z0-9]{1,5})'
                matches = re.findall(file_pattern, link_text)
                if matches:
                    return matches[0]

            # Method 4: Get from title or other attributes
            for attr in ['title', 'data-filename', 'aria-label']:
                attr_value = link.get(attr, '')
                if attr_value and '.' in attr_value:
                    # Clean and extract filename-like part
                    import re
                    cleaned = re.sub(r'[<>:"/\\|?*]', '_', attr_value.strip())
                    if '.' in cleaned and len(cleaned) > 1:
                        return cleaned

            # Method 5: Generate from URL if it looks like a file endpoint
            if '/files/' in href:
                file_id_match = re.search(r'/files/(\d+)', href)
                if file_id_match:
                    file_id = file_id_match.group(1)
                    # Try to guess extension from content-type or URL context
                    return f"canvas_file_{file_id}.pdf"  # Default to PDF

            return None

        except Exception as e:
            self.logger.debug(f"Error extracting filename from {href}", exception=e)
            return None

    async def _download_file_from_web(self, file_info: Dict[str, Any], files_folder: Path) -> bool:
        """Download a file from web URL."""
        try:
            url = file_info['url']
            filename = self.sanitize_filename(file_info['filename'])

            if not filename:
                filename = f"downloaded_file_{int(time.time())}"

            file_path = files_folder / filename

            # Avoid overwriting existing files
            counter = 1
            original_path = file_path
            while file_path.exists():
                name = original_path.stem
                suffix = original_path.suffix
                file_path = files_folder / f"{name}_{counter}{suffix}"
                counter += 1

            self.logger.info(f"Downloading file: {filename}")

            # CORE FIX: Resolve module item URLs to actual download URLs
            course_id = file_info.get('course_id', '')
            if course_id and '/modules/items/' in url:
                resolved_url = self._resolve_canvas_file_url(url, course_id)
                if resolved_url != url:
                    self.logger.info(f"🔄 URL RESOLVED: {filename}")
                    url = resolved_url

            # Download the file
            response = self.web_session.get(url, stream=True, timeout=60)
            response.raise_for_status()

            # Check if it's actually a file (not an HTML error page)
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' in content_type and file_path.suffix.lower() != '.html':
                self.logger.warning(f"Skipping {filename} - appears to be HTML page, not file")
                return False

            # Write file
            async with aiofiles.open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        await f.write(chunk)

            # Verify file was created and has content
            if file_path.exists() and file_path.stat().st_size > 0:
                # Save metadata
                metadata_file = files_folder / f"{filename}.metadata.json"
                metadata = {
                    'original_url': url,
                    'filename': filename,
                    'download_timestamp': datetime.now().isoformat(),
                    'file_size': file_path.stat().st_size,
                    'content_type': content_type,
                    'extraction_method': 'web_scraping',
                    'title': file_info.get('title', ''),
                    'type': file_info.get('type', '')
                }

                async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(metadata, indent=2))

                self.logger.info(f"Successfully downloaded: {filename} ({file_path.stat().st_size} bytes)")
                return True
            else:
                self.logger.warning(f"Downloaded file is empty: {filename}")
                if file_path.exists():
                    file_path.unlink()  # Remove empty file
                return False

        except Exception as e:
            self.logger.error(f"Failed to download file {file_info.get('filename', 'unknown')}", exception=e)
            return False

    async def _create_module_overview(self, module: Module, metadata: Dict[str, Any],
                                    module_folder: Path):
        """Create module overview file (original functionality)."""
        try:
            module_name = getattr(module, 'name', 'Unknown Module')
            overview_content = self._generate_module_overview(module, metadata)
            overview_file = module_folder / f"{self.sanitize_filename(module_name)}_overview.html"

            async with aiofiles.open(overview_file, 'w', encoding='utf-8') as f:
                await f.write(overview_content)

            # Also save metadata
            metadata_file = module_folder / "module_metadata.json"
            async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False, default=str))

        except Exception as e:
            self.logger.error(f"Failed to create module overview", exception=e)

    async def _process_module_items_api(self, module: Module, metadata: Dict[str, Any],
                                      module_folder: Path):
        """Process module items using API (original functionality for compatibility)."""
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
                    await self._process_module_item_api(item, items_folder)
                except Exception as e:
                    self.logger.warning(f"Failed to process module item",
                                      item_id=item.get('id', 'unknown'),
                                      item_title=item.get('title', 'unknown'),
                                      exception=e)

        except Exception as e:
            self.logger.error(f"Failed to process module items via API", exception=e)

    async def _process_module_item_api(self, item: Dict[str, Any], items_folder: Path):
        """Process a single module item using API (original functionality)."""
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
            await self._create_item_shortcut_api(item, items_folder)

        except Exception as e:
            self.logger.warning(f"Failed to process individual module item via API",
                              item_id=item.get('id', 'unknown'),
                              exception=e)

    async def _create_item_shortcut_api(self, item: Dict[str, Any], items_folder: Path):
        """Create shortcut/reference file for module items (original functionality)."""
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

    def extract_metadata(self, module: Module) -> Dict[str, Any]:
        """Extract comprehensive metadata from a module (original functionality)."""
        try:
            # Basic module information
            metadata = {
                'id': getattr(module, 'id', None),
                'name': str(getattr(module, 'name', '')),
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
        """Extract metadata from a module item (original functionality)."""
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

    def _generate_module_overview(self, module: Module, metadata: Dict[str, Any]) -> str:
        """Generate HTML overview for a module (original functionality)."""
        try:
            module_name = getattr(module, 'name', 'Unknown Module')

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

            if metadata.get('unlock_at'):
                lines.append(f"Unlocks: {metadata['unlock_at']}")

            lines.append("")

            # Module items
            if metadata.get('items'):
                lines.append("Module Items:")
                for i, item in enumerate(metadata['items'], 1):
                    item_title = item.get('title', 'Untitled')
                    item_type = item.get('type', 'unknown')
                    lines.append(f"  {i}. [{item_type}] {item_title}")
                lines.append("")

            # Footer
            lines.extend([
                "-" * 60,
                f"Downloaded from Canvas on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "",
                "ENHANCED with web scraping for actual file downloads!"
            ])

            return "\n".join(lines)

        except Exception as e:
            self.logger.warning(f"Failed to generate module overview", exception=e)
            return f"Module: {getattr(module, 'name', 'Unknown')}\nError generating overview: {e}"

    async def process_content_item(self, item: Any, course_folder: Path, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a module item for download (required by BaseDownloader).

        This method handles processing of individual module items.
        The enhanced version processes modules with web scraping.

        Args:
            item: Canvas module object
            course_folder: Base folder path for the course
            metadata: Pre-extracted module metadata

        Returns:
            Optional[Dict[str, Any]]: Download result information
        """
        try:
            module_name = self.sanitize_filename(getattr(item, 'name', 'unknown'))
            module_folder = course_folder / module_name
            module_folder.mkdir(parents=True, exist_ok=True)

            # Save module metadata
            metadata_file = module_folder / 'module_info.json'

            async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False, default=str))

            return {
                'module_id': getattr(item, 'id', 'unknown'),
                'module_name': getattr(item, 'name', 'unknown'),
                'folder_path': str(module_folder),
                'files_created': ['module_info.json'],
                'success': True
            }

        except Exception as e:
            self.logger.error(f"Failed to process module",
                            module_id=getattr(item, 'id', 'unknown'),
                            exception=e)
            return None

    async def _create_course_module_index(self, items_metadata: List[Dict[str, Any]]):
        """Create course-wide module index (original functionality)."""
        try:
            index_file = self.course_folder / "course_modules_index.html"

            lines = [
                "<h1>Course Modules Index</h1>",
                f"<p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
                "<p><strong>ENHANCED</strong> with web scraping for actual file downloads!</p>",
                "<ul>"
            ]

            for item in items_metadata:
                module_name = item.get('name', 'Unknown Module')
                item_count = item.get('item_count', 0)
                files_downloaded = item.get('files_downloaded', 0)

                status_icon = "✅" if files_downloaded > 0 else "📄"
                lines.append(f"<li>{status_icon} <strong>{module_name}</strong> - {item_count} items, {files_downloaded} files downloaded</li>")

            lines.extend([
                "</ul>",
                "<hr>",
                "<p><em>Enhanced ModulesDownloader with web scraping capabilities</em></p>"
            ])

            async with aiofiles.open(index_file, 'w', encoding='utf-8') as f:
                await f.write('\n'.join(lines))

        except Exception as e:
            self.logger.warning(f"Failed to create course module index", exception=e)

    async def _save_debug_info(self, soup: BeautifulSoup, debug_folder: Path, module):
        """Save debugging information when no files are found."""
        try:
            # Save all links found on the page
            all_links = soup.find_all('a', href=True)
            links_info = []

            for i, link in enumerate(all_links[:50]):  # Limit to first 50
                href = link.get('href', '')
                text = link.get_text(strip=True)
                title = link.get('title', '')

                links_info.append({
                    'index': i,
                    'href': href,
                    'text': text[:100],
                    'title': title[:100],
                    'classes': link.get('class', []),
                    'has_file_extension': any(
                        ext in href.lower() + text.lower() for ext in ['.pdf', '.doc', '.ppt', '.xls'])
                })

            debug_links_file = debug_folder / f"all_links_debug.json"
            async with aiofiles.open(debug_links_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(links_info, indent=2, ensure_ascii=False))

            # Save page structure info
            structure_info = {
                'total_links': len(all_links),
                'module_name': getattr(module, 'name', 'unknown'),
                'module_id': getattr(module, 'id', 'unknown'),
                'page_title': soup.title.string if soup.title else 'No title',
                'has_module_items': bool(soup.find_all(class_=re.compile(r'.*module.*item.*'))),
                'has_attachments': bool(soup.find_all(class_=re.compile(r'.*attachment.*'))),
                'has_files_refs': bool(soup.find_all(href=re.compile(r'.*/files/.*'))),
                'body_classes': soup.body.get('class', []) if soup.body else []
            }

            debug_structure_file = debug_folder / f"page_structure_debug.json"
            async with aiofiles.open(debug_structure_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(structure_info, indent=2, ensure_ascii=False))

            self.logger.info(f"DEBUGGING: Saved debug info to {debug_folder}")
            self.logger.info(
                f"DEBUGGING: Structure - Total links: {structure_info['total_links']}, Has module items: {structure_info['has_module_items']}")

        except Exception as e:
            self.logger.error("Failed to save debug info", exception=e)

# Register the ENHANCED downloader with the factory (this replaces the original)
from .base import ContentDownloaderFactory
ContentDownloaderFactory.register_downloader('modules', ModulesDownloader)