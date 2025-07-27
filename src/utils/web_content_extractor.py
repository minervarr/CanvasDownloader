"""
Web Content Extractor - Core Module

This module extracts actual file download URLs from Canvas module pages by scraping
the web interface. It finds the PDFs and files that the Canvas API misses.

Purpose: Get real download URLs for files like PDFs that show in Canvas but not in API
Strategy: Scrape rendered Canvas module pages → Extract file links → Download directly

Usage:
    extractor = WebContentExtractor(cookies_path="config/cookies.txt")
    files = extractor.extract_module_files(course_id, module_id)
    for file_info in files:
        extractor.download_file(file_info['url'], file_info['filename'])
"""

import requests
import re
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse, unquote
from dataclasses import dataclass
import json

try:
    from bs4 import BeautifulSoup

    BEAUTIFULSOUP_AVAILABLE = True
except ImportError:
    print("BeautifulSoup not available. Install with: pip install beautifulsoup4")
    BEAUTIFULSOUP_AVAILABLE = False

from ..utils.logger import get_logger


@dataclass
class FileInfo:
    """Information about a file found on Canvas."""
    filename: str
    url: str
    file_type: str
    size: Optional[str] = None
    module_name: str = ""
    item_title: str = ""
    content_id: Optional[str] = None


class WebContentExtractor:
    """
    Web Content Extractor for Canvas

    This class scrapes Canvas web pages to extract actual file download URLs
    that are missing from the Canvas API responses.

    Key Features:
    - Uses browser cookies for authentication
    - Scrapes module pages to find file links
    - Extracts direct download URLs
    - Handles Canvas-specific link patterns
    - Downloads files directly from web interface
    """

    def __init__(self, cookies_path: str, base_url: str = None):
        """
        Initialize the web content extractor.

        Args:
            cookies_path: Path to browser cookies file (Netscape format)
            base_url: Base Canvas URL (will be auto-detected if not provided)
        """
        self.logger = get_logger(__name__)
        self.cookies_path = Path(cookies_path)
        self.base_url = base_url
        self.session = requests.Session()

        # Load cookies for authentication
        self._load_cookies()

        # Set up session headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        self.logger.info("Web content extractor initialized",
                         cookies_loaded=self.cookies_path.exists())

    def _load_cookies(self):
        """Load browser cookies from file."""
        if not self.cookies_path.exists():
            self.logger.warning(f"Cookies file not found: {self.cookies_path}")
            return

        try:
            with open(self.cookies_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split('\t')
                        if len(parts) >= 7:
                            domain = parts[0]
                            name = parts[5]
                            value = parts[6]

                            self.session.cookies.set(name, value, domain=domain)

            self.logger.info(f"Loaded {len(self.session.cookies)} cookies from {self.cookies_path}")

        except Exception as e:
            self.logger.error(f"Failed to load cookies", exception=e)

    def extract_module_files(self, course_id: str, module_id: str) -> List[FileInfo]:
        """
        Extract file information from a Canvas module page.

        Args:
            course_id: Canvas course ID
            module_id: Canvas module ID

        Returns:
            List[FileInfo]: List of files found in the module
        """
        if not BEAUTIFULSOUP_AVAILABLE:
            self.logger.error("BeautifulSoup not available for web scraping")
            return []

        try:
            # Construct module URL
            if not self.base_url:
                # Auto-detect base URL from cookies
                self.base_url = self._detect_canvas_url()

            module_url = f"{self.base_url}/courses/{course_id}/modules/{module_id}"

            self.logger.info(f"Extracting files from module",
                             course_id=course_id,
                             module_id=module_id,
                             url=module_url)

            # Fetch the module page
            response = self.session.get(module_url, timeout=30)
            response.raise_for_status()

            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract files from the page
            files = []

            # Method 1: Look for file links in module items
            files.extend(self._extract_from_module_items(soup, course_id))

            # Method 2: Look for attachment links
            files.extend(self._extract_from_attachments(soup, course_id))

            # Method 3: Look for direct file links
            files.extend(self._extract_from_direct_links(soup, course_id))

            self.logger.info(f"Found {len(files)} files in module",
                             module_id=module_id,
                             files=[f.filename for f in files])

            return files

        except Exception as e:
            self.logger.error(f"Failed to extract module files",
                              course_id=course_id,
                              module_id=module_id,
                              exception=e)
            return []

    def _detect_canvas_url(self) -> str:
        """Auto-detect Canvas base URL from cookies."""
        for cookie in self.session.cookies:
            if 'instructure.com' in cookie.domain:
                # Extract subdomain
                if cookie.domain.startswith('.'):
                    domain = cookie.domain[1:]  # Remove leading dot
                else:
                    domain = cookie.domain

                return f"https://{domain}"

        # Fallback
        return "https://canvas.instructure.com"

    def _extract_from_module_items(self, soup: BeautifulSoup, course_id: str) -> List[FileInfo]:
        """Extract files from module item elements."""
        files = []

        # Look for module items with file links
        module_items = soup.find_all(['div', 'li'], class_=re.compile(r'.*module.*item.*|.*context_module_item.*'))

        for item in module_items:
            try:
                # Look for file links
                file_links = item.find_all('a', href=re.compile(r'.*/files/.*|.*\.pdf|.*\.docx?|.*\.pptx?|.*\.xlsx?'))

                for link in file_links:
                    file_info = self._extract_file_info_from_link(link, course_id)
                    if file_info:
                        # Get module context
                        module_title = self._find_module_title(item)
                        if module_title:
                            file_info.module_name = module_title

                        files.append(file_info)

            except Exception as e:
                self.logger.debug(f"Error processing module item", exception=e)
                continue

        return files

    def _extract_from_attachments(self, soup: BeautifulSoup, course_id: str) -> List[FileInfo]:
        """Extract files from attachment sections."""
        files = []

        # Look for attachment sections
        attachment_sections = soup.find_all(['div', 'section'], class_=re.compile(r'.*attachment.*|.*file.*'))

        for section in attachment_sections:
            links = section.find_all('a', href=True)
            for link in links:
                if self._is_file_link(link.get('href', '')):
                    file_info = self._extract_file_info_from_link(link, course_id)
                    if file_info:
                        files.append(file_info)

        return files

    def _extract_from_direct_links(self, soup: BeautifulSoup, course_id: str) -> List[FileInfo]:
        """Extract direct file links from anywhere on the page."""
        files = []

        # Find all links that look like files
        all_links = soup.find_all('a', href=True)

        for link in all_links:
            href = link.get('href', '')
            if self._is_file_link(href):
                file_info = self._extract_file_info_from_link(link, course_id)
                if file_info:
                    files.append(file_info)

        return files

    def _is_file_link(self, href: str) -> bool:
        """Check if a href looks like a file download link."""
        if not href:
            return False

        # Check for common file patterns
        file_patterns = [
            r'/files/\d+',  # Canvas file ID pattern
            r'\.pdf($|\?)',  # PDF files
            r'\.docx?($|\?)',  # Word documents
            r'\.pptx?($|\?)',  # PowerPoint
            r'\.xlsx?($|\?)',  # Excel
            r'/courses/\d+/files/',  # Course files
            r'download\?.*file',  # Download parameters
        ]

        return any(re.search(pattern, href, re.IGNORECASE) for pattern in file_patterns)

    def _extract_file_info_from_link(self, link, course_id: str) -> Optional[FileInfo]:
        """Extract file information from a link element."""
        try:
            href = link.get('href', '')
            if not href:
                return None

            # Make URL absolute
            if href.startswith('/'):
                href = urljoin(self.base_url, href)

            # Extract filename
            filename = self._extract_filename_from_url(href)
            if not filename:
                filename = link.get_text(strip=True)

            if not filename:
                return None

            # Determine file type
            file_type = self._determine_file_type(href, filename)

            # Get additional info
            size = self._extract_file_size(link)
            item_title = link.get_text(strip=True)

            # Extract content ID if available
            content_id = self._extract_content_id(href)

            return FileInfo(
                filename=filename,
                url=href,
                file_type=file_type,
                size=size,
                item_title=item_title,
                content_id=content_id
            )

        except Exception as e:
            self.logger.debug(f"Error extracting file info from link", exception=e)
            return None

    def _extract_filename_from_url(self, url: str) -> str:
        """Extract filename from URL."""
        try:
            # Parse URL
            parsed = urlparse(url)
            path = unquote(parsed.path)

            # Get filename from path
            filename = Path(path).name

            # If no extension, try to get from query parameters
            if '.' not in filename and 'filename=' in parsed.query:
                for param in parsed.query.split('&'):
                    if param.startswith('filename='):
                        filename = unquote(param.split('=', 1)[1])
                        break

            return filename if filename and '.' in filename else ""

        except Exception:
            return ""

    def _determine_file_type(self, url: str, filename: str) -> str:
        """Determine file type from URL and filename."""
        # Get extension
        ext = Path(filename).suffix.lower()

        type_mapping = {
            '.pdf': 'PDF Document',
            '.doc': 'Word Document',
            '.docx': 'Word Document',
            '.ppt': 'PowerPoint',
            '.pptx': 'PowerPoint',
            '.xls': 'Excel Spreadsheet',
            '.xlsx': 'Excel Spreadsheet',
            '.txt': 'Text File',
            '.zip': 'Archive',
            '.jpg': 'Image',
            '.jpeg': 'Image',
            '.png': 'Image',
            '.gif': 'Image',
        }

        return type_mapping.get(ext, 'File')

    def _extract_file_size(self, link) -> Optional[str]:
        """Extract file size from link text or nearby elements."""
        try:
            # Look for size in link text
            text = link.get_text()
            size_match = re.search(r'\(([0-9.]+\s*[KMGT]?B)\)', text)
            if size_match:
                return size_match.group(1)

            # Look for size in parent elements
            parent = link.parent
            if parent:
                parent_text = parent.get_text()
                size_match = re.search(r'\b([0-9.]+\s*[KMGT]?B)\b', parent_text)
                if size_match:
                    return size_match.group(1)

            return None

        except Exception:
            return None

    def _extract_content_id(self, url: str) -> Optional[str]:
        """Extract Canvas content ID from URL."""
        try:
            # Look for file ID in URL path
            match = re.search(r'/files/(\d+)', url)
            if match:
                return match.group(1)

            # Look for content_id in query parameters
            parsed = urlparse(url)
            for param in parsed.query.split('&'):
                if param.startswith('content_id='):
                    return param.split('=', 1)[1]

            return None

        except Exception:
            return None

    def _find_module_title(self, element) -> str:
        """Find module title from element context."""
        try:
            # Look for module header in ancestors
            current = element
            for _ in range(5):  # Look up 5 levels
                if current is None:
                    break

                # Look for module title
                title_elem = current.find(['h1', 'h2', 'h3'], class_=re.compile(r'.*module.*title.*'))
                if title_elem:
                    return title_elem.get_text(strip=True)

                current = current.parent

            return ""

        except Exception:
            return ""

    def download_file(self, url: str, filepath: Path, max_retries: int = 3) -> bool:
        """
        Download a file from URL to filepath.

        Args:
            url: File download URL
            filepath: Local path to save file
            max_retries: Maximum download attempts

        Returns:
            bool: True if download successful
        """
        try:
            # Ensure directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)

            for attempt in range(max_retries):
                try:
                    self.logger.info(f"Downloading file (attempt {attempt + 1})",
                                     url=url,
                                     filepath=str(filepath))

                    # Download with streaming
                    response = self.session.get(url, stream=True, timeout=60)
                    response.raise_for_status()

                    # Write to file
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    # Verify file was created and has content
                    if filepath.exists() and filepath.stat().st_size > 0:
                        self.logger.info(f"File downloaded successfully",
                                         filepath=str(filepath),
                                         size=filepath.stat().st_size)
                        return True
                    else:
                        raise Exception("Downloaded file is empty or missing")

                except Exception as e:
                    self.logger.warning(f"Download attempt {attempt + 1} failed",
                                        exception=e)
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        raise

            return False

        except Exception as e:
            self.logger.error(f"Failed to download file",
                              url=url,
                              filepath=str(filepath),
                              exception=e)
            return False

    def extract_all_course_files(self, course_id: str, module_ids: List[str] = None) -> Dict[str, List[FileInfo]]:
        """
        Extract files from all modules in a course.

        Args:
            course_id: Canvas course ID
            module_ids: Specific module IDs to process (if None, will discover all)

        Returns:
            Dict[str, List[FileInfo]]: Module ID -> List of files
        """
        try:
            if module_ids is None:
                module_ids = self._discover_course_modules(course_id)

            all_files = {}

            for module_id in module_ids:
                try:
                    files = self.extract_module_files(course_id, module_id)
                    if files:
                        all_files[module_id] = files

                    # Be nice to the server
                    time.sleep(1)

                except Exception as e:
                    self.logger.error(f"Failed to extract files from module",
                                      module_id=module_id,
                                      exception=e)
                    continue

            total_files = sum(len(files) for files in all_files.values())
            self.logger.info(f"Extracted {total_files} files from {len(all_files)} modules",
                             course_id=course_id)

            return all_files

        except Exception as e:
            self.logger.error(f"Failed to extract course files",
                              course_id=course_id,
                              exception=e)
            return {}

    def _discover_course_modules(self, course_id: str) -> List[str]:
        """Discover all module IDs in a course."""
        try:
            modules_url = f"{self.base_url}/courses/{course_id}/modules"
            response = self.session.get(modules_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract module IDs from the page
            module_ids = []

            # Look for module elements with data-module-id
            modules = soup.find_all(attrs={'data-module-id': True})
            for module in modules:
                module_id = module.get('data-module-id')
                if module_id:
                    module_ids.append(module_id)

            # Alternative: look for links to modules
            if not module_ids:
                links = soup.find_all('a', href=re.compile(r'/modules/\d+'))
                for link in links:
                    match = re.search(r'/modules/(\d+)', link.get('href', ''))
                    if match:
                        module_ids.append(match.group(1))

            # Remove duplicates and return
            return list(set(module_ids))

        except Exception as e:
            self.logger.error(f"Failed to discover course modules", exception=e)
            return []


# Factory function for easy integration
def create_web_content_extractor(cookies_path: str = "config/cookies.txt",
                                 base_url: str = None) -> WebContentExtractor:
    """
    Factory function to create a web content extractor.

    Args:
        cookies_path: Path to browser cookies file
        base_url: Canvas base URL (auto-detected if not provided)

    Returns:
        WebContentExtractor: Configured extractor instance
    """
    return WebContentExtractor(cookies_path=cookies_path, base_url=base_url)