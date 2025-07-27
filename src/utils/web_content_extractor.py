"""
Fixed Web Content Extractor - URL Resolution Fix

This fixes the core issue: extracting content_id from Canvas module item URLs
and resolving them to actual file download URLs using the Canvas API.

CHANGES MADE:
1. Added Canvas API client parameter to constructor
2. Modified _extract_file_info_from_link() to resolve Canvas file URLs
3. Added _resolve_canvas_file_url() method to get actual download URLs
4. Kept all existing functionality and structure intact

This solves the PDF detection issue while preserving the original project design.
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
    FIXED: Web Content Extractor for Canvas

    This class now correctly resolves Canvas module item URLs to actual file download URLs
    by extracting content_id and using the Canvas API to get the real download links.
    """

    def __init__(self, cookies_path: str, base_url: str = None, canvas_client=None):
        """
        Initialize the web content extractor.

        Args:
            cookies_path: Path to browser cookies file
            base_url: Canvas base URL (auto-detected if None)
            canvas_client: Canvas API client for URL resolution (REQUIRED FOR FIX)
        """
        self.cookies_path = cookies_path
        self.base_url = base_url
        self.canvas_client = canvas_client  # NEW: Canvas API client for file resolution
        self.logger = get_logger(__name__)

        # Initialize requests session
        self.session = requests.Session()

        # Load cookies if available
        if self.cookies_path and Path(self.cookies_path).exists():
            self._load_cookies()
        else:
            self.logger.warning(f"Cookies file not found: {cookies_path}")

    def _load_cookies(self):
        """Load cookies from file for web authentication."""
        try:
            with open(self.cookies_path, 'r', encoding='utf-8') as f:
                cookies_text = f.read()

            # Parse Netscape cookie format
            for line in cookies_text.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split('\t')
                    if len(parts) >= 7:
                        domain, _, path, _, _, name, value = parts[:7]
                        self.session.cookies.set(name, value, domain=domain, path=path)

            self.logger.info(f"Loaded {len(self.session.cookies)} cookies from {self.cookies_path}")

        except Exception as e:
            self.logger.error(f"Failed to load cookies", exception=e)

    def _extract_file_info_from_link(self, link, course_id: str) -> Optional[FileInfo]:
        """
        FIXED: Extract file information from a link element.

        Now properly resolves Canvas module item URLs to actual file download URLs.
        """
        try:
            href = link.get('href', '')
            if not href:
                return None

            # Make URL absolute
            if href.startswith('/'):
                href = urljoin(self.base_url, href)

            # Extract filename from link text or attributes
            filename = self._extract_filename_from_element(link, href)
            if not filename:
                return None

            # CORE FIX: Resolve Canvas module item URLs to actual file URLs
            actual_file_url, content_id = self._resolve_canvas_file_url(href, course_id)

            if not actual_file_url:
                self.logger.debug(f"Could not resolve file URL for: {href}")
                return None

            # Create FileInfo with the resolved download URL
            file_info = FileInfo(
                filename=filename,
                url=actual_file_url,  # Use resolved download URL instead of module item URL
                file_type=self._determine_file_type(actual_file_url, filename),
                size=self._extract_file_size(link),
                item_title=link.get_text(strip=True),
                content_id=content_id
            )

            return file_info

        except Exception as e:
            self.logger.debug(f"Error extracting file info from link", exception=e)
            return None

    def _resolve_canvas_file_url(self, url: str, course_id: str) -> Tuple[Optional[str], Optional[str]]:
        """
        CORE FIX: Resolve Canvas module item URL to actual file download URL.

        This is the key method that fixes the PDF detection issue.

        Args:
            url: Canvas module item URL (like /courses/123/modules/items/456)
            course_id: Canvas course ID

        Returns:
            Tuple[Optional[str], Optional[str]]: (download_url, content_id) or (None, None)
        """
        try:
            # Check if this is a Canvas module item URL
            module_item_match = re.search(r'/courses/(\d+)/modules/items/(\d+)', url)
            if not module_item_match:
                # Not a module item URL, might be a direct file URL
                if '/files/' in url:
                    return url, self._extract_content_id_from_url(url)
                return None, None

            course_id_from_url = module_item_match.group(1)
            module_item_id = module_item_match.group(2)

            # Use Canvas API to get the module item details
            if not self.canvas_client:
                self.logger.warning("Canvas API client not available for URL resolution")
                return None, None

            try:
                # Get the course object
                course = self.canvas_client.get_course(course_id)

                # Find the module that contains this item
                modules = course.get_modules()

                for module in modules:
                    try:
                        module_items = module.get_module_items()
                        for item in module_items:
                            if str(item.id) == module_item_id:
                                # Found the module item!
                                if hasattr(item, 'content_id') and item.content_id:
                                    # This is a file item, get the actual file
                                    try:
                                        file_obj = course.get_file(item.content_id)
                                        download_url = getattr(file_obj, 'url', None)
                                        if download_url:
                                            self.logger.info(f"Resolved module item {module_item_id} to file URL: {download_url[:100]}...")
                                            return download_url, str(item.content_id)
                                    except Exception as e:
                                        self.logger.debug(f"Could not get file for content_id {item.content_id}: {e}")

                                elif hasattr(item, 'url') and item.url:
                                    # Might be an external URL or other type
                                    return item.url, None

                    except Exception as e:
                        self.logger.debug(f"Error processing module items: {e}")
                        continue

            except Exception as e:
                self.logger.debug(f"Error accessing Canvas API for URL resolution: {e}")

            return None, None

        except Exception as e:
            self.logger.debug(f"Error resolving Canvas file URL: {e}")
            return None, None

    def _extract_content_id_from_url(self, url: str) -> Optional[str]:
        """Extract content_id from a Canvas file URL."""
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

    def _extract_filename_from_element(self, link, href: str) -> Optional[str]:
        """Extract filename from link element and URL."""
        try:
            # Method 1: Get from URL path
            parsed = urlparse(href)
            path_filename = Path(unquote(parsed.path)).name
            if path_filename and '.' in path_filename and len(path_filename) > 1:
                return path_filename

            # Method 2: Get from query parameters
            from urllib.parse import parse_qs
            query_params = parse_qs(parsed.query)
            filename_params = ['filename', 'name', 'file', 'attachment']
            for param in filename_params:
                if param in query_params and query_params[param]:
                    filename = unquote(query_params[param][0])
                    if '.' in filename:
                        return filename

            # Method 3: Get from link text
            link_text = link.get_text(strip=True)
            if link_text:
                import re
                file_pattern = r'([^/\\:*?"<>|]+\.[a-zA-Z0-9]{1,5})'
                matches = re.findall(file_pattern, link_text)
                if matches:
                    return matches[0]

            # Method 4: Get from title or other attributes
            for attr in ['title', 'data-filename', 'aria-label']:
                attr_value = link.get(attr, '')
                if attr_value and '.' in attr_value:
                    import re
                    cleaned = re.sub(r'[<>:"/\\|?*]', '_', attr_value.strip())
                    if '.' in cleaned and len(cleaned) > 1:
                        return cleaned

            return None

        except Exception as e:
            self.logger.debug(f"Error extracting filename from {href}", exception=e)
            return None

    def _determine_file_type(self, url: str, filename: str) -> str:
        """Determine file type from URL and filename."""
        ext = Path(filename).suffix.lower()
        type_mapping = {
            '.pdf': 'PDF Document',
            '.doc': 'Word Document', '.docx': 'Word Document',
            '.ppt': 'PowerPoint', '.pptx': 'PowerPoint',
            '.xls': 'Excel Spreadsheet', '.xlsx': 'Excel Spreadsheet',
            '.txt': 'Text File', '.zip': 'Archive',
            '.jpg': 'Image', '.jpeg': 'Image', '.png': 'Image', '.gif': 'Image',
        }
        return type_mapping.get(ext, 'File')

    def _extract_file_size(self, link) -> Optional[str]:
        """Extract file size from link text or nearby elements."""
        try:
            text = link.get_text()
            size_match = re.search(r'\(([0-9.]+\s*[KMGT]?B)\)', text)
            if size_match:
                return size_match.group(1)

            parent = link.parent
            if parent:
                parent_text = parent.get_text()
                size_match = re.search(r'\b([0-9.]+\s*[KMGT]?B)\b', parent_text)
                if size_match:
                    return size_match.group(1)
            return None
        except Exception:
            return None

    def extract_module_files(self, course_id: str, module_id: str) -> List[FileInfo]:
        """
        Extract files from a Canvas module.

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
            files.extend(self._extract_from_module_items(soup, course_id))
            files.extend(self._extract_from_attachments(soup, course_id))
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
                domain = cookie.domain[1:] if cookie.domain.startswith('.') else cookie.domain
                return f"https://{domain}"
        return "https://canvas.instructure.com"

    def _extract_from_module_items(self, soup: BeautifulSoup, course_id: str) -> List[FileInfo]:
        """Extract files from module item elements."""
        files = []
        module_items = soup.find_all(['div', 'li'], class_=re.compile(r'.*module.*item.*|.*context_module_item.*'))

        for item in module_items:
            try:
                file_links = item.find_all('a', href=re.compile(r'.*/files/.*|.*\.pdf|.*\.docx?|.*\.pptx?|.*\.xlsx?'))
                for link in file_links:
                    file_info = self._extract_file_info_from_link(link, course_id)
                    if file_info:
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

        file_patterns = [
            r'/files/\d+',  # Canvas file ID pattern
            r'\.pdf($|\?)', r'\.docx?($|\?)', r'\.pptx?($|\?)', r'\.xlsx?($|\?)',
            r'\.txt($|\?)', r'\.zip($|\?)', r'\.jpg($|\?)', r'\.png($|\?)'
        ]
        return any(re.search(pattern, href, re.IGNORECASE) for pattern in file_patterns)

    def _find_module_title(self, element) -> str:
        """Find module title from element context."""
        try:
            current = element
            for _ in range(5):  # Look up 5 levels
                if current is None:
                    break
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
            url: File download URL (now properly resolved!)
            filepath: Local path to save file
            max_retries: Maximum download attempts

        Returns:
            bool: True if download successful
        """
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)

            for attempt in range(max_retries):
                try:
                    self.logger.info(f"Downloading file (attempt {attempt + 1})",
                                     url=url,
                                     filepath=str(filepath))

                    response = self.session.get(url, stream=True, timeout=60)
                    response.raise_for_status()

                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    if filepath.exists() and filepath.stat().st_size > 0:
                        self.logger.info(f"File downloaded successfully",
                                         filepath=str(filepath),
                                         size=filepath.stat().st_size)
                        return True
                    else:
                        raise Exception("Downloaded file is empty or missing")

                except Exception as e:
                    self.logger.warning(f"Download attempt {attempt + 1} failed", exception=e)
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    else:
                        raise

            return False

        except Exception as e:
            self.logger.error(f"Failed to download file",
                              url=url,
                              filepath=str(filepath),
                              exception=e)
            return False


def create_web_content_extractor(cookies_path: str = "config/cookies.txt",
                                 canvas_client=None) -> WebContentExtractor:
    """
    Factory function to create a web content extractor.

    Args:
        cookies_path: Path to browser cookies file
        canvas_client: Canvas API client for URL resolution

    Returns:
        WebContentExtractor: Configured extractor instance
    """
    return WebContentExtractor(cookies_path=cookies_path, canvas_client=canvas_client)