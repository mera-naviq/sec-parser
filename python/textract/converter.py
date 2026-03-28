"""
HTML to PDF Converter
Uses Playwright headless Chromium to render SEC filings to PDF.
"""

import asyncio
import re
import tempfile
from pathlib import Path
from typing import Optional, Tuple

import structlog
from playwright.async_api import async_playwright, Browser, Page

from config import get_settings

logger = structlog.get_logger()


class HtmlToPdfConverter:
    """Converts HTML to PDF using headless Chromium."""

    def __init__(self):
        self.settings = get_settings()
        self._browser: Optional[Browser] = None
        self._playwright = None

    async def __aenter__(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    def _rewrite_relative_urls(self, html: str, base_url: str) -> str:
        """
        Rewrite relative URLs to absolute URLs for proper PDF rendering.

        SEC HTML often has relative asset URLs that need to be absolute.
        """
        # Extract base path from URL
        # e.g., https://www.sec.gov/Archives/edgar/data/1862281/000119312525303466/
        base_path = "/".join(base_url.rsplit("/", 1)[:-1]) + "/"

        # Rewrite relative src and href attributes
        patterns = [
            (r'src="(?!http)([^"]+)"', f'src="{base_path}\\1"'),
            (r"src='(?!http)([^']+)'", f"src='{base_path}\\1'"),
            (r'href="(?!http)(?!#)([^"]+)"', f'href="{base_path}\\1"'),
            (r"href='(?!http)(?!#)([^']+)'", f"href='{base_path}\\1'"),
        ]

        result = html
        for pattern, replacement in patterns:
            result = re.sub(pattern, replacement, result)

        return result

    def _inject_print_styles(self, html: str) -> str:
        """Inject CSS to improve PDF rendering."""
        style = """
        <style>
            @page {
                size: letter;
                margin: 0.5in;
            }
            body {
                font-family: Arial, sans-serif;
                font-size: 10pt;
            }
            table {
                page-break-inside: auto;
                border-collapse: collapse;
            }
            tr {
                page-break-inside: avoid;
                page-break-after: auto;
            }
            td, th {
                padding: 2px 4px;
                border: 1px solid #ccc;
            }
        </style>
        """
        # Insert before </head> or at start
        if "</head>" in html.lower():
            return re.sub(
                r"(</head>)", style + r"\1", html, flags=re.IGNORECASE, count=1
            )
        return style + html

    async def convert_to_pdf(
        self, html: str, base_url: str, output_path: Optional[str] = None
    ) -> Tuple[bytes, int]:
        """
        Convert HTML to PDF.

        Args:
            html: Raw HTML content
            base_url: Original URL for resolving relative paths
            output_path: Optional path to save PDF

        Returns:
            Tuple of (PDF bytes, page count)

        Raises:
            Exception: If conversion fails
        """
        logger.info("Converting HTML to PDF", base_url=base_url, html_size=len(html))

        # Prepare HTML
        html = self._rewrite_relative_urls(html, base_url)
        html = self._inject_print_styles(html)

        # Create a new page
        page = await self._browser.new_page()

        try:
            # Set content with longer timeout for large documents
            await page.set_content(html, wait_until="networkidle", timeout=60000)

            # Generate PDF
            pdf_bytes = await page.pdf(
                format="Letter",
                print_background=True,
                margin={
                    "top": "0.5in",
                    "right": "0.5in",
                    "bottom": "0.5in",
                    "left": "0.5in",
                },
            )

            # Count pages (approximate from PDF size)
            # Average page is ~50KB, but this is rough
            # Better: parse PDF for actual page count
            page_count = self._count_pdf_pages(pdf_bytes)

            logger.info(
                "PDF conversion complete",
                pdf_size=len(pdf_bytes),
                page_count=page_count,
            )

            # Save if path provided
            if output_path:
                Path(output_path).write_bytes(pdf_bytes)

            return pdf_bytes, page_count

        except Exception as e:
            logger.error("PDF conversion failed", error=str(e))
            raise

        finally:
            await page.close()

    def _count_pdf_pages(self, pdf_bytes: bytes) -> int:
        """
        Count pages in a PDF.

        Uses a simple regex to find page count in PDF metadata.
        """
        # Look for /Count N in the PDF
        count_match = re.search(rb"/Count\s+(\d+)", pdf_bytes)
        if count_match:
            return int(count_match.group(1))

        # Fallback: count /Page objects
        page_matches = re.findall(rb"/Type\s*/Page[^s]", pdf_bytes)
        return len(page_matches) if page_matches else 1

    async def convert_with_fallback(
        self, html: str, base_url: str
    ) -> Tuple[Optional[bytes], int, bool]:
        """
        Convert HTML to PDF with fallback handling.

        Args:
            html: Raw HTML content
            base_url: Original URL

        Returns:
            Tuple of (PDF bytes or None, page count, exceeded_max_pages)
        """
        try:
            pdf_bytes, page_count = await self.convert_to_pdf(html, base_url)

            exceeded = page_count > self.settings.max_textract_pages

            if exceeded:
                logger.warning(
                    "PDF exceeds max pages for Textract",
                    page_count=page_count,
                    max_pages=self.settings.max_textract_pages,
                )

            return pdf_bytes, page_count, exceeded

        except Exception as e:
            logger.error(
                "PDF conversion failed, will use Claude-only extraction",
                error=str(e),
            )
            return None, 0, True
