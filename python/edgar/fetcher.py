"""
EDGAR Filing Fetcher
Fetches N-CSR/N-CSRS filings from SEC EDGAR with proper rate limiting.
"""

import asyncio
import re
from typing import Optional, Dict, Any
from datetime import datetime

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import get_settings, FILING_STATUS

logger = structlog.get_logger()


class RateLimiter:
    """Simple rate limiter for SEC EDGAR requests."""

    def __init__(self, requests_per_second: float = 10.0):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Wait if necessary to respect rate limit."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self.last_request_time
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_request_time = asyncio.get_event_loop().time()


class EdgarFetcher:
    """Fetches SEC EDGAR filings with rate limiting and retry logic."""

    def __init__(self):
        self.settings = get_settings()
        self.rate_limiter = RateLimiter(self.settings.sec_requests_per_second)
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={
                "User-Agent": self.settings.sec_user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=8),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        before_sleep=lambda retry_state: logger.warning(
            "Retrying EDGAR fetch",
            attempt=retry_state.attempt_number,
            error=str(retry_state.outcome.exception()) if retry_state.outcome else None,
        ),
    )
    async def fetch_filing_html(self, url: str) -> str:
        """
        Fetch filing HTML from SEC EDGAR.

        Args:
            url: Full URL to the filing HTML

        Returns:
            Raw HTML content

        Raises:
            ValueError: If SEC blocks the request (rate limiting)
            httpx.HTTPError: If fetch fails after retries
        """
        await self.rate_limiter.acquire()

        logger.info("Fetching filing", url=url)

        response = await self._client.get(url)
        response.raise_for_status()

        html = response.text

        # Check for SEC rate limiting/blocking
        if "Your Request Originates from an Undeclared Automated Tool" in html:
            raise ValueError("SEC rate limit exceeded - request was blocked")
        if "SEC.gov | Request Rate Threshold Exceeded" in html:
            raise ValueError("SEC rate threshold exceeded")

        logger.info("Filing fetched successfully", url=url, size=len(html))

        return html

    def extract_cik_from_url(self, url: str) -> str:
        """Extract CIK from SEC filing URL."""
        match = re.search(r"/data/(\d+)/", url)
        return match.group(1) if match else "unknown"

    def extract_accession_from_url(self, url: str) -> str:
        """Extract accession number from SEC filing URL."""
        # URL format: .../data/CIK/ACCESSION_NUMBER_NO_DASHES/filename.htm
        match = re.search(r"/data/\d+/(\d+)/", url)
        if match:
            raw = match.group(1)
            # Convert to standard format: XXXXXXXXXX-XX-XXXXXX
            if len(raw) == 18:
                return f"{raw[:10]}-{raw[10:12]}-{raw[12:]}"
            return raw
        return "unknown"

    def parse_filing_metadata_from_html(self, html: str) -> Dict[str, Any]:
        """
        Extract basic metadata from filing HTML.

        Returns:
            Dict with fund_name, report_type, report_period_end, etc.
        """
        metadata = {
            "fund_name": None,
            "report_type": "N-CSR",
            "report_period_end": None,
            "fiscal_year_end": None,
            "manager_name": None,
        }

        # Report type
        if "N-CSRS" in html[:5000]:
            metadata["report_type"] = "N-CSRS"

        # Fund name from CONFORMED-NAME
        name_match = re.search(r"<CONFORMED-NAME>([^<]+)", html, re.IGNORECASE)
        if name_match:
            metadata["fund_name"] = name_match.group(1).strip()

        # Report period from CONFORMED PERIOD OF REPORT
        period_match = re.search(
            r"CONFORMED PERIOD OF REPORT[:\s]*(\d{8})", html, re.IGNORECASE
        )
        if period_match:
            raw_date = period_match.group(1)
            # Convert YYYYMMDD to YYYY-MM-DD
            metadata["report_period_end"] = (
                f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            )

        # Alternative date patterns
        if not metadata["report_period_end"]:
            alt_patterns = [
                r"period\s+of\s+report[:\s]*(\d{4}-\d{2}-\d{2})",
                r"as\s+of\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})",
                r"period\s+ending[:\s]*([A-Za-z]+\s+\d{1,2},?\s+\d{4})",
            ]
            for pattern in alt_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    date_str = match.group(1)
                    try:
                        if "-" in date_str:
                            metadata["report_period_end"] = date_str
                        else:
                            parsed = datetime.strptime(
                                date_str.replace(",", ""), "%B %d %Y"
                            )
                            metadata["report_period_end"] = parsed.strftime("%Y-%m-%d")
                    except ValueError:
                        pass
                    break

        return metadata

    async def fetch_and_parse(self, url: str) -> Dict[str, Any]:
        """
        Fetch filing and extract metadata.

        Returns:
            Dict with html, cik, accession_number, and metadata fields
        """
        html = await self.fetch_filing_html(url)
        metadata = self.parse_filing_metadata_from_html(html)

        return {
            "html": html,
            "cik": self.extract_cik_from_url(url),
            "accession_number": self.extract_accession_from_url(url),
            "url": url,
            **metadata,
        }
