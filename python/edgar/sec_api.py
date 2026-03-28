"""
SEC EDGAR API Client
Interfaces with SEC's submission and full-text search APIs.
"""

import asyncio
import re
from typing import Optional, Dict, Any, List
from datetime import datetime

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import get_settings

logger = structlog.get_logger()


class SecApi:
    """Client for SEC EDGAR APIs."""

    BASE_URL = "https://data.sec.gov"
    EFTS_URL = "https://efts.sec.gov/LATEST/search-index"

    def __init__(self):
        self.settings = get_settings()
        self._client: Optional[httpx.AsyncClient] = None
        self._last_request = 0.0
        self._min_interval = 0.1  # 100ms between requests

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={
                "User-Agent": self.settings.sec_user_agent,
                "Accept": "application/json",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()

    async def _rate_limit(self):
        """Ensure we don't exceed SEC rate limits."""
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request = asyncio.get_event_loop().time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=8),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def get_company_submissions(self, cik: str) -> Dict[str, Any]:
        """
        Get company submission history from SEC.

        Args:
            cik: Company CIK (with or without leading zeros)

        Returns:
            Full submissions JSON from SEC API
        """
        await self._rate_limit()

        # Pad CIK to 10 digits
        padded_cik = cik.zfill(10)
        url = f"{self.BASE_URL}/submissions/CIK{padded_cik}.json"

        logger.info("Fetching company submissions", cik=cik, url=url)

        response = await self._client.get(url)
        response.raise_for_status()

        return response.json()

    async def get_filing_metadata(
        self, cik: str, accession_number: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific filing.

        Args:
            cik: Company CIK
            accession_number: SEC accession number (format: XXXXXXXXXX-XX-XXXXXX)

        Returns:
            Filing metadata or None if not found
        """
        submissions = await self.get_company_submissions(cik)

        # Normalize accession number for comparison
        normalized = accession_number.replace("-", "")

        recent = submissions.get("filings", {}).get("recent", {})
        accessions = recent.get("accessionNumber", [])

        for i, acc in enumerate(accessions):
            if acc.replace("-", "") == normalized:
                return {
                    "accessionNumber": acc,
                    "filingDate": recent.get("filingDate", [])[i],
                    "reportDate": recent.get("reportDate", [])[i],
                    "form": recent.get("form", [])[i],
                    "primaryDocument": recent.get("primaryDocument", [])[i],
                }

        return None

    async def get_ncsr_filings_for_cik(
        self, cik: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get all N-CSR and N-CSRS filings for a company.

        Args:
            cik: Company CIK
            limit: Maximum number of filings to return

        Returns:
            List of filing metadata dicts
        """
        submissions = await self.get_company_submissions(cik)

        filings = []
        recent = submissions.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        primary_docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form in ("N-CSR", "N-CSRS") and len(filings) < limit:
                acc = accessions[i]
                # Build the filing URL
                acc_no_dash = acc.replace("-", "")
                url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_dash}/{primary_docs[i]}"

                filings.append(
                    {
                        "cik": cik,
                        "accessionNumber": acc,
                        "filingDate": filing_dates[i],
                        "reportDate": report_dates[i],
                        "form": form,
                        "primaryDocument": primary_docs[i],
                        "url": url,
                    }
                )

        logger.info(
            "Found N-CSR filings", cik=cik, count=len(filings), limit=limit
        )

        return filings

    async def search_ncsr_filings(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Search for N-CSR/N-CSRS filings using EDGAR full-text search.

        Args:
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            limit: Maximum results

        Returns:
            List of filing metadata
        """
        await self._rate_limit()

        # Build search query
        params = {
            "q": "",
            "dateRange": "custom",
            "forms": "N-CSR,N-CSRS",
            "startdt": date_from or "2020-01-01",
            "enddt": date_to or datetime.now().strftime("%Y-%m-%d"),
            "from": 0,
            "size": min(limit, 100),
        }

        logger.info("Searching EDGAR for N-CSR filings", params=params)

        response = await self._client.get(self.EFTS_URL, params=params)
        response.raise_for_status()

        data = response.json()
        hits = data.get("hits", {}).get("hits", [])

        filings = []
        for hit in hits:
            source = hit.get("_source", {})
            filings.append(
                {
                    "cik": source.get("ciks", [""])[0],
                    "accessionNumber": source.get("adsh"),
                    "filingDate": source.get("file_date"),
                    "form": source.get("form"),
                    "companyName": source.get("display_names", [""])[0],
                }
            )

        return filings

    def build_filing_url(self, cik: str, accession_number: str, filename: str) -> str:
        """
        Build the full URL to a filing document.

        Args:
            cik: Company CIK
            accession_number: SEC accession number
            filename: Document filename (e.g., 'd851218dncsrs.htm')

        Returns:
            Full URL to the document
        """
        acc_no_dash = accession_number.replace("-", "")
        return f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_dash}/{filename}"
