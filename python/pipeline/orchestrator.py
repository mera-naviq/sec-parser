"""
Pipeline Orchestrator
Main coordinator for the SEC N-CSR parsing pipeline.
"""

import asyncio
import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime

import structlog

from config import get_settings, FILING_STATUS
from edgar import EdgarFetcher, SecApi
from textract import HtmlToPdfConverter, TextractExtractor
from claude import ClaudeBatchProcessor, PromptTemplates, ClaudeResponseParser
from db import SupabaseClient
from .mapper import DataMapper, MappedHolding
from .validator import DataValidator, ValidationResult

logger = structlog.get_logger()


@dataclass
class PipelineResult:
    """Result of running the pipeline on a single filing."""
    filing_id: str
    success: bool
    holdings_count: int = 0
    confidence_score: float = 0.0
    error: Optional[str] = None
    warnings: List[str] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class PipelineOrchestrator:
    """Orchestrates the full N-CSR parsing pipeline."""

    def __init__(self):
        self.settings = get_settings()
        self.db: Optional[SupabaseClient] = None

    async def __aenter__(self):
        self.db = SupabaseClient()
        await self.db.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.db:
            await self.db.close()

    async def run_filing(self, url: str, extraction_strategy: str = "claude_only") -> PipelineResult:
        """
        Run the full pipeline on a single filing.

        Strategies:
        - textract_primary: Textract for tables + Claude for validation (original)
        - claude_only: Claude extracts everything from HTML (cheapest, most accurate)
        - hybrid: Claude for holdings, Textract for financial statements

        Args:
            url: SEC filing URL
            extraction_strategy: One of "textract_primary", "claude_only", "hybrid"

        Returns:
            PipelineResult with success status and metrics
        """
        filing_id = None

        try:
            # Step 1: EDGAR Fetch
            logger.info("Step 1: Fetching filing from EDGAR", url=url, strategy=extraction_strategy)

            async with EdgarFetcher() as fetcher:
                filing_data = await fetcher.fetch_and_parse(url)

            cik = filing_data["cik"]
            accession_number = filing_data["accession_number"]
            html = filing_data["html"]

            # Create or update filing record
            filing_id = await self.db.upsert_filing(
                cik=cik,
                fund_name=filing_data.get("fund_name") or "Unknown Fund",
                report_type=filing_data.get("report_type") or "N-CSR",
                report_period_end=filing_data.get("report_period_end") or datetime.now().strftime("%Y-%m-%d"),
                sec_accession_number=accession_number,
                sec_filing_url=url,
                status=FILING_STATUS["FETCHING"],
            )

            # Initialize extraction variables
            schedule_rows = []
            subtotals = []
            section_headers = []
            financial_tables = {}
            pdf_bytes = None

            # Step 2: PDF Conversion (only if needed for Textract)
            use_textract_for_holdings = extraction_strategy == "textract_primary"
            use_textract_for_financials = extraction_strategy in ["textract_primary", "hybrid"]

            if use_textract_for_holdings or use_textract_for_financials:
                await self.db.update_filing_status(filing_id, FILING_STATUS["CONVERTING"])
                logger.info("Step 2: Converting HTML to PDF")

                try:
                    async with HtmlToPdfConverter() as converter:
                        pdf_bytes, pdf_page_count, exceeded_max = await converter.convert_with_fallback(
                            html, url
                        )

                        if exceeded_max or pdf_bytes is None:
                            logger.info(
                                "PDF conversion failed or too large, falling back",
                                page_count=pdf_page_count,
                            )
                            # Fallback: disable Textract usage
                            use_textract_for_holdings = False
                            if extraction_strategy == "textract_primary":
                                use_textract_for_financials = False
                except Exception as e:
                    logger.warning(f"PDF conversion failed: {e}, continuing without Textract")
                    use_textract_for_holdings = False
                    use_textract_for_financials = False

            # Step 3: Textract Extraction (if applicable)
            if pdf_bytes and (use_textract_for_holdings or use_textract_for_financials):
                await self.db.update_filing_status(filing_id, FILING_STATUS["EXTRACTING"])
                logger.info("Step 3: Textract extraction", for_holdings=use_textract_for_holdings, for_financials=use_textract_for_financials)

                s3_pdf_key = f"raw/{cik}/{accession_number.replace('-', '')}/filing.pdf"
                extractor = TextractExtractor()

                try:
                    extraction_result = await extractor.extract(pdf_bytes, s3_pdf_key)

                    if use_textract_for_holdings:
                        schedule_rows = extraction_result.schedule_rows
                        subtotals = extraction_result.subtotals
                        section_headers = extraction_result.section_headers

                    if use_textract_for_financials:
                        financial_tables = extraction_result.financial_tables

                    await self.db.update_filing(filing_id, {"raw_pdf_s3_key": s3_pdf_key})
                except Exception as e:
                    logger.warning(f"Textract extraction failed: {e}, continuing with Claude-only")
                    use_textract_for_holdings = False
            else:
                logger.info("Step 3: Skipping Textract (using Claude-only for holdings)")

            # Step 4: Claude Batch Processing
            logger.info("Step 4: Claude batch processing")
            await self.db.update_filing_status(filing_id, FILING_STATUS["PARSING"])

            async with ClaudeBatchProcessor() as claude:
                batch_requests = []

                # Always: Cover page metadata
                batch_requests.append({
                    "custom_id": "cover_page_metadata",
                    "prompt": PromptTemplates.cover_page_metadata(html[:3000]),
                })

                # Always: Footnotes
                footnotes_html = self._extract_footnotes_section(html)
                if footnotes_html:
                    batch_requests.append({
                        "custom_id": "footnotes",
                        "prompt": PromptTemplates.footnotes(footnotes_html[:10000]),
                    })

                # Always: Notes to Financial Statements
                notes_html = self._extract_notes_section(html)
                if notes_html:
                    batch_requests.append({
                        "custom_id": "notes_financial_statements",
                        "prompt": PromptTemplates.notes_financial_statements(notes_html[:20000]),
                    })

                # Financial statements (from Textract if hybrid/textract_primary, else from HTML via Claude)
                if financial_tables:
                    tables_text = json.dumps(financial_tables, indent=2)
                    batch_requests.append({
                        "custom_id": "financial_statements",
                        "prompt": PromptTemplates.financial_statements(tables_text[:15000]),
                    })

                # Holdings extraction strategy
                if use_textract_for_holdings and schedule_rows:
                    # Textract primary: use Claude for validation only
                    batch_requests.append({
                        "custom_id": "schedule_validation",
                        "prompt": PromptTemplates.schedule_validation(
                            json.dumps(schedule_rows[:100], indent=2),
                            json.dumps(subtotals, indent=2),
                            json.dumps(section_headers, indent=2),
                        ),
                    })
                else:
                    # Claude-only or hybrid: Claude extracts holdings directly from HTML
                    batch_requests.append({
                        "custom_id": "full_schedule_extraction",
                        "prompt": PromptTemplates.full_schedule_extraction(html),
                    })

                # Process batch
                from claude.batch import BatchRequest
                requests = [
                    BatchRequest(custom_id=r["custom_id"], prompt=r["prompt"])
                    for r in batch_requests
                ]

                responses = await claude.process_batch_with_retry(requests)

            # Step 5: Parse & Map
            logger.info("Step 5: Parsing and mapping")

            # Parse metadata
            metadata = None
            if "cover_page_metadata" in responses and responses["cover_page_metadata"]:
                metadata = ClaudeResponseParser.parse_metadata(responses["cover_page_metadata"])
                if metadata.fund_name:
                    await self.db.update_filing(filing_id, {
                        "fund_name": metadata.fund_name,
                        "manager_name": metadata.manager_name,
                        "manager_address": metadata.manager_address,
                        "fiscal_year_end": metadata.fiscal_year_end,
                        "period_label": metadata.period_label,
                    })

            # Parse footnotes
            footnotes_list = []
            footnotes_dict = {}
            if "footnotes" in responses and responses["footnotes"]:
                footnotes_list = ClaudeResponseParser.parse_footnotes(responses["footnotes"])
                footnotes_dict = {f.footnote_id: f.text for f in footnotes_list}

            # Parse notes
            notes_data = {}
            if "notes_financial_statements" in responses and responses["notes_financial_statements"]:
                notes_data = ClaudeResponseParser.parse_notes_financial_statements(
                    responses["notes_financial_statements"]
                )

            # Parse financial statements
            fin_statements = {}
            if "financial_statements" in responses and responses["financial_statements"]:
                fin_statements = ClaudeResponseParser.parse_financial_statements(
                    responses["financial_statements"]
                )

            # Parse holdings based on strategy
            validation_data = None
            if "schedule_validation" in responses and responses["schedule_validation"]:
                validation_data = ClaudeResponseParser.parse_validation(
                    responses["schedule_validation"]
                )

            if "full_schedule_extraction" in responses and responses["full_schedule_extraction"]:
                claude_schedule = ClaudeResponseParser.parse_full_schedule(
                    responses["full_schedule_extraction"]
                )
                schedule_rows = claude_schedule["holdings"]
                subtotals = claude_schedule["subtotals"]
                logger.info(f"Claude extracted {len(schedule_rows)} holdings")

            # Map holdings
            validation_results = validation_data.misaligned_rows if validation_data else None
            mapped_holdings = DataMapper.map_holdings_batch(
                schedule_rows,
                validation_results,
                section_headers,
            )

            # Step 6: Validate & Score
            logger.info("Step 6: Validating and scoring")

            await self.db.update_filing_status(filing_id, FILING_STATUS["VALIDATING"])

            validation_result = DataValidator.validate_all(
                holdings=mapped_holdings,
                financial_statements=fin_statements,
                footnotes=footnotes_dict,
                metadata_period=filing_data.get("report_period_end"),
                edgar_period=None,  # TODO: Get from EDGAR API
            )

            # Step 7: Write to Supabase
            logger.info("Step 7: Writing to database")

            # Write holdings
            await self.db.write_holdings(
                filing_id=filing_id,
                holdings=mapped_holdings,
                cik=cik,
                fund_name=filing_data.get("fund_name") or metadata.fund_name if metadata else "Unknown",
                report_period_end=filing_data.get("report_period_end"),
            )

            # Write section subtotals
            for subtotal in subtotals:
                await self.db.write_section_subtotal(
                    filing_id=filing_id,
                    section_name=subtotal.get("section_name") or subtotal.get("holding_name", ""),
                    total_cost=subtotal.get("cost") or subtotal.get("total_cost"),
                    total_fair_value=subtotal.get("fair_value") or subtotal.get("total_fair_value"),
                    pct_of_nav=subtotal.get("section_pct_of_nav") or subtotal.get("pct_of_nav"),
                )

            # Write footnotes
            for fn in footnotes_list:
                await self.db.write_footnote(
                    filing_id=filing_id,
                    footnote_id=fn.footnote_id,
                    text=fn.text,
                    is_foreign_currency=fn.is_foreign_currency,
                    is_restricted_security=fn.is_restricted_security,
                    is_continuation_fund=fn.is_continuation_fund,
                    is_fair_value_methodology=fn.is_fair_value_methodology,
                )

            # Write financial statements
            if fin_statements:
                # Add credit facility from notes
                if notes_data.get("credit_facility"):
                    cf = notes_data["credit_facility"]
                    fin_statements.update({
                        "credit_facility_lender": cf.lender,
                        "credit_facility_commitment": cf.commitment_amount,
                        "credit_facility_outstanding": cf.outstanding_balance,
                        "credit_facility_rate": cf.interest_rate,
                        "credit_facility_maturity": cf.maturity_date,
                    })

                await self.db.write_financial_statements(filing_id, fin_statements)

            # Write unfunded commitments
            for uc in notes_data.get("unfunded_commitments", []):
                await self.db.write_unfunded_commitment(
                    filing_id=filing_id,
                    holding_name=uc.holding_name,
                    unfunded_commitment_usd=uc.unfunded_commitment_usd,
                )

            # Update filing with final status
            warnings_json = [
                {"code": w.code, "message": w.message, "severity": w.severity}
                for w in validation_result.warnings
            ]

            await self.db.update_filing(filing_id, {
                "status": FILING_STATUS["OK"],
                "confidence_score": validation_result.confidence_score,
                "extraction_warnings": json.dumps(warnings_json),
            })

            logger.info(
                "Pipeline complete",
                filing_id=filing_id,
                holdings_count=len(mapped_holdings),
                confidence_score=validation_result.confidence_score,
            )

            return PipelineResult(
                filing_id=filing_id,
                success=True,
                holdings_count=len(mapped_holdings),
                confidence_score=validation_result.confidence_score,
                warnings=[w.message for w in validation_result.warnings],
            )

        except Exception as e:
            logger.error("Pipeline failed", url=url, error=str(e), exc_info=True)

            if filing_id:
                await self.db.update_filing(filing_id, {
                    "status": FILING_STATUS["FAILED"],
                    "error_message": str(e),
                })

            return PipelineResult(
                filing_id=filing_id or "unknown",
                success=False,
                error=str(e),
            )

    def _extract_footnotes_section(self, html: str) -> Optional[str]:
        """Extract the footnotes section from HTML."""
        import re

        # Look for footnotes after Schedule of Investments
        schedule_idx = html.lower().find("schedule of investments")
        if schedule_idx == -1:
            return None

        # Look for a section with numbered footnotes
        section = html[schedule_idx:schedule_idx + 100000]

        # Find footnote patterns
        footnote_patterns = [
            r"<p[^>]*>\s*\(\d+\)\s*.+?</p>",
            r"<td[^>]*>\s*\*+\s*.+?</td>",
            r"<div[^>]*>\s*\(\d+\)\s*.+?</div>",
        ]

        for pattern in footnote_patterns:
            matches = re.findall(pattern, section, re.IGNORECASE | re.DOTALL)
            if len(matches) >= 2:
                return "\n".join(matches)

        return None

    def _extract_notes_section(self, html: str) -> Optional[str]:
        """Extract Notes to Financial Statements section."""
        import re

        # Look for Notes to Financial Statements header
        match = re.search(
            r"notes\s+to\s+(the\s+)?(consolidated\s+)?financial\s+statements",
            html,
            re.IGNORECASE,
        )

        if not match:
            return None

        start_idx = match.start()

        # Find end (usually at next major section or end of document)
        end_patterns = [
            r"report\s+of\s+independent",
            r"board\s+of\s+(directors|trustees)",
            r"additional\s+information",
            r"proxy\s+voting",
        ]

        end_idx = len(html)
        for pattern in end_patterns:
            end_match = re.search(pattern, html[start_idx + 1000:], re.IGNORECASE)
            if end_match:
                possible_end = start_idx + 1000 + end_match.start()
                if possible_end < end_idx:
                    end_idx = possible_end

        return html[start_idx:end_idx]

    async def run_batch(
        self,
        urls: List[str],
        concurrency: int = 3,
    ) -> List[PipelineResult]:
        """
        Run pipeline on multiple filings with controlled concurrency.

        Args:
            urls: List of filing URLs
            concurrency: Maximum concurrent processing

        Returns:
            List of PipelineResult objects
        """
        semaphore = asyncio.Semaphore(concurrency)

        async def process_with_semaphore(url: str) -> PipelineResult:
            async with semaphore:
                return await self.run_filing(url)

        tasks = [process_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to PipelineResult
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(PipelineResult(
                    filing_id="unknown",
                    success=False,
                    error=str(result),
                ))
            else:
                processed_results.append(result)

        return processed_results
