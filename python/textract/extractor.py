"""
AWS Textract Table Extractor
Extracts structured table data from PDFs using AWS Textract.
"""

import asyncio
import re
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

import boto3
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import get_settings

logger = structlog.get_logger()


@dataclass
class TableCell:
    """Represents a cell in a Textract table."""

    row_index: int
    col_index: int
    text: str
    confidence: float
    row_span: int = 1
    col_span: int = 1


@dataclass
class TableRow:
    """Represents a row in a Textract table."""

    row_index: int
    cells: List[TableCell] = field(default_factory=list)

    @property
    def cell_texts(self) -> List[str]:
        return [c.text for c in sorted(self.cells, key=lambda x: x.col_index)]


@dataclass
class ExtractedTable:
    """Represents an extracted table."""

    table_index: int
    page_number: int
    headers: List[str]
    rows: List[TableRow]
    table_type: Optional[str] = None  # schedule, balance_sheet, operations, etc.


@dataclass
class ExtractionResult:
    """Result of Textract extraction."""

    schedule_rows: List[Dict[str, Any]]
    subtotals: List[Dict[str, Any]]
    section_headers: List[Dict[str, Any]]
    financial_tables: Dict[str, List[Dict[str, Any]]]
    raw_tables: List[ExtractedTable]


class TextractExtractor:
    """Extracts tables from PDFs using AWS Textract."""

    # Column header patterns for Schedule of Investments
    SCHEDULE_HEADERS = [
        "fund investments",
        "investment",
        "description",
        "cost",
        "fair value",
        "value",
        "footnotes",
        "notes",
        "investment type",
        "investment purpose",
        "geographic region",
        "acquisition date",
        "maturity date",
    ]

    # Financial statement table identifiers
    FIN_STATEMENT_PATTERNS = {
        "balance_sheet": [
            "statement of assets and liabilities",
            "assets and liabilities",
            "balance sheet",
        ],
        "operations": [
            "statement of operations",
            "operations",
            "income statement",
        ],
        "changes": [
            "statement of changes in net assets",
            "changes in net assets",
        ],
        "cash_flows": [
            "statement of cash flows",
            "cash flows",
        ],
        "highlights": [
            "financial highlights",
            "per share data",
        ],
    }

    def __init__(self):
        self.settings = get_settings()
        self._textract = boto3.client(
            "textract",
            aws_access_key_id=self.settings.aws_access_key_id,
            aws_secret_access_key=self.settings.aws_secret_access_key,
            region_name=self.settings.aws_region,
        )
        self._s3 = boto3.client(
            "s3",
            aws_access_key_id=self.settings.aws_access_key_id,
            aws_secret_access_key=self.settings.aws_secret_access_key,
            region_name=self.settings.aws_region,
        )

    async def upload_to_s3(self, pdf_bytes: bytes, s3_key: str) -> str:
        """Upload PDF to S3 for Textract processing."""
        logger.info("Uploading PDF to S3", bucket=self.settings.aws_s3_bucket, key=s3_key)

        # Run in executor since boto3 is not async
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._s3.put_object(
                Bucket=self.settings.aws_s3_bucket,
                Key=s3_key,
                Body=pdf_bytes,
                ContentType="application/pdf",
            ),
        )

        return f"s3://{self.settings.aws_s3_bucket}/{s3_key}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=8),
    )
    async def start_analysis(self, s3_key: str) -> str:
        """
        Start async Textract analysis job.

        Returns:
            Job ID
        """
        logger.info("Starting Textract analysis", s3_key=s3_key)

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self._textract.start_document_analysis(
                DocumentLocation={
                    "S3Object": {
                        "Bucket": self.settings.aws_s3_bucket,
                        "Name": s3_key,
                    }
                },
                FeatureTypes=["TABLES"],
            ),
        )

        job_id = response["JobId"]
        logger.info("Textract job started", job_id=job_id)

        return job_id

    async def poll_for_completion(self, job_id: str) -> Dict[str, Any]:
        """
        Poll Textract job until completion.

        Returns:
            Full Textract response
        """
        logger.info("Polling Textract job", job_id=job_id)

        loop = asyncio.get_event_loop()

        while True:
            response = await loop.run_in_executor(
                None,
                lambda: self._textract.get_document_analysis(JobId=job_id),
            )

            status = response["JobStatus"]
            logger.info("Textract job status", job_id=job_id, status=status)

            if status == "SUCCEEDED":
                # Get all pages of results
                all_blocks = response.get("Blocks", [])
                next_token = response.get("NextToken")

                while next_token:
                    response = await loop.run_in_executor(
                        None,
                        lambda: self._textract.get_document_analysis(
                            JobId=job_id, NextToken=next_token
                        ),
                    )
                    all_blocks.extend(response.get("Blocks", []))
                    next_token = response.get("NextToken")

                return {"Blocks": all_blocks, "JobStatus": "SUCCEEDED"}

            elif status == "FAILED":
                error = response.get("StatusMessage", "Unknown error")
                logger.error("Textract job failed", job_id=job_id, error=error)
                raise Exception(f"Textract job failed: {error}")

            # Wait before polling again
            await asyncio.sleep(self.settings.batch_poll_interval_seconds)

    def _parse_blocks_to_tables(self, blocks: List[Dict]) -> List[ExtractedTable]:
        """Parse Textract blocks into structured tables."""
        tables = []

        # Build block ID to block mapping
        block_map = {b["Id"]: b for b in blocks}

        # Find all TABLE blocks
        table_blocks = [b for b in blocks if b["BlockType"] == "TABLE"]

        for table_idx, table_block in enumerate(table_blocks):
            page_num = table_block.get("Page", 1)
            rows: Dict[int, TableRow] = {}

            # Get cells for this table
            if "Relationships" not in table_block:
                continue

            for rel in table_block["Relationships"]:
                if rel["Type"] != "CHILD":
                    continue

                for cell_id in rel["Ids"]:
                    cell_block = block_map.get(cell_id)
                    if not cell_block or cell_block["BlockType"] != "CELL":
                        continue

                    row_idx = cell_block.get("RowIndex", 1)
                    col_idx = cell_block.get("ColumnIndex", 1)

                    # Get cell text from child WORD blocks
                    cell_text = ""
                    if "Relationships" in cell_block:
                        for cell_rel in cell_block["Relationships"]:
                            if cell_rel["Type"] == "CHILD":
                                for word_id in cell_rel["Ids"]:
                                    word_block = block_map.get(word_id)
                                    if word_block and word_block["BlockType"] in (
                                        "WORD",
                                        "SELECTION_ELEMENT",
                                    ):
                                        cell_text += word_block.get("Text", "") + " "

                    cell = TableCell(
                        row_index=row_idx,
                        col_index=col_idx,
                        text=cell_text.strip(),
                        confidence=cell_block.get("Confidence", 0),
                        row_span=cell_block.get("RowSpan", 1),
                        col_span=cell_block.get("ColumnSpan", 1),
                    )

                    if row_idx not in rows:
                        rows[row_idx] = TableRow(row_index=row_idx)
                    rows[row_idx].cells.append(cell)

            # Sort rows and extract headers
            sorted_rows = [rows[i] for i in sorted(rows.keys())]
            headers = sorted_rows[0].cell_texts if sorted_rows else []

            table = ExtractedTable(
                table_index=table_idx,
                page_number=page_num,
                headers=headers,
                rows=sorted_rows,
            )

            # Classify table type
            table.table_type = self._classify_table(table)

            tables.append(table)

        return tables

    def _classify_table(self, table: ExtractedTable) -> Optional[str]:
        """Classify table type based on headers and content."""
        header_text = " ".join(table.headers).lower()

        # Check if it's a Schedule of Investments
        schedule_signals = sum(
            1 for h in self.SCHEDULE_HEADERS if h in header_text
        )
        if schedule_signals >= 2:
            return "schedule"

        # Check financial statement types
        for stmt_type, patterns in self.FIN_STATEMENT_PATTERNS.items():
            for pattern in patterns:
                if pattern in header_text:
                    return stmt_type

        return None

    def _is_section_header(self, row: TableRow) -> Optional[Dict[str, Any]]:
        """Check if row is a section header like 'Primary Investments - 2.3%'."""
        texts = row.cell_texts
        if len(texts) < 1:
            return None

        first_cell = texts[0]

        # Pattern: "Section Name - X.X%"
        match = re.match(
            r"^([\w\s]+(?:Investments?|Fund|Securities))\s*[-–]\s*([\d.]+)\s*%?\s*$",
            first_cell,
            re.IGNORECASE,
        )
        if match:
            return {
                "section_name": match.group(1).strip(),
                "pct_of_nav": float(match.group(2)),
            }

        return None

    def _is_subtotal_row(self, row: TableRow) -> bool:
        """Check if row is a subtotal row."""
        first_cell = row.cell_texts[0] if row.cell_texts else ""
        return any(
            kw in first_cell.lower()
            for kw in ["total", "subtotal", "net investments"]
        )

    def _parse_schedule_row(
        self, row: TableRow, headers: List[str]
    ) -> Optional[Dict[str, Any]]:
        """Parse a schedule row into a holding dict."""
        cells = row.cell_texts

        if len(cells) < 2:
            return None

        # Map cells to fields based on headers
        result = {"raw_row": cells}

        header_lower = [h.lower() for h in headers]

        for i, cell in enumerate(cells):
            if i >= len(header_lower):
                continue

            header = header_lower[i]
            value = cell.strip()

            if not value or value == "-":
                continue

            if "investment" in header or "description" in header or "fund" in header:
                result["holding_name"] = value
            elif "cost" in header:
                result["cost"] = self._parse_money(value)
            elif "fair" in header or "value" in header:
                result["fair_value"] = self._parse_money(value)
            elif "footnote" in header or "note" in header:
                result["footnote_refs"] = self._parse_footnotes(value)
            elif "type" in header:
                result["investment_type"] = value
            elif "purpose" in header:
                result["investment_purpose"] = value
            elif "region" in header or "geographic" in header:
                result["geographic_region"] = value
            elif "acquisition" in header:
                result["acquisition_date"] = value
            elif "maturity" in header:
                result["maturity_date"] = value

        # Must have at least a name and value
        if "holding_name" not in result:
            return None
        if "fair_value" not in result and "cost" not in result:
            return None

        return result

    def _parse_money(self, value: str) -> Optional[float]:
        """Parse monetary value string to float."""
        if not value:
            return None

        # Handle parentheses for negative
        is_negative = "(" in value and ")" in value

        # Remove non-numeric except . and ,
        cleaned = re.sub(r"[^\d.,]", "", value)

        # Handle thousands separator
        cleaned = cleaned.replace(",", "")

        try:
            result = float(cleaned)
            return -result if is_negative else result
        except ValueError:
            return None

    def _parse_footnotes(self, value: str) -> List[str]:
        """Parse footnote references from a cell."""
        # Match patterns like (3), (14), *, **, (a), [1]
        refs = []

        # Numbered in parens: (3), (14)
        refs.extend(re.findall(r"\((\d+)\)", value))

        # Letters in parens: (a), (b)
        refs.extend(re.findall(r"\(([a-zA-Z])\)", value))

        # Asterisks
        asterisks = re.findall(r"(\*+)", value)
        refs.extend(asterisks)

        # Bracketed: [1], [a]
        refs.extend(re.findall(r"\[(\w+)\]", value))

        return list(set(refs))

    async def extract(self, pdf_bytes: bytes, s3_key: str) -> ExtractionResult:
        """
        Run full extraction pipeline.

        Args:
            pdf_bytes: PDF content
            s3_key: S3 key to upload PDF to

        Returns:
            ExtractionResult with parsed tables
        """
        # Upload to S3
        await self.upload_to_s3(pdf_bytes, s3_key)

        # Start Textract job
        job_id = await self.start_analysis(s3_key)

        # Poll for completion
        response = await self.poll_for_completion(job_id)

        # Parse blocks into tables
        blocks = response.get("Blocks", [])
        tables = self._parse_blocks_to_tables(blocks)

        logger.info("Parsed tables from Textract", table_count=len(tables))

        # Categorize tables and extract data
        schedule_rows = []
        subtotals = []
        section_headers = []
        financial_tables = {
            "balance_sheet": [],
            "operations": [],
            "changes": [],
            "cash_flows": [],
            "highlights": [],
        }

        for table in tables:
            if table.table_type == "schedule":
                # Parse schedule table
                for row in table.rows[1:]:  # Skip header row
                    # Check for section header
                    section = self._is_section_header(row)
                    if section:
                        section_headers.append(section)
                        continue

                    # Check for subtotal
                    if self._is_subtotal_row(row):
                        parsed = self._parse_schedule_row(row, table.headers)
                        if parsed:
                            subtotals.append(parsed)
                        continue

                    # Parse as regular holding row
                    parsed = self._parse_schedule_row(row, table.headers)
                    if parsed:
                        schedule_rows.append(parsed)

            elif table.table_type in financial_tables:
                # Store financial statement rows
                for row in table.rows:
                    financial_tables[table.table_type].append(
                        {"cells": row.cell_texts}
                    )

        logger.info(
            "Extraction complete",
            schedule_rows=len(schedule_rows),
            subtotals=len(subtotals),
            section_headers=len(section_headers),
        )

        return ExtractionResult(
            schedule_rows=schedule_rows,
            subtotals=subtotals,
            section_headers=section_headers,
            financial_tables=financial_tables,
            raw_tables=tables,
        )
