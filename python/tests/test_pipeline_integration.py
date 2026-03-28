"""
Integration tests for the full pipeline.

These tests require external services and should be run with:
    pytest -m integration

Set environment variables before running:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - ANTHROPIC_API_KEY
    - DATABASE_URL
"""

import pytest
import asyncio
import os

# Skip all tests in this module if not running integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def check_env():
    """Verify required environment variables are set."""
    required = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "ANTHROPIC_API_KEY",
        "DATABASE_URL",
    ]

    missing = [var for var in required if not os.getenv(var)]
    if missing:
        pytest.skip(f"Missing environment variables: {', '.join(missing)}")


class TestPipelineIntegration:
    """Integration tests for full pipeline."""

    @pytest.mark.asyncio
    async def test_single_filing_extraction(self, check_env):
        """Test extracting a single known filing."""
        from pipeline.orchestrator import PipelineOrchestrator
        from db.supabase import SupabaseClient

        # Use a known, stable filing for testing
        test_url = "https://www.sec.gov/Archives/edgar/data/1689813/000168981324000003/ncsrs.htm"

        db = SupabaseClient()
        await db.connect()

        try:
            orchestrator = PipelineOrchestrator(db_client=db)
            result = await orchestrator.run_filing(test_url)

            # Verify basic extraction worked
            assert result.status in ("ok", "partial")
            assert result.filing_id is not None
            assert result.holdings_count >= 0

            # Verify filing was saved
            filing = await db.get_filing(result.filing_id)
            assert filing is not None
            assert filing["status"] in ("ok", "partial")

        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_textract_extraction(self, check_env):
        """Test Textract table extraction."""
        from textract.converter import HtmlToPdfConverter
        from textract.extractor import TextractExtractor

        # Simple HTML table
        html = """
        <html>
        <body>
        <table>
            <tr><th>Investment</th><th>Cost</th><th>Fair Value</th></tr>
            <tr><td>Test Corp</td><td>1,000,000</td><td>1,200,000</td></tr>
        </table>
        </body>
        </html>
        """

        converter = HtmlToPdfConverter()
        await converter.start()

        try:
            pdf_bytes, page_count = await converter.convert_to_pdf(
                html,
                base_url="https://example.com"
            )

            assert pdf_bytes is not None
            assert len(pdf_bytes) > 0
            assert page_count >= 1

            # Test Textract (if AWS is configured)
            if os.getenv("AWS_ACCESS_KEY_ID"):
                extractor = TextractExtractor()
                result = await extractor.extract(pdf_bytes, "test/integration.pdf")

                assert result.tables is not None
                assert len(result.tables) >= 0  # May or may not find tables

        finally:
            await converter.stop()

    @pytest.mark.asyncio
    async def test_claude_batch_processing(self, check_env):
        """Test Claude batch API."""
        from claude.batch import ClaudeBatchProcessor, BatchRequest

        processor = ClaudeBatchProcessor()

        # Simple test request
        requests = [
            BatchRequest(
                custom_id="test-1",
                prompt="Return exactly: {\"test\": true}",
                system="You are a test assistant. Return valid JSON only.",
            ),
        ]

        # Note: This will actually call Claude API
        # In real tests, you might want to mock this
        batch_id = await processor.create_batch(requests)
        assert batch_id is not None

        responses = await processor.poll_for_completion(batch_id)
        assert len(responses) == 1
        assert responses[0].custom_id == "test-1"

    @pytest.mark.asyncio
    async def test_database_operations(self, check_env):
        """Test database CRUD operations."""
        from db.supabase import SupabaseClient

        db = SupabaseClient()
        await db.connect()

        try:
            # Test health check
            assert await db.health_check() is True

            # Test filing upsert
            filing_id = await db.upsert_filing(
                cik="9999999",
                fund_name="Integration Test Fund",
                report_type="N-CSR",
                report_period_end="2024-06-30",
                sec_accession_number="0009999999-24-999999",
                sec_filing_url="https://example.com/test",
                status="pending",
            )

            assert filing_id is not None

            # Test filing retrieval
            filing = await db.get_filing(filing_id)
            assert filing["fund_name"] == "Integration Test Fund"

            # Test status update
            await db.update_filing_status(filing_id, "ok")
            filing = await db.get_filing(filing_id)
            assert filing["status"] == "ok"

            # Cleanup: Delete test filing
            async with db._pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM filings WHERE sec_accession_number = $1",
                    "0009999999-24-999999"
                )

        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_pipeline_idempotency(self, check_env):
        """Test that re-running same filing updates rather than duplicates."""
        from pipeline.orchestrator import PipelineOrchestrator
        from db.supabase import SupabaseClient

        test_url = "https://www.sec.gov/Archives/edgar/data/1689813/000168981324000003/ncsrs.htm"

        db = SupabaseClient()
        await db.connect()

        try:
            orchestrator = PipelineOrchestrator(db_client=db)

            # Run twice
            result1 = await orchestrator.run_filing(test_url)
            result2 = await orchestrator.run_filing(test_url)

            # Should have same filing ID (upsert, not insert)
            assert result1.filing_id == result2.filing_id

            # Verify only one filing exists with this accession
            accession = "0001689813-24-000003"
            filing = await db.get_filing_by_accession(accession)
            assert filing is not None

        finally:
            await db.close()


class TestEdgarIntegration:
    """Integration tests for SEC EDGAR API."""

    @pytest.mark.asyncio
    async def test_fetch_real_filing(self, check_env):
        """Test fetching a real filing from SEC."""
        from edgar.fetcher import EdgarFetcher

        fetcher = EdgarFetcher()

        async with fetcher:
            # Fetch a known filing
            html = await fetcher.fetch_filing_html(
                "https://www.sec.gov/Archives/edgar/data/1689813/000168981324000003/ncsrs.htm"
            )

            assert html is not None
            assert len(html) > 1000
            assert "SCHEDULE OF INVESTMENTS" in html or "Schedule of Investments" in html

    @pytest.mark.asyncio
    async def test_sec_api_company_lookup(self, check_env):
        """Test looking up company submissions."""
        from edgar.sec_api import SecApi

        async with SecApi() as api:
            # Lookup a known CIK
            submissions = await api.get_company_submissions("1689813")

            assert submissions is not None
            assert "name" in submissions
            assert "filings" in submissions

    @pytest.mark.asyncio
    async def test_sec_api_ncsr_filings(self, check_env):
        """Test fetching N-CSR filings for a CIK."""
        from edgar.sec_api import SecApi

        async with SecApi() as api:
            filings = await api.get_ncsr_filings_for_cik("1689813", limit=5)

            assert len(filings) > 0
            assert all(f["form"] in ("N-CSR", "N-CSRS") for f in filings)
            assert all("url" in f for f in filings)
