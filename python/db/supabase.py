"""
Supabase/PostgreSQL Client
Direct database access for bulk operations.
"""

import json
from typing import Dict, Any, List, Optional
from datetime import date, datetime

import asyncpg
import structlog

from config import get_settings

logger = structlog.get_logger()


class SupabaseClient:
    """
    Direct PostgreSQL client for Supabase.

    Uses asyncpg for async database operations.
    All writes are wrapped in transactions for consistency.
    """

    def __init__(self):
        self.settings = get_settings()
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Create connection pool."""
        logger.info("Connecting to database")

        self._pool = await asyncpg.create_pool(
            self.settings.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )

        logger.info("Database connected")

    async def close(self):
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("Database connection closed")

    async def health_check(self) -> bool:
        """Check database connectivity."""
        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error("Database health check failed", error=str(e))
            return False

    # =========================================================================
    # FILINGS
    # =========================================================================

    async def upsert_filing(
        self,
        cik: str,
        fund_name: str,
        report_type: str,
        report_period_end: str,
        sec_accession_number: str,
        sec_filing_url: str,
        status: str = "pending",
    ) -> str:
        """
        Create or update a filing record.

        Returns:
            Filing ID
        """
        # Parse date
        if isinstance(report_period_end, str):
            try:
                report_period_end = datetime.strptime(report_period_end, "%Y-%m-%d").date()
            except ValueError:
                report_period_end = date.today()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO filings (
                    cik, fund_name, report_type, report_period_end,
                    sec_accession_number, sec_filing_url, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (sec_accession_number) DO UPDATE SET
                    fund_name = EXCLUDED.fund_name,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING id
                """,
                cik, fund_name, report_type, report_period_end,
                sec_accession_number, sec_filing_url, status,
            )

            return str(row["id"])

    async def update_filing_status(self, filing_id: str, status: str):
        """Update filing status."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE filings SET status = $1, updated_at = NOW() WHERE id = $2",
                status, filing_id,
            )

    async def update_filing(self, filing_id: str, updates: Dict[str, Any]):
        """Update filing fields."""
        if not updates:
            return

        # Build SET clause
        set_parts = []
        values = []
        for i, (key, value) in enumerate(updates.items(), start=1):
            set_parts.append(f"{key} = ${i}")
            values.append(value)

        values.append(filing_id)
        set_clause = ", ".join(set_parts)

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"UPDATE filings SET {set_clause}, updated_at = NOW() WHERE id = ${len(values)}",
                *values,
            )

    async def get_filing(self, filing_id: str) -> Optional[Dict[str, Any]]:
        """Get filing by ID."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM filings WHERE id = $1",
                filing_id,
            )
            return dict(row) if row else None

    async def get_filing_by_accession(self, accession_number: str) -> Optional[Dict[str, Any]]:
        """Get filing by accession number."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM filings WHERE sec_accession_number = $1",
                accession_number,
            )
            return dict(row) if row else None

    # =========================================================================
    # HOLDINGS
    # =========================================================================

    async def write_holdings(
        self,
        filing_id: str,
        holdings: List[Any],  # List[MappedHolding]
        cik: str,
        fund_name: str,
        report_period_end: str,
    ):
        """
        Write holdings for a filing.

        Deletes existing holdings and inserts new ones in a transaction.
        """
        if isinstance(report_period_end, str):
            try:
                report_period_end = datetime.strptime(report_period_end, "%Y-%m-%d").date()
            except ValueError:
                report_period_end = date.today()

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Delete existing holdings
                await conn.execute(
                    "DELETE FROM holdings WHERE filing_id = $1",
                    filing_id,
                )

                # Insert new holdings
                for h in holdings:
                    await conn.execute(
                        """
                        INSERT INTO holdings (
                            filing_id, cik, fund_name, report_period_end,
                            holding_name, footnote_refs, investment_type,
                            investment_purpose, geographic_region, position,
                            reference_rate_spread, maturity_date, acquisition_date,
                            cost, fair_value, moic, is_restricted,
                            section_name, section_pct_of_nav,
                            raw_textract_row, extraction_source, confidence
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, $18, $19,
                            $20, $21, $22
                        )
                        """,
                        filing_id, cik, fund_name, report_period_end,
                        h.holding_name, h.footnote_refs, h.investment_type,
                        h.investment_purpose, h.geographic_region, h.position,
                        h.reference_rate_spread, h.maturity_date, h.acquisition_date,
                        h.cost, h.fair_value, h.moic, h.is_restricted,
                        h.section_name, h.section_pct_of_nav,
                        json.dumps(h.raw_textract_row) if h.raw_textract_row else None,
                        h.extraction_source, h.confidence,
                    )

        logger.info("Holdings written", filing_id=filing_id, count=len(holdings))

    # =========================================================================
    # SECTION SUBTOTALS
    # =========================================================================

    async def write_section_subtotal(
        self,
        filing_id: str,
        section_name: str,
        total_cost: Optional[float],
        total_fair_value: Optional[float],
        pct_of_nav: Optional[float],
    ):
        """Write a section subtotal."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO section_subtotals (
                    filing_id, section_name, total_cost, total_fair_value, pct_of_nav
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                filing_id, section_name, total_cost, total_fair_value, pct_of_nav,
            )

    # =========================================================================
    # FOOTNOTES
    # =========================================================================

    async def write_footnote(
        self,
        filing_id: str,
        footnote_id: str,
        text: str,
        is_foreign_currency: bool = False,
        is_restricted_security: bool = False,
        is_continuation_fund: bool = False,
        is_fair_value_methodology: bool = False,
    ):
        """Write a footnote."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO filing_footnotes (
                    filing_id, footnote_id, text,
                    is_foreign_currency, is_restricted_security,
                    is_continuation_fund, is_fair_value_methodology
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (filing_id, footnote_id) DO UPDATE SET
                    text = EXCLUDED.text,
                    is_foreign_currency = EXCLUDED.is_foreign_currency,
                    is_restricted_security = EXCLUDED.is_restricted_security,
                    is_continuation_fund = EXCLUDED.is_continuation_fund,
                    is_fair_value_methodology = EXCLUDED.is_fair_value_methodology
                """,
                filing_id, footnote_id, text,
                is_foreign_currency, is_restricted_security,
                is_continuation_fund, is_fair_value_methodology,
            )

    # =========================================================================
    # FINANCIAL STATEMENTS
    # =========================================================================

    async def write_financial_statements(
        self,
        filing_id: str,
        data: Dict[str, Any],
    ):
        """Write financial statement data."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO financial_statements (
                    filing_id,
                    investments_at_fair_value, cash_and_equivalents, total_assets,
                    credit_facility_borrowings, management_fees_payable,
                    incentive_fees_payable, redemptions_payable,
                    total_liabilities, net_assets,
                    total_investment_income, management_fees, incentive_fees,
                    interest_expense, total_expenses, net_investment_income_loss,
                    net_realized_gain_loss, net_change_unrealized,
                    net_increase_from_operations,
                    capital_contributions, capital_distributions,
                    net_assets_beginning, net_assets_end,
                    purchases_of_investments, proceeds_from_realizations,
                    proceeds_from_borrowings, repayment_of_borrowings,
                    net_change_in_cash,
                    credit_facility_lender, credit_facility_commitment,
                    credit_facility_outstanding, credit_facility_rate,
                    credit_facility_maturity,
                    raw_assets_liabilities, raw_operations, raw_cash_flows
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19,
                    $20, $21, $22, $23, $24, $25, $26, $27, $28,
                    $29, $30, $31, $32, $33, $34, $35, $36
                )
                ON CONFLICT (filing_id) DO UPDATE SET
                    investments_at_fair_value = EXCLUDED.investments_at_fair_value,
                    cash_and_equivalents = EXCLUDED.cash_and_equivalents,
                    total_assets = EXCLUDED.total_assets,
                    credit_facility_borrowings = EXCLUDED.credit_facility_borrowings,
                    management_fees_payable = EXCLUDED.management_fees_payable,
                    incentive_fees_payable = EXCLUDED.incentive_fees_payable,
                    redemptions_payable = EXCLUDED.redemptions_payable,
                    total_liabilities = EXCLUDED.total_liabilities,
                    net_assets = EXCLUDED.net_assets,
                    total_investment_income = EXCLUDED.total_investment_income,
                    management_fees = EXCLUDED.management_fees,
                    incentive_fees = EXCLUDED.incentive_fees,
                    interest_expense = EXCLUDED.interest_expense,
                    total_expenses = EXCLUDED.total_expenses,
                    net_investment_income_loss = EXCLUDED.net_investment_income_loss,
                    net_realized_gain_loss = EXCLUDED.net_realized_gain_loss,
                    net_change_unrealized = EXCLUDED.net_change_unrealized,
                    net_increase_from_operations = EXCLUDED.net_increase_from_operations,
                    capital_contributions = EXCLUDED.capital_contributions,
                    capital_distributions = EXCLUDED.capital_distributions,
                    net_assets_beginning = EXCLUDED.net_assets_beginning,
                    net_assets_end = EXCLUDED.net_assets_end,
                    purchases_of_investments = EXCLUDED.purchases_of_investments,
                    proceeds_from_realizations = EXCLUDED.proceeds_from_realizations,
                    proceeds_from_borrowings = EXCLUDED.proceeds_from_borrowings,
                    repayment_of_borrowings = EXCLUDED.repayment_of_borrowings,
                    net_change_in_cash = EXCLUDED.net_change_in_cash,
                    credit_facility_lender = EXCLUDED.credit_facility_lender,
                    credit_facility_commitment = EXCLUDED.credit_facility_commitment,
                    credit_facility_outstanding = EXCLUDED.credit_facility_outstanding,
                    credit_facility_rate = EXCLUDED.credit_facility_rate,
                    credit_facility_maturity = EXCLUDED.credit_facility_maturity,
                    raw_assets_liabilities = EXCLUDED.raw_assets_liabilities,
                    raw_operations = EXCLUDED.raw_operations,
                    raw_cash_flows = EXCLUDED.raw_cash_flows
                """,
                filing_id,
                data.get("investments_at_fair_value"),
                data.get("cash_and_equivalents"),
                data.get("total_assets"),
                data.get("credit_facility_borrowings"),
                data.get("management_fees_payable"),
                data.get("incentive_fees_payable"),
                data.get("redemptions_payable"),
                data.get("total_liabilities"),
                data.get("net_assets"),
                data.get("total_investment_income"),
                data.get("management_fees"),
                data.get("incentive_fees"),
                data.get("interest_expense"),
                data.get("total_expenses"),
                data.get("net_investment_income_loss"),
                data.get("net_realized_gain_loss"),
                data.get("net_change_unrealized"),
                data.get("net_increase_from_operations"),
                data.get("capital_contributions"),
                data.get("capital_distributions"),
                data.get("net_assets_beginning"),
                data.get("net_assets_end"),
                data.get("purchases_of_investments"),
                data.get("proceeds_from_realizations"),
                data.get("proceeds_from_borrowings"),
                data.get("repayment_of_borrowings"),
                data.get("net_change_in_cash"),
                data.get("credit_facility_lender"),
                data.get("credit_facility_commitment"),
                data.get("credit_facility_outstanding"),
                data.get("credit_facility_rate"),
                data.get("credit_facility_maturity"),
                json.dumps(data.get("raw_assets_liabilities")) if data.get("raw_assets_liabilities") else None,
                json.dumps(data.get("raw_operations")) if data.get("raw_operations") else None,
                json.dumps(data.get("raw_cash_flows")) if data.get("raw_cash_flows") else None,
            )

    # =========================================================================
    # UNFUNDED COMMITMENTS
    # =========================================================================

    async def write_unfunded_commitment(
        self,
        filing_id: str,
        holding_name: str,
        unfunded_commitment_usd: float,
    ):
        """Write an unfunded commitment."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO unfunded_commitments (
                    filing_id, holding_name, unfunded_commitment_usd
                ) VALUES ($1, $2, $3)
                """,
                filing_id, holding_name, unfunded_commitment_usd,
            )

    # =========================================================================
    # PIPELINE RUNS
    # =========================================================================

    async def create_pipeline_run(
        self,
        run_type: str,
        input_params: Dict[str, Any],
    ) -> str:
        """Create a pipeline run record."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO pipeline_runs (run_type, input_params)
                VALUES ($1, $2)
                RETURNING id
                """,
                run_type, json.dumps(input_params),
            )
            return str(row["id"])

    async def update_pipeline_run(
        self,
        run_id: str,
        filings_total: int,
        filings_ok: int,
        filings_failed: int,
        run_log: List[Dict[str, Any]],
    ):
        """Update pipeline run with results."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pipeline_runs SET
                    filings_total = $1,
                    filings_ok = $2,
                    filings_failed = $3,
                    run_log = $4,
                    completed_at = NOW()
                WHERE id = $5
                """,
                filings_total, filings_ok, filings_failed,
                json.dumps(run_log), run_id,
            )

    # =========================================================================
    # QUERIES
    # =========================================================================

    async def get_filings_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get all filings with a given status."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM filings WHERE status = $1 ORDER BY created_at DESC",
                status,
            )
            return [dict(r) for r in rows]

    async def get_filings_summary(self) -> Dict[str, int]:
        """Get count of filings by status."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT status, COUNT(*) as count FROM filings GROUP BY status"
            )
            return {r["status"]: r["count"] for r in rows}
