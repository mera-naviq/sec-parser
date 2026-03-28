-- SEC Parser Elite Database Schema
-- Run this migration in Supabase SQL Editor

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- FILINGS TABLE
-- Tracks each N-CSR/N-CSRS filing processed
-- ============================================================================
CREATE TABLE IF NOT EXISTS filings (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cik                   TEXT NOT NULL,
  fund_name             TEXT NOT NULL,
  report_type           TEXT NOT NULL CHECK (report_type IN ('N-CSR', 'N-CSRS')),
  report_period_end     DATE NOT NULL,
  fiscal_year_end       TEXT,
  period_label          TEXT,
  manager_name          TEXT,
  manager_address       TEXT,
  sec_accession_number  TEXT UNIQUE NOT NULL,
  sec_filing_url        TEXT NOT NULL,
  raw_html_s3_key       TEXT,
  raw_pdf_s3_key        TEXT,
  status                TEXT NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending','fetching','converting','extracting',
                                            'parsing','validating','complete','failed')),
  error_message         TEXT,
  textract_job_id       TEXT,
  claude_batch_id       TEXT,
  confidence_score      NUMERIC(5,2),
  extraction_warnings   JSONB DEFAULT '[]',
  created_at            TIMESTAMPTZ DEFAULT NOW(),
  updated_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings(cik);
CREATE INDEX IF NOT EXISTS idx_filings_period ON filings(report_period_end);
CREATE INDEX IF NOT EXISTS idx_filings_status ON filings(status);
CREATE INDEX IF NOT EXISTS idx_filings_accession ON filings(sec_accession_number);

-- ============================================================================
-- HOLDINGS TABLE
-- One row per investment position in the Schedule of Investments
-- ============================================================================
CREATE TABLE IF NOT EXISTS holdings (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id               UUID NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
  cik                     TEXT NOT NULL,
  fund_name               TEXT NOT NULL,
  report_period_end       DATE NOT NULL,

  -- Identity
  holding_name            TEXT NOT NULL,
  footnote_refs           TEXT[],

  -- Classification
  investment_type         TEXT,
  investment_purpose      TEXT,
  geographic_region       TEXT,

  -- Debt-specific fields
  position                TEXT,
  reference_rate_spread   TEXT,
  maturity_date           DATE,

  -- Valuation
  acquisition_date        DATE,
  cost                    NUMERIC(20,2),
  fair_value              NUMERIC(20,2),
  unrealized_gain_loss    NUMERIC(20,2) GENERATED ALWAYS AS (fair_value - cost) STORED,
  moic                    NUMERIC(10,4),
  is_underwater           BOOLEAN GENERATED ALWAYS AS (fair_value < cost) STORED,
  is_restricted           BOOLEAN DEFAULT FALSE,

  -- Portfolio weight
  section_name            TEXT,
  section_pct_of_nav      NUMERIC(6,3),

  -- Raw extraction
  raw_textract_row        JSONB,
  extraction_source       TEXT CHECK (extraction_source IN ('textract','claude','manual')),
  confidence              TEXT CHECK (confidence IN ('high','medium','low')),

  created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_holdings_filing_id ON holdings(filing_id);
CREATE INDEX IF NOT EXISTS idx_holdings_cik ON holdings(cik);
CREATE INDEX IF NOT EXISTS idx_holdings_period ON holdings(report_period_end);
CREATE INDEX IF NOT EXISTS idx_holdings_investment_type ON holdings(investment_type);
CREATE INDEX IF NOT EXISTS idx_holdings_purpose ON holdings(investment_purpose);
CREATE INDEX IF NOT EXISTS idx_holdings_region ON holdings(geographic_region);
CREATE INDEX IF NOT EXISTS idx_holdings_name ON holdings(holding_name);

-- ============================================================================
-- SECTION SUBTOTALS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS section_subtotals (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id         UUID NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
  section_name      TEXT NOT NULL,
  total_cost        NUMERIC(20,2),
  total_fair_value  NUMERIC(20,2),
  pct_of_nav        NUMERIC(6,3),
  created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_section_subtotals_filing ON section_subtotals(filing_id);

-- ============================================================================
-- FILING FOOTNOTES TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS filing_footnotes (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id   UUID NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
  footnote_id TEXT NOT NULL,
  text        TEXT NOT NULL,
  is_foreign_currency BOOLEAN DEFAULT FALSE,
  is_restricted_security BOOLEAN DEFAULT FALSE,
  is_continuation_fund BOOLEAN DEFAULT FALSE,
  is_fair_value_methodology BOOLEAN DEFAULT FALSE,
  created_at  TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(filing_id, footnote_id)
);

CREATE INDEX IF NOT EXISTS idx_filing_footnotes_filing ON filing_footnotes(filing_id);

-- ============================================================================
-- FINANCIAL STATEMENTS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS financial_statements (
  id                              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id                       UUID NOT NULL REFERENCES filings(id) ON DELETE CASCADE,

  -- Assets & Liabilities
  investments_at_fair_value       NUMERIC(20,2),
  cash_and_equivalents            NUMERIC(20,2),
  total_assets                    NUMERIC(20,2),
  credit_facility_borrowings      NUMERIC(20,2),
  management_fees_payable         NUMERIC(20,2),
  incentive_fees_payable          NUMERIC(20,2),
  redemptions_payable             NUMERIC(20,2),
  total_liabilities               NUMERIC(20,2),
  net_assets                      NUMERIC(20,2),

  -- Operations
  total_investment_income         NUMERIC(20,2),
  management_fees                 NUMERIC(20,2),
  incentive_fees                  NUMERIC(20,2),
  interest_expense                NUMERIC(20,2),
  total_expenses                  NUMERIC(20,2),
  net_investment_income_loss      NUMERIC(20,2),
  net_realized_gain_loss          NUMERIC(20,2),
  net_change_unrealized           NUMERIC(20,2),
  net_increase_from_operations    NUMERIC(20,2),

  -- Capital flows
  capital_contributions           NUMERIC(20,2),
  capital_distributions           NUMERIC(20,2),
  net_assets_beginning            NUMERIC(20,2),
  net_assets_end                  NUMERIC(20,2),

  -- Cash flows
  purchases_of_investments        NUMERIC(20,2),
  proceeds_from_realizations      NUMERIC(20,2),
  proceeds_from_borrowings        NUMERIC(20,2),
  repayment_of_borrowings         NUMERIC(20,2),
  net_change_in_cash              NUMERIC(20,2),

  -- Credit facility
  credit_facility_lender          TEXT,
  credit_facility_commitment      NUMERIC(20,2),
  credit_facility_outstanding     NUMERIC(20,2),
  credit_facility_rate            TEXT,
  credit_facility_maturity        DATE,

  -- Full raw JSONB for anything not covered above
  raw_assets_liabilities          JSONB,
  raw_operations                  JSONB,
  raw_cash_flows                  JSONB,
  raw_financial_highlights        JSONB,
  raw_credit_facility             JSONB,

  created_at                      TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(filing_id)
);

CREATE INDEX IF NOT EXISTS idx_financial_statements_filing ON financial_statements(filing_id);

-- ============================================================================
-- UNFUNDED COMMITMENTS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS unfunded_commitments (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id               UUID NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
  holding_name            TEXT NOT NULL,
  unfunded_commitment_usd NUMERIC(20,2),
  created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_unfunded_commitments_filing ON unfunded_commitments(filing_id);

-- ============================================================================
-- PIPELINE RUNS TABLE
-- Audit log for every CLI run
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_type        TEXT,
  input_params    JSONB,
  filings_total   INTEGER DEFAULT 0,
  filings_ok      INTEGER DEFAULT 0,
  filings_failed  INTEGER DEFAULT 0,
  started_at      TIMESTAMPTZ DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  run_log         JSONB DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_type ON pipeline_runs(run_type);

-- ============================================================================
-- UPDATED_AT TRIGGER
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_filings_updated_at ON filings;
CREATE TRIGGER update_filings_updated_at
    BEFORE UPDATE ON filings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- ROW LEVEL SECURITY (optional, enable if needed)
-- ============================================================================
-- ALTER TABLE filings ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE holdings ENABLE ROW LEVEL SECURITY;
-- etc.

-- ============================================================================
-- GRANTS (for service role)
-- ============================================================================
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
-- GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO service_role;
