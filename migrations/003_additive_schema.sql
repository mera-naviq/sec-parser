-- SEC Parser Elite - Additive Schema Migration
-- This adds new tables/columns WITHOUT dropping existing data

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- FILINGS TABLE (create if not exists, or add missing columns)
-- ============================================================================
CREATE TABLE IF NOT EXISTS filings (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cik                   TEXT NOT NULL,
  fund_name             TEXT NOT NULL,
  report_type           TEXT NOT NULL,
  report_period_end     DATE NOT NULL,
  sec_accession_number  TEXT UNIQUE NOT NULL,
  sec_filing_url        TEXT NOT NULL,
  status                TEXT NOT NULL DEFAULT 'pending',
  created_at            TIMESTAMPTZ DEFAULT NOW(),
  updated_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Add columns if they don't exist
DO $$
BEGIN
  -- filings columns
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='fiscal_year_end') THEN
    ALTER TABLE filings ADD COLUMN fiscal_year_end TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='period_label') THEN
    ALTER TABLE filings ADD COLUMN period_label TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='manager_name') THEN
    ALTER TABLE filings ADD COLUMN manager_name TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='manager_address') THEN
    ALTER TABLE filings ADD COLUMN manager_address TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='raw_html_s3_key') THEN
    ALTER TABLE filings ADD COLUMN raw_html_s3_key TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='raw_pdf_s3_key') THEN
    ALTER TABLE filings ADD COLUMN raw_pdf_s3_key TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='error_message') THEN
    ALTER TABLE filings ADD COLUMN error_message TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='textract_job_id') THEN
    ALTER TABLE filings ADD COLUMN textract_job_id TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='claude_batch_id') THEN
    ALTER TABLE filings ADD COLUMN claude_batch_id TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='confidence_score') THEN
    ALTER TABLE filings ADD COLUMN confidence_score NUMERIC(5,2);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='filings' AND column_name='extraction_warnings') THEN
    ALTER TABLE filings ADD COLUMN extraction_warnings JSONB DEFAULT '[]';
  END IF;
END $$;

-- Create indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings(cik);
CREATE INDEX IF NOT EXISTS idx_filings_period ON filings(report_period_end);
CREATE INDEX IF NOT EXISTS idx_filings_status ON filings(status);
CREATE INDEX IF NOT EXISTS idx_filings_accession ON filings(sec_accession_number);

-- ============================================================================
-- Add filing_id to holdings if it doesn't exist
-- ============================================================================
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='filing_id') THEN
    ALTER TABLE holdings ADD COLUMN filing_id UUID REFERENCES filings(id) ON DELETE CASCADE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='footnote_refs') THEN
    ALTER TABLE holdings ADD COLUMN footnote_refs TEXT[];
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='investment_type') THEN
    ALTER TABLE holdings ADD COLUMN investment_type TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='investment_purpose') THEN
    ALTER TABLE holdings ADD COLUMN investment_purpose TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='geographic_region') THEN
    ALTER TABLE holdings ADD COLUMN geographic_region TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='position') THEN
    ALTER TABLE holdings ADD COLUMN position TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='reference_rate_spread') THEN
    ALTER TABLE holdings ADD COLUMN reference_rate_spread TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='maturity_date') THEN
    ALTER TABLE holdings ADD COLUMN maturity_date DATE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='acquisition_date') THEN
    ALTER TABLE holdings ADD COLUMN acquisition_date DATE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='moic') THEN
    ALTER TABLE holdings ADD COLUMN moic NUMERIC(10,4);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='is_restricted') THEN
    ALTER TABLE holdings ADD COLUMN is_restricted BOOLEAN DEFAULT FALSE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='section_name') THEN
    ALTER TABLE holdings ADD COLUMN section_name TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='section_pct_of_nav') THEN
    ALTER TABLE holdings ADD COLUMN section_pct_of_nav NUMERIC(6,3);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='raw_textract_row') THEN
    ALTER TABLE holdings ADD COLUMN raw_textract_row JSONB;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='extraction_source') THEN
    ALTER TABLE holdings ADD COLUMN extraction_source TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='holdings' AND column_name='confidence') THEN
    ALTER TABLE holdings ADD COLUMN confidence NUMERIC(5,4);
  END IF;
END $$;

-- Create holdings indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_holdings_filing_id ON holdings(filing_id);

-- ============================================================================
-- SECTION SUBTOTALS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS section_subtotals (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id         UUID REFERENCES filings(id) ON DELETE CASCADE,
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
  filing_id   UUID REFERENCES filings(id) ON DELETE CASCADE,
  footnote_id TEXT NOT NULL,
  text        TEXT NOT NULL,
  is_foreign_currency BOOLEAN DEFAULT FALSE,
  is_restricted_security BOOLEAN DEFAULT FALSE,
  is_continuation_fund BOOLEAN DEFAULT FALSE,
  is_fair_value_methodology BOOLEAN DEFAULT FALSE,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Add unique constraint if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'filing_footnotes_filing_id_footnote_id_key'
  ) THEN
    ALTER TABLE filing_footnotes ADD CONSTRAINT filing_footnotes_filing_id_footnote_id_key UNIQUE(filing_id, footnote_id);
  END IF;
EXCEPTION WHEN others THEN
  -- Constraint might already exist with different name
  NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_filing_footnotes_filing ON filing_footnotes(filing_id);

-- ============================================================================
-- FINANCIAL STATEMENTS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS financial_statements (
  id                              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id                       UUID REFERENCES filings(id) ON DELETE CASCADE,
  investments_at_fair_value       NUMERIC(20,2),
  cash_and_equivalents            NUMERIC(20,2),
  total_assets                    NUMERIC(20,2),
  credit_facility_borrowings      NUMERIC(20,2),
  management_fees_payable         NUMERIC(20,2),
  incentive_fees_payable          NUMERIC(20,2),
  redemptions_payable             NUMERIC(20,2),
  total_liabilities               NUMERIC(20,2),
  net_assets                      NUMERIC(20,2),
  total_investment_income         NUMERIC(20,2),
  management_fees                 NUMERIC(20,2),
  incentive_fees                  NUMERIC(20,2),
  interest_expense                NUMERIC(20,2),
  total_expenses                  NUMERIC(20,2),
  net_investment_income_loss      NUMERIC(20,2),
  net_realized_gain_loss          NUMERIC(20,2),
  net_change_unrealized           NUMERIC(20,2),
  net_increase_from_operations    NUMERIC(20,2),
  capital_contributions           NUMERIC(20,2),
  capital_distributions           NUMERIC(20,2),
  net_assets_beginning            NUMERIC(20,2),
  net_assets_end                  NUMERIC(20,2),
  purchases_of_investments        NUMERIC(20,2),
  proceeds_from_realizations      NUMERIC(20,2),
  proceeds_from_borrowings        NUMERIC(20,2),
  repayment_of_borrowings         NUMERIC(20,2),
  net_change_in_cash              NUMERIC(20,2),
  credit_facility_lender          TEXT,
  credit_facility_commitment      NUMERIC(20,2),
  credit_facility_outstanding     NUMERIC(20,2),
  credit_facility_rate            TEXT,
  credit_facility_maturity        DATE,
  raw_assets_liabilities          JSONB,
  raw_operations                  JSONB,
  raw_cash_flows                  JSONB,
  raw_financial_highlights        JSONB,
  raw_credit_facility             JSONB,
  created_at                      TIMESTAMPTZ DEFAULT NOW()
);

-- Add unique constraint if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'financial_statements_filing_id_key'
  ) THEN
    ALTER TABLE financial_statements ADD CONSTRAINT financial_statements_filing_id_key UNIQUE(filing_id);
  END IF;
EXCEPTION WHEN others THEN
  NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_financial_statements_filing ON financial_statements(filing_id);

-- ============================================================================
-- UNFUNDED COMMITMENTS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS unfunded_commitments (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id               UUID REFERENCES filings(id) ON DELETE CASCADE,
  holding_name            TEXT NOT NULL,
  unfunded_commitment_usd NUMERIC(20,2),
  created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_unfunded_commitments_filing ON unfunded_commitments(filing_id);

-- ============================================================================
-- PIPELINE RUNS TABLE
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
-- SUCCESS
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'SEC Parser Elite additive migration completed successfully!';
END $$;
