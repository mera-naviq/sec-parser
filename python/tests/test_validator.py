"""
Tests for DataValidator.

Tests reconciliation logic and confidence scoring.
"""

import pytest
from pipeline.validator import DataValidator, ValidationResult
from pipeline.mapper import MappedHolding


def make_holding(
    name: str = "Test Holding",
    cost: float = 1000000.0,
    fair_value: float = 1200000.0,
    **kwargs
) -> MappedHolding:
    """Helper to create test holdings."""
    defaults = {
        "holding_name": name,
        "footnote_refs": None,
        "investment_type": "Equity",
        "investment_purpose": None,
        "geographic_region": None,
        "position": None,
        "reference_rate_spread": None,
        "maturity_date": None,
        "acquisition_date": None,
        "cost": cost,
        "fair_value": fair_value,
        "moic": fair_value / cost if cost else None,
        "is_restricted": False,
        "section_name": None,
        "section_pct_of_nav": None,
        "raw_textract_row": None,
        "extraction_source": "textract",
        "confidence": 0.95,
    }
    defaults.update(kwargs)
    return MappedHolding(**defaults)


class TestValidateAll:
    """Tests for DataValidator.validate_all()"""

    def test_valid_data_no_warnings(self):
        """Test that valid data produces no warnings."""
        holdings = [
            make_holding("Holding A", 1000000, 1200000),
            make_holding("Holding B", 2000000, 2500000),
        ]
        financial = {
            "investments_at_fair_value": 3700000,
            "total_assets": 4000000,
        }

        result = DataValidator.validate_all(
            holdings=holdings,
            financial_statements=financial,
            footnotes=[],
            metadata_period="2024-06-30",
            edgar_period="2024-06-30",
        )

        assert isinstance(result, ValidationResult)
        assert result.is_valid is True
        assert len(result.warnings) == 0

    def test_period_mismatch_warning(self):
        """Test warning when report periods don't match."""
        holdings = [make_holding()]

        result = DataValidator.validate_all(
            holdings=holdings,
            financial_statements={},
            footnotes=[],
            metadata_period="2024-06-30",
            edgar_period="2024-03-31",
        )

        assert any("period" in w.lower() for w in result.warnings)

    def test_fair_value_reconciliation_within_tolerance(self):
        """Test that fair value sums within 1% tolerance pass."""
        holdings = [
            make_holding("A", 1000000, 1000000),
            make_holding("B", 1000000, 1000000),
        ]
        financial = {
            "investments_at_fair_value": 2005000,  # 0.25% difference
        }

        result = DataValidator.validate_all(
            holdings=holdings,
            financial_statements=financial,
            footnotes=[],
            metadata_period="2024-06-30",
            edgar_period="2024-06-30",
        )

        # Should pass with small difference
        assert not any("reconciliation" in w.lower() for w in result.warnings)

    def test_fair_value_reconciliation_outside_tolerance(self):
        """Test warning when fair values don't reconcile."""
        holdings = [
            make_holding("A", 1000000, 1000000),
            make_holding("B", 1000000, 1000000),
        ]
        financial = {
            "investments_at_fair_value": 3000000,  # 50% difference
        }

        result = DataValidator.validate_all(
            holdings=holdings,
            financial_statements=financial,
            footnotes=[],
            metadata_period="2024-06-30",
            edgar_period="2024-06-30",
        )

        assert any("reconcil" in w.lower() for w in result.warnings)

    def test_empty_holdings_warning(self):
        """Test warning when no holdings are extracted."""
        result = DataValidator.validate_all(
            holdings=[],
            financial_statements={"investments_at_fair_value": 10000000},
            footnotes=[],
            metadata_period="2024-06-30",
            edgar_period="2024-06-30",
        )

        assert result.is_valid is False
        assert any("no holdings" in w.lower() for w in result.warnings)

    def test_duplicate_holdings_warning(self):
        """Test warning when duplicate holdings are detected."""
        holdings = [
            make_holding("Same Company LP", 1000000, 1200000),
            make_holding("Same Company LP", 1000000, 1200000),
        ]

        result = DataValidator.validate_all(
            holdings=holdings,
            financial_statements={},
            footnotes=[],
            metadata_period="2024-06-30",
            edgar_period="2024-06-30",
        )

        assert any("duplicate" in w.lower() for w in result.warnings)


class TestConfidenceScore:
    """Tests for confidence score computation."""

    def test_high_confidence_clean_data(self):
        """Test high confidence for clean data."""
        holdings = [
            make_holding("A", 1000000, 1200000, confidence=0.98),
            make_holding("B", 2000000, 2400000, confidence=0.97),
        ]
        financial = {"investments_at_fair_value": 3600000}

        score = DataValidator.compute_confidence_score(
            holdings=holdings,
            validation_warnings=[],
            financial_statements=financial,
        )

        assert score >= 0.9

    def test_lower_confidence_with_warnings(self):
        """Test reduced confidence when warnings exist."""
        holdings = [make_holding()]

        score_clean = DataValidator.compute_confidence_score(
            holdings=holdings,
            validation_warnings=[],
            financial_statements={},
        )

        score_with_warnings = DataValidator.compute_confidence_score(
            holdings=holdings,
            validation_warnings=["Period mismatch", "Reconciliation failed"],
            financial_statements={},
        )

        assert score_with_warnings < score_clean

    def test_confidence_with_low_extraction_confidence(self):
        """Test overall confidence reflects low extraction confidence."""
        holdings = [
            make_holding("A", confidence=0.5),
            make_holding("B", confidence=0.6),
        ]

        score = DataValidator.compute_confidence_score(
            holdings=holdings,
            validation_warnings=[],
            financial_statements={},
        )

        assert score < 0.8

    def test_confidence_bounds(self):
        """Test that confidence is always between 0 and 1."""
        # Edge case: many warnings
        score = DataValidator.compute_confidence_score(
            holdings=[],
            validation_warnings=["w1", "w2", "w3", "w4", "w5"] * 10,
            financial_statements={},
        )

        assert 0.0 <= score <= 1.0


class TestInvestmentTypeClassification:
    """Tests for investment type detection."""

    def test_detect_debt_investment(self):
        """Test detection of debt investments."""
        assert DataValidator.is_debt_investment("Senior Secured First Lien Term Loan")
        assert DataValidator.is_debt_investment("Subordinated Notes")
        assert DataValidator.is_debt_investment("Convertible Debt")

    def test_detect_equity_investment(self):
        """Test detection of equity investments."""
        assert DataValidator.is_equity_investment("Common Stock")
        assert DataValidator.is_equity_investment("Preferred Equity")
        assert DataValidator.is_equity_investment("Class A Shares")

    def test_detect_fund_investment(self):
        """Test detection of fund investments."""
        assert DataValidator.is_fund_investment("Private Equity Fund LP")
        assert DataValidator.is_fund_investment("Venture Capital Partnership")
        assert DataValidator.is_fund_investment("Co-Investment Fund")
