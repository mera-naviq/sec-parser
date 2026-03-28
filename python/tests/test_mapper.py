"""
Tests for DataMapper.

Tests mapping of raw Textract rows to MappedHolding objects.
"""

import pytest
from pipeline.mapper import DataMapper, MappedHolding


class TestDataMapper:
    """Tests for DataMapper.map_holding()"""

    def test_basic_holding_with_all_fields(self):
        """Test mapping a holding with all standard fields populated."""
        raw = {
            "holding_name": "Acme Corp Series A Preferred",
            "investment_type": "Equity - Preferred Stock",
            "investment_purpose": "Co-Investment",
            "geographic_region": "North America",
            "position": "1,000,000 shares",
            "cost": "5000000",
            "fair_value": "7500000",
            "footnote_refs": ["(a)", "(b)"],
        }

        result = DataMapper.map_holding(raw, None, "Investments")

        assert result is not None
        assert result.holding_name == "Acme Corp Series A Preferred"
        assert result.investment_type == "Equity - Preferred Stock"
        assert result.investment_purpose == "Co-Investment"
        assert result.geographic_region == "North America"
        assert result.position == "1,000,000 shares"
        assert result.cost == 5000000.0
        assert result.fair_value == 7500000.0
        assert result.footnote_refs == ["(a)", "(b)"]

    def test_holding_with_debt_fields(self):
        """Test mapping a debt holding with rate spread and maturity."""
        raw = {
            "holding_name": "XYZ Holdings Term Loan",
            "investment_type": "Debt - Senior Secured First Lien",
            "reference_rate_spread": "SOFR + 5.50%",
            "maturity_date": "2028-06-15",
            "cost": "10000000",
            "fair_value": "9800000",
        }

        result = DataMapper.map_holding(raw, None, "Debt Investments")

        assert result is not None
        assert result.holding_name == "XYZ Holdings Term Loan"
        assert result.investment_type == "Debt - Senior Secured First Lien"
        assert result.reference_rate_spread == "SOFR + 5.50%"
        assert result.maturity_date == "2028-06-15"
        assert result.cost == 10000000.0
        assert result.fair_value == 9800000.0

    def test_holding_with_moic(self):
        """Test mapping a holding with MOIC calculation."""
        raw = {
            "holding_name": "Growth Fund LP",
            "investment_type": "Fund Investment",
            "cost": "2000000",
            "fair_value": "3600000",
        }

        result = DataMapper.map_holding(raw, None, None)

        assert result is not None
        assert result.moic == pytest.approx(1.8, rel=0.01)

    def test_holding_with_zero_cost_no_moic(self):
        """Test that MOIC is None when cost is zero."""
        raw = {
            "holding_name": "Gift Shares",
            "investment_type": "Equity - Common Stock",
            "cost": "0",
            "fair_value": "500000",
        }

        result = DataMapper.map_holding(raw, None, None)

        assert result is not None
        assert result.moic is None

    def test_holding_with_restricted_footnote(self):
        """Test that restricted status is detected from footnotes."""
        raw = {
            "holding_name": "Private Co Shares",
            "investment_type": "Equity",
            "cost": "1000000",
            "fair_value": "1200000",
            "footnote_refs": ["(r)"],
        }
        validation = {
            "is_restricted": True,
        }

        result = DataMapper.map_holding(raw, validation, None)

        assert result is not None
        assert result.is_restricted is True

    def test_holding_with_negative_fair_value(self):
        """Test mapping a holding with negative fair value (e.g., derivatives)."""
        raw = {
            "holding_name": "Interest Rate Swap",
            "investment_type": "Derivative",
            "cost": "0",
            "fair_value": "-250000",
        }

        result = DataMapper.map_holding(raw, None, "Derivatives")

        assert result is not None
        assert result.fair_value == -250000.0

    def test_holding_with_currency_formatting(self):
        """Test mapping values with currency symbols and commas."""
        raw = {
            "holding_name": "Euro Holdings GmbH",
            "investment_type": "Equity",
            "cost": "$1,500,000",
            "fair_value": "€1,800,000",
        }

        result = DataMapper.map_holding(raw, None, None)

        assert result is not None
        assert result.cost == 1500000.0
        assert result.fair_value == 1800000.0

    def test_holding_with_parenthetical_negative(self):
        """Test parsing negative values in parentheses."""
        raw = {
            "holding_name": "Distressed Asset",
            "investment_type": "Debt",
            "cost": "5,000,000",
            "fair_value": "(1,000,000)",
        }

        result = DataMapper.map_holding(raw, None, None)

        assert result is not None
        assert result.fair_value == -1000000.0

    def test_holding_missing_name_returns_none(self):
        """Test that holdings without names are rejected."""
        raw = {
            "holding_name": "",
            "investment_type": "Equity",
            "cost": "1000000",
            "fair_value": "1200000",
        }

        result = DataMapper.map_holding(raw, None, None)

        assert result is None

    def test_holding_with_section_info(self):
        """Test that section information is captured."""
        raw = {
            "holding_name": "Tech Startup Inc",
            "investment_type": "Equity - Common",
            "cost": "500000",
            "fair_value": "2000000",
        }

        result = DataMapper.map_holding(raw, None, "Information Technology")

        assert result is not None
        assert result.section_name == "Information Technology"


class TestParseNumericValue:
    """Tests for numeric parsing edge cases."""

    def test_parse_with_thousands_separator(self):
        """Test parsing numbers with thousand separators."""
        raw = {"holding_name": "Test", "cost": "1,234,567.89", "fair_value": "0"}
        result = DataMapper.map_holding(raw, None, None)
        assert result.cost == 1234567.89

    def test_parse_percentage_strip(self):
        """Test that percentages are not treated as values."""
        raw = {
            "holding_name": "Test",
            "cost": "1000000",
            "fair_value": "1100000",
            "section_pct_of_nav": "5.5%",
        }
        result = DataMapper.map_holding(raw, None, None)
        assert result.section_pct_of_nav == 5.5

    def test_parse_empty_string_as_none(self):
        """Test that empty strings become None."""
        raw = {
            "holding_name": "Test",
            "cost": "",
            "fair_value": "1000000",
        }
        result = DataMapper.map_holding(raw, None, None)
        assert result.cost is None
        assert result.fair_value == 1000000.0

    def test_parse_dash_as_none(self):
        """Test that dashes are treated as None."""
        raw = {
            "holding_name": "Test",
            "cost": "-",
            "fair_value": "—",  # em-dash
        }
        result = DataMapper.map_holding(raw, None, None)
        assert result.cost is None
        assert result.fair_value is None
