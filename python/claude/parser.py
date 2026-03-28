"""
Claude Response Parser
Parses and validates JSON responses from Claude.
"""

import json
import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


@dataclass
class ParsedMetadata:
    """Parsed cover page metadata."""
    fund_name: Optional[str] = None
    cik: Optional[str] = None
    report_type: Optional[str] = None
    report_period_end: Optional[str] = None
    fiscal_year_end: Optional[str] = None
    period_label: Optional[str] = None
    manager_name: Optional[str] = None
    manager_address: Optional[str] = None


@dataclass
class ParsedFootnote:
    """Parsed footnote."""
    footnote_id: str
    text: str
    is_foreign_currency: bool = False
    is_restricted_security: bool = False
    is_continuation_fund: bool = False
    is_fair_value_methodology: bool = False


@dataclass
class ParsedCreditFacility:
    """Parsed credit facility information."""
    lender: Optional[str] = None
    commitment_amount: Optional[float] = None
    outstanding_balance: Optional[float] = None
    interest_rate: Optional[str] = None
    maturity_date: Optional[str] = None


@dataclass
class ParsedUnfundedCommitment:
    """Parsed unfunded commitment."""
    holding_name: str
    unfunded_commitment_usd: float


@dataclass
class ParsedValidation:
    """Parsed validation results."""
    validation_results: List[Dict[str, Any]]
    misaligned_rows: List[Dict[str, Any]]
    warnings: List[str]
    confidence_score: float


class ClaudeResponseParser:
    """Parses JSON responses from Claude."""

    @staticmethod
    def _extract_json(text: str) -> Dict[str, Any]:
        """
        Extract JSON from Claude response text.

        Handles cases where Claude includes markdown code fences or preamble.
        """
        if not text:
            return {}

        # Try direct parse first
        try:
            return json.loads(text.strip())
        except json.JSONDecodeError:
            pass

        # Try to find JSON in code fences
        json_match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try to find raw JSON object
        json_match = re.search(r"(\{[\s\S]*\})", text)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        logger.warning("Failed to parse JSON from Claude response", text=text[:500])
        return {}

    @classmethod
    def parse_metadata(cls, response: str) -> ParsedMetadata:
        """Parse cover page metadata response."""
        data = cls._extract_json(response)

        return ParsedMetadata(
            fund_name=data.get("fund_name"),
            cik=data.get("cik"),
            report_type=data.get("report_type"),
            report_period_end=data.get("report_period_end"),
            fiscal_year_end=data.get("fiscal_year_end"),
            period_label=data.get("period_label"),
            manager_name=data.get("manager_name"),
            manager_address=data.get("manager_address"),
        )

    @classmethod
    def parse_footnotes(cls, response: str) -> List[ParsedFootnote]:
        """Parse footnotes response."""
        data = cls._extract_json(response)
        footnotes_data = data.get("footnotes", {})

        footnotes = []
        for footnote_id, info in footnotes_data.items():
            if isinstance(info, dict):
                footnotes.append(ParsedFootnote(
                    footnote_id=str(footnote_id),
                    text=info.get("text", ""),
                    is_foreign_currency=bool(info.get("is_foreign_currency")),
                    is_restricted_security=bool(info.get("is_restricted_security")),
                    is_continuation_fund=bool(info.get("is_continuation_fund")),
                    is_fair_value_methodology=bool(info.get("is_fair_value_methodology")),
                ))
            elif isinstance(info, str):
                # Simple format: just the text
                footnotes.append(ParsedFootnote(
                    footnote_id=str(footnote_id),
                    text=info,
                ))

        return footnotes

    @classmethod
    def parse_notes_financial_statements(cls, response: str) -> Dict[str, Any]:
        """Parse Notes to Financial Statements response."""
        data = cls._extract_json(response)

        result = {
            "organization_description": data.get("organization_description"),
            "valuation_hierarchy": data.get("valuation_hierarchy", {}),
            "credit_facility": None,
            "unfunded_commitments": [],
            "fee_structure": data.get("fee_structure", {}),
            "significant_events": data.get("significant_events", []),
        }

        # Parse credit facility
        cf_data = data.get("credit_facility", {})
        if cf_data:
            result["credit_facility"] = ParsedCreditFacility(
                lender=cf_data.get("lender"),
                commitment_amount=cf_data.get("commitment_amount"),
                outstanding_balance=cf_data.get("outstanding_balance"),
                interest_rate=cf_data.get("interest_rate"),
                maturity_date=cf_data.get("maturity_date"),
            )

        # Parse unfunded commitments
        for uc in data.get("unfunded_commitments", []):
            if uc.get("holding_name") and uc.get("unfunded_commitment_usd"):
                result["unfunded_commitments"].append(ParsedUnfundedCommitment(
                    holding_name=uc["holding_name"],
                    unfunded_commitment_usd=float(uc["unfunded_commitment_usd"]),
                ))

        return result

    @classmethod
    def parse_financial_statements(cls, response: str) -> Dict[str, Any]:
        """Parse financial statements response."""
        data = cls._extract_json(response)

        result = {
            # Assets & Liabilities
            "investments_at_fair_value": None,
            "cash_and_equivalents": None,
            "total_assets": None,
            "credit_facility_borrowings": None,
            "management_fees_payable": None,
            "incentive_fees_payable": None,
            "redemptions_payable": None,
            "total_liabilities": None,
            "net_assets": None,
            # Operations
            "total_investment_income": None,
            "management_fees": None,
            "incentive_fees": None,
            "interest_expense": None,
            "total_expenses": None,
            "net_investment_income_loss": None,
            "net_realized_gain_loss": None,
            "net_change_unrealized": None,
            "net_increase_from_operations": None,
            # Capital flows
            "capital_contributions": None,
            "capital_distributions": None,
            "net_assets_beginning": None,
            "net_assets_end": None,
            # Cash flows
            "purchases_of_investments": None,
            "proceeds_from_realizations": None,
            "proceeds_from_borrowings": None,
            "repayment_of_borrowings": None,
            "net_change_in_cash": None,
            # Raw data
            "raw_assets_liabilities": None,
            "raw_operations": None,
            "raw_cash_flows": None,
        }

        # Map from nested response structure
        assets = data.get("assets_and_liabilities", {})
        for key in ["investments_at_fair_value", "cash_and_equivalents", "total_assets",
                    "credit_facility_borrowings", "management_fees_payable",
                    "incentive_fees_payable", "redemptions_payable",
                    "total_liabilities", "net_assets"]:
            if key in assets:
                result[key] = assets[key]

        operations = data.get("operations", {})
        for key in ["total_investment_income", "management_fees", "incentive_fees",
                    "interest_expense", "total_expenses", "net_investment_income_loss",
                    "net_realized_gain_loss", "net_change_unrealized",
                    "net_increase_from_operations"]:
            if key in operations:
                result[key] = operations[key]

        capital = data.get("capital_activity", {})
        for key in ["capital_contributions", "capital_distributions",
                    "net_assets_beginning", "net_assets_end"]:
            if key in capital:
                result[key] = capital[key]

        cash = data.get("cash_flows", {})
        for key in ["purchases_of_investments", "proceeds_from_realizations",
                    "proceeds_from_borrowings", "repayment_of_borrowings",
                    "net_change_in_cash"]:
            if key in cash:
                result[key] = cash[key]

        # Store raw data for reference
        result["raw_assets_liabilities"] = assets
        result["raw_operations"] = operations
        result["raw_cash_flows"] = cash

        return result

    @classmethod
    def parse_validation(cls, response: str) -> ParsedValidation:
        """Parse schedule validation response."""
        data = cls._extract_json(response)

        return ParsedValidation(
            validation_results=data.get("validation_results", []),
            misaligned_rows=data.get("misaligned_rows", []),
            warnings=data.get("warnings", []),
            confidence_score=float(data.get("confidence_score", 0)),
        )

    @classmethod
    def parse_full_schedule(cls, response: str) -> Dict[str, Any]:
        """Parse full schedule extraction (Claude-only mode) response."""
        data = cls._extract_json(response)

        holdings = data.get("holdings", [])
        subtotals = data.get("section_subtotals", [])
        value_scale = data.get("value_scale", "actual")

        # Determine multiplier
        scale_multiplier = 1
        if value_scale == "thousands":
            scale_multiplier = 1000
        elif value_scale == "millions":
            scale_multiplier = 1000000

        # Apply scale to values
        for holding in holdings:
            if holding.get("cost"):
                holding["cost"] = holding["cost"] * scale_multiplier
            if holding.get("fair_value"):
                holding["fair_value"] = holding["fair_value"] * scale_multiplier

        for subtotal in subtotals:
            if subtotal.get("total_cost"):
                subtotal["total_cost"] = subtotal["total_cost"] * scale_multiplier
            if subtotal.get("total_fair_value"):
                subtotal["total_fair_value"] = subtotal["total_fair_value"] * scale_multiplier

        return {
            "holdings": holdings,
            "subtotals": subtotals,
            "value_scale": value_scale,
            "scale_multiplier": scale_multiplier,
        }
