"""
Data Validator
Validates extracted data and computes confidence scores.
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field

import structlog

from .mapper import MappedHolding

logger = structlog.get_logger()


@dataclass
class ValidationWarning:
    """A validation warning."""
    code: str
    message: str
    severity: str = "medium"  # low, medium, high
    details: Optional[Dict[str, Any]] = None


@dataclass
class ValidationResult:
    """Result of validation."""
    is_valid: bool
    confidence_score: float
    warnings: List[ValidationWarning] = field(default_factory=list)


class DataValidator:
    """Validates extracted data and computes confidence scores."""

    # Tolerance for value comparisons
    VALUE_TOLERANCE = 0.005  # 0.5%

    @classmethod
    def validate_balance_sheet_reconciliation(
        cls,
        holdings: List[MappedHolding],
        financial_statements: Dict[str, Any],
    ) -> Tuple[bool, Optional[ValidationWarning]]:
        """
        Verify that sum of holdings fair values ≈ investments_at_fair_value.

        Returns:
            Tuple of (passes, warning if fails)
        """
        if not holdings or not financial_statements:
            return True, None

        holdings_total = sum(h.fair_value for h in holdings if h.fair_value)
        stated_total = financial_statements.get("investments_at_fair_value")

        if stated_total is None:
            return True, None

        # Check within tolerance
        if stated_total == 0:
            if holdings_total == 0:
                return True, None
            else:
                return False, ValidationWarning(
                    code="BALANCE_SHEET_MISMATCH",
                    message=f"Holdings total ${holdings_total:,.2f} but stated investments is $0",
                    severity="high",
                    details={
                        "holdings_total": holdings_total,
                        "stated_total": stated_total,
                    },
                )

        diff_pct = abs(holdings_total - stated_total) / stated_total

        if diff_pct > cls.VALUE_TOLERANCE:
            return False, ValidationWarning(
                code="BALANCE_SHEET_MISMATCH",
                message=f"Holdings total ${holdings_total:,.2f} differs from stated ${stated_total:,.2f} by {diff_pct:.1%}",
                severity="high",
                details={
                    "holdings_total": holdings_total,
                    "stated_total": stated_total,
                    "difference_pct": diff_pct,
                },
            )

        return True, None

    @classmethod
    def validate_net_assets_equation(
        cls,
        financial_statements: Dict[str, Any],
    ) -> Tuple[bool, Optional[ValidationWarning]]:
        """
        Verify net_assets ≈ total_assets - total_liabilities.

        Returns:
            Tuple of (passes, warning if fails)
        """
        if not financial_statements:
            return True, None

        net_assets = financial_statements.get("net_assets")
        total_assets = financial_statements.get("total_assets")
        total_liabilities = financial_statements.get("total_liabilities")

        if net_assets is None or total_assets is None or total_liabilities is None:
            return True, None

        calculated = total_assets - total_liabilities

        if calculated == 0:
            if net_assets == 0:
                return True, None
            else:
                return False, ValidationWarning(
                    code="NET_ASSETS_MISMATCH",
                    message=f"Net assets ${net_assets:,.2f} but assets - liabilities = $0",
                    severity="high",
                )

        diff_pct = abs(net_assets - calculated) / abs(calculated)

        if diff_pct > cls.VALUE_TOLERANCE:
            return False, ValidationWarning(
                code="NET_ASSETS_MISMATCH",
                message=f"Net assets ${net_assets:,.2f} differs from calculated ${calculated:,.2f}",
                severity="medium",
                details={
                    "stated_net_assets": net_assets,
                    "calculated_net_assets": calculated,
                    "total_assets": total_assets,
                    "total_liabilities": total_liabilities,
                },
            )

        return True, None

    @classmethod
    def validate_footnote_references(
        cls,
        holdings: List[MappedHolding],
        footnotes: Dict[str, str],
    ) -> Tuple[bool, List[ValidationWarning]]:
        """
        Verify all footnote references in holdings resolve to actual footnotes.

        Returns:
            Tuple of (all_resolved, list of warnings)
        """
        warnings = []

        # Collect all footnote refs from holdings
        all_refs = set()
        for h in holdings:
            if h.footnote_refs:
                all_refs.update(h.footnote_refs)

        # Check each ref resolves
        unresolved = []
        for ref in all_refs:
            if ref not in footnotes:
                unresolved.append(ref)

        if unresolved:
            warnings.append(ValidationWarning(
                code="UNRESOLVED_FOOTNOTES",
                message=f"Footnote references not found: {', '.join(unresolved)}",
                severity="low",
                details={"unresolved": unresolved},
            ))

        return len(unresolved) == 0, warnings

    @classmethod
    def validate_period_consistency(
        cls,
        metadata_period: Optional[str],
        edgar_period: Optional[str],
    ) -> Tuple[bool, Optional[ValidationWarning]]:
        """
        Verify report_period_end matches what EDGAR API reports.

        Returns:
            Tuple of (matches, warning if mismatch)
        """
        if not metadata_period or not edgar_period:
            return True, None

        if metadata_period != edgar_period:
            return False, ValidationWarning(
                code="PERIOD_MISMATCH",
                message=f"Extracted period {metadata_period} differs from EDGAR {edgar_period}",
                severity="low",
                details={
                    "extracted": metadata_period,
                    "edgar": edgar_period,
                },
            )

        return True, None

    @classmethod
    def compute_confidence_score(
        cls,
        holdings: List[MappedHolding],
        validation_warnings: List[ValidationWarning],
        financial_statements: Optional[Dict[str, Any]] = None,
    ) -> float:
        """
        Compute overall confidence score (0-100).

        Scoring:
        - Start at 100
        - Deduct 20 if balance sheet reconciliation fails
        - Deduct 10 if >5% of holdings have low confidence
        - Deduct 5 per unresolvable footnote reference (max -15)
        - Deduct 10 if financial statements are missing or incomplete

        Returns:
            Confidence score 0-100
        """
        score = 100.0

        # Check for high severity warnings
        for warning in validation_warnings:
            if warning.code == "BALANCE_SHEET_MISMATCH":
                score -= 20
            elif warning.code == "NET_ASSETS_MISMATCH":
                score -= 10
            elif warning.code == "UNRESOLVED_FOOTNOTES":
                count = len(warning.details.get("unresolved", []))
                score -= min(count * 5, 15)
            elif warning.code == "PERIOD_MISMATCH":
                score -= 5

        # Check holdings confidence
        if holdings:
            low_confidence_count = sum(1 for h in holdings if h.confidence == "low")
            low_confidence_pct = low_confidence_count / len(holdings)
            if low_confidence_pct > 0.05:
                score -= 10

        # Check financial statements completeness
        if financial_statements:
            required_fields = [
                "investments_at_fair_value",
                "total_assets",
                "total_liabilities",
                "net_assets",
            ]
            missing = sum(1 for f in required_fields if financial_statements.get(f) is None)
            if missing >= 2:
                score -= 10

        return max(0, min(100, score))

    @classmethod
    def validate_all(
        cls,
        holdings: List[MappedHolding],
        financial_statements: Optional[Dict[str, Any]],
        footnotes: Dict[str, str],
        metadata_period: Optional[str] = None,
        edgar_period: Optional[str] = None,
    ) -> ValidationResult:
        """
        Run all validations and compute confidence score.

        Args:
            holdings: List of mapped holdings
            financial_statements: Parsed financial statement data
            footnotes: Dict of footnote_id -> text
            metadata_period: Period from HTML parsing
            edgar_period: Period from EDGAR API

        Returns:
            ValidationResult with is_valid, confidence_score, and warnings
        """
        all_warnings: List[ValidationWarning] = []

        # Balance sheet reconciliation
        passes, warning = cls.validate_balance_sheet_reconciliation(
            holdings, financial_statements or {}
        )
        if warning:
            all_warnings.append(warning)

        # Net assets equation
        passes, warning = cls.validate_net_assets_equation(financial_statements or {})
        if warning:
            all_warnings.append(warning)

        # Footnote references
        passes, warnings = cls.validate_footnote_references(holdings, footnotes)
        all_warnings.extend(warnings)

        # Period consistency
        passes, warning = cls.validate_period_consistency(metadata_period, edgar_period)
        if warning:
            all_warnings.append(warning)

        # Compute confidence
        confidence = cls.compute_confidence_score(
            holdings, all_warnings, financial_statements
        )

        # Determine if valid (no high severity warnings)
        is_valid = not any(w.severity == "high" for w in all_warnings)

        logger.info(
            "Validation complete",
            is_valid=is_valid,
            confidence_score=confidence,
            warning_count=len(all_warnings),
        )

        return ValidationResult(
            is_valid=is_valid,
            confidence_score=confidence,
            warnings=all_warnings,
        )
