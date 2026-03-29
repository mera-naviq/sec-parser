"""
Data Mapper
Maps raw extracted data to database schema.
"""

import re
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from dataclasses import dataclass

import structlog

from config import (
    INVESTMENT_TYPES,
    INVESTMENT_PURPOSES,
    GEOGRAPHIC_REGIONS,
    EXTRACTION_SOURCES,
)

logger = structlog.get_logger()


@dataclass
class MappedHolding:
    """A holding mapped to the database schema."""
    holding_name: str
    fair_value: float
    cost: Optional[float] = None
    footnote_refs: Optional[List[str]] = None
    investment_type: Optional[str] = None
    investment_purpose: Optional[str] = None
    geographic_region: Optional[str] = None
    position: Optional[str] = None
    reference_rate_spread: Optional[str] = None
    maturity_date: Optional[date] = None
    acquisition_date: Optional[date] = None
    is_restricted: bool = False
    section_name: Optional[str] = None
    section_pct_of_nav: Optional[float] = None
    moic: Optional[float] = None
    raw_textract_row: Optional[Dict[str, Any]] = None
    extraction_source: str = "textract"
    confidence: float = 1.0  # 0.0-1.0 scale


class DataMapper:
    """Maps extracted data to database schema."""

    # Patterns for detecting investment types
    INVESTMENT_TYPE_PATTERNS = {
        "Secondary": [r"secondary", r"secondaries"],
        "Co-Investment": [r"co-?invest", r"coinvest", r"direct\s+co"],
        "Direct": [r"direct\s+investment", r"direct\s+equity"],
        "Short-Term": [r"short[-\s]?term", r"money\s+market", r"treasury"],
        "Primary": [r"primary", r"fund\s+investment"],
    }

    # Patterns for detecting investment purposes
    INVESTMENT_PURPOSE_PATTERNS = {
        "Buyouts": [r"buyout", r"lbo", r"leveraged"],
        "Venture Capital": [r"venture", r"\bvc\b", r"startup", r"seed", r"series\s+[a-d]"],
        "Growth Equity": [r"growth", r"expansion"],
        "Debt/Credit": [r"debt", r"credit", r"loan", r"mezzanine", r"senior", r"subordinate"],
        "Real Estate": [r"real\s+estate", r"reit", r"property"],
        "Infrastructure": [r"infrastructure", r"infra"],
        "Natural Resources": [r"natural\s+resource", r"energy", r"oil", r"gas"],
        "Distressed": [r"distress", r"turnaround", r"special\s+sit"],
        "Fund of Funds": [r"fund\s+of\s+fund", r"fof"],
        "Hedge Fund": [r"hedge"],
    }

    # Patterns for detecting geographic regions
    REGION_PATTERNS = {
        "Europe": [r"europe", r"european", r"\beu\b", r"\buk\b", r"britain", r"german", r"french"],
        "Asia-Pacific": [r"asia", r"asian", r"china", r"japan", r"korea", r"india", r"singapore", r"apac"],
        "Latin America": [r"latin", r"latam", r"brazil", r"mexico", r"south\s+america"],
        "Middle East & Africa": [r"africa", r"middle\s+east", r"mena"],
        "Global": [r"global", r"worldwide", r"international"],
        "North America": [r"\bus\b", r"u\.s\.", r"america", r"north\s+america", r"united\s+states"],
    }

    # Position patterns (for debt)
    POSITION_PATTERNS = {
        "First Lien": [r"first\s+lien"],
        "Second Lien": [r"second\s+lien"],
        "Senior Secured": [r"senior\s+secured"],
        "Senior Unsecured": [r"senior\s+unsecured"],
        "Subordinated": [r"subordinated", r"sub\s+debt"],
        "Mezzanine": [r"mezzanine", r"mezz"],
        "Preferred Equity": [r"preferred"],
        "Common Equity": [r"common"],
        "Warrant": [r"warrant"],
    }

    @classmethod
    def detect_investment_type(cls, name: str, context: str = "") -> Optional[str]:
        """Detect investment type from holding name and context."""
        combined = (name + " " + context).lower()

        for inv_type, patterns in cls.INVESTMENT_TYPE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, combined, re.IGNORECASE):
                    return inv_type

        return "Primary"  # Default

    @classmethod
    def detect_investment_purpose(cls, name: str) -> Optional[str]:
        """Detect investment purpose from holding name."""
        name_lower = name.lower()

        for purpose, patterns in cls.INVESTMENT_PURPOSE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, name_lower):
                    return purpose

        return "Other"

    @classmethod
    def detect_geographic_region(cls, name: str) -> Optional[str]:
        """Detect geographic region from holding name."""
        name_lower = name.lower()

        for region, patterns in cls.REGION_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, name_lower):
                    return region

        return "North America"  # Default for US filings

    @classmethod
    def detect_position(cls, name: str) -> Optional[str]:
        """Detect position type (for debt investments)."""
        name_lower = name.lower()

        for position, patterns in cls.POSITION_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, name_lower):
                    return position

        return None

    @classmethod
    def parse_money(cls, value: Any) -> Optional[float]:
        """Parse monetary value to float."""
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            # Handle empty/dash
            value = value.strip()
            if not value or value == "-" or value == "—":
                return None

            # Handle parentheses for negative
            is_negative = "(" in value and ")" in value

            # Remove non-numeric except . and ,
            cleaned = re.sub(r"[^\d.,]", "", value)
            cleaned = cleaned.replace(",", "")

            try:
                result = float(cleaned)
                return -result if is_negative else result
            except ValueError:
                return None

        return None

    @classmethod
    def parse_date(cls, value: Any) -> Optional[date]:
        """Parse date string to date object."""
        if value is None:
            return None

        if isinstance(value, date):
            return value

        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None

            # Try various formats
            formats = [
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%m/%d/%y",
                "%B %d, %Y",
                "%b %d, %Y",
                "%Y%m%d",
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue

        return None

    @classmethod
    def parse_footnotes(cls, value: Any) -> List[str]:
        """Parse footnote references."""
        if not value:
            return []

        if isinstance(value, list):
            return [str(v) for v in value]

        if isinstance(value, str):
            refs = []
            # Match (3), (14), *, **, (a), [1]
            refs.extend(re.findall(r"\((\d+)\)", value))
            refs.extend(re.findall(r"\(([a-zA-Z])\)", value))
            refs.extend(re.findall(r"(\*+)", value))
            refs.extend(re.findall(r"\[(\w+)\]", value))
            return list(set(refs))

        return []

    @classmethod
    def check_restricted(cls, name: str, footnotes: List[str]) -> bool:
        """Check if holding is restricted."""
        # Common indicators
        if "*" in name or "†" in name:
            return True
        if re.search(r"\brestricted\b", name, re.IGNORECASE):
            return True
        if "*" in footnotes or "**" in footnotes:
            return True
        return False

    @classmethod
    def clean_holding_name(cls, name: str) -> str:
        """Clean holding name (remove footnote refs, trailing punctuation)."""
        if not name:
            return ""

        # Remove footnote indicators
        name = re.sub(r"\s*\(\d+\)\s*", "", name)
        name = re.sub(r"\s*\([a-zA-Z]\)\s*", "", name)
        name = re.sub(r"\s*\*+\s*$", "", name)
        name = re.sub(r"\s*†+\s*$", "", name)
        name = re.sub(r"\s*\[\w+\]\s*", "", name)

        # Clean up whitespace
        name = " ".join(name.split())

        return name.strip()

    @classmethod
    def map_holding(
        cls,
        raw: Dict[str, Any],
        validation_result: Optional[Dict[str, Any]] = None,
        current_section: Optional[str] = None,
    ) -> Optional[MappedHolding]:
        """
        Map a raw extracted holding to the database schema.

        Args:
            raw: Raw holding data from Textract or Claude
            validation_result: Validation info for this row (if any)
            current_section: Current section name (e.g., "Primary Investments")

        Returns:
            MappedHolding or None if invalid
        """
        # Get holding name
        name = raw.get("holding_name", "")
        if not name:
            return None

        # Parse values
        fair_value = cls.parse_money(raw.get("fair_value"))
        cost = cls.parse_money(raw.get("cost"))

        # Must have fair value
        if fair_value is None:
            return None

        # Parse other fields
        footnote_refs = cls.parse_footnotes(raw.get("footnote_refs"))
        acquisition_date = cls.parse_date(raw.get("acquisition_date"))
        maturity_date = cls.parse_date(raw.get("maturity_date"))

        # Clean name and check restrictions
        clean_name = cls.clean_holding_name(name)
        is_restricted = cls.check_restricted(name, footnote_refs)

        # Detect classifications
        investment_type = raw.get("investment_type") or cls.detect_investment_type(clean_name, current_section or "")
        investment_purpose = raw.get("investment_purpose") or cls.detect_investment_purpose(clean_name)
        geographic_region = raw.get("geographic_region") or cls.detect_geographic_region(clean_name)
        position = raw.get("position") or cls.detect_position(clean_name)

        # Compute MOIC
        moic = None
        if cost and cost > 0:
            moic = fair_value / cost

        # Determine confidence (0.0-1.0 scale)
        confidence = 1.0
        if validation_result:
            if validation_result.get("issue"):
                confidence = 0.5
            elif validation_result.get("warning"):
                confidence = 0.75

        return MappedHolding(
            holding_name=clean_name,
            fair_value=fair_value,
            cost=cost,
            footnote_refs=footnote_refs if footnote_refs else None,
            investment_type=investment_type,
            investment_purpose=investment_purpose,
            geographic_region=geographic_region,
            position=position,
            reference_rate_spread=raw.get("reference_rate_spread"),
            maturity_date=maturity_date,
            acquisition_date=acquisition_date,
            is_restricted=is_restricted,
            section_name=current_section or raw.get("section_name"),
            section_pct_of_nav=raw.get("section_pct_of_nav"),
            moic=moic,
            raw_textract_row=raw.get("raw_row"),
            extraction_source=raw.get("extraction_source", "textract"),
            confidence=confidence,
        )

    @classmethod
    def map_holdings_batch(
        cls,
        raw_holdings: List[Dict[str, Any]],
        validation_results: Optional[List[Dict[str, Any]]] = None,
        section_headers: Optional[List[Dict[str, Any]]] = None,
    ) -> List[MappedHolding]:
        """
        Map a batch of raw holdings to MappedHolding objects.

        Args:
            raw_holdings: List of raw holding dicts
            validation_results: Validation results from Claude (optional)
            section_headers: Section headers with names and percentages

        Returns:
            List of MappedHolding objects
        """
        # Build validation lookup
        validation_map = {}
        if validation_results:
            for v in validation_results:
                if "row_index" in v:
                    validation_map[v["row_index"]] = v

        # Track current section
        current_section = None
        section_idx = 0
        sorted_sections = sorted(section_headers or [], key=lambda s: s.get("pct_of_nav", 0), reverse=True)

        mapped = []
        for i, raw in enumerate(raw_holdings):
            # Check if we should update section
            if raw.get("section_name"):
                current_section = raw["section_name"]
            elif section_idx < len(sorted_sections):
                current_section = sorted_sections[section_idx].get("section_name")

            validation = validation_map.get(i)
            holding = cls.map_holding(raw, validation, current_section)

            if holding:
                mapped.append(holding)

        logger.info("Mapped holdings", total_raw=len(raw_holdings), mapped=len(mapped))

        return mapped
