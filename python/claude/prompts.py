"""
Claude Prompt Templates
All prompts for the SEC N-CSR parsing pipeline.
"""


class PromptTemplates:
    """Collection of prompt templates for Claude batch processing."""

    @staticmethod
    def cover_page_metadata(html_snippet: str) -> str:
        """Prompt for extracting cover page metadata."""
        return f"""Extract fund metadata from this SEC N-CSR/N-CSRS filing cover page.

Return ONLY valid JSON with no preamble, no markdown code fences, no explanation.

Expected JSON schema:
{{
    "fund_name": "string - Full legal name of the fund",
    "cik": "string - SEC Central Index Key (10 digits)",
    "report_type": "string - Either 'N-CSR' or 'N-CSRS'",
    "report_period_end": "string - Date in YYYY-MM-DD format",
    "fiscal_year_end": "string or null - Month/day like 'September 30' or 'December 31'",
    "period_label": "string or null - e.g. 'Annual Report', 'Semi-Annual Report'",
    "manager_name": "string or null - Investment manager/adviser name",
    "manager_address": "string or null - Manager business address"
}}

If a value is not present in the document, use null. Do not guess or hallucinate values.

Document excerpt:
{html_snippet}"""

    @staticmethod
    def footnotes(footnotes_html: str) -> str:
        """Prompt for extracting and categorizing footnotes."""
        return f"""Extract footnotes from this SEC N-CSR filing's Schedule of Investments.

Return ONLY valid JSON with no preamble, no markdown code fences, no explanation.

Expected JSON schema:
{{
    "footnotes": {{
        "1": {{
            "text": "Full footnote text",
            "is_foreign_currency": boolean,
            "is_restricted_security": boolean,
            "is_continuation_fund": boolean,
            "is_fair_value_methodology": boolean
        }},
        "2": {{ ... }},
        "*": {{ ... }},
        "**": {{ ... }}
    }}
}}

Rules:
- Keys are the footnote identifiers as they appear (numbers, letters, asterisks)
- is_foreign_currency: true if footnote mentions foreign currency denomination
- is_restricted_security: true if footnote mentions securities that cannot be freely sold
- is_continuation_fund: true if footnote mentions continuation vehicle or successor fund
- is_fair_value_methodology: true if footnote explains valuation methodology
- If a value is not present, use null. Do not guess or hallucinate values.

Document excerpt:
{footnotes_html}"""

    @staticmethod
    def notes_financial_statements(notes_html: str) -> str:
        """Prompt for extracting data from Notes to Financial Statements."""
        return f"""Extract key information from the Notes to Financial Statements section of this SEC N-CSR filing.

Return ONLY valid JSON with no preamble, no markdown code fences, no explanation.

Expected JSON schema:
{{
    "organization_description": "string or null - 1-2 sentence description of fund organization and structure",

    "valuation_hierarchy": {{
        "level_1_description": "string or null - What qualifies as Level 1",
        "level_2_description": "string or null - What qualifies as Level 2",
        "level_3_description": "string or null - What qualifies as Level 3",
        "level_3_percentage": "number or null - Percentage of assets in Level 3"
    }},

    "credit_facility": {{
        "lender": "string or null - Name of lending institution",
        "commitment_amount": "number or null - Total commitment in USD",
        "outstanding_balance": "number or null - Amount drawn in USD",
        "interest_rate": "string or null - Rate description (e.g., 'SOFR + 1.75%')",
        "maturity_date": "string or null - Date in YYYY-MM-DD format"
    }},

    "unfunded_commitments": [
        {{
            "holding_name": "string - Name of the investment",
            "unfunded_commitment_usd": "number - Amount in USD"
        }}
    ],

    "fee_structure": {{
        "management_fee_rate": "string or null - e.g., '1.50% annually'",
        "incentive_fee_rate": "string or null - e.g., '20% of profits'",
        "incentive_hurdle": "string or null - e.g., '8% preferred return'"
    }},

    "significant_events": [
        "string - Description of any material events disclosed"
    ]
}}

If a value is not present in the document, use null. Do not guess or hallucinate values.
For arrays, use empty array [] if no items found.

Document excerpt:
{notes_html}"""

    @staticmethod
    def financial_statements(tables_text: str) -> str:
        """Prompt for mapping financial statement line items."""
        return f"""Map the financial statement data from this SEC N-CSR filing to the schema below.

Return ONLY valid JSON with no preamble, no markdown code fences, no explanation.

Expected JSON schema:
{{
    "assets_and_liabilities": {{
        "investments_at_fair_value": "number or null - Total investments at fair value",
        "cash_and_equivalents": "number or null - Cash and cash equivalents",
        "total_assets": "number or null",
        "credit_facility_borrowings": "number or null - Amounts borrowed under credit facility",
        "management_fees_payable": "number or null",
        "incentive_fees_payable": "number or null",
        "redemptions_payable": "number or null - Amounts due to departing investors",
        "total_liabilities": "number or null",
        "net_assets": "number or null - Net assets (should equal total_assets - total_liabilities)"
    }},

    "operations": {{
        "total_investment_income": "number or null - Dividends, interest, etc.",
        "management_fees": "number or null",
        "incentive_fees": "number or null",
        "interest_expense": "number or null",
        "total_expenses": "number or null",
        "net_investment_income_loss": "number or null",
        "net_realized_gain_loss": "number or null",
        "net_change_unrealized": "number or null - Change in unrealized appreciation/depreciation",
        "net_increase_from_operations": "number or null"
    }},

    "capital_activity": {{
        "capital_contributions": "number or null",
        "capital_distributions": "number or null",
        "net_assets_beginning": "number or null - Net assets at start of period",
        "net_assets_end": "number or null - Net assets at end of period"
    }},

    "cash_flows": {{
        "purchases_of_investments": "number or null",
        "proceeds_from_realizations": "number or null",
        "proceeds_from_borrowings": "number or null",
        "repayment_of_borrowings": "number or null",
        "net_change_in_cash": "number or null"
    }}
}}

Rules:
- All monetary values should be numbers in USD (no currency symbols or commas)
- Negative values should be negative numbers (not in parentheses)
- If a value is not present, use null. Do not guess or hallucinate values.
- Match line items by meaning, not exact wording (e.g., "Total investments" = "investments_at_fair_value")

Financial Statement Tables:
{tables_text}"""

    @staticmethod
    def schedule_validation(
        schedule_rows: str, subtotals: str, section_headers: str
    ) -> str:
        """Prompt for validating Textract extraction."""
        return f"""Validate the Schedule of Investments extraction from this SEC N-CSR filing.

You are given:
1. Extracted holding rows with cost and fair value
2. Section subtotals as stated in the document
3. Section headers with stated percentages of NAV

Your task:
1. Verify that sum of costs matches each section's stated total cost
2. Verify that sum of fair values matches each section's stated total fair value
3. Identify any rows where values appear misaligned (e.g., cost in fair value column)
4. Check for duplicate rows or missing data
5. Provide an overall confidence score

Return ONLY valid JSON with no preamble, no markdown code fences, no explanation.

Expected JSON schema:
{{
    "validation_results": [
        {{
            "section_name": "string",
            "calculated_total_cost": "number",
            "stated_total_cost": "number or null",
            "cost_matches": "boolean",
            "calculated_total_fair_value": "number",
            "stated_total_fair_value": "number or null",
            "fair_value_matches": "boolean"
        }}
    ],

    "misaligned_rows": [
        {{
            "row_index": "number - 0-based index in the schedule_rows array",
            "holding_name": "string",
            "issue": "string - Description of the misalignment",
            "suggested_fix": "string or null - How to correct it"
        }}
    ],

    "warnings": [
        "string - Any other issues found"
    ],

    "confidence_score": "number - 0 to 100, where 100 is perfect extraction"
}}

Rules:
- Allow 0.5% tolerance for rounding when comparing sums to stated totals
- If a field is not present, use null. Do not guess or hallucinate values.

EXTRACTED SCHEDULE ROWS:
{schedule_rows}

STATED SUBTOTALS:
{subtotals}

SECTION HEADERS:
{section_headers}"""

    @staticmethod
    def full_schedule_extraction(html: str) -> str:
        """
        Primary prompt for Claude-based holdings extraction.
        Designed to accurately distinguish individual holdings from summary tables.
        """
        return f"""Extract the complete Schedule of Investments from this SEC N-CSR filing.

CRITICAL: You must distinguish between:
1. INDIVIDUAL HOLDINGS (what we want): Specific fund names like "Advent International GPE X", "Apollo Investment Fund IX", "Berkshire Fund IX, L.P."
2. SUMMARY/ALLOCATION TABLES (skip these): Categories like "Buyouts", "Venture Capital", "United States", "Europe", "Asia" - these are aggregates, NOT holdings

Return ONLY valid JSON with no preamble, no markdown code fences, no explanation.

Expected JSON schema:
{{
    "holdings": [
        {{
            "holding_name": "string - SPECIFIC fund/investment name (e.g., 'Berkshire Fund IX, L.P.', 'Apollo Overseas Partners IX')",
            "investment_type": "string or null - Primary, Secondary, Co-Investment, Direct",
            "investment_purpose": "string or null - Buyouts, Venture Capital, Debt/Credit, Growth Equity, etc.",
            "geographic_region": "string or null - North America, Europe, Asia-Pacific, etc.",
            "acquisition_date": "string or null - Date in YYYY-MM-DD format",
            "maturity_date": "string or null - Date in YYYY-MM-DD format (for debt)",
            "cost": "number or null - Original cost in USD",
            "fair_value": "number - Current fair value in USD (required)",
            "footnote_refs": ["string"] - Array of footnote references like ['3', '14', '*', 'a', 'd'],
            "is_restricted": "boolean - true if marked with asterisk or restricted notation",
            "section_name": "string or null - e.g., 'Primary Investments', 'Secondary Investments', 'Direct Co-Investments'"
        }}
    ],

    "section_subtotals": [
        {{
            "section_name": "string",
            "total_cost": "number or null",
            "total_fair_value": "number or null",
            "pct_of_nav": "number or null - Percentage of net assets"
        }}
    ],

    "value_scale": "string - 'actual' if values in dollars, 'thousands' if in thousands, 'millions' if in millions"
}}

CRITICAL RULES:
1. ONLY extract rows that represent SPECIFIC NAMED INVESTMENTS (fund names, LP names, company names)
2. DO NOT extract rows from summary tables like:
   - "Investment Purpose Allocation" (Buyouts, Venture Capital, Growth, etc.)
   - "Geographic Allocation" (United States, Europe, Asia, etc.)
   - "Industry Classification" tables
   - Any table that shows totals BY CATEGORY rather than individual holdings
3. Holdings have SPECIFIC NAMES like:
   - "EQT X CO-INVESTMENT (K) SCSP"
   - "Advent International GPE X-D SCSp"
   - "Blackstone Capital Partners VIII L.P."
   - "Carlyle Partners VII, L.P."
4. Holdings DO NOT have generic names like:
   - "Buyouts" (this is a category)
   - "United States" (this is a geography)
   - "Venture Capital" (this is a purpose)
5. Look for the main "Schedule of Investments" or "Consolidated Schedule of Investments" table
6. The Schedule typically has columns: Investment/Description, Geographic Region, Acquisition Date, Cost, Fair Value, % of NAV, Footnotes
7. fair_value is REQUIRED for each holding - skip rows without a fair value
8. If a value is not present, use null. Do not guess or hallucinate values.
9. Extract ALL individual holdings - there may be dozens to hundreds
10. Look for indicators like "(in thousands)", "(000s)" to determine value_scale

Document:
{html[:120000]}"""
