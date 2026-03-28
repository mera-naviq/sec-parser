"""
Configuration management for SEC Parser Elite.
All environment variables are loaded here.
"""

import os
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # AWS
    aws_access_key_id: str = Field(..., env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(..., env="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field(default="us-east-2", env="AWS_REGION")
    aws_s3_bucket: str = Field(default="sec-filings-raw-psr-485141927807-us-east-2-an", env="AWS_S3_BUCKET")

    # Anthropic
    anthropic_api_key: str = Field(..., env="ANTHROPIC_API_KEY")

    # Supabase
    supabase_url: str = Field(..., env="SUPABASE_URL")
    supabase_service_role_key: str = Field(..., env="SUPABASE_SERVICE_ROLE_KEY")
    database_url: str = Field(..., env="DATABASE_URL")

    # SEC EDGAR
    sec_user_agent: str = Field(
        default="AltAve/1.0 contact@yourdomain.com",
        env="SEC_USER_AGENT"
    )

    # Pipeline
    max_textract_pages: int = Field(default=150, env="MAX_TEXTRACT_PAGES")
    claude_model: str = Field(default="claude-sonnet-4-20250514", env="CLAUDE_MODEL")
    batch_poll_interval_seconds: int = Field(default=30, env="BATCH_POLL_INTERVAL_SECONDS")

    # Rate limiting
    sec_requests_per_second: float = 10.0
    sec_request_delay: float = 0.1  # 100ms between requests

    # Retry settings
    max_retries: int = 3
    retry_delays: list = [2, 4, 8]  # exponential backoff

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Allow extra env vars like RUN_INTEGRATION_TESTS


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get or create settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


# Constants
FILING_STATUS = {
    "PENDING": "pending",
    "FETCHING": "fetching",
    "CONVERTING": "converting",
    "EXTRACTING": "extracting",
    "PARSING": "parsing",
    "VALIDATING": "validating",
    "COMPLETE": "complete",
    "FAILED": "failed",
}

INVESTMENT_TYPES = [
    "Primary",
    "Secondary",
    "Co-Investment",
    "Direct",
    "Short-Term",
    "Other",
]

INVESTMENT_PURPOSES = [
    "Buyouts",
    "Venture Capital",
    "Growth Equity",
    "Debt/Credit",
    "Real Estate",
    "Infrastructure",
    "Natural Resources",
    "Distressed",
    "Fund of Funds",
    "Hedge Fund",
    "Other",
]

GEOGRAPHIC_REGIONS = [
    "North America",
    "Europe",
    "Asia-Pacific",
    "Latin America",
    "Middle East & Africa",
    "Global",
]

EXTRACTION_SOURCES = ["textract", "claude", "manual"]
CONFIDENCE_LEVELS = ["high", "medium", "low"]
