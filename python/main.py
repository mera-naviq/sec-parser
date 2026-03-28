"""
SEC Parser Elite - Main Entry Point
Provides both CLI and FastAPI health check endpoint for Railway.
"""

import asyncio
import sys
from typing import Optional

import click
import structlog
from dotenv import load_dotenv
from fastapi import FastAPI
from uvicorn import Config, Server

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# FastAPI app for health checks
app = FastAPI(title="SEC Parser Elite")


@app.get("/health")
async def health_check():
    """Health check endpoint for Railway."""
    from db import SupabaseClient

    db = SupabaseClient()
    try:
        await db.connect()
        db_ok = await db.health_check()
        await db.close()
        return {"status": "ok", "db": "connected" if db_ok else "error"}
    except Exception as e:
        return {"status": "error", "db": str(e)}


@click.group()
def cli():
    """SEC Parser Elite - N-CSR/N-CSRS Filing Parser"""
    pass


@cli.command()
@click.option("--url", required=True, help="SEC filing URL")
def filing(url: str):
    """Parse a single filing by URL."""
    asyncio.run(_run_single_filing(url))


@cli.command()
@click.option("--accession", required=True, help="SEC accession number (format: XXXXXXXXXX-XX-XXXXXX)")
def accession(accession: str):
    """Parse a single filing by accession number."""
    asyncio.run(_run_by_accession(accession))


@cli.command()
@click.option("--cik", required=True, help="Company CIK")
@click.option("--limit", default=10, help="Maximum filings to process")
def cik(cik: str, limit: int):
    """Parse all N-CSR filings for a CIK."""
    asyncio.run(_run_for_cik(cik, limit))


@cli.command()
@click.option("--file", "filepath", required=True, help="File with URLs (one per line)")
@click.option("--concurrency", default=3, help="Number of concurrent processes")
def batch(filepath: str, concurrency: int):
    """Parse multiple filings from a file."""
    asyncio.run(_run_batch(filepath, concurrency))


@cli.command()
@click.option("--id", "filing_id", required=True, help="Filing ID to retry")
def retry(filing_id: str):
    """Retry a failed filing by ID."""
    asyncio.run(_run_retry(filing_id))


@cli.command()
def status():
    """Show status of all filings."""
    asyncio.run(_show_status())


@cli.command()
@click.option("--port", default=8000, help="Port for health check server")
def serve(port: int):
    """Start the health check server."""
    config = Config(app=app, host="0.0.0.0", port=port, log_level="info")
    server = Server(config)
    asyncio.run(server.serve())


async def _run_single_filing(url: str):
    """Run pipeline on a single filing."""
    from pipeline import PipelineOrchestrator

    logger.info("Starting single filing parse", url=url)

    async with PipelineOrchestrator() as pipeline:
        result = await pipeline.run_filing(url)

    if result.success:
        logger.info(
            "Filing parsed successfully",
            filing_id=result.filing_id,
            holdings_count=result.holdings_count,
            confidence_score=result.confidence_score,
        )
        click.echo(f"\n✓ Success: {result.holdings_count} holdings extracted")
        click.echo(f"  Filing ID: {result.filing_id}")
        click.echo(f"  Confidence: {result.confidence_score:.1f}%")

        if result.warnings:
            click.echo(f"\n  Warnings:")
            for w in result.warnings:
                click.echo(f"    - {w}")
    else:
        logger.error("Filing parse failed", error=result.error)
        click.echo(f"\n✗ Failed: {result.error}", err=True)
        sys.exit(1)


async def _run_by_accession(accession_number: str):
    """Run pipeline on a filing by accession number."""
    from edgar import SecApi

    logger.info("Looking up filing by accession", accession=accession_number)

    # Find the filing URL
    # We need to search or know the CIK
    click.echo(f"Looking up accession {accession_number}...")
    click.echo("Note: This requires knowing the CIK. Use --url or --cik instead.")
    sys.exit(1)


async def _run_for_cik(cik_value: str, limit: int):
    """Run pipeline on all N-CSR filings for a CIK."""
    from edgar import SecApi
    from pipeline import PipelineOrchestrator
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

    logger.info("Processing filings for CIK", cik=cik_value, limit=limit)

    async with SecApi() as api:
        filings = await api.get_ncsr_filings_for_cik(cik_value, limit=limit)

    if not filings:
        click.echo(f"No N-CSR filings found for CIK {cik_value}")
        return

    click.echo(f"Found {len(filings)} filings for CIK {cik_value}")

    urls = [f["url"] for f in filings]

    async with PipelineOrchestrator() as pipeline:
        results = await pipeline.run_batch(urls, concurrency=3)

    # Summary
    ok = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)

    click.echo(f"\n{'='*50}")
    click.echo(f"Processed: {len(results)}")
    click.echo(f"  ✓ Success: {ok}")
    click.echo(f"  ✗ Failed: {failed}")


async def _run_batch(filepath: str, concurrency: int):
    """Run pipeline on multiple filings from a file."""
    from pipeline import PipelineOrchestrator

    # Read URLs from file
    with open(filepath) as f:
        urls = [line.strip() for line in f if line.strip() and line.startswith("http")]

    if not urls:
        click.echo("No valid URLs found in file")
        sys.exit(1)

    click.echo(f"Processing {len(urls)} filings with concurrency {concurrency}")

    async with PipelineOrchestrator() as pipeline:
        results = await pipeline.run_batch(urls, concurrency=concurrency)

    # Summary
    ok = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)
    total_holdings = sum(r.holdings_count for r in results if r.success)

    click.echo(f"\n{'='*50}")
    click.echo(f"Processed: {len(results)}")
    click.echo(f"  ✓ Success: {ok}")
    click.echo(f"  ✗ Failed: {failed}")
    click.echo(f"  Total Holdings: {total_holdings}")

    if failed > 0:
        click.echo("\nFailed filings:")
        for i, r in enumerate(results):
            if not r.success:
                click.echo(f"  {urls[i]}: {r.error}")


async def _run_retry(filing_id: str):
    """Retry a failed filing."""
    from db import SupabaseClient
    from pipeline import PipelineOrchestrator

    logger.info("Retrying filing", filing_id=filing_id)

    db = SupabaseClient()
    await db.connect()

    filing = await db.get_filing(filing_id)
    if not filing:
        click.echo(f"Filing not found: {filing_id}")
        await db.close()
        sys.exit(1)

    url = filing["sec_filing_url"]
    await db.close()

    click.echo(f"Retrying: {url}")

    await _run_single_filing(url)


async def _show_status():
    """Show status of all filings."""
    from db import SupabaseClient

    db = SupabaseClient()
    await db.connect()

    summary = await db.get_filings_summary()
    await db.close()

    click.echo("\nFiling Status Summary:")
    click.echo("-" * 30)

    total = 0
    for status, count in sorted(summary.items()):
        icon = "✓" if status == "complete" else "✗" if status == "failed" else "○"
        click.echo(f"  {icon} {status}: {count}")
        total += count

    click.echo("-" * 30)
    click.echo(f"  Total: {total}")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No arguments - start the health check server
        cli(["serve"])
    else:
        cli()
