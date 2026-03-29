"""
SEC Parser Elite - Main Entry Point
Provides both CLI and FastAPI health check endpoint for Railway.
"""

import asyncio
import sys
import uuid
from typing import Optional, List
from datetime import datetime

import click
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from uvicorn import Config, Server

# Load environment variables
load_dotenv()

# Simple logging setup
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# FastAPI app for health checks and parse API
app = FastAPI(title="SEC Parser Elite")

# Add CORS middleware for dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your dashboard domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job tracking (for simplicity - could use Redis for persistence)
parse_jobs: dict = {}


# Pydantic models for API
class ParseRequest(BaseModel):
    urls: List[str]
    concurrency: int = 3


class ParseJobStatus(BaseModel):
    job_id: str
    status: str  # pending, running, completed, failed
    total: int
    completed: int
    failed: int
    current_url: Optional[str] = None
    results: List[dict] = []
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None


@app.get("/health")
async def health_check():
    """Health check endpoint for Railway - simple and fast."""
    return {"status": "ok"}


@app.get("/health/imports")
async def health_check_imports():
    """Test if all modules can be imported."""
    results = {}

    # Test config import
    try:
        from config import get_settings
        settings = get_settings()
        results["config"] = "ok"
    except Exception as e:
        results["config"] = f"error: {e}"

    # Test db import
    try:
        from db import SupabaseClient
        results["db"] = "ok"
    except Exception as e:
        results["db"] = f"error: {e}"

    # Test pipeline import
    try:
        from pipeline import PipelineOrchestrator
        results["pipeline"] = "ok"
    except Exception as e:
        results["pipeline"] = f"error: {e}"

    return {"imports": results}


@app.get("/health/db")
async def health_check_db():
    """Deep health check including database connectivity."""
    try:
        from db import SupabaseClient
        db = SupabaseClient()
        await db.connect()
        db_ok = await db.health_check()
        await db.close()
        return {"status": "ok", "db": "connected" if db_ok else "error"}
    except Exception as e:
        import traceback
        error_tb = traceback.format_exc()
        logger.error(f"Database health check failed: {e}")
        return {"status": "error", "db": str(e), "traceback": error_tb}


@app.post("/parse")
async def start_parse_job(request: ParseRequest, background_tasks: BackgroundTasks):
    """Start a new parsing job for multiple URLs."""
    if not request.urls:
        raise HTTPException(status_code=400, detail="No URLs provided")

    # Filter valid URLs
    valid_urls = [url.strip() for url in request.urls if url.strip().startswith("http")]
    if not valid_urls:
        raise HTTPException(status_code=400, detail="No valid URLs found")

    # Create job
    job_id = str(uuid.uuid4())[:8]
    parse_jobs[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "total": len(valid_urls),
        "completed": 0,
        "failed": 0,
        "current_url": None,
        "results": [],
        "started_at": None,
        "completed_at": None,
        "error": None,
        "urls": valid_urls,
        "concurrency": min(request.concurrency, 5),  # Cap at 5
    }

    # Start background processing
    background_tasks.add_task(run_parse_job, job_id)

    return {"job_id": job_id, "total_urls": len(valid_urls), "status": "pending"}


@app.get("/parse/{job_id}")
async def get_parse_job_status(job_id: str):
    """Get the status of a parsing job."""
    if job_id not in parse_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = parse_jobs[job_id]
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "total": job["total"],
        "completed": job["completed"],
        "failed": job["failed"],
        "current_url": job["current_url"],
        "results": job["results"],
        "started_at": job["started_at"],
        "completed_at": job["completed_at"],
        "error": job["error"],
    }


@app.get("/parse")
async def list_parse_jobs():
    """List all parsing jobs."""
    return {
        "jobs": [
            {
                "job_id": job["job_id"],
                "status": job["status"],
                "total": job["total"],
                "completed": job["completed"],
                "failed": job["failed"],
                "started_at": job["started_at"],
                "completed_at": job["completed_at"],
            }
            for job in parse_jobs.values()
        ]
    }


async def run_parse_job(job_id: str):
    """Background task to run the parsing job."""
    from pipeline import PipelineOrchestrator

    job = parse_jobs[job_id]
    job["status"] = "running"
    job["started_at"] = datetime.utcnow().isoformat()

    try:
        async with PipelineOrchestrator() as pipeline:
            for i, url in enumerate(job["urls"]):
                job["current_url"] = url

                try:
                    result = await pipeline.run_filing(url)

                    if result.success:
                        job["completed"] += 1
                        job["results"].append({
                            "url": url,
                            "status": "ok",
                            "filing_id": str(result.filing_id),
                            "holdings_count": result.holdings_count,
                            "confidence_score": result.confidence_score,
                        })
                    else:
                        job["failed"] += 1
                        job["results"].append({
                            "url": url,
                            "status": "failed",
                            "error": result.error,
                        })
                except Exception as e:
                    job["failed"] += 1
                    job["results"].append({
                        "url": url,
                        "status": "failed",
                        "error": str(e),
                    })

        job["status"] = "completed"
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
    finally:
        job["current_url"] = None
        job["completed_at"] = datetime.utcnow().isoformat()


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
        click.echo(f"\n[OK] Success: {result.holdings_count} holdings extracted")
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
        icon = "[OK]" if status == "ok" else "[FAIL]" if status == "failed" else "[...]"
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
