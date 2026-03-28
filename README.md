# SEC Parser Elite

A robust, production-grade N-CSR filing parser for SEC EDGAR filings. Uses a hybrid extraction approach combining AWS Textract for structured table extraction with Claude AI for semantic parsing.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SEC Parser Elite                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │  EDGAR   │───▶│  HTML    │───▶│ Textract │───▶│  Claude  │              │
│  │  Fetch   │    │  → PDF   │    │ Extract  │    │  Batch   │              │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘              │
│       │                                               │                      │
│       ▼                                               ▼                      │
│  ┌──────────┐                                   ┌──────────┐                │
│  │ Metadata │                                   │  Parse   │                │
│  │ Extract  │                                   │   Map    │                │
│  └──────────┘                                   └──────────┘                │
│       │                                               │                      │
│       └───────────────────┬───────────────────────────┘                      │
│                           ▼                                                  │
│                     ┌──────────┐    ┌──────────┐    ┌──────────┐           │
│                     │ Validate │───▶│   DB     │───▶│ Supabase │           │
│                     │  Reconcile    │  Write   │    │ Postgres │           │
│                     └──────────┘    └──────────┘    └──────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Pipeline Steps

1. **EDGAR Fetch** - Download N-CSR/N-CSRS filing HTML from SEC with rate limiting
2. **HTML → PDF** - Convert HTML to PDF using Playwright headless Chromium
3. **Textract Extract** - Upload PDF to S3, run AWS Textract for table extraction
4. **Claude Batch** - Process tables through Claude Message Batches API (50% cost savings)
5. **Parse & Map** - Map extracted data to typed holdings, financials, footnotes
6. **Validate** - Reconcile totals, check for duplicates, compute confidence scores
7. **DB Write** - Write to Supabase PostgreSQL with transaction safety

## Prerequisites

- **Python 3.12+**
- **Node.js 20+** (for CLI)
- **Docker** (for local development)
- **AWS Account** with Textract and S3 access
- **Anthropic API Key** for Claude
- **Supabase Project** or PostgreSQL database

## Setup

### 1. Clone and Configure

```bash
git clone <repository-url>
cd sec-parser

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
```

### 2. Environment Variables

```env
# AWS Textract
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
AWS_S3_BUCKET=your-bucket-name

# Anthropic
ANTHROPIC_API_KEY=sk-ant-...

# Supabase/PostgreSQL
SUPABASE_URL=https://xxx.supabase.co
DATABASE_URL=postgresql://postgres:password@db.xxx.supabase.co:5432/postgres

# SEC EDGAR
SEC_USER_AGENT=YourCompany/1.0 contact@yourdomain.com

# Optional
MAX_TEXTRACT_PAGES=150
CLAUDE_MODEL=claude-sonnet-4-20250514
```

### 3. Database Setup

Run the migration to create tables:

```bash
# Using psql
psql $DATABASE_URL -f migrations/001_initial_schema.sql

# Or using Supabase SQL Editor
# Copy contents of migrations/001_initial_schema.sql
```

### 4. Install Dependencies

```bash
# Python
cd python
pip install -r requirements.txt
playwright install chromium

# Node.js CLI
cd ../node
npm install
npm run build
```

### 5. Local Development with Docker

```bash
docker-compose up -d

# View logs
docker-compose logs -f parser
```

## CLI Usage

### Parse Single Filing

```bash
# Using Python directly
cd python
python main.py filing "https://www.sec.gov/Archives/edgar/data/1689813/000168981324000003/ncsrs.htm"

# Using Node.js CLI
cd node
npm run cli filing "https://www.sec.gov/Archives/edgar/data/1689813/000168981324000003/ncsrs.htm"
```

### Parse All Filings for a CIK

```bash
python main.py cik 1689813 --limit 10
```

### Batch Processing

```bash
# Create a file with URLs (one per line)
echo "https://www.sec.gov/Archives/edgar/..." > urls.txt
echo "https://www.sec.gov/Archives/edgar/..." >> urls.txt

# Run batch
python main.py batch urls.txt --concurrency 3
```

### Retry Failed Filings

```bash
python main.py retry
```

### Check Status

```bash
python main.py status
```

## API Endpoints

When deployed, the service exposes:

- `GET /health` - Health check endpoint (used by Railway)

## Running Tests

```bash
cd python

# Unit tests
pytest tests/ -v

# Integration tests (requires credentials)
pytest tests/ -v -m integration

# With coverage
pytest tests/ --cov=. --cov-report=html
```

## Deployment

### Railway

1. Connect your repository to Railway
2. Set environment variables in Railway dashboard
3. Deploy - Railway will use `railway.toml` configuration

```bash
# Manual deploy
railway up
```

### Docker

```bash
# Build image
docker build -t sec-parser .

# Run
docker run -d \
  --env-file .env \
  -p 8080:8080 \
  sec-parser
```

## Cost Estimates

### Per Filing (Typical 50-page N-CSR)

| Service | Cost |
|---------|------|
| AWS Textract | ~$0.75 (50 pages × $0.015/page) |
| Claude Batch | ~$0.15 (5 requests × ~2K tokens avg) |
| S3 Storage | ~$0.001 |
| **Total** | **~$0.90/filing** |

### Monthly Estimates

| Volume | Cost |
|--------|------|
| 100 filings | ~$90 |
| 500 filings | ~$450 |
| 1,000 filings | ~$900 |

*Note: Claude Batch API provides 50% discount vs standard API calls*

## Database Schema

### Core Tables

- **filings** - Filing metadata, status, accession numbers
- **holdings** - Individual investment holdings with full attributes
- **financial_statements** - Balance sheet, income, cash flow data
- **section_subtotals** - Per-section cost/value totals
- **filing_footnotes** - Footnote definitions and classifications
- **unfunded_commitments** - Unfunded capital commitments
- **pipeline_runs** - Pipeline execution history

### Key Indexes

- `filings.sec_accession_number` - Unique, for idempotent upserts
- `holdings.fund_name` - For fund-level queries
- `holdings.holding_name` - For cross-fund analysis

## Troubleshooting

### SEC Rate Limiting

If you see "Undeclared Automated Tool" errors:
- Ensure `SEC_USER_AGENT` is set with valid contact info
- The pipeline automatically retries with exponential backoff

### Textract Failures

- Check AWS credentials have `textract:*` and `s3:*` permissions
- Verify S3 bucket exists and is in same region
- PDF page count may exceed limit (default 150)

### Claude Batch Timeouts

- Batches can take up to 24 hours for large jobs
- Use `status` command to check progress
- Failed requests are automatically retried

### Database Connection Issues

- Verify `DATABASE_URL` uses direct connection (not pooler for transactions)
- Check Supabase connection limits
- Ensure migrations have been run

## Project Structure

```
sec-parser/
├── python/
│   ├── config.py           # Settings and constants
│   ├── main.py             # CLI entry point + FastAPI health
│   ├── edgar/
│   │   ├── fetcher.py      # HTML fetching with rate limiting
│   │   └── sec_api.py      # SEC EDGAR API client
│   ├── textract/
│   │   ├── converter.py    # HTML → PDF with Playwright
│   │   └── extractor.py    # AWS Textract integration
│   ├── claude/
│   │   ├── batch.py        # Message Batches API
│   │   ├── parser.py       # Response parsing
│   │   └── prompts.py      # Prompt templates
│   ├── pipeline/
│   │   ├── orchestrator.py # Main pipeline logic
│   │   ├── mapper.py       # Data mapping
│   │   └── validator.py    # Validation & reconciliation
│   ├── db/
│   │   └── supabase.py     # PostgreSQL client
│   └── tests/
├── node/
│   └── src/
│       └── cli.ts          # Node.js CLI wrapper
├── migrations/
│   └── 001_initial_schema.sql
├── Dockerfile
├── docker-compose.yml
├── railway.toml
└── README.md
```

## License

Proprietary - AltAve

## Support

For issues, contact the development team or open an issue in the repository.
