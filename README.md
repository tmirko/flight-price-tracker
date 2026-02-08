# flight-price-tracker

Track flight price changes for a single route via SerpApi (Google Flights), persist run history to Parquet, and generate a Markdown report with links to raw evidence JSON + SHA256.

## Prerequisites

- Python 3.10+
- [`uv`](https://github.com/astral-sh/uv)
- (Optional) Node.js 18+ / npm (only needed for the Evidence.dev UI)

## Setup

1. Install Python deps:
   ```bash
   uv sync --dev
   ```

2. Create `.env` with your SerpApi key:
   ```bash
   cp .env.example .env
   # edit .env and set SERPAPI_API_KEY=...
   ```

3. Edit `config.yaml` (route, currency, filters, etc.).

## Run the tracker (CLI)

Run one tracking execution:

```bash
uv run flight-price-tracker run --config config.yaml
```

Outputs:

- Parquet: `data/flight_price_tracker/`
- Raw evidence: `evidence/route=.../run_date=.../*.json` + `*.sha256`
- Reports: `reports/latest.md` and (optionally) `reports/YYYY-MM-DD.md`

## Open the Evidence.dev UI (local browser)

The Evidence UI reads the tracker Parquet files via DuckDB (`read_parquet(...)`) and serves evidence JSON files from `evidence/`.

```bash
cd evidence_ui
npm ci
npm run sources -- --sources flight_price_tracker
npm run dev
```

Then open: http://localhost:3000

## Automation (GitHub Actions)

`.github/workflows/track.yml` runs the tracker on a schedule. Configure the repo secret `SERPAPI_API_KEY` for it to work.