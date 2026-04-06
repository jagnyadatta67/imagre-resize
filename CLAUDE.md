# bulk-cloudinary — Claude Code Guide

## Project Overview

Bulk image processing pipeline for Lifestyle Stores (e-commerce). Downloads product images from **Azure Blob Storage**, transforms them to 1450×2100px via **Cloudinary**, and re-uploads to Azure. Tracks all state in **MySQL** with a crash-safe, resumable worker queue. A **Flask UI** supports browsing results and manual reprocessing.

---

## Architecture

```
input_csv/ ──> csv_loader.py ──> MySQL queue (sku_queue)
                                      │
                              ThreadPoolExecutor workers
                                      │
                              azure_client.py (download)
                                      │
                              converter.py (Cloudinary transform)
                                      │
                              azure_client.py (upload to lifestyle-converted/)
                                      │
                              MySQL results (sku_results, image_results)
```

**Key design decisions:**
- Pull model: workers atomically claim tasks with `UPDATE ... LIMIT 1` — no duplicate processing
- All image bytes handled in-memory (`BytesIO`) — no temp files on disk
- Crash-safe: `running` rows are re-queued on next run; `done` rows are skipped

---

## Module Map

| File | Role |
|------|------|
| `run.py` | CLI entry point, spawns worker threads |
| `config.py` | Central config from `.env` |
| `modules/db.py` | MySQL schema + atomic queue/results persistence |
| `modules/azure_client.py` | Azure Blob Storage I/O (thread-safe singleton) |
| `modules/converter.py` | Cloudinary transformation pipeline |
| `modules/csv_loader.py` | CSV parsing with SKU deduplication |
| `ui/app.py` | Flask web UI (business + developer views) |
| `fetch_skus_from_mongo.py` | Export SKU lists from Content Central MongoDB |
| `fetch_skus_from_unbxd.py` | Fetch SKUs via Unbxd search API |
| `reprocess_fill.py` | Targeted reprocessing with `crop:fill,gravity:auto` |
| `test_cloudinary.py` | Debug utility for building/testing Cloudinary URLs |
| `download_images_from_azure.py` | Bulk Azure download for local inspection |

---

## Running the Project

### Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in credentials
python -c "from modules import db; db.init_db()"
```

### Bulk Processing
```bash
python run.py <csv_folder> [--workers N] [--pad-mode auto|white|gen_fill|no] [--dry-run]

# Examples:
python run.py input_csv --workers 10
python run.py input_csv_KIDS --workers 5 --pad-mode no
python run.py input_csv --dry-run
```

### Resume After Crash
Re-run the same command — completed SKUs are skipped, `running` rows are re-queued.

### Flask UI
```bash
cd ui && flask run --host 0.0.0.0 --port 5000
```

### Utilities
```bash
# Fetch SKUs from MongoDB
python fetch_skus_from_mongo.py --from-date 2024-01-01 --category women

# Reprocess specific images
python reprocess_fill.py --input input_reprocess/text_heavy.csv --workers 10
python reprocess_fill.py --from-db   # all images marked text_heavy in MySQL

# Test a Cloudinary transform
python test_cloudinary.py --url "https://..." --transform "w:1450,h:1450,crop:fit"
```

---

## Environment Variables (`.env`)

```
AZURE_ACCOUNT_NAME=
AZURE_ACCOUNT_KEY=
CLOUDINARY_CLOUD_NAME=
CLOUDINARY_API_KEY=
CLOUDINARY_API_SECRET=
MYSQL_HOST=
MYSQL_PORT=3306
MYSQL_DB=
MYSQL_USER=
MYSQL_PASSWORD=
TARGET_W=1450
TARGET_H=2100
BULK_WORKERS=10
```

**Never commit `.env`.** The `config.py` file has hardcoded fallback defaults for some credentials — do not rely on those defaults; always use `.env`.

---

## Known Issues & Risks

1. **Hardcoded credential fallbacks in `config.py`** — real API keys are in default values. Must be removed; credentials should only come from `.env`.
2. **No retry/backoff for Cloudinary 429** — pipeline stops on rate limit. Consider exponential backoff before re-queuing.
3. **No unit tests** — only manual E2E validation. Adding tests for `converter.py` and `csv_loader.py` would be high-value.
4. **Google Vision API is dead code** — imported in `requirements.txt` and referenced in schema, but never called. `vision_data` is hardcoded to `"no_vision"`.
5. **Unbxd container detection has no retry** — if Unbxd is slow/down, the entire pipeline stalls.
6. **Flask UI has no pagination** — querying large result sets (>10K SKUs) will be slow.
7. **SAS URLs hardcoded to 2-hour expiry** — links break for long-running review sessions.

---

## CSV Input Format

Each CSV file in the input folder should have:
- A column named `sku_id` (or falls back to first column)
- Optional: `container` (Azure container hint), `reprocess` (true/false), `category`

File naming convention: `{category}-{descriptor}.csv` — category is derived from the filename.

---

## MySQL Tables

| Table | Purpose |
|-------|---------|
| `sku_runs` | Run-level summary (start/end time, counts) |
| `sku_queue` | Worker task queue (atomic claims via `status` column) |
| `sku_results` | Canonical per-SKU outcome (latest run wins) |
| `image_results` | Per-image audit trail (Cloudinary URL, error codes, transformation mode) |

---

## External Services

- **Azure Blob Storage** — source images in containers like `in-media`, `in-media-ea`; output to `{container}/lifestyle-converted/`
- **Cloudinary** — image transform + CDN (pad/fill/gen_fill modes)
- **MySQL 8.0+** — task queue & persistence
- **Unbxd Search API** — product metadata & container detection
- **MongoDB (Content Central)** — source of product SKU data
- **Cloudflare Image Resizing** — reverse CDN for zero-egress serving
