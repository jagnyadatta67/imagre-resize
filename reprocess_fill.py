"""
reprocess_fill.py — Targeted reprocess pipeline.

Reprocesses specific images using  crop:fill, gravity:auto  (no Vision AI).
Takes a CSV of  sku_id,filename  and replaces those images in Azure +
updates image_results in MySQL.

Usage
-----
  # From a CSV file:
  python reprocess_fill.py --input text_heavy.csv

  # CSV format (header optional):
  #   sku_id,filename
  #   1000015566290-White-OffWhite,1000015566290-White-OffWhite-1000015566290_01-2100.jpg

  # From MySQL directly (all text_heavy images):
  python reprocess_fill.py --from-db

  # Dry run (no upload, no DB update):
  python reprocess_fill.py --input text_heavy.csv --dry-run

  # Control workers:
  python reprocess_fill.py --input text_heavy.csv --workers 10
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import cloudinary
import cloudinary.uploader
import requests as _req
from tqdm import tqdm

from config import (
    CLOUDINARY_CLOUD, CLOUDINARY_KEY, CLOUDINARY_SECRET,
    TARGET_W, TARGET_H, BULK_WORKERS,
)
from modules import db
from modules.db import update_reprocess_transform
from modules.azure_client import AzureClient

# ── Cloudinary singleton ──────────────────────────────────────
cloudinary.config(
    cloud_name = CLOUDINARY_CLOUD,
    api_key    = CLOUDINARY_KEY,
    api_secret = CLOUDINARY_SECRET,
    secure     = True,
)

# ── Logging ───────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(name)-25s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/reprocess_fill.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("reprocess_fill")

TRANSFORM_DATA = (
    f"engine:cloudinary,crop:fill,gravity:auto,"
    f"w:{TARGET_W},h:{TARGET_H},reason:text_heavy_fix"
)


# ============================================================
# LOAD INPUT
# ============================================================

def load_from_csv(path: str) -> list[dict]:
    """Load sku_id, filename pairs from a CSV file."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line in reader:
            if len(line) >= 2:
                sku_id   = line[0].strip().strip('"')
                filename = line[1].strip().strip('"')
                # Skip header row
                if sku_id.lower() == "sku_id" or filename.lower() == "filename":
                    continue
                if sku_id and filename:
                    rows.append({"sku_id": sku_id, "filename": filename})
    return rows


def load_from_db() -> list[dict]:
    """Load all text_heavy images directly from MySQL."""
    conn = db._conn()
    cur  = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT sku_id, filename
        FROM   image_results
        WHERE  transform_data LIKE '%reason:text_heavy%'
        ORDER  BY sku_id, filename
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"sku_id": r["sku_id"], "filename": r["filename"]} for r in rows]


# ============================================================
# CORE: PROCESS ONE IMAGE
# ============================================================

def process_one(
    item:    dict,
    azure:   AzureClient,
    run_id:  str,
    dry_run: bool,
) -> str:
    """
    Download → Cloudinary crop:fill gravity:auto → Upload to Azure → Update MySQL.
    Returns "done" | "failed".
    """
    sku_id   = item["sku_id"]
    filename = item["filename"]
    img_log  = logging.getLogger(f"fill.{sku_id[:30]}")

    # ── 1. Get container from DB ──────────────────────────────
    existing = db.get_sku_status(sku_id)
    if not existing or not existing.get("container_name"):
        img_log.error(f"No DB record for sku_id={sku_id} — skipping")
        return "failed"

    container = existing["container_name"]
    blob_name = f"lifestyle/{filename}"

    # ── 2. Download original from Azure ──────────────────────
    try:
        image_bytes = azure.download_blob_bytes(container, blob_name)
        img_log.info(f"Downloaded  {filename}  from {container}")
    except Exception as exc:
        img_log.error(f"Download failed  {filename}: {exc}")
        return "failed"

    if dry_run:
        img_log.info(f"DRY RUN — skipping Cloudinary + upload for {filename}")
        return "done"

    # ── 3. Upload to Cloudinary with crop:fill gravity:auto ───
    try:
        public_id      = filename.rsplit(".", 1)[0]
        transformation = [{"width": TARGET_W, "height": TARGET_H,
                           "crop": "fill", "gravity": "auto"}]
        result = cloudinary.uploader.upload(
            io.BytesIO(image_bytes),
            folder          = sku_id,
            public_id       = public_id,
            overwrite       = True,
            unique_filename = False,
            transformation  = transformation,
        )
        cloudinary_url = result["secure_url"]
        img_log.info(f"Cloudinary  {filename}  → {cloudinary_url}")

        # Fetch transformed bytes
        resp = _req.get(cloudinary_url, timeout=30)
        resp.raise_for_status()
        output_bytes = resp.content

    except Exception as exc:
        img_log.error(f"Cloudinary failed  {filename}: {exc}")
        return "failed"

    # ── 4. Upload result back to Azure ────────────────────────
    try:
        azure_url = azure.upload_to_newc(filename, output_bytes, container)
        img_log.info(f"Azure upload  {filename}  → {azure_url}")
    except Exception as exc:
        img_log.error(f"Azure upload failed  {filename}: {exc}")
        return "failed"

    # ── 5. Update image_results — set reprocess_transform_data + bump version ──
    try:
        new_version = update_reprocess_transform(
            sku_id                   = sku_id,
            filename                 = filename,
            reprocess_transform_data = TRANSFORM_DATA,
            new_cloudinary_url       = cloudinary_url,
            new_azure_url            = azure_url,
        )
        img_log.info(
            f"DB updated  {filename}  "
            f"reprocess_transform_data={TRANSFORM_DATA}  "
            f"CF cache version → v={new_version}"
        )
    except Exception as exc:
        img_log.error(f"DB update failed  {filename}: {exc}")
        return "failed"

    return "done"


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reprocess specific images with crop:fill gravity:auto"
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--input",   "-i", help="CSV file with sku_id,filename columns")
    src.add_argument("--from-db", action="store_true",
                     help="Load all text_heavy images directly from MySQL")
    parser.add_argument("--workers", "-w", type=int, default=BULK_WORKERS,
                        help=f"Parallel workers (default: {BULK_WORKERS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download only — no Cloudinary upload or DB update")
    args = parser.parse_args()

    # ── Init DB (creates transform_replacements if not exists) ─
    db.init_db()

    # ── Load items ────────────────────────────────────────────
    if args.from_db:
        items = load_from_db()
        source = "MySQL (text_heavy)"
    else:
        items = load_from_csv(args.input)
        source = args.input

    total  = len(items)
    run_id = f"fill-{str(uuid.uuid4())[:8]}"
    azure  = AzureClient()

    print(f"\n{'='*60}")
    print(f"  REPROCESS → crop:fill  gravity:auto")
    print(f"  Source    : {source}")
    print(f"  Images    : {total}")
    print(f"  Workers   : {args.workers}")
    print(f"  Dry run   : {args.dry_run}")
    print(f"  Run ID    : {run_id}")
    print(f"  Started   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    if total == 0:
        print("  No images to process.")
        return

    # ── Process with thread pool ──────────────────────────────
    counts = {"done": 0, "failed": 0}

    pbar = tqdm(
        total      = total,
        desc       = "Images",
        unit       = "img",
        bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
    )

    def _run(item):
        result = process_one(item, azure, run_id, args.dry_run)
        return result

    with ThreadPoolExecutor(max_workers=min(args.workers, total)) as executor:
        futures = {executor.submit(_run, item): item for item in items}
        for future in as_completed(futures):
            try:
                status = future.result()
            except Exception as exc:
                status = "failed"
                log.error(f"Unexpected error: {exc}")
            counts[status] = counts.get(status, 0) + 1
            pbar.set_postfix(done=counts["done"], failed=counts["failed"])
            pbar.update(1)

    pbar.close()

    # ── Summary ───────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  COMPLETE  run_id={run_id}")
    print(f"  Done      : {counts['done']}")
    print(f"  Failed    : {counts['failed']}")
    print(f"  Finished  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Log       : logs/reprocess_fill.log")
    print(f"{'='*60}\n")

    sys.exit(1 if counts["failed"] > 0 else 0)


if __name__ == "__main__":
    main()
