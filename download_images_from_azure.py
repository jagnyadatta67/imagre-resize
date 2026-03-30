"""
download_images_from_azure.py
─────────────────────────────
Read input_csv/skus_extracted.csv, then for every SKU download all matching
blobs from the correct Azure container into  downloaded_images/{sku_id}/.

CSV columns expected:  sku_id, container, reprocess

Blob prefix pattern:   max-new/{sku_id}
  e.g. max-new/1000016372939-Blue-BLUE-1000016372913_01-2100.jpg

Usage
─────
  python download_images_from_azure.py                          # default CSV
  python download_images_from_azure.py --csv input_csv/skus_extracted.csv
  python download_images_from_azure.py --out downloaded_images
  python download_images_from_azure.py --workers 10
  python download_images_from_azure.py --dry-run               # list only, no download
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

AZURE_ACCOUNT_NAME = os.getenv("AZURE_ACCOUNT_NAME", "lmgonlinemedia")
AZURE_ACCOUNT_KEY  = os.getenv("AZURE_ACCOUNT_KEY", "")
BLOB_PREFIX        = "max-new/"          # folder inside each container

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Azure helpers ─────────────────────────────────────────────────────────────

def _connection_string() -> str:
    return (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={AZURE_ACCOUNT_NAME};"
        f"AccountKey={AZURE_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )


def _get_service_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(_connection_string())


def list_blobs_for_sku(svc: BlobServiceClient, container: str, sku_id: str) -> list[str]:
    """Return all blob names in  container  that start with  max-new/{sku_id}."""
    prefix = f"{BLOB_PREFIX}{sku_id}"
    try:
        cc = svc.get_container_client(container)
        return [b.name for b in cc.list_blobs(name_starts_with=prefix)]
    except Exception as exc:
        log.error(f"[list]  container={container}  sku={sku_id}  → {exc}")
        return []


def download_blob(svc: BlobServiceClient, container: str, blob_name: str, dest_path: Path) -> bool:
    """Download a single blob to dest_path. Returns True on success."""
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        data = svc.get_container_client(container).download_blob(blob_name).readall()
        dest_path.write_bytes(data)
        return True
    except Exception as exc:
        log.error(f"[download]  {blob_name}  → {exc}")
        return False


# ── CSV reader ────────────────────────────────────────────────────────────────

def load_csv(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            sku_id    = row.get("sku_id", "").strip()
            container = row.get("container", "").strip()
            if sku_id and container:
                rows.append({"sku_id": sku_id, "container": container})
    return rows


# ── Worker ────────────────────────────────────────────────────────────────────

def process_sku(
    svc: BlobServiceClient,
    sku_id: str,
    container: str,
    out_dir: Path,
    dry_run: bool,
) -> dict:
    blobs = list_blobs_for_sku(svc, container, sku_id)

    if not blobs:
        log.warning(f"NO BLOBS  container={container}  sku={sku_id}")
        return {"sku_id": sku_id, "container": container, "found": 0, "downloaded": 0}

    log.info(f"Found {len(blobs):>2} blob(s)  container={container}  sku={sku_id}")

    if dry_run:
        for b in blobs:
            log.info(f"  [dry-run] {b}")
        return {"sku_id": sku_id, "container": container, "found": len(blobs), "downloaded": 0}

    downloaded = 0
    sku_dir = out_dir / sku_id
    for blob_name in blobs:
        filename  = Path(blob_name).name          # strip  max-new/  prefix
        dest_path = sku_dir / filename
        if download_blob(svc, container, blob_name, dest_path):
            log.info(f"  ✓ {filename}")
            downloaded += 1

    return {"sku_id": sku_id, "container": container, "found": len(blobs), "downloaded": downloaded}


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Download Azure blobs by SKU ID from CSV")
    parser.add_argument("--csv",     default="input_csv/skus_extracted.csv", help="Input CSV path")
    parser.add_argument("--out",     default="downloaded_images",            help="Output folder")
    parser.add_argument("--workers", type=int, default=5,                    help="Parallel workers")
    parser.add_argument("--dry-run", action="store_true",                    help="List blobs only, no download")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    out_dir  = Path(args.out)

    if not csv_path.exists():
        log.error(f"CSV not found: {csv_path}")
        sys.exit(1)

    rows = load_csv(csv_path)
    if not rows:
        log.error("CSV is empty or has no valid rows.")
        sys.exit(1)

    if not AZURE_ACCOUNT_KEY:
        log.error("AZURE_ACCOUNT_KEY is not set in .env")
        sys.exit(1)

    log.info(f"SKUs to process : {len(rows)}")
    log.info(f"Output folder   : {out_dir}")
    log.info(f"Workers         : {args.workers}")
    log.info(f"Dry-run         : {args.dry_run}")
    print()

    svc = _get_service_client()

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                process_sku,
                svc,
                row["sku_id"],
                row["container"],
                out_dir,
                args.dry_run,
            ): row
            for row in rows
        }
        for fut in as_completed(futures):
            results.append(fut.result())

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("─" * 60)
    print(f"{'SKU ID':<45} {'Container':<14} {'Found':>5} {'DL':>5}")
    print("─" * 60)

    total_found = total_dl = 0
    for r in sorted(results, key=lambda x: x["sku_id"]):
        print(f"{r['sku_id']:<45} {r['container']:<14} {r['found']:>5} {r['downloaded']:>5}")
        total_found += r["found"]
        total_dl    += r["downloaded"]

    print("─" * 60)
    print(f"{'TOTAL':<45} {'':14} {total_found:>5} {total_dl:>5}")
    print()

    if args.dry_run:
        print("Dry-run complete — no files written.")
    else:
        print(f"Done. Files saved to: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
