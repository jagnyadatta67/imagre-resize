"""
fetch_skus_from_unbxd_csv.py — Enrich a CSV of SKU IDs via Unbxd API.

For each SKU ID in the input CSV:
  1. Strip hyphens from the SKU to build the Unbxd query
     e.g. 1000016410556-Black-Black → 1000016410556BlackBlack
  2. Hit Unbxd search API
  3. Extract:
       - L2 category  (allCategories entry with exactly 1 hyphen)
       - container    (imageUrl domain → DOMAIN_TO_CONTAINER mapping)
       - productCode  (canonical SKU ID from Unbxd)
  4. Group results by L2 category
  5. Write one CSV per L2 category into input_csv/
       input_csv/{l2-category}.csv
       columns: sku_id, container, reprocess

Usage
-----
  python fetch_skus_from_unbxd_csv.py --input path/to/skus.csv
  python fetch_skus_from_unbxd_csv.py --input path/to/skus.csv --workers 10
  python fetch_skus_from_unbxd_csv.py --input path/to/skus.csv --output-dir input_csv_unbxd/

Input CSV must have a column named: sku_id  (or SKU_ID / sku / SKU / product_id)
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests

# ── Bootstrap: load .env from same directory ──────────────────
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from config import UNBXD_URL, DOMAIN_TO_CONTAINER, FALLBACK_CONTAINER   # noqa: E402

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers= [logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("fetch_unbxd_csv")

# ── Constants ─────────────────────────────────────────────────
_SKU_COL_CANDIDATES = ["sku_id", "SKU_ID", "sku", "SKU", "product_id", "PRODUCT_ID"]

_UNBXD_HEADERS = {
    "accept":             "*/*",
    "accept-language":    "en-US,en;q=0.9",
    "content-type":       "application/json",
    "origin":             "https://www.lifestylestores.com",
    "referer":            "https://www.lifestylestores.com/",
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "cross-site",
    "unbxd-user-id":      "uid-9134783168516",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


# ── Helpers ───────────────────────────────────────────────────

def _sku_to_query(sku_id: str) -> str:
    """1000016410556-Black-Black → 1000016410556BlackBlack"""
    parts = sku_id.split("-")
    if not parts:
        return sku_id
    # First part is the numeric ID, rest are colour words — title-case and join
    return parts[0] + "".join(p.title() for p in parts[1:])


def _pick_l2_category(all_categories: list[str]) -> str | None:
    """
    Return the L2 category — the entry with exactly 1 hyphen.
    e.g. ["kids", "kids-boysclothing", "kids-boysclothing-shortsanddungrees"]
         → "kids-boysclothing"
    If multiple L2 entries exist, pick the most specific (longest).
    Returns None if no L2 found.
    """
    l2 = [c for c in all_categories if c.count("-") == 1]
    if not l2:
        return None
    return max(l2, key=len)


def _pick_container(image_urls: list[str] | str) -> str:
    """
    Extract Azure container from the first imageUrl domain.
    Falls back to FALLBACK_CONTAINER if domain not mapped.
    """
    if isinstance(image_urls, str):
        image_urls = [image_urls]
    for url in image_urls:
        host = urlparse(url).netloc.lower()
        for domain, container in DOMAIN_TO_CONTAINER.items():
            if domain in host:
                return container
    return FALLBACK_CONTAINER


# ── Core: fetch one SKU from Unbxd ───────────────────────────

def fetch_sku(sku_id: str, retries: int = 3) -> dict | None:
    """
    Query Unbxd for sku_id.
    Returns { sku_id, container, l2_category } or None on failure.
    """
    query = _sku_to_query(sku_id)

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                UNBXD_URL,
                headers = _UNBXD_HEADERS,
                params  = {
                    "rows":             "1",
                    "page":             "1",
                    "q":                query,
                    "facet":            "false",
                    "selectedfacet":    "false",
                    "fields":           "productCode,imageUrl,allCategories",
                    "filter":           ["inStock:\"1\"", "approvalStatus:\"1\""],
                    "stats":            "price",
                },
                timeout = 10,
            )
            resp.raise_for_status()
            data     = resp.json()
            products = data.get("response", {}).get("products", [])

            if not products:
                log.warning(f"  [{sku_id}] No results from Unbxd (query={query})")
                return None

            product       = products[0]
            all_categories= product.get("allCategories", [])
            image_urls    = product.get("imageUrl", [])
            product_code  = product.get("productCode", sku_id)

            l2  = _pick_l2_category(all_categories)
            con = _pick_container(image_urls)

            if not l2:
                log.warning(f"  [{sku_id}] No L2 category found in {all_categories}")
                return None

            log.info(f"  [{sku_id}] → l2={l2}  container={con}")
            return {
                "sku_id":    product_code,
                "container": con,
                "l2":        l2,
            }

        except requests.exceptions.RequestException as exc:
            log.warning(f"  [{sku_id}] attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)   # exponential backoff

    log.error(f"  [{sku_id}] gave up after {retries} attempts")
    return None


# ── Input CSV reader ─────────────────────────────────────────

def _read_sku_ids(csv_path: Path) -> list[str]:
    sku_ids = []
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader     = csv.DictReader(fh)
        fieldnames = [f.strip() for f in (reader.fieldnames or [])]

        sku_col = next((c for c in _SKU_COL_CANDIDATES if c in fieldnames), None)
        if not sku_col:
            # fallback: first column
            sku_col = fieldnames[0] if fieldnames else None
        if not sku_col:
            raise ValueError(f"Cannot find SKU column in {csv_path.name}. Columns: {fieldnames}")

        for row in reader:
            sku_id = (row.get(sku_col) or "").strip()
            if sku_id:
                sku_ids.append(sku_id)

    log.info(f"Read {len(sku_ids)} SKU IDs from {csv_path.name}")
    return sku_ids


# ── Output CSV writer ─────────────────────────────────────────

def _write_category_csvs(
    results:    list[dict],
    output_dir: Path,
) -> dict[str, int]:
    """
    Group results by l2 category and write one CSV per category.
    Returns { category: row_count } summary.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group by L2
    by_category: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        by_category[r["l2"]].append(r)

    summary = {}
    for category, rows in sorted(by_category.items()):
        out_path = output_dir / f"{category}.csv"

        # If file already exists, append new SKUs (deduplicate by sku_id)
        existing_skus: set[str] = set()
        if out_path.exists():
            with open(out_path, newline="", encoding="utf-8") as fh:
                for existing_row in csv.DictReader(fh):
                    existing_skus.add((existing_row.get("sku_id") or "").strip())

        new_rows = [r for r in rows if r["sku_id"] not in existing_skus]
        mode     = "a" if out_path.exists() else "w"

        with open(out_path, mode, newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if mode == "w":
                writer.writerow(["sku_id", "container", "reprocess"])
            for r in new_rows:
                writer.writerow([r["sku_id"], r["container"], ""])

        total = len(existing_skus) + len(new_rows)
        summary[category] = total
        log.info(f"  → {out_path.name}  (+{len(new_rows)} new,  {total} total)")

    return summary


# ── Main ──────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch SKU metadata from Unbxd and save per-category CSVs."
    )
    parser.add_argument("--input",      required=True, help="Input CSV with sku_id column")
    parser.add_argument("--output-dir", default="input_csv",
                        help="Output folder for category CSVs (default: input_csv/)")
    parser.add_argument("--workers",    type=int, default=5,
                        help="Parallel Unbxd requests (default: 5)")
    args = parser.parse_args()

    input_path  = Path(args.input)
    output_dir  = BASE_DIR / args.output_dir

    if not input_path.exists():
        log.error(f"Input file not found: {input_path}")
        sys.exit(1)

    sku_ids = _read_sku_ids(input_path)
    if not sku_ids:
        log.error("No SKU IDs found in input file.")
        sys.exit(1)

    log.info(f"Fetching {len(sku_ids)} SKUs from Unbxd  (workers={args.workers})")
    log.info(f"Output dir: {output_dir}")
    print()

    # ── Parallel Unbxd fetch ──────────────────────────────────
    results:  list[dict] = []
    failed:   list[str]  = []

    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="unbxd") as ex:
        future_to_sku = {ex.submit(fetch_sku, s): s for s in sku_ids}
        done = 0
        for future in as_completed(future_to_sku):
            sku = future_to_sku[future]
            done += 1
            try:
                result = future.result()
                if result:
                    results.append(result)
                else:
                    failed.append(sku)
            except Exception as exc:
                log.error(f"  [{sku}] unexpected error: {exc}")
                failed.append(sku)
            print(f"\r  Progress: {done}/{len(sku_ids)}", end="", flush=True)

    print()
    print()

    if not results:
        log.error("No results retrieved. Check Unbxd connectivity.")
        sys.exit(1)

    # ── Write CSVs ────────────────────────────────────────────
    summary = _write_category_csvs(results, output_dir)

    # ── Summary ───────────────────────────────────────────────
    print("=" * 55)
    print(f"  DONE")
    print(f"  Fetched   : {len(results)}/{len(sku_ids)} SKUs")
    print(f"  Failed    : {len(failed)}")
    print(f"  Categories: {len(summary)}")
    print()
    for cat, cnt in sorted(summary.items()):
        print(f"    {cat:<40} {cnt} SKUs")
    print("=" * 55)

    if failed:
        print(f"\n  Failed SKUs ({len(failed)}):")
        for s in failed:
            print(f"    {s}")


if __name__ == "__main__":
    main()
