"""
fetch_skus_from_unbxd_BS.py — Fetch ALL babyshop product SKUs from the Unbxd category API
in a single paginated call, then split into per-L2-category CSVs.

Strategy
--------
  One API call:  p=allCategories_uFilter:baby
                 fields=productCode,imageUrl,allCategories

  Each product carries an `allCategories` list such as:
    ["baby-clothing-tops", "baby", "baby-clothing"]

  L2 category rule — keep slugs that:
    1. start with "baby-"
    2. contain exactly ONE hyphen  (e.g. "baby-clothing", not "baby-clothing-tops")

  A single product can belong to multiple L2 categories → it is written to
  each matching CSV.

Generated CSVs are fully compatible with csv_loader.py:
  sku_id      — productCode from Unbxd  (e.g. "1000016259497-Blue-Blue")
  container   — auto-derived from imageUrl hostname (e.g. "in-media-ea")
  reprocess   — left blank (default False)

Filename convention:  input_csv_BS/{l2-slug}.csv
  e.g.  input_csv_BS/baby-clothing.csv

Container derivation from imageUrl hostname:
  media-ea.landmarkshops.in  →  in-media-ea
  media.landmarkshops.in     →  in-media
  media-uk.landmarkshops.in  →  in-media-uk

Usage
-----
  python fetch_skus_from_unbxd_BS.py                  # fetch & write all L2 CSVs
  python fetch_skus_from_unbxd_BS.py --out-dir /tmp/csv_out
  python fetch_skus_from_unbxd_BS.py --rows 100
  python fetch_skus_from_unbxd_BS.py --dry-run        # fetch but don't write files
  python fetch_skus_from_unbxd_BS.py --cat babyshop   # override top-level category key

Environment variables (override defaults or set in .env):
  UNBXD_API_KEY   — 32-char key in the URL
  UNBXD_SITE_KEY  — site identifier in the URL
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt = "%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Unbxd config ───────────────────────────────────────────────────────────────

UNBXD_API_KEY  = os.getenv("BABYSHOP_UNBXD_API_KEY",  "04c81a1a0317cf9ec15eeb77ae6dcb43")
UNBXD_SITE_KEY = os.getenv("BABYSHOP_UNBXD_SITE_KEY", "ss-unbxd-aapac-prod-babyshop-LandMark48741720534433")

BASE_URL = f"https://search.unbxd.io/{UNBXD_API_KEY}/{UNBXD_SITE_KEY}/category"

# Single call — fetch all products with allCategories for local bucketing
FIELDS = "productCode,imageUrl,allCategories"

# Fixed filters applied to every request
BASE_FILTERS = [
    ('filter', 'inStock:"1"'),
    ('filter', 'approvalStatus:"1"'),
]

# Request headers matching the browser curl
HEADERS = {
    "accept":           "*/*",
    "accept-language":  "en-US,en;q=0.9",
    "content-type":     "application/json",
    "origin":           "https://www.lifestylestores.com",
    "referer":          "https://www.lifestylestores.com/",
    "unbxd-user-id":    "uid-pipeline-fetcher",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


# ── Container hostname map ─────────────────────────────────────────────────────
_HOSTNAME_TO_CONTAINER: dict[str, str] = {
    "media-ea.landmarkshops.in":  "in-media-ea",
    "media.landmarkshops.in":     "in-media",
    "media-uk.landmarkshops.in":  "in-media-uk",
}


def _derive_container(image_url: str) -> str:
    """
    Extract Azure container name from an Unbxd imageUrl.

    Returns empty string if the hostname is not recognised (pipeline will
    fall back to its own container-resolution logic).
    """
    if not image_url:
        return ""
    try:
        hostname = urlparse(image_url).hostname or ""
        return _HOSTNAME_TO_CONTAINER.get(hostname, "")
    except Exception:
        return ""


def _extract_l2_categories(all_categories: list[str], cat: str) -> list[str]:
    """
    Return only L2 category slugs from a product's allCategories list.

    L2 rule:
      - starts with "{cat}-"   e.g. "baby-"
      - contains exactly ONE hyphen  →  "baby-clothing" ✓, "baby-clothing-tops" ✗
    """
    prefix = f"{cat}-"
    expected_hyphens = cat.count("-") + 1
    return [
        slug for slug in all_categories
        if isinstance(slug, str)
        and slug.startswith(prefix)
        and slug.count("-") == expected_hyphens
    ]


# ── Core fetch logic ───────────────────────────────────────────────────────────

def fetch_all_products(
    cat: str,
    rows: int = 48,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    max_count: Optional[int] = None,
) -> list[dict]:
    """
    Paginate through the Unbxd category endpoint for ALL babyshop products.

    Parameters
    ----------
    cat         : top-level category key (e.g. "baby", "babyshop")
    rows        : results per page (default 48, API typically allows up to 100)
    max_retries : per-page retry attempts on transient HTTP errors
    retry_delay : seconds to wait between retries

    Returns
    -------
    list of dicts:
        {
            "sku_id":         "1000016259497-Blue-Blue",
            "container":      "in-media-ea",
            "l2_categories":  ["baby-clothing", "baby-footwear"],
        }
    """
    products_out: list[dict] = []
    page = 1

    while True:
        cat_filter = f"allCategories_uFilter:{cat}"
        params = [
            ("rows",     rows),
            ("page",     page),
            ("pagetype", "boolean"),
            ("p",        cat_filter),
            ("facet",    "false"),
            ("fields",   FIELDS),
        ] + BASE_FILTERS

        # ── Request with retry ─────────────────────────────────────────────────
        data: Optional[dict] = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(
                    BASE_URL,
                    params  = params,
                    headers = HEADERS,
                    timeout = 20,
                )
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.exceptions.RequestException as exc:
                log.warning(
                    f"  Page {page}, attempt {attempt}/{max_retries} failed: {exc}"
                )
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    log.error(
                        f"  Giving up on page {page} after {max_retries} attempts."
                    )
                    return products_out

        if data is None:
            break

        response_block = data.get("response", {})
        products       = response_block.get("products", [])

        if not products:
            log.info(f"  Page {page}: 0 products — pagination complete.")
            break

        page_items: list[dict] = []
        for p in products:
            product_code = (p.get("productCode") or "").strip()
            if not product_code:
                continue

            image_urls = p.get("imageUrl") or []
            first_url  = image_urls[0] if isinstance(image_urls, list) and image_urls else ""
            container  = _derive_container(first_url)

            raw_cats = p.get("allCategories") or []
            if isinstance(raw_cats, str):
                raw_cats = [raw_cats]
            l2_cats = _extract_l2_categories(raw_cats, cat)

            page_items.append({
                "sku_id":        product_code,
                "container":     container,
                "l2_categories": l2_cats,
            })

        products_out.extend(page_items)

        if max_count and len(products_out) >= max_count:
            products_out = products_out[:max_count]
            total = response_block.get("numberOfProducts", "?")
            log.info(
                f"  Page {page}: {len(page_items)} product(s)  "
                f"[running total: {len(products_out)} / {total}]  — count limit reached"
            )
            break

        total = response_block.get("numberOfProducts", "?")
        log.info(
            f"  Page {page}: {len(page_items)} product(s)  "
            f"[running total: {len(products_out)} / {total}]"
        )

        if len(products) < rows:
            break

        page += 1
        time.sleep(0.25)

    return products_out


# ── CSV writer ─────────────────────────────────────────────────────────────────

def write_csv(products: list[dict], out_path: Path) -> None:
    """Write products to a CSV file compatible with csv_loader.py."""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["sku_id", "container", "reprocess"])
        for item in products:
            writer.writerow([item["sku_id"], item["container"], ""])

    log.info(f"  Written {len(products)} row(s) → {out_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch ALL babyshop SKUs from Unbxd in one call, "
            "split by L2 allCategories, write input CSVs."
        )
    )
    parser.add_argument(
        "--cat",
        default = "boysclothing",
        metavar = "CAT",
        help    = "Top-level category key to fetch (default: boysclothing)",
    )
    parser.add_argument(
        "--count",
        type    = int,
        default = None,
        metavar = "N",
        help    = "Stop after fetching N products total (default: fetch all)",
    )
    parser.add_argument(
        "--out-dir",
        default = "input_csv_BS",
        metavar = "DIR",
        help    = "Output directory for CSV files (default: input_csv_BS/)",
    )
    parser.add_argument(
        "--rows",
        type    = int,
        default = 48,
        metavar = "N",
        help    = "Results per page (default: 48, max ~100)",
    )
    parser.add_argument(
        "--dry-run",
        action  = "store_true",
        help    = "Fetch products but do NOT write CSV files (for testing)",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    log.info(
        f"Starting Unbxd babyshop SKU fetch — cat={args.cat}, "
        f"rows/page={args.rows}, out_dir={out_dir}"
        f"{' [DRY RUN]' if args.dry_run else ''}"
    )
    log.info(f"  Filter: allCategories_uFilter:{args.cat}")

    # ── Fetch all products ─────────────────────────────────────────────────────
    all_products = fetch_all_products(cat=args.cat, rows=args.rows, max_count=args.count)

    log.info(f"\n  Total raw products fetched: {len(all_products)}")

    # ── Bucket by L2 category ─────────────────────────────────────────────────
    buckets: dict[str, dict[str, dict]] = defaultdict(dict)
    no_l2_count = 0

    for item in all_products:
        l2_cats = item["l2_categories"]
        if not l2_cats:
            no_l2_count += 1
            continue
        for cat in l2_cats:
            sku_id = item["sku_id"]
            if sku_id not in buckets[cat]:
                buckets[cat][sku_id] = {
                    "sku_id":    sku_id,
                    "container": item["container"],
                }

    if no_l2_count:
        log.warning(
            f"  {no_l2_count} product(s) had no L2 {args.cat}-* category — skipped"
        )

    # ── Write CSVs & collect summary ──────────────────────────────────────────
    summary: list[tuple[str, int, int]] = []

    for slug in sorted(buckets):
        unique_products = list(buckets[slug].values())
        no_container    = sum(1 for i in unique_products if not i["container"])

        if no_container:
            log.warning(
                f"  {slug}: {no_container} product(s) have no recognised container "
                f"(pipeline will resolve via Azure fallback)"
            )

        if not args.dry_run:
            write_csv(unique_products, out_dir / f"{slug}.csv")
        else:
            log.info(
                f"  [DRY RUN] {slug}: would write {len(unique_products)} row(s) "
                f"to {out_dir}/{slug}.csv"
            )

        summary.append((slug, len(unique_products), no_container))

    # ── Final summary ──────────────────────────────────────────────────────────
    total_skus = sum(c for _, c, _ in summary)

    log.info("\n── Summary ─────────────────────────────────────────────────────")
    log.info(f"  {'L2 Category':<38} {'SKUs':>6}  {'No-container':>12}")
    log.info(f"  {'-'*38} {'-'*6}  {'-'*12}")
    for slug, count, no_cont in summary:
        log.info(f"  {slug:<38} {count:>6}  {no_cont:>12}")
    log.info(f"  {'-'*38} {'-'*6}")
    log.info(f"  {'TOTAL':<38} {total_skus:>6}")

    if not args.dry_run and summary:
        log.info(f"\n  CSVs written to: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
