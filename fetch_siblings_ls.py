"""
fetch_siblings_ls.py — Fetch color-variant (sibling) SKUs from Unbxd q=* search
and generate CSVs ready for the existing bulk-cloudinary pipeline.

Background
----------
Unbxd products carry a `sibiling` field (Unbxd's own spelling) that lists every
colour variant of a product as a delimited string:

    "<sku_id>#<colour>#<thumbnail_url>"

This script:
  1. Paginates the Unbxd search endpoint with q=* to get ALL live products.
  2. For every product that has a `sibiling` list, parses each entry.
  3. Derives the Azure container from the thumbnail URL domain.
  4. Assigns the parent product's L2 category to each sibling.
  5. Skips sibling SKUs that already have status='done' in MySQL sku_results
     (pass --force to override).
  6. Deduplicates by sku_id — first-seen L2 category wins.
  7. Writes one CSV per L2 category to input_csv_siblings/ (or --out-dir).

Output CSV columns (compatible with csv_loader.py):
  sku_id, container, category

Usage
-----
  python fetch_siblings_ls.py                   # fetch all, skip done
  python fetch_siblings_ls.py --dry-run         # fetch but don't write files
  python fetch_siblings_ls.py --force           # include already-done SKUs
  python fetch_siblings_ls.py --rows 100        # larger page size
  python fetch_siblings_ls.py --out-dir /tmp/s  # custom output folder
  python fetch_siblings_ls.py --categories men-tops,women-tops  # filter L2 cats

Environment variables (from .env):
  UNBXD_API_KEY, UNBXD_SITE_KEY  — Unbxd credentials
  MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD  — DB connection
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
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
UNBXD_API_KEY  = os.getenv("UNBXD_API_KEY",  "91b7857213222075bbcd3ea2dc72d026")
UNBXD_SITE_KEY = os.getenv("UNBXD_SITE_KEY", "ss-unbxd-aapac-prod-lifestyle-LandMark48741706891693")
SEARCH_URL     = f"https://search.unbxd.io/{UNBXD_API_KEY}/{UNBXD_SITE_KEY}/search"

UNBXD_HEADERS = {
    "accept":           "*/*",
    "accept-language":  "en-US,en;q=0.9",
    "content-type":     "application/json",
    "origin":           "https://www.lifestylestores.com",
    "referer":          "https://www.lifestylestores.com/",
    "unbxd-user-id":    "uid-pipeline-siblings",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
}

BASE_FILTERS = [
    ("filter", 'inStock:"1"'),
    ("filter", 'approvalStatus:"1"'),
]


# ── Container map (same as config.py / unbxd_client.py) ───────────────────────
DOMAIN_TO_CONTAINER: dict[str, str] = {
    "media-ea.landmarkshops.in": "in-media-ea",
    "media.landmarkshops.in":    "in-media",
    "media-us.landmarkshops.in": "in-media-us",
    "media-uk.landmarkshops.in": "in-media-uk",
}


def _container_from_url(url: str) -> str:
    """Derive Azure container from thumbnail URL domain."""
    if not url:
        return ""
    try:
        domain = urlparse(url).netloc or ""
        return DOMAIN_TO_CONTAINER.get(domain, "")
    except Exception:
        return ""


def _l2_from_categories(all_categories: list[str]) -> str:
    """
    Return the L2 category slug from allCategories.
    L2 rule: exactly 2 hyphen-separated parts, e.g. "men-tops", "women-bags".
    Returns the first match, or the first entry if none match.
    """
    for c in all_categories:
        if isinstance(c, str) and c.count("-") == 1:
            return c.strip()
    # Fallback: use first entry
    return (all_categories[0].strip() if all_categories else "unknown")


def _parse_sibling_entries(sibiling_field) -> list[tuple[str, str]]:
    """
    Parse the Unbxd `sibiling` field into (sku_id, thumbnail_url) pairs.

    The field is a list of strings, each formatted as:
        "<sku_id>#<colour_label>#<thumbnail_url>"

    Some entries may have more than 2 '#' characters in the URL — split on
    the FIRST two '#' only so the URL is preserved intact.

    Returns list of (sku_id, url) — colour label is discarded.
    """
    results: list[tuple[str, str]] = []

    if not sibiling_field:
        return results

    # Normalise to list
    if isinstance(sibiling_field, str):
        entries = [sibiling_field]
    else:
        entries = list(sibiling_field)

    for entry in entries:
        if not isinstance(entry, str) or not entry.strip():
            continue
        parts = entry.split("#", 2)   # max 3 parts: sku_id, colour, url
        if len(parts) < 1:
            continue
        sku_id = parts[0].strip()
        url    = parts[2].strip() if len(parts) >= 3 else ""
        if sku_id:
            results.append((sku_id, url))

    return results


# ── Done-SKU DB lookup ─────────────────────────────────────────────────────────

def _load_done_skus() -> set[str]:
    """
    Return the set of sku_ids that already have status='done' in sku_results.
    Returns an empty set if DB connection fails (script continues without filter).
    """
    try:
        import pymysql
        conn = pymysql.connect(
            host     = os.getenv("MYSQL_HOST",     "localhost"),
            port     = int(os.getenv("MYSQL_PORT", "3306")),
            db       = os.getenv("MYSQL_DB",       "image_pipeline"),
            user     = os.getenv("MYSQL_USER",     "root"),
            password = os.getenv("MYSQL_PASSWORD", "root123"),
            charset  = "utf8mb4",
            cursorclass = pymysql.cursors.DictCursor,
            connect_timeout = 10,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT sku_id FROM sku_results WHERE status = 'done'")
            rows = cur.fetchall()
        conn.close()
        done = {r["sku_id"] for r in rows}
        log.info(f"  Loaded {len(done):,} done SKUs from DB (will be skipped).")
        return done
    except ImportError:
        log.warning("  pymysql not installed — skipping done-SKU filter.")
        return set()
    except Exception as exc:
        log.warning(f"  DB connect failed ({exc}) — skipping done-SKU filter.")
        return set()


# ── Unbxd paginated fetch ──────────────────────────────────────────────────────

def fetch_all_siblings(
    rows: int = 100,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> list[dict]:
    """
    Paginate Unbxd q=* search requesting sibiling + allCategories fields.

    For each product that has a non-empty `sibiling` list, parse each sibling
    entry and collect:
      {
        "sku_id":    str,
        "container": str,   # from URL domain
        "l2":        str,   # from parent allCategories
      }

    Duplicates across pages are NOT deduplicated here — caller deduplicates.

    Returns list of dicts, one per sibling occurrence.
    """
    collected: list[dict] = []
    page       = 1
    total_seen = 0   # total products seen (including those without siblings)

    log.info(f"  Unbxd search URL: {SEARCH_URL}")
    log.info(f"  Fields: sibiling, allCategories  |  rows/page: {rows}")

    while True:
        params = [
            ("rows",   rows),
            ("page",   page),
            ("q",      "*"),
            ("facet",  "false"),
            ("fields", "sibiling,allCategories"),
        ] + BASE_FILTERS

        data: dict | None = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(
                    SEARCH_URL,
                    params  = params,
                    headers = UNBXD_HEADERS,
                    timeout = 20,
                )
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.exceptions.RequestException as exc:
                log.warning(f"  Page {page}, attempt {attempt}/{max_retries}: {exc}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    log.error(f"  Giving up on page {page} after {max_retries} attempts.")
                    return collected

        if data is None:
            break

        response_block = data.get("response", {})
        products       = response_block.get("products", [])

        if not products:
            log.info(f"  Page {page}: 0 products — pagination complete.")
            break

        total_seen += len(products)
        page_siblings = 0

        for product in products:
            sibiling_raw = product.get("sibiling") or []
            if not sibiling_raw:
                continue

            # Parent's category list
            raw_cats = product.get("allCategories") or []
            if isinstance(raw_cats, str):
                raw_cats = [raw_cats]
            l2 = _l2_from_categories(raw_cats)

            for sku_id, thumb_url in _parse_sibling_entries(sibiling_raw):
                container = _container_from_url(thumb_url)
                collected.append({
                    "sku_id":    sku_id,
                    "container": container,
                    "l2":        l2,
                })
                page_siblings += 1

        total = response_block.get("numberOfProducts", "?")
        log.info(
            f"  Page {page}: {len(products)} products, {page_siblings} siblings  "
            f"[products seen so far: {total_seen} / {total}]"
        )

        # Stop on last page (Unbxd returns fewer rows than requested)
        if len(products) < rows:
            break

        page += 1
        time.sleep(0.25)   # polite rate limiting

    return collected


# ── CSV writer ─────────────────────────────────────────────────────────────────

def write_csv(rows_data: list[dict], out_path: Path) -> None:
    """Write `sku_id,container,category` CSV compatible with csv_loader.py."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["sku_id", "container", "category"])
        for item in rows_data:
            writer.writerow([item["sku_id"], item["container"], item["l2"]])
    log.info(f"  Written {len(rows_data):,} rows → {out_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Lifestyle sibling (colour-variant) SKUs from Unbxd "
            "and write input CSVs for the bulk-cloudinary pipeline."
        )
    )
    parser.add_argument(
        "--out-dir",
        default = "input_csv_siblings",
        metavar = "DIR",
        help    = "Output directory for CSV files (default: input_csv_siblings/)",
    )
    parser.add_argument(
        "--rows",
        type    = int,
        default = 100,
        metavar = "N",
        help    = "Results per page from Unbxd (default: 100, max ~100)",
    )
    parser.add_argument(
        "--dry-run",
        action = "store_true",
        help   = "Fetch and parse but do NOT write CSV files",
    )
    parser.add_argument(
        "--force",
        action = "store_true",
        help   = "Include SKUs already marked done in sku_results (default: skip them)",
    )
    parser.add_argument(
        "--categories",
        metavar = "CATS",
        default = "",
        help    = (
            "Comma-separated list of L2 categories to include "
            "(e.g. men-tops,women-bags). Default: all categories."
        ),
    )
    args = parser.parse_args()

    # Parse --categories filter
    cat_filter: set[str] = set()
    if args.categories:
        cat_filter = {c.strip().lower() for c in args.categories.split(",") if c.strip()}
        log.info(f"  Category filter: {sorted(cat_filter)}")

    out_dir = Path(args.out_dir)

    log.info("=" * 62)
    log.info("  fetch_siblings_ls.py — Lifestyle sibling SKU fetcher")
    log.info(f"  out_dir  : {out_dir.resolve()}")
    log.info(f"  rows/pg  : {args.rows}")
    log.info(f"  dry-run  : {args.dry_run}")
    log.info(f"  force    : {args.force}")
    log.info("=" * 62)

    # ── Load done SKUs from DB (unless --force) ────────────────────────────────
    done_skus: set[str] = set()
    if not args.force:
        done_skus = _load_done_skus()
    else:
        log.info("  --force: skipping done-SKU filter.")

    # ── Fetch all siblings from Unbxd ─────────────────────────────────────────
    log.info("\nFetching siblings from Unbxd (q=*)…")
    raw = fetch_all_siblings(rows=args.rows)
    log.info(f"\n  Raw sibling entries collected: {len(raw):,}")

    # ── Deduplicate by sku_id (first occurrence wins) ─────────────────────────
    seen: set[str]  = set()
    deduped: list[dict] = []
    for item in raw:
        sid = item["sku_id"]
        if sid in seen:
            continue
        seen.add(sid)
        deduped.append(item)
    log.info(f"  After dedup               : {len(deduped):,}")

    # ── Skip done SKUs ────────────────────────────────────────────────────────
    if done_skus:
        before = len(deduped)
        deduped = [i for i in deduped if i["sku_id"] not in done_skus]
        skipped = before - len(deduped)
        log.info(f"  After skipping done SKUs  : {len(deduped):,}  ({skipped:,} skipped)")

    # ── Apply --categories filter ─────────────────────────────────────────────
    if cat_filter:
        before = len(deduped)
        deduped = [i for i in deduped if i["l2"].lower() in cat_filter]
        log.info(
            f"  After category filter     : {len(deduped):,}  "
            f"({before - len(deduped):,} excluded)"
        )

    if not deduped:
        log.info("\n  Nothing to write — all SKUs filtered out.")
        sys.exit(0)

    # ── Bucket by L2 category ─────────────────────────────────────────────────
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in deduped:
        buckets[item["l2"]].append(item)

    # ── Write CSVs ────────────────────────────────────────────────────────────
    log.info(f"\nWriting CSVs to: {out_dir.resolve()}")
    summary: list[tuple[str, int, int]] = []   # (l2, count, no-container)

    for l2_slug in sorted(buckets):
        items       = buckets[l2_slug]
        no_cont     = sum(1 for i in items if not i["container"])
        csv_path    = out_dir / f"{l2_slug}.csv"

        if no_cont:
            log.warning(
                f"  {l2_slug}: {no_cont:,} SKU(s) have no recognised container "
                f"(pipeline will resolve from Azure fallback)"
            )

        if not args.dry_run:
            write_csv(items, csv_path)
        else:
            log.info(
                f"  [DRY RUN] {l2_slug}: would write {len(items):,} rows "
                f"→ {csv_path}"
            )

        summary.append((l2_slug, len(items), no_cont))

    # ── Summary table ─────────────────────────────────────────────────────────
    total_written = sum(c for _, c, _ in summary)

    log.info("\n── Summary ─────────────────────────────────────────────────────")
    log.info(f"  {'L2 Category':<38} {'SKUs':>7}  {'No-container':>12}")
    log.info(f"  {'-'*38} {'-'*7}  {'-'*12}")
    for slug, count, no_cont in summary:
        flag = "  ⚠" if no_cont else ""
        log.info(f"  {slug:<38} {count:>7,}  {no_cont:>12,}{flag}")
    log.info(f"  {'-'*38} {'-'*7}")
    log.info(f"  {'TOTAL':<38} {total_written:>7,}")

    if not args.dry_run and summary:
        log.info(f"\n  CSVs written to: {out_dir.resolve()}")
        log.info(
            "  Run the pipeline with:\n"
            f"    python run.py {out_dir} --workers 10 --pad-mode no"
        )


if __name__ == "__main__":
    main()
