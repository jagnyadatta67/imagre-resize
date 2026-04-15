"""
check-image-health.py — Verify ALL live product images are accessible on the CDN.

Flow
----
  1. Paginate Unbxd search API (rows=1000) — fetch productCode, gallaryImages, allCategories
  2. For each SKU, check gallery images sequentially via HEAD requests
     — stop at the first broken image and mark the SKU as broken
  3. Fire parallel workers (ThreadPoolExecutor), one task per SKU
  4. Broken SKUs written to image_health/{l2-category}.csv  (productCode + container)
  5. health_summary.txt written with per-category breakdown

Features
--------
  - Auto-paginates all ~42 pages (41,990 products)
  - Parallel SKU workers (ThreadPoolExecutor)
  - Per-SKU sequential gallery check — bails on first broken image
  - Per-L2-category broken CSVs  (e.g. men-tops.csv, women-dresses.csv)
  - Resume support (image_health/.checked.txt checkpoint)
  - tqdm progress bar

Usage
-----
  python check-image-health.py
  python check-image-health.py --workers 100
  python check-image-health.py --pages 1            # test with 1 page (~1000 products)
  python check-image-health.py --pages 0            # all pages
  python check-image-health.py --use-new            # check lifestyle-new folder
  python check-image-health.py --no-resume
  python check-image-health.py --debug
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter

# ── Bootstrap ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Add project root to sys.path so modules.db is importable
import sys
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from modules import db as pipeline_db

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers= [logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("image_health")

# ── Config ─────────────────────────────────────────────────────────────────────
UNBXD_API_KEY  = os.getenv("UNBXD_API_KEY",  "91b7857213222075bbcd3ea2dc72d026")
UNBXD_SITE_KEY = os.getenv("UNBXD_SITE_KEY", "ss-unbxd-aapac-prod-lifestyle-LandMark48741706891693")
UNBXD_SEARCH_URL = f"https://search.unbxd.io/{UNBXD_API_KEY}/{UNBXD_SITE_KEY}/search"

OUTPUT_DIR      = BASE_DIR / "image_health"
CHECKPOINT_FILE = OUTPUT_DIR / ".checked.txt"
SUMMARY_FILE    = OUTPUT_DIR / "health_summary.txt"

UNBXD_ROWS        = 100
DEFAULT_WORKERS   = 50
HEAD_TIMEOUT      = 10      # seconds per HEAD request
PAGE_DELAY        = 0.25    # seconds between Unbxd pages
MAX_RETRIES_UNBXD = 3
RETRY_DELAY_UNBXD = 2.0

# ── Domain → Azure container mapping ──────────────────────────────────────────
DOMAIN_TO_CONTAINER = {
    "media-ea.landmarkshops.in": "in-media-ea",
    "media.landmarkshops.in":    "in-media",
    "media-us.landmarkshops.in": "in-media-us",
    "media-uk.landmarkshops.in": "in-media-uk",
}

def url_to_container(url: str) -> str:
    """Extract Azure container name from a gallery image URL domain."""
    try:
        domain = urlparse(url).netloc
        return DOMAIN_TO_CONTAINER.get(domain, domain or "unknown")
    except Exception:
        return "unknown"

# ── Unbxd request headers ──────────────────────────────────────────────────────
UNBXD_HEADERS = {
    "accept":          "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type":    "application/json",
    "origin":          "https://www.lifestylestores.com",
    "referer":         "https://www.lifestylestores.com/",
    "unbxd-user-id":   "uid-pipeline-health-check",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}


# ── Unbxd Paginator ────────────────────────────────────────────────────────────
def fetch_all_products(max_pages: int | None = None) -> list[dict]:
    """
    Paginate Unbxd search API and collect all products.

    Returns list of:
      {
        "productCode":  "...",
        "gallery_urls": ["url1", "url2", ...],
        "l2_category":  "men-tops"          # allCategories[1], fallback "unknown"
      }
    """
    products_out: list[dict] = []
    page  = 1
    total = None

    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=2, pool_maxsize=2))

    while True:
        if max_pages and page > max_pages:
            log.info(f"Reached --pages limit ({max_pages}), stopping fetch.")
            break

        params = [
            ("rows",   UNBXD_ROWS),
            ("page",   page),
            ("q",      "*"),
            ("facet",  "false"),
            ("fields", "productCode,gallaryImages,allCategories"),
            ("filter", 'inStock:"1"'),
            ("filter", 'approvalStatus:"1"'),
        ]

        data = None
        for attempt in range(1, MAX_RETRIES_UNBXD + 1):
            try:
                resp = session.get(
                    UNBXD_SEARCH_URL,
                    params  = params,
                    headers = UNBXD_HEADERS,
                    timeout = 30,
                )
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.exceptions.RequestException as exc:
                log.warning(f"Page {page} attempt {attempt}/{MAX_RETRIES_UNBXD}: {exc}")
                if attempt < MAX_RETRIES_UNBXD:
                    time.sleep(RETRY_DELAY_UNBXD * attempt)
                else:
                    log.error(f"Giving up on page {page}")
                    return products_out

        if data is None:
            break

        response_block = data.get("response", {})
        products       = response_block.get("products", [])

        if total is None:
            total = response_block.get("numberOfProducts", "?")
            log.info(f"Total products in Unbxd: {total}")

        if not products:
            log.info(f"Page {page}: empty — pagination complete")
            break

        for p in products:
            product_code = (p.get("productCode") or "").strip()
            if not product_code:
                continue

            gallery = p.get("gallaryImages") or []
            if not isinstance(gallery, list):
                gallery = [gallery] if gallery else []

            # L2 = the entry with exactly 2 hyphen-separated parts
            # e.g. ["men", "men-tops", "men-tops-tshirts"] → "men-tops"
            all_cats    = p.get("allCategories") or []
            l2_category = next(
                (c.strip() for c in all_cats if len(c.strip().split("-")) == 2),
                all_cats[0].strip() if all_cats else "unknown"   # fallback to L1 or unknown
            )

            products_out.append({
                "productCode":  product_code,
                "gallery_urls": gallery,
                "l2_category":  l2_category,
            })

        log.info(f"Page {page}: {len(products)} products  [total collected: {len(products_out)} / {total}]")

        if len(products) < UNBXD_ROWS:
            log.info("Last page reached.")
            break

        page += 1
        time.sleep(PAGE_DELAY)

    return products_out


# ── Checkpoint helpers ─────────────────────────────────────────────────────────
def load_checkpoint() -> set[str]:
    if not CHECKPOINT_FILE.exists():
        return set()
    with open(CHECKPOINT_FILE, encoding="utf-8") as fh:
        ids = {line.strip() for line in fh if line.strip()}
    log.info(f"Resume: {len(ids)} already-checked productCodes loaded")
    return ids


def append_checkpoint(codes: list[str]) -> None:
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as fh:
        for c in codes:
            fh.write(c + "\n")


# ── Per-category broken CSV writer ────────────────────────────────────────────
_csv_lock = threading.Lock()

def write_broken_row(product_code: str, container: str, l2_category: str) -> None:
    """
    Append one broken SKU to image_health/{l2_category}.csv (thread-safe).
    Columns: productCode, container
    """
    csv_path = OUTPUT_DIR / f"{l2_category}.csv"
    with _csv_lock:
        is_new = not csv_path.exists()
        with open(csv_path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["productCode", "container"])
            if is_new:
                writer.writeheader()
            writer.writerow({"productCode": product_code, "container": container})


# ── HEAD Checker ───────────────────────────────────────────────────────────────
def check_sku(product: dict, session: requests.Session) -> dict:
    """
    Two-phase gallery image check:

    Phase 1 — First image:
      If broken → SKU has never been processed into lifestyle-new/.
      Result type: 'needs_transformation'
      → written to sku_needs_transformation table + CSV

    Phase 2 — Remaining images (only if first is OK):
      Each broken image = that specific source image never existed.
      Result type: 'partial_broken' (can have multiple broken images)
      → written to broken_image_health table

    If all OK → type: 'ok'
    """
    product_code = product["productCode"]
    gallery_urls = product["gallery_urls"]
    l2_category  = product["l2_category"]

    if not gallery_urls:
        return {"productCode": product_code, "type": "no_url",
                "container": "", "l2_category": l2_category}

    # container is always derived from first URL domain (stable, unaffected by --use-new)
    container = url_to_container(gallery_urls[0])

    # ── Phase 1: Check first image ─────────────────────────────────────────────
    first_url = gallery_urls[0]
    try:
        resp = session.head(first_url, timeout=HEAD_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            log.warning(f"[{resp.status_code}] {product_code} ({l2_category}) → {first_url}")
            return {
                "productCode": product_code,
                "type":        "needs_transformation",
                "container":   container,
                "l2_category": l2_category,
                "broken_url":  first_url,
                "error":       str(resp.status_code),
            }
    except requests.exceptions.Timeout:
        log.warning(f"[TIMEOUT] {product_code} ({l2_category}) → {first_url}")
        return {
            "productCode": product_code,
            "type":        "needs_transformation",
            "container":   container,
            "l2_category": l2_category,
            "broken_url":  first_url,
            "error":       "timeout",
        }
    except Exception as exc:
        log.warning(f"[ERROR] {product_code} ({l2_category}) → {exc}")
        return {
            "productCode": product_code,
            "type":        "needs_transformation",
            "container":   container,
            "l2_category": l2_category,
            "broken_url":  first_url,
            "error":       str(exc),
        }

    # ── Phase 2: Check remaining images (first was OK) ────────────────────────
    broken_images: list[dict] = []
    for idx, url in enumerate(gallery_urls[1:], start=2):
        if not url:
            continue
        try:
            resp = session.head(url, timeout=HEAD_TIMEOUT, allow_redirects=True)
            if resp.status_code != 200:
                log.warning(f"[{resp.status_code}] img#{idx} {product_code} ({l2_category}) → {url}")
                broken_images.append({"position": idx, "url": url, "error": str(resp.status_code)})
        except requests.exceptions.Timeout:
            log.warning(f"[TIMEOUT] img#{idx} {product_code} ({l2_category}) → {url}")
            broken_images.append({"position": idx, "url": url, "error": "timeout"})
        except Exception as exc:
            log.warning(f"[ERROR] img#{idx} {product_code} ({l2_category}) → {exc}")
            broken_images.append({"position": idx, "url": url, "error": str(exc)})

    if broken_images:
        return {
            "productCode":  product_code,
            "type":         "partial_broken",
            "container":    container,
            "l2_category":  l2_category,
            "broken_images": broken_images,
        }

    return {
        "productCode": product_code,
        "type":        "ok",
        "container":   container,
        "l2_category": l2_category,
    }


# ── Main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check all Unbxd product images via CDN HEAD requests."
    )
    parser.add_argument("--workers",   "-w", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel HEAD request workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--pages",     "-p", type=int, default=1,
                        help="Limit to N pages from Unbxd (default: 1 page = ~1000 products; use 0 for all pages)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore checkpoint and recheck everything")
    parser.add_argument("--use-new",   action="store_true",
                        help="Rewrite gallery URLs: /lifestyle/ → /lifestyle-new/ before checking")
    parser.add_argument("--debug",     action="store_true",
                        help="Enable DEBUG level logging")
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Ensure DB tables exist (safe to run every time — all CREATE IF NOT EXISTS)
    pipeline_db.init_db()

    # Truncate health tables so every run reflects fresh CDN state
    log.info("Truncating broken_image_health and sku_needs_transformation for fresh run...")
    pipeline_db.truncate_health_tables()

    log.info("=" * 60)
    log.info("check-image-health  (Unbxd → CDN HEAD check)")
    log.info(f"  Workers    : {args.workers}")
    pages_display = f"{args.pages} page(s) (~{args.pages * UNBXD_ROWS:,} products max)" if args.pages else "all pages"
    log.info(f"  Pages      : {pages_display}")
    log.info(f"  Resume     : {not args.no_resume}")
    log.info(f"  Use new    : {args.use_new}  (/lifestyle/ → /lifestyle-new/)" if args.use_new else "  Use new    : False")
    log.info(f"  Output dir : {OUTPUT_DIR}")
    log.info("=" * 60)

    # ── Step 1: Fetch all products from Unbxd ──────────────────────────────────
    log.info("Fetching product list from Unbxd...")
    all_products = fetch_all_products(max_pages=args.pages or None)
    log.info(f"Fetched {len(all_products)} products total")

    # ── Step 1b: Rewrite URLs if --use-new ─────────────────────────────────────
    if args.use_new:
        for p in all_products:
            p["gallery_urls"] = [
                u.replace("/lifestyle/", "/lifestyle-new/") for u in p["gallery_urls"]
            ]
        log.info("URLs rewritten: /lifestyle/ → /lifestyle-new/")

    # ── Step 2: Apply checkpoint ───────────────────────────────────────────────
    if args.no_resume:
        todo = all_products
        log.info("Resume disabled — checking all products")
    else:
        already_done = load_checkpoint()
        todo = [p for p in all_products if p["productCode"] not in already_done]
        skipped = len(all_products) - len(todo)
        if skipped:
            log.info(f"Skipping {skipped} already-checked  →  {len(todo)} remaining")

    if not todo:
        log.info("Nothing to check — all products already in checkpoint.")
        return

    # ── Step 3: Parallel HEAD checks ──────────────────────────────────────────
    log.info(f"Starting HEAD checks for {len(todo)} products with {args.workers} workers...")

    head_session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections = args.workers,
        pool_maxsize     = args.workers,
        max_retries      = 1,
    )
    head_session.mount("https://", adapter)
    head_session.mount("http://",  adapter)

    counters         = {"ok": 0, "broken": 0, "no_url": 0}
    category_broken  = defaultdict(int)   # l2_category → broken count
    checked_codes: list[str] = []

    try:
        from tqdm import tqdm
        progress = tqdm(total=len(todo), unit="sku", desc="Checking CDN", dynamic_ncols=True)
        def tick(): progress.update(1)
        def close_progress(): progress.close()
    except ImportError:
        log.warning("tqdm not installed — no progress bar (pip install tqdm)")
        done_count = [0]
        def tick():
            done_count[0] += 1
            if done_count[0] % 500 == 0 or done_count[0] == len(todo):
                pct = done_count[0] / len(todo) * 100
                print(f"\r  Progress: {done_count[0]}/{len(todo)} ({pct:.1f}%)", end="", flush=True)
        def close_progress(): print()

    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="head") as ex:
        futures = {
            ex.submit(check_sku, product, head_session): product
            for product in todo
        }
        for future in as_completed(futures):
            product = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                log.error(f"Unexpected error for {product['productCode']}: {exc}")
                result = {
                    "productCode": product["productCode"],
                    "ok":          False,
                    "error":       str(exc),
                    "container":   "",
                    "l2_category": product.get("l2_category", "unknown"),
                }

            checked_codes.append(result["productCode"])

            result_type = result.get("type", "ok")
            cat = result.get("l2_category", "unknown")

            if result_type == "no_url":
                counters["no_url"] += 1

            elif result_type == "ok":
                counters["ok"] += 1

            elif result_type == "needs_transformation":
                # First image broken → full SKU transformation needed
                counters["needs_transformation"] = counters.get("needs_transformation", 0) + 1
                category_broken[cat] += 1
                container = result.get("container", "")
                broken_url = result.get("broken_url", "")
                # Write CSV (backward compat)
                write_broken_row(result["productCode"], container, cat)
                # Write to DB
                pipeline_db.upsert_needs_transformation([{
                    "sku_id":     result["productCode"],
                    "category":   cat,
                    "container":  container,
                    "broken_url": broken_url,
                }])

            elif result_type == "partial_broken":
                # Non-first images broken → source images missing, inform business
                broken_images = result.get("broken_images", [])
                counters["partial_broken"] = counters.get("partial_broken", 0) + 1
                category_broken[cat] += len(broken_images)
                container = result.get("container", "")
                # Write each broken image to DB
                db_rows = [
                    {
                        "sku_id":         result["productCode"],
                        "category":       cat,
                        "broken_url":     bi["url"],
                        "image_position": bi["position"],
                    }
                    for bi in broken_images
                ]
                pipeline_db.upsert_broken_images(db_rows)
                # Write to CSV for reference
                for bi in broken_images:
                    write_broken_row(result["productCode"], container, f"{cat}_partial")

            # Checkpoint every 500 completions
            if len(checked_codes) % 500 == 0:
                append_checkpoint(checked_codes[-500:])

            tick()

    close_progress()

    # Final checkpoint flush
    remainder = len(checked_codes) % 500
    if remainder:
        append_checkpoint(checked_codes[-remainder:])

    head_session.close()

    # ── Step 4: Summary ────────────────────────────────────────────────────────
    total_checked = counters["ok"] + counters.get("needs_transformation", 0) + counters.get("partial_broken", 0) + counters["no_url"]
    total_broken  = counters.get("needs_transformation", 0) + counters.get("partial_broken", 0)
    pass_rate     = (counters["ok"] / total_checked * 100) if total_checked else 0

    summary_lines = [
        "=" * 55,
        "IMAGE HEALTH CHECK — SUMMARY",
        "=" * 55,
        f"  Total products    : {len(all_products)}",
        f"  Checked this run  : {total_checked}",
        f"  OK (200)          : {counters['ok']}",
        f"  Needs Transform   : {counters.get('needs_transformation', 0)}",
        f"  Partial Broken    : {counters.get('partial_broken', 0)}",
        f"  No URL            : {counters['no_url']}",
        f"  Pass rate         : {pass_rate:.2f}%",
        "=" * 55,
        "  BROKEN BY L2 CATEGORY",
        "-" * 55,
    ]

    for cat in sorted(category_broken):
        count    = category_broken[cat]
        csv_file = OUTPUT_DIR / f"{cat}.csv"
        summary_lines.append(f"  {cat:<35} {count:>5}  →  {csv_file.name}")

    summary_lines += [
        "=" * 55,
        f"  Output folder : {OUTPUT_DIR}",
        "=" * 55,
    ]

    print()
    for line in summary_lines:
        log.info(line)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(summary_lines) + "\n")

    log.info(f"Summary saved to {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
