"""
check-image-health.py — Verify ALL live product images are accessible on the CDN.

Flow
----
  1. Paginate Unbxd search API (rows=1000) to collect all productCode + imageUrl
  2. For each product, derive CDN check URL from the imageUrl:
       https://{domain}/cdn-cgi/image/h=550,w=550,q=85,fit=cover/{path}
  3. Fire parallel HEAD requests (default 50 workers)
  4. Log non-200 responses in real time
  5. Write broken_images.csv + health_summary.txt to image_health/

Features
--------
  - Auto-paginates all ~42 pages (41,990 products)
  - Parallel HEAD workers (ThreadPoolExecutor)
  - Resume support (image_health/.checked.txt checkpoint)
  - tqdm progress bar
  - Only checks _01 (first/listing) image per product

Usage
-----
  python check-image-health.py
  python check-image-health.py --workers 100
  python check-image-health.py --pages 1            # test with 1 page only
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter

# ── Bootstrap ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

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
BROKEN_CSV      = OUTPUT_DIR / "broken_images.csv"
SUMMARY_FILE    = OUTPUT_DIR / "health_summary.txt"

UNBXD_ROWS          = 1000
DEFAULT_WORKERS     = 50
HEAD_TIMEOUT        = 10       # seconds per HEAD request
PAGE_DELAY          = 0.25     # seconds between Unbxd pages
MAX_RETRIES_UNBXD   = 3
RETRY_DELAY_UNBXD   = 2.0

# Unbxd request headers (matches browser fingerprint)
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
      {"productCode": "...", "imageUrl": "...", "check_url": "..."}
    """
    products_out: list[dict] = []
    page   = 1
    total  = None

    session = requests.Session()
    session.mount("https://", HTTPAdapter(pool_connections=2, pool_maxsize=2))

    while True:
        if max_pages and page > max_pages:
            log.info(f"Reached --pages limit ({max_pages}), stopping fetch.")
            break

        params = [
            ("rows",              UNBXD_ROWS),
            ("page",              page),
            ("q",                 "*"),
            ("facet",             "false"),
            ("fields",            "productCode,imageUrl"),
            ("filter",            'inStock:"1"'),
            ("filter",            'approvalStatus:"1"'),
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

            image_urls = p.get("imageUrl") or []
            first_url  = image_urls[0] if isinstance(image_urls, list) and image_urls else ""
            if isinstance(first_url, list):
                first_url = first_url[0] if first_url else ""

            products_out.append({
                "productCode": product_code,
                "imageUrl":    first_url,
                "check_url":   first_url,   # hit imageUrl directly, no CDN transform
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


# ── Broken CSV writer ──────────────────────────────────────────────────────────
_csv_lock = threading.Lock()

def write_broken_row(row: dict) -> None:
    """Append one broken image row to broken_images.csv (thread-safe)."""
    with _csv_lock:
        is_new = not BROKEN_CSV.exists()
        with open(BROKEN_CSV, "a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["productCode", "imageUrl", "check_url", "status_code", "error"])
            if is_new:
                writer.writeheader()
            writer.writerow(row)


# ── HEAD Checker ───────────────────────────────────────────────────────────────
def check_image(product: dict, session: requests.Session) -> dict:
    """
    Fire a HEAD request for one product's CDN URL.
    Returns result dict with status_code / error.
    """
    check_url = product["check_url"]
    result = {
        "productCode": product["productCode"],
        "imageUrl":    product["imageUrl"],
        "check_url":   check_url,
        "status_code": None,
        "error":       "",
        "ok":          False,
    }

    if not check_url:
        result["error"] = "no_url"
        return result

    try:
        resp = session.head(check_url, timeout=HEAD_TIMEOUT, allow_redirects=True)
        result["status_code"] = resp.status_code
        result["ok"]          = (resp.status_code == 200)
        if not result["ok"]:
            log.warning(f"[{resp.status_code}] {product['productCode']} → {check_url}")
    except requests.exceptions.Timeout:
        result["error"] = "timeout"
        log.warning(f"[TIMEOUT] {product['productCode']} → {check_url}")
    except requests.exceptions.ConnectionError as exc:
        result["error"] = f"conn_error: {exc}"
        log.warning(f"[CONN_ERR] {product['productCode']} → {check_url}")
    except Exception as exc:
        result["error"] = str(exc)
        log.warning(f"[ERROR] {product['productCode']} → {exc}")

    return result


# ── Main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check all Unbxd product images via CDN HEAD requests."
    )
    parser.add_argument("--workers",   "-w", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel HEAD request workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--pages",     "-p", type=int, default=None,
                        help="Limit to N pages from Unbxd (default: all pages)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore checkpoint and recheck everything")
    parser.add_argument("--debug",     action="store_true",
                        help="Enable DEBUG level logging")
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("check-image-health  (Unbxd → CDN HEAD check)")
    log.info(f"  Workers    : {args.workers}")
    log.info(f"  Pages      : {args.pages or 'all'}")
    log.info(f"  Resume     : {not args.no_resume}")
    log.info(f"  Output dir : {OUTPUT_DIR}")
    log.info("=" * 60)

    # ── Step 1: Fetch all products from Unbxd ──────────────────────────────────
    log.info("Fetching product list from Unbxd...")
    all_products = fetch_all_products(max_pages=args.pages)
    log.info(f"Fetched {len(all_products)} products total")

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

    # Shared HTTP session for HEAD requests
    head_session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections = args.workers,
        pool_maxsize     = args.workers,
        max_retries      = 1,
    )
    head_session.mount("https://", adapter)
    head_session.mount("http://",  adapter)

    counters = {"ok": 0, "broken": 0, "no_url": 0}
    checked_codes: list[str] = []

    # tqdm progress bar
    try:
        from tqdm import tqdm
        progress = tqdm(total=len(todo), unit="img", desc="Checking CDN", dynamic_ncols=True)
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
            ex.submit(check_image, product, head_session): product
            for product in todo
        }
        for future in as_completed(futures):
            product = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                log.error(f"Unexpected error for {product['productCode']}: {exc}")
                result = {**product, "status_code": None, "error": str(exc), "ok": False}

            checked_codes.append(result["productCode"])

            if result.get("error") == "no_url":
                counters["no_url"] += 1
            elif result["ok"]:
                counters["ok"] += 1
            else:
                counters["broken"] += 1
                write_broken_row({
                    "productCode": result["productCode"],
                    "imageUrl":    result["imageUrl"],
                    "check_url":   result["check_url"],
                    "status_code": result["status_code"],
                    "error":       result["error"],
                })

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
    total_checked = counters["ok"] + counters["broken"] + counters["no_url"]
    pass_rate     = (counters["ok"] / total_checked * 100) if total_checked else 0

    summary_lines = [
        "=" * 55,
        "IMAGE HEALTH CHECK — SUMMARY",
        "=" * 55,
        f"  Total products    : {len(all_products)}",
        f"  Checked this run  : {total_checked}",
        f"  ✅ OK (200)        : {counters['ok']}",
        f"  ❌ Broken          : {counters['broken']}",
        f"  ⚠️  No URL          : {counters['no_url']}",
        f"  Pass rate         : {pass_rate:.2f}%",
        "=" * 55,
        f"  broken_images.csv : {BROKEN_CSV}",
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
