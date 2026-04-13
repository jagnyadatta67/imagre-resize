"""
fetch_skus_bs.py — Fetch Baby Shop SKUs from Unbxd by L2 category and write CSVs.

Usage:
    python3 fetch_skus_bs.py --categories boysclothing-babyboys girlsclothing-babygirls
    python3 fetch_skus_bs.py --categories boysclothing-babyboys --output input_csv_babyshop/

Output:
    input_csv_babyshop/babyshop-{category}.csv
    Columns: sku_id, container, brand

The script paginates through ALL products in each category (48 rows/page).
Container is derived from the imageUrl/gallaryImages domain.
brand is always "babyshop".
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

# ── Bootstrap path so config can be imported from project root ──
sys.path.insert(0, str(Path(__file__).parent))
from config import BABYSHOP_UNBXD_API_KEY, BABYSHOP_UNBXD_SITE_KEY, DOMAIN_TO_CONTAINER, FALLBACK_CONTAINER

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s %(levelname)-8s %(message)s",
)
log = logging.getLogger("fetch_skus_bs")

BABYSHOP_CATEGORY_URL = (
    f"https://search.unbxd.io"
    f"/{BABYSHOP_UNBXD_API_KEY}"
    f"/{BABYSHOP_UNBXD_SITE_KEY}"
    f"/category"
)

BABYSHOP_HEADERS = {
    "accept":          "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type":    "application/json",
    "origin":          "https://www.babyshop.in",
    "referer":         "https://www.babyshop.in/",
    "unbxd-user-id":   "uid-pipeline-babyshop",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
}

PAGE_SIZE = 48


def _url_to_container(url: str) -> str:
    """Derive Azure container name from CDN image URL domain."""
    try:
        domain = urlparse(url).netloc.lower()
        for d, container in DOMAIN_TO_CONTAINER.items():
            if d in domain:
                return container
    except Exception:
        pass
    return FALLBACK_CONTAINER


def fetch_category_page(session: requests.Session, category: str, page: int) -> dict:
    """Fetch one page of products for a category. Returns raw JSON response."""
    params = {
        "rows":           PAGE_SIZE,
        "page":           page,
        "pagetype":       "boolean",
        "p":              f"allCategories_uFilter:{category}",
        "facet":          "false",
        "selectedfacet":  "true",
        "fields":         "productCode,imageUrl,gallaryImages,allCategories",
        "filter":         'inStock:"1"',
        "stats":          "price",
    }
    # filter needs to be repeated — pass as list
    resp = session.get(
        BABYSHOP_CATEGORY_URL,
        params=[
            ("rows",          PAGE_SIZE),
            ("page",          page),
            ("pagetype",      "boolean"),
            ("p",             f"allCategories_uFilter:{category}"),
            ("facet",         "false"),
            ("selectedfacet", "true"),
            ("fields",        "productCode,imageUrl,gallaryImages,allCategories"),
            ("filter",        'inStock:"1"'),
            ("filter",        'approvalStatus:"1"'),
            ("stats",         "price"),
        ],
        headers=BABYSHOP_HEADERS,
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all_skus_for_category(session: requests.Session, category: str) -> list[dict]:
    """
    Paginate through all products for a category.
    Returns list of {sku_id, container}.
    """
    skus: list[dict] = []
    page  = 1
    total = None

    while True:
        log.info(f"  [{category}] page {page} ...")
        try:
            data = fetch_category_page(session, category, page)
        except Exception as exc:
            log.error(f"  [{category}] page {page} error: {exc}")
            break

        response   = data.get("response", {})
        products   = response.get("products", [])
        if total is None:
            total = response.get("numberOfProducts", 0)
            log.info(f"  [{category}] total products: {total}")

        if not products:
            break

        for p in products:
            sku_id = (p.get("productCode") or "").strip()
            if not sku_id:
                continue

            # Derive container from gallaryImages[0] or imageUrl
            gallery = p.get("gallaryImages") or []
            if isinstance(gallery, str):
                gallery = [gallery]
            image_url = gallery[0] if gallery else (p.get("imageUrl") or "")
            if isinstance(image_url, list):
                image_url = image_url[0] if image_url else ""

            container = _url_to_container(image_url) if image_url else FALLBACK_CONTAINER
            skus.append({"sku_id": sku_id, "container": container})

        fetched_so_far = (page - 1) * PAGE_SIZE + len(products)
        log.info(f"  [{category}] fetched {fetched_so_far}/{total}")

        if fetched_so_far >= total or len(products) < PAGE_SIZE:
            break

        page += 1
        time.sleep(0.1)  # gentle rate limiting

    # Deduplicate by sku_id (keep first occurrence)
    seen: set[str] = set()
    unique: list[dict] = []
    for s in skus:
        if s["sku_id"] not in seen:
            seen.add(s["sku_id"])
            unique.append(s)

    log.info(f"  [{category}] unique SKUs: {len(unique)}")
    return unique


def write_csv(output_dir: Path, category: str, skus: list[dict]) -> Path:
    """Write sku_id,container,brand CSV for a category. Returns the file path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"babyshop-{category}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["sku_id", "container", "brand"])
        writer.writeheader()
        for s in skus:
            writer.writerow({"sku_id": s["sku_id"], "container": s["container"], "brand": "babyshop"})

    log.info(f"  Written: {filename}  ({len(skus)} rows)")
    return filename


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Baby Shop SKUs from Unbxd by category and write CSVs."
    )
    parser.add_argument(
        "--categories", nargs="+", required=True,
        metavar="CATEGORY",
        help="L2 category slugs, e.g. boysclothing-babyboys girlsclothing-babygirls",
    )
    parser.add_argument(
        "--output", default="input_csv_babyshop",
        metavar="DIR",
        help="Output directory for CSV files (default: input_csv_babyshop/)",
    )
    args = parser.parse_args()

    if not BABYSHOP_UNBXD_API_KEY or not BABYSHOP_UNBXD_SITE_KEY:
        log.error("BABYSHOP_UNBXD_API_KEY and BABYSHOP_UNBXD_SITE_KEY must be set in .env")
        sys.exit(1)

    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)

    output_dir = Path(args.output)
    total_skus = 0

    for category in args.categories:
        category = category.strip().lower()
        log.info(f"Fetching category: {category}")
        skus = fetch_all_skus_for_category(session, category)
        if skus:
            write_csv(output_dir, category, skus)
            total_skus += len(skus)
        else:
            log.warning(f"  No SKUs found for category: {category}")

    session.close()
    log.info(f"Done. Total SKUs written: {total_skus} across {len(args.categories)} category/ies")
    log.info(f"Run pipeline: python3 run.py {args.output}/ --workers 10 --pad-mode auto")


if __name__ == "__main__":
    main()
