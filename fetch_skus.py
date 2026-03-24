"""
fetch_skus.py — Fetch all SKUs from Unbxd search API and write to CSV.

Usage
-----
  # Query only (no category filter)
  python fetch_skus.py --query men

  # Query + category filter  (format:  q:filterKey:filterValue)
  python fetch_skus.py --query men:allCategories:men-tops
  python fetch_skus.py --query women:allCategories:women-dresses --output input_csv/dresses.csv

  # Extra options
  python fetch_skus.py --query bags --rows 48 --output input_csv/bags.csv

What it does
------------
  1. Calls Unbxd search API with pagination (page 1, 2, … until all results fetched)
  2. Extracts productCode → sku_id
  3. Maps imageUrl domain → Azure container name
  4. Writes sku_id, container, reprocess to CSV
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from urllib.parse import urlparse

import requests

# ── Unbxd API config ──────────────────────────────────────────
UNBXD_BASE_URL = (
    "https://search.unbxd.io"
    "/91b7857213222075bbcd3ea2dc72d026"
    "/ss-unbxd-aapac-prod-lifestyle-LandMark48741706891693"
    "/search"
)

# Full field list matching the production curl request
UNBXD_FIELDS = (
    "concept,createDate,employeePrice,isConceptDelivery,name,"
    "percentageDiscount,productType,productCode,color,sibiling,"
    "price,productUrl,childDetail,summary,uniqueId,wasPrice,"
    "imageUrl,gallaryImages,badgeVisible,inStock,approvalStatus,"
    "stats,sislogo,membershipPrice"
)

HEADERS = {
    "accept":              "*/*",
    "accept-language":     "en-US,en;q=0.9",
    "content-type":        "application/json",
    "origin":              "https://www.lifestylestores.com",
    "priority":            "u=1, i",
    "referer":             "https://www.lifestylestores.com/",
    "sec-ch-ua":           '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile":    "?0",
    "sec-ch-ua-platform":  '"macOS"',
    "sec-fetch-dest":      "empty",
    "sec-fetch-mode":      "cors",
    "sec-fetch-site":      "cross-site",
    "unbxd-user-id":       "uid-9134783168516",
    "user-agent":          (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

# ── Domain → Azure container mapping ─────────────────────────
DOMAIN_TO_CONTAINER = {
    "media-ea.landmarkshops.in": "in-media-ea",
    "media.landmarkshops.in":    "in-media",
    "media-us.landmarkshops.in": "in-media-us",
    "media-uk.landmarkshops.in": "in-media-uk",
}

FALLBACK_CONTAINER = "in-media"


# ── Helpers ───────────────────────────────────────────────────

def get_container_from_image_urls(image_urls: list[str] | str) -> str:
    """Map the first matching imageUrl domain to an Azure container name."""
    if isinstance(image_urls, str):
        image_urls = [image_urls]

    for url in image_urls:
        host = urlparse(url).netloc.lower()
        for domain, container in DOMAIN_TO_CONTAINER.items():
            if domain in host:
                return container

    return FALLBACK_CONTAINER


def fetch_page(query: str, page: int, rows: int, category: str | None) -> dict:
    """
    Fetch a single page from Unbxd API.

    Args:
        query:    Unbxd q= value  (e.g. "men")
        page:     1-based page number
        rows:     results per page
        category: allCategories filter value, e.g. "men-tops"  (or None)
    """
    # Always-on filters
    filters = ['inStock:"1"', 'approvalStatus:"1"']

    # Optional category filter passed from CLI  (e.g. allCategories:"men-tops")
    if category:
        filters.append(f'allCategories:"{category}"')

    params = {
        "q":                    query,
        "rows":                 rows,
        "page":                 page,
        "fields":               UNBXD_FIELDS,
        "filter":               filters,           # requests sends multi-value correctly
        "facet":                "true",
        "selectedfacet":        "true",
        "facet.multiselect":    "true",
        "stats":                "price",
    }
    resp = requests.get(UNBXD_BASE_URL, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


UNBXD_MAX_ROWS = 100   # Unbxd API hard cap — never returns more than 100 per page


def fetch_all_skus(query: str, category: str | None = None, rows: int = 100) -> list[dict]:
    """
    Paginate through ALL results for the query + optional category filter.
    Returns list of {sku_id, container, reprocess} dicts.
    """
    # Unbxd API silently caps at 100 — enforce it here so pagination is correct
    rows = min(rows, UNBXD_MAX_ROWS)

    skus:          list[dict] = []
    page           = 1
    total          = 0
    total_fetched  = 0

    cat_label = f"  category filter : allCategories={category}" if category else "  category filter : (none)"
    print(f"\n{'='*55}")
    print(f"  Unbxd PROD fetch")
    print(f"  query           : {query}")
    print(cat_label)
    print(f"  rows per page   : {rows}")
    print(f"{'='*55}")

    while True:
        print(f"  Page {page:>3}  fetched={total_fetched:<5} ...", end=" ", flush=True)

        try:
            data = fetch_page(query, page, rows, category)
        except requests.RequestException as exc:
            print(f"\nERROR: {exc}")
            sys.exit(1)

        response = data.get("response", {})
        products = response.get("products", [])
        total    = response.get("numberOfProducts", 0)

        print(f"got {len(products)} products  (total={total})")

        for product in products:
            sku_id     = product.get("productCode", "").strip()
            image_urls = product.get("imageUrl", [])
            container  = get_container_from_image_urls(image_urls)

            if sku_id:
                skus.append({
                    "sku_id":    sku_id,
                    "container": container,
                    "reprocess": "",
                })

        # Track actual products received (not rows * page — API may cap)
        total_fetched += len(products)
        if not products or total_fetched >= total:
            break

        page += 1
        time.sleep(0.3)   # polite delay between pages

    return skus


def write_csv(skus: list[dict], output_path: str) -> None:
    """Write SKU list to CSV file."""
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["sku_id", "container", "reprocess"])
        writer.writeheader()
        writer.writerows(skus)


# ── Container breakdown summary ───────────────────────────────

def print_summary(skus: list[dict], output_path: str) -> None:
    from collections import Counter
    counts = Counter(s["container"] for s in skus)

    print(f"\n{'='*50}")
    print(f"  DONE — {len(skus)} SKUs written to: {output_path}")
    print(f"{'='*50}")
    print(f"  Container breakdown:")
    for container, count in sorted(counts.items()):
        print(f"    {container:<20} {count:>5} SKUs")
    print(f"{'='*50}\n")
    print(f"  To run the pipeline:")
    print(f"    python run.py {os.path.dirname(output_path) or 'input_csv'}")
    print()


# ── Main ──────────────────────────────────────────────────────

def _parse_query(raw: str) -> tuple[str, str | None]:
    """
    Parse the --query argument into (q, category).

    Formats accepted
    ----------------
      "men"                          →  q="men",   category=None
      "men:allCategories:men-tops"   →  q="men",   category="men-tops"
      "women:allCategories:women-dresses"
    """
    parts = raw.split(":", 2)   # max 3 parts
    if len(parts) == 3 and parts[1].lower() == "allcategories":
        return parts[0].strip(), parts[2].strip()
    # No filter embedded — return raw query as-is
    return raw.strip(), None


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch SKUs from Unbxd (UAT) and write to CSV for the bulk pipeline.\n\n"
            "  python fetch_skus.py --query men\n"
            "  python fetch_skus.py --query men:allCategories:men-tops\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--query",  "-q", default="bags",
        help=(
            "Search query, optionally with a category filter.\n"
            "Format:  <q>  OR  <q>:allCategories:<category>\n"
            "Example: men:allCategories:men-tops"
        ))
    parser.add_argument("--output", "-o", default="input_csv/skus.csv",
        help="Output CSV path (default: input_csv/skus.csv)")
    parser.add_argument("--rows",   "-r", type=int, default=100,
        help="Results per page (default: 100, max: 100 — Unbxd API hard cap)")
    args = parser.parse_args()

    q, category = _parse_query(args.query)
    skus = fetch_all_skus(query=q, category=category, rows=args.rows)

    if not skus:
        print("No SKUs found. Check your query or API connection.")
        sys.exit(1)

    write_csv(skus, args.output)
    print_summary(skus, args.output)


if __name__ == "__main__":
    main()
