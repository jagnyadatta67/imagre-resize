"""
fetch_skus.py — Fetch all SKUs from Unbxd search API and write to CSV.

Usage
-----
  python fetch_skus.py --query bags --output input_csv/skus.csv
  python fetch_skus.py --query bags --output input_csv/skus.csv --rows 100

What it does
------------
  1. Calls Unbxd search API with pagination (rows per page, all pages)
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

HEADERS = {
    "unbxd-user-id":       "uid-9134783168516",
    "sec-ch-ua-platform":  '"macOS"',
    "Referer":             "https://www.lifestylestores.com/",
    "User-Agent":          (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua":           '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "Content-Type":        "application/json",
    "sec-ch-ua-mobile":    "?0",
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


def fetch_page(query: str, start: int, rows: int) -> dict:
    """Fetch a single page from Unbxd API."""
    params = {
        "q":      query,
        "rows":   rows,
        "start":  start,
        "fields": "productCode,imageUrl",
        "filter": ["inStock:\"1\"", "approvalStatus:\"1\""],
        "stats":  "price",
    }
    resp = requests.get(UNBXD_BASE_URL, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def fetch_all_skus(query: str, rows: int = 100) -> list[dict]:
    """
    Paginate through ALL results for the query.
    Returns list of {sku_id, container} dicts.
    """
    skus: list[dict] = []
    start = 0
    total = None

    print(f"\nFetching SKUs for query: '{query}'")
    print("-" * 50)

    while True:
        print(f"  Fetching page start={start} rows={rows} ...", end=" ", flush=True)

        try:
            data = fetch_page(query, start, rows)
        except requests.RequestException as exc:
            print(f"ERROR: {exc}")
            sys.exit(1)

        response      = data.get("response", {})
        products      = response.get("products", [])
        total         = response.get("numberOfProducts", 0)

        print(f"got {len(products)} products  (total={total})")

        for product in products:
            sku_id      = product.get("productCode", "").strip()
            image_urls  = product.get("imageUrl", [])
            container   = get_container_from_image_urls(image_urls)

            if sku_id:
                skus.append({
                    "sku_id":    sku_id,
                    "container": container,
                    "reprocess": "",
                })

        start += rows

        # Stop when we've fetched all pages
        if start >= total or not products:
            break

        # Polite delay between pages
        time.sleep(0.3)

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

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch SKUs from Unbxd and write to CSV for the bulk pipeline"
    )
    parser.add_argument("--query",  "-q", default="bags",
        help="Search query (default: bags)")
    parser.add_argument("--output", "-o", default="input_csv/skus.csv",
        help="Output CSV path (default: input_csv/skus.csv)")
    parser.add_argument("--rows",   "-r", type=int, default=100,
        help="Results per page (default: 100, max: 100)")
    args = parser.parse_args()

    skus = fetch_all_skus(query=args.query, rows=args.rows)

    if not skus:
        print("No SKUs found. Check your query or API connection.")
        sys.exit(1)

    write_csv(skus, args.output)
    print_summary(skus, args.output)


if __name__ == "__main__":
    main()
