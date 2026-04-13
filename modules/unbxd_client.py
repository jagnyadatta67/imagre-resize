"""
unbxd_client.py — Fetch product data from Unbxd search API.

Used by the web UI "Process by SKU List" / "Baby Shop" features to:
  1. Look up each SKU in Unbxd → get gallaryImages, allCategories
  2. Derive Azure container from image URL domain
  3. Auto-detect pad mode from L2 category
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

from config import (
    BABYSHOP_UNBXD_API_KEY, BABYSHOP_UNBXD_SITE_KEY,
)

log = logging.getLogger("unbxd_client")

UNBXD_API_KEY    = os.getenv("UNBXD_API_KEY",  "91b7857213222075bbcd3ea2dc72d026")
UNBXD_SITE_KEY   = os.getenv("UNBXD_SITE_KEY", "ss-unbxd-aapac-prod-lifestyle-LandMark48741706891693")
UNBXD_SEARCH_URL = f"https://search.unbxd.io/{UNBXD_API_KEY}/{UNBXD_SITE_KEY}/search"

BABYSHOP_SEARCH_URL = (
    f"https://search.unbxd.io/{BABYSHOP_UNBXD_API_KEY}/{BABYSHOP_UNBXD_SITE_KEY}/search"
)

DOMAIN_TO_CONTAINER: dict[str, str] = {
    "media-ea.landmarkshops.in": "in-media-ea",
    "media.landmarkshops.in":    "in-media",
    "media-us.landmarkshops.in": "in-media-us",
    "media-uk.landmarkshops.in": "in-media-uk",
}

UNBXD_HEADERS = {
    "accept":          "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type":    "application/json",
    "origin":          "https://www.lifestylestores.com",
    "referer":         "https://www.lifestylestores.com/",
    "unbxd-user-id":   "uid-pipeline-skuprocess",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
}


_AUTO_KEYWORDS = ("beauty", "watches", "addons", "sunglass", "accessories")

def pad_mode_for_category(category: str) -> str:
    """
    Auto-detect Cloudinary pad mode from L2 category.

      Always 'auto' (pad with bg) if category contains any of:
        beauty, watches, addons, sunglass, accessories

      Otherwise:
        men-*, women-*, kids-*  →  'no'   (crop:fill — apparel)
        everything else         →  'auto'
    """
    cat = (category or "").lower().strip()
    if any(kw in cat for kw in _AUTO_KEYWORDS):
        return "auto"
    if cat.startswith(("men", "women", "kids")):
        return "no"
    return "auto"


def url_to_container(url: str) -> str:
    """Extract Azure container name from a CDN image URL domain."""
    try:
        domain = urlparse(url).netloc
        return DOMAIN_TO_CONTAINER.get(domain, "in-media")
    except Exception:
        return "in-media"


def _fetch_single(sku_id: str, session: requests.Session, search_url: str = None) -> dict:
    """
    Query Unbxd for one SKU. Returns:
      found=True  → {sku_id, found, category, container, image_count, pad_mode, source_blobs}
      found=False → {sku_id, found, reason}
    search_url defaults to Lifestyle UNBXD_SEARCH_URL; pass BABYSHOP_SEARCH_URL for Baby Shop.
    """
    url    = search_url or UNBXD_SEARCH_URL
    params = [
        ("rows",   1),
        ("page",   1),
        ("q",      sku_id),
        ("facet",  "false"),
        ("fields", "productCode,gallaryImages,allCategories"),
        ("filter", 'inStock:"1"'),
        ("filter", 'approvalStatus:"1"'),
    ]
    try:
        resp = session.get(
            url, params=params,
            headers=UNBXD_HEADERS, timeout=15,
        )
        resp.raise_for_status()
        products = resp.json().get("response", {}).get("products", [])

        # Prefer exact productCode match
        product = next(
            (p for p in products if p.get("productCode") == sku_id),
            products[0] if products else None,
        )

        if not product:
            return {"sku_id": sku_id, "found": False, "reason": "not_in_unbxd"}

        gallery = product.get("gallaryImages") or []
        if not isinstance(gallery, list):
            gallery = [gallery] if gallery else []

        if not gallery:
            return {"sku_id": sku_id, "found": False, "reason": "no_gallery_images"}

        # Container from first gallery URL domain
        container = url_to_container(gallery[0])

        # Extract EXACT blob paths from gallery URLs — do NOT use SKU prefix listing
        # e.g. https://media-uk.landmarkshops.in/lifestyle/1000008664304-1000008664303_01-2100.jpg
        #   →  lifestyle/1000008664304-1000008664303_01-2100.jpg
        blobs = [urlparse(u).path.lstrip("/") for u in gallery if u]

        # L2 category = entry with exactly 2 hyphen-separated parts
        all_cats = product.get("allCategories") or []
        l2 = next(
            (c.strip() for c in all_cats if len(c.strip().split("-")) == 2),
            all_cats[0].strip() if all_cats else "unknown",
        )

        return {
            "sku_id":       sku_id,
            "found":        True,
            "category":     l2,
            "container":    container,
            "image_count":  len(gallery),
            "pad_mode":     pad_mode_for_category(l2),
            "source_blobs": blobs,   # exact blob paths → passed to pipeline worker
        }

    except Exception as exc:
        log.warning(f"Unbxd fetch error for {sku_id}: {exc}")
        return {"sku_id": sku_id, "found": False, "reason": str(exc)}


def fetch_skus_from_unbxd(
    sku_ids: list[str],
    workers: int = 20,
) -> dict:
    """
    Fetch multiple SKUs from Unbxd in parallel.

    Returns:
      {
        "found":     [{sku_id, category, container, image_count, pad_mode}, ...],
        "not_found": [{sku_id, reason}, ...]
      }
    """
    if not sku_ids:
        return {"found": [], "not_found": []}

    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections = min(workers, 20),
        pool_maxsize     = min(workers, 20),
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)

    found: list[dict]     = []
    not_found: list[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_fetch_single, sku_id.strip(), session): sku_id
            for sku_id in sku_ids if sku_id.strip()
        }
        for future in as_completed(futures):
            result = future.result()
            if result.get("found"):
                found.append(result)
            else:
                not_found.append(result)
                log.info(f"SKU not found in Unbxd: {result['sku_id']} — {result.get('reason')}")

    session.close()

    # Sort found by sku_id for consistent ordering
    found.sort(key=lambda x: x["sku_id"])
    return {"found": found, "not_found": not_found}


def fetch_skus_from_babyshop_unbxd(
    sku_ids: list[str],
    workers: int = 20,
) -> dict:
    """
    Fetch multiple SKUs from Babyshop Unbxd in parallel.
    Same structure as fetch_skus_from_unbxd but uses BABYSHOP_SEARCH_URL.

    Returns:
      {
        "found":     [{sku_id, category, container, image_count, pad_mode, source_blobs}, ...],
        "not_found": [{sku_id, reason}, ...]
      }
    """
    if not sku_ids:
        return {"found": [], "not_found": []}

    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections = min(workers, 20),
        pool_maxsize     = min(workers, 20),
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)

    found: list[dict]     = []
    not_found: list[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_fetch_single, sku_id.strip(), session, BABYSHOP_SEARCH_URL): sku_id
            for sku_id in sku_ids if sku_id.strip()
        }
        for future in as_completed(futures):
            result = future.result()
            if result.get("found"):
                found.append(result)
            else:
                not_found.append(result)
                log.info(f"[Babyshop] SKU not found: {result['sku_id']} — {result.get('reason')}")

    session.close()

    found.sort(key=lambda x: x["sku_id"])
    return {"found": found, "not_found": not_found}
