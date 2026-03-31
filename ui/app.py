"""
app.py — Flask UI for the Bulk Image Pipeline.

Two views on a single page, toggled by a pill switch in the navbar.

  Business View  — Search SKU → see processed images with Original toggle
  Developer View — 3 tabs: Runs | Failed SKUs | SKU Detail

Routes
------
  GET /                           Unified home (both views)
  GET /?bq=<sku>                  Business search results
  GET /?dq=<sku>                  Developer SKU detail search
  GET /api/run/<run_id>/skus      AJAX: SKU queue rows for expandable run row
  GET /sku/<sku_id>               Legacy redirect → /?dq=<sku_id>
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from urllib.parse import urlparse

import mysql.connector
from mysql.connector import pooling as _mysql_pooling
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for
from markupsafe import Markup

# ── Load .env from parent directory ──────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ── Module imports (pipeline modules live in BASE_DIR) ────────
# Must come AFTER load_dotenv so env vars are available at import time
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from modules.azure_client import AzureClient                     # noqa: E402
from modules.converter import reprocess_single_image             # noqa: E402

_azure = AzureClient()   # singleton — thread-safe per Azure SDK docs

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB       = os.getenv("MYSQL_DB", "image_pipeline")
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

AZURE_ACCOUNT_NAME = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY  = os.getenv("AZURE_ACCOUNT_KEY", "")
TARGET_CONTAINER   = os.getenv("TARGET_CONTAINER", "lifestyle-converted")

# ── Category group metadata (L1 pill labels + emoji) ─────────
_GROUP_META: dict[str, dict] = {
    "men":      {"label": "Men",      "emoji": "👔"},
    "women":    {"label": "Women",    "emoji": "👗"},
    "kids":     {"label": "Kids",     "emoji": "👦"},
    "footwear": {"label": "Footwear", "emoji": "👟"},
    "beauty":   {"label": "Beauty",   "emoji": "💄"},
    "bags":     {"label": "Bags",     "emoji": "👜"},
}

# ── Cloudflare CDN — container → CF domain mapping ────────────
# Reverse of DOMAIN_TO_CONTAINER in config.py.
# Processed images are served via CF Image Resizing — zero Azure egress.
_CF_DOMAIN_MAP: dict[str, str] = {
    "in-media-ea": "media-ea.landmarkshops.in",
    "in-media":    "media.landmarkshops.in",
    "in-media-us": "media-us.landmarkshops.in",
    "in-media-uk": "media-uk.landmarkshops.in",
}


app = Flask(__name__)
app.secret_key = "pipeline-ui-secret"

# Expose GROUP_META as a JS-safe dict for all templates
app.jinja_env.globals["_GROUP_META_JS"] = json.dumps(
    {k: {"label": v["label"]} for k, v in _GROUP_META.items()}
)


# ── DB connection pool ────────────────────────────────────────
# Single pool shared across all Flask requests.
# pool_size=5 is enough for the UI — all queries are short reads.
# Connections are returned to the pool when db.close() is called.

_UI_POOL: _mysql_pooling.MySQLConnectionPool | None = None


def _get_pool() -> _mysql_pooling.MySQLConnectionPool:
    global _UI_POOL
    if _UI_POOL is None:
        _UI_POOL = _mysql_pooling.MySQLConnectionPool(
            pool_name  = "ui",
            pool_size  = 5,
            host       = MYSQL_HOST,
            port       = MYSQL_PORT,
            database   = MYSQL_DB,
            user       = MYSQL_USER,
            password   = MYSQL_PASSWORD,
            autocommit = True,
            charset    = "utf8mb4",
            collation  = "utf8mb4_unicode_ci",
        )
    return _UI_POOL


def get_db() -> mysql.connector.MySQLConnection:
    """Return a pooled connection. Caller must call db.close() to return it."""
    return _get_pool().get_connection()


# ── Azure SAS helpers ─────────────────────────────────────────

def make_sas_url(azure_url: str, hours: int = 2) -> str:
    """Return a time-limited SAS URL for a private Azure blob."""
    if not azure_url:
        return ""
    try:
        parsed    = urlparse(azure_url)
        parts     = parsed.path.lstrip("/").split("/", 1)
        container = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ""
        sas = generate_blob_sas(
            account_name   = AZURE_ACCOUNT_NAME,
            container_name = container,
            blob_name      = blob_name,
            account_key    = AZURE_ACCOUNT_KEY,
            permission     = BlobSasPermissions(read=True),
            expiry         = datetime.utcnow() + timedelta(hours=hours),
        )
        return f"{azure_url}?{sas}"
    except Exception:
        return azure_url


def make_original_sas_url(processed_azure_url: str) -> str:
    """
    Derive original-image SAS URL from a processed URL.

    Processed : {container}/lifestyle-converted/{filename}
    Original  : {container}/lifestyle/{filename}
    """
    if processed_azure_url and f"/{TARGET_CONTAINER}/" in processed_azure_url:
        original_url = processed_azure_url.replace(f"/{TARGET_CONTAINER}/", "/lifestyle/")
        return make_sas_url(original_url)
    return ""


def make_cf_url(azure_url: str) -> str:
    """
    Convert an Azure blob URL to a plain Cloudflare CDN URL (no resizing).

    Azure : https://lmgonlinemedia.blob.core.windows.net/{container}/{blob_path}
    CF    : https://{cf_domain}/{blob_path}

    Returns "" when the container is not in _CF_DOMAIN_MAP —
    caller should fall back to SAS URL.
    """
    if not azure_url:
        return ""
    try:
        parsed    = urlparse(azure_url)
        parts     = parsed.path.lstrip("/").split("/", 1)
        container = parts[0]
        blob_path = parts[1] if len(parts) > 1 else ""
        domain    = _CF_DOMAIN_MAP.get(container)
        if not domain or not blob_path:
            return ""
        return f"https://{domain}/{blob_path}"
    except Exception:
        return ""


def make_cf_original_url(processed_azure_url: str) -> str:
    """
    Derive CF CDN URL for the original (pre-processing) image.
    Swaps /{TARGET_CONTAINER}/ → /lifestyle/ in the blob path, then builds CF URL.
    Returns "" when container not mapped or path doesn't contain TARGET_CONTAINER.
    """
    if not processed_azure_url or f"/{TARGET_CONTAINER}/" not in processed_azure_url:
        return ""
    original_azure = processed_azure_url.replace(f"/{TARGET_CONTAINER}/", "/lifestyle/")
    return make_cf_url(original_azure)


# ── Template filters ──────────────────────────────────────────

@app.template_filter("status_badge")
def status_badge_filter(status: str) -> Markup:
    cfg = {
        "done":             ("✅", "badge-done"),
        "done_with_errors": ("⚠️",  "badge-warn"),
        "failed":           ("❌", "badge-failed"),
        "skipped":          ("⏭️",  "badge-skipped"),
        "pending":          ("⏳", "badge-pending"),
        "running":          ("🔄", "badge-running"),
    }
    emoji, cls = cfg.get(status or "", ("❓", "badge-pending"))
    return Markup(f'<span class="badge {cls}">{emoji} {status}</span>')


@app.template_filter("method_badge")
def method_badge_filter(method: str) -> Markup:
    cfg = {
        "cloudinary_pad":     ("☁️",  "badge-cloudinary"),
        "cloudinary_reframe": ("🔲", "badge-cloudinary"),
        "pillow":             ("✂️",  "badge-pillow"),
        "pillow_crop":        ("✂️",  "badge-pillow"),
    }
    emoji, cls = cfg.get(method or "", ("❓", "badge-pending"))
    return Markup(f'<span class="badge {cls}">{emoji} {method}</span>')


@app.template_filter("fmt_dt")
def fmt_dt_filter(dt) -> str:
    if not dt:
        return "—"
    if isinstance(dt, str):
        return dt[:19]
    return dt.strftime("%Y-%m-%d %H:%M:%S")


@app.template_filter("run_duration")
def run_duration_filter(row: dict) -> str:
    s = row.get("started_at")
    f = row.get("finished_at")
    if not s or not f:
        return "—"
    secs = int((f - s).total_seconds())
    if secs < 60:
        return f"{secs}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


@app.template_filter("from_json")
def from_json_filter(val) -> list:
    if not val:
        return []
    try:
        return json.loads(val)
    except Exception:
        return []


# ── Data helpers ──────────────────────────────────────────────

def _get_category_groups(cur) -> list[dict]:
    """
    Return L1 group pills: aggregate sku_results by the prefix before the first '-'.
    e.g. 'beauty-face' → group 'beauty'.
    Ordered by a fixed display order (defined in _GROUP_META), unknowns go last.
    """
    cur.execute("""
        SELECT
            LOWER(SUBSTRING_INDEX(TRIM(COALESCE(category,'')), '-', 1)) AS grp,
            COUNT(*)                                                      AS total,
            SUM(status IN ('done','skipped'))                             AS passed,
            SUM(status = 'failed')                                        AS failed
        FROM  sku_results
        WHERE category IS NOT NULL AND category != ''
        GROUP BY grp
        HAVING grp != ''
        ORDER BY grp
    """)
    rows   = cur.fetchall()
    order  = list(_GROUP_META.keys())
    groups = []
    for row in rows:
        meta = _GROUP_META.get(row["grp"], {"label": row["grp"].title(), "emoji": "🏷️"})
        groups.append({
            "name":   row["grp"],
            "label":  meta["label"],
            "emoji":  meta["emoji"],
            "total":  int(row["total"]  or 0),
            "passed": int(row["passed"] or 0),
            "failed": int(row["failed"] or 0),
        })
    # Sort by fixed order, unknowns at the end
    groups.sort(key=lambda g: order.index(g["name"]) if g["name"] in order else 999)
    return groups


def _get_stats(cur) -> dict:
    cur.execute("""
        SELECT
            COUNT(*)                                    AS total,
            SUM(status IN ('done', 'skipped'))          AS passed,
            SUM(status = 'done')                        AS done,
            SUM(status = 'failed')                      AS failed,
            SUM(status = 'skipped')                     AS skipped,
            SUM(azure_uploaded)                         AS total_images
        FROM sku_results
    """)
    return cur.fetchone() or {}


def _get_recent_runs(cur, limit: int = 25) -> list:
    cur.execute("""
        SELECT run_id, csv_folder, csv_files, total_skus,
               done_count, failed_count, skipped_count,
               status, started_at, finished_at
        FROM   sku_runs
        ORDER  BY started_at DESC
        LIMIT  %s
    """, (limit,))
    return cur.fetchall()


def _get_failed_skus(cur) -> list:
    cur.execute("""
        SELECT sku_id, container_name, blob_count,
               error_code, error_msg, last_processed_at, run_id
        FROM   sku_results
        WHERE  status = 'failed'
        ORDER  BY last_processed_at DESC
    """)
    return cur.fetchall()


def _build_business_results(cur, query: str) -> list:
    """
    For each matching SKU return its summary + per-image processed/original URLs.
    Used exclusively by the Business View.
    """
    cur.execute("""
        SELECT sku_id, status, container_name, blob_count, azure_uploaded, last_processed_at
        FROM   sku_results
        WHERE  sku_id LIKE %s
        ORDER  BY last_processed_at DESC
        LIMIT  50
    """, (f"%{query}%",))
    skus = cur.fetchall()

    results = []
    for sku in skus:
        # ── latest row per filename (for azure_url / status) ──────────────
        cur.execute("""
            SELECT filename, azure_url, status, reprocess_count
            FROM   image_results
            WHERE  sku_id = %s
            ORDER  BY processed_at DESC
        """, (sku["sku_id"],))
        all_imgs = cur.fetchall()

        # ── MAX(reprocess_count) per filename ─────────────────────────────
        # Pipeline re-runs insert new rows with reprocess_count=0, which would
        # overwrite the version param. Fetching the MAX separately ensures the
        # highest version is always used for CF cache-busting, regardless of
        # which row is latest.
        cur.execute("""
            SELECT filename, COALESCE(MAX(reprocess_count), 0) AS max_rc
            FROM   image_results
            WHERE  sku_id = %s
            GROUP  BY filename
        """, (sku["sku_id"],))
        max_rc_map = {r["filename"]: (r["max_rc"] or 0) for r in cur.fetchall()}

        # Deduplicate: keep the latest row per filename
        seen   = set()
        images = []
        for img in all_imgs:
            fname     = img["filename"]
            azure_url = img["azure_url"] or ""
            if fname and fname not in seen:
                seen.add(fname)

                # CF CDN URL — always use MAX(reprocess_count) across all rows
                # so that pipeline re-runs don't reset the cache-busting param
                rcount        = max_rc_map.get(fname, 0)
                processed_url = make_cf_url(azure_url) or make_sas_url(azure_url)
                if rcount > 0:
                    sep           = "&" if "?" in processed_url else "?"
                    processed_url = f"{processed_url}{sep}v={rcount}"
                original_url  = make_cf_original_url(azure_url) or make_original_sas_url(azure_url)

                images.append({
                    "filename":       fname,
                    "processed_url":  processed_url,
                    "original_url":   original_url,
                    "has_original":   bool(original_url),
                    "status":         img["status"],
                })
        images.sort(key=lambda x: x["filename"])
        sku["images"] = images
        results.append(sku)
    return results


def _build_dev_sku(cur, query: str):
    """
    Return (sku_row, images) for Developer SKU Detail tab.
    Tries exact match first, then partial.
    """
    cur.execute("SELECT * FROM sku_results WHERE sku_id = %s", (query,))
    sku = cur.fetchone()
    if not sku:
        cur.execute("""
            SELECT * FROM sku_results WHERE sku_id LIKE %s LIMIT 1
        """, (f"%{query}%",))
        sku = cur.fetchone()
    if not sku:
        return None, []

    cur.execute("""
        SELECT filename, method, cloudinary_url, azure_url,
               status, error_code, error_msg, processed_at
        FROM   image_results
        WHERE  sku_id = %s
        ORDER  BY processed_at DESC
    """, (sku["sku_id"],))
    all_imgs = cur.fetchall()

    seen   = set()
    images = []
    for img in all_imgs:
        fname = img["filename"]
        if fname and fname not in seen:
            seen.add(fname)
            img["preview_url"] = (
                img["cloudinary_url"] or make_sas_url(img["azure_url"])
            )
            images.append(img)
    images.sort(key=lambda x: x["filename"] or "")
    return sku, images


# ── Routes ────────────────────────────────────────────────────

@app.route("/")
def index():
    bq = request.args.get("bq", "").strip()   # business search query
    dq = request.args.get("dq", "").strip()   # developer SKU search query
    show_reprocess = request.args.get("re") == "1"

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        stats            = _get_stats(cur)
        recent_runs      = _get_recent_runs(cur)
        failed_skus      = _get_failed_skus(cur)
        category_groups  = _get_category_groups(cur)

        business_results    = _build_business_results(cur, bq) if bq else []
        dev_sku, dev_images = _build_dev_sku(cur, dq) if dq else (None, [])
    finally:
        cur.close()
        db.close()   # returns connection to pool

    return render_template(
        "index.html",
        stats            = stats,
        recent_runs      = recent_runs,
        failed_skus      = failed_skus,
        category_groups  = category_groups,
        business_results = business_results,
        bq               = bq,
        dev_sku          = dev_sku,
        dev_images       = dev_images,
        dq               = dq,
        show_reprocess   = show_reprocess,
    )


@app.route("/api/run/<run_id>/skus")
def api_run_skus(run_id: str):
    """
    AJAX endpoint — returns JSON list of SKU queue rows for a given run.
    Used by the Developer → Runs tab expandable row.
    """
    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT q.id, q.sku_id, q.status, q.worker_id,
                   q.claimed_at, q.finished_at, q.error_msg,
                   r.container_name, r.blob_count,
                   r.azure_uploaded, r.error_code
            FROM   sku_queue q
            LEFT JOIN sku_results r ON r.sku_id = q.sku_id
            WHERE  q.run_id = %s
            ORDER  BY q.id
        """, (run_id,))
        rows = cur.fetchall()
    finally:
        cur.close()
        db.close()   # returns connection to pool

    for row in rows:
        # Serialize datetimes for JSON
        for k in ("claimed_at", "finished_at"):
            if row[k]:
                row[k] = row[k].strftime("%Y-%m-%d %H:%M:%S")
        # Duration in seconds
        if row.get("claimed_at") and row.get("finished_at"):
            s = datetime.strptime(row["claimed_at"],  "%Y-%m-%d %H:%M:%S")
            f = datetime.strptime(row["finished_at"], "%Y-%m-%d %H:%M:%S")
            row["duration_s"] = int((f - s).total_seconds())
        else:
            row["duration_s"] = None

    return jsonify(rows)


# ── SKU List (paginated business view) ───────────────────────

@app.route("/skus")
def sku_list():
    category = request.args.get("category", "").strip().lower()
    group    = request.args.get("group",    "").strip().lower()   # L1 "View All" filter
    try:
        page = max(1, int(request.args.get("page", 1)))
    except (ValueError, TypeError):
        page = 1
    status   = request.args.get("status", "all")
    q        = request.args.get("q", "").strip()
    per_page = 24
    offset   = (page - 1) * per_page

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        category_groups = _get_category_groups(cur)

        if not category and not group:
            # ── Level 0: Full category grid ───────────────────
            cur.execute("""
                SELECT
                    COALESCE(NULLIF(category,''), 'uncategorized') AS category,
                    COUNT(*)                                        AS total,
                    SUM(status IN ('done','skipped'))               AS passed,
                    SUM(status = 'failed')                          AS failed,
                    SUM(azure_uploaded)                             AS total_images,
                    JSON_UNQUOTE(JSON_EXTRACT(
                        COALESCE(
                            MAX(CASE WHEN status='done'    THEN azure_urls END),
                            MAX(CASE WHEN status='skipped' THEN azure_urls END)
                        ), '$[0]'
                    ))                                              AS sample_azure_url
                FROM  sku_results
                GROUP BY COALESCE(NULLIF(category,''), 'uncategorized')
                ORDER BY category
            """)
            categories = cur.fetchall()
            for cat in categories:
                raw = cat.get("sample_azure_url") or ""
                cat["thumb_url"] = make_cf_url(raw) or make_sas_url(raw)
            return render_template(
                "skus.html",
                categories      = categories,
                category        = None,
                group           = None,
                category_groups = category_groups,
            )

        if group and not category:
            # ── Level 1 → "View All {group}" subcategory grid ─
            # Shows only subcategory cards that belong to this group prefix.
            meta       = _GROUP_META.get(group, {"label": group.title(), "emoji": "🏷️"})
            group_label = meta["label"]
            cur.execute("""
                SELECT
                    COALESCE(NULLIF(category,''), 'uncategorized') AS category,
                    COUNT(*)                                        AS total,
                    SUM(status IN ('done','skipped'))               AS passed,
                    SUM(status = 'failed')                          AS failed,
                    COALESCE(SUM(azure_uploaded), 0)               AS total_images,
                    JSON_UNQUOTE(JSON_EXTRACT(
                        COALESCE(
                            MAX(CASE WHEN status='done'    THEN azure_urls END),
                            MAX(CASE WHEN status='skipped' THEN azure_urls END)
                        ), '$[0]'
                    ))                                              AS sample_azure_url
                FROM  sku_results
                WHERE LOWER(SUBSTRING_INDEX(TRIM(COALESCE(category,'')), '-', 1)) = %s
                GROUP BY category
                ORDER BY category
            """, (group,))
            categories = cur.fetchall()
            for cat in categories:
                raw = cat.get("sample_azure_url") or ""
                cat["thumb_url"] = make_cf_url(raw) or make_sas_url(raw)
            return render_template(
                "skus.html",
                categories      = categories,
                category        = None,
                group           = group,
                group_label     = group_label,
                category_groups = category_groups,
            )

        # ── Level 2: SKU list for selected category ───────────
        # "uncategorized" matches rows where category IS NULL or empty
        if category == "uncategorized":
            where_parts = ["(category IS NULL OR category = '')"]
            params      = []
        else:
            where_parts = ["category = %s"]
            params      = [category]

        if status == "done":
            where_parts.append("status IN ('done', 'skipped')")
        elif status == "failed":
            where_parts.append("status = 'failed'")
        if q:
            where_parts.append("sku_id LIKE %s")
            params.append(f"%{q}%")
        where = "WHERE " + " AND ".join(where_parts)

        cur.execute(f"SELECT COUNT(*) AS total FROM sku_results {where}", params)
        total = (cur.fetchone() or {}).get("total", 0)

        cur.execute(f"""
            SELECT s.sku_id, s.status, s.blob_count, s.azure_uploaded, s.last_processed_at,
                   COALESCE(
                       s.listing_azure_url,
                       JSON_UNQUOTE(JSON_EXTRACT(s.azure_urls, '$[0]'))
                   )                                                 AS first_azure_url,
                   (SELECT MAX(ir.reprocess_count)
                    FROM   image_results ir
                    WHERE  ir.sku_id = s.sku_id)                    AS max_reprocess_version,
                   (SELECT COUNT(DISTINCT ir2.filename)
                    FROM   image_results ir2
                    WHERE  ir2.sku_id = s.sku_id
                      AND  ir2.reprocess_count = (
                               SELECT MAX(ir3.reprocess_count)
                               FROM   image_results ir3
                               WHERE  ir3.sku_id = s.sku_id
                           )
                      AND  ir2.reprocess_count > 0)                 AS reprocessed_count
            FROM   sku_results s
            {where}
            ORDER  BY s.last_processed_at DESC
            LIMIT  %s OFFSET %s
        """, params + [per_page, offset])
        rows = cur.fetchall()
    finally:
        cur.close()
        db.close()

    skus = []
    for row in rows:
        # Thumbnail is always the _01 image (first_azure_url = listing_azure_url ?? azure_urls[0]).
        # The blob is overwritten in-place on reprocess, so the URL never changes —
        # only ?v=N changes to bust the Cloudflare cache.
        raw      = row.get("first_azure_url") or ""
        version  = row.get("max_reprocess_version") or 0
        rc       = row.get("reprocessed_count") or 0
        base_url = make_cf_url(raw) or make_sas_url(raw)
        if base_url:
            sep      = "&" if "?" in base_url else "?"
            base_url = f"{base_url}{sep}v={version}"
        image_count = rc if rc > 0 else (row.get("azure_uploaded") or 0)
        skus.append({**row, "thumb_url": base_url, "image_count": image_count})

    total_pages = max(1, (total + per_page - 1) // per_page)

    return render_template(
        "skus.html",
        categories      = None,
        category        = category,
        group           = group,
        skus            = skus,
        page            = page,
        total_pages     = total_pages,
        total           = total,
        per_page        = per_page,
        status          = status,
        q               = q,
        category_groups = category_groups,
    )


# ── Category group L2 subcategories (AJAX) ───────────────────

@app.route("/api/category-group/<group_name>")
def api_category_group(group_name: str):
    """
    Return subcategories + counts for a given L1 group name.
    e.g. /api/category-group/beauty  →
      [{ category:'beauty-face', label:'Face', total:874, passed:850, failed:24 }, ...]
    """
    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                category,
                COUNT(*)                           AS total,
                SUM(status IN ('done','skipped'))   AS passed,
                SUM(status = 'failed')              AS failed
            FROM  sku_results
            WHERE LOWER(SUBSTRING_INDEX(TRIM(COALESCE(category,'')), '-', 1)) = %s
              AND category IS NOT NULL AND category != ''
            GROUP BY category
            ORDER BY category
        """, (group_name.lower(),))
        rows = cur.fetchall()
    finally:
        cur.close()
        db.close()

    result = []
    for r in rows:
        cat = r["category"] or ""
        # Label = everything after the first '-', title-cased
        label = cat.split("-", 1)[1].replace("-", " ").title() if "-" in cat else cat.title()
        result.append({
            "category": cat,
            "label":    label,
            "total":    int(r["total"]  or 0),
            "passed":   int(r["passed"] or 0),
            "failed":   int(r["failed"] or 0),
        })
    return jsonify(result)


# ── Per-image manual reprocess ────────────────────────────────

@app.route("/api/reprocess-image", methods=["POST"])
def api_reprocess_image():
    """
    Manually reprocess one image without Vision AI.

    POST JSON body:
      { "sku_id": "...", "filename": "...", "method": "gen_fill|auto|fill|pillow" }

    Flow:
      1. Look up container_name from sku_results
      2. Download original from {container}/lifestyle/{filename}
      3. Apply chosen method (Cloudinary PAD/FILL or Pillow)
      4. Upload result to {container}/lifestyle-newc/{filename}
      5. Insert audit row into image_results
      6. Return { ok, new_url, method, used_cloudinary }
    """
    data     = request.get_json(force=True) or {}
    sku_id   = (data.get("sku_id")   or "").strip()
    filename = (data.get("filename") or "").strip()
    method   = (data.get("method")   or "auto").strip()

    if not sku_id or not filename:
        return jsonify({"ok": False, "error": "sku_id and filename are required"}), 400
    if method not in {"gen_fill", "auto", "fill", "center"}:
        return jsonify({"ok": False, "error": "method must be gen_fill, auto, fill, or center"}), 400

    # ── 1. Fetch container from DB ────────────────────────────
    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT container_name FROM sku_results WHERE sku_id = %s",
            (sku_id,)
        )
        row = cur.fetchone()
    finally:
        cur.close()
        db.close()

    if not row or not row.get("container_name"):
        return jsonify({"ok": False, "error": f"No DB record for SKU '{sku_id}'"}), 404

    container_name = row["container_name"]
    logger = logging.getLogger(f"reprocess.{sku_id[:30]}")

    try:
        # ── 2. Download original ──────────────────────────────
        blob_name   = f"lifestyle/{filename}"
        image_bytes = _azure.download_blob_bytes(container_name, blob_name)

        # ── 3. Reprocess (no Vision AI) ───────────────────────
        output_bytes, used_cloudinary, cloudinary_url, _vision_data, _transform_data = reprocess_single_image(
            image_bytes, filename, sku_id, method, logger
        )

        # ── 4. Upload result to lifestyle-newc/ ───────────────
        azure_url = _azure.upload_to_newc(filename, output_bytes, container_name)

        # ── 5. Save audit row (with incremented reprocess_count) ─
        db2  = get_db()
        cur2 = db2.cursor(dictionary=True)
        try:
            # Get MAX reprocess_count across entire SKU (not just this file)
            # so all images in the SKU stay on the same version number.
            cur2.execute("""
                SELECT COALESCE(MAX(reprocess_count), 0) AS max_count
                FROM   image_results
                WHERE  sku_id = %s
            """, (sku_id,))
            max_row         = cur2.fetchone()
            reprocess_count = (max_row["max_count"] or 0) + 1

            cur2.execute("""
                INSERT INTO image_results
                    (run_id, sku_id, blob_name, filename, method,
                     cloudinary_url, azure_url, status, processed_at,
                     reprocess_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'done', %s, %s)
            """, (
                "manual-reprocess", sku_id, blob_name, filename,
                f"reprocess_{method}", cloudinary_url, azure_url, datetime.now(),
                reprocess_count,
            ))

            # Sync all rows for this SKU to the same reprocess_count
            cur2.execute("""
                UPDATE image_results
                SET    reprocess_count = %s
                WHERE  sku_id = %s
            """, (reprocess_count, sku_id))

            # If the reprocessed image is the _01 (listing) image, keep listing_azure_url
            # in sku_results current — the blob URL is the same (overwritten in-place),
            # but we refresh the column so COALESCE always returns an explicit value.
            cur2.execute("""
                UPDATE sku_results
                SET    listing_azure_url = CASE
                           WHEN listing_azure_url IS NULL
                             OR listing_azure_url LIKE %s
                           THEN %s
                           ELSE listing_azure_url
                       END
                WHERE  sku_id = %s
            """, (f"%/{filename}", azure_url, sku_id))
        finally:
            cur2.close()
            db2.close()

        # ── 6. Build CF URL with version param to bust cache ─────
        base_url = make_cf_url(azure_url) or make_sas_url(azure_url)
        sep      = "&" if "?" in base_url else "?"
        new_url  = f"{base_url}{sep}v={reprocess_count}"
        return jsonify({
            "ok":              True,
            "method":          method,
            "used_cloudinary": used_cloudinary,
            "azure_url":       azure_url,
            "new_url":         new_url,          # versioned CF URL for card refresh
            "reprocess_count": reprocess_count,  # ?v=N for CF cache busting
        })

    except Exception as exc:
        logger.exception(f"Reprocess failed  sku={sku_id}  file={filename}")
        # Save failure audit row (best-effort)
        try:
            db3  = get_db()
            cur3 = db3.cursor()
            try:
                cur3.execute("""
                    INSERT INTO image_results
                        (run_id, sku_id, blob_name, filename, method,
                         status, error_code, error_msg, processed_at)
                    VALUES (%s, %s, %s, %s, %s, 'failed', %s, %s, %s)
                """, (
                    "manual-reprocess", sku_id, f"lifestyle/{filename}", filename,
                    f"reprocess_{method}", type(exc).__name__, str(exc)[:2000],
                    datetime.now(),
                ))
            finally:
                cur3.close()
                db3.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── Legacy redirect ───────────────────────────────────────────

@app.route("/sku/<path:sku_id>")
def sku_detail_redirect(sku_id: str):
    return redirect(url_for("index", dq=sku_id))


@app.route("/search")
def search_redirect():
    q = request.args.get("q", "").strip()
    return redirect(url_for("index", bq=q))


@app.route("/failed")
def failed_redirect():
    return redirect(url_for("index"))


# ── Run ───────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n  🚀  Pipeline UI  →  http://localhost:5000\n")
    app.run(debug=True, port=5000, host="0.0.0.0")
