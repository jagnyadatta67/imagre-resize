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

import io
import json
import logging
import os
import sys
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import urlparse

import mysql.connector
from mysql.connector import pooling as _mysql_pooling
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
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
from modules import db as pipeline_db                            # noqa: E402
from modules.pipeline_runner import run_pipeline as _run_pipeline  # noqa: E402

_azure = AzureClient()   # singleton — thread-safe per Azure SDK docs

# Active web-triggered pipeline runs: run_id → {category, pad_mode, started_at}
_active_pipeline_runs: dict = {}
_active_pipeline_lock = threading.Lock()

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB       = os.getenv("MYSQL_DB", "image_pipeline")
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

AZURE_ACCOUNT_NAME = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY  = os.getenv("AZURE_ACCOUNT_KEY", "")
TARGET_CONTAINER        = os.getenv("TARGET_CONTAINER",       "lifestyle-converted")
SOURCE_BLOB_PREFIX      = os.getenv("SOURCE_BLOB_PREFIX",     "lifestyle/")
BABYSHOP_TARGET_FOLDER  = os.getenv("BABYSHOP_TARGET_FOLDER", "babyshopstores-new")
BABYSHOP_SOURCE_PREFIX  = os.getenv("BABYSHOP_SOURCE_PREFIX", "babyshopstores/")
# UPLOAD_USERS kept as a no-op reference — no longer used for validation (login handles auth)
UPLOAD_USERS: list[str] = []

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
app.secret_key = os.getenv("FLASK_SECRET_KEY", "pipeline-ui-secret-CHANGE-ME")
app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024  # 20 MB max upload

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


# ── UI auth schema + helpers ──────────────────────────────────

def _ensure_ui_schema() -> None:
    """Create UI tables if they don't exist (safe to call every startup)."""
    db  = get_db()
    cur = db.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ui_users (
                id            INT AUTO_INCREMENT PRIMARY KEY,
                username      VARCHAR(100) NOT NULL UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                is_active     TINYINT(1)   NOT NULL DEFAULT 1,
                created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS broken_image_health (
                id           INT AUTO_INCREMENT PRIMARY KEY,
                sku_id       VARCHAR(300) NOT NULL,
                category     VARCHAR(100),
                broken_url   TEXT         NOT NULL,
                checked_at   DATETIME     DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_bih_sku      (sku_id),
                INDEX idx_bih_category (category)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sku_comments (
                id         INT AUTO_INCREMENT PRIMARY KEY,
                sku_id     VARCHAR(255) NOT NULL,
                filename   VARCHAR(255) NOT NULL,
                comment    TEXT         NOT NULL,
                username   VARCHAR(100) DEFAULT NULL,
                created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_sku_file (sku_id, filename)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        db.commit()
    finally:
        cur.close()
        db.close()


_ensure_ui_schema()   # runs once at import / startup time


# ── Presence store ────────────────────────────────────────────
# Tracks which users are currently viewing which SKU detail page.
# Structure: { sku_id: { username: last_seen_datetime } }
# No DB needed — in-memory, resets on server restart.
# Timeout: 2 minutes (PRESENCE_TTL_SECONDS)

PRESENCE_TTL_SECONDS = 1200  # 20 minutes
_presence: dict[str, dict[str, datetime]] = {}
_presence_lock = threading.Lock()


def _presence_update(sku_id: str, username: str) -> None:
    """Register/refresh a user as actively viewing a SKU."""
    with _presence_lock:
        if sku_id not in _presence:
            _presence[sku_id] = {}
        _presence[sku_id][username] = datetime.utcnow()


def _presence_get(sku_id: str, exclude_user: str | None = None) -> list[str]:
    """
    Return list of usernames actively viewing a SKU (within TTL).
    Optionally exclude the requesting user (so you don't see yourself).
    Also evicts expired entries.
    """
    now     = datetime.utcnow()
    cutoff  = timedelta(seconds=PRESENCE_TTL_SECONDS)
    result  = []
    with _presence_lock:
        viewers = _presence.get(sku_id, {})
        expired = [u for u, ts in viewers.items() if (now - ts) > cutoff]
        for u in expired:
            del viewers[u]
        for u, ts in viewers.items():
            if u != exclude_user:
                result.append(u)
    return result


def _current_user() -> str | None:
    """Return the logged-in username from the Flask session, or None."""
    return session.get("username")


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

    Lifestyle  : {container}/lifestyle-newc/{filename}    → {container}/lifestyle/{filename}
    Baby Shop  : {container}/babyshopstores-new/{filename} → {container}/babyshopstores/{filename}
    """
    if not processed_azure_url:
        return ""
    if f"/{TARGET_CONTAINER}/" in processed_azure_url:
        return make_sas_url(processed_azure_url.replace(f"/{TARGET_CONTAINER}/", f"/{SOURCE_BLOB_PREFIX}"))
    if f"/{BABYSHOP_TARGET_FOLDER}/" in processed_azure_url:
        return make_sas_url(processed_azure_url.replace(f"/{BABYSHOP_TARGET_FOLDER}/", f"/{BABYSHOP_SOURCE_PREFIX}"))
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
    Lifestyle  : swap /{TARGET_CONTAINER}/       → /{SOURCE_BLOB_PREFIX}
    Baby Shop  : swap /{BABYSHOP_TARGET_FOLDER}/ → /{BABYSHOP_SOURCE_PREFIX}
    Returns "" when container not mapped or no known target folder found.
    """
    if not processed_azure_url:
        return ""
    if f"/{TARGET_CONTAINER}/" in processed_azure_url:
        return make_cf_url(processed_azure_url.replace(f"/{TARGET_CONTAINER}/", f"/{SOURCE_BLOB_PREFIX}"))
    if f"/{BABYSHOP_TARGET_FOLDER}/" in processed_azure_url:
        return make_cf_url(processed_azure_url.replace(f"/{BABYSHOP_TARGET_FOLDER}/", f"/{BABYSHOP_SOURCE_PREFIX}"))
    return ""


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

def _get_category_groups(cur, brand: str = "lifestyle") -> list[dict]:
    """
    Return L1 group pills: aggregate sku_results by the prefix before the first '-'.
    e.g. 'beauty-face' → group 'beauty'.
    Ordered by a fixed display order (defined in _GROUP_META), unknowns go last.
    brand='lifestyle' → exclude babyshop brand rows
    brand='babyshop'  → only babyshop brand rows
    """
    if brand == "babyshop":
        brand_clause = "AND brand = 'babyshop'"
    else:
        brand_clause = "AND (brand IS NULL OR brand != 'babyshop')"

    cur.execute(f"""
        SELECT
            LOWER(SUBSTRING_INDEX(TRIM(COALESCE(category,'')), '-', 1)) AS grp,
            COUNT(*)                                                      AS total,
            SUM(status IN ('done','skipped'))                             AS passed,
            SUM(status = 'failed')                                        AS failed
        FROM  sku_results
        WHERE category IS NOT NULL AND category != ''
          {brand_clause}
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
        WHERE (brand IS NULL OR brand != 'babyshop')
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

    # ── Fetch QC status for all returned SKUs in one query ────────────────
    sku_ids   = [s["sku_id"] for s in skus]
    qc_map: dict[str, dict] = {}
    if sku_ids:
        fmt = ",".join(["%s"] * len(sku_ids))
        cur.execute(f"""
            SELECT sku_id, username, updated_at
            FROM   sku_comments
            WHERE  filename = '__QC__' AND sku_id IN ({fmt})
        """, sku_ids)
        for r in cur.fetchall():
            qc_map[r["sku_id"]] = {"qc_by": r["username"], "qc_at": r["updated_at"]}

    results = []
    for sku in skus:
        sku["qc_by"] = (qc_map.get(sku["sku_id"]) or {}).get("qc_by")
        sku["qc_at"] = (qc_map.get(sku["sku_id"]) or {}).get("qc_at")
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

@app.route("/login", methods=["GET", "POST"])
def login():
    """
    Login page — only required to access ?re=1 (upload/reprocess view).
    All other pages remain open.

    GET  /login?next=<url>  → render login form
    POST /login             → verify credentials → redirect to next
    """
    next_url = (request.args.get("next") or request.form.get("next") or "/").strip()
    # Prevent open-redirect: next must be a relative path on this server
    if not next_url.startswith("/"):
        next_url = "/"
    error = None

    if request.method == "POST":
        username = (request.form.get("username") or "").strip().lower()
        password =  request.form.get("password") or ""

        db  = get_db()
        cur = db.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT password_hash FROM ui_users WHERE username = %s AND is_active = 1",
                (username,)
            )
            row = cur.fetchone()
        finally:
            cur.close()
            db.close()

        if row and check_password_hash(row["password_hash"], password):
            session["username"] = username
            session.permanent   = True   # default 31-day lifetime
            return redirect(next_url)
        error = "Invalid username or password."

    return render_template("login.html", next=next_url, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/BabyShop")
@app.route("/babyshop")
def babyshop_page():
    """Dedicated Baby Shop browse + search page."""
    if request.args.get("re") == "1" and not _current_user():
        return redirect(url_for("login", next=request.url))

    search   = request.args.get("q", "").strip()
    category = request.args.get("category", "").strip()
    status   = request.args.get("status", "").strip()
    page     = max(1, int(request.args.get("page", "1")))
    per_page = 48
    offset   = (page - 1) * per_page

    # Category pills
    cats = pipeline_db.get_brand_categories("babyshop")

    # SKU results (when searching or filtering by category)
    results = []
    total   = 0
    if search or category:
        results, total = pipeline_db.get_skus_by_brand(
            "babyshop",
            category = category or None,
            status   = status   or None,
            search   = search   or None,
            limit    = per_page,
            offset   = offset,
        )
        for r in results:
            if r.get("last_processed_at"):
                r["last_processed_at"] = r["last_processed_at"].strftime("%Y-%m-%d %H:%M:%S")

    total_pages = max(1, -(-total // per_page))  # ceil division

    return render_template(
        "babyshop.html",
        cats          = cats,
        results       = results,
        total         = total,
        total_pages   = total_pages,
        page          = page,
        search        = search,
        category      = category,
        status        = status,
        show_reprocess= bool(_current_user()),
    )


@app.route("/")
def index():
    bq = request.args.get("bq", "").strip()   # business search query
    dq = request.args.get("dq", "").strip()   # developer SKU search query
    # ── Auth guard: ?re=1 link triggers login for unauthenticated users ──
    # Once logged in, buttons appear automatically on every search page.
    if request.args.get("re") == "1" and not _current_user():
        return redirect(url_for("login", next=request.url))

    # Reprocess + Upload buttons auto-appear for any logged-in user
    show_reprocess  = bool(_current_user())
    show_upload_all = bool(_current_user())

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        stats            = _get_stats(cur)
        recent_runs      = _get_recent_runs(cur)
        failed_skus      = _get_failed_skus(cur)
        category_groups    = _get_category_groups(cur, "lifestyle")
        bs_category_groups = _get_category_groups(cur, "babyshop")

        business_results    = _build_business_results(cur, bq) if bq else []
        dev_sku, dev_images = _build_dev_sku(cur, dq) if dq else (None, [])

        # ── Baby Shop redirect banner ─────────────────────────
        # If bq search returns no Lifestyle results, check if it's a Baby Shop SKU
        bs_redirect_sku = None
        if bq and not business_results:
            cur.execute("""
                SELECT sku_id FROM sku_results
                WHERE  sku_id LIKE %s AND brand = 'babyshop'
                LIMIT  1
            """, (f"%{bq}%",))
            bs_row = cur.fetchone()
            if bs_row:
                bs_redirect_sku = bs_row["sku_id"]
    finally:
        cur.close()
        db.close()   # returns connection to pool

    return render_template(
        "index.html",
        stats            = stats,
        recent_runs      = recent_runs,
        failed_skus      = failed_skus,
        category_groups    = category_groups,
        bs_category_groups = bs_category_groups,
        business_results   = business_results,
        bq               = bq,
        dev_sku          = dev_sku,
        dev_images       = dev_images,
        dq               = dq,
        show_reprocess   = show_reprocess,
        show_upload_all  = show_upload_all,
        upload_users     = [],            # kept for template compat — dropdown removed
        bs_redirect_sku  = bs_redirect_sku,
    )


@app.route("/api/process-skuids/fetch", methods=["POST"])
def api_process_skuids_fetch():
    """
    Fetch SKU metadata from Unbxd for a list of SKU IDs.
    Body: {sku_ids: ["id1", "id2", ...]}
    Returns: {found: [...], not_found: [...]}
    """
    if not _current_user():
        return jsonify({"error": "Login required"}), 401

    data    = request.get_json() or {}
    raw_ids = data.get("sku_ids") or []
    sku_ids = [s.strip() for s in raw_ids if str(s).strip()]

    if not sku_ids:
        return jsonify({"error": "No SKU IDs provided"}), 400
    if len(sku_ids) > 1000:
        return jsonify({"error": "Max 1000 SKUs per batch"}), 400

    from modules.unbxd_client import fetch_skus_from_unbxd
    result = fetch_skus_from_unbxd(sku_ids, workers=20)
    return jsonify(result)


@app.route("/api/process-skuids/start", methods=["POST"])
def api_process_skuids_start():
    """
    Start pipeline for a confirmed SKU list (after Unbxd fetch preview).
    Body: {skus: [{sku_id, container, category, pad_mode}, ...], workers}
    Returns: {run_id, total}
    """
    if not _current_user():
        return jsonify({"error": "Login required"}), 401

    data    = request.get_json() or {}
    skus    = data.get("skus", [])
    workers = max(1, min(int(data.get("workers", 10)), 50))

    if not skus:
        return jsonify({"error": "No SKUs provided"}), 400

    sku_list = [
        {
            "sku_id":       s["sku_id"],
            "container":    s.get("container", ""),
            "category":     s.get("category", ""),
            "pad_mode":     s.get("pad_mode", "auto"),
            "reprocess":    False,
            "source_blobs": s.get("source_blobs"),  # exact blob paths from Unbxd
        }
        for s in skus
    ]

    import uuid as _uuid
    run_id = str(_uuid.uuid4())

    with _active_pipeline_lock:
        _active_pipeline_runs[run_id] = {
            "category":   "sku-list",
            "pad_mode":   "mixed (auto-detected)",
            "workers":    workers,
            "total":      len(sku_list),
            "started_at": datetime.now().isoformat(),
            "status":     "running",
        }

    def _bg_run():
        try:
            _run_pipeline(
                sku_list = sku_list,
                run_id   = run_id,
                pad_mode = "auto",   # fallback; each task overrides via pad_mode column
                workers  = workers,
                source   = "web/sku-list",
            )
        except Exception as exc:
            import logging as _lg
            _lg.getLogger("web.pipeline").error(f"SKU list pipeline error: {exc}")
        finally:
            with _active_pipeline_lock:
                if run_id in _active_pipeline_runs:
                    _active_pipeline_runs[run_id]["status"] = "done"

    import threading as _threading
    t = _threading.Thread(target=_bg_run, name=f"skulist-{run_id[:8]}", daemon=True)
    t.start()

    return jsonify({"run_id": run_id, "total": len(sku_list)})


@app.route("/api/process-missing/categories")
def api_process_missing_categories():
    """Return pending transformation categories with SKU counts."""
    try:
        cats = pipeline_db.get_pending_transformation_categories()
        stats = pipeline_db.get_transformation_run_stats()
        return jsonify({"categories": cats, "stats": stats})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/process-missing/start", methods=["POST"])
def api_process_missing_start():
    """
    Trigger pipeline for pending SKUs in one or more categories.
    Body JSON: {categories: null|[str,...], pad_mode, workers}
      - categories=null  → process ALL pending categories
      - categories=[...] → process only those categories
    Returns: {run_id, total, pad_mode}
    """
    if not _current_user():
        return jsonify({"error": "Login required"}), 401

    data       = request.get_json() or {}
    categories = data.get("categories")   # None = all, list = specific
    pad_mode   = data.get("pad_mode", "auto")
    workers    = max(1, min(int(data.get("workers", 10)), 50))

    if pad_mode not in ("auto", "white", "gen_fill", "no"):
        pad_mode = "auto"

    # Normalise: empty list → treat as all
    if isinstance(categories, list) and len(categories) == 0:
        categories = None

    # Fetch pending SKUs — if multiple categories, query each and merge
    if categories is None:
        sku_rows = pipeline_db.get_pending_transformation_skus(None)
        cat_label = "all"
    else:
        sku_rows = []
        for cat in categories:
            sku_rows.extend(pipeline_db.get_pending_transformation_skus(cat))
        cat_label = ", ".join(categories)

    if not sku_rows:
        return jsonify({"error": "No pending SKUs for this selection"}), 400

    # Deduplicate by sku_id (safety in case categories overlap)
    seen = set()
    deduped = []
    for r in sku_rows:
        if r["sku_id"] not in seen:
            seen.add(r["sku_id"])
            deduped.append(r)
    sku_rows = deduped

    # Build sku_list for pipeline_runner
    sku_list = [
        {"sku_id": r["sku_id"], "container": r["container"],
         "category": r["category"], "reprocess": False}
        for r in sku_rows
    ]

    import uuid as _uuid
    run_id = str(_uuid.uuid4())

    # Mark as processing so they don't show as pending while running
    pipeline_db.mark_transformations_processing([r["sku_id"] for r in sku_rows])

    # Track in active runs
    with _active_pipeline_lock:
        _active_pipeline_runs[run_id] = {
            "category":   cat_label,
            "pad_mode":   pad_mode,
            "workers":    workers,
            "total":      len(sku_list),
            "started_at": datetime.now().isoformat(),
            "status":     "running",
        }

    def _bg_run():
        try:
            _run_pipeline(
                sku_list = sku_list,
                run_id   = run_id,
                pad_mode = pad_mode,
                workers  = workers,
                source   = f"web/{cat_label}",
            )
            # Mark done/reset failed back to pending
            pipeline_db.mark_transformations_done_by_run(run_id)
        except Exception as exc:
            import logging as _lg
            _lg.getLogger("web.pipeline").error(f"Background pipeline error: {exc}")
            # Reset processing → pending on error
            pipeline_db.mark_transformations_done_by_run(run_id)
        finally:
            with _active_pipeline_lock:
                if run_id in _active_pipeline_runs:
                    _active_pipeline_runs[run_id]["status"] = "done"

    import threading as _threading
    t = _threading.Thread(target=_bg_run, name=f"pipeline-{run_id[:8]}", daemon=True)
    t.start()

    return jsonify({"run_id": run_id, "total": len(sku_list), "pad_mode": pad_mode})


@app.route("/api/process-missing/status/<run_id>")
def api_process_missing_status(run_id: str):
    """Poll pipeline run progress. Returns queue stats + is_complete flag."""
    db_conn = get_db()
    cur     = db_conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT status, COUNT(*) AS cnt
            FROM   sku_queue
            WHERE  run_id = %s
            GROUP  BY status
        """, (run_id,))
        queue_rows = cur.fetchall()

        cur.execute(
            "SELECT status, done_count, failed_count, skipped_count, total_skus FROM sku_runs WHERE run_id = %s",
            (run_id,)
        )
        run_row = cur.fetchone()
    finally:
        cur.close()
        db_conn.close()

    stats = {r["status"]: r["cnt"] for r in queue_rows}
    is_complete = run_row and run_row["status"] in ("done", "done_with_errors") if run_row else False

    with _active_pipeline_lock:
        meta = _active_pipeline_runs.get(run_id, {})

    return jsonify({
        "run_id":     run_id,
        "pending":    stats.get("pending",  0),
        "running":    stats.get("running",  0),
        "done":       stats.get("done",     0),
        "failed":     stats.get("failed",   0),
        "skipped":    stats.get("skipped",  0),
        "total":      run_row["total_skus"] if run_row else 0,
        "is_complete": bool(is_complete),
        "meta":        meta,
    })


@app.route("/api/broken-images")
def api_broken_images():
    """
    AJAX endpoint — returns paginated broken image health rows as JSON.
    Query params: category, search, limit, offset
    """
    category = request.args.get("category", "").strip() or None
    search   = request.args.get("search",   "").strip() or None
    limit    = min(int(request.args.get("limit",  "200")), 1000)
    offset   = int(request.args.get("offset", "0"))

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        where, params = [], []
        if category:
            where.append("category = %s"); params.append(category)
        if search:
            where.append("sku_id LIKE %s"); params.append(f"%{search}%")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        cur.execute(f"SELECT COUNT(*) AS cnt FROM broken_image_health {where_sql}", params)
        total = cur.fetchone()["cnt"]

        cur.execute(
            f"SELECT sku_id, category, broken_url, checked_at "
            f"FROM   broken_image_health {where_sql} "
            f"ORDER  BY category, sku_id "
            f"LIMIT %s OFFSET %s",
            params + [limit, offset]
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        db.close()

    for r in rows:
        if r.get("checked_at"):
            r["checked_at"] = r["checked_at"].strftime("%Y-%m-%d %H:%M:%S")
    return jsonify({"total": total, "rows": rows})


@app.route("/api/broken-images/categories")
def api_broken_image_categories():
    """Return distinct categories + counts for the broken images filter bar."""
    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT category, COUNT(*) AS cnt
            FROM   broken_image_health
            GROUP  BY category
            ORDER  BY cnt DESC
        """)
        rows = cur.fetchall()
    finally:
        cur.close()
        db.close()
    return jsonify(rows)


# ============================================================
# BABY SHOP ROUTES
# ============================================================

@app.route("/api/babyshop/categories")
def api_babyshop_categories():
    """Return Baby Shop categories with done/total counts."""
    try:
        rows = pipeline_db.get_brand_categories("babyshop")
        for r in rows:
            if r.get("done_count") is not None:
                r["done_count"] = int(r["done_count"])
            if r.get("total_count") is not None:
                r["total_count"] = int(r["total_count"])
        return jsonify(rows)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/babyshop/skus")
def api_babyshop_skus():
    """
    Paginated SKU list for Baby Shop browse.
    Query params: category, status (done|failed), search, limit, offset
    """
    category = request.args.get("category", "").strip() or None
    status   = request.args.get("status",   "").strip() or None
    search   = request.args.get("search",   "").strip() or None
    limit    = min(int(request.args.get("limit",  "50")), 200)
    offset   = int(request.args.get("offset", "0"))

    try:
        rows, total = pipeline_db.get_skus_by_brand(
            "babyshop", category=category, status=status,
            search=search, limit=limit, offset=offset,
        )
        for r in rows:
            if r.get("last_processed_at"):
                r["last_processed_at"] = r["last_processed_at"].strftime("%Y-%m-%d %H:%M:%S")
        return jsonify({"total": total, "rows": rows})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/babyshop/fetch", methods=["POST"])
def api_babyshop_fetch():
    """
    Fetch Baby Shop SKU metadata from Babyshop Unbxd.
    Body: {sku_ids: ["id1", "id2", ...]}
    """
    if not _current_user():
        return jsonify({"error": "Login required"}), 401

    data    = request.get_json() or {}
    raw_ids = data.get("sku_ids") or []
    sku_ids = [s.strip() for s in raw_ids if str(s).strip()]

    if not sku_ids:
        return jsonify({"error": "No SKU IDs provided"}), 400
    if len(sku_ids) > 1000:
        return jsonify({"error": "Max 1000 SKUs per batch"}), 400

    from modules.unbxd_client import fetch_skus_from_babyshop_unbxd
    result = fetch_skus_from_babyshop_unbxd(sku_ids, workers=20)
    return jsonify(result)


@app.route("/api/babyshop/start", methods=["POST"])
def api_babyshop_start():
    """
    Start Baby Shop pipeline for a confirmed SKU list (after Unbxd fetch preview).
    Body: {skus: [{sku_id, container, category, pad_mode, source_blobs}, ...], workers}
    brand="babyshop" is injected server-side; reprocess=False always.
    """
    if not _current_user():
        return jsonify({"error": "Login required"}), 401

    data    = request.get_json() or {}
    skus    = data.get("skus", [])
    workers = max(1, min(int(data.get("workers", 10)), 50))

    if not skus:
        return jsonify({"error": "No SKUs provided"}), 400

    sku_list = [
        {
            "sku_id":       s["sku_id"],
            "container":    s.get("container", ""),
            "category":     s.get("category", ""),
            "pad_mode":     s.get("pad_mode", "auto"),
            "reprocess":    False,   # no reprocess for Baby Shop
            "source_blobs": s.get("source_blobs"),
            "brand":        "babyshop",
        }
        for s in skus
    ]

    import uuid as _uuid
    run_id = str(_uuid.uuid4())

    with _active_pipeline_lock:
        _active_pipeline_runs[run_id] = {
            "category":   "babyshop",
            "pad_mode":   "mixed (auto-detected)",
            "workers":    workers,
            "total":      len(sku_list),
            "started_at": datetime.now().isoformat(),
            "status":     "running",
        }

    def _bg_run():
        try:
            _run_pipeline(
                sku_list = sku_list,
                run_id   = run_id,
                pad_mode = "auto",
                workers  = workers,
                source   = "web/babyshop",
            )
        except Exception as exc:
            import logging as _lg
            _lg.getLogger("web.babyshop").error(f"Baby Shop pipeline error: {exc}")
        finally:
            with _active_pipeline_lock:
                if run_id in _active_pipeline_runs:
                    _active_pipeline_runs[run_id]["status"] = "done"

    import threading as _threading
    t = _threading.Thread(target=_bg_run, name=f"babyshop-{run_id[:8]}", daemon=True)
    t.start()

    return jsonify({"run_id": run_id, "total": len(sku_list)})


@app.route("/api/babyshop/status/<run_id>")
def api_babyshop_status(run_id: str):
    """Poll Baby Shop pipeline progress — reuses same logic as process-missing status."""
    return api_process_missing_status(run_id)


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
        category_groups    = _get_category_groups(cur, "lifestyle")
        bs_category_groups = _get_category_groups(cur, "babyshop")

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
                categories         = categories,
                category           = None,
                group              = None,
                category_groups    = category_groups,
                bs_category_groups = bs_category_groups,
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
                categories         = categories,
                category           = None,
                group              = group,
                group_label        = group_label,
                category_groups    = category_groups,
                bs_category_groups = bs_category_groups,
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
                      AND  ir2.reprocess_count > 0)                 AS reprocessed_count,
                   (SELECT username FROM sku_comments
                    WHERE  sku_id = s.sku_id AND filename = '__QC__'
                    LIMIT  1)                                        AS qc_by
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
        status             = status,
        q                  = q,
        category_groups    = category_groups,
        bs_category_groups = bs_category_groups,
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


# ── Presence API ──────────────────────────────────────────────

@app.route("/api/presence/heartbeat", methods=["POST"])
def api_presence_heartbeat():
    """
    Called by JS every 30s while a user is viewing a SKU detail page.
    Body: { "sku_id": "..." }
    Requires login.
    """
    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401

    data   = request.get_json(silent=True) or {}
    sku_id = (data.get("sku_id") or "").strip()
    if not sku_id:
        return jsonify({"ok": False, "error": "missing sku_id"}), 400

    _presence_update(sku_id, user)

    # Return other active viewers so the UI can update immediately after heartbeat
    others = _presence_get(sku_id, exclude_user=user)
    return jsonify({"ok": True, "viewers": others})


@app.route("/api/presence/<path:sku_id>")
def api_presence_get(sku_id: str):
    """
    Poll endpoint — returns list of active viewers for a SKU (excluding self).
    Called every 15s by the SKU detail page JS.
    """
    user   = _current_user()
    others = _presence_get(sku_id, exclude_user=user)
    return jsonify({"viewers": others})


# ── SKU Image Comments ────────────────────────────────────────

@app.route("/api/qc", methods=["POST"])
def api_qc():
    """
    Mark or undo QC Done for a SKU.
    Body: { "sku_id": "...", "action": "done" | "undo" }
    Requires login.
    """
    user = _current_user()
    if not user:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401

    data   = request.get_json(silent=True) or {}
    sku_id = (data.get("sku_id") or "").strip()
    action = (data.get("action") or "").strip()

    if not sku_id or action not in ("done", "undo"):
        return jsonify({"ok": False, "error": "missing sku_id or invalid action"}), 400

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        if action == "done":
            cur.execute("""
                INSERT INTO sku_comments (sku_id, filename, comment, username)
                VALUES (%s, '__QC__', 'QC Done', %s)
                ON DUPLICATE KEY UPDATE
                    username   = VALUES(username),
                    comment    = 'QC Done',
                    updated_at = CURRENT_TIMESTAMP
            """, (sku_id, user))
            db.commit()
            # Return the saved timestamp
            cur.execute(
                "SELECT updated_at FROM sku_comments WHERE sku_id = %s AND filename = '__QC__'",
                (sku_id,)
            )
            row    = cur.fetchone()
            qc_at  = str(row["updated_at"]) if row else ""
            return jsonify({"ok": True, "action": "done", "qc_by": user, "qc_at": qc_at})
        else:
            cur.execute(
                "DELETE FROM sku_comments WHERE sku_id = %s AND filename = '__QC__'",
                (sku_id,)
            )
            db.commit()
            return jsonify({"ok": True, "action": "undo"})
    finally:
        cur.close()
        db.close()


@app.route("/api/comment", methods=["POST"])
def api_save_comment():
    """
    Save or update a comment for a specific image within a SKU.
    Body: { sku_id, filename, comment }
    No login required — anonymous comments allowed (username = null).
    """
    data     = request.get_json(silent=True) or {}
    sku_id   = (data.get("sku_id")   or "").strip()
    filename = (data.get("filename") or "").strip()
    comment  = (data.get("comment")  or "").strip()

    if not sku_id or not filename:
        return jsonify({"ok": False, "error": "missing sku_id or filename"}), 400

    username = _current_user()   # None if not logged in

    db  = get_db()
    cur = db.cursor()
    try:
        if comment:
            cur.execute("""
                INSERT INTO sku_comments (sku_id, filename, comment, username)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    comment    = VALUES(comment),
                    username   = VALUES(username),
                    updated_at = CURRENT_TIMESTAMP
            """, (sku_id, filename, comment, username))
        else:
            # Empty comment = delete the row
            cur.execute(
                "DELETE FROM sku_comments WHERE sku_id = %s AND filename = %s",
                (sku_id, filename)
            )
        db.commit()
    finally:
        cur.close()
        db.close()

    return jsonify({"ok": True})


@app.route("/api/comments/<path:sku_id>")
def api_get_comments(sku_id: str):
    """
    Return all saved comments for a SKU as a dict: { filename: comment }.
    Called on page load to pre-fill comment boxes.
    """
    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT filename, comment FROM sku_comments WHERE sku_id = %s",
            (sku_id,)
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        db.close()

    return jsonify({r["filename"]: r["comment"] for r in rows})


@app.route("/export/comments")
def export_comments():
    """
    Download all SKU image comments as a CSV file.
    Columns: sku_id, filename, comment, username, updated_at
    """
    import csv
    import io
    from flask import Response

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT sku_id, filename, comment, username, updated_at
            FROM sku_comments
            ORDER BY updated_at DESC
        """)
        rows = cur.fetchall()
    finally:
        cur.close()
        db.close()

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["sku_id", "filename", "comment", "username", "updated_at"])
    writer.writeheader()
    for row in rows:
        writer.writerow({
            "sku_id":     row["sku_id"],
            "filename":   row["filename"],
            "comment":    row["comment"],
            "username":   row["username"] or "",
            "updated_at": str(row["updated_at"]),
        })

    output = buf.getvalue()
    return Response(
        output,
        mimetype    = "text/csv",
        headers     = {"Content-Disposition": "attachment; filename=sku_comments.csv"},
    )


# ── QC Report ────────────────────────────────────────────────

@app.route("/api/qc-report")
def api_qc_report():
    """
    Returns per-user QC summary:
      [{ username, total, skus: [{sku_id, qc_at, category}] }]
    Requires login.
    """
    if not _current_user():
        return jsonify({"ok": False, "error": "not_logged_in"}), 401

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        # Per-user totals + last activity
        cur.execute("""
            SELECT
                username,
                COUNT(*)          AS total,
                MAX(updated_at)   AS last_at
            FROM sku_comments
            WHERE filename = '__QC__'
              AND username IS NOT NULL
            GROUP BY username
            ORDER BY total DESC
        """)
        summary_rows = cur.fetchall()

        users = []
        for r in summary_rows:
            uname = r["username"]
            # SKU list for this user with category hint from sku_results
            cur.execute("""
                SELECT sc.sku_id, sc.updated_at AS qc_at,
                       COALESCE(sr.category, '') AS category
                FROM sku_comments sc
                LEFT JOIN sku_results sr ON sr.sku_id = sc.sku_id
                WHERE sc.filename  = '__QC__'
                  AND sc.username  = %s
                ORDER BY sc.updated_at DESC
            """, (uname,))
            sku_rows = cur.fetchall()

            users.append({
                "username": uname,
                "total":    int(r["total"]),
                "last_at":  r["last_at"].strftime("%Y-%m-%d %H:%M") if r["last_at"] else "",
                "skus": [
                    {
                        "sku_id":   s["sku_id"],
                        "qc_at":    s["qc_at"].strftime("%Y-%m-%d %H:%M") if s["qc_at"] else "",
                        "category": s["category"],
                    }
                    for s in sku_rows
                ],
            })
    finally:
        cur.close()
        db.close()

    return jsonify({"ok": True, "users": users})


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
    if not _current_user():
        return jsonify({"ok": False, "error": "Authentication required"}), 401

    data     = request.get_json(force=True) or {}
    sku_id   = (data.get("sku_id")   or "").strip()
    filename = (data.get("filename") or "").strip()
    method   = (data.get("method")   or "auto").strip()

    if not sku_id or not filename:
        return jsonify({"ok": False, "error": "sku_id and filename are required"}), 400
    if method not in {"gen_fill", "auto", "fill", "center"}:
        return jsonify({"ok": False, "error": "method must be gen_fill, auto, fill, or center"}), 400

    # ── 1. Fetch container + brand from DB ───────────────────────
    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT container_name, brand FROM sku_results WHERE sku_id = %s",
            (sku_id,)
        )
        row = cur.fetchone()
    finally:
        cur.close()
        db.close()

    if not row or not row.get("container_name"):
        return jsonify({"ok": False, "error": f"No DB record for SKU '{sku_id}'"}), 404

    container_name = row["container_name"]
    brand          = row.get("brand") or ""
    logger         = logging.getLogger(f"reprocess.{sku_id[:30]}")

    # ── Brand-based source/target paths ──────────────────────────
    if brand == "babyshop":
        src_prefix    = BABYSHOP_SOURCE_PREFIX   # "babyshopstores/"
        target_folder = BABYSHOP_TARGET_FOLDER   # "babyshopstores-new"
    else:
        src_prefix    = SOURCE_BLOB_PREFIX       # "lifestyle/"
        target_folder = None                     # uses TARGET_CONTAINER default

    try:
        # ── 2. Download original ──────────────────────────────
        blob_name   = f"{src_prefix}{filename}"
        image_bytes = _azure.download_blob_bytes(container_name, blob_name)

        # ── 3. Reprocess (no Vision AI) ───────────────────────
        output_bytes, used_cloudinary, cloudinary_url, _vision_data, _transform_data = reprocess_single_image(
            image_bytes, filename, sku_id, method, logger
        )

        # ── 4. Upload result ──────────────────────────────────
        azure_url = _azure.upload_to_newc(filename, output_bytes, container_name, target_folder=target_folder)

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
                     reprocess_count, uploaded_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'done', %s, %s, %s)
            """, (
                "manual-reprocess", sku_id, blob_name, filename,
                f"reprocess_{method}", cloudinary_url, azure_url, datetime.now(),
                reprocess_count, _current_user(),
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
                         status, error_code, error_msg, processed_at, uploaded_by)
                    VALUES (%s, %s, %s, %s, %s, 'failed', %s, %s, %s, %s)
                """, (
                    "manual-reprocess", sku_id, blob_name, filename,
                    f"reprocess_{method}", type(exc).__name__, str(exc)[:2000],
                    datetime.now(), _current_user(),
                ))
            finally:
                cur3.close()
                db3.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── Per-image manual upload ───────────────────────────────────

@app.route("/api/upload-image", methods=["POST"])
def api_upload_image():
    """
    Upload a replacement image for a single processed file.

    POST multipart/form-data:
      sku_id, filename, file (binary)
      uploaded_by is taken from the Flask session (login required)

    Flow:
      1. Verify session login (401 if not authenticated)
      2. Fetch container_name from sku_results
      3. Upload raw bytes to {container}/{TARGET_CONTAINER}/{filename} (overwrite)
      4. Insert audit row into image_results (method='manual_upload', uploaded_by=…)
      5. Sync reprocess_count across all rows for this SKU
      6. Return { ok, new_url, reprocess_count }
    """
    if not _current_user():
        return jsonify({"ok": False, "error": "Authentication required"}), 401

    sku_id      = request.form.get("sku_id",   "").strip()
    filename    = request.form.get("filename", "").strip()
    uploaded_by = _current_user()   # always the logged-in session user
    file        = request.files.get("file")

    if not sku_id or not filename:
        return jsonify({"ok": False, "error": "sku_id and filename are required"}), 400
    if not file or not file.filename:
        return jsonify({"ok": False, "error": "No file provided"}), 400

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("SELECT container_name, brand FROM sku_results WHERE sku_id = %s", (sku_id,))
        row = cur.fetchone()
    finally:
        cur.close()
        db.close()

    if not row or not row.get("container_name"):
        return jsonify({"ok": False, "error": f"No DB record for SKU '{sku_id}'"}), 404

    container_name = row["container_name"]
    _brand         = row.get("brand") or ""
    _target_folder = BABYSHOP_TARGET_FOLDER if _brand == "babyshop" else None

    try:
        image_bytes = file.read()
        azure_url   = _azure.upload_to_newc(filename, image_bytes, container_name, target_folder=_target_folder)

        db2  = get_db()
        cur2 = db2.cursor(dictionary=True)
        try:
            cur2.execute("""
                SELECT COALESCE(MAX(reprocess_count), 0) AS max_count
                FROM   image_results WHERE sku_id = %s
            """, (sku_id,))
            reprocess_count = (cur2.fetchone()["max_count"] or 0) + 1

            _dest_folder = BABYSHOP_TARGET_FOLDER if _brand == "babyshop" else TARGET_CONTAINER
            cur2.execute("""
                INSERT INTO image_results
                    (run_id, sku_id, blob_name, filename, method,
                     azure_url, status, processed_at, reprocess_count, uploaded_by)
                VALUES (%s, %s, %s, %s, 'manual_upload', %s, 'done', %s, %s, %s)
            """, (
                "manual-upload", sku_id, f"{_dest_folder}/{filename}",
                filename, azure_url, datetime.now(), reprocess_count, uploaded_by,
            ))
            cur2.execute("""
                UPDATE image_results SET reprocess_count = %s WHERE sku_id = %s
            """, (reprocess_count, sku_id))
            db2.commit()
        finally:
            cur2.close()
            db2.close()

        base_url = make_cf_url(azure_url) or make_sas_url(azure_url)
        sep      = "&" if "?" in base_url else "?"
        new_url  = f"{base_url}{sep}v={reprocess_count}"
        return jsonify({
            "ok":              True,
            "filename":        filename,
            "new_url":         new_url,
            "reprocess_count": reprocess_count,
            "uploaded_by":     uploaded_by,
        })

    except Exception as exc:
        logging.getLogger("upload").exception("upload-image failed  sku=%s  file=%s", sku_id, filename)
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── SKU-level bulk upload (drag & drop → Azure) ──────────────

@app.route("/api/bulk-upload-images", methods=["POST"])
def api_bulk_upload_images():
    """
    Upload multiple replacement images for a SKU at once.

    POST multipart/form-data:
      sku_id         — the SKU being updated
      files          — one or more image files (JPG/PNG)

    For each file:
      - Filename must match an existing image_results row for this SKU (else skipped)
      - Uploads to {container}/{TARGET_CONTAINER}/{filename} (overwrite)
      - Updates image_results: status=done, uploaded_by, method=manual_upload
    After all uploads, syncs reprocess_count across all rows for the SKU.
    Returns: { ok, replaced:[{filename, new_url}], skipped:[filename], uploaded_by, reprocess_count }
    """
    if not _current_user():
        return jsonify({"ok": False, "error": "Authentication required"}), 401

    sku_id      = request.form.get("sku_id", "").strip()
    uploaded_by = _current_user()
    files       = request.files.getlist("files")

    if not sku_id:
        return jsonify({"ok": False, "error": "sku_id is required"}), 400
    if not files:
        return jsonify({"ok": False, "error": "No files provided"}), 400

    # ── DB: get container + known filenames for this SKU ──────
    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("SELECT container_name, brand FROM sku_results WHERE sku_id = %s LIMIT 1", (sku_id,))
        row = cur.fetchone()
        if not row or not row.get("container_name"):
            return jsonify({"ok": False, "error": f"SKU '{sku_id}' not found"}), 404
        container      = row["container_name"]
        _bulk_brand    = row.get("brand") or ""
        _bulk_target   = BABYSHOP_TARGET_FOLDER if _bulk_brand == "babyshop" else None

        cur.execute("""
            SELECT DISTINCT filename FROM image_results
            WHERE sku_id = %s AND filename IS NOT NULL AND filename != ''
        """, (sku_id,))
        known = {r["filename"] for r in cur.fetchall()}
    finally:
        cur.close()
        db.close()

    # ── Process each uploaded file ────────────────────────────
    replaced = []
    skipped  = []

    for f in files:
        fname = f.filename.strip() if f.filename else ""
        if not fname:
            continue

        # If image_results has no rows for this SKU yet (never processed),
        # allow any file. Otherwise enforce filename must match a known row.
        if known and fname not in known:
            skipped.append(fname)
            continue

        try:
            image_bytes = f.read()
            azure_url   = _azure.upload_to_newc(fname, image_bytes, container, target_folder=_bulk_target)

            # Update image_results row for this filename
            db2  = get_db()
            cur2 = db2.cursor(dictionary=True)
            try:
                cur2.execute("""
                    UPDATE image_results
                    SET    status      = 'done',
                           method      = 'manual_upload',
                           azure_url   = %s,
                           uploaded_by = %s,
                           processed_at = %s
                    WHERE  sku_id    = %s
                      AND  filename  = %s
                """, (azure_url, uploaded_by, datetime.now(), sku_id, fname))
                db2.commit()
            finally:
                cur2.close()
                db2.close()

            base_url = make_cf_url(azure_url) or make_sas_url(azure_url)
            replaced.append({"filename": fname, "azure_url": azure_url, "base_url": base_url})

        except Exception as exc:
            app.logger.warning(f"[bulk-upload] failed {fname}: {exc}")
            skipped.append(fname)

    if not replaced:
        return jsonify({
            "ok":      False,
            "error":   "No files matched known images for this SKU",
            "skipped": skipped,
        }), 400

    # ── Sync reprocess_count across all images in SKU ─────────
    db3  = get_db()
    cur3 = db3.cursor(dictionary=True)
    try:
        cur3.execute("""
            SELECT COALESCE(MAX(reprocess_count), 0) AS max_rc
            FROM image_results WHERE sku_id = %s
        """, (sku_id,))
        new_rc = (cur3.fetchone()["max_rc"] or 0) + 1
        cur3.execute("""
            UPDATE image_results SET reprocess_count = %s WHERE sku_id = %s
        """, (new_rc, sku_id))
        db3.commit()
    finally:
        cur3.close()
        db3.close()

    # Build new_url with cache-busting version param
    result_replaced = []
    for item in replaced:
        sep     = "&" if "?" in item["base_url"] else "?"
        new_url = f"{item['base_url']}{sep}v={new_rc}"
        result_replaced.append({"filename": item["filename"], "new_url": new_url})

    return jsonify({
        "ok":              True,
        "replaced":        result_replaced,
        "skipped":         skipped,
        "uploaded_by":     uploaded_by,
        "reprocess_count": new_rc,
    })


# ── SKU-level bulk download (original images → ZIP) ──────────

@app.route("/api/download-sku-originals/<path:sku_id>")
def api_download_sku_originals(sku_id: str):
    """
    Build a ZIP of all original images for a SKU in-memory (no disk I/O),
    downloading from Azure in parallel, then stream to browser.
    Requires login.
    """
    from flask import Response

    if not _current_user():
        return jsonify({"ok": False, "error": "not_logged_in"}), 401

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        # Container from sku_results
        cur.execute(
            "SELECT container_name FROM sku_results WHERE sku_id = %s LIMIT 1",
            (sku_id,)
        )
        row = cur.fetchone()
        if not row or not row["container_name"]:
            return jsonify({"ok": False, "error": "SKU not found"}), 404
        container = row["container_name"]

        # blob_name = full Azure path (e.g. lifestyle/SKU_01.jpg)
        # filename  = base name only  (e.g. SKU_01.jpg)
        cur.execute("""
            SELECT   blob_name, filename
            FROM     image_results
            WHERE    sku_id    = %s
              AND    blob_name IS NOT NULL
              AND    blob_name != ''
            GROUP BY blob_name, filename
            ORDER BY filename
        """, (sku_id,))
        images = cur.fetchall()
    finally:
        cur.close()
        db.close()

    if not images:
        return jsonify({"ok": False, "error": "No images found for this SKU"}), 404

    # ── Parallel download from Azure ─────────────────────────
    def _fetch(img):
        try:
            data = _azure.download_blob_bytes(container, img["blob_name"])
            app.logger.info(f"[dl-zip] {img['filename']}  {len(data)} bytes")
            return img["filename"], data
        except Exception as exc:
            app.logger.warning(f"[dl-zip] skip {img['blob_name']}: {exc}")
            return None, None

    results = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch, img): img for img in images}
        for fut in as_completed(futures):
            fname, data = fut.result()
            if fname and data:
                results[fname] = data

    # ── Build ZIP in memory ───────────────────────────────────
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fname in sorted(results):
            zf.writestr(fname, results[fname])
    buf.seek(0)

    safe_name = sku_id.replace("/", "_")
    return Response(
        buf.read(),
        mimetype = "application/zip",
        headers  = {
            "Content-Disposition": f'attachment; filename="{safe_name}.zip"',
        },
    )


# ── SKU-level bulk upload ─────────────────────────────────────

@app.route("/api/upload-sku-images", methods=["POST"])
def api_upload_sku_images():
    """
    Upload replacement images for multiple files in one SKU at once.

    POST multipart/form-data:
      sku_id, uploaded_by,
      file_{filename}  — one entry per image (key = "file_" + original filename)

    Each uploaded file replaces {container}/{TARGET_CONTAINER}/{filename} in Azure.
    All inserted rows share the same incremented reprocess_count.
    Returns { ok, results: [{filename, ok, new_url}], errors: [{filename, error}] }
    """
    if not _current_user():
        return jsonify({"ok": False, "error": "Authentication required"}), 401

    sku_id      = request.form.get("sku_id", "").strip()
    uploaded_by = _current_user()   # always the logged-in session user

    if not sku_id:
        return jsonify({"ok": False, "error": "sku_id is required"}), 400

    db  = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("SELECT container_name FROM sku_results WHERE sku_id = %s", (sku_id,))
        row = cur.fetchone()
    finally:
        cur.close()
        db.close()

    if not row or not row.get("container_name"):
        return jsonify({"ok": False, "error": f"No DB record for SKU '{sku_id}'"}), 404

    container_name = row["container_name"]

    # Compute next reprocess_count once — shared across all files in this batch
    db2  = get_db()
    cur2 = db2.cursor(dictionary=True)
    try:
        cur2.execute("""
            SELECT COALESCE(MAX(reprocess_count), 0) AS max_count
            FROM   image_results WHERE sku_id = %s
        """, (sku_id,))
        reprocess_count = (cur2.fetchone()["max_count"] or 0) + 1
    finally:
        cur2.close()
        db2.close()

    results = []
    errors  = []

    for key, file in request.files.items():
        # Keys sent by the JS are "file_{filename}"
        if not key.startswith("file_") or not file.filename:
            continue
        filename = key[len("file_"):]

        try:
            image_bytes = file.read()
            azure_url   = _azure.upload_to_newc(filename, image_bytes, container_name)

            db3  = get_db()
            cur3 = db3.cursor(dictionary=True)
            try:
                cur3.execute("""
                    INSERT INTO image_results
                        (run_id, sku_id, blob_name, filename, method,
                         azure_url, status, processed_at, reprocess_count, uploaded_by)
                    VALUES (%s, %s, %s, %s, 'manual_upload', %s, 'done', %s, %s, %s)
                """, (
                    "manual-upload", sku_id, f"{TARGET_CONTAINER}/{filename}",
                    filename, azure_url, datetime.now(), reprocess_count, uploaded_by,
                ))
                db3.commit()
            finally:
                cur3.close()
                db3.close()

            base_url = make_cf_url(azure_url) or make_sas_url(azure_url)
            sep      = "&" if "?" in base_url else "?"
            results.append({"filename": filename, "ok": True,
                            "new_url": f"{base_url}{sep}v={reprocess_count}"})

        except Exception as exc:
            logging.getLogger("upload").exception(
                "upload-sku-images failed  sku=%s  file=%s", sku_id, filename)
            errors.append({"filename": filename, "error": str(exc)})

    if not results and not errors:
        return jsonify({"ok": False, "error": "No files received"}), 400

    # Sync all existing rows for this SKU to the same reprocess_count
    if results:
        db4  = get_db()
        cur4 = db4.cursor(dictionary=True)
        try:
            cur4.execute("""
                UPDATE image_results SET reprocess_count = %s WHERE sku_id = %s
            """, (reprocess_count, sku_id))
            db4.commit()
        finally:
            cur4.close()
            db4.close()

    return jsonify({
        "ok":              len(errors) == 0,
        "results":         results,
        "errors":          errors,
        "reprocess_count": reprocess_count,
    })


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
