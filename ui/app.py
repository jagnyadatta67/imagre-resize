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
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

import mysql.connector
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for
from markupsafe import Markup

# ── Load .env from parent directory ──────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB       = os.getenv("MYSQL_DB", "image_pipeline")
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

AZURE_ACCOUNT_NAME = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY  = os.getenv("AZURE_ACCOUNT_KEY", "")

app = Flask(__name__)
app.secret_key = "pipeline-ui-secret"


# ── DB helper ─────────────────────────────────────────────────

def get_db():
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        database=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASSWORD,
    )


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
    if processed_azure_url and "/lifestyle-converted/" in processed_azure_url:
        original_url = processed_azure_url.replace("/lifestyle-converted/", "/lifestyle/")
        return make_sas_url(original_url)
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

def _get_stats(cur) -> dict:
    cur.execute("""
        SELECT
            COUNT(*)              AS total,
            SUM(status = 'done')  AS done,
            SUM(status = 'failed')AS failed,
            SUM(status = 'skipped') AS skipped,
            SUM(azure_uploaded)   AS total_images
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
        cur.execute("""
            SELECT filename, azure_url, status
            FROM   image_results
            WHERE  sku_id = %s
            ORDER  BY processed_at DESC
        """, (sku["sku_id"],))
        all_imgs = cur.fetchall()

        # Deduplicate: keep the latest row per filename
        seen   = set()
        images = []
        for img in all_imgs:
            fname = img["filename"]
            if fname and fname not in seen:
                seen.add(fname)
                processed_sas = make_sas_url(img["azure_url"]) if img["azure_url"] else ""
                original_sas  = make_original_sas_url(img["azure_url"]) if img["azure_url"] else ""
                images.append({
                    "filename":      fname,
                    "processed_url": processed_sas,
                    "original_url":  original_sas,
                    "has_original":  bool(original_sas),
                    "status":        img["status"],
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

    db  = get_db()
    cur = db.cursor(dictionary=True)

    stats       = _get_stats(cur)
    recent_runs = _get_recent_runs(cur)
    failed_skus = _get_failed_skus(cur)

    business_results             = _build_business_results(cur, bq) if bq else []
    dev_sku, dev_images          = _build_dev_sku(cur, dq) if dq else (None, [])

    cur.close()
    db.close()

    return render_template(
        "index.html",
        stats            = stats,
        recent_runs      = recent_runs,
        failed_skus      = failed_skus,
        business_results = business_results,
        bq               = bq,
        dev_sku          = dev_sku,
        dev_images       = dev_images,
        dq               = dq,
    )


@app.route("/api/run/<run_id>/skus")
def api_run_skus(run_id: str):
    """
    AJAX endpoint — returns JSON list of SKU queue rows for a given run.
    Used by the Developer → Runs tab expandable row.
    """
    db  = get_db()
    cur = db.cursor(dictionary=True)
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
    cur.close()
    db.close()

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
