"""
db.py — MySQL persistence layer for the bulk image pipeline.

Tables
------
sku_runs      : one row per CLI run  (summary counters + timestamps)
sku_queue     : one row per SKU per run  ← THE QUEUE  (pending/running/done/failed/skipped)
sku_results   : one canonical row per SKU across all runs  (upserted)
image_results : one row per image per run  (full audit trail)

Queue flow
----------
  INSERT all SKUs → status='pending'
  Workers loop:
      SELECT … FOR UPDATE SKIP LOCKED → status='running'   (atomic claim)
      Process SKU
      UPDATE → status='done'|'failed'|'skipped'

SKIP LOCKED requires MySQL 8.0+
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

import mysql.connector
from mysql.connector import pooling

from config import MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD

log = logging.getLogger(__name__)

_pool: Optional[pooling.MySQLConnectionPool] = None


def _get_pool() -> pooling.MySQLConnectionPool:
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name   = "pipeline",
            pool_size   = 20,          # enough for 10 workers + monitor queries
            host        = MYSQL_HOST,
            port        = MYSQL_PORT,
            database    = MYSQL_DB,
            user        = MYSQL_USER,
            password    = MYSQL_PASSWORD,
            autocommit  = False,
            charset     = "utf8mb4",
            collation   = "utf8mb4_unicode_ci",
        )
    return _pool


def _conn() -> mysql.connector.MySQLConnection:
    return _get_pool().get_connection()


# ============================================================
# SCHEMA INIT
# ============================================================

def init_db() -> None:
    conn = _conn()
    cur  = conn.cursor()

    # ── Run summary ───────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sku_runs (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            run_id        VARCHAR(36)  UNIQUE NOT NULL,
            csv_folder    VARCHAR(500),
            csv_files     TEXT,
            total_skus    INT          DEFAULT 0,
            done_count    INT          DEFAULT 0,
            failed_count  INT          DEFAULT 0,
            skipped_count INT          DEFAULT 0,
            status        VARCHAR(20)  DEFAULT 'running',
            started_at    DATETIME,
            finished_at   DATETIME,
            INDEX idx_status     (status),
            INDEX idx_started_at (started_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # ── Queue  (one row per SKU per run, drives worker pull loop) ─
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sku_queue (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            run_id      VARCHAR(36)  NOT NULL,
            sku_id      VARCHAR(200) NOT NULL,
            container   VARCHAR(100),
            reprocess   TINYINT(1)   DEFAULT 0,
            category    VARCHAR(100),
            status      VARCHAR(20)  DEFAULT 'pending',
            worker_id   VARCHAR(100),
            claimed_at  DATETIME,
            finished_at DATETIME,
            error_msg   TEXT,
            INDEX idx_queue_claim (run_id, status, id),
            INDEX idx_queue_sku   (sku_id),
            INDEX idx_queue_run   (run_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    # Add category column to sku_queue if upgrading existing DB
    try:
        cur.execute("ALTER TABLE sku_queue ADD COLUMN category VARCHAR(100)")
        conn.commit()
    except Exception as e:
        if "1060" in str(e) or "Duplicate column" in str(e):
            pass   # column already exists — safe to ignore
        else:
            raise

    # ── SKU results (canonical, upserted on every run) ────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sku_results (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            run_id              VARCHAR(36)  NOT NULL,
            sku_id              VARCHAR(200) NOT NULL,
            status              VARCHAR(20)  DEFAULT 'pending',
            category            VARCHAR(100),
            container_name      VARCHAR(100),
            container_source    VARCHAR(20),
            reprocess           TINYINT(1)   DEFAULT 0,
            blob_count          INT          DEFAULT 0,
            cloudinary_sent     INT          DEFAULT 0,
            cloudinary_skipped  INT          DEFAULT 0,
            azure_uploaded      INT          DEFAULT 0,
            cloudinary_urls     JSON,
            azure_urls          JSON,
            listing_azure_url   TEXT,
            error_code          VARCHAR(100),
            error_msg           TEXT,
            last_processed_at   DATETIME,
            reprocessed_at      DATETIME,
            UNIQUE KEY uq_sku_id  (sku_id),
            INDEX      idx_run_id (run_id),
            INDEX      idx_status (status),
            INDEX      idx_category (category)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    # Add category column to sku_results if upgrading existing DB
    try:
        cur.execute("ALTER TABLE sku_results ADD COLUMN category VARCHAR(100)")
        conn.commit()
    except Exception as e:
        if "1060" in str(e) or "Duplicate column" in str(e):
            pass   # column already exists — safe to ignore
        else:
            raise

    try:
        cur.execute("ALTER TABLE sku_results ADD INDEX idx_category (category)")
        conn.commit()
    except Exception as e:
        if "1061" in str(e) or "Duplicate key name" in str(e):
            pass   # index already exists — safe to ignore
        else:
            raise

    try:
        cur.execute("ALTER TABLE sku_results ADD COLUMN listing_azure_url TEXT")
        conn.commit()
    except Exception as e:
        if "1060" in str(e) or "Duplicate column" in str(e):
            pass
        else:
            raise

    # ── Per-image audit trail ─────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS image_results (
            id               INT AUTO_INCREMENT PRIMARY KEY,
            run_id           VARCHAR(36)  NOT NULL,
            sku_id           VARCHAR(200) NOT NULL,
            blob_name        VARCHAR(500),
            filename         VARCHAR(300),
            method           VARCHAR(20),
            cloudinary_url   TEXT,
            azure_url        TEXT,
            status           VARCHAR(20),
            error_code       VARCHAR(100),
            error_msg        TEXT,
            processed_at     DATETIME,
            reprocess_count  INT          DEFAULT 0,
            vision_data      VARCHAR(500),
            transform_data   VARCHAR(500),
            INDEX idx_sku_id (sku_id),
            INDEX idx_run_id (run_id),
            INDEX idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    # Migrations — safe to run on existing tables
    for col_sql in [
        "ALTER TABLE image_results ADD COLUMN reprocess_count INT DEFAULT 0",
        "ALTER TABLE image_results ADD COLUMN vision_data VARCHAR(500)",
        "ALTER TABLE image_results ADD COLUMN transform_data VARCHAR(500)",
        "ALTER TABLE image_results ADD COLUMN reprocess_transform_data VARCHAR(500)",
    ]:
        try:
            cur.execute(col_sql)
            conn.commit()
        except Exception as e:
            if "1060" in str(e) or "Duplicate column" in str(e):
                pass   # column already exists — safe to ignore
            else:
                raise

    conn.commit()
    cur.close()
    conn.close()
    log.info("Database schema ready")


# ============================================================
# RUN HELPERS
# ============================================================

def create_run(run_id: str, csv_folder: str, csv_files: list[str], total_skus: int) -> None:
    conn = _conn()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO sku_runs (run_id, csv_folder, csv_files, total_skus, status, started_at)
        VALUES (%s, %s, %s, %s, 'running', %s)
    """, (run_id, csv_folder, json.dumps(csv_files), total_skus, datetime.now()))
    conn.commit()
    cur.close()
    conn.close()


def finish_run(run_id: str, done: int, failed: int, skipped: int) -> None:
    conn   = _conn()
    cur    = conn.cursor()
    status = "done" if failed == 0 else "done_with_errors"
    cur.execute("""
        UPDATE sku_runs
        SET done_count=%s, failed_count=%s, skipped_count=%s,
            status=%s, finished_at=%s
        WHERE run_id=%s
    """, (done, failed, skipped, status, datetime.now(), run_id))
    conn.commit()
    cur.close()
    conn.close()


# ============================================================
# QUEUE — INSERT / CLAIM / COMPLETE
# ============================================================

def insert_queue_batch(run_id: str, sku_list: list[dict]) -> int:
    """
    Bulk-insert all SKUs as 'pending' into sku_queue.
    Returns the number of rows inserted.
    """
    conn = _conn()
    cur  = conn.cursor()
    rows = [
        (run_id, s["sku_id"], s.get("container"), 1 if s["reprocess"] else 0, s.get("category"))
        for s in sku_list
    ]
    cur.executemany("""
        INSERT INTO sku_queue (run_id, sku_id, container, reprocess, category, status)
        VALUES (%s, %s, %s, %s, %s, 'pending')
    """, rows)
    conn.commit()
    inserted = len(rows)
    cur.close()
    conn.close()
    log.info(f"Queue: inserted {inserted} tasks for run {run_id}")
    return inserted


def claim_next_task(run_id: str, worker_id: str) -> Optional[dict]:
    """
    Atomically claim the next pending task for this worker.

    Uses UPDATE … LIMIT 1 then SELECT pattern — compatible with MySQL 5.x+.
    (MySQL 8.0+ SELECT … FOR UPDATE SKIP LOCKED is NOT required.)

    Returns the task row dict, or None when no pending tasks remain.
    """
    conn = _conn()
    cur  = conn.cursor(dictionary=True)
    try:
        now = datetime.now()

        # Step 1: Atomically claim the next pending row via UPDATE LIMIT 1.
        # MySQL guarantees this UPDATE touches exactly one row and is
        # internally locked, so two concurrent workers can never claim
        # the same row.
        cur.execute("""
            UPDATE sku_queue
            SET    status = 'running', worker_id = %s, claimed_at = %s
            WHERE  run_id = %s AND status = 'pending'
            ORDER  BY id
            LIMIT  1
        """, (worker_id, now, run_id))
        conn.commit()

        if cur.rowcount == 0:
            return None   # queue is empty — nothing left to process

        # Step 2: Read back the row we just claimed.
        # Within a single run_id each worker processes one task at a time,
        # so (run_id, worker_id, status='running') is unique here.
        cur.execute("""
            SELECT id, sku_id, container, reprocess, category
            FROM   sku_queue
            WHERE  run_id = %s AND worker_id = %s AND status = 'running'
            ORDER  BY claimed_at DESC
            LIMIT  1
        """, (run_id, worker_id))
        return cur.fetchone()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def mark_queue_complete(queue_id: int, status: str, error_msg: Optional[str] = None) -> None:
    """
    Update a queue row to its final status: 'done' | 'failed' | 'skipped'.
    Called by the worker after process_sku() returns or raises.
    """
    conn = _conn()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE sku_queue
        SET    status = %s, finished_at = %s, error_msg = %s
        WHERE  id = %s
    """, (status, datetime.now(), error_msg, queue_id))
    conn.commit()
    cur.close()
    conn.close()


def get_queue_stats(run_id: str) -> dict:
    """Return counts per status for a given run (useful for monitoring)."""
    conn = _conn()
    cur  = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT status, COUNT(*) AS cnt
        FROM   sku_queue
        WHERE  run_id = %s
        GROUP  BY status
    """, (run_id,))
    stats = {row["status"]: row["cnt"] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return stats


# ============================================================
# SKU RESULT HELPERS
# ============================================================

def get_sku_status(sku_id: str) -> Optional[dict]:
    """Return the most recent DB row for this SKU, or None."""
    conn = _conn()
    cur  = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT status, blob_count, last_processed_at, container_name FROM sku_results WHERE sku_id=%s",
        (sku_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def get_sku_container(sku_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    Return (container_name, container_source) for a SKU from sku_results.
    Returns (None, None) if the SKU has no DB record.
    """
    conn = _conn()
    cur  = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT container_name, container_source FROM sku_results WHERE sku_id = %s",
        (sku_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return row["container_name"], row["container_source"]
    return None, None


def upsert_sku_result(
    run_id:             str,
    sku_id:             str,
    status:             str,
    container_name:     str,
    container_source:   str,
    reprocess:          bool,
    blob_count:         int,
    cloudinary_sent:    int,
    cloudinary_skipped: int,
    azure_uploaded:     int,
    cloudinary_urls:    list[str],
    azure_urls:         list[str],
    error_code:         Optional[str] = None,
    error_msg:          Optional[str] = None,
    category:           Optional[str] = None,
) -> None:
    """
    INSERT the first time a SKU is seen; UPDATE on every subsequent run.
    reprocessed_at is set (or refreshed) only when reprocess=True.
    listing_cloudinary_url is always cloudinary_urls[0] — the first (sorted) image,
    which is the _01 shot used on the listing page.
    """
    conn = _conn()
    cur  = conn.cursor()
    now  = datetime.now()

    # First Azure URL = _01 image (blobs are sorted alphabetically before processing)
    listing_azure_url = azure_urls[0] if azure_urls else None

    cur.execute("""
        INSERT INTO sku_results
            (run_id, sku_id, status, category, container_name, container_source,
             reprocess, blob_count, cloudinary_sent, cloudinary_skipped,
             azure_uploaded, cloudinary_urls, azure_urls, listing_azure_url,
             error_code, error_msg, last_processed_at, reprocessed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            run_id            = VALUES(run_id),
            status            = VALUES(status),
            category          = VALUES(category),
            container_name    = VALUES(container_name),
            container_source  = VALUES(container_source),
            reprocess         = VALUES(reprocess),
            blob_count        = VALUES(blob_count),
            cloudinary_sent   = VALUES(cloudinary_sent),
            cloudinary_skipped= VALUES(cloudinary_skipped),
            azure_uploaded    = VALUES(azure_uploaded),
            cloudinary_urls   = VALUES(cloudinary_urls),
            azure_urls        = VALUES(azure_urls),
            listing_azure_url = VALUES(listing_azure_url),
            error_code        = VALUES(error_code),
            error_msg         = VALUES(error_msg),
            last_processed_at = VALUES(last_processed_at),
            reprocessed_at    = IF(VALUES(reprocess) = 1, VALUES(last_processed_at), reprocessed_at)
    """, (
        run_id, sku_id, status, category, container_name, container_source,
        1 if reprocess else 0,
        blob_count, cloudinary_sent, cloudinary_skipped, azure_uploaded,
        json.dumps(cloudinary_urls),
        json.dumps(azure_urls),
        listing_azure_url,
        error_code, error_msg,
        now,
        now if reprocess else None,
    ))
    conn.commit()
    cur.close()
    conn.close()


def mark_skipped(run_id: str, sku_id: str, category: Optional[str] = None) -> None:
    conn = _conn()
    cur  = conn.cursor()
    now  = datetime.now()
    cur.execute("""
        INSERT INTO sku_results (run_id, sku_id, status, category, last_processed_at)
        VALUES (%s, %s, 'skipped', %s, %s)
        ON DUPLICATE KEY UPDATE
            run_id            = VALUES(run_id),
            status            = IF(status = 'done', 'done', 'skipped'),
            category          = COALESCE(VALUES(category), category),
            last_processed_at = VALUES(last_processed_at)
    """, (run_id, sku_id, category, now))
    conn.commit()
    cur.close()
    conn.close()


# ============================================================
# IMAGE RESULT HELPERS
# ============================================================

def save_image_result(
    run_id:         str,
    sku_id:         str,
    blob_name:      str,
    filename:       str,
    method:         str,
    cloudinary_url: Optional[str],
    azure_url:      Optional[str],
    status:         str,
    error_code:     Optional[str] = None,
    error_msg:      Optional[str] = None,
    vision_data:    Optional[str] = None,
    transform_data: Optional[str] = None,
) -> None:
    conn = _conn()
    cur  = conn.cursor()

    # Get MAX reprocess_count across the entire SKU (not just this file)
    # so all images in the SKU stay on the same version number.
    cur.execute("""
        SELECT COALESCE(MAX(reprocess_count), 0) AS max_rc
        FROM   image_results
        WHERE  sku_id = %s
    """, (sku_id,))
    row = cur.fetchone()
    reprocess_count = (row[0] or 0) + 1

    cur.execute("""
        INSERT INTO image_results
            (run_id, sku_id, blob_name, filename, method,
             cloudinary_url, azure_url, status,
             error_code, error_msg, processed_at, reprocess_count,
             vision_data, transform_data)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        run_id, sku_id, blob_name, filename, method,
        cloudinary_url, azure_url, status,
        error_code, error_msg, datetime.now(), reprocess_count,
        vision_data, transform_data,
    ))

    # Sync all other rows for this SKU to the same reprocess_count
    cur.execute("""
        UPDATE image_results
        SET    reprocess_count = %s
        WHERE  sku_id = %s
    """, (reprocess_count, sku_id))

    conn.commit()
    cur.close()
    conn.close()


def update_reprocess_transform(
    sku_id:                   str,
    filename:                 str,
    reprocess_transform_data: str,
    new_cloudinary_url:       Optional[str],
    new_azure_url:            Optional[str],
) -> int:
    """
    Update the latest image_results row for this sku_id+filename:
      - sets reprocess_transform_data  (new transform applied)
      - updates cloudinary_url + azure_url to the new ones
      - increments reprocess_count across ALL rows for this SKU
        so CloudFlare cache is busted (?v=N)
    The original transform_data is preserved as-is for audit.
    Returns the new reprocess_count.
    """
    conn = _conn()
    cur  = conn.cursor()

    # Get current MAX reprocess_count for this SKU
    cur.execute("""
        SELECT COALESCE(MAX(reprocess_count), 0) AS max_rc
        FROM   image_results
        WHERE  sku_id = %s
    """, (sku_id,))
    row             = cur.fetchone()
    new_version     = (row[0] or 0) + 1

    # Update this specific image row
    cur.execute("""
        UPDATE image_results
        SET    reprocess_transform_data = %s,
               cloudinary_url           = %s,
               azure_url                = %s,
               reprocess_count          = %s
        WHERE  sku_id   = %s
        AND    filename = %s
        ORDER  BY processed_at DESC
        LIMIT  1
    """, (
        reprocess_transform_data,
        new_cloudinary_url,
        new_azure_url,
        new_version,
        sku_id,
        filename,
    ))

    # Sync ALL rows for this SKU to same version → CF busts cache for all
    cur.execute("""
        UPDATE image_results
        SET    reprocess_count = %s
        WHERE  sku_id = %s
    """, (new_version, sku_id))

    conn.commit()
    cur.close()
    conn.close()
    return new_version
