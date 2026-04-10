"""
pipeline_runner.py — Core pipeline logic, callable from both CLI (run.py) and web (ui/app.py).

Extracted from run.py so the same worker pool can be triggered programmatically
without requiring CSV files or argparse.
"""

from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
from typing import Optional

from modules import db
from modules.azure_client import AzureClient
from modules.converter import convert_image_bytes

log = logging.getLogger("pipeline_runner")


# ============================================================
# API-LIMIT DETECTION
# ============================================================

_LIMIT_KEYWORDS = [
    "quota", "rate limit", "rate_limit", "too many requests",
    "resource exhausted", "resourceexhausted", "insufficient credits",
    "billing", "payment required", "limit exceeded", "quota exceeded",
    "credit", "plan limit", "monthly limit", "daily limit",
]


def _is_api_limit(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if status == 429:
        return True
    return any(kw in str(exc).lower() for kw in _LIMIT_KEYWORDS)


def _identify_api(exc: Exception) -> str:
    module = (type(exc).__module__ or "").lower()
    if "google" in module or "vision" in module:
        return "Google Vision"
    if "cloudinary" in module:
        return "Cloudinary"
    if "azure" in module:
        return "Azure"
    return type(exc).__name__


class StopSignal(Exception):
    """Raised when an API quota / billing error is detected."""
    pass


# ============================================================
# PER-SKU PIPELINE
# ============================================================

def process_sku(
    sku_entry:  dict,
    run_id:     str,
    azure:      AzureClient,
    pad_mode:   str,
    stop_lock:  Lock,
    stop_flag:  list,
    dry_run:    bool,
) -> str:
    """
    Full pipeline for one SKU.
    Returns "done" | "skipped" | "failed".
    Raises StopSignal on API limit.
    """
    sku_id    = sku_entry["sku_id"]
    container = sku_entry.get("container")
    reprocess = sku_entry.get("reprocess", False)
    category  = sku_entry.get("category")
    sku_log   = logging.getLogger(f"sku.{sku_id}")

    # Skip if already done and not forced reprocess
    existing = db.get_sku_status(sku_id)
    if existing and existing["status"] in ("done", "skipped") and not reprocess:
        sku_log.info(f"SKIP {sku_id}  (already done)")
        db.mark_skipped(run_id, sku_id, category=category)
        return "skipped"

    # Resolve container
    if container:
        container_name, container_source = container, "db"
        sku_log.info(f"Container from DB: {container_name}")
    else:
        container_name, container_source = azure.detect_container(sku_id)
        sku_log.info(f"Container detected ({container_source}): {container_name}")

    sku_log.info(f"=== START  {sku_id}  container={container_name} ===")

    try:
        blobs = azure.list_sku_blobs(container_name, sku_id)
    except Exception as exc:
        sku_log.error(f"list_blobs error: {exc}")
        db.upsert_sku_result(
            run_id, sku_id, "failed", container_name, container_source,
            reprocess, 0, 0, 0, 0, [], [],
            error_code=type(exc).__name__, error_msg=str(exc),
            category=category,
        )
        return "failed"

    if not blobs:
        sku_log.warning(f"No blobs found — container={container_name}  prefix=lifestyle/{sku_id}")
        db.upsert_sku_result(
            run_id, sku_id, "failed", container_name, container_source,
            reprocess, 0, 0, 0, 0, [], [],
            error_code="NO_BLOBS",
            error_msg=f"No blobs in {container_name} for prefix lifestyle/{sku_id}",
            category=category,
        )
        return "failed"

    sku_log.info(f"Found {len(blobs)} blob(s)")

    if dry_run:
        sku_log.info(f"DRY RUN — skipping convert + upload for {len(blobs)} blob(s)")
        return "skipped"

    cloudinary_sent    = 0
    cloudinary_skipped = 0
    azure_uploaded     = 0
    cloudinary_urls: list[str] = []
    azure_urls:      list[str] = []

    for blob_name in blobs:
        if stop_flag[0]:
            sku_log.warning("Stop flag set — aborting remaining blobs")
            break

        filename = blob_name.split("/")[-1]

        try:
            image_bytes = azure.download_blob_bytes(container_name, blob_name)

            output_bytes, used_cloudinary, cloudinary_url, vision_data, transform_data = \
                convert_image_bytes(image_bytes, filename, sku_id, sku_log, pad_mode=pad_mode)

            azure_url = azure.upload_to_newc(filename, output_bytes, container_name)

            if used_cloudinary:
                cloudinary_sent += 1
                if cloudinary_url:
                    cloudinary_urls.append(cloudinary_url)
            else:
                cloudinary_skipped += 1

            azure_uploaded += 1
            azure_urls.append(azure_url)

            db.save_image_result(
                run_id, sku_id, blob_name, filename,
                "cloudinary" if used_cloudinary else "pillow",
                cloudinary_url, azure_url, "done",
                vision_data=vision_data, transform_data=transform_data,
            )
            sku_log.info(f"  ✓ {filename}  via={'cloudinary' if used_cloudinary else 'pillow'}")

        except StopSignal:
            raise

        except Exception as exc:
            if _is_api_limit(exc):
                api = _identify_api(exc)
                with stop_lock:
                    stop_flag[0] = True
                sku_log.error(f"API LIMIT ({api}): {exc}")
                db.save_image_result(
                    run_id, sku_id, blob_name, filename,
                    "unknown", None, None, "failed",
                    error_code="API_LIMIT", error_msg=f"[{api}] {exc}",
                )
                raise StopSignal(f"{api} — {exc}") from exc

            sku_log.error(f"  ✗ {filename}  {type(exc).__name__}: {exc}")
            db.save_image_result(
                run_id, sku_id, blob_name, filename,
                "unknown", None, None, "failed",
                error_code=type(exc).__name__, error_msg=str(exc),
            )

    final_status = "done" if azure_uploaded > 0 else "failed"
    db.upsert_sku_result(
        run_id, sku_id, final_status,
        container_name, container_source, reprocess,
        len(blobs), cloudinary_sent, cloudinary_skipped, azure_uploaded,
        cloudinary_urls, azure_urls,
        category=category,
    )
    sku_log.info(
        f"=== DONE  {sku_id}  "
        f"blobs={len(blobs)}  cloudinary={cloudinary_sent}  "
        f"pillow={cloudinary_skipped}  uploaded={azure_uploaded} ==="
    )
    return final_status


# ============================================================
# WORKER LOOP
# ============================================================

def _postfix(counts: dict) -> dict:
    return {
        "done":    counts.get("done",    0),
        "failed":  counts.get("failed",  0),
        "skipped": counts.get("skipped", 0),
    }


def worker_loop(
    run_id:      str,
    azure:       AzureClient,
    pad_mode:    str,
    stop_flag:   list,
    stop_lock:   Lock,
    dry_run:     bool,
    counts:      dict,
    counts_lock: Lock,
    pbar=None,           # optional tqdm bar — None for web context
) -> None:
    worker_id = threading.current_thread().name

    while not stop_flag[0]:
        task = db.claim_next_task(run_id, worker_id)
        if task is None:
            break

        queue_id  = task["id"]
        sku_entry = {
            "sku_id":    task["sku_id"],
            "container": task["container"],
            "reprocess": bool(task["reprocess"]),
            "category":  task.get("category"),
        }

        status = "failed"
        error  = None

        try:
            status = process_sku(
                sku_entry, run_id, azure,
                pad_mode, stop_lock, stop_flag, dry_run,
            )
        except StopSignal as exc:
            error  = str(exc)
            status = "failed"
            db.mark_queue_complete(queue_id, "failed", error)
            with counts_lock:
                counts["failed"] = counts.get("failed", 0) + 1
                if pbar:
                    pbar.set_postfix(**_postfix(counts))
                    pbar.update(1)
            raise

        except Exception as exc:
            error  = f"{type(exc).__name__}: {exc}"
            status = "failed"
            log.error(f"Unhandled error  sku={sku_entry['sku_id']}  worker={worker_id}: {exc}")

        db.mark_queue_complete(queue_id, status, error)

        with counts_lock:
            counts[status] = counts.get(status, 0) + 1
            if pbar:
                pbar.set_postfix(**_postfix(counts))
                pbar.update(1)


# ============================================================
# PUBLIC API — run_pipeline()
# ============================================================

def run_pipeline(
    sku_list:   list[dict],
    run_id:     str,
    pad_mode:   str  = "auto",
    workers:    int  = 10,
    dry_run:    bool = False,
    source:     str  = "web-trigger",   # shown in sku_runs.csv_folder
    pbar=None,                           # optional tqdm — pass from CLI, None for web
) -> dict:
    """
    Core pipeline entry point. Callable from CLI and web.

    sku_list items: {sku_id, container (optional), category (optional), reprocess (optional)}

    Returns: {done, failed, skipped}
    """
    azure  = AzureClient()
    total  = len(sku_list)

    db.create_run(run_id, source, [], total)
    queued = db.insert_queue_batch(run_id, sku_list)

    log.info(
        f"Pipeline started — run_id={run_id}  skus={total}  "
        f"workers={workers}  pad_mode={pad_mode}  source={source}"
    )

    counts:      dict[str, int] = {"done": 0, "skipped": 0, "failed": 0}
    counts_lock: Lock           = Lock()
    stop_flag:   list[bool]     = [False]
    stop_lock:   Lock           = Lock()
    api_limit_hit               = False

    n_workers = min(workers, queued) if queued > 0 else 1

    try:
        with ThreadPoolExecutor(
            max_workers        = n_workers,
            thread_name_prefix = "worker",
        ) as executor:
            futures = [
                executor.submit(
                    worker_loop,
                    run_id, azure, pad_mode,
                    stop_flag, stop_lock,
                    dry_run,
                    counts, counts_lock, pbar,
                )
                for _ in range(n_workers)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except StopSignal as exc:
                    if not api_limit_hit:
                        api_limit_hit = True
                        log.error(f"API LIMIT — stopping all workers: {exc}")
                except Exception as exc:
                    log.error(f"Worker thread error: {exc}")
    finally:
        if pbar:
            pbar.close()

    db.finish_run(run_id, counts["done"], counts["failed"], counts["skipped"])

    log.info(
        f"Pipeline complete — run_id={run_id}  "
        f"done={counts['done']}  failed={counts['failed']}  skipped={counts['skipped']}"
    )
    return counts
