"""
run.py — Bulk image transfer pipeline (CLI entry point).

Usage
-----
  python run.py /path/to/csv_folder [options]

  --workers N           Parallel SKU workers   (default: BULK_WORKERS from .env)
  --pad-mode MODE       auto | white | gen_fill (default: auto)
  --dry-run             List blobs only, no conversion or upload

Queue model (Option B — MySQL as queue)
----------------------------------------
  Phase 1 — LOAD
    All SKUs from CSV are bulk-inserted into sku_queue as 'pending'.

  Phase 2 — PROCESS  (N worker threads, PULL model)
    Each worker loops:
      ① claim_next_task()  →  SELECT FOR UPDATE SKIP LOCKED  →  status='running'
      ② process_sku()      →  download bytes → convert → upload → MySQL
      ③ mark_queue_complete()  →  status='done'|'failed'|'skipped'
      ④ back to ①  until claim returns None (queue empty)

  Crash-safe resume
    If the process is killed mid-run, 'running' rows are left in sku_queue.
    On the NEXT run:
      - A new run_id is created.
      - SKUs already in sku_results with status='done' are immediately skipped.
      - All other SKUs (including previously in-flight ones) are re-queued.

  API-limit handling
    Any Cloudinary / Google Vision 429 / quota error sets a shared stop_flag.
    Workers check stop_flag at the top of every loop iteration — no new tasks
    are claimed after the flag is set. Running tasks finish their current image.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
from typing import Optional

from tqdm import tqdm

from config import BULK_WORKERS, VERBOSE_LOG
from modules import db
from modules.azure_client import AzureClient
from modules.converter import convert_image_bytes
from modules.csv_loader import load_csvs_from_folder


# ============================================================
# LOGGING SETUP
# ============================================================

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level     = logging.INFO,
    format    = "%(asctime)s  %(levelname)-8s  %(name)-25s  %(message)s",
    datefmt   = "%Y-%m-%d %H:%M:%S",
    handlers  = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/pipeline.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("run")

# ── Suppress noisy third-party loggers unless VERBOSE_LOG=true ──
if not VERBOSE_LOG:
    for _noisy in [
        "azure.core.pipeline.policies.http_logging_policy",
        "azure.core",
        "azure.storage",
        "urllib3",
        "requests",
    ]:
        logging.getLogger(_noisy).setLevel(logging.WARNING)


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
    Raises StopSignal on API limit — caller marks queue row and re-raises.
    """
    sku_id    = sku_entry["sku_id"]
    container = sku_entry["container"]
    reprocess = sku_entry["reprocess"]
    category  = sku_entry.get("category")
    sku_log   = logging.getLogger(f"sku.{sku_id}")

    # ── Skip if already done (and not forced reprocess) ──────
    existing = db.get_sku_status(sku_id)
    if existing and existing["status"] == "done" and not reprocess:
        sku_log.info(f"SKIP {sku_id}  (already done — add reprocess=true in CSV to override)")
        db.mark_skipped(run_id, sku_id, category=category)
        return "skipped"

    # ── Resolve source container ──────────────────────────────
    if container:
        container_name, container_source = container, "csv"
        sku_log.info(f"Container from CSV: {container_name}")
    else:
        container_name, container_source = azure.detect_container(sku_id)
        sku_log.info(f"Container from Unbxd ({container_source}): {container_name}")

    sku_log.info(f"=== START  {sku_id}  container={container_name} ===")

    # ── List source blobs ─────────────────────────────────────
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
            error_msg=f"No blobs found in {container_name} for prefix lifestyle/{sku_id}",
            category=category,
        )
        return "failed"

    sku_log.info(f"Found {len(blobs)} blob(s)")

    if dry_run:
        sku_log.info(f"DRY RUN — skipping convert + upload for {len(blobs)} blob(s)")
        return "skipped"

    # ── Process each image in memory ─────────────────────────
    cloudinary_sent    = 0
    cloudinary_skipped = 0
    azure_uploaded     = 0
    cloudinary_urls: list[str] = []
    azure_urls:      list[str] = []

    for blob_name in blobs:

        # Abort remaining blobs if another worker hit an API limit
        if stop_flag[0]:
            sku_log.warning("Stop flag set — aborting remaining blobs in this SKU")
            break

        filename = blob_name.split("/")[-1]

        try:
            # a. Download blob bytes into memory (no disk write)
            image_bytes = azure.download_blob_bytes(container_name, blob_name)

            # b. Convert in memory: Vision → Cloudinary or Pillow
            output_bytes, used_cloudinary, cloudinary_url = convert_image_bytes(
                image_bytes, filename, sku_id, sku_log, pad_mode=pad_mode
            )

            # c. Upload result to Azure {container}/lifestyle-converted/
            azure_url = azure.upload_to_newc(filename, output_bytes, container_name)

            # d. Counters
            if used_cloudinary:
                cloudinary_sent += 1
                if cloudinary_url:
                    cloudinary_urls.append(cloudinary_url)
            else:
                cloudinary_skipped += 1

            azure_uploaded += 1
            azure_urls.append(azure_url)

            # e. Per-image row in MySQL
            db.save_image_result(
                run_id, sku_id, blob_name, filename,
                "cloudinary" if used_cloudinary else "pillow",
                cloudinary_url, azure_url, "done",
            )
            sku_log.info(
                f"  ✓ {filename}"
                f"  via={'cloudinary' if used_cloudinary else 'pillow'}"
            )

        except StopSignal:
            raise   # propagate straight to worker_loop

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

            # Non-fatal image error → log, save, continue next blob
            sku_log.error(f"  ✗ {filename}  {type(exc).__name__}: {exc}")
            db.save_image_result(
                run_id, sku_id, blob_name, filename,
                "unknown", None, None, "failed",
                error_code=type(exc).__name__, error_msg=str(exc),
            )

    # ── Upsert SKU summary ────────────────────────────────────
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
# WORKER LOOP  (pull model — each thread loops on MySQL queue)
# ============================================================

def worker_loop(
    run_id:    str,
    azure:     AzureClient,
    pad_mode:  str,
    stop_flag: list,
    stop_lock: Lock,
    dry_run:   bool,
    counts:    dict,
    counts_lock: Lock,
    pbar:      tqdm,
) -> None:
    """
    Runs inside a worker thread.
    Repeatedly claims the next pending task from MySQL and processes it.
    Exits naturally when there are no more pending tasks OR stop_flag is set.
    """
    worker_id = threading.current_thread().name

    while not stop_flag[0]:

        # ── Claim next task from MySQL queue (atomic) ─────────
        task = db.claim_next_task(run_id, worker_id)
        if task is None:
            # Queue is empty — this worker is done
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
            # Re-raise so the executor can catch it once at the top level
            db.mark_queue_complete(queue_id, "failed", error)
            with counts_lock:
                counts["failed"] += 1
                pbar.set_postfix(**_postfix(counts))
                pbar.update(1)
            raise

        except Exception as exc:
            error  = f"{type(exc).__name__}: {exc}"
            status = "failed"
            log.error(f"Unhandled error  sku={sku_entry['sku_id']}  worker={worker_id}: {exc}")

        # ── Mark queue row complete ───────────────────────────
        db.mark_queue_complete(queue_id, status, error)

        # ── Update shared progress counters (thread-safe) ─────
        with counts_lock:
            counts[status] = counts.get(status, 0) + 1
            pbar.set_postfix(**_postfix(counts))
            pbar.update(1)


def _postfix(counts: dict) -> dict:
    return {
        "done":    counts.get("done",    0),
        "failed":  counts.get("failed",  0),
        "skipped": counts.get("skipped", 0),
    }


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk image pipeline: Azure → Vision + Cloudinary → Azure lifestyle-newc"
    )
    parser.add_argument("csv_folder",
        help="Folder containing one or more .csv files")
    parser.add_argument("--workers", type=int, default=BULK_WORKERS,
        help=f"Parallel worker threads (default: {BULK_WORKERS})")
    parser.add_argument("--pad-mode", default="auto",
        choices=["auto", "white", "gen_fill"],
        help="Cloudinary background fill for pad operations (default: auto)")
    parser.add_argument("--dry-run", action="store_true",
        help="Detect containers + list blobs only — no conversion or upload")
    args = parser.parse_args()

    # ── Init ──────────────────────────────────────────────────
    db.init_db()
    azure  = AzureClient()
    run_id = str(uuid.uuid4())

    # ── Load CSVs ─────────────────────────────────────────────
    sku_list, csv_files = load_csvs_from_folder(args.csv_folder)
    total = len(sku_list)

    print(f"\n{'='*65}")
    print(f"  BULK IMAGE PIPELINE")
    print(f"  Run ID    : {run_id}")
    print(f"  CSV folder: {args.csv_folder}")
    print(f"  CSV files : {', '.join(csv_files)}")
    print(f"  Total SKUs: {total}")
    print(f"  Workers   : {args.workers}")
    print(f"  Pad mode  : {args.pad_mode}")
    print(f"  Dry run   : {args.dry_run}")
    print(f"  Started   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*65}\n")

    # ── Phase 1: Write all SKUs to MySQL queue ─────────────────
    db.create_run(run_id, args.csv_folder, csv_files, total)
    queued = db.insert_queue_batch(run_id, sku_list)

    log.info(
        f"Queue ready — {queued} tasks inserted  "
        f"(workers={args.workers}  pad_mode={args.pad_mode})"
    )

    # ── Phase 2: Workers pull from queue ──────────────────────
    counts:      dict[str, int] = {"done": 0, "skipped": 0, "failed": 0}
    counts_lock: Lock           = Lock()
    stop_flag:   list[bool]     = [False]
    stop_lock:   Lock           = Lock()
    api_limit_hit               = False

    n_workers = min(args.workers, queued)   # no point spawning more workers than tasks

    pbar = tqdm(
        total      = queued,
        desc       = "SKUs",
        unit       = "sku",
        bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
    )

    try:
        with ThreadPoolExecutor(
            max_workers        = n_workers,
            thread_name_prefix = "worker",
        ) as executor:

            futures = [
                executor.submit(
                    worker_loop,
                    run_id, azure, args.pad_mode,
                    stop_flag, stop_lock,
                    args.dry_run,
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
        pbar.close()

    # ── Finish ────────────────────────────────────────────────
    db.finish_run(run_id, counts["done"], counts["failed"], counts["skipped"])

    queue_stats = db.get_queue_stats(run_id)

    print(f"\n{'='*65}")
    print(f"  COMPLETE  run_id={run_id}")
    print(f"  Done      : {counts['done']}")
    print(f"  Failed    : {counts['failed']}")
    print(f"  Skipped   : {counts['skipped']}")
    print(f"  Queue     : {queue_stats}")
    if api_limit_hit:
        print(f"  ⚠  Stopped early — API limit hit.")
        print(f"     Re-run the same command to resume.")
        print(f"     Done SKUs will be skipped automatically.")
    print(f"  Finished  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Log       : logs/pipeline.log")
    print(f"{'='*65}\n")

    sys.exit(1 if counts["failed"] > 0 else 0)


if __name__ == "__main__":
    main()
