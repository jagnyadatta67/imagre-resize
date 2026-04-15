"""
run.py — Bulk image transfer pipeline (CLI entry point).

Usage
-----
  python run.py /path/to/csv_folder [options]

  --workers N           Parallel SKU workers   (default: BULK_WORKERS from .env)
  --pad-mode MODE       auto | white | gen_fill | no  (default: auto)
  --dry-run             List blobs only, no conversion or upload

Queue model (MySQL pull queue)
------------------------------
  Phase 1 — LOAD:   All SKUs from CSV are bulk-inserted into sku_queue as 'pending'.
  Phase 2 — PROCESS: N worker threads pull tasks atomically from the queue.
  Resume:   Re-run the same command — 'done' SKUs are skipped automatically.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from datetime import datetime

from tqdm import tqdm

from config import BULK_WORKERS, VERBOSE_LOG
from modules import db
from modules.csv_loader import load_csvs_from_folder
from modules.pipeline_runner import StopSignal, run_pipeline


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

if not VERBOSE_LOG:
    for _noisy in [
        "azure.core.pipeline.policies.http_logging_policy",
        "azure.core",
        "azure.storage",
        "urllib3",
        "requests",
    ]:
        logging.getLogger(_noisy).setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk image pipeline: Azure → Cloudinary → Azure lifestyle-converted"
    )
    parser.add_argument("csv_folder",
        help="Folder containing one or more .csv files")
    parser.add_argument("--workers", type=int, default=BULK_WORKERS,
        help=f"Parallel worker threads (default: {BULK_WORKERS})")
    parser.add_argument("--pad-mode", default="auto",
        choices=["auto", "white", "gen_fill", "no"],
        help="Cloudinary bg mode: auto|white|gen_fill=pad with bg, no=crop=fill (default: auto)")
    parser.add_argument("--dry-run", action="store_true",
        help="Detect containers + list blobs only — no conversion or upload")
    parser.add_argument("--reprocess", action="store_true",
        help="Reprocess all SKUs even if already done (global override)")
    args = parser.parse_args()

    db.init_db()
    run_id = str(uuid.uuid4())

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
    print(f"  Reprocess : {args.reprocess}")
    print(f"  Started   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*65}\n")

    pbar = tqdm(
        total      = total,
        desc       = "SKUs",
        unit       = "sku",
        bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
    )

    counts = run_pipeline(
        sku_list        = sku_list,
        run_id          = run_id,
        pad_mode        = args.pad_mode,
        workers         = args.workers,
        dry_run         = args.dry_run,
        source          = args.csv_folder,
        pbar            = pbar,
        force_pad_mode  = True,
        force_reprocess = args.reprocess,
    )

    queue_stats = db.get_queue_stats(run_id)

    print(f"\n{'='*65}")
    print(f"  COMPLETE  run_id={run_id}")
    print(f"  Done      : {counts['done']}")
    print(f"  Failed    : {counts['failed']}")
    print(f"  Skipped   : {counts['skipped']}")
    print(f"  Queue     : {queue_stats}")
    print(f"  Finished  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Log       : logs/pipeline.log")
    print(f"{'='*65}\n")

    sys.exit(1 if counts["failed"] > 0 else 0)


if __name__ == "__main__":
    main()
