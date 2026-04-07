"""
fetch-data-from-unbxd.py — Export color_variant_ids from Content Central MongoDB.

For each color_variant_id in the input CSV:
  1. Batch query MongoDB using $in (100 IDs per batch)
  2. Extract: image_container (VERSION_TO_CONTAINER), category (L1-L2)
  3. Group results by category
  4. Write one CSV per category into MONGO_CSV/
       MONGO_CSV/{category}.csv
       columns: color_variant_id, container, reprocess

Features
--------
  - Batch $in queries   → ~100x fewer MongoDB round trips
  - ThreadPoolExecutor  → parallel batch workers
  - Resume support      → skips already-processed IDs (MONGO_CSV/.processed.txt)
  - tqdm progress bar   → live progress for 15k+ records
  - Debug logging       → full visibility at every step

Usage
-----
  python fetch-data-from-unbxd.py --input input.csv
  python fetch-data-from-unbxd.py --input input.csv --workers 10 --batch-size 100
  python fetch-data-from-unbxd.py --input input.csv --no-resume
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv

# ── Bootstrap ─────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers= [logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("fetch_mongo_csv")

# ── Silence noisy pymongo topology DEBUG logs ─────────────────
logging.getLogger("pymongo").setLevel(logging.WARNING)

# ==============================
# 🔧 CONFIG
# ==============================
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://cc_revamp:XZXYEay6be4BySY157@10.126.23.4:27017/")
DB_NAME         = "product-service"
COLLECTION_NAME = "products"

OUTPUT_DIR      = BASE_DIR / "MONGO_CSV"
CHECKPOINT_FILE = OUTPUT_DIR / ".processed.txt"

DEFAULT_BATCH_SIZE = 100
DEFAULT_WORKERS    = 10

# ==============================
# 📦 VERSION → CONTAINER MAP
# ==============================
VERSION_TO_CONTAINER = {
    "V4": "in-media-ea",
    "V3": "in-media",
    "V2": "in-media-us",
    "V1": "in-media-uk",
}

# Accepted column names for color_variant_id input
_CV_COL_CANDIDATES = [
    "color_variant_id", "COLOR_VARIANT_ID",
    "sku_id", "SKU_ID", "sku", "SKU", "product_id", "PRODUCT_ID",
]

# ==============================
# 🧠 CATEGORY EXTRACTOR
# ==============================
def extract_category(categories: list) -> str:
    if not categories:
        return "unknown"
    current = categories[0]
    levels  = []
    while current:
        levels.append(current.get("name", ""))
        current = current.get("sub_category")
    return "-".join(levels[:2]) if len(levels) >= 2 else "-".join(levels)


# ==============================
# 📥 READ INPUT CSV
# ==============================
def read_input(csv_path: Path) -> list[str]:
    ids = []
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader     = csv.DictReader(fh)
        fieldnames = [f.strip() for f in (reader.fieldnames or [])]
        log.debug(f"CSV columns: {fieldnames}")

        cv_col = next((c for c in _CV_COL_CANDIDATES if c in fieldnames), None)
        if not cv_col:
            cv_col = fieldnames[0] if fieldnames else None
        if not cv_col:
            log.error(f"Cannot find color_variant_id column. Columns: {fieldnames}")
            sys.exit(1)

        log.info(f"Using column '{cv_col}' as color_variant_id source")

        for row in reader:
            val = (row.get(cv_col) or "").strip()
            if val:
                ids.append(val)

    # Deduplicate preserving order
    seen = set()
    unique = []
    for v in ids:
        if v not in seen:
            seen.add(v)
            unique.append(v)

    log.info(f"Read {len(ids)} rows → {len(unique)} unique color_variant_ids from {csv_path.name}")
    return unique


# ==============================
# 💾 CHECKPOINT (RESUME)
# ==============================
def load_checkpoint() -> set[str]:
    if not CHECKPOINT_FILE.exists():
        return set()
    with open(CHECKPOINT_FILE, encoding="utf-8") as fh:
        ids = {line.strip() for line in fh if line.strip()}
    log.info(f"Resume: {len(ids)} already-processed IDs loaded from checkpoint")
    return ids


def append_checkpoint(ids: list[str]) -> None:
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as fh:
        for i in ids:
            fh.write(i + "\n")


# ==============================
# 🔄 BATCH WORKER
# ==============================
def process_batch(
    batch:      list[str],
    collection,             # pymongo Collection (thread-safe)
    lock:       threading.Lock,
    category_map: dict,
    not_found:  list,
) -> int:
    """
    Query MongoDB for a batch of color_variant_ids using $in.
    Populates category_map and not_found in-place (thread-safe via lock).
    Returns count of matched IDs.
    """
    try:
        docs = list(collection.find(
            {"colors.color_variant_id": {"$in": batch}},
            {"colors": 1, "categories": 1, "image_container": 1},
        ))
    except Exception as exc:
        log.error(f"MongoDB batch query failed: {exc}")
        with lock:
            not_found.extend(batch)
        return 0

    # Build lookup: color_variant_id → doc
    found_ids: set[str] = set()
    results:   list[dict] = []

    for doc in docs:
        version   = doc.get("image_container", "")
        container = VERSION_TO_CONTAINER.get(version, "in-media-uk")
        category  = extract_category(doc.get("categories", []))

        for color in doc.get("colors", []):
            cv_id = color.get("color_variant_id", "")
            if cv_id in batch and cv_id not in found_ids:
                found_ids.add(cv_id)
                log.debug(f"[{cv_id}] → container={container}  category={category}")
                results.append({
                    "color_variant_id": cv_id,
                    "container":        container,
                    "reprocess":        "true",
                    "_category":        category,
                })

    # IDs in batch but not returned by Mongo
    missing = [cv for cv in batch if cv not in found_ids]
    if missing:
        log.warning(f"Not found in MongoDB ({len(missing)}): {missing[:5]}{'...' if len(missing) > 5 else ''}")

    with lock:
        for r in results:
            cat = r.pop("_category")
            category_map[cat].append(r)
        not_found.extend(missing)

    return len(found_ids)


# ==============================
# 📤 WRITE OUTPUT FILES
# ==============================
def write_output(category_map: dict) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for category, rows in sorted(category_map.items()):
        out_path = OUTPUT_DIR / f"{category}.csv"

        # Deduplicate against existing file
        existing: set[str] = set()
        if out_path.exists():
            with open(out_path, newline="", encoding="utf-8") as fh:
                for er in csv.DictReader(fh):
                    existing.add((er.get("color_variant_id") or "").strip())
            log.debug(f"[{category}] Existing file: {len(existing)} entries")

        new_rows = [r for r in rows if r["color_variant_id"] not in existing]
        mode     = "a" if out_path.exists() else "w"

        with open(out_path, mode, newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["color_variant_id", "container", "reprocess"])
            if mode == "w":
                writer.writeheader()
            writer.writerows(new_rows)

        total = len(existing) + len(new_rows)
        log.info(f"  → {out_path.name:<45}  +{len(new_rows):>5} new   {total:>6} total")


# ==============================
# 🧾 WRITE NOT FOUND FILE
# ==============================
def write_not_found(not_found: list) -> None:
    if not not_found:
        log.debug("No missing color_variant_ids — skipping not_found.csv")
        return

    out_path = BASE_DIR / "not_found.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["color_variant_id"])
        for item in not_found:
            writer.writerow([item])

    log.warning(f"Not found: {len(not_found)} color_variant_id(s) → not_found.csv")


# ==============================
# 🚀 ENTRY POINT
# ==============================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export color_variant_ids from MongoDB into per-category CSVs (batch + parallel)."
    )
    parser.add_argument("--input",      "-i", default="input.csv",
                        help="Input CSV with color_variant_id column (default: input.csv)")
    parser.add_argument("--workers",    "-w", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel MongoDB workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--batch-size", "-b", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"IDs per $in query (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--no-resume",  action="store_true",
                        help="Ignore checkpoint and reprocess everything")
    parser.add_argument("--debug",      action="store_true",
                        help="Enable DEBUG level logging")
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    input_path = Path(args.input)
    if not input_path.exists():
        log.error(f"Input file not found: {input_path}")
        sys.exit(1)

    log.info("=" * 60)
    log.info("fetch-data-from-unbxd  (MongoDB → MONGO_CSV)")
    log.info(f"  Input      : {input_path}")
    log.info(f"  Output     : {OUTPUT_DIR}")
    log.info(f"  Workers    : {args.workers}")
    log.info(f"  Batch size : {args.batch_size}")
    log.info(f"  Resume     : {not args.no_resume}")
    log.info("=" * 60)

    # ── Read input ────────────────────────────────────────────
    all_ids = read_input(input_path)

    # ── Apply checkpoint (resume) ─────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if args.no_resume:
        todo = all_ids
        log.info("Resume disabled — processing all IDs")
    else:
        already_done = load_checkpoint()
        todo = [i for i in all_ids if i not in already_done]
        skipped = len(all_ids) - len(todo)
        if skipped:
            log.info(f"Skipping {skipped} already-processed IDs  →  {len(todo)} remaining")

    if not todo:
        log.info("Nothing to process — all IDs already in checkpoint.")
        return

    # ── Build batches ─────────────────────────────────────────
    batches = [todo[i:i + args.batch_size] for i in range(0, len(todo), args.batch_size)]
    log.info(f"Total: {len(todo)} IDs  →  {len(batches)} batches of {args.batch_size}")

    # ── Connect MongoDB ───────────────────────────────────────
    from pymongo import MongoClient  # noqa: PLC0415

    log.info(f"Connecting to MongoDB: {MONGO_URI}")
    client     = MongoClient(MONGO_URI, maxPoolSize=args.workers + 2)
    collection = client[DB_NAME][COLLECTION_NAME]
    log.info(f"Connected → db={DB_NAME}  collection={COLLECTION_NAME}")

    # ── Shared state ──────────────────────────────────────────
    category_map: dict[str, list[dict]] = defaultdict(list)
    not_found:    list[str]             = []
    lock          = threading.Lock()

    # ── tqdm progress bar ─────────────────────────────────────
    try:
        from tqdm import tqdm
        progress = tqdm(total=len(batches), unit="batch",
                        desc="Querying MongoDB", dynamic_ncols=True)
        def tick():
            progress.update(1)
        def close_progress():
            progress.close()
    except ImportError:
        log.warning("tqdm not installed — no progress bar (pip install tqdm)")
        done_count = [0]
        def tick():
            done_count[0] += 1
            if done_count[0] % 10 == 0 or done_count[0] == len(batches):
                pct = done_count[0] / len(batches) * 100
                print(f"\r  Progress: {done_count[0]}/{len(batches)} batches ({pct:.0f}%)", end="", flush=True)
        def close_progress():
            print()

    # ── Parallel batch processing ─────────────────────────────
    matched_total = 0
    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="mongo") as ex:
        futures = {
            ex.submit(process_batch, batch, collection, lock, category_map, not_found): batch
            for batch in batches
        }
        for future in as_completed(futures):
            batch = futures[future]
            try:
                matched_total += future.result()
                # Checkpoint completed batch
                append_checkpoint(batch)
            except Exception as exc:
                log.error(f"Batch failed unexpectedly: {exc}")
                with lock:
                    not_found.extend(batch)
            tick()

    close_progress()
    client.close()
    log.info("MongoDB connection closed")

    # ── Write output ──────────────────────────────────────────
    print()
    log.info("Writing output CSVs...")
    write_output(category_map)
    write_not_found(not_found)

    # ── Summary ───────────────────────────────────────────────
    total_written = sum(len(v) for v in category_map.values())
    print()
    log.info("=" * 60)
    log.info(f"  DONE")
    log.info(f"  Processed  : {len(todo)} IDs")
    log.info(f"  Matched    : {matched_total}")
    log.info(f"  Not found  : {len(not_found)}")
    log.info(f"  Categories : {len(category_map)}")
    log.info(f"  Written    : {total_written} rows across {len(category_map)} file(s)")
    if not_found:
        log.warning(f"  {len(not_found)} missing IDs saved to not_found.csv")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
