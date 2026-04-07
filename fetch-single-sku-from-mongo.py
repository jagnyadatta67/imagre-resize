"""
fetch-single-sku-from-mongo.py — Export SINGLE-SKU base_product_ids from MongoDB.

For each base_product_id in the input CSV:
  1. Batch query MongoDB using $in + filter product_type=SINGLE-SKU (100 IDs per batch)
  2. Extract: image_container (VERSION_TO_CONTAINER), category (L1-L2)
  3. Group results by category
  4. Write one CSV per category into MONGO_CSV/
       MONGO_CSV/{category}.csv
       columns: sku_id, container, reprocess

Features
--------
  - Batch $in queries   → ~100x fewer MongoDB round trips
  - ThreadPoolExecutor  → parallel batch workers
  - Resume support      → skips already-processed IDs (MONGO_CSV/.processed_single.txt)
  - tqdm progress bar   → live progress for 15k+ records
  - Debug logging       → full visibility at every step

Usage
-----
  python fetch-single-sku-from-mongo.py --input input.csv
  python fetch-single-sku-from-mongo.py --input input.csv --workers 10 --batch-size 100
  python fetch-single-sku-from-mongo.py --input input.csv --no-resume
  python fetch-single-sku-from-mongo.py --input input.csv --debug
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
log = logging.getLogger("fetch_single_sku")

# ── Silence noisy pymongo topology DEBUG logs ─────────────────
logging.getLogger("pymongo").setLevel(logging.WARNING)

# ==============================
# 🔧 CONFIG
# ==============================
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://cc_revamp:XZXYEay6be4BySY157@10.126.23.4:27017/")
DB_NAME         = "product-service"
COLLECTION_NAME = "products"

OUTPUT_DIR       = BASE_DIR / "MONGO_CSV_single"
CHECKPOINT_FILE  = OUTPUT_DIR / ".processed_single.txt"

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

# Accepted column names for base_product_id input
_ID_COL_CANDIDATES = [
    "base_product_id", "BASE_PRODUCT_ID",
    "product_id", "PRODUCT_ID",
    "sku_id", "SKU_ID", "sku", "SKU",
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

        id_col = next((c for c in _ID_COL_CANDIDATES if c in fieldnames), None)
        if not id_col:
            id_col = fieldnames[0] if fieldnames else None
        if not id_col:
            log.error(f"Cannot find base_product_id column. Columns: {fieldnames}")
            sys.exit(1)

        log.info(f"Using column '{id_col}' as base_product_id source")

        for row in reader:
            val = (row.get(id_col) or "").strip()
            if val:
                ids.append(val)

    # Deduplicate preserving order
    seen   = set()
    unique = []
    for v in ids:
        if v not in seen:
            seen.add(v)
            unique.append(v)

    log.info(f"Read {len(ids)} rows → {len(unique)} unique base_product_ids from {csv_path.name}")
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
    batch:        list[str],
    collection,                   # pymongo Collection (thread-safe reads)
    lock:         threading.Lock,
    category_map: dict,
    not_found:    list,
    skipped_type: list,
) -> int:
    """
    Query MongoDB for a batch of base_product_ids using $in + SINGLE-SKU filter.
    Populates category_map, not_found, skipped_type in-place (thread-safe via lock).
    Returns count of matched IDs.
    """
    try:
        docs = list(collection.find(
            {
                "base_product_id": {"$in": batch},
                "product_type":    "SINGLE-SKU",
            },
            {
                "base_product_id": 1,
                "image_container": 1,
                "categories":      1,
                "product_type":    1,
            },
        ))
    except Exception as exc:
        log.error(f"MongoDB batch query failed: {exc}")
        with lock:
            not_found.extend(batch)
        return 0

    # Build set of found IDs
    found_ids: set[str] = set()
    results:   list[dict] = []

    for doc in docs:
        pid       = doc.get("base_product_id", "")
        version   = doc.get("image_container", "")
        container = VERSION_TO_CONTAINER.get(version, "in-media-uk")
        category  = extract_category(doc.get("categories", []))

        log.debug(f"[{pid}] image_container={version!r} → container={container}  category={category}")

        found_ids.add(pid)
        results.append({
            "sku_id":    pid,
            "container": container,
            "reprocess": "",
            "_category": category,
        })

    # Check for IDs that exist in Mongo but are NOT SINGLE-SKU
    # (they won't appear in docs because of the product_type filter)
    # Do a second quick check to distinguish "not found" vs "wrong type"
    found_any_type: set[str] = set()
    missing = [pid for pid in batch if pid not in found_ids]
    if missing:
        try:
            other_docs = list(collection.find(
                {"base_product_id": {"$in": missing}},
                {"base_product_id": 1, "product_type": 1},
            ))
            for d in other_docs:
                pid  = d.get("base_product_id", "")
                ptype = d.get("product_type", "unknown")
                found_any_type.add(pid)
                log.warning(f"[{pid}] Skipped — product_type={ptype!r} (not SINGLE-SKU)")
        except Exception:
            pass

    truly_missing = [pid for pid in missing if pid not in found_any_type]
    wrong_type    = [pid for pid in missing if pid in found_any_type]

    if truly_missing:
        log.warning(f"Not found in MongoDB ({len(truly_missing)}): "
                    f"{truly_missing[:5]}{'...' if len(truly_missing) > 5 else ''}")
    if wrong_type:
        log.warning(f"Wrong product_type ({len(wrong_type)}): "
                    f"{wrong_type[:5]}{'...' if len(wrong_type) > 5 else ''}")

    with lock:
        for r in results:
            cat = r.pop("_category")
            category_map[cat].append(r)
        not_found.extend(truly_missing)
        skipped_type.extend(wrong_type)

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
                    val = (er.get("sku_id") or "").strip()
                    if val:
                        existing.add(val)
            log.debug(f"[{category}] Existing file: {len(existing)} entries")

        new_rows = [r for r in rows if r["sku_id"] not in existing]
        mode     = "a" if out_path.exists() else "w"

        with open(out_path, mode, newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["sku_id", "container", "reprocess"])
            if mode == "w":
                writer.writeheader()
            writer.writerows(new_rows)

        total = len(existing) + len(new_rows)
        log.info(f"  → {out_path.name:<45}  +{len(new_rows):>5} new   {total:>6} total")


# ==============================
# 🧾 WRITE NOT FOUND / SKIPPED FILES
# ==============================
def write_not_found(not_found: list, skipped_type: list) -> None:
    if not_found:
        out_path = BASE_DIR / "not_found_single.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["base_product_id"])
            for item in not_found:
                writer.writerow([item])
        log.warning(f"Not found: {len(not_found)} ID(s) → not_found_single.csv")

    if skipped_type:
        out_path = BASE_DIR / "skipped_wrong_type.csv"
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["base_product_id"])
            for item in skipped_type:
                writer.writerow([item])
        log.warning(f"Wrong product_type: {len(skipped_type)} ID(s) → skipped_wrong_type.csv")


# ==============================
# 🚀 ENTRY POINT
# ==============================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export SINGLE-SKU base_product_ids from MongoDB into per-category CSVs."
    )
    parser.add_argument("--input",      "-i", default="input.csv",
                        help="Input CSV with base_product_id column (default: input.csv)")
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
    log.info("fetch-single-sku-from-mongo  (MongoDB → MONGO_CSV_single)")
    log.info(f"  Input      : {input_path}")
    log.info(f"  Output     : {OUTPUT_DIR}")
    log.info(f"  Filter     : product_type = SINGLE-SKU")
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
    skipped_type: list[str]             = []
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
                print(f"\r  Progress: {done_count[0]}/{len(batches)} batches ({pct:.0f}%)",
                      end="", flush=True)
        def close_progress():
            print()

    # ── Parallel batch processing ─────────────────────────────
    matched_total = 0
    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="mongo") as ex:
        futures = {
            ex.submit(process_batch, batch, collection, lock,
                      category_map, not_found, skipped_type): batch
            for batch in batches
        }
        for future in as_completed(futures):
            batch = futures[future]
            try:
                matched_total += future.result()
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
    write_not_found(not_found, skipped_type)

    # ── Summary ───────────────────────────────────────────────
    total_written = sum(len(v) for v in category_map.values())
    print()
    log.info("=" * 60)
    log.info(f"  DONE")
    log.info(f"  Processed    : {len(todo)} IDs")
    log.info(f"  Matched      : {matched_total}  (SINGLE-SKU)")
    log.info(f"  Wrong type   : {len(skipped_type)}  (not SINGLE-SKU → skipped_wrong_type.csv)")
    log.info(f"  Not found    : {len(not_found)}  (→ not_found_single.csv)")
    log.info(f"  Categories   : {len(category_map)}")
    log.info(f"  Written      : {total_written} rows across {len(category_map)} file(s)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
