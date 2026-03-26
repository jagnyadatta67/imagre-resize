"""
fetch_skus_from_mongo.py — Fetch product SKUs from the Content Central MongoDB
and write one CSV per category into the input_csv/ folder.

Filters applied:
  1. created_at  >= "2024-01-01"          (string comparison — YYYY-MM-DD HH:MM:SS)
  2. colors[].push_to_production_status == "finished"   (at least one color)
  3. categories[].name                  == "women"       (top-level category)

Output CSV columns (compatible with csv_loader.py):
  sku_id     — color_variant_id   e.g. "1000007626343-Red-RED"
  container  — left blank (pipeline resolves via Azure)
  reprocess  — left blank (default False)

⚠ NOTE: The MongoDB password contains a '#' which MUST be URL-encoded as '%23'.
   Raw URI  :  mongodb://cc_revamp_uat:S2dFvA7Z47DZQ#p@10.126.33.228:27017/
   Encoded  :  mongodb://cc_revamp_uat:S2dFvA7Z47DZQ%23p@10.126.33.228:27017/

Usage
-----
  python fetch_skus_from_mongo.py                          # all categories default
  python fetch_skus_from_mongo.py --from-date 2024-06-01  # override start date
  python fetch_skus_from_mongo.py --out-dir /tmp/csv_out
  python fetch_skus_from_mongo.py --db-name mydb --collection products
  python fetch_skus_from_mongo.py --dry-run               # print count, no CSV write
  python fetch_skus_from_mongo.py --stats                 # print breakdown by category
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt = "%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── MongoDB config ─────────────────────────────────────────────────────────────
# ⚠ The '#' in the password MUST be URL-encoded as '%23' for pymongo to parse
# the URI correctly. The raw password is: S2dFvA7Z47DZQ#p
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://cc_revamp:XZXYEay6be4BySY157@10.126.23.4:27017/"
)
MONGO_DB         = os.getenv("MONGO_DB",         "product-service")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "products")


# ── Default query parameters ───────────────────────────────────────────────────
DEFAULT_FROM_DATE = "2024-01-01"
CATEGORY_NAME     = "women"


# ── MongoDB query ──────────────────────────────────────────────────────────────
#
# Document structure (confirmed from sample):
# {
#   "created_at": "2019-04-29 09:42:25",           ← STRING  YYYY-MM-DD HH:MM:SS
#   "categories": [
#       {
#           "name": "shoesandbags",                 ← top-level category
#           "sub_category": {
#               "name": "menaccessories",
#               "sub_category": { "name": "backpack" }
#           }
#       }
#   ],
#   "colors": [
#       {
#           "name": "Red",
#           "color_variant_id": "1000007626343-Red-RED",   ← sku_id
#           "push_to_production_status": "finished",
#           ...
#       },
#       ...
#   ]
# }
#
# Query logic:
#   - created_at  >=  "2024-01-01"
#     (lexicographic comparison works for YYYY-MM-DD HH:MM:SS format)
#
#   - colors array has AT LEAST ONE element where push_to_production_status = "finished"
#     ($elemMatch is required to match conditions on the SAME array element)
#
#   - categories array has AT LEAST ONE element where name = "women"
#     (MongoDB dot-notation on array field matches any element)
#
def build_query(from_date: str, category: str) -> dict:
    return {
        "created_at": {
            "$gte": f"{from_date} 00:00:00"   # pad to full datetime string
        },
        "colors": {
            "$elemMatch": {
                "push_to_production_status": "finished"
            }
        },
        "categories.name": category,
    }


# Projection — only fetch fields we actually need (faster, less memory)
PROJECTION = {
    "_id":                   0,
    "base_product_id":       1,
    "colors.color_variant_id":             1,
    "colors.push_to_production_status":    1,
    "colors.name":                         1,
    "categories":                          1,
    "created_at":                          1,
}


# ── Core fetch logic ───────────────────────────────────────────────────────────

def fetch_skus_from_mongo(
    from_date:  str,
    category:   str,
    batch_size: int = 500,
) -> list[dict]:
    """
    Query MongoDB and return a flat list of SKU dicts.

    Only colors with push_to_production_status == "finished" are included
    (a product may have some finished and some not-finished colors).

    Returns
    -------
    list of {"sku_id": "...", "container": ""}
    """
    try:
        from pymongo import MongoClient
    except ImportError:
        log.error("pymongo is not installed. Run:  pip install pymongo")
        sys.exit(1)

    log.info(f"Connecting to MongoDB at {MONGO_URI.split('@')[-1]} ...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)

    try:
        # Ping to confirm connection before running query
        client.admin.command("ping")
        log.info("  MongoDB connection OK")
    except Exception as exc:
        log.error(f"  MongoDB connection FAILED: {exc}")
        sys.exit(1)

    db         = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    query      = build_query(from_date, category)

    log.info(f"  Database   : {MONGO_DB}")
    log.info(f"  Collection : {MONGO_COLLECTION}")
    log.info(f"  Query      : {query}")

    # Count first for progress reporting
    total_docs = collection.count_documents(query)
    log.info(f"  Matching documents: {total_docs}")

    if total_docs == 0:
        client.close()
        return []

    # Cursor with batch_size to avoid loading everything into memory at once
    cursor = collection.find(query, PROJECTION).batch_size(batch_size)

    skus: list[dict] = []
    doc_count = 0

    for doc in cursor:
        doc_count += 1
        colors = doc.get("colors") or []

        for color in colors:
            # Only export colors that are actually finished
            if color.get("push_to_production_status") != "finished":
                continue

            cv_id = (color.get("color_variant_id") or "").strip()
            if not cv_id:
                continue

            skus.append({
                "sku_id":    cv_id,
                "container": "",    # Azure container resolved by pipeline
            })

        if doc_count % 1000 == 0:
            log.info(f"  Processed {doc_count}/{total_docs} documents ...")

    client.close()
    log.info(f"  Total documents scanned : {doc_count}")
    log.info(f"  Total finished SKUs     : {len(skus)}")
    return skus


# ── Stats helper ───────────────────────────────────────────────────────────────

def fetch_category_stats(from_date: str) -> None:
    """
    Print a breakdown of how many finished-color products exist
    per top-level category for the given date range.
    Useful for exploring the data before running a full export.
    """
    try:
        from pymongo import MongoClient
    except ImportError:
        log.error("pymongo is not installed.")
        sys.exit(1)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    db         = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    pipeline = [
        # Step 1 — date filter + at least one finished color
        {
            "$match": {
                "created_at": {"$gte": f"{from_date} 00:00:00"},
                "colors": {
                    "$elemMatch": {"push_to_production_status": "finished"}
                },
            }
        },
        # Step 2 — unwind categories array
        {"$unwind": "$categories"},
        # Step 3 — group by top-level category name
        {
            "$group": {
                "_id":   "$categories.name",
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
    ]

    log.info(f"\nCategory breakdown (created_at >= {from_date}, any color finished):")
    log.info(f"  {'Category':<30} {'Products':>10}")
    log.info(f"  {'-'*30} {'-'*10}")
    total = 0
    for doc in collection.aggregate(pipeline):
        log.info(f"  {doc['_id']:<30} {doc['count']:>10}")
        total += doc["count"]
    log.info(f"  {'TOTAL':<30} {total:>10}")
    client.close()


# ── Explore / diagnostics ──────────────────────────────────────────────────────

def explore(collection_name: str) -> None:
    """
    Inspect the actual field values in the collection to diagnose why
    the main query returns 0 results.

    Prints:
      1. Total document count
      2. One sample document (keys + top-level values only, no image arrays)
      3. Distinct values of  created_at  (first 5 — to confirm format)
      4. Distinct values of  categories.name  (all)
      5. Distinct values of  colors.push_to_production_status  (all)
      6. Count with NO date filter (checks category + status filter alone)
      7. Count with NO category filter (checks date + status filter alone)
    """
    try:
        from pymongo import MongoClient
    except ImportError:
        log.error("pymongo is not installed.")
        sys.exit(1)

    client     = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    db         = client[MONGO_DB]
    collection = db[collection_name]

    print("\n" + "=" * 65)
    print("EXPLORE MODE — diagnosing why query returns 0 results")
    print("=" * 65)

    # ── 1. Total docs ─────────────────────────────────────────────────────────
    total = collection.count_documents({})
    print(f"\n[1] Total documents in collection '{collection_name}': {total}")

    # ── 2. Sample document (safe subset of fields) ────────────────────────────
    sample = collection.find_one(
        {},
        {
            "_id": 0,
            "base_product_id":   1,
            "product_type":      1,
            "created_at":        1,
            "concept":           1,
            "region":            1,
            "categories":        1,
            "colors.name":                         1,
            "colors.color_variant_id":             1,
            "colors.push_to_production_status":    1,
        }
    )
    if sample:
        print("\n[2] Sample document (safe fields):")
        import json
        print(json.dumps(sample, indent=4, default=str))
    else:
        print("\n[2] Collection appears empty.")
        client.close()
        return

    # ── 3. created_at format (first 5 distinct values) ───────────────────────
    print("\n[3] Sample 'created_at' values (first 5):")
    for doc in collection.find({}, {"_id": 0, "created_at": 1}).limit(5):
        val = doc.get("created_at")
        print(f"    {val!r}  (type: {type(val).__name__})")

    # ── 4. Distinct categories.name ──────────────────────────────────────────
    print("\n[4] Distinct 'categories.name' values:")
    cat_names = collection.distinct("categories.name")
    for name in sorted(cat_names):
        print(f"    {name!r}")

    # ── 5. Distinct colors.push_to_production_status ─────────────────────────
    print("\n[5] Distinct 'colors.push_to_production_status' values:")
    statuses = collection.distinct("colors.push_to_production_status")
    for s in sorted(str(v) for v in statuses):
        print(f"    {s!r}")

    # ── 6. Count: category + status filter ONLY (no date) ────────────────────
    count_no_date = collection.count_documents({
        "colors":          {"$elemMatch": {"push_to_production_status": "finished"}},
        "categories.name": CATEGORY_NAME,
    })
    print(f"\n[6] Docs matching (category='{CATEGORY_NAME}' + status='finished') "
          f"with NO date filter: {count_no_date}")

    # ── 7. Count: date + status filter ONLY (no category) ────────────────────
    count_no_cat = collection.count_documents({
        "created_at": {"$gte": f"{DEFAULT_FROM_DATE} 00:00:00"},
        "colors":     {"$elemMatch": {"push_to_production_status": "finished"}},
    })
    print(f"\n[7] Docs matching (date>={DEFAULT_FROM_DATE} + status='finished') "
          f"with NO category filter: {count_no_cat}")

    # ── 8. Count: date filter ONLY ───────────────────────────────────────────
    count_date_only = collection.count_documents({
        "created_at": {"$gte": f"{DEFAULT_FROM_DATE} 00:00:00"},
    })
    print(f"\n[8] Docs matching date>={DEFAULT_FROM_DATE} ONLY: {count_date_only}")

    print("\n" + "=" * 65)
    client.close()


# ── CSV writer ─────────────────────────────────────────────────────────────────

def write_csv(skus: list[dict], out_path: Path) -> None:
    """Write SKUs to a CSV compatible with csv_loader.py."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["sku_id", "container", "reprocess"])
        for item in skus:
            writer.writerow([item["sku_id"], item["container"], ""])
    log.info(f"  Written {len(skus)} row(s) → {out_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    global MONGO_DB, MONGO_COLLECTION   # declared first — before any use

    parser = argparse.ArgumentParser(
        description=(
            "Fetch finished-color SKUs from Content Central MongoDB "
            "and write input CSV for the image pipeline."
        )
    )
    parser.add_argument(
        "--from-date",
        default = DEFAULT_FROM_DATE,
        metavar = "YYYY-MM-DD",
        help    = f"Earliest created_at date to include (default: {DEFAULT_FROM_DATE})",
    )
    parser.add_argument(
        "--category",
        default = CATEGORY_NAME,
        metavar = "NAME",
        help    = f"Top-level category name filter (default: {CATEGORY_NAME})",
    )
    parser.add_argument(
        "--out-dir",
        default = "input_csv",
        metavar = "DIR",
        help    = "Output directory for CSV files (default: input_csv/)",
    )
    parser.add_argument(
        "--out-file",
        default = None,
        metavar = "FILENAME",
        help    = (
            "Output CSV filename (default: {category}-mongo-{from_date}.csv). "
            "Relative to --out-dir."
        ),
    )
    parser.add_argument(
        "--db-name",
        default = MONGO_DB,
        metavar = "DB",
        help    = f"MongoDB database name (default: {MONGO_DB})",
    )
    parser.add_argument(
        "--collection",
        default = MONGO_COLLECTION,
        metavar = "COLL",
        help    = f"MongoDB collection name (default: {MONGO_COLLECTION})",
    )
    parser.add_argument(
        "--dry-run",
        action = "store_true",
        help   = "Fetch and count SKUs but do NOT write CSV",
    )
    parser.add_argument(
        "--stats",
        action = "store_true",
        help   = "Print category breakdown and exit (no CSV written)",
    )
    parser.add_argument(
        "--explore",
        action = "store_true",
        help   = "Inspect actual field values to diagnose query mismatches",
    )
    args = parser.parse_args()

    # Apply user-supplied overrides
    MONGO_DB         = args.db_name
    MONGO_COLLECTION = args.collection

    # ── Explore mode ──────────────────────────────────────────────────────────
    if args.explore:
        explore(args.collection)
        return

    # ── Stats mode ────────────────────────────────────────────────────────────
    if args.stats:
        fetch_category_stats(args.from_date)
        return

    # ── Resolve output path ───────────────────────────────────────────────────
    out_dir  = Path(args.out_dir)
    filename = args.out_file or f"{args.category}-mongo-{args.from_date}.csv"
    out_path = out_dir / filename

    log.info(
        f"MongoDB SKU fetch — "
        f"category={args.category!r}  "
        f"from={args.from_date}  "
        f"out={out_path}"
        f"{' [DRY RUN]' if args.dry_run else ''}"
    )

    # ── Fetch ─────────────────────────────────────────────────────────────────
    skus = fetch_skus_from_mongo(
        from_date = args.from_date,
        category  = args.category,
    )

    if not skus:
        log.warning("No matching SKUs found — CSV not written.")
        return

    # ── Deduplicate (preserve first-seen order) ───────────────────────────────
    seen:   set[str]   = set()
    unique: list[dict] = []
    for item in skus:
        if item["sku_id"] not in seen:
            seen.add(item["sku_id"])
            unique.append(item)

    dup_count = len(skus) - len(unique)
    if dup_count:
        log.info(f"  Deduplication: removed {dup_count} duplicate(s)")

    # ── Write ─────────────────────────────────────────────────────────────────
    if not args.dry_run:
        write_csv(unique, out_path)
    else:
        log.info(f"  [DRY RUN] would write {len(unique)} row(s) to {out_path}")

    log.info(f"\n  Final unique SKU count: {len(unique)}")


if __name__ == "__main__":
    main()
