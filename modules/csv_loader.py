"""
csv_loader.py — Read all .csv files from a folder into a deduplicated SKU list.

Expected CSV columns (any order, case variants accepted):
  sku_id     — required
  container  — optional Azure container name (skips Unbxd if present)
  reprocess  — optional true/1/yes to force reprocess even if already done

Deduplication rule (same sku_id in multiple CSVs):
  - reprocess=True wins over False
  - last non-empty container hint wins
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_SKU_COL_CANDIDATES = ["sku_id", "SKU_ID", "sku", "SKU", "product_id", "PRODUCT_ID"]
_CONTAINER_COL      = "container"
_REPROCESS_COL      = "reprocess"
_TRUE_VALUES        = {"true", "1", "yes"}


def load_csvs_from_folder(folder_path: str) -> tuple[list[dict], list[str]]:
    """
    Read every *.csv file in folder_path.
    Category is derived from the filename stem (e.g. men.csv → "men").

    Returns
    -------
    sku_list   : list of {sku_id, container, reprocess, category}
    csv_files  : list of filenames that were loaded
    """
    folder = Path(folder_path)
    if not folder.is_dir():
        raise ValueError(f"Not a directory: {folder_path}")

    csv_paths = sorted(folder.glob("*.csv"))
    if not csv_paths:
        raise ValueError(f"No .csv files found in {folder_path}")

    log.info(f"Found {len(csv_paths)} CSV file(s): {[p.name for p in csv_paths]}")

    # sku_id → merged dict
    merged: dict[str, dict] = {}

    for csv_path in csv_paths:
        category = csv_path.stem.lower()   # men.csv → "men"
        for row in _read_single_csv(csv_path):
            sku_id   = row["sku_id"]
            existing = merged.get(sku_id, {"container": None, "reprocess": False, "category": category})
            merged[sku_id] = {
                "sku_id":    sku_id,
                # last non-empty container hint wins
                "container": row["container"] or existing["container"],
                # True wins
                "reprocess": row["reprocess"] or existing["reprocess"],
                # first file wins for category
                "category":  existing.get("category") or category,
            }

    sku_list = list(merged.values())
    reprocess_count = sum(1 for s in sku_list if s["reprocess"])
    log.info(
        f"Loaded {len(sku_list)} unique SKU(s) "
        f"({reprocess_count} flagged for reprocess)"
    )
    return sku_list, [p.name for p in csv_paths]


# ── Internal ──────────────────────────────────────────────────

def _read_single_csv(csv_path: Path) -> list[dict]:
    rows: list[dict] = []

    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader     = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []

        sku_col = _find_sku_col(fieldnames)
        if not sku_col:
            log.warning(f"  {csv_path.name}: cannot identify SKU column — skipping file")
            return []

        has_container = _CONTAINER_COL in fieldnames
        has_reprocess = _REPROCESS_COL in fieldnames

        for row in reader:
            sku_id = (row.get(sku_col) or "").strip()
            if not sku_id:
                continue

            container_val = (row.get(_CONTAINER_COL) or "").strip() if has_container else ""
            reprocess_val = (row.get(_REPROCESS_COL) or "").strip().lower() if has_reprocess else ""

            rows.append({
                "sku_id":    sku_id,
                "container": container_val or None,
                "reprocess": reprocess_val in _TRUE_VALUES,
            })

    log.info(f"  {csv_path.name}: {len(rows)} row(s)")
    return rows


def _find_sku_col(fieldnames: list[str]) -> Optional[str]:
    for candidate in _SKU_COL_CANDIDATES:
        if candidate in fieldnames:
            return candidate
    # last resort: first column
    return fieldnames[0] if fieldnames else None
