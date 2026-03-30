"""
converter.py — In-memory image conversion pipeline.

NO files are read from or written to disk.
All operations work on raw bytes / BytesIO objects.

Decision tree per image
-----------------------
pad_mode = "no" / "fill"  → Cloudinary FILL  (crop:fill,  gravity:auto)
pad_mode = "auto"         → Cloudinary PAD   (crop:pad,   background:auto)
pad_mode = "gen_fill"     → Cloudinary PAD   (crop:pad,   background:gen_fill)

No Google Vision API calls. No Pillow.

Returns
-------
(output_bytes, used_cloudinary, cloudinary_url, vision_data, transform_data)
"""

from __future__ import annotations

import io
import logging
from typing import Optional

import requests as _req
import cloudinary
import cloudinary.uploader

from config import (
    TARGET_W, TARGET_H,
    CLOUDINARY_CLOUD, CLOUDINARY_KEY, CLOUDINARY_SECRET,
)

log = logging.getLogger(__name__)

cloudinary.config(
    cloud_name = CLOUDINARY_CLOUD,
    api_key    = CLOUDINARY_KEY,
    api_secret = CLOUDINARY_SECRET,
    secure     = True,
)

log.info("=" * 62)
log.info("  CLOUDINARY   cloud=%-20s  key=%s***", CLOUDINARY_CLOUD, CLOUDINARY_KEY[:6])
log.info("  GOOGLE VISION  disabled — no Vision API calls")
log.info("=" * 62)


# ============================================================
# PUBLIC ENTRY POINT — WORKER
# ============================================================

def convert_image_bytes(
    image_bytes:  bytes,
    filename:     str,
    sku_id:       str,
    logger:       logging.Logger,
    pad_mode:     str = "auto",
) -> tuple[bytes, bool, Optional[str], str, str]:
    """
    Convert raw image bytes to TARGET_W × TARGET_H JPEG bytes.

    Parameters
    ----------
    image_bytes  : raw source image bytes (downloaded from Azure)
    filename     : original filename (used as Cloudinary public_id)
    sku_id       : SKU identifier (used as Cloudinary folder)
    logger       : SKU-scoped logger
    pad_mode     : "no" | "fill" | "auto" | "gen_fill"
                   "no" / "fill" → Cloudinary FILL (no padding)
                   "auto"        → Cloudinary PAD  background:auto
                   "gen_fill"    → Cloudinary PAD  background:gen_fill

    Returns
    -------
    output_bytes    : final JPEG bytes ready for Azure upload
    used_cloudinary : always True (Cloudinary is always used)
    cloudinary_url  : Cloudinary CDN URL
    vision_data     : "no_vision" (Vision API disabled)
    transform_data  : human-readable string describing the transformation
    """
    transformation, td = _build_transformation(pad_mode, filename, logger)
    out_bytes, used, url = _cloudinary_upload_and_fetch(
        image_bytes, sku_id, filename, transformation, logger
    )
    return out_bytes, used, url, "no_vision", td


# ============================================================
# PUBLIC ENTRY POINT — REPROCESS (Flask UI)
# ============================================================

def reprocess_single_image(
    image_bytes: bytes,
    filename:    str,
    sku_id:      str,
    method:      str,
    logger:      logging.Logger,
) -> tuple[bytes, bool, Optional[str], str, str]:
    """
    Reprocess a single image from the Flask UI.
    method is taken directly from user input — no Vision AI.

    Parameters
    ----------
    method : "no" | "fill" | "auto" | "gen_fill"
        "no" / "fill"  → Cloudinary FILL  (crop:fill, gravity:auto)
        "auto"         → Cloudinary PAD   background:auto
        "gen_fill"     → Cloudinary PAD   background:gen_fill

    Returns
    -------
    (output_bytes, used_cloudinary, cloudinary_url, vision_data, transform_data)
    """
    transformation, td = _build_transformation(method, filename, logger)
    out_bytes, used, url = _cloudinary_upload_and_fetch(
        image_bytes, sku_id, filename, transformation, logger
    )
    return out_bytes, used, url, "no_vision:reprocess", td


# ============================================================
# SHARED TRANSFORMATION BUILDER
# ============================================================

def _build_transformation(
    pad_mode: str,
    filename: str,
    logger:   logging.Logger,
) -> tuple[list, str]:
    """
    Build Cloudinary transformation list and audit string from pad_mode.

    pad_mode "no" / "fill" → FILL  (no padding, smart crop)
    pad_mode "auto"        → PAD   background:auto
    pad_mode "gen_fill"    → PAD   background:gen_fill
    """
    # Normalise: "fill" is an alias for "no"
    if pad_mode == "fill":
        pad_mode = "no"

    if pad_mode == "center":
        transformation = [{"width": TARGET_W, "height": TARGET_H,
                           "crop": "fill", "gravity": "center"}]
        td = f"engine:cloudinary,crop:fill,gravity:center,w:{TARGET_W},h:{TARGET_H}"
        logger.info(f"[{filename}] Cloudinary FILL  (center crop)")
    elif pad_mode == "no":
        transformation = [{"width": TARGET_W, "height": TARGET_H,
                           "crop": "fill", "gravity": "auto"}]
        td = f"engine:cloudinary,crop:fill,gravity:auto,w:{TARGET_W},h:{TARGET_H}"
        logger.info(f"[{filename}] Cloudinary FILL  (no pad)")
    else:
        bg = _pad_bg(pad_mode)
        transformation = [{"width": TARGET_W, "height": TARGET_H,
                           "crop": "pad",  "gravity": "center",
                           "background":   bg}]
        td = f"engine:cloudinary,crop:pad,gravity:center,bg:{bg},w:{TARGET_W},h:{TARGET_H}"
        logger.info(f"[{filename}] Cloudinary PAD  bg={bg}")

    return transformation, td


# ============================================================
# CLOUDINARY UPLOAD + FETCH
# ============================================================

def _cloudinary_upload_and_fetch(
    image_bytes:    bytes,
    sku_id:         str,
    filename:       str,
    transformation: list,
    logger:         logging.Logger,
) -> tuple[bytes, bool, str]:
    """
    Upload BytesIO to Cloudinary → fetch the transformed result into memory.
    Returns (output_bytes, True, cloudinary_url).
    """
    public_id = filename.rsplit(".", 1)[0]   # strip extension

    result = cloudinary.uploader.upload(
        io.BytesIO(image_bytes),
        folder          = sku_id,
        public_id       = public_id,
        overwrite       = True,
        unique_filename = False,
        transformation  = transformation,
    )
    cloudinary_url = result["secure_url"]
    logger.info(f"[{filename}] Cloudinary URL: {cloudinary_url}")

    # Fetch transformed result into memory (no disk write)
    resp = _req.get(cloudinary_url, timeout=30)
    resp.raise_for_status()

    return resp.content, True, cloudinary_url


# ============================================================
# HELPERS
# ============================================================

def _pad_bg(pad_mode: str) -> str:
    """Map pad_mode string to Cloudinary background value."""
    return {"auto": "auto", "gen_fill": "gen_fill", "white": "white"}.get(pad_mode, "auto")
