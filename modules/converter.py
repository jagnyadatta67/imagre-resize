"""
converter.py — In-memory image conversion pipeline.

NO files are read from or written to disk.
All operations work on raw bytes / BytesIO objects.

Decision tree per image
-----------------------
1. Google Vision text detection
   └── text-heavy (infographic / size guide)
       → Cloudinary PAD  (auto background by default)

2. Google Vision object localisation  (not text-heavy)
   ├── FULL_AI=true  → always Cloudinary
   ├── no detection  → local Pillow center-crop
   ├── bbox wider than target → Cloudinary FILL or PAD depending on head room
   └── safe bbox  → local Pillow crop around object center

Returns
-------
(output_bytes: bytes, used_cloudinary: bool, cloudinary_url: str | None)
"""

from __future__ import annotations

import io
import logging
from typing import Optional

import numpy as np
import requests as _req
import cloudinary
import cloudinary.uploader
from PIL import Image
from google.cloud import vision

from config import (
    TARGET_W, TARGET_H, TARGET_RATIO,
    JPEG_QUALITY, TEXT_THRESHOLD, FULL_AI,
    CLOUDINARY_CLOUD, CLOUDINARY_KEY, CLOUDINARY_SECRET,
)

log = logging.getLogger(__name__)

# ── Singletons (initialised once, safe for multi-threading) ──
_vision_client = vision.ImageAnnotatorClient()

cloudinary.config(
    cloud_name = CLOUDINARY_CLOUD,
    api_key    = CLOUDINARY_KEY,
    api_secret = CLOUDINARY_SECRET,
    secure     = True,
)


# ============================================================
# PUBLIC ENTRY POINT
# ============================================================

def convert_image_bytes(
    image_bytes:  bytes,
    filename:     str,
    sku_id:       str,
    logger:       logging.Logger,
    pad_mode:     str = "auto",
) -> tuple[bytes, bool, Optional[str]]:
    """
    Convert raw image bytes to TARGET_W × TARGET_H JPEG bytes.

    Parameters
    ----------
    image_bytes  : raw source image bytes (downloaded from Azure)
    filename     : original filename (used as Cloudinary public_id)
    sku_id       : SKU identifier (used as Cloudinary folder)
    logger       : SKU-scoped logger
    pad_mode     : "auto" | "white" | "gen_fill"

    Returns
    -------
    output_bytes    : final JPEG bytes ready for Azure upload
    used_cloudinary : True if a Cloudinary API call was made
    cloudinary_url  : Cloudinary CDN URL (None when Pillow path used)
    """
    # ── Step 1: text-heavy check ─────────────────────────────
    if _is_text_heavy(image_bytes, logger):
        logger.info(f"[{filename}] text-heavy → Cloudinary pad  bg={pad_mode}")
        return _cloudinary_pad(image_bytes, sku_id, filename, logger, pad_mode)

    # ── Step 2: smart crop / AI reframe ──────────────────────
    return _smart_crop_or_ai(image_bytes, sku_id, filename, logger, pad_mode)


# ============================================================
# GOOGLE VISION — TEXT DETECTION
# ============================================================

def _is_text_heavy(image_bytes: bytes, logger: logging.Logger) -> bool:
    response = _vision_client.text_detection(vision.Image(content=image_bytes))
    texts    = response.text_annotations
    if not texts:
        return False
    char_count = len(texts[0].description.strip())
    logger.debug(f"Vision text chars={char_count}  threshold={TEXT_THRESHOLD}")
    return char_count > TEXT_THRESHOLD


# ============================================================
# GOOGLE VISION — OBJECT DETECTION
# ============================================================

def _detect_bbox(image_bytes: bytes, logger: logging.Logger) -> Optional[tuple]:
    """
    Returns merged normalised bounding box (x_min, y_min, x_max, y_max)
    for the highest-priority detected object, or None if nothing found.

    Priority: Person > Footwear > Clothing > largest object (fallback)
    """
    response = _vision_client.object_localization(vision.Image(content=image_bytes))
    objects  = response.localized_object_annotations

    logger.debug(f"Vision objects={len(objects)}")
    if not objects:
        logger.warning("Vision: no objects detected")
        return None

    # Log all detections for audit
    for i, obj in enumerate(objects):
        v = obj.bounding_poly.normalized_vertices
        logger.debug(
            f"  [{i}] name={obj.name!r:<14}  score={obj.score:.3f}  "
            f"bbox=({v[0].x:.3f},{v[0].y:.3f})→({v[2].x:.3f},{v[2].y:.3f})"
        )

    person_objs = [o for o in objects if o.name == "Person"]
    if person_objs:
        target, label = person_objs, "Person"
    else:
        shoe_objs = [o for o in objects if o.name in ("Shoe", "Sandal", "Footwear")]
        if shoe_objs:
            target, label = shoe_objs, "Footwear"
        else:
            cloth_objs = [
                o for o in objects
                if o.name in ("Dress", "Clothing", "Apparel", "Top", "Shirt")
            ]
            if cloth_objs:
                target, label = cloth_objs, "Clothing"
            else:
                target = [max(objects, key=lambda o: _bbox_area(o))]
                label  = f"Fallback({target[0].name})"

    bbox = _merge_bboxes(target)
    logger.debug(
        f"Vision winner={label}  "
        f"bbox=({bbox[0]:.3f},{bbox[1]:.3f})→({bbox[2]:.3f},{bbox[3]:.3f})"
    )
    return bbox


def _bbox_area(obj) -> float:
    v = obj.bounding_poly.normalized_vertices
    return (v[2].x - v[0].x) * (v[2].y - v[0].y)


def _merge_bboxes(objs) -> tuple[float, float, float, float]:
    x_min = min(o.bounding_poly.normalized_vertices[0].x for o in objs)
    y_min = min(o.bounding_poly.normalized_vertices[0].y for o in objs)
    x_max = max(o.bounding_poly.normalized_vertices[2].x for o in objs)
    y_max = max(o.bounding_poly.normalized_vertices[2].y for o in objs)
    return x_min, y_min, x_max, y_max


# ============================================================
# SMART CROP ORCHESTRATOR
# ============================================================

def _smart_crop_or_ai(
    image_bytes: bytes,
    sku_id:      str,
    filename:    str,
    logger:      logging.Logger,
    pad_mode:    str,
) -> tuple[bytes, bool, Optional[str]]:

    img      = Image.open(io.BytesIO(image_bytes))
    img_w, img_h = img.size
    bbox     = _detect_bbox(image_bytes, logger)

    # Force all through Cloudinary if FULL_AI=true
    if FULL_AI:
        logger.info(f"[{filename}] FULL_AI → Cloudinary reframe")
        return _cloudinary_reframe(image_bytes, sku_id, filename, bbox, logger, pad_mode)

    # No detection → safe local center crop
    if not bbox:
        logger.info(f"[{filename}] no detection → local center crop")
        return _pil_center_crop(img), False, None

    x_min_norm, _, x_max_norm, _ = bbox
    x_min        = int(x_min_norm * img_w)
    x_max        = int(x_max_norm * img_w)
    bbox_width   = x_max - x_min
    target_width = int(img_h * TARGET_RATIO)

    if bbox_width > target_width:
        logger.info(f"[{filename}] bbox wider than target → Cloudinary reframe")
        return _cloudinary_reframe(image_bytes, sku_id, filename, bbox, logger, pad_mode)

    # Safe local crop centred on object
    obj_cx = (x_min + x_max) // 2
    left   = max(0, min(obj_cx - target_width // 2, img_w - target_width))
    right  = left + target_width
    logger.info(f"[{filename}] local crop  ({left},0)→({right},{img_h})")
    return _pil_to_bytes(img.crop((left, 0, right, img_h))), False, None


# ============================================================
# CLOUDINARY PATHS
# ============================================================

def _cloudinary_reframe(
    image_bytes: bytes,
    sku_id:      str,
    filename:    str,
    bbox:        Optional[tuple],
    logger:      logging.Logger,
    pad_mode:    str,
) -> tuple[bytes, bool, str]:
    bg = _pad_bg(pad_mode)

    if not bbox:
        # No bbox → safe fill
        transformation = [{"width": TARGET_W, "height": TARGET_H,
                           "crop": "fill", "gravity": "auto"}]
        logger.info(f"[{filename}] Cloudinary FILL (no bbox)")
    else:
        _, y_min_norm, _, _ = bbox
        if y_min_norm < 0.05:
            # Head touching top → fill to avoid cutting off
            transformation = [{"width": TARGET_W, "height": TARGET_H,
                               "crop": "fill", "gravity": "auto"}]
            logger.info(f"[{filename}] Cloudinary FILL (head tight, y_min={y_min_norm:.3f})")
        else:
            transformation = [{"width": TARGET_W, "height": TARGET_H,
                               "crop": "pad",  "gravity": "center",
                               "background": bg}]
            logger.info(f"[{filename}] Cloudinary PAD  bg={bg}  y_min={y_min_norm:.3f}")

    return _cloudinary_upload_and_fetch(image_bytes, sku_id, filename, transformation, logger)


def _cloudinary_pad(
    image_bytes: bytes,
    sku_id:      str,
    filename:    str,
    logger:      logging.Logger,
    pad_mode:    str,
) -> tuple[bytes, bool, str]:
    bg = _pad_bg(pad_mode)
    transformation = [{"width": TARGET_W, "height": TARGET_H,
                       "crop": "pad", "gravity": "center",
                       "background": bg}]
    return _cloudinary_upload_and_fetch(image_bytes, sku_id, filename, transformation, logger)


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
# PILLOW HELPERS
# ============================================================

def _pil_center_crop(img: Image.Image) -> bytes:
    w, h = img.size
    if w / h > TARGET_RATIO:
        new_w = int(h * TARGET_RATIO)
        left  = (w - new_w) // 2
        img   = img.crop((left, 0, left + new_w, h))
    else:
        new_h = int(w / TARGET_RATIO)
        top   = (h - new_h) // 2
        img   = img.crop((0, top, w, top + new_h))
    return _pil_to_bytes(img)


def _pil_to_bytes(img: Image.Image) -> bytes:
    img = img.resize((TARGET_W, TARGET_H), Image.LANCZOS)
    if img.mode != "RGB":
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=JPEG_QUALITY,
             optimize=True, progressive=True)
    return buf.getvalue()


def _pad_bg(pad_mode: str) -> str:
    return {"white": "white", "auto": "auto", "gen_fill": "gen_fill"}.get(pad_mode, "auto")
