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
import os
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

# ── One-time startup: log which accounts are in use ───────────
try:
    import google.auth as _gauth
    _gcreds, _gproject = _gauth.default()
    _google_account = (
        getattr(_gcreds, "service_account_email", None)
        or getattr(_gcreds, "_service_account_email", None)
        or "user-account (ADC)"
    )
except Exception as _e:
    _google_account = f"unavailable ({_e})"

_google_cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "(default ADC / env not set)")

log.info("=" * 62)
log.info("  CLOUDINARY   cloud=%-20s  key=%s***", CLOUDINARY_CLOUD, CLOUDINARY_KEY[:6])
log.info("  GOOGLE VISION  account   : %s", _google_account)
log.info("  GOOGLE VISION  cred file : %s", _google_cred_path)
log.info("=" * 62)


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
    # ── Single Vision API call for both text + object detection ──
    texts, objects, labels, web_entities = _vision_analyze(image_bytes, logger)

    # ── Build vision_data string for DB audit ─────────────────
    text_heavy   = _is_text_heavy(texts, logger)
    bag_override = False

    # ── Step 1: text-heavy check ─────────────────────────────
    if text_heavy:
        # Bags/accessories with patterned linings are falsely flagged as
        # text-heavy (repeated logo dots count as chars). Cross-check
        # OBJ + LABEL + WEB — if any signal a bag, skip PAD.
        if _is_bag(objects, labels, web_entities, logger):
            bag_override = True
            logger.info(f"[{filename}] text-heavy overridden — bag detected → smart crop")
        else:
            logger.info(f"[{filename}] text-heavy → Cloudinary fill  gravity:auto")
            output_bytes, used_cloudinary, cloudinary_url, transform_data = \
                _cloudinary_pad(image_bytes, sku_id, filename, logger, pad_mode)
            vision_data = _build_vision_data(texts, objects, labels, web_entities, text_heavy, bag_override)
            return output_bytes, used_cloudinary, cloudinary_url, vision_data, transform_data

    vision_data = _build_vision_data(texts, objects, labels, web_entities, text_heavy, bag_override)

    # ── Step 2: smart crop / AI reframe ──────────────────────
    output_bytes, used_cloudinary, cloudinary_url, transform_data = \
        _smart_crop_or_ai(image_bytes, sku_id, filename, logger, pad_mode, objects)
    return output_bytes, used_cloudinary, cloudinary_url, vision_data, transform_data


def reprocess_single_image(
    image_bytes: bytes,
    filename:    str,
    sku_id:      str,
    method:      str,
    logger:      logging.Logger,
) -> tuple[bytes, bool, Optional[str]]:
    """
    Reprocess a single image WITHOUT Vision AI.
    Called from the Flask UI for manual per-image reprocessing.

    Parameters
    ----------
    method : 'gen_fill' | 'auto' | 'fill' | 'pillow'
        gen_fill  → Cloudinary PAD with AI-generated background
        auto      → Cloudinary PAD with edge-colour background
        fill      → Cloudinary FILL (smart crop, no padding)
        pillow    → Local Pillow centre-crop (free, no external API)

    Returns
    -------
    (output_bytes, used_cloudinary, cloudinary_url)
    """
    if method == "pillow":
        logger.info(f"[{filename}] manual reprocess → Pillow centre crop")
        img = Image.open(io.BytesIO(image_bytes))
        return _pil_center_crop(img), False, None

    if method == "fill":
        # Cloudinary FILL — smart auto-gravity, no background padding
        transformation = [{"width": TARGET_W, "height": TARGET_H,
                           "crop": "fill", "gravity": "auto"}]
        logger.info(f"[{filename}] manual reprocess → Cloudinary FILL")
        out_bytes, used, url = _cloudinary_upload_and_fetch(
            image_bytes, sku_id, filename, transformation, logger
        )
        return out_bytes, used, url

    # gen_fill or auto → Cloudinary PAD with the chosen background
    logger.info(f"[{filename}] manual reprocess → Cloudinary PAD bg={method}")
    out_bytes, used, url, _td = _cloudinary_pad(
        image_bytes, sku_id, filename, logger, pad_mode=method
    )
    return out_bytes, used, url


# ============================================================
# GOOGLE VISION — COMBINED SINGLE CALL
# ============================================================

def _vision_analyze(image_bytes: bytes, logger: logging.Logger):
    """
    Single Vision API call: TEXT, OBJECT, LABEL, WEB.
    Returns (text_annotations, localized_object_annotations, label_annotations, web_entities).
    """
    request = vision.AnnotateImageRequest(
        image    = vision.Image(content=image_bytes),
        features = [
            vision.Feature(type_=vision.Feature.Type.TEXT_DETECTION),
            vision.Feature(type_=vision.Feature.Type.OBJECT_LOCALIZATION),
            vision.Feature(type_=vision.Feature.Type.LABEL_DETECTION, max_results=20),
            vision.Feature(type_=vision.Feature.Type.WEB_DETECTION,   max_results=10),
        ],
    )
    response     = _vision_client.annotate_image(request)
    texts        = response.text_annotations
    objects      = response.localized_object_annotations
    labels       = response.label_annotations
    web_entities = response.web_detection.web_entities if response.web_detection else []

    logger.info(
        f"Vision text_chars={len(texts[0].description.strip()) if texts else 0}  "
        f"objects={len(objects)}  labels={len(labels)}  web_entities={len(web_entities)}"
    )
    return texts, objects, labels, web_entities


# ============================================================
# GOOGLE VISION — TEXT DETECTION
# ============================================================

def _is_text_heavy(texts, logger: logging.Logger) -> bool:
    if not texts:
        return False
    char_count = len(texts[0].description.strip())
    logger.info(f"Vision text chars={char_count}  threshold={TEXT_THRESHOLD}")
    return char_count > TEXT_THRESHOLD


# Bag/accessory keywords checked across OBJ + LABEL + WEB signals.
# Patterned linings (logo dots) cause false text-heavy verdicts on bags —
# any match here overrides text-heavy and routes to smart crop.
_BAG_KEYWORDS = {
    "Bag", "Handbag", "Tote bag", "Backpack", "Luggage & bags",
    "Shoulder Bag", "Hand luggage", "Satchel", "Clutch bag",
    "Briefcase", "Suitcase", "Wallet", "Fashion accessory",
}

def _is_bag(objects, labels, web_entities, logger: logging.Logger) -> bool:
    """
    Return True if any Vision signal (OBJ / LABEL / WEB) identifies a bag.
    Checks all three because OBJ often returns nothing for product shots.
    """
    # 1. Object localization
    for obj in objects:
        if obj.name in _BAG_KEYWORDS:
            logger.info(f"Vision bag override via OBJ: '{obj.name}' score={obj.score:.3f}")
            return True

    # 2. Label detection (scene-level — most reliable for product shots)
    for lbl in labels:
        if lbl.description in _BAG_KEYWORDS:
            logger.info(f"Vision bag override via LABEL: '{lbl.description}' score={lbl.score:.3f}")
            return True

    # 3. Web entities
    for ent in web_entities:
        if ent.description and ent.description in _BAG_KEYWORDS:
            logger.info(f"Vision bag override via WEB: '{ent.description}' score={ent.score:.3f}")
            return True

    return False


def _build_vision_data(texts, objects, labels, web_entities, text_heavy: bool, bag_override: bool) -> str:
    """Build a compact comma-separated key:value string for DB storage."""
    text_chars  = len(texts[0].description.strip()) if texts else 0
    obj_names   = "|".join(o.name for o in objects)          or "none"
    label_names = "|".join(l.description for l in labels[:5]) or "none"
    web_names   = "|".join(e.description for e in web_entities[:3] if e.description) or "none"
    return (
        f"text_chars:{text_chars},"
        f"text_heavy:{str(text_heavy).lower()},"
        f"bag_override:{str(bag_override).lower()},"
        f"objects:{obj_names},"
        f"labels:{label_names},"
        f"web:{web_names}"
    )[:500]   # hard cap to fit VARCHAR(500)


# ============================================================
# GOOGLE VISION — OBJECT DETECTION
# ============================================================

def _detect_bbox(objects, logger: logging.Logger) -> Optional[tuple]:
    """
    Returns merged normalised bounding box (x_min, y_min, x_max, y_max)
    for the highest-priority detected object, or None if nothing found.

    Priority: Person > Footwear > Clothing > largest object (fallback)
    """
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
    objects,
) -> tuple[bytes, bool, Optional[str], str]:

    img          = Image.open(io.BytesIO(image_bytes))
    img_w, img_h = img.size
    bbox         = _detect_bbox(objects, logger)

    # Force all through Cloudinary if FULL_AI=true
    if FULL_AI:
        logger.info(f"[{filename}] FULL_AI → Cloudinary reframe")
        return _cloudinary_reframe(image_bytes, sku_id, filename, bbox, logger, pad_mode)

    # No detection → safe local center crop
    if not bbox:
        logger.info(f"[{filename}] no detection → local center crop")
        w, h = img.size
        if w / h > TARGET_RATIO:
            new_w  = int(h * TARGET_RATIO)
            left   = (w - new_w) // 2
            top, right, bottom = 0, left + new_w, h
        else:
            new_h  = int(w / TARGET_RATIO)
            top    = (h - new_h) // 2
            left, right, bottom = 0, w, top + new_h
        td = f"engine:pillow,method:center_crop,left:{left},top:{top},right:{right},bottom:{bottom}"
        return _pil_center_crop(img), False, None, td

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
    td = f"engine:pillow,method:object_crop,cx:{obj_cx},left:{left},top:0,right:{right},bottom:{img_h}"
    return _pil_to_bytes(img.crop((left, 0, right, img_h))), False, None, td


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
) -> tuple[bytes, bool, str, str]:
    bg = _pad_bg(pad_mode)

    if not bbox:
        transformation = [{"width": TARGET_W, "height": TARGET_H,
                           "crop": "fill", "gravity": "auto"}]
        td = f"engine:cloudinary,crop:fill,gravity:auto,w:{TARGET_W},h:{TARGET_H}"
        logger.info(f"[{filename}] Cloudinary FILL (no bbox)")
    else:
        _, y_min_norm, _, _ = bbox
        if y_min_norm < 0.05:
            transformation = [{"width": TARGET_W, "height": TARGET_H,
                               "crop": "fill", "gravity": "auto"}]
            td = f"engine:cloudinary,crop:fill,gravity:auto,w:{TARGET_W},h:{TARGET_H},reason:head_tight"
            logger.info(f"[{filename}] Cloudinary FILL (head tight, y_min={y_min_norm:.3f})")
        elif pad_mode == "no":
            transformation = [
                {"width": TARGET_W, "height": TARGET_H, "crop": "fill", "gravity": "auto"},
                {"quality": "auto", "fetch_format": "auto"},
            ]
            td = f"engine:cloudinary,crop:fill,gravity:auto,quality:auto,fetch_format:auto,w:{TARGET_W},h:{TARGET_H}"
            logger.info(f"[{filename}] Cloudinary FILL  y_min={y_min_norm:.3f}")
        else:
            transformation = [{"width": TARGET_W, "height": TARGET_H,
                               "crop": "pad",  "gravity": "center",
                               "background": bg}]
            td = f"engine:cloudinary,crop:pad,gravity:center,bg:{bg},w:{TARGET_W},h:{TARGET_H}"
            logger.info(f"[{filename}] Cloudinary PAD  bg={bg}  y_min={y_min_norm:.3f}")

    out_bytes, used, url = _cloudinary_upload_and_fetch(image_bytes, sku_id, filename, transformation, logger)
    return out_bytes, used, url, td


def _cloudinary_pad(
    image_bytes: bytes,
    sku_id:      str,
    filename:    str,
    logger:      logging.Logger,
    pad_mode:    str,
) -> tuple[bytes, bool, str, str]:
    # Text-heavy images → crop:fill gravity:auto (same as reprocess logic)
    transformation = [{"width": TARGET_W, "height": TARGET_H,
                       "crop": "fill", "gravity": "auto"}]
    td = f"engine:cloudinary,crop:fill,gravity:auto,w:{TARGET_W},h:{TARGET_H},reason:text_heavy"
    out_bytes, used, url = _cloudinary_upload_and_fetch(image_bytes, sku_id, filename, transformation, logger)
    return out_bytes, used, url, td


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
