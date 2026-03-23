import os
import re
import json
import time
from PIL import Image
from google.cloud import vision

import base64
import io
import cloudinary
import cloudinary.uploader

# ============================================================
# CONFIG
# ============================================================

PROJECT_ID = "image-poc-488002"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-flash"
#MODEL_NAME = "imagen-4.0-fast-generate-001"

INPUT_ROOT = "styles"
OUTPUT_ROOT = "output"
DOWNLOAD_ENABLED = True   # or FalsePROJECT_ID
FULL_AI = False   # or True


TARGET_W = 1450
TARGET_H = 2100
TARGET_RATIO = TARGET_W / TARGET_H
FINAL_DPI = (72, 72)
vision_client = vision.ImageAnnotatorClient()

# --- Cloudinary ---
cloudinary.config(
    cloud_name = "doslxvsej",
    api_key = "965752784145569",
    api_secret = "347dE1L37vR3KUyW03_9sTkk-9s",
    secure = True
)

# =====================================================
# TEXT DETECTION (INFOGRAPHIC CHECK)
# =====================================================

def _is_text_heavy(image_path, text_threshold=40):

    with open(image_path, "rb") as f:
        content = f.read()

    image = vision.Image(content=content)
    response = vision_client.text_detection(image=image)

    texts = response.text_annotations

    if not texts:
        return False

    full_text = texts[0].description
    char_count = len(full_text.strip())

    return char_count > text_threshold


# =====================================================
# CENTER CROP
# =====================================================

def center_crop(img):

    w, h = img.size

    if w / h > TARGET_RATIO:
        new_w = int(h * TARGET_RATIO)
        left = (w - new_w) // 2
        return img.crop((left, 0, left + new_w, h))
    else:
        new_h = int(w / TARGET_RATIO)
        top = (h - new_h) // 2
        return img.crop((0, top, w, top + new_h))


# =====================================================
# PAD FOR INFOGRAPHIC
# =====================================================

import numpy as np
from PIL import Image

def get_bbox_area(obj):
    vertices = obj.bounding_poly.normalized_vertices

    xs = [v.x for v in vertices if v.x is not None]
    ys = [v.y for v in vertices if v.y is not None]

    if not xs or not ys:
        return 0

    width = max(xs) - min(xs)
    height = max(ys) - min(ys)

    return width * height


def detect_edge_background(resized_img, strip_size=20):
    np_img = np.array(resized_img)

    edges = np.concatenate([
        np_img[:strip_size,:,:].reshape(-1,3),
        np_img[-strip_size:,:,:].reshape(-1,3),
        np_img[:,:strip_size,:].reshape(-1,3),
        np_img[:,-strip_size:,:].reshape(-1,3),
    ])

    brightness = np.mean(edges, axis=1)
    low = np.percentile(brightness, 10)
    high = np.percentile(brightness, 90)

    filtered = edges[(brightness >= low) & (brightness <= high)]
    return tuple(np.median(filtered, axis=0).astype(int))


def process_size_guide(image_path, safety_margin=0.97):
    """
    safety_margin:
        1.00 = max zoom (may crop)
        0.98 = slight zoom reduction
        0.95 = safer
    """

    img = Image.open(image_path).convert("RGB")
    img_w, img_h = img.size

    scale_h = TARGET_H / img_h
    scale_w = TARGET_W / img_w

    # 🔥 choose safe scale
    scale = min(scale_h, scale_w) * safety_margin

    new_w = int(img_w * scale)
    new_h = int(img_h * scale)

    resized = img.resize((new_w, new_h), Image.LANCZOS)

    bg_color = detect_edge_background(resized)

    canvas = Image.new("RGB", (TARGET_W, TARGET_H), bg_color)

    left = (TARGET_W - new_w) // 2
    top = (TARGET_H - new_h) // 2

    canvas.paste(resized, (left, top))

    return canvas

# -------------------------------------------------
# MERGE SELECTED OBJECTS
# -------------------------------------------------

def merge_bboxes(objs):
    x_min = min(obj.bounding_poly.normalized_vertices[0].x for obj in objs)
    y_min = min(obj.bounding_poly.normalized_vertices[0].y for obj in objs)
    x_max = max(obj.bounding_poly.normalized_vertices[2].x for obj in objs)
    y_max = max(obj.bounding_poly.normalized_vertices[2].y for obj in objs)

    return x_min, y_min, x_max, y_max


# -------------------------------------------------
# DOWNLOAD IMAGES FROM CLOUDINARY
# -------------------------------------------------

import os
import requests

def download_image(image_url, style_name, OUTPUT_ROOT):

    try:
        # Build path → OUTPUT_ROOT/style_name
        download_folder = os.path.join(OUTPUT_ROOT, style_name)
        os.makedirs(download_folder, exist_ok=True)

        # Extract clean filename (remove query params)
        filename = image_url.split("/")[-1].split("?")[0]

        save_path = os.path.join(download_folder, filename)

        print(f"   ⬇ Downloading (overwrite enabled): {filename}")

        response = requests.get(image_url, stream=True)
        response.raise_for_status()

        # Always overwrite
        with open(save_path, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)

        print(f"   ✅ Saved to: {save_path}")

        return save_path

    except Exception as e:
        print(f"   ❌ Download failed: {str(e)}")
        return None


# ============================================================
# AI Reframing using cloudinary
# ============================================================

def smart_reframe_ai(image_path, style_name, filename, bbox_data):

    # -------------------------------------------------
    # 1️⃣ If detection failed → SAFE MODE (no AI)
    # -------------------------------------------------
    if not bbox_data:
        print("   ⚠ No BBOX → Using FILL (Safe Mode)")
        transformation = [
            {
                "width": TARGET_W,
                "height": TARGET_H,
                "crop": "fill",
                "gravity": "auto"
            }
        ]

    else:
        x_min_norm, y_min_norm, x_max_norm, y_max_norm = bbox_data

        # Head cut detection
        top_touch = y_min_norm < 0.05

        # -------------------------------------------------
        # 2️⃣ Tight crop → No AI
        # -------------------------------------------------
        if top_touch:
            print("   🎯 Tight crop detected → Using FILL (No AI)")
            transformation = [
                {
                    "width": TARGET_W,
                    "height": TARGET_H,
                    "crop": "fill",
                    "gravity": "auto"
                }
            ]

        # -------------------------------------------------
        # 3️⃣ Full-body overflow → AI Extend
        # -------------------------------------------------
        else:
            print("   🤖 Full-body overflow → Using AI Extend")
            transformation = [
                {
                    "width": TARGET_W,
                    "height": TARGET_H,
                    "crop": "pad",
                    "gravity": "center",
                    "background": "gen_fill"
                }
            ]

    upload_result = cloudinary.uploader.upload(
        image_path,
        folder=style_name,
        public_id=os.path.splitext(filename)[0],
        overwrite=True,
        unique_filename=False,
        transformation=transformation
    )

    return upload_result["secure_url"]
    

# =====================================================
# SMART CROP (PRODUCT)
# =====================================================

def detect_main_object_bbox(image_path):

    print(f"\n🔍 Vision Detection: {image_path}")

    with open(image_path, "rb") as f:
        content = f.read()

    image = vision.Image(content=content)
    response = vision_client.object_localization(image=image)
    objects = response.localized_object_annotations

    if not objects:
        print("⚠ No objects detected")
        return None

    print("   🧠 Detected objects:", [obj.name for obj in objects])

    def bbox_area(obj):
        v = obj.bounding_poly.normalized_vertices
        return (
            (v[2].x - v[0].x) *
            (v[2].y - v[0].y)
        )

    # Print object details
    for obj in objects:
        print(
            f"   ➜ {obj.name} | "
            f"Score: {round(obj.score,3)} | "
            f"Area: {round(bbox_area(obj),4)}"
        )

    # -------------------------------------------------
    # PRIORITY 1️⃣ Person
    # -------------------------------------------------
    person_objs = [obj for obj in objects if obj.name == "Person"]

    if person_objs:
        print("   👤 Person detected")
        target_objs = person_objs
        label = "Person"

    # -------------------------------------------------
    # PRIORITY 2️⃣ Footwear (merge all)
    # -------------------------------------------------
    else:
        shoe_objs = [
            obj for obj in objects
            if obj.name in ["Shoe", "Sandal", "Footwear"]
        ]

        if shoe_objs:
            print(f"   👟 Found {len(shoe_objs)} footwear items → merging")
            target_objs = shoe_objs
            label = "Footwear"

        # -------------------------------------------------
        # PRIORITY 3️⃣ Clothing
        # -------------------------------------------------
        else:
            priority_labels = ["Dress", "Clothing", "Apparel", "Top", "Shirt"]
            clothing_objs = [
                obj for obj in objects
                if obj.name in priority_labels
            ]

            if clothing_objs:
                print("   👕 Clothing detected")
                target_objs = clothing_objs
                label = "Clothing"
            else:
                print("   ⚠ Fallback to largest object")
                target_objs = [max(objects, key=bbox_area)]
                label = "Fallback"

    x_min, y_min, x_max, y_max = merge_bboxes(target_objs)

    print(f"   🎯 Selected label: {label}")
    print(f"   📦 Merged BBOX: ({x_min:.3f}, {y_min:.3f}) → ({x_max:.3f}, {y_max:.3f})")

    return x_min, y_min, x_max, y_max


def smart_crop_or_ai(image_path, style_name,filename):

    img = Image.open(image_path)
    img_w, img_h = img.size

    result = detect_main_object_bbox(image_path)

    if FULL_AI:
        return smart_reframe_ai(image_path, style_name,filename, result)

    if not result:
        print("   ⚠ No detection → center crop")
        return center_crop(img)

    x_min_norm, y_min_norm, x_max_norm, y_max_norm = result

    x_min = int(x_min_norm * img_w)
    x_max = int(x_max_norm * img_w)

    bbox_width = x_max - x_min
    target_width = int(img_h * TARGET_RATIO)

    print(f"   📐 BBOX Width: {bbox_width}")
    print(f"   🎯 Target Width: {target_width}")

    # 🔥 AI trigger
    if bbox_width > target_width:
        print("   🤖 BBOX wider than target → Using AI Reframe")
        return smart_reframe_ai(image_path, style_name,filename, result)

    # Otherwise safe crop
    object_center_x = (x_min + x_max) // 2
    left = object_center_x - target_width // 2
    left = max(0, min(left, img_w - target_width))
    right = left + target_width

    print(f"   ✂ Crop Window: ({left}, 0) → ({right}, {img_h})")

    return img.crop((left, 0, right, img_h))


# =====================================================
# MAIN ENGINE
# =====================================================

def run_migration(input_root, output_root):

    for root, _, files in os.walk(input_root):

        images = [f for f in files if f.lower().endswith((".jpg", ".jpeg", ".png"))]
        if not images:
            continue

        images.sort()

        style_name = os.path.basename(root)
        output_folder = os.path.join(output_root, style_name)
        os.makedirs(output_folder, exist_ok=True)

        last_image = images[-1]

        print(f"\n📂 STYLE: {style_name}")

        for filename in images:

            input_path = os.path.join(root, filename)
            output_path = os.path.join(
                output_folder,
                os.path.splitext(filename)[0] + ".jpg"
            )

            print(f"\n   🖼 Processing: {filename}")

            # 🔍 Only override if last image AND infographic
            if filename == last_image and _is_text_heavy(input_path):
                print("   📊 INFOGRAPHIC → PAD")
                result = process_size_guide(input_path)
            else:
                print("   👕 PRODUCT → SMART CROP")
                result = smart_crop_or_ai(input_path, style_name,filename)

            # If result is a URL (Cloudinary case)
            if isinstance(result, str):
                if result and DOWNLOAD_ENABLED:
                    download_image(result, style_name, OUTPUT_ROOT)
                else:
                    print("   ☁ Cloudinary image used → skipping local save")
                continue

            result = result.resize((TARGET_W, TARGET_H), Image.LANCZOS)

            if result.mode != "RGB":
                result = result.convert("RGB")

            result.save(
                output_path,
                format="JPEG",
                quality=90,
                dpi=FINAL_DPI,
                optimize=True,
                progressive=True
            )

            print("   ✅ Saved")


# =====================================================
# RUN
# =====================================================

if __name__ == "__main__":
    run_migration(INPUT_ROOT, OUTPUT_ROOT)