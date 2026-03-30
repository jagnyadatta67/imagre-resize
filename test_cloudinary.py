"""
test_cloudinary.py
──────────────────
Build and print a Cloudinary fetch URL from a source image URL + transformation string.
No upload required — uses Cloudinary's free fetch delivery.

Usage:
    python test_cloudinary.py \
        --url "https://media.landmarkshops.in/max-new/sku_01-2100.jpg" \
        --transform "w:1450,h:1450,crop:fit"

Supports natural colon format (w:1450,crop:fit) — auto-converted to Cloudinary format (w_1450,c_fit).
"""

import argparse
import sys
from urllib.parse import quote
from config import CLOUDINARY_CLOUD

# ── Key aliases (colon format → Cloudinary prefix) ────────────────────────────
_KEY_MAP = {
    "w":          "w",
    "width":      "w",
    "h":          "h",
    "height":     "h",
    "crop":       "c",
    "gravity":    "g",
    "background": "b",
    "bg":         "b",
    "quality":    "q",
    "format":     "f",
    "radius":     "r",
    "angle":      "a",
    "effect":     "e",
    "overlay":    "l",
    "underlay":   "u",
    "opacity":    "o",
    "zoom":       "z",
    "x":          "x",
    "y":          "y",
}


def _convert_transform(raw: str) -> str:
    """
    Convert natural colon format to Cloudinary underscore format.
    e.g. "w:1450,h:1450,crop:fit,gravity:auto" → "w_1450,h_1450,c_fit,g_auto"

    Parts that are already in underscore format are passed through unchanged.
    """
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    converted = []

    for part in parts:
        if ":" in part:
            key, _, val = part.partition(":")
            key = key.strip().lower()
            val = val.strip()
            prefix = _KEY_MAP.get(key, key)   # fallback: use key as-is
            converted.append(f"{prefix}_{val}")
        else:
            # Already in underscore format (e.g. w_1450) — pass through
            converted.append(part)

    return ",".join(converted)


def build_fetch_url(cloud: str, transform: str, source_url: str) -> str:
    """Build Cloudinary fetch delivery URL."""
    encoded = quote(source_url, safe="")
    return f"https://res.cloudinary.com/{cloud}/image/fetch/{transform}/{encoded}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a Cloudinary fetch URL with a custom transformation."
    )
    parser.add_argument("--url",       required=True,  help="Source image URL")
    parser.add_argument("--transform", required=True,  help="Transformation string (colon or underscore format)")
    parser.add_argument("--cloud",     default=CLOUDINARY_CLOUD, help="Cloudinary cloud name (default from config)")
    args = parser.parse_args()

    raw_transform   = args.transform
    clean_transform = _convert_transform(raw_transform)
    fetch_url       = build_fetch_url(args.cloud, clean_transform, args.url)

    print()
    print(f"  Source     : {args.url}")
    print(f"  Raw        : {raw_transform}")
    print(f"  Converted  : {clean_transform}")
    print(f"  Cloud      : {args.cloud}")
    print()
    print(f"  Result URL :")
    print(f"  {fetch_url}")
    print()

    # Try to copy to clipboard (best-effort, no crash if unavailable)
    try:
        import subprocess
        subprocess.run(["pbcopy"], input=fetch_url.encode(), check=True)
        print("  ✅ URL copied to clipboard")
    except Exception:
        pass

    print()


if __name__ == "__main__":
    main()
