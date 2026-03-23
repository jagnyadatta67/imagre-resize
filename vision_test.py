"""
Usage:
    python3 vision_test.py <image_path>
    python3 vision_test.py 1000016068163-1000016068162_08-2100.jpg
"""

import sys
from google.cloud import vision


def run_vision(image_path: str):
    with open(image_path, "rb") as f:
        content = f.read()

    client = vision.ImageAnnotatorClient()
    resp = client.annotate_image({
        "image": vision.Image(content=content),
        "features": [
            {"type_": vision.Feature.Type.LABEL_DETECTION,      "max_results": 20},
            {"type_": vision.Feature.Type.OBJECT_LOCALIZATION,  "max_results": 20},
            {"type_": vision.Feature.Type.TEXT_DETECTION},
            {"type_": vision.Feature.Type.SAFE_SEARCH_DETECTION},
            {"type_": vision.Feature.Type.IMAGE_PROPERTIES},
            {"type_": vision.Feature.Type.WEB_DETECTION},
            {"type_": vision.Feature.Type.CROP_HINTS},
        ],
    })

    print("=" * 60)
    print("LABELS")
    print("=" * 60)
    for l in resp.label_annotations:
        print(f"  {l.description:<30}  {l.score:.2f}")

    print("\n" + "=" * 60)
    print("OBJECTS")
    print("=" * 60)
    if resp.localized_object_annotations:
        for o in resp.localized_object_annotations:
            verts = [(round(v.x, 3), round(v.y, 3)) for v in o.bounding_poly.normalized_vertices]
            print(f"  {o.name:<25}  {o.score:.2f}  bbox={verts}")
    else:
        print("  None detected")

    print("\n" + "=" * 60)
    print("TEXT")
    print("=" * 60)
    if resp.text_annotations:
        print("Text ",repr(resp.text_annotations[0].description[:300]))
    else:
        print("  None detected")

    print("\n" + "=" * 60)
    print("SAFE SEARCH")
    print("=" * 60)
    ss = resp.safe_search_annotation
    print(f"  adult={ss.adult.name}  racy={ss.racy.name}  violence={ss.violence.name}")

    print("\n" + "=" * 60)
    print("DOMINANT COLORS")
    print("=" * 60)
    for c in resp.image_properties_annotation.dominant_colors.colors[:6]:
        rgb = c.color
        print(f"  RGB({int(rgb.red):3},{int(rgb.green):3},{int(rgb.blue):3})  score={c.score:.3f}  fraction={c.pixel_fraction:.3f}")

    print("\n" + "=" * 60)
    print("CROP HINTS")
    print("=" * 60)
    for h in resp.crop_hints_annotation.crop_hints:
        verts = [(v.x, v.y) for v in h.bounding_poly.vertices]
        print(f"  confidence={h.confidence:.3f}  bbox={verts}")

    print("\n" + "=" * 60)
    print("WEB DETECTION")
    print("=" * 60)
    web = resp.web_detection
    print(f"  Best guess: {[e.label for e in web.best_guess_labels]}")
    for e in web.web_entities[:8]:
        print(f"  {e.description:<35}  score={e.score:.3f}")

    print("\nDone.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 vision_test.py <image_path>")
        sys.exit(1)
    run_vision(sys.argv[1])
