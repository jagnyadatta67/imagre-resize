import os
from dotenv import load_dotenv

load_dotenv()

# ── Azure ─────────────────────────────────────────────────────
AZURE_ACCOUNT_NAME  = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY   = os.getenv("AZURE_ACCOUNT_KEY", "")
SOURCE_BLOB_PREFIX  = os.getenv("SOURCE_BLOB_PREFIX", "lifestyle/")
TARGET_CONTAINER    = os.getenv("TARGET_CONTAINER", "lifestyle-newc")
FALLBACK_CONTAINER  = os.getenv("FALLBACK_CONTAINER", "in-media")

# ── Cloudinary ────────────────────────────────────────────────


CLOUDINARY_CLOUD    = os.getenv("CLOUDINARY_CLOUD_NAME", "dhaia5qbl")
CLOUDINARY_KEY      = os.getenv("CLOUDINARY_API_KEY", "123194615629736")
CLOUDINARY_SECRET   = os.getenv("CLOUDINARY_API_SECRET", "KlJF3zzcbbM3tAjeEdEKSkdTsPk")


# ── MySQL ─────────────────────────────────────────────────────
MYSQL_HOST          = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT          = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB            = os.getenv("MYSQL_DB", "image_pipeline")
MYSQL_USER          = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD      = os.getenv("MYSQL_PASSWORD", "root123")

# ── Image output ──────────────────────────────────────────────
TARGET_W            = int(os.getenv("TARGET_W", "1450"))
TARGET_H            = int(os.getenv("TARGET_H", "2100"))
TARGET_RATIO        = TARGET_W / TARGET_H
JPEG_QUALITY        = int(os.getenv("JPEG_QUALITY", "90"))
TEXT_THRESHOLD      = int(os.getenv("TEXT_THRESHOLD", "40"))

# ── Upload users (comma-separated list for manual upload dropdown) ──
UPLOAD_USERS        = [u.strip() for u in os.getenv("UPLOAD_USERS", "").split(",") if u.strip()]

# ── Pipeline behaviour ────────────────────────────────────────
BULK_WORKERS        = int(os.getenv("BULK_WORKERS", "10"))
FULL_AI             = os.getenv("FULL_AI", "false").lower() == "true"
VERBOSE_LOG         = os.getenv("VERBOSE_LOG", "true").lower() == "true"

# ── Unbxd container detection ─────────────────────────────────
UNBXD_URL = (
    "https://search.unbxd.io"
    "/91b7857213222075bbcd3ea2dc72d026"
    "/ss-unbxd-aapac-prod-lifestyle-LandMark48741706891693"
    "/search"
)
DOMAIN_TO_CONTAINER = {
    "media-ea.landmarkshops.in": "in-media-ea",
    "media.landmarkshops.in":    "in-media",
    "media-us.landmarkshops.in": "in-media-us",
    "media-uk.landmarkshops.in": "in-media-uk",
}

# ── Baby Shop brand settings ───────────────────────────────────
BABYSHOP_UNBXD_API_KEY  = os.getenv("BABYSHOP_UNBXD_API_KEY",  "04c81a1a0317cf9ec15eeb77ae6dcb43")
BABYSHOP_UNBXD_SITE_KEY = os.getenv("BABYSHOP_UNBXD_SITE_KEY", "ss-unbxd-aapac-prod-babyshop-LandMark48741720534433")
BABYSHOP_SOURCE_PREFIX  = os.getenv("BABYSHOP_SOURCE_PREFIX",  "babyshopstores/")
BABYSHOP_TARGET_FOLDER  = os.getenv("BABYSHOP_TARGET_FOLDER",  "babyshopstores-new")
