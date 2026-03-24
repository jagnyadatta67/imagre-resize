#!/usr/bin/env python
"""
envcheck.py - Quick environment & account checker.

Run:
    python envcheck.py   (works with python 2 or 3 -- auto-switches to python3)

Prints:
  - All .env values
  - Google Vision account (service-account email, project, cred file)
  - Cloudinary account (cloud info via live API ping)
"""

import os
import sys

# -- Auto-switch to python3 / venv if running under Python 2 ---
if sys.version_info[0] < 3:
    _dir = os.path.dirname(os.path.abspath(__file__))
    _venv_py = os.path.join(_dir, "venv", "bin", "python3")
    _py = _venv_py if os.path.isfile(_venv_py) else "python3"
    os.execvp(_py, [_py] + sys.argv)

import json

from dotenv import load_dotenv
load_dotenv()

W = 62  # line width

def header(title):
    print("\n" + "=" * W)
    print("  " + title)
    print("=" * W)

def row(label, value):
    print("  {:<28}  {}".format(label, value))


# ==============================================================
# 1.  ALL .env VALUES
# ==============================================================
header("ENV  (.env)")

ENV_KEYS = [
    # Azure
    ("AZURE_ACCOUNT_NAME",             False),
    ("AZURE_ACCOUNT_KEY",              True),   # masked
    ("SOURCE_BLOB_PREFIX",             False),
    ("TARGET_CONTAINER",               False),
    ("FALLBACK_CONTAINER",             False),
    # Cloudinary
    ("CLOUDINARY_CLOUD_NAME",          False),
    ("CLOUDINARY_API_KEY",             False),
    ("CLOUDINARY_API_SECRET",          True),   # masked
    # MySQL
    ("MYSQL_HOST",                     False),
    ("MYSQL_PORT",                     False),
    ("MYSQL_DB",                       False),
    ("MYSQL_USER",                     False),
    ("MYSQL_PASSWORD",                 True),   # masked
    # Google
    ("GOOGLE_APPLICATION_CREDENTIALS", False),
    # Image / Pipeline
    ("TARGET_W",                       False),
    ("TARGET_H",                       False),
    ("JPEG_QUALITY",                   False),
    ("TEXT_THRESHOLD",                 False),
    ("BULK_WORKERS",                   False),
    ("FULL_AI",                        False),
    ("VERBOSE_LOG",                    False),
]

for key, mask in ENV_KEYS:
    val = os.getenv(key)
    if val is None:
        display = "(not set)"
    elif mask and len(val) > 6:
        display = val[:6] + "***"
    else:
        display = val
    row(key, display)


# ==============================================================
# 2.  GOOGLE VISION ACCOUNT
# ==============================================================
header("GOOGLE VISION  ACCOUNT")

cred_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

# -- Read JSON key file directly (most reliable) ---------------
if cred_file and os.path.isfile(cred_file):
    try:
        with open(cred_file) as f:
            key_data = json.load(f)
        row("Cred file",          cred_file)
        row("Type",               key_data.get("type",           "-"))
        row("Project ID",         key_data.get("project_id",     "-"))
        row("Client Email",       key_data.get("client_email",   "-"))
        row("Client ID",          key_data.get("client_id",      "-"))
        row("Token URI",          key_data.get("token_uri",      "-"))
    except Exception as e:
        row("Cred file",          cred_file)
        row("Error reading file", str(e))
else:
    row("Cred file", cred_file or "(GOOGLE_APPLICATION_CREDENTIALS not set)")

# -- Verify via google.auth.default() --------------------------
try:
    import google.auth
    creds, project = google.auth.default()
    row("Auth project ID",    project or "-")
    if project:
        row("GCP Console URL", "https://console.cloud.google.com/home/dashboard?project=" + project)
    sa_email = (
        getattr(creds, "service_account_email", None)
        or getattr(creds, "_service_account_email", None)
        or "user-account (ADC)"
    )
    row("Auth account",       sa_email)
    row("Auth token valid",   "Yes (credentials loaded OK)")
except Exception as e:
    row("google.auth.default()", "FAILED - " + str(e))

# -- Vision API client ping ------------------------------------
try:
    from google.cloud import vision
    client = vision.ImageAnnotatorClient()
    row("Vision client",      "Created OK")
except Exception as e:
    row("Vision client",      "FAILED - " + str(e))


# ==============================================================
# 3.  CLOUDINARY ACCOUNT
# ==============================================================
header("CLOUDINARY  ACCOUNT")

cloud_name = os.getenv("CLOUDINARY_CLOUD_NAME", "")
api_key    = os.getenv("CLOUDINARY_API_KEY",    "")
api_secret = os.getenv("CLOUDINARY_API_SECRET", "")

row("Cloud Name",  cloud_name or "(not set)")
row("API Key",     api_key    or "(not set)")
row("API Secret",  (api_secret[:6] + "***") if len(api_secret) > 6 else "(not set)")

# -- Live ping: fetch account usage from Cloudinary API --------
try:
    import cloudinary
    import cloudinary.api

    cloudinary.config(
        cloud_name = cloud_name,
        api_key    = api_key,
        api_secret = api_secret,
        secure     = True,
    )
    usage = cloudinary.api.usage()

    row("Plan",         usage.get("plan",         "-"))
    row("Last Updated", usage.get("last_updated", "-"))

    # Transformations
    tr = usage.get("transformations", {})
    if isinstance(tr, dict):
        row("Transformations", "used={}  limit={}  used%={}".format(
            tr.get("usage", 0), tr.get("limit", "-"), tr.get("used_percent", "-")))
    else:
        row("Transformations", str(tr))

    # Storage
    st = usage.get("storage", {})
    if isinstance(st, dict):
        st_gb = round(st.get("usage", 0) / 1e9, 3) if st.get("usage") else 0
        row("Storage", "{} GB  limit={}  used%={}".format(
            st_gb, st.get("limit", "-"), st.get("used_percent", "-")))
    else:
        row("Storage", str(st))

    # Bandwidth
    bw = usage.get("bandwidth", {})
    if isinstance(bw, dict):
        bw_gb = round(bw.get("usage", 0) / 1e9, 3) if bw.get("usage") else 0
        row("Bandwidth", "{} GB  limit={}  used%={}".format(
            bw_gb, bw.get("limit", "-"), bw.get("used_percent", "-")))
    else:
        row("Bandwidth", str(bw))

    # Requests (plain int on Free plan)
    rq = usage.get("requests", {})
    if isinstance(rq, dict):
        row("Requests", "used={}  limit={}  used%={}".format(
            rq.get("usage", 0), rq.get("limit", "-"), rq.get("used_percent", "-")))
    else:
        row("Requests", str(rq))

    # Resources (plain int on Free plan)
    res = usage.get("resources", {})
    if isinstance(res, dict):
        row("Resources", "used={}  limit={}".format(
            res.get("usage", 0), res.get("limit", "-")))
    else:
        row("Resources", str(res))

    row("API Status", "OK")

except Exception as e:
    row("API ping", "FAILED - " + str(e))


# ==============================================================
print("\n" + "=" * W)
print("  Done.")
print("=" * W + "\n")
