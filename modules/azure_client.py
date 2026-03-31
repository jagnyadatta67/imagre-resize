"""
azure_client.py — Azure Blob Storage operations.

All blob content is handled in-memory (BytesIO). No files are written to disk.

Responsibilities
----------------
- List source blobs for a given SKU ID
- Download blob bytes into memory
- Upload converted image bytes to lifestyle-newc (flat, no subfolder)
- Detect the correct source container via Unbxd API
"""

from __future__ import annotations

import io
import logging
from typing import Optional
from urllib.parse import urlparse

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings

from config import (
    AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY,
    SOURCE_BLOB_PREFIX, TARGET_CONTAINER,
    FALLBACK_CONTAINER, UNBXD_URL, DOMAIN_TO_CONTAINER,
)

log = logging.getLogger(__name__)


def _connection_string() -> str:
    return (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={AZURE_ACCOUNT_NAME};"
        f"AccountKey={AZURE_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )


def _blob_url(container_name: str, blob_path: str) -> str:
    return (
        f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        f"/{container_name}/{blob_path}"
    )


class AzureClient:
    """
    Thread-safe Azure Blob client. Create once and share across all workers.
    The underlying BlobServiceClient is thread-safe per the Azure SDK docs.
    """

    def __init__(self) -> None:
        self._svc = BlobServiceClient.from_connection_string(_connection_string())

    # ── Internal ──────────────────────────────────────────────

    def _container(self, name: str):
        return self._svc.get_container_client(name)

    # ── Source: list blobs ────────────────────────────────────

    def list_sku_blobs(self, container_name: str, sku_id: str) -> list[str]:
        """
        Return blob names that start with  lifestyle/{sku_id}
        (e.g. lifestyle/1100004294-Multicolour_01.jpg).
        Raises on auth / network errors so the caller can log them.
        """
        prefix = f"{SOURCE_BLOB_PREFIX}{sku_id}"
        blobs  = sorted(
            b.name
            for b in self._container(container_name).list_blobs(name_starts_with=prefix)
        )
        log.debug(f"list_sku_blobs  container={container_name}  sku={sku_id}  found={len(blobs)}")
        return blobs

    # ── Source: download blob into memory ─────────────────────

    def download_blob_bytes(self, container_name: str, blob_name: str) -> bytes:
        """Download blob and return raw bytes. No disk I/O."""
        buf = io.BytesIO()
        self._container(container_name).get_blob_client(blob_name).download_blob().readinto(buf)
        return buf.getvalue()

    # ── Target: upload to lifestyle-converted/ inside the same container ──

    def upload_to_newc(self, filename: str, image_bytes: bytes, container_name: str) -> str:
        """
        Upload image_bytes to  {container_name}/lifestyle-converted/{filename}.
        Same container as the source, output goes into the lifestyle-converted/ folder.
        Returns the full Azure blob URL.
        """
        blob_path = f"{TARGET_CONTAINER}/{filename}"
        self._container(container_name).upload_blob(
            blob_path,
            io.BytesIO(image_bytes),
            overwrite=True,
            content_settings=ContentSettings(content_type="image/jpeg"),
        )
        url = _blob_url(container_name, blob_path)
        log.debug(f"Uploaded → {url}")
        return url

    # ── Unbxd container detection ─────────────────────────────

    def detect_container(self, sku_id: str) -> tuple[str, str]:
        """
        Call Unbxd search API → parse imageUrl domain → Azure container name.

        Returns
        -------
        (container_name, source)
          source is "unbxd" when detected, "fallback" when not found / error.
        """
        _headers = {
            "accept":             "*/*",
            "accept-language":    "en-US,en;q=0.9",
            "content-type":       "application/json",
            "origin":             "https://www.lifestylestores.com",
            "referer":            "https://www.lifestylestores.com/",
            "sec-ch-ua":          '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest":     "empty",
            "sec-fetch-mode":     "cors",
            "sec-fetch-site":     "cross-site",
            "unbxd-user-id":      "uid-9134783168516",
            "user-agent":         (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        }
        try:
            resp = requests.get(
                UNBXD_URL,
                headers=_headers,
                params={"q": sku_id, "facet": "false", "fields": "imageUrl"},
                timeout=8,
            )
            resp.raise_for_status()
            products = resp.json().get("response", {}).get("products", [])

            if products:
                image_urls = products[0].get("imageUrl", [])
                if isinstance(image_urls, str):
                    image_urls = [image_urls]

                for url in image_urls:
                    host = urlparse(url).netloc.lower()
                    for domain, container in DOMAIN_TO_CONTAINER.items():
                        if domain in host:
                            log.debug(f"Unbxd  sku={sku_id}  container={container}")
                            return container, "unbxd"

        except Exception as exc:
            log.warning(f"Unbxd detection failed  sku={sku_id}: {exc}")

        log.debug(f"Fallback  sku={sku_id}  container={FALLBACK_CONTAINER}")
        return FALLBACK_CONTAINER, "fallback"
