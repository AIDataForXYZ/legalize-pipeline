"""Germany gesetze-im-internet.de HTTP client.

Data source: https://www.gesetze-im-internet.de/
Operator: BMJ (Bundesministerium der Justiz) via juris GmbH
Format: ZIP containing gii-norm XML (DTD v1.01)
License: Public domain (official federal law publications)
"""

from __future__ import annotations

import io
import logging
import time
import zipfile
from typing import TYPE_CHECKING

import requests

from legalize.fetcher.base import LegislativeClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

GII_BASE = "https://www.gesetze-im-internet.de"
GII_TOC = f"{GII_BASE}/gii-toc.xml"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_RPS = 2.0


class GIIClient(LegislativeClient):
    """HTTP client for gesetze-im-internet.de.

    Each law is a ZIP file containing a single gii-norm XML document.
    The TOC XML lists all ~6900 federal laws with their ZIP URLs.
    norm_id is the URL slug (e.g., "gg", "bgb", "stgb").
    """

    @classmethod
    def create(cls, country_config: CountryConfig) -> GIIClient:
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", GII_BASE),
            timeout=source.get("request_timeout", DEFAULT_TIMEOUT),
            max_retries=source.get("max_retries", DEFAULT_MAX_RETRIES),
            requests_per_second=source.get("requests_per_second", DEFAULT_RPS),
        )

    def __init__(
        self,
        base_url: str = GII_BASE,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        requests_per_second: float = DEFAULT_RPS,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._min_interval = 1.0 / requests_per_second
        self._last_request: float = 0
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize)",
            }
        )

    def get_text(self, norm_id: str) -> bytes:
        """Download and extract the XML for a law.

        Args:
            norm_id: URL slug (e.g., "gg", "bgb", "stgb")

        Returns:
            Raw gii-norm XML bytes.
        """
        url = f"{self._base_url}/{norm_id}/xml.zip"
        zip_bytes = self._get(url)
        return self._extract_xml(zip_bytes, norm_id)

    def get_metadata(self, norm_id: str) -> bytes:
        """Metadata is embedded in the XML, so this returns the same XML."""
        return self.get_text(norm_id)

    def get_toc(self) -> bytes:
        """Fetch the full TOC XML listing all laws."""
        return self._get(GII_TOC)

    def close(self) -> None:
        self._session.close()

    # -- Internal helpers --

    def _get(self, url: str) -> bytes:
        """GET with rate limiting and retry."""
        now = time.monotonic()
        wait = self._min_interval - (now - self._last_request)
        if wait > 0:
            time.sleep(wait)

        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                r = self._session.get(url, timeout=self._timeout)
                self._last_request = time.monotonic()

                if r.status_code == 429 or r.status_code >= 500:
                    delay = 2**attempt
                    logger.warning("GII %d on %s, retrying in %ds", r.status_code, url, delay)
                    time.sleep(delay)
                    continue

                r.raise_for_status()
                return r.content

            except requests.RequestException as exc:
                last_exc = exc
                delay = 2**attempt
                logger.warning("GII request error: %s, retry in %ds", exc, delay)
                time.sleep(delay)

        raise last_exc or RuntimeError(f"Failed to fetch {url}")

    @staticmethod
    def _extract_xml(zip_bytes: bytes, norm_id: str) -> bytes:
        """Extract the single XML file from a GII ZIP archive."""
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            xml_files = [n for n in zf.namelist() if n.endswith(".xml")]
            if not xml_files:
                raise ValueError(f"No XML file in ZIP for {norm_id}")
            return zf.read(xml_files[0])
