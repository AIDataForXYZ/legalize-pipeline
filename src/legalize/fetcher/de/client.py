"""Germany gesetze-im-internet.de HTTP client.

Data source: https://www.gesetze-im-internet.de/
Operator: BMJ (Bundesministerium der Justiz) via juris GmbH
Format: ZIP containing gii-norm XML (DTD v1.01)
License: Public domain (official federal law publications)
"""

from __future__ import annotations

import io
import logging
import zipfile
from typing import TYPE_CHECKING

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

GII_BASE = "https://www.gesetze-im-internet.de"
GII_TOC = f"{GII_BASE}/gii-toc.xml"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_RPS = 2.0


class GIIClient(HttpClient):
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
            request_timeout=source.get("request_timeout", DEFAULT_TIMEOUT),
            max_retries=source.get("max_retries", DEFAULT_MAX_RETRIES),
            requests_per_second=source.get("requests_per_second", DEFAULT_RPS),
        )

    def __init__(
        self,
        base_url: str = GII_BASE,
        request_timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        requests_per_second: float = DEFAULT_RPS,
    ) -> None:
        super().__init__(
            base_url=base_url,
            request_timeout=request_timeout,
            max_retries=max_retries,
            requests_per_second=requests_per_second,
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

    def head_zip(self, norm_id: str) -> dict[str, str]:
        """HEAD request for a law ZIP to check Last-Modified / ETag."""
        url = f"{self._base_url}/{norm_id}/xml.zip"
        resp = self._request("HEAD", url)
        return dict(resp.headers)

    @staticmethod
    def _extract_xml(zip_bytes: bytes, norm_id: str) -> bytes:
        """Extract the single XML file from a GII ZIP archive."""
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            xml_files = [n for n in zf.namelist() if n.endswith(".xml")]
            if not xml_files:
                raise ValueError(f"No XML file in ZIP for {norm_id}")
            return zf.read(xml_files[0])
