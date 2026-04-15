"""Justice Canada client -- reads consolidated XML from local clone or HTTP.

Primary mode: read from a local clone of justicecanada/laws-lois-xml.
Fallback mode: download individual XML files via HTTPS.

The local clone is strongly preferred for bootstrap (instant access to all
~11,600 files without HTTP overhead). The HTTP fallback exists for daily
updates when the git clone is not available.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

BASE_URL = "https://laws-lois.justice.gc.ca"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_RPS = 1.0

# Namespace used in Justice Canada XML root attributes.
LIMS_NS = "http://justice.gc.ca/lims"


class JusticeCanadaClient(HttpClient):
    """Client for Justice Canada consolidated legislation XML.

    Reads from a local git clone of justicecanada/laws-lois-xml when
    available; falls back to HTTPS downloads for individual files.
    """

    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        xml_dir: str = "",
        data_dir: str = "",
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
        self._xml_dir = Path(xml_dir) if xml_dir else None
        self._data_dir = Path(data_dir) if data_dir else Path(".")

    @classmethod
    def create(cls, country_config: CountryConfig) -> JusticeCanadaClient:
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", BASE_URL),
            xml_dir=source.get("xml_dir", ""),
            data_dir=country_config.data_dir,
            request_timeout=source.get("request_timeout", DEFAULT_TIMEOUT),
            max_retries=source.get("max_retries", DEFAULT_MAX_RETRIES),
            requests_per_second=source.get("requests_per_second", DEFAULT_RPS),
        )

    # -- LegislativeClient interface ------------------------------------------

    def get_text(self, norm_id: str) -> bytes:
        """Return the full XML for a norm.

        norm_id format: "eng/acts/A-1" or "fra/reglements/SOR-99-129"
        """
        # Try local clone first.
        if self._xml_dir:
            xml_path = self._xml_dir / f"{norm_id}.xml"
            if xml_path.exists():
                return xml_path.read_bytes()

        # Fallback: HTTP download.
        lang, category, file_id = _parse_norm_id(norm_id)
        url = f"{self._base_url}/{lang}/XML/{file_id}.xml"
        logger.info("Downloading %s", url)
        return self._get(url)

    def get_metadata(self, norm_id: str) -> bytes:
        """Same data as get_text -- metadata is embedded in the XML."""
        return self.get_text(norm_id)


def _parse_norm_id(norm_id: str) -> tuple[str, str, str]:
    """Parse 'eng/acts/A-1' into (lang, category, file_id).

    >>> _parse_norm_id("eng/acts/A-1")
    ('eng', 'acts', 'A-1')
    >>> _parse_norm_id("fra/reglements/SOR-99-129")
    ('fra', 'reglements', 'SOR-99-129')
    """
    parts = norm_id.split("/", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid CA norm_id: {norm_id!r} (expected lang/category/id)")
    return parts[0], parts[1], parts[2]
