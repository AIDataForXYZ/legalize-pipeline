"""Romanian legislative client (legislatie.just.ro).

The Portal Legislativ has a SOAP API for discovery and HTML pages for content.
Each law is identified by a numeric document ID (e.g., 47355 for the Constitution).

Content pages:
    /Public/DetaliiDocumentAfis/{ID}  -- consolidated text (structured HTML)
    /Public/DetaliiDocument/{ID}      -- detail page (metadata + version history)

The site requires a browser-like User-Agent header (403 without it).
Content is UTF-8 encoded.

Historical versions: each consolidated version has its own document ID,
listed in the "istoric consolidări" section of the detail page. This is the
same "archived-version URLs" pattern used by Belgium (be/).
"""

from __future__ import annotations

import base64
import json
import logging
import re
from datetime import date
from typing import TYPE_CHECKING

from lxml import html as lxml_html

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://legislatie.just.ro"

_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Regex to extract document IDs from internal links.
_DOC_ID_RE = re.compile(r"DetaliiDocument(?:Afis)?/(\d+)")

# Romanian date format in version history: DD.MM.YYYY
_RO_DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

# Romanian month names for parsing dates from metadata text.
_RO_MONTHS = {
    "ianuarie": 1,
    "februarie": 2,
    "martie": 3,
    "aprilie": 4,
    "mai": 5,
    "iunie": 6,
    "iulie": 7,
    "august": 8,
    "septembrie": 9,
    "octombrie": 10,
    "noiembrie": 11,
    "decembrie": 12,
}

_META_DATE_RE = re.compile(
    r"nr\.\s*\d+\s+din\s+(\d{1,2})\s+(\w+)\s+(\d{4})",
)


def _parse_ro_date(text: str) -> date | None:
    """Parse DD.MM.YYYY date from Romanian text."""
    m = _RO_DATE_RE.search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _parse_pub_date_text(text: str) -> date | None:
    """Parse a publication date from text like 'nr. 767 din 31 octombrie 2003'."""
    m = _META_DATE_RE.search(text)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = _RO_MONTHS.get(month_name)
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass
    return None


class RoClient(HttpClient):
    """HTTP client for Romanian consolidated legislation via HTML scraping.

    Single-entry cache avoids redundant fetches when the pipeline calls
    get_metadata() and get_text() for the same norm.
    """

    @classmethod
    def create(cls, country_config: CountryConfig) -> RoClient:
        """Create RoClient from CountryConfig."""
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", DEFAULT_BASE_URL),
            request_timeout=source.get("request_timeout", 60),
            max_retries=source.get("max_retries", 5),
            requests_per_second=source.get("requests_per_second", 2.0),
            extra_headers={"User-Agent": _BROWSER_UA},
        )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._cached_url: str | None = None
        self._cached_bytes: bytes | None = None

    def _cached_get(self, url: str) -> bytes:
        """GET with single-entry cache."""
        if self._cached_url == url and self._cached_bytes is not None:
            return self._cached_bytes
        data = self._get(url)
        self._cached_url = url
        self._cached_bytes = data
        return data

    # ----- URL helpers -----

    def text_url(self, doc_id: str) -> str:
        """URL for the consolidated text page."""
        return f"{self._base_url}/Public/DetaliiDocumentAfis/{doc_id}"

    def detail_url(self, doc_id: str) -> str:
        """URL for the detail page (metadata + version history)."""
        return f"{self._base_url}/Public/DetaliiDocument/{doc_id}"

    # ----- LegislativeClient contract -----

    def get_text(self, norm_id: str) -> bytes:
        """Fetch the consolidated text HTML page.

        norm_id is the numeric document ID as a string (e.g., "47355").
        Returns raw bytes (UTF-8).
        """
        return self._cached_get(self.text_url(norm_id))

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch the detail page (metadata + version history).

        Returns raw bytes (UTF-8). The detail page contains the version
        history in the 'forme_act' section, plus metadata fields.
        """
        return self._cached_get(self.detail_url(norm_id))

    # ----- Historical version walking -----

    def get_suvestine(self, norm_id: str) -> bytes:
        """Fetch all historical versions of a law and return as JSON blob.

        Walks the "istoric consolidări" on the detail page to find all
        consolidated version IDs and dates. Then fetches each version's
        text page. Returns a JSON-encoded bytes object with the structure:

            {
                "base_id": "798",
                "versions": [
                    {
                        "version_id": "221405",
                        "date": "1991-06-04",
                        "text_b64": "<base64-encoded HTML>"
                    },
                    ...
                ]
            }

        Versions are ordered oldest-first (chronological).
        """
        detail_html = self._cached_get(self.detail_url(norm_id))
        versions = self._extract_version_list(detail_html)

        if not versions:
            # No version history -- return just the current version.
            current_text = self._cached_get(self.text_url(norm_id))
            pub_date = self._extract_pub_date_from_detail(detail_html)
            return json.dumps(
                {
                    "base_id": norm_id,
                    "versions": [
                        {
                            "version_id": norm_id,
                            "date": pub_date.isoformat() if pub_date else "1970-01-02",
                            "text_b64": base64.b64encode(current_text).decode("ascii"),
                        }
                    ],
                }
            ).encode("utf-8")

        # Fetch each version's text page.
        version_entries = []
        for v_date, v_id in versions:
            try:
                text_data = self._get(self.text_url(v_id))
                version_entries.append(
                    {
                        "version_id": v_id,
                        "date": v_date.isoformat(),
                        "text_b64": base64.b64encode(text_data).decode("ascii"),
                    }
                )
            except Exception:
                logger.warning(
                    "Failed to fetch version %s (date %s) for norm %s",
                    v_id,
                    v_date,
                    norm_id,
                )
                continue

        # Add the current consolidated version (the detail page's own ID)
        # if it's not already in the list.
        known_ids = {v["version_id"] for v in version_entries}
        if norm_id not in known_ids:
            current_text = self._cached_get(self.text_url(norm_id))
            pub_date = self._extract_pub_date_from_detail(detail_html)
            if pub_date:
                version_entries.append(
                    {
                        "version_id": norm_id,
                        "date": pub_date.isoformat(),
                        "text_b64": base64.b64encode(current_text).decode("ascii"),
                    }
                )

        # Sort oldest-first.
        version_entries.sort(key=lambda v: v["date"])

        return json.dumps(
            {
                "base_id": norm_id,
                "versions": version_entries,
            }
        ).encode("utf-8")

    def _extract_version_list(self, detail_html: bytes) -> list[tuple[date, str]]:
        """Extract (date, version_doc_id) pairs from the detail page.

        Parses the 'forme_act' section for consolidation history links.
        Returns list sorted oldest-first.
        """
        tree = lxml_html.fromstring(detail_html)
        versions: list[tuple[date, str]] = []
        seen_ids: set[str] = set()

        for el in tree.xpath('//*[contains(@class, "forme_act")]'):
            for link in el.xpath('.//a[contains(@href, "DetaliiDocument")]'):
                href = link.get("href", "")
                text = link.text_content().strip()
                m = _DOC_ID_RE.search(href)
                if not m:
                    continue
                doc_id = m.group(1)
                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)

                v_date = _parse_ro_date(text)
                if v_date is None:
                    # "Republicarea N" entries -- skip, they are republications
                    # without a specific date in the link text.
                    continue
                versions.append((v_date, doc_id))

        versions.sort(key=lambda v: v[0])
        return versions

    def _extract_pub_date_from_detail(self, detail_html: bytes) -> date | None:
        """Extract publication date from the detail page metadata."""
        tree = lxml_html.fromstring(detail_html)
        pub = tree.xpath('//span[@class="S_PUB_BDY"]')
        if pub:
            return _parse_pub_date_text(pub[0].text_content())
        # Fallback: parse from title (S_DEN).
        den = tree.xpath('//span[@class="S_DEN"]')
        if den:
            return _parse_pub_date_text(den[0].text_content())
        return None
