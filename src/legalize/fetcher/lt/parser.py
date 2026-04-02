"""Parser for Lithuanian TAR metadata (JSON) and e-tar.lt text (HTML).

Metadata comes from data.gov.lt Spinta API as JSON.
Full text comes from e-tar.lt as HTML (consolidated versions).
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import (
    Block,
    NormMetadata,
    NormStatus,
    Paragraph,
    Rank,
    Version,
)

# Map Lithuanian act type names (rusis) to rank strings
RUSIS_TO_RANK: dict[str, str] = {
    "Konstitucija": "konstitucija",
    "Konstitucinis įstatymas": "konstitucinis_istatymas",
    "Įstatymas": "istatymas",
    "Kodeksas": "istatymas",
    "Vyriausybės nutarimas": "vyriausybes_nutarimas",
    "Prezidento dekretas": "prezidento_dekretas",
    "Ministro įsakymas": "ministro_isakymas",
    "Savivaldybės sprendimas": "savivaldybes_sprendimas",
    "Nutarimas": "nutarimas",
    "Įsakymas": "isakymas",
    "Sprendimas": "sprendimas",
}

# Map Lithuanian status values to NormStatus
STATUS_MAP: dict[str, NormStatus] = {
    "Galiojantis": NormStatus.IN_FORCE,
    "galiojantis": NormStatus.IN_FORCE,
    "Negaliojantis": NormStatus.REPEALED,
    "negaliojantis": NormStatus.REPEALED,
}

# Lithuanian structural element patterns for HTML parsing
_ARTICLE_RE = re.compile(r"(?P<num>\d+)\s*straipsnis\b", re.IGNORECASE)
_CHAPTER_RE = re.compile(r"(?P<num>[IVXLCDM]+)\s*(?:skyrius|SKYRIUS)\b", re.IGNORECASE)
_SECTION_RE = re.compile(r"(?P<num>[IVXLCDM]+|\d+)\s*(?:skirsnis|SKIRSNIS)\b", re.IGNORECASE)
_PART_RE = re.compile(r"(?P<num>[IVXLCDM]+|\d+)\s*(?:dalis|DALIS)\b", re.IGNORECASE)


def _parse_date(s: str | None) -> date | None:
    """Parse ISO date string (YYYY-MM-DD)."""
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, IndexError):
        return None


def _strip_html(s: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", s).strip()


def _html_to_paragraphs(html: str) -> list[Paragraph]:
    """Convert HTML text into a list of Paragraph objects.

    Splits on block-level tags (<p>, <div>, <br>, headings) and
    classifies each paragraph based on Lithuanian structural patterns.
    """
    paragraphs: list[Paragraph] = []

    # Split on block-level elements
    chunks = re.split(r"<(?:p|div|br|h[1-6])[^>]*>", html, flags=re.IGNORECASE)

    for chunk in chunks:
        text = _strip_html(chunk).strip()
        if not text:
            continue

        # Classify the paragraph
        css_class = "text"
        if _ARTICLE_RE.search(text):
            css_class = "article_heading"
        elif _CHAPTER_RE.search(text):
            css_class = "chapter_heading"
        elif _SECTION_RE.search(text):
            css_class = "section_heading"
        elif _PART_RE.search(text):
            css_class = "part_heading"

        paragraphs.append(Paragraph(css_class=css_class, text=text))

    return paragraphs


class TARTextParser(TextParser):
    """Parses HTML from e-tar.lt into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse e-tar.lt HTML into Block objects.

        Groups consecutive paragraphs under article headings into blocks.
        If no article structure is detected, returns a single block with
        all content.
        """
        html = data.decode("utf-8", errors="replace")
        paragraphs = _html_to_paragraphs(html)

        if not paragraphs:
            return []

        blocks: list[Block] = []
        current_id = "full"
        current_title = "Full text"
        current_paragraphs: list[Paragraph] = []
        block_index = 0

        for para in paragraphs:
            if para.css_class == "article_heading":
                # Save previous block if it has content
                if current_paragraphs:
                    blocks.append(self._make_block(current_id, current_title, current_paragraphs))
                # Start new block
                match = _ARTICLE_RE.search(para.text)
                num = match.group("num") if match else str(block_index)
                current_id = f"str{num}"
                current_title = para.text
                current_paragraphs = [para]
                block_index += 1
            else:
                current_paragraphs.append(para)

        # Save last block
        if current_paragraphs:
            blocks.append(self._make_block(current_id, current_title, current_paragraphs))

        return blocks

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform timeline from e-tar.lt HTML.

        e-TAR provides consolidated text; reform history requires
        cross-referencing amendment metadata. Returns empty list for now.
        """
        return []

    @staticmethod
    def _make_block(block_id: str, title: str, paragraphs: list[Paragraph]) -> Block:
        """Create a Block with a single version from paragraphs."""
        version = Version(
            norm_id=block_id,
            publication_date=date(1900, 1, 1),
            effective_date=date(1900, 1, 1),
            paragraphs=tuple(paragraphs),
        )
        return Block(
            id=block_id,
            block_type="article" if block_id != "full" else "full",
            title=title,
            versions=(version,),
        )


class TARMetadataParser(MetadataParser):
    """Parses data.gov.lt Spinta API JSON into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse JSON metadata from data.gov.lt.

        Args:
            data: JSON bytes from the Spinta API response.
            norm_id: TAR identifier (e.g., "TAR-2000-12345").
        """
        api_data = json.loads(data)
        items = api_data.get("_data", [])

        if not items:
            raise ValueError(f"No metadata found for TAR identifier {norm_id}")

        item = items[0]

        title = item.get("pavadinimas", "").strip()
        short_title = item.get("trumpas_pavadinimas", "").strip() or title
        tar_id = item.get("tar_identifikatorius", norm_id).strip()

        # Rank mapping
        rusis = item.get("rusis", "").strip()
        rank_str = RUSIS_TO_RANK.get(rusis, "kita")

        # Dates
        pub_date = _parse_date(item.get("priemimo_data")) or date(1900, 1, 1)
        entry_date = _parse_date(item.get("isigaliojimo_data"))
        expiry_date = _parse_date(item.get("galiojimo_pabaigos_data"))

        # Status
        status_raw = item.get("statusas", "").strip()
        status = STATUS_MAP.get(status_raw, NormStatus.IN_FORCE)
        if expiry_date and not status_raw:
            status = NormStatus.REPEALED

        institution = item.get("institucija", "").strip()
        source_url = f"https://www.e-tar.lt/portal/lt/legalAct/{tar_id}"

        return NormMetadata(
            title=title,
            short_title=short_title,
            identifier=tar_id,
            country="lt",
            rank=Rank(rank_str),
            publication_date=pub_date,
            status=status,
            department=institution,
            source=source_url,
            last_modified=entry_date,
        )
