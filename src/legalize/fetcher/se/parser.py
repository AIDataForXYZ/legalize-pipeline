"""Parsers for Swedish legislation from the Riksdagen API.

Parses the JSON document format from Riksdagen's open data API
into structured Bloque objects and NormaMetadata.

Text parsing ported from the TypeScript riksdagen-provision-parser.ts,
adapted for the legalize pipeline data model.

References:
  - https://data.riksdagen.se/
  - https://rkrattsbaser.gov.se/sfsr (amendment register)
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import (
    Bloque,
    EstadoNorma,
    NormaMetadata,
    Paragraph,
    Rango,
    Reform,
    Version,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Swedish Rango constants
# ─────────────────────────────────────────────

# Defined as Rango strings — consistent with how Spain/France do it.
# Rango is a free-form str subclass, so any string is valid.
_RANGO_GRUNDLAG = Rango("grundlag")       # Fundamental law (constitution)
_RANGO_BALK = Rango("balk")               # Code (e.g. Brottsbalken)
_RANGO_LAG = Rango("lag")                 # Act/Law
_RANGO_FORORDNING = Rango("forordning")   # Ordinance/Regulation

# ─────────────────────────────────────────────
# Title → Rango detection
# ─────────────────────────────────────────────

# Known fundamental laws (grundlag) by keyword in title
_GRUNDLAG_KEYWORDS = (
    "grundlag",
    "regeringsformen",
    "tryckfrihetsförordningen",
    "yttrandefrihetsgrundlagen",
    "successionsordningen",
)


def _detect_rango(title: str) -> Rango:
    """Detect the normative rank from a Swedish statute title.

    Detection order (most specific first):
    1. Known fundamental law keywords -> grundlag
    2. "balk" in title -> balk (code)
    3. "förordning" in title -> forordning (ordinance)
    4. "lag" in title -> lag (act) — default for most statutes
    """
    title_lower = title.lower()

    for keyword in _GRUNDLAG_KEYWORDS:
        if keyword in title_lower:
            return _RANGO_GRUNDLAG

    if "balk" in title_lower:
        return _RANGO_BALK

    if "förordning" in title_lower:
        return _RANGO_FORORDNING

    # Default: lag (act) — most Swedish SFS entries are laws
    return _RANGO_LAG


# ─────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────

def _parse_date_se(date_str: str | None) -> date | None:
    """Parse a date string from Riksdagen JSON.

    Expected format: YYYY-MM-DD. Returns None for empty/invalid.
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    if not date_str:
        return None
    try:
        # Extract YYYY-MM-DD from possibly longer strings
        match = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        if match:
            return date.fromisoformat(match.group(1))
        return None
    except ValueError:
        return None


# ─────────────────────────────────────────────
# Text parsing — chapter/section regex
# ─────────────────────────────────────────────

_CHAPTER_PATTERN = re.compile(r"^(\d+)\s*kap\.\s*(.*)$", re.UNICODE)
_SECTION_PATTERN = re.compile(r"^(\d+\s*[a-z]?)\s*§\s*(.*)$", re.IGNORECASE | re.UNICODE)
_LAW_NOTE_PATTERN = re.compile(r"^Lag\s+\(\d{4}:\d+\)\.?$", re.UNICODE)


def _normalize_section_ref(section: str) -> str:
    """Normalize a section reference: strip whitespace, lowercase."""
    return re.sub(r"\s+", " ", section).strip().lower()


def _section_ordinal(section: str) -> int | None:
    """Convert a section ref like '3 a' to an ordinal (300 + 1 = 301).

    Used for monotonicity checks to detect out-of-order sections.
    """
    match = re.match(r"^(\d+)(?:\s*([a-z]))?$", section, re.IGNORECASE)
    if not match:
        return None
    base = int(match.group(1))
    suffix = (match.group(2) or "").lower()
    if not suffix:
        return base * 100
    offset = ord(suffix) - 96  # a=1, b=2, ...
    return base * 100 + max(offset, 0)


def _is_likely_title(line: str) -> bool:
    """Check if a line looks like a section title (not a provision start)."""
    return (
        0 < len(line) < 100
        and bool(re.match(r"^[A-ZÅÄÖ]", line))
        and not re.match(r"^\d+\s*(kap\.|§)", line)
        and not _LAW_NOTE_PATTERN.match(line)
    )


def _parse_provisions(text: str) -> list[dict[str, Any]]:
    """Parse Swedish statute text into provisions.

    Ported from riksdagen-provision-parser.ts with the same
    chapter activation and section monotonicity checks.

    Returns a list of dicts with keys:
        provision_ref, chapter, section, title, content
    """
    lines = text.split("\n")
    provisions: list[dict[str, Any]] = []
    seen_refs: set[str] = set()
    last_ordinal_by_chapter: dict[str, int] = {}

    current_chapter: str | None = None
    pending_chapter: str | None = None
    current_section: str | None = None
    current_title: str | None = None
    pending_title: str | None = None
    current_content: list[str] = []

    def flush_current_section() -> None:
        nonlocal current_section, current_title
        if not current_section or not current_content:
            current_section = None
            current_title = None
            current_content.clear()
            return

        section = _normalize_section_ref(current_section)
        provision_ref = f"{current_chapter}:{section}" if current_chapter else section

        provisions.append({
            "provision_ref": provision_ref,
            "chapter": current_chapter,
            "section": section,
            "title": current_title,
            "content": " ".join(current_content).strip(),
        })

        seen_refs.add(provision_ref)

        if current_chapter:
            ordinal = _section_ordinal(section)
            if ordinal is not None:
                last_ordinal_by_chapter[current_chapter] = ordinal

        current_section = None
        current_title = None
        current_content.clear()

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        # Check for chapter marker
        chapter_match = _CHAPTER_PATTERN.match(line)
        if chapter_match:
            flush_current_section()
            pending_chapter = chapter_match.group(1)
            pending_title = None
            continue

        # Check for section marker
        section_match = _SECTION_PATTERN.match(line)
        if section_match:
            normalized_section = _normalize_section_ref(section_match.group(1))
            section_num_match = re.match(r"^(\d+)", normalized_section)
            section_num = int(section_num_match.group(1)) if section_num_match else None
            remainder = section_match.group(2).strip()

            chapter_for_section = current_chapter
            chapter_activated = False

            if pending_chapter:
                if not current_chapter or section_num == 1:
                    chapter_for_section = pending_chapter
                    chapter_activated = chapter_for_section != current_chapter
                pending_chapter = None

            provision_ref = (
                f"{chapter_for_section}:{normalized_section}"
                if chapter_for_section
                else normalized_section
            )

            candidate_ordinal = _section_ordinal(normalized_section)
            current_ordinal = (
                _section_ordinal(current_section) if current_section else None
            )
            last_ordinal = (
                last_ordinal_by_chapter.get(chapter_for_section)
                if chapter_for_section
                else None
            )

            # Suppression checks (same as TypeScript parser)
            is_duplicate = provision_ref in seen_refs
            is_out_of_order_current = (
                current_ordinal is not None
                and candidate_ordinal is not None
                and candidate_ordinal <= current_ordinal
            )
            is_out_of_order_history = (
                not chapter_activated
                and last_ordinal is not None
                and candidate_ordinal is not None
                and candidate_ordinal <= last_ordinal
            )
            is_inline_ref = (
                not chapter_activated
                and current_section is not None
                and len(current_content) > 0
                and remainder
                and bool(re.match(r"^[a-zåäö]", remainder))
            )

            if (
                is_duplicate
                or is_out_of_order_current
                or is_out_of_order_history
                or is_inline_ref
            ):
                if current_section:
                    current_content.append(line)
                continue

            title_for_section = pending_title
            pending_title = None
            flush_current_section()

            current_chapter = chapter_for_section
            current_section = normalized_section
            current_title = title_for_section

            if remainder:
                current_content.append(remainder)
            continue

        # Check for title line
        if not current_section and _is_likely_title(line):
            pending_title = line
            continue

        if current_section and not current_content and _is_likely_title(line):
            current_title = line
            continue

        # Content line
        if current_section:
            current_content.append(line)

    flush_current_section()
    return provisions


# ─────────────────────────────────────────────
# JSON extraction helpers
# ─────────────────────────────────────────────

def _extract_text_from_json(data: bytes) -> str:
    """Extract the plain text field from Riksdagen document JSON.

    Expected structure: {"dokumentstatus": {"dokument": {"text": "..."}}}
    """
    doc_json = json.loads(data)
    doc = doc_json.get("dokumentstatus", {}).get("dokument", {})
    return doc.get("text", "")


def _extract_html_from_json(data: bytes) -> str:
    """Extract the HTML field from Riksdagen document JSON.

    Expected structure: {"dokumentstatus": {"dokument": {"html": "..."}}}
    """
    doc_json = json.loads(data)
    doc = doc_json.get("dokumentstatus", {}).get("dokument", {})
    return doc.get("html", "")


def _extract_dokuppgift(data: bytes) -> dict[str, str]:
    """Extract key-value metadata from dokuppgift in Riksdagen JSON.

    The dokuppgift field contains a list of {uppgift, text} dicts.
    Returns them as a flat dict keyed by uppgift name.
    """
    doc_json = json.loads(data)
    uppgifter = (
        doc_json.get("dokumentstatus", {}).get("dokuppgift", {}).get("uppgift")
        or []
    )

    result: dict[str, str] = {}
    for item in uppgifter:
        key = item.get("uppgift", "")
        value = item.get("text", "")
        if key:
            result[key] = value
    return result


def _extract_html_metadata(html: str) -> dict[str, str]:
    """Extract metadata from the HTML header of a Riksdagen document.

    Searches for patterns like <b>Key</b>: Value in the first 3000 chars.
    """
    if not html:
        return {}

    header = html[:3000]
    pairs = re.findall(
        r"<b>\s*([^<:]+?)\s*</b>\s*:\s*([^<]+)\s*(?:<br|$)",
        header,
        re.IGNORECASE,
    )

    metadata: dict[str, str] = {}
    for key, value in pairs:
        key = " ".join(key.split()).strip()
        value = " ".join(value.split()).strip()
        if key and value:
            metadata[key] = value
    return metadata


# ─────────────────────────────────────────────
# SFSR amendment register parsing
# ─────────────────────────────────────────────

_SFSR_AMENDMENT_PATTERN = re.compile(r"Ändring,\s*SFS\s+(\d{4}:\d+)")
_SFSR_OMFATTNING_PATTERN = re.compile(r"Omfattning:\s*([^\n<]+)")
_SFSR_SECTION_PATTERN = re.compile(
    r"(?:ändr?\.?|ny|nya|upph\.?)\s+([\d\s,]+)\s*kap\.\s*([\d\s,a-z§]+)",
    re.IGNORECASE,
)
_SFSR_SIMPLE_SECTION_PATTERN = re.compile(
    r"(?:ändr?\.?|ny|nya|upph\.?)\s+([\d\s,a-z]+)\s*§§?",
    re.IGNORECASE,
)


def _parse_affected_sections(omfattning: str) -> tuple[str, ...]:
    """Parse the Omfattning field to extract affected section references.

    Examples:
        "ändr. 1 kap. 3, 5 §§" -> ("1:3", "1:5")
        "ändr. 3, 5 §§" -> ("3", "5")
        "ny 2 kap. 4 a §" -> ("2:4 a",)
    """
    sections: list[str] = []

    # Match chapter-qualified sections: "1 kap. 3, 5 §§"
    for match in _SFSR_SECTION_PATTERN.finditer(omfattning):
        chapters_str = match.group(1).strip()
        sections_str = match.group(2).strip()

        # Parse chapter numbers (usually just one)
        chapters = [c.strip() for c in re.split(r"[,\s]+", chapters_str) if c.strip()]

        # Parse section numbers: "3, 5" or "4 a"
        # Split on commas, then clean up
        section_parts = [s.strip().rstrip("§ ") for s in sections_str.split(",")]
        section_parts = [s.strip() for s in section_parts if s.strip()]

        for chapter in chapters:
            for section in section_parts:
                if section and re.match(r"^\d+", section):
                    sections.append(f"{chapter}:{section}")

    # Match non-chapter sections: "ändr. 3, 5 §§"
    if not sections:
        for match in _SFSR_SIMPLE_SECTION_PATTERN.finditer(omfattning):
            sections_str = match.group(1).strip()
            section_parts = [s.strip() for s in sections_str.split(",")]
            for section in section_parts:
                section = section.strip()
                if section and re.match(r"^\d+", section):
                    sections.append(section)

    return tuple(sections)


def _parse_sfsr_html(html: str) -> list[Reform]:
    """Parse the SFSR HTML to extract amendment history.

    Looks for entries like "Ändring, SFS YYYY:NNN" followed by
    "Omfattning:" fields describing affected sections.

    Returns a list of Reform objects.
    """
    reforms: list[Reform] = []

    # Split by amendment entries
    entries = _SFSR_AMENDMENT_PATTERN.split(html)

    # entries[0] is before the first match, then alternating: sfs_number, content
    for i in range(1, len(entries), 2):
        sfs_number = entries[i]
        content = entries[i + 1] if i + 1 < len(entries) else ""

        # Extract Omfattning
        omfattning_match = _SFSR_OMFATTNING_PATTERN.search(content)
        affected = ()
        if omfattning_match:
            affected = _parse_affected_sections(omfattning_match.group(1))

        # Extract date from the SFS number (year part)
        year_match = re.match(r"(\d{4})", sfs_number)
        if year_match:
            year = int(year_match.group(1))
            # Use January 1 of the year as approximate date
            # (exact date would require additional lookup)
            reforma_date = date(year, 1, 1)
        else:
            continue

        reforms.append(Reform(
            fecha=reforma_date,
            id_norma=f"SFS {sfs_number}",
            bloques_afectados=affected,
        ))

    return reforms


# ─────────────────────────────────────────────
# Public classes — TextParser / MetadataParser
# ─────────────────────────────────────────────


class SwedishTextParser(TextParser):
    """Parses Riksdagen JSON document text into Bloque objects."""

    def parse_texto(self, data: bytes) -> list[Any]:
        """Parse the Riksdagen JSON into a list of Bloque objects.

        Extracts the text field from the JSON, then parses chapters
        and sections using regex patterns ported from the TypeScript
        riksdagen-provision-parser.ts.

        Args:
            data: Full Riksdagen document JSON as bytes.

        Returns:
            List of Bloque objects (sections and articles).
        """
        text = _extract_text_from_json(data)
        if not text:
            logger.warning("No text content in Riksdagen document")
            return []

        provisions = _parse_provisions(text)
        return _provisions_to_blocks(provisions)

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract amendment history from SFSR HTML embedded in data.

        The data bytes are expected to contain a combined JSON with
        both the document text AND the SFSR HTML. If no SFSR data
        is available, extracts what reform info is possible from
        the document metadata.

        Args:
            data: Combined JSON bytes with optional SFSR HTML.

        Returns:
            List of Reform objects.
        """
        try:
            combined = json.loads(data)
        except json.JSONDecodeError:
            logger.warning("Failed to parse reform data as JSON")
            return []

        # Check for SFSR HTML in the combined data
        sfsr_html = combined.get("sfsr_html", "")
        if sfsr_html:
            return _parse_sfsr_html(sfsr_html)

        # Fallback: try to extract from document HTML metadata
        doc = combined.get("dokumentstatus", {}).get("dokument", {})
        html = doc.get("html", "")
        if html:
            html_meta = _extract_html_metadata(html)
            amended_through = html_meta.get("Ändrad t.o.m.", "")
            if amended_through:
                sfs_match = re.match(r"SFS\s+(\d{4}:\d+)", amended_through)
                if sfs_match:
                    sfs = sfs_match.group(1)
                    year = int(sfs[:4])
                    return [Reform(
                        fecha=date(year, 1, 1),
                        id_norma=f"SFS {sfs}",
                        bloques_afectados=(),
                    )]

        return []

    def extract_reforms_from_sfsr(self, sfsr_html: bytes | str) -> list[Reform]:
        """Parse raw SFSR HTML into Reform objects.

        Called directly when SFSR HTML is fetched separately (not embedded in JSON).
        """
        if isinstance(sfsr_html, bytes):
            sfsr_html = sfsr_html.decode("utf-8", errors="replace")
        return _parse_sfsr_html(sfsr_html)


class SwedishMetadataParser(MetadataParser):
    """Parses metadata from Riksdagen document JSON."""

    def parse(self, data: bytes, norm_id: str) -> NormaMetadata:
        """Parse Riksdagen JSON into NormaMetadata.

        Extracts metadata from the dokumentstatus.dokument and
        dokumentstatus.dokuppgift fields.

        Args:
            data: Full Riksdagen document JSON as bytes.
            norm_id: The SFS number, e.g. "1962:700".

        Returns:
            NormaMetadata with Swedish-specific fields.

        Raises:
            ValueError: If essential metadata cannot be extracted.
        """
        try:
            doc_json = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON for SFS {norm_id}") from exc

        doc = doc_json.get("dokumentstatus", {}).get("dokument", {})
        dokuppgift = _extract_dokuppgift(data)

        # Title
        title = (
            doc.get("titel", "")
            or dokuppgift.get("titel", "")
            or f"SFS {norm_id}"
        )
        title = " ".join(title.split()).strip()

        # Short title
        short_title = _titulo_corto_se(title, norm_id)

        # Dates
        html = doc.get("html", "")
        html_meta = _extract_html_metadata(html)

        utfardad = html_meta.get("Utfärdad") or doc.get("datum", "")
        fecha_pub = _parse_date_se(utfardad)
        if fecha_pub is None:
            # Fallback: use the year from the SFS number
            year_match = re.match(r"(\d{4})", norm_id)
            if year_match:
                fecha_pub = date(int(year_match.group(1)), 1, 1)
            else:
                raise ValueError(
                    f"Could not extract publication date for SFS {norm_id}"
                )

        andrad_tom = html_meta.get("Ändrad t.o.m.", "")
        fecha_modif = _parse_date_se(andrad_tom)

        # Status
        estado = EstadoNorma.VIGENTE
        if html_meta.get("Upphävd"):
            estado = EstadoNorma.DEROGADA

        # Rank
        rank = _detect_rango(title)

        # Identifier
        # Normalize for filesystem: "1962:700" → "SFS-1962-700"
        identifier = f"SFS-{norm_id.replace(':', '-')}"

        # Source URL
        sfs_slug = norm_id.replace(":", "-")
        source_url = (
            f"https://www.riksdagen.se/sv/dokument-och-lagar/"
            f"dokument/svensk-forfattningssamling/sfs-{sfs_slug}"
        )

        return NormaMetadata(
            titulo=title,
            titulo_corto=short_title,
            identificador=identifier,
            pais="se",
            rango=rank,
            fecha_publicacion=fecha_pub,
            estado=estado,
            departamento="",
            fuente=source_url,
            fecha_ultima_modificacion=fecha_modif,
        )


# ─────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────


def _titulo_corto_se(raw_title: str, norm_id: str) -> str:
    """Generate a short title for a Swedish statute.

    Examples:
        "Brottsbalk (1962:700)" -> "Brottsbalken"
        "Lag (2018:218) med kompletterande..." -> "Lag (2018:218)"
        "Tryckfrihetsförordningen (1949:105)" -> "Tryckfrihetsförordningen"
    """
    if not raw_title:
        return f"SFS {norm_id}"

    # If title starts with a known short form, extract it
    # Pattern: "Name (YYYY:NNN) rest..." -> "Name"
    match = re.match(r"^([^(]+?)\s*\(\d{4}:\d+\)", raw_title)
    if match:
        short = match.group(1).strip()
        # For "Lag" or "Förordning" alone, include the SFS number
        if short.lower() in ("lag", "förordning"):
            sfs_match = re.search(r"\((\d{4}:\d+)\)", raw_title)
            if sfs_match:
                return f"{short} ({sfs_match.group(1)})"
        return short

    # Truncate at first parenthesis
    paren_idx = raw_title.find("(")
    if paren_idx > 0:
        return raw_title[:paren_idx].strip()

    return raw_title


def _provisions_to_blocks(provisions: list[dict[str, Any]]) -> list[Bloque]:
    """Convert parsed provisions to Bloque objects.

    Groups provisions by chapter, creating section Bloques for
    chapter headings and article Bloques for individual sections.
    """
    blocks: list[Bloque] = []
    current_chapter: str | None = None

    for prov in provisions:
        chapter = prov.get("chapter")
        section = prov.get("section", "")
        provision_ref = prov.get("provision_ref", "")
        title = prov.get("title")
        content = prov.get("content", "")

        # Emit chapter heading when chapter changes
        if chapter and chapter != current_chapter:
            current_chapter = chapter
            chapter_title = f"{chapter} kap."
            if title:
                chapter_title = f"{chapter} kap. {title}"

            blocks.append(Bloque(
                id=f"kap_{chapter}",
                tipo="section",
                titulo=chapter_title,
                versions=(Version(
                    id_norma="",
                    fecha_publicacion=date(1900, 1, 1),
                    fecha_vigencia=date(1900, 1, 1),
                    paragraphs=(
                        Paragraph(
                            css_class="titulo_tit",
                            text=chapter_title,
                        ),
                    ),
                ),),
            ))

        # Build paragraphs for the section/article
        paragraphs: list[Paragraph] = []

        # Section number as heading
        if section:
            section_label = f"{section} §"
            paragraphs.append(Paragraph(css_class="articulo", text=section_label))

        # Section title if present
        if title and chapter == current_chapter:
            # Only add title if it was not already used as chapter title
            pass

        # Content paragraphs
        if content:
            # Split on double spaces or sentence boundaries for readability
            paragraphs.append(Paragraph(css_class="parrafo", text=content))

        if paragraphs:
            blocks.append(Bloque(
                id=provision_ref or f"s_{section}",
                tipo="article",
                titulo=f"{section} §" if section else provision_ref,
                versions=(Version(
                    id_norma="",
                    fecha_publicacion=date(1900, 1, 1),
                    fecha_vigencia=date(1900, 1, 1),
                    paragraphs=tuple(paragraphs),
                ),),
            ))

    return blocks
