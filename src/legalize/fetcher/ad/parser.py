"""Parsers for BOPA (Andorra) HTML documents and document metadata.

Two HTML formats coexist in BOPA:

* **Format A — modern (≈2015 → today)**: InDesign-exported HTML with semantic
  CSS classes (``Titol-3`` … ``Titol-7``, ``Body-text``, ``Body-text-i``,
  ``signatura``, ``Data``, ``TAULES_*``). Wrapped in a proper
  ``<html><body><div id="_idContainer000">…</div></body></html>``.

* **Format B — legacy (1989 → ≈2014)**: plain text with ``<br>`` only, no
  CSS classes, sometimes wrapped in ``<tr><td>`` for layout. Filename is a
  numeric or hex ID (``7586``, ``1B73A``, ``6074E``). The Constitució 1993
  is in this format.

The parser detects the format from the presence of ``Titol-3``/``Body-text``
classes and falls back to Format B otherwise. Format B output is lower
fidelity (one big paragraph per ``<br>``-separated line) but the laws are
still readable.
"""

from __future__ import annotations

import json
import logging
import re
import urllib.parse
from datetime import date, datetime
from typing import Any

from lxml import html as lxml_html

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import (
    Block,
    NormMetadata,
    NormStatus,
    Paragraph,
    Rank,
    Version,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Andorran ranks (Rank is a free-form str subclass)
# ─────────────────────────────────────────────

RANK_LLEI = Rank("llei")
RANK_DECRET_LLEI = Rank("decret_llei")  # Legislació delegada
RANK_REGLAMENT = Rank("reglament")  # Decret aprovant un Reglament
RANK_CONSTITUCIO = Rank("constitucio")
RANK_OTRO = Rank("otro")


# ─────────────────────────────────────────────
# BOPA → engine CSS class mapping
# ─────────────────────────────────────────────

# Maps BOPA's InDesign classes to the engine's vocabulary used by
# ``transformer/markdown.py``. Anything not in this map renders as a plain
# paragraph (``parrafo``), which produces a regular line of body text.
_BOPA_CSS_TO_ENGINE: dict[str, str] = {
    # Titol-3 is the law title — already rendered as H1 from metadata.title.
    # Skipped via _BOPA_CSS_SKIP to avoid duplication.
    "Titol-4": "titulo_tit",  # Títol I, II, III (## H2)
    "Titol-4-pg": "titulo_tit",
    "Titol-5": "capitulo_tit",  # Capítol / Disposicions / Exposició de motius (### H3)
    "Titol-5-pg": "capitulo_tit",
    "Titol-6": "seccion",  # Subsection (#### H4)
    "Titol-7": "articulo",  # Article N. Title (##### H5)
    "Titol-7-plus": "articulo",
    # Signatures and dates: render as italic/bold lines
    "Data": "firma_ministro",
    "signatura": "firma_rey",
}

# Classes whose content should NOT be emitted at all.
_BOPA_CSS_SKIP: frozenset[str] = frozenset(
    {
        "invisible",  # SEO-only hidden text
        "Titol-3",  # Law title — already in H1 from metadata
        "Placeholder-Text",  # Inline span wrapper inside Titol-3
    }
)

# Body-text classes mapped to "parrafo". Listed explicitly so we can detect
# them as "real" content for Format A detection.
_BOPA_BODY_CLASSES: frozenset[str] = frozenset(
    {
        "Body-text",
        "Body-text-i",
        "Body-text-ii",
        "Body-text-NOS",
        "Body-text-nos-ii",
        "Body-text-nos-iiii",
        "Placeholder-Text",
    }
)


# ─────────────────────────────────────────────
# Encoding detection
# ─────────────────────────────────────────────


def _decode_html(raw: bytes) -> str:
    """Decode BOPA HTML bytes, detecting the encoding from the BOM.

    BOPA blobs come in either UTF-16-LE (with ``\\xff\\xfe`` BOM) or UTF-8
    (no BOM). The encoding is per-file, not per-era. Some legacy and modern
    files use UTF-16; others use UTF-8.
    """
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    # Default UTF-8 with replacement on bad bytes
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


# ─────────────────────────────────────────────
# Title → identifier extraction
# ─────────────────────────────────────────────

# Anchor to the START of the title to avoid collisions where a corrigendum
# or modificadora references the original law in its body text:
#   "Llei 18/2024, del 19 de desembre, de caça."        → match BOPA-L-2024-18
#   "Correcció d'errata... a la Llei 18/2024..."        → no match → fallback
_LLEI_RE = re.compile(
    r"^Llei(?:\s+qualificada)?\s+(\d+)/(\d{4})\b",
    re.IGNORECASE,
)
# "Decret 501/2024, del 23-12-2024, ..." → match BOPA-D-2024-501
# "Decret legislatiu del 4-3-2020 de publicació del text refós..." → no match → fallback
_DECRET_RE = re.compile(
    r"^Decret(?:\s+legislatiu)?\s+(\d+)/(\d{4})\b",
    re.IGNORECASE,
)


def _extract_law_number(title: str) -> tuple[str, str] | None:
    """Extract ``(year, number)`` from a Llei title.

    >>> _extract_law_number("Llei 18/2024, del 19 de desembre, de caça.")
    ('2024', '18')
    >>> _extract_law_number("Llei qualificada 4/2025, ...")
    ('2025', '4')
    """
    match = _LLEI_RE.search(title)
    if match:
        return match.group(2), match.group(1)
    return None


def _extract_decret_number(title: str) -> tuple[str, str] | None:
    """Extract ``(year, number)`` from a Decret/Reglament title."""
    match = _DECRET_RE.search(title)
    if match:
        return match.group(2), match.group(1)
    return None


def _build_identifier(rank: Rank, title: str, nom_document: str, any_butlleti: str) -> str:
    """Build a canonical identifier following the ``BOPA-{type}-{year}-{number}`` pattern.

    Falls back to ``BOPA-X-{nomDocument}`` for legacy documents whose title
    doesn't include a clean ``N/YYYY`` number.
    """
    if rank == RANK_CONSTITUCIO:
        # Single Constitució — use the year of the original referendum.
        return "BOPA-C-1993"

    if rank == RANK_LLEI:
        nums = _extract_law_number(title)
        if nums:
            year, num = nums
            return f"BOPA-L-{year}-{num}"
        return f"BOPA-L-{any_butlleti}-{nom_document}"

    if rank == RANK_DECRET_LLEI:
        nums = _extract_decret_number(title) or _extract_law_number(title)
        if nums:
            year, num = nums
            return f"BOPA-LD-{year}-{num}"
        return f"BOPA-LD-{any_butlleti}-{nom_document}"

    if rank == RANK_REGLAMENT:
        nums = _extract_decret_number(title)
        if nums:
            year, num = nums
            return f"BOPA-D-{year}-{num}"
        return f"BOPA-D-{any_butlleti}-{nom_document}"

    return f"BOPA-X-{any_butlleti}-{nom_document}"


# ─────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────


def _parse_iso_datetime(value: str | None) -> date | None:
    """Parse an ISO 8601 datetime (with or without timezone) into a ``date``."""
    if not value:
        return None
    try:
        # Strip everything after the date portion to be robust against
        # Azure's "+00:00" / ".000" / "Z" suffixes.
        return date.fromisoformat(value[:10])
    except ValueError:
        try:
            # Last-resort: full datetime parse
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except ValueError:
            return None


# ─────────────────────────────────────────────
# Format A — modern HTML parsing
# ─────────────────────────────────────────────


def _is_format_a(text: str) -> bool:
    """Detect Format A (modern InDesign HTML) by presence of ``Titol-`` or ``Body-text`` classes."""
    return (
        ('class="Titol-3"' in text) or ('class="Body-text"' in text) or ("class='Titol-3'" in text)
    )


def _element_text(elem) -> str:
    """Extract clean text from an lxml element, preserving ``<br>`` as a separator.

    BOPA signatures use ``<br>`` to separate name/title lines and to align
    columns of co-signers. ``itertext()`` collapses these into a single string
    with no whitespace, mashing words together. We walk the tree manually and
    insert " / " between siblings separated by a ``<br>``, then collapse runs
    of whitespace.
    """

    def _walk(node) -> list[str]:
        out: list[str] = []
        if node.text:
            out.append(node.text)
        for child in node:
            tag = child.tag if isinstance(child.tag, str) else None
            if tag == "br":
                out.append(" / ")
            else:
                out.extend(_walk(child))
            if child.tail:
                out.append(child.tail)
        return out

    text = "".join(_walk(elem))
    # Collapse runs of whitespace (including non-breaking spaces) to single spaces
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    # Tidy up the " / " separator: trim duplicate slashes and surrounding spaces
    text = re.sub(r"\s*/\s*(?:/\s*)+", " / ", text)
    text = text.strip(" /")
    return text.strip()


def _table_to_markdown(table_elem) -> str:
    """Convert a BOPA InDesign-exported ``<table>`` element to Markdown.

    BOPA tables use:

    * ``<p class="TAULES_Encap-alament">`` inside cells for header rows
    * ``<p class="TAULES_TaulaText">`` inside cells for data rows

    There's no ``<th>`` tag — header detection is by class.
    """
    rows: list[list[str]] = []
    header_indices: set[int] = set()

    for idx, tr in enumerate(table_elem.iter("tr")):
        cells = tr.findall("td")
        if not cells:
            continue
        row = []
        is_header = False
        for td in cells:
            # Detect header by inner <p class="TAULES_Encap-alament">
            for p in td.iter("p"):
                cls = (p.get("class") or "").split()
                if "TAULES_Encap-alament" in cls or "TAULES_Encap-alament" in (
                    p.get("class") or ""
                ):
                    is_header = True
                    break
            text = _element_text(td)
            # Escape pipe characters for Markdown table syntax
            row.append(text.replace("|", "\\|").replace("\n", " ").strip())
        rows.append(row)
        if is_header and idx == 0:
            header_indices.add(idx)

    if not rows:
        return ""

    # Normalize column count
    max_cols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < max_cols:
            r.append("")

    # Use the first row as header (whether it had Encap-alament or not, to keep
    # Markdown valid). Markdown tables MUST have a header row.
    header = rows[0]
    body = rows[1:]

    out_lines = []
    out_lines.append("| " + " | ".join(header) + " |")
    out_lines.append("| " + " | ".join(["---"] * max_cols) + " |")
    for row in body:
        out_lines.append("| " + " | ".join(row) + " |")

    return "\n".join(out_lines)


def _parse_format_a(text: str) -> list[Paragraph]:
    """Parse a Format A modern BOPA HTML body into ``Paragraph`` objects.

    Walks the document in tree order, mapping each ``<p>`` to a paragraph
    with its engine CSS class. ``<table>`` elements are emitted as a single
    pre-rendered Markdown paragraph; their inner ``<p>`` cells are skipped.
    """
    # lxml tolerates missing wrappers and BOM-stripped UTF-16
    try:
        tree = lxml_html.fromstring(text)
    except (ValueError, lxml_html.etree.ParserError) as exc:
        logger.warning("lxml failed to parse Format A document: %s", exc)
        return []

    paragraphs: list[Paragraph] = []

    # NB: lxml element wrappers are short-lived; ``id(elem)`` is not stable
    # because the wrapper can be garbage-collected and reused for a different
    # element. We use ``elem.xpath('ancestor::table')`` for the inside-table
    # check and a tag-name token to dedupe table emissions.
    emitted_tables: set[str] = set()

    for elem in tree.iter():
        tag = elem.tag if isinstance(elem.tag, str) else None

        if tag == "table":
            # Build a stable token for this table from its source line + attributes
            token = (
                f"{elem.sourceline}:{elem.get('id') or ''}:"
                f"{(elem.get('class') or '')}:{len(elem.findall('.//tr'))}"
            )
            if token in emitted_tables:
                continue
            emitted_tables.add(token)
            md_table = _table_to_markdown(elem)
            if md_table:
                paragraphs.append(Paragraph(css_class="table_row", text=md_table))
            continue

        if tag != "p":
            continue
        # Skip <p>s that live INSIDE a table — those are rendered as table cells
        if elem.xpath("ancestor::table"):
            continue

        cls_attr = elem.get("class") or ""
        classes = cls_attr.split()
        primary = classes[0] if classes else ""

        if primary in _BOPA_CSS_SKIP:
            continue

        text_content = _element_text(elem)
        if not text_content:
            continue

        engine_class = _BOPA_CSS_TO_ENGINE.get(primary, "parrafo")
        paragraphs.append(Paragraph(css_class=engine_class, text=text_content))

    return paragraphs


# ─────────────────────────────────────────────
# Format B — legacy plain-text fallback parser
# ─────────────────────────────────────────────


def _parse_format_b(text: str) -> list[Paragraph]:
    """Parse a Format B legacy BOPA HTML body into ``Paragraph`` objects.

    Legacy documents (1989 → ≈2014) have no semantic CSS — just text and
    ``<br>`` line breaks, sometimes wrapped in ``<tr><td>``. We extract all
    text content, split by line breaks, and emit each non-empty line as a
    paragraph. Lower fidelity than Format A but the legal text is preserved.
    """
    # Replace <br> with newlines so itertext() gives us split content
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</div\s*>", "\n", text, flags=re.IGNORECASE)

    try:
        tree = lxml_html.fromstring(f"<root>{text}</root>")
    except (ValueError, lxml_html.etree.ParserError):
        # Last resort: brute-force tag stripping
        plain = re.sub(r"<[^>]+>", "", text)
        return _plain_to_paragraphs(plain)

    plain = "".join(t for t in tree.itertext() if t)
    return _plain_to_paragraphs(plain)


_LEGACY_PARA_END_RE = re.compile(
    r"[.;:!?]$|^Article\s|^Capítol\s|^Títol\s|^Disposici", re.IGNORECASE
)
_LEGACY_HEADING_RE = re.compile(
    r"^(?:Article|Capítol|Títol|Disposicions?|Disposició|Annex|Preàmbul)\b",
    re.IGNORECASE,
)


def _plain_to_paragraphs(plain: str) -> list[Paragraph]:
    """Convert a plain-text legacy blob into engine ``Paragraph`` objects.

    Pre-2015 BOPA documents are typewriter-style: hard line wraps every ~50
    characters with a ``<br>`` between every line. To produce readable prose
    we re-flow lines into paragraphs by joining contiguous lines that don't
    end with sentence punctuation. Lines that look like structural markers
    ("Article 1", "Capítol primer", "Disposicions transitòries", etc.) are
    treated as their own paragraph and emitted with the matching engine class
    so the Markdown renderer assigns a heading level.
    """
    lines = [ln.strip() for ln in plain.splitlines() if ln.strip()]
    if not lines:
        return []

    paragraphs: list[Paragraph] = []
    buffer: list[str] = []

    def flush(css_class: str = "parrafo") -> None:
        if buffer:
            paragraphs.append(Paragraph(css_class=css_class, text=" ".join(buffer)))
            buffer.clear()

    for line in lines:
        is_heading = bool(_LEGACY_HEADING_RE.match(line))
        ends_paragraph = bool(_LEGACY_PARA_END_RE.search(line))

        if is_heading:
            # Flush whatever was being collected as a normal paragraph,
            # then emit the heading as its own paragraph with a heading class
            # so the Markdown renderer styles it correctly.
            flush()
            heading_class = _legacy_heading_class(line)
            paragraphs.append(Paragraph(css_class=heading_class, text=line))
            continue

        buffer.append(line)
        if ends_paragraph:
            flush()

    flush()
    return paragraphs


def _legacy_heading_class(line: str) -> str:
    """Pick an engine heading class for a legacy structural line."""
    lower = line.lower()
    if lower.startswith("títol") or lower.startswith("titol"):
        return "titulo_tit"
    if lower.startswith("capítol") or lower.startswith("capitol"):
        return "capitulo_tit"
    if lower.startswith("article"):
        return "articulo"
    if lower.startswith("disposici") or lower.startswith("annex") or lower.startswith("preàmbul"):
        return "seccion"
    return "parrafo"


# ─────────────────────────────────────────────
# Public TextParser
# ─────────────────────────────────────────────


class BOPATextParser(TextParser):
    """Parses BOPA HTML documents into a list of ``Block`` objects.

    Each BOPA document becomes a single ``Block`` containing one ``Version``
    with all paragraphs from the source HTML. BOPA does not publish
    consolidated texts — every published document is its own self-contained
    legal text — so there is no per-block versioning to track here.

    Input format
    ------------
    ``data`` is a JSON-encoded bundle produced by ``BOPAClient.get_text``::

        {"html": "<utf-8 doc HTML>", "publication_date": "YYYY-MM-DD",
         "article_date": "YYYY-MM-DD"}

    For backwards compatibility (and to keep tests simple) we also accept
    raw HTML bytes — in that case the Version uses a 1900-01-01 placeholder
    date which the engine corrects via metadata further downstream.
    """

    def parse_text(self, data: bytes) -> list[Any]:
        html, pub_date = _unwrap_bundle(data)

        if _is_format_a(html):
            paragraphs = _parse_format_a(html)
        else:
            paragraphs = _parse_format_b(html)

        if not paragraphs:
            logger.warning("Parsed BOPA document but produced no paragraphs")
            return []

        # Wrap everything in a single Block — BOPA documents are atomic units.
        version = Version(
            norm_id="",
            publication_date=pub_date,
            effective_date=pub_date,
            paragraphs=tuple(paragraphs),
        )
        block = Block(
            id="body",
            block_type="article",
            title="",
            versions=(version,),
        )
        return [block]


def _unwrap_bundle(data: bytes) -> tuple[str, date]:
    """Decode the JSON bundle produced by ``BOPAClient.get_text``.

    Falls back to treating ``data`` as raw HTML bytes for backwards
    compatibility — useful in tests that pass fixture HTML directly. In the
    fallback path the publication date is the 1900-01-01 placeholder.
    """
    pub_date_default = date(1900, 1, 1)

    if not data:
        return "", pub_date_default

    # The JSON bundle starts with `{"html"` after stripping whitespace.
    if data[:1] == b"{":
        try:
            bundle = json.loads(data)
        except json.JSONDecodeError:
            return _decode_html(data), pub_date_default
        html = bundle.get("html") or ""
        pub_str = bundle.get("publication_date") or ""
        pub_date = _parse_iso_datetime(pub_str) or pub_date_default
        return html, pub_date

    # Raw HTML bytes (legacy path used by tests)
    return _decode_html(data), pub_date_default


# ─────────────────────────────────────────────
# Metadata parser
# ─────────────────────────────────────────────


def _detect_rank(organisme: str) -> Rank:
    """Map a BOPA ``organisme`` (human name) to a ``Rank``."""
    if organisme == "Lleis":
        return RANK_LLEI
    if organisme == "Reglaments":
        return RANK_REGLAMENT
    if organisme == "Legislació delegada":
        return RANK_DECRET_LLEI
    if organisme.startswith("Constitució"):
        return RANK_CONSTITUCIO
    return RANK_OTRO


def _decode_sumari(sumari: str) -> str:
    """URL-decode a ``sumari`` field returned by the BOPA API.

    The BOPA API URL-encodes the sumari with a mix of percent-encoding and
    form-encoding (``+`` for spaces). Some entries are also wrapped in raw
    HTML markup like ``<div class="ExternalClassXYZ">…</div>``. We decode
    both encodings, strip HTML tags, and collapse whitespace.
    """
    if not sumari:
        return ""
    try:
        decoded = urllib.parse.unquote_plus(sumari)
    except (TypeError, ValueError):
        return sumari
    # Strip raw HTML tags (some sumari entries are wrapped in <div class="...">)
    decoded = re.sub(r"<[^>]+>", "", decoded)
    # Decode HTML entities and collapse whitespace
    decoded = decoded.replace("&nbsp;", " ").replace("\xa0", " ")
    decoded = re.sub(r"\s+", " ", decoded)
    return decoded.strip()


def _build_source_url(any_butlleti: str, num_butlleti: str, nom_document: str) -> str:
    """Build a public BOPA URL for a document.

    Uses the same ``/bopa/{YYY}{NNN}/Pagines/{nomDocument}.aspx`` format as
    the cross-reference links found inside modificadora documents.
    """
    try:
        year_int = int(any_butlleti)
        num_int = int(num_butlleti)
    except (TypeError, ValueError):
        return f"https://www.bopa.ad/bopa/{any_butlleti}/{num_butlleti}/{nom_document}"
    offset = year_int - 1988
    bucket = f"{offset:03d}{num_int:03d}"
    return f"https://www.bopa.ad/bopa/{bucket}/Pagines/{nom_document}.aspx"


class BOPAMetadataParser(MetadataParser):
    """Parses BOPA document metadata (API JSON) into ``NormMetadata``."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        try:
            doc = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON for AD document {norm_id!r}") from exc

        organisme = doc.get("organisme") or ""
        rank = _detect_rank(organisme)

        sumari_raw = doc.get("sumari") or ""
        title = _decode_sumari(sumari_raw)
        if not title:
            title = doc.get("nomDocument", norm_id)

        any_butlleti = str(doc.get("anyButlleti") or "")
        num_butlleti = str(doc.get("numButlleti") or "")
        nom_document = doc.get("nomDocument") or ""

        identifier = _build_identifier(rank, title, nom_document, any_butlleti)

        # Dates: publication_date = dataPublicacioButlleti (BOPA gazette date),
        # following the same convention as ES (BOE date) and SE (SFS date).
        pub_date = _parse_iso_datetime(doc.get("dataPublicacioButlleti"))
        if pub_date is None:
            pub_date = _parse_iso_datetime(doc.get("dataArticle"))
        if pub_date is None:
            raise ValueError(f"Could not extract publication date for {identifier}")

        article_date = _parse_iso_datetime(doc.get("dataArticle"))

        source_url = _build_source_url(any_butlleti, num_butlleti, nom_document)

        # Country-specific fields rendered in YAML frontmatter
        extra: list[tuple[str, str]] = []
        if article_date and article_date != pub_date:
            extra.append(("signature_date", article_date.isoformat()))
        if num_butlleti and any_butlleti:
            extra.append(("bopa_issue", f"BOPA {num_butlleti}/{any_butlleti}"))
        if nom_document:
            extra.append(("bopa_document_id", nom_document))
        if doc.get("isExtra") in ("True", True):
            extra.append(("bopa_extra", "true"))

        return NormMetadata(
            title=title,
            short_title=title,
            identifier=identifier,
            country="ad",
            rank=rank,
            publication_date=pub_date,
            status=NormStatus.IN_FORCE,
            department="Govern del Principat d'Andorra",
            source=source_url,
            last_modified=pub_date,
            extra=tuple(extra),
        )
