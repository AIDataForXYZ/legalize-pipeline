"""Mexico parser — multi-source.

Bytes returned by ``MXClient`` are JSON envelopes (``{source, norm_id, …}``)
that wrap whatever raw payload came back from the portal — for Diputados,
the consolidated PDF as base64. The parser dispatches on the envelope's
``source`` field to the right per-source helper.

Implemented today: Diputados (single-snapshot text + index-based metadata).
Other sources still raise ``NotImplementedError``.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import re
import unicodedata
from datetime import date
from typing import Any

import pdfplumber

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import Block, NormMetadata, NormStatus, Paragraph, Rank, Version

logger = logging.getLogger(__name__)

_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")

# Match "Artículo 1o.", "Artículo 1.", "Artículo 1.-", "Articulo 1", "ARTÍCULO 1o."
# Also accepts "Único" variants, common in short-amendment laws.
_ARTICULO_RE = re.compile(
    r"^\s*(?:art[íi]culo|artículo)\s+"
    r"(\d+(?:\s*[ºo°])?|[úu]nico|primero|segundo|tercero|cuarto|quinto|sexto|s[ée]ptimo|octavo|noveno|d[ée]cimo)"
    r"[\s.\-]*",
    re.IGNORECASE,
)

# Section / chapter / title headings — for grouping.
_TITULO_RE = re.compile(r"^\s*T[ÍI]TULO\b", re.IGNORECASE)
_CAPITULO_RE = re.compile(r"^\s*CAP[ÍI]TULO\b", re.IGNORECASE)
_SECCION_RE = re.compile(r"^\s*SECCI[ÓO]N\b", re.IGNORECASE)
_LIBRO_RE = re.compile(r"^\s*LIBRO\b", re.IGNORECASE)


# ── Module helpers ─────────────────────────────────────────────────────


def _decode_envelope(data: bytes) -> dict[str, Any]:
    """Decode the JSON envelope produced by MXClient. Raises on malformed data."""
    try:
        envelope = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"MX parser expected JSON envelope, got: {exc}") from exc
    if not isinstance(envelope, dict) or "source" not in envelope:
        raise ValueError("MX parser envelope is missing 'source' key")
    return envelope


def _clean_line(text: str) -> str:
    """Normalize whitespace and strip control chars for one line."""
    text = unicodedata.normalize("NFC", text)
    text = _CONTROL_RE.sub("", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_pdf_text(pdf_bytes: bytes) -> list[str]:
    """Return one string per page of the PDF."""
    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages.append(text)
    return pages


_PAGINATION_RE = re.compile(r"^(?:p[áa]gina\s+)?\d+\s+de\s+\d+$", re.IGNORECASE)
_DIPUTADOS_BOILERPLATE_RE = re.compile(
    r"(c[áa]mara de diputados|congreso de la uni[óo]n|"
    r"secretar[íi]a general|secretar[íi]a de servicios parlamentarios|"
    r"centro de documentaci[óo]n)",
    re.IGNORECASE,
)
_LAST_REFORM_FOOTER_RE = re.compile(
    r"^[ÚU]ltima\s+[Rr]eforma\s+(?:DOF|publicada)", re.IGNORECASE
)


def _split_into_lines(pages: list[str]) -> list[str]:
    """Flatten pages into lines, dropping per-page header/footer noise.

    Diputados PDFs repeat several institutional banners on every page
    (the document title in all caps, "Cámara de Diputados…" footers,
    pagination markers, "Última Reforma DOF…" stamps). We drop them so
    article text reads cleanly; an article-level pass merges line wraps.
    """
    out: list[str] = []

    # Pre-compute lines that appear on most pages (n>1 page or repeated >2x).
    # Anything matching is treated as repeating template chrome.
    counts: dict[str, int] = {}
    for page_text in pages:
        seen_in_page: set[str] = set()
        for raw in page_text.splitlines():
            line = _clean_line(raw)
            if line and line not in seen_in_page:
                seen_in_page.add(line)
                counts[line] = counts.get(line, 0) + 1
    repeating_chrome = {
        line for line, count in counts.items() if len(pages) > 2 and count >= 3
    }

    for page_text in pages:
        for raw in page_text.splitlines():
            line = _clean_line(raw)
            if not line:
                continue
            if _PAGINATION_RE.match(line):
                continue
            if _LAST_REFORM_FOOTER_RE.match(line):
                continue
            if _DIPUTADOS_BOILERPLATE_RE.search(line):
                continue
            if line in repeating_chrome:
                continue
            out.append(line)
    return out


def _articulo_id(token: str) -> str:
    """Normalize an article identifier captured by _ARTICULO_RE into a slug."""
    t = token.lower().strip()
    t = re.sub(r"\s+[ºo°]$", "", t)
    return t.replace(" ", "")


def _diputados_blocks(envelope: dict[str, Any]) -> list[Block]:
    """Single-snapshot block builder: split a Diputados PDF into articles.

    Each ``Articulo N`` becomes a Block with one Version. Headings above
    each article (TÍTULO/CAPÍTULO/SECCIÓN/LIBRO) are emitted as their own
    blocks so the section structure survives in the rendered Markdown.
    """
    pdf_b64 = envelope.get("pdf_b64")
    if not pdf_b64:
        raise ValueError("Diputados envelope is missing 'pdf_b64'")
    pdf_bytes = base64.b64decode(pdf_b64)

    norm_id = envelope["norm_id"]
    pub_date = date.fromisoformat(envelope["publication_date"])
    last_reform = envelope.get("last_reform_date")
    effective_date = (
        date.fromisoformat(last_reform) if last_reform else pub_date
    )

    pages = _extract_pdf_text(pdf_bytes)
    lines = _split_into_lines(pages)

    blocks: list[Block] = []
    current_article_id: str | None = None
    current_article_title: str | None = None
    current_article_paragraphs: list[Paragraph] = []
    section_paragraph_buffer: list[Paragraph] = []  # headings without an article yet

    def flush_article() -> None:
        nonlocal current_article_id, current_article_title, current_article_paragraphs
        if current_article_id is None:
            return
        blocks.append(
            Block(
                id=f"art-{current_article_id}",
                block_type="article",
                title=current_article_title or f"Artículo {current_article_id}",
                versions=(
                    Version(
                        norm_id=norm_id,
                        publication_date=pub_date,
                        effective_date=effective_date,
                        paragraphs=tuple(current_article_paragraphs),
                    ),
                ),
            )
        )
        current_article_id = None
        current_article_title = None
        current_article_paragraphs = []

    def emit_section(css_class: str, text: str, sec_id: str) -> None:
        # Section-level headings live in their own block so the renderer can
        # nest them above the following articles.
        blocks.append(
            Block(
                id=sec_id,
                block_type="section",
                title=text,
                versions=(
                    Version(
                        norm_id=norm_id,
                        publication_date=pub_date,
                        effective_date=effective_date,
                        paragraphs=(Paragraph(css_class=css_class, text=text),),
                    ),
                ),
            )
        )

    section_counter = 0
    for line in lines:
        m = _ARTICULO_RE.match(line)
        if m:
            flush_article()
            current_article_id = _articulo_id(m.group(1))
            current_article_title = line.rstrip(".")
            current_article_paragraphs = [
                Paragraph(css_class="articulo", text=line)
            ]
            # Drain any pending section headings (none expected — we emit them eagerly)
            section_paragraph_buffer.clear()
            continue

        if _LIBRO_RE.match(line) or _TITULO_RE.match(line):
            flush_article()
            section_counter += 1
            emit_section("titulo_tit", line, f"sec-{section_counter}")
            continue
        if _CAPITULO_RE.match(line):
            flush_article()
            section_counter += 1
            emit_section("capitulo_tit", line, f"sec-{section_counter}")
            continue
        if _SECCION_RE.match(line):
            flush_article()
            section_counter += 1
            emit_section("seccion", line, f"sec-{section_counter}")
            continue

        if current_article_id is not None:
            current_article_paragraphs.append(
                Paragraph(css_class="parrafo", text=line)
            )
        # else: free-form preamble (fechas de promulgación, etc.) — drop for now.

    flush_article()
    return blocks


def _diputados_metadata(envelope: dict[str, Any], norm_id: str) -> NormMetadata:
    title = envelope["title"]
    pub_date = date.fromisoformat(envelope["publication_date"])
    last_reform = envelope.get("last_reform_date")
    last_modified = (
        date.fromisoformat(last_reform) if last_reform else None
    )
    rank = Rank(envelope.get("rank") or "ley")

    extra: list[tuple[str, str]] = [
        ("source_name", "diputados"),
        ("abbrev", envelope["abbrev"]),
    ]
    if envelope.get("doc_url"):
        extra.append(("doc_url", envelope["doc_url"]))
    if last_reform:
        extra.append(("last_reform_dof", last_reform))

    return NormMetadata(
        title=title,
        short_title=title.split(",")[0][:120].strip(),
        identifier=norm_id,
        country="mx",
        rank=rank,
        publication_date=pub_date,
        status=NormStatus.IN_FORCE,
        department="Cámara de Diputados",
        source=envelope["pdf_url"],
        last_modified=last_modified,
        pdf_url=envelope["pdf_url"],
        extra=tuple(extra),
    )


# ── Public parser classes ──────────────────────────────────────────────


class MXTextParser(TextParser):
    """Parse Mexican consolidated text. Dispatches per source via envelope."""

    def parse_text(self, data: bytes) -> list[Any]:
        envelope = _decode_envelope(data)
        if envelope["source"] == "diputados":
            return _diputados_blocks(envelope)
        raise NotImplementedError(
            f"MX text parser not wired for source '{envelope['source']}'."
        )


class MXMetadataParser(MetadataParser):
    """Parse Mexican norm metadata. Dispatches per source via envelope."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        envelope = _decode_envelope(data)
        if envelope["source"] == "diputados":
            return _diputados_metadata(envelope, norm_id)
        raise NotImplementedError(
            f"MX metadata parser not wired for source '{envelope['source']}'."
        )
