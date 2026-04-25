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

# Article-heading detector. Captures the article number/ordinal AND any text
# that follows on the same visual line (PDF wraps the title with body).
# Examples that match:
#   "Artículo 1o. En los Estados Unidos Mexicanos…"
#   "Artículo 4o.- La mujer y el hombre son iguales…"
#   "ARTÍCULO 123. Toda persona tiene derecho al trabajo…"
#   "Artículo Único.- Se reforma…"
_ARTICULO_RE = re.compile(
    r"^(Art[íi]culo|ART[ÍI]CULO)\s+"
    r"(?P<num>\d+(?:\s*[ºo°])?|[Úú]nico|Primero|Segundo|Tercero|Cuarto|Quinto|Sexto|S[ée]ptimo|Octavo|Noveno|D[ée]cimo|PRIMERO|SEGUNDO|TERCERO|CUARTO|QUINTO|SEXTO|S[ÉE]PTIMO|OCTAVO|NOVENO|D[ÉE]CIMO)"
    r"\s*(?P<sep>[.\-]+)?\s*"
    r"(?P<rest>.*)$",
)

# Section / chapter / title headings — for grouping.
# Requirements to avoid matching prose mid-sentence:
#   1. Must start with the exact keyword in ALL-CAPS (title-case prose like
#      "Capítulo, el reglamento…" is already excluded by the uppercase anchor).
#   2. After optional whitespace, must be followed ONLY by an ordinal word,
#      Roman numeral, digit, ALL-CAPS text, or end-of-line.  A comma
#      immediately after the keyword always means prose — reject it.
_SECTION_SUFFIX = (
    r"(?:\s+"
    r"(?:"
    r"[IVXLC]+[.\-]?"                                    # Roman numeral (I, II, IV…)
    r"|[0-9]+"                                           # arabic number
    r"|(?:PRIM|SEG|TERC|CUART|QUINT|SEXT|S[ÉE]PT|OCT|NOV|D[ÉE]C)\w*"  # ordinal stems
    r"|[A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ\s\d.\-]*"               # ALL-CAPS text
    r")"
    r")?\s*$"
)
_TITULO_RE = re.compile(r"^T[ÍI]TULO" + _SECTION_SUFFIX)
_CAPITULO_RE = re.compile(r"^CAP[ÍI]TULO" + _SECTION_SUFFIX)
_SECCION_RE = re.compile(r"^SECCI[ÓO]N" + _SECTION_SUFFIX)
_LIBRO_RE = re.compile(r"^LIBRO" + _SECTION_SUFFIX)
_TRANSITORIOS_RE = re.compile(
    r"^\s*ART[ÍI]CULOS?\s+TRANSITORIOS?(?:\s+DE\s+DECRETOS?\s+DE\s+REFORMA)?\s*$",
    re.IGNORECASE,
)

# Reform-provenance stamps Diputados injects after each amended fragment:
#   "Párrafo reformado DOF 04-12-2006, 10-06-2011"
#   "Artículo reformado DOF 14-08-2001"
#   "Fracción adicionada DOF 12-04-2019"
#   "Apartado A reformado DOF …"
# Tagged as nota_pie so the renderer emits them as quoted small text instead
# of being mistaken for actual law text.
_REFORM_STAMP_RE = re.compile(
    r"^\s*(?:p[áa]rrafo|art[íi]culo|fracci[óo]n|inciso|apartado|"
    r"subinciso|secci[óo]n|cap[íi]tulo|t[íi]tulo|fe\s+de\s+erratas)\b"
    r"[\s\S]*?\bDOF\s+\d{2}-\d{2}-\d{4}",
    re.IGNORECASE,
)

# Sub-article structure detectors. The Mexican federal style nests articles
# into Apartados (single capital letter), then fracciones (Roman numeral),
# then incisos (lowercase letter). PDF text-extraction does not preserve
# blank lines between these, so we use the leading marker to force a
# paragraph break.
_APARTADO_RE = re.compile(r"^[A-Z]\.\s+\S")
_FRACCION_RE = re.compile(
    r"^(?:[IVX]+|[ivx]+)[.\-\)]\s+\S",
    re.IGNORECASE,
)
_INCISO_RE = re.compile(r"^[a-z]\)\s+\S")


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


def _split_into_lines(pages: list[str]) -> list[str | None]:
    """Flatten pages into lines, preserving paragraph breaks as ``None``.

    Diputados PDFs repeat institutional banners on every page (the document
    title in all caps, "Cámara de Diputados…" footers, pagination markers,
    "Última Reforma DOF…" stamps). We drop them. Blank lines from the PDF
    survive as ``None`` so the block builder can use them as paragraph
    separators (otherwise every visual line wrap looks like a new para).
    Page boundaries are forced into ``None`` so a paragraph never silently
    spans pages of different content.
    """
    # First pass: count line frequencies so per-page chrome can be detected
    # even when the static patterns above don't catch it.
    counts: dict[str, int] = {}
    for page_text in pages:
        seen: set[str] = set()
        for raw in page_text.splitlines():
            line = _clean_line(raw)
            if line and line not in seen:
                seen.add(line)
                counts[line] = counts.get(line, 0) + 1
    repeating_chrome = {
        line for line, count in counts.items() if len(pages) > 2 and count >= 3
    }

    out: list[str | None] = []
    for page_idx, page_text in enumerate(pages):
        last_emitted_blank = True  # collapse leading blanks per page
        for raw in page_text.splitlines():
            line = _clean_line(raw)
            if not line:
                if not last_emitted_blank:
                    out.append(None)
                    last_emitted_blank = True
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
            last_emitted_blank = False
        # Force a paragraph break at every page boundary.
        if page_idx < len(pages) - 1 and not last_emitted_blank:
            out.append(None)

    return out


def _articulo_id(token: str) -> str:
    """Normalize an article identifier into a slug (e.g. '1o' → '1o', 'Único' → 'unico')."""
    t = token.lower().strip()
    t = re.sub(r"\s+[ºo°]$", "o", t)
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return t.replace(" ", "")


def _article_heading_text(num: str, sep: str | None) -> str:
    """Render a clean article heading from the captured number + separator."""
    head = f"Artículo {num.strip()}"
    if sep:
        sep = sep.strip()
        # Preserve the punctuation style the source used so the heading reads
        # naturally (some articles use ".-", others just ".").
        if sep.startswith("."):
            head += sep
        else:
            head += f" {sep}"
    else:
        head += "."
    return head


def _diputados_blocks(envelope: dict[str, Any]) -> list[Block]:
    """Build Block/Version trees from a Diputados PDF envelope.

    Single-snapshot: each Block has exactly one Version dated to the law's
    most recent DOF reform (the date in the Markdown frontmatter and on
    the resulting git commit). Real reform-by-reform history requires DOF
    integration and lives behind that adapter.
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
    line_stream = _split_into_lines(pages)

    blocks: list[Block] = []
    article_seq = 0  # used to disambiguate repeated "Artículo Único" in transitorios

    # State for the article currently being built
    current_article_id: str | None = None
    current_article_title: str | None = None
    current_article_paragraphs: list[Paragraph] = []
    pending_body_lines: list[str] = []
    pending_kind: str | None = None  # "body" | "stamp"

    def flush_pending_paragraph() -> None:
        """Merge accumulated lines into a single paragraph and tag it."""
        nonlocal pending_kind
        if not pending_body_lines:
            pending_kind = None
            return
        text = " ".join(pending_body_lines).strip()
        pending_body_lines.clear()
        kind = pending_kind or "body"
        pending_kind = None
        if not text or current_article_id is None:
            return
        css = "nota_pie" if kind == "stamp" else "parrafo"
        current_article_paragraphs.append(Paragraph(css_class=css, text=text))

    def flush_article() -> None:
        nonlocal current_article_id, current_article_title, current_article_paragraphs
        flush_pending_paragraph()
        if current_article_id is None:
            return
        blocks.append(
            Block(
                id=current_article_id,
                block_type="article",
                title=current_article_title or current_article_id,
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

    section_counter = 0

    def emit_section(css_class: str, text: str) -> None:
        nonlocal section_counter
        flush_article()
        section_counter += 1
        blocks.append(
            Block(
                id=f"sec-{section_counter}",
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

    for entry in line_stream:
        if entry is None:
            # PDF blank line / page break → end the running paragraph
            flush_pending_paragraph()
            continue

        line = entry

        if _TRANSITORIOS_RE.match(line):
            emit_section("titulo_tit", line)
            continue
        if _LIBRO_RE.match(line) or _TITULO_RE.match(line):
            emit_section("titulo_tit", line)
            continue
        if _CAPITULO_RE.match(line):
            emit_section("capitulo_tit", line)
            continue
        if _SECCION_RE.match(line):
            emit_section("seccion", line)
            continue

        m = _ARTICULO_RE.match(line)
        if m:
            flush_article()
            article_seq += 1
            num = m.group("num")
            sep = m.group("sep")
            rest = m.group("rest").strip()
            slug = _articulo_id(num)
            # Disambiguate repeated "Artículo Único" entries in transitorios
            # so each gets a unique block id (otherwise frontmatter slugs collide).
            current_article_id = f"art-{slug}-{article_seq}"
            current_article_title = _article_heading_text(num, sep)
            current_article_paragraphs = [
                Paragraph(css_class="articulo", text=current_article_title)
            ]
            if rest:
                # The article's first body line was on the same visual line as
                # the heading. Seed the pending paragraph with it.
                pending_body_lines.append(rest)
            continue

        if current_article_id is None:
            # Free-form preamble (decree title, promulgation block) — drop.
            continue

        is_stamp = bool(_REFORM_STAMP_RE.match(line))
        is_sub_marker = bool(
            _APARTADO_RE.match(line)
            or _FRACCION_RE.match(line)
            or _INCISO_RE.match(line)
        )

        # Force a paragraph break when:
        #   - the running paragraph is a stamp and we just hit body text (or vice versa)
        #   - the new line is a sub-article marker (Apartado / fracción / inciso)
        if pending_body_lines:
            switching_kind = (
                (pending_kind == "stamp" and not is_stamp)
                or (pending_kind == "body" and is_stamp)
            )
            if switching_kind or is_sub_marker:
                flush_pending_paragraph()

        if is_stamp:
            pending_kind = "stamp"
        elif pending_kind is None:
            pending_kind = "body"
        pending_body_lines.append(line)

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
