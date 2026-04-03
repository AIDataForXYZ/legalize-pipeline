"""Parser for Portuguese DRE legislation.

Parses HTML text from dre.tretas.org SQLite dump and JSON metadata
into the generic Block/NormMetadata data model.

Text structure of Portuguese laws:
    Parte → Titulo → Capitulo → Seccao → Subseccao → Artigo → Numero → Alinea
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

# ─── Type code mapping for filesystem-safe identifiers ───

TYPE_CODE_MAP: dict[str, str] = {
    "LEI": "L",
    "LEI CONSTITUCIONAL": "LC",
    "LEI ORGÂNICA": "LO",
    "DECRETO LEI": "DL",
    "DECRETO-LEI": "DL",
    "DECRETO REGULAMENTAR": "DR",
    "DECRETO REGULAMENTAR REGIONAL": "DRR",
    "DECRETO LEGISLATIVO REGIONAL": "DLR",
    "DECRETO": "D",
    "PORTARIA": "P",
    "DESPACHO NORMATIVO": "DN",
    "DESPACHO": "DSP",
    "RESOLUÇÃO": "R",
    "RESOLUÇÃO DO CONSELHO DE MINISTROS": "RCM",
    "RESOLUÇÃO DA ASSEMBLEIA DA REPÚBLICA": "RAR",
    "AVISO": "AV",
    "DECLARAÇÃO": "DCL",
    "DECLARAÇÃO DE RECTIFICAÇÃO": "DCLR",
    "ACÓRDÃO": "AC",
}

# ─── Rank mapping ───

RANK_MAP: dict[str, str] = {
    "LEI": "lei",
    "LEI CONSTITUCIONAL": "lei-constitucional",
    "LEI ORGÂNICA": "lei-organica",
    "DECRETO LEI": "decreto-lei",
    "DECRETO-LEI": "decreto-lei",
    "DECRETO REGULAMENTAR": "decreto-regulamentar",
    "DECRETO REGULAMENTAR REGIONAL": "decreto-regulamentar-regional",
    "DECRETO LEGISLATIVO REGIONAL": "decreto-legislativo-regional",
    "DECRETO": "decreto",
    "PORTARIA": "portaria",
    "DESPACHO NORMATIVO": "despacho-normativo",
    "DESPACHO": "despacho",
    "RESOLUÇÃO": "resolucao",
    "RESOLUÇÃO DO CONSELHO DE MINISTROS": "resolucao-cm",
    "RESOLUÇÃO DA ASSEMBLEIA DA REPÚBLICA": "resolucao-ar",
    "AVISO": "aviso",
    "DECLARAÇÃO": "declaracao",
    "DECLARAÇÃO DE RECTIFICAÇÃO": "declaracao-rectificacao",
    "ACÓRDÃO": "acordao",
}

# ─── Structural regex patterns ───

# Article heading: "Artigo 1.º", "Artigo 123.º-A", "Artigo único"
_RE_ARTIGO = re.compile(r"^(Artigo\s+(?:\d+\.º(?:-[A-Z]+)?|único))\s*$", re.IGNORECASE)

# Article heading with inline title: "Artigo 1.º Objeto"
_RE_ARTIGO_WITH_TITLE = re.compile(
    r"^(Artigo\s+(?:\d+\.º(?:-[A-Z]+)?|único))\s+(.+)$", re.IGNORECASE
)

# Structural divisions (case-insensitive)
_RE_PARTE = re.compile(r"^(PARTE\s+[IVXLCDM]+)\b(.*)$", re.IGNORECASE)
_RE_LIVRO = re.compile(r"^(LIVRO\s+[IVXLCDM]+)\b(.*)$", re.IGNORECASE)
_RE_TITULO = re.compile(r"^(TÍTULO\s+[IVXLCDM]+)\b(.*)$", re.IGNORECASE)
_RE_CAPITULO = re.compile(r"^(CAPÍTULO\s+[IVXLCDM]+)\b(.*)$", re.IGNORECASE)
_RE_SECCAO = re.compile(r"^(SECÇÃO\s+[IVXLCDM]+)\b(.*)$", re.IGNORECASE)
_RE_SUBSECCAO = re.compile(r"^(SUBSECÇÃO\s+[IVXLCDM]+)\b(.*)$", re.IGNORECASE)


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    # Common HTML entities
    text = text.replace("&amp;", "&")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("&quot;", '"')
    text = text.replace("&#39;", "'")
    text = text.replace("&nbsp;", " ")
    return text


def _clean_line(line: str) -> str:
    """Strip and normalize whitespace in a single line."""
    return re.sub(r"\s+", " ", line).strip()


def _classify_line(line: str) -> tuple[str, str, str]:
    """Classify a text line into (block_type, css_class, text).

    Returns:
        (block_type, css_class, cleaned_text)
        block_type: 'parte', 'livro', 'titulo', 'capitulo', 'seccao',
                    'subseccao', 'artigo', or 'text'
        css_class: the Paragraph css_class for markdown rendering
    """
    # Structural divisions (largest to smallest)
    for regex, btype, css in [
        (_RE_PARTE, "parte", "titulo_tit"),
        (_RE_LIVRO, "livro", "titulo_tit"),
        (_RE_TITULO, "titulo", "titulo_tit"),
        (_RE_CAPITULO, "capitulo", "capitulo_tit"),
        (_RE_SECCAO, "seccao", "seccion"),
        (_RE_SUBSECCAO, "subseccao", "seccion"),
    ]:
        m = regex.match(line)
        if m:
            label = m.group(1).strip()
            rest = m.group(2).strip().lstrip("—–-").strip()
            text = f"{label} — {rest}" if rest else label
            return btype, css, text

    # Article with inline title
    m = _RE_ARTIGO_WITH_TITLE.match(line)
    if m:
        return "artigo", "articulo", f"{m.group(1)} — {m.group(2)}"

    # Article heading only
    m = _RE_ARTIGO.match(line)
    if m:
        return "artigo", "articulo", m.group(1)

    return "text", "parrafo", line


def _parse_text_to_blocks(html: str, pub_date: date, norm_id: str) -> list[Block]:
    """Parse HTML text into Block objects.

    Each article becomes a Block. Structural headings (Parte, Titulo,
    Capitulo, Seccao) become their own Blocks with heading paragraphs.

    Args:
        html: Raw HTML text from dreapp_documenttext.
        pub_date: Publication date for the Version.
        norm_id: Source norm identifier for the Version.

    Returns:
        List of Block objects.
    """
    plain = _strip_html(html)
    lines = plain.split("\n")

    blocks: list[Block] = []
    current_paragraphs: list[Paragraph] = []
    current_id = ""
    current_type = ""
    current_title = ""
    block_counter = 0

    def _flush_block():
        nonlocal current_paragraphs, current_id, current_type, current_title
        if not current_paragraphs:
            return
        # Skip blocks that are only whitespace
        if all(not p.text.strip() for p in current_paragraphs):
            current_paragraphs = []
            return

        version = Version(
            norm_id=norm_id,
            publication_date=pub_date,
            effective_date=pub_date,
            paragraphs=tuple(current_paragraphs),
        )
        blocks.append(
            Block(
                id=current_id,
                block_type=current_type,
                title=current_title,
                versions=(version,),
            )
        )
        current_paragraphs = []

    for raw_line in lines:
        line = _clean_line(raw_line)
        if not line:
            continue

        btype, css, text = _classify_line(line)

        if btype in ("parte", "livro", "titulo", "capitulo", "seccao", "subseccao"):
            # Structural heading: flush current block, create heading block
            _flush_block()
            block_counter += 1
            current_id = f"{btype}-{block_counter}"
            current_type = btype
            current_title = text
            current_paragraphs = [Paragraph(css_class=css, text=text)]
            _flush_block()

        elif btype == "artigo":
            # New article: flush previous, start new block
            _flush_block()
            block_counter += 1
            current_id = f"art-{block_counter}"
            current_type = "artigo"
            current_title = text
            current_paragraphs = [Paragraph(css_class="articulo", text=text)]

        else:
            # Body text paragraph
            if not current_id:
                # Preamble text before first structural element
                block_counter += 1
                current_id = f"preambulo-{block_counter}"
                current_type = "preambulo"
                current_title = "Preâmbulo"
            current_paragraphs.append(Paragraph(css_class="parrafo", text=text))

    # Flush final block
    _flush_block()

    return blocks


class DRETextParser(TextParser):
    """Parses Portuguese DRE HTML text into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse HTML text from tretas.org dump into Block objects.

        The data is raw HTML from dreapp_documenttext.text.
        Since we don't have version history from the SQLite dump,
        each block gets a single Version with the publication date.

        We store a placeholder date/norm_id — the MetadataParser provides
        the real values, and the pipeline reconciles them in extract_reforms.
        """
        html = data.decode("utf-8", errors="replace")

        if not html.strip():
            return []

        # Use placeholder values — the pipeline fills in real dates
        # via extract_reforms() from the blocks' version data
        return _parse_text_to_blocks(
            html,
            pub_date=date(1900, 1, 1),
            norm_id="PLACEHOLDER",
        )

    def parse_text_with_date(self, data: bytes, pub_date: date, norm_id: str) -> list[Any]:
        """Parse with known publication date and norm_id.

        Called when metadata is available before text parsing.
        """
        html = data.decode("utf-8", errors="replace")
        if not html.strip():
            return []
        return _parse_text_to_blocks(html, pub_date, norm_id)


def _parse_date(s: str) -> date | None:
    """Parse a date string in common formats."""
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _make_identifier(doc_type: str, number: str) -> str:
    """Build a filesystem-safe identifier from doc_type and number.

    Examples:
        ("LEI", "39/2016") → "DRE-L-39-2016"
        ("DECRETO LEI", "111-A/2017") → "DRE-DL-111-A-2017"
        ("PORTARIA", "180/2024") → "DRE-P-180-2024"
    """
    type_upper = doc_type.strip().upper()
    code = TYPE_CODE_MAP.get(type_upper, "X")

    # Normalize number: replace / with - and strip whitespace
    safe_number = number.strip().replace("/", "-").replace(" ", "-")

    # Remove any remaining filesystem-unsafe characters
    safe_number = re.sub(r"[^a-zA-Z0-9\-]", "", safe_number)

    if not safe_number:
        return f"DRE-{code}-UNKNOWN"

    return f"DRE-{code}-{safe_number}"


class DREMetadataParser(MetadataParser):
    """Parses tretas.org document metadata JSON into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse JSON metadata from DREClient.get_metadata().

        Args:
            data: JSON bytes from the client.
            norm_id: The claint ID (used as fallback identifier).
        """
        meta = json.loads(data)

        doc_type = meta.get("doc_type", "").strip()
        number = meta.get("number", "").strip()
        date_str = meta.get("date", "")
        pub_date = _parse_date(date_str) or date(1900, 1, 1)

        identifier = _make_identifier(doc_type, number)

        # Determine rank
        type_upper = doc_type.upper()
        rank_str = RANK_MAP.get(type_upper, "outro")

        # Determine status
        in_force = meta.get("in_force", True)
        status = NormStatus.IN_FORCE if in_force else NormStatus.REPEALED

        # Build title: "Lei 39/2016, de 19 de Dezembro"
        # The notes field from the source contains the summary/abstract
        summary = meta.get("notes", "").strip()
        title = f"{doc_type.title()} {number}" if number else doc_type.title()
        short_title = summary[:120].rstrip(".") if summary else title

        # Department from emiting_body (semicolon-separated)
        emiting = meta.get("emiting_body", "").strip()
        department = ", ".join(part.strip().title() for part in emiting.split(";") if part.strip())

        # Source URL priority: ELI > dre_pdf > tretas.org fallback
        eli = meta.get("eli", "")
        dre_pdf = meta.get("dre_pdf", "")
        source = eli or dre_pdf or f"https://dre.tretas.org/dre/{norm_id}/"

        # Portugal-specific extra fields for frontmatter
        extra: list[tuple[str, str]] = []
        if department:
            extra.append(("department", department))
        if summary:
            extra.append(("summary", summary[:500]))
        if eli and eli != source:
            extra.append(("eli", eli))

        return NormMetadata(
            title=title,
            short_title=short_title,
            identifier=identifier,
            country="pt",
            rank=Rank(rank_str),
            publication_date=pub_date,
            status=status,
            department=department or "Diario da Republica",
            source=source,
            summary=summary,
            pdf_url=dre_pdf if dre_pdf else None,
            extra=tuple(extra),
        )
