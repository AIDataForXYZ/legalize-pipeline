"""Parser for IMPO JSON documents (Uruguay).

IMPO returns JSON (Latin-1 encoded) with ?json=true on any norm URL.
Top-level fields: tipoNorma, nroNorma, anioNorma, nombreNorma,
fechaPublicacion, fechaPromulgacion, firmantes, articulos[].
Article fields: nroArticulo, textoArticulo, titulosArticulo,
notasArticulo, urlArticulo.
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

# Map IMPO tipoNorma to internal rank values
TIPO_TO_RANK: dict[str, str] = {
    "ley": "ley",
    "decreto ley": "decreto_ley",
    "decreto-ley": "decreto_ley",
    "decreto": "decreto",
    "constitucion": "constitucion",
    "constitucion de la republica": "constitucion",
    "codigo": "codigo",
    "resolucion": "resolucion",
    "reglamento": "reglamento",
    "ordenanza departamental": "ordenanza_departamental",
}


def _parse_date(s: str) -> date | None:
    """Parse DD/MM/YYYY date string (Uruguayan format)."""
    if not s or not s.strip():
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _strip_html(s: str) -> str:
    """Remove HTML tags from a string."""
    if not s:
        return ""
    return re.sub(r"<[^>]+>", "", s).strip()


def _decode_json(data: bytes) -> dict:
    """Decode IMPO JSON, trying Latin-1 first then UTF-8."""
    for encoding in ("latin-1", "utf-8"):
        try:
            return json.loads(data.decode(encoding))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    raise ValueError("Could not decode IMPO JSON response")


def _extract_section_titles(raw: str) -> list[str]:
    """Extract section/chapter titles from titulosArticulo field.

    IMPO uses <br> as separator between nested titles.
    """
    if not raw:
        return []
    parts = re.split(r"<br\s*/?>", raw, flags=re.IGNORECASE)
    return [_strip_html(p).strip() for p in parts if _strip_html(p).strip()]


def _make_identifier(doc: dict, collection: str) -> str:
    """Build the filesystem identifier from IMPO JSON fields.

    Examples:
        UY-ley-19996
        UY-decreto-ley-14261
        UY-constitucion-1967
    """
    tipo = (doc.get("tipoNorma") or "").strip().lower()
    nro = (doc.get("nroNorma") or "").strip()
    anio = doc.get("anioNorma")

    # Map collection path to identifier prefix
    if "constitucion" in collection or "constitucion" in tipo:
        year = anio or _extract_year_from_collection(collection)
        return f"UY-constitucion-{year}"

    if "decretos-ley" in collection or ("decreto" in tipo and "ley" in tipo):
        return f"UY-decreto-ley-{nro}" if nro else "UY-decreto-ley-unknown"

    if "leyes" in collection or "ley" in tipo:
        return f"UY-ley-{nro}" if nro else "UY-ley-unknown"

    if "decreto" in collection or "decreto" in tipo:
        year_suffix = f"-{anio}" if anio else ""
        return f"UY-decreto-{nro}{year_suffix}" if nro else "UY-decreto-unknown"

    # Fallback
    slug = re.sub(r"[^a-z0-9]+", "-", tipo).strip("-") if tipo else "unknown"
    return f"UY-{slug}-{nro}" if nro else f"UY-{slug}"


def _extract_year_from_collection(collection: str) -> str:
    """Extract year from collection path like 'constitucion/1967-1967'."""
    match = re.search(r"(\d{4})", collection)
    return match.group(1) if match else "unknown"


def _heading_css_class(title: str) -> str:
    """Map a section/chapter title to a CSS class the markdown renderer knows.

    Uses the existing CSS→Markdown mapping from transformer/markdown.py:
    - titulo → ## (TITULO, LIBRO)
    - capitulo_tit → ### (CAPITULO)
    - seccion → #### (SECCION, SUBSECCION)
    """
    upper = title.upper()
    if upper.startswith(("TITULO", "LIBRO", "PARTE")):
        return "titulo"
    if upper.startswith("CAPITULO"):
        return "capitulo_tit"
    # SECCION, SUBSECCION, and anything else
    return "seccion"


class IMPOTextParser(TextParser):
    """Parses IMPO JSON into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse IMPO JSON into a list of Block objects.

        Each article becomes one Block with a single Version.
        Section/chapter headings from titulosArticulo are emitted
        as separate heading Blocks before their first article.
        """
        if not data:
            return []

        doc = _decode_json(data)
        articulos = doc.get("articulos", [])
        if not articulos:
            return []

        pub_date = _parse_date(doc.get("fechaPublicacion", "")) or date(1900, 1, 1)
        norm_identifier = doc.get("nroNorma", "unknown")

        blocks: list[Block] = []

        for art in articulos:
            nro = (art.get("nroArticulo") or "").strip()
            texto = (art.get("textoArticulo") or "").strip()
            titulos_raw = art.get("titulosArticulo", "")

            # Emit section/chapter heading blocks using css_classes
            # that the markdown renderer already recognizes
            section_titles = _extract_section_titles(titulos_raw)
            for title in section_titles:
                css = _heading_css_class(title)
                heading_id = f"heading-{nro}"
                heading_version = Version(
                    norm_id=norm_identifier,
                    publication_date=pub_date,
                    effective_date=pub_date,
                    paragraphs=(Paragraph(css_class=css, text=title),),
                )
                blocks.append(
                    Block(
                        id=heading_id,
                        block_type="heading",
                        title=title,
                        versions=(heading_version,),
                    )
                )

            # Skip articles with only "(*)" (cross-reference placeholder)
            if texto == "(*)":
                paragraphs = (Paragraph(css_class="articulo", text="(*)"),)
            else:
                # Build paragraphs from the article text only
                # Notes (notasArticulo) are IMPO editorial content
                # (cross-references, amendment history), not legal text
                cleaned = _strip_html(texto)
                paragraphs = (Paragraph(css_class="articulo", text=cleaned),) if cleaned else ()

            if not paragraphs:
                continue

            art_id = f"art-{nro}" if nro else f"art-{len(blocks)}"
            art_title = art.get("tituloArticulo", "") or f"Articulo {nro}"
            art_title = _strip_html(art_title).strip()

            version = Version(
                norm_id=norm_identifier,
                publication_date=pub_date,
                effective_date=pub_date,
                paragraphs=tuple(paragraphs),
            )

            blocks.append(
                Block(
                    id=art_id,
                    block_type="articulo",
                    title=art_title,
                    versions=(version,),
                )
            )

        return blocks

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform points from IMPO JSON.

        IMPO provides consolidated text only — no point-in-time history.
        Returns a single-entry list with the publication date.
        """
        if not data:
            return []

        doc = _decode_json(data)
        pub_date = _parse_date(doc.get("fechaPublicacion", ""))
        if not pub_date:
            return []

        from legalize.models import Reform

        block_ids = [
            f"art-{(a.get('nroArticulo') or '').strip()}" for a in doc.get("articulos", [])
        ]
        return [Reform(date=pub_date, norm_id="bootstrap", affected_blocks=tuple(block_ids))]


class IMPOMetadataParser(MetadataParser):
    """Parses IMPO JSON metadata into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse IMPO JSON into NormMetadata.

        Args:
            data: Raw JSON bytes from IMPO.
            norm_id: URL path segment, e.g. "leyes/19996-2021".
        """
        if not data:
            raise ValueError(f"Empty data for {norm_id}")

        doc = _decode_json(data)

        tipo_raw = (doc.get("tipoNorma") or "").strip()
        nombre = (doc.get("nombreNorma") or "").strip()
        nro = (doc.get("nroNorma") or "").strip()

        # Build title: "Ley N 19996 - Nombre de la ley"
        if nombre:
            title = f"{tipo_raw} N\u00b0 {nro} - {nombre}" if nro else nombre
        elif nro:
            title = f"{tipo_raw} N\u00b0 {nro}"
        else:
            title = tipo_raw or norm_id

        short_title = nombre or title

        identifier = _make_identifier(doc, norm_id)
        rank_str = TIPO_TO_RANK.get(tipo_raw.lower(), "otro")
        pub_date = _parse_date(doc.get("fechaPublicacion", "")) or date(1900, 1, 1)

        # Status: check if any article has derogation notes
        status = NormStatus.IN_FORCE

        firmantes = (doc.get("firmantes") or "").strip()
        source_url = f"https://www.impo.com.uy/bases/{norm_id}"

        return NormMetadata(
            title=title,
            short_title=short_title,
            identifier=identifier,
            country="uy",
            rank=Rank(rank_str),
            publication_date=pub_date,
            status=status,
            department=firmantes,
            source=source_url,
        )
