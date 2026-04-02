"""Parser for Chilean BCN XML documents (Ley Chile).

XML schema: http://www.leychile.cl/esquemas/EsquemaIntercambioNorma-v1-0.xsd
Namespace: http://www.leychile.cl/esquemas

Structure:
  <Norma normaId="..." derogado="..." fechaVersion="...">
    <Identificador fechaPromulgacion="..." fechaPublicacion="...">
      <TiposNumeros><TipoNumero><Tipo/><Numero/></TipoNumero></TiposNumeros>
      <Organismos><Organismo/></Organismos>
    </Identificador>
    <Metadatos>
      <TituloNorma/> <Materias/> <NombresUsoComun/>
    </Metadatos>
    <Encabezado><Texto/></Encabezado>
    <EstructurasFuncionales>
      <EstructuraFuncional tipoParte="Capítulo|Artículo|..." idParte="..." derogado="...">
        <Texto/>
        <Metadatos><NombreParte/><TituloParte/></Metadatos>
        <EstructurasFuncionales>...</EstructurasFuncionales>  <!-- nested -->
      </EstructuraFuncional>
    </EstructurasFuncionales>
    <Promulgacion><Texto/></Promulgacion>
  </Norma>
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any
from xml.etree import ElementTree as ET

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import (
    Block,
    NormMetadata,
    NormStatus,
    Paragraph,
    Rank,
    Version,
)

NS = "http://www.leychile.cl/esquemas"


def _tag(name: str) -> str:
    """Return namespaced tag."""
    return f"{{{NS}}}{name}"


def _find_text(el: ET.Element, path: str) -> str:
    """Find a child element and return its text, or empty string."""
    child = el.find(path)
    if child is not None and child.text:
        return child.text.strip()
    return ""


def _parse_date(s: str) -> date | None:
    """Parse YYYY-MM-DD date string."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _clean_text(text: str) -> str:
    """Clean BCN article text: strip margin annotations and normalize whitespace.

    BCN formats some consolidated texts (esp. the Constitution) with reform
    references in a right column, separated by 6+ spaces from the legal text.
    Example:  "La familia es el núcleo                  CPR Art. 1° D.O."
    These are layout artifacts from the Diario Oficial and must be removed.
    """
    # Patterns that identify margin annotations (reform references)
    _ANNOTATION_RE = re.compile(
        r"(?:CPR|LEY|D\.O\.|Art\.|Nº|N°|\d{2}\.\d{2}\.\d{4}"
        r"|único|D\.S\.|D\.F\.L\.|D\.L\.)"
    )
    # Pure annotation continuation lines (e.g., "Nº1 D.O. 16.06.1999")
    _CONTINUATION_RE = re.compile(r"^(?:Nº\d|único|N°\d|\d{2}\.\d{2}\.\d{4}|D\.O\.\s)")

    lines = text.split("\n")
    cleaned = []
    for line in lines:
        line = line.rstrip()
        # Check for content + 6+ spaces + annotation on the same line
        match = re.match(r"^(.+?)\s{6,}(.+)$", line)
        if match:
            main_part = match.group(1).rstrip()
            margin_part = match.group(2).strip()
            if _ANNOTATION_RE.search(margin_part):
                cleaned.append(main_part)
                continue
        else:
            # Check if the entire line is a continuation of a margin annotation
            stripped = line.strip()
            if stripped and _CONTINUATION_RE.match(stripped):
                continue
        cleaned.append(line.strip())
    return "\n".join(cleaned)


# ── Text Parser ──


class CLTextParser(TextParser):
    """Parses BCN XML into Block/Version/Paragraph objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse XML text into structural blocks.

        Recursively walks EstructuraFuncional elements.
        Each article-level element becomes a Block.
        Chapter/title elements become heading Blocks.
        """
        root = ET.fromstring(data)
        norma_id = root.get("normaId", "unknown")

        blocks: list[Block] = []

        # Encabezado (header/preamble) — optional block
        encabezado = root.find(_tag("Encabezado"))
        if encabezado is not None:
            text = _find_text(encabezado, _tag("Texto"))
            if text:
                fecha = _parse_date(encabezado.get("fechaVersion", "")) or date(1900, 1, 1)
                version = Version(
                    norm_id=norma_id,
                    publication_date=fecha,
                    effective_date=fecha,
                    paragraphs=(Paragraph(css_class="encabezado", text=_clean_text(text)),),
                )
                blocks.append(
                    Block(
                        id=f"{norma_id}-encabezado",
                        block_type="encabezado",
                        title="Encabezado",
                        versions=(version,),
                    )
                )

        # EstructurasFuncionales — recursive
        efs = root.find(_tag("EstructurasFuncionales"))
        if efs is not None:
            blocks.extend(self._parse_estructuras(efs, norma_id))

        return blocks

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform timeline from BCN XML.

        BCN tracks amendments via Vinculaciones (separate endpoint).
        From the XML itself we can extract version dates per article.
        """
        blocks = self.parse_text(data)
        from legalize.transformer.xml_parser import extract_reforms

        return extract_reforms(blocks)

    def _parse_estructuras(self, parent: ET.Element, norma_id: str) -> list[Block]:
        """Recursively parse EstructuraFuncional elements into Blocks."""
        blocks: list[Block] = []

        for ef in parent.findall(_tag("EstructuraFuncional")):
            tipo_parte = ef.get("tipoParte", "")
            id_parte = ef.get("idParte", "")
            fecha_str = ef.get("fechaVersion", "")
            fecha = _parse_date(fecha_str) or date(1900, 1, 1)

            # Extract text and metadata
            texto = _find_text(ef, _tag("Texto"))
            meta_el = ef.find(_tag("Metadatos"))
            nombre_parte = ""
            titulo_parte = ""
            if meta_el is not None:
                np = meta_el.find(_tag("NombreParte"))
                if np is not None and np.get("presente") == "si" and np.text:
                    nombre_parte = np.text.strip()
                tp = meta_el.find(_tag("TituloParte"))
                if tp is not None and tp.get("presente") == "si" and tp.text:
                    titulo_parte = tp.text.strip()

            # Build block title
            title = titulo_parte or nombre_parte or tipo_parte
            if tipo_parte in ("Artículo",) and nombre_parte:
                title = f"Artículo {nombre_parte}"

            # Block type mapping
            block_type = _map_tipo_parte(tipo_parte)

            # Create paragraphs from text
            paragraphs: tuple[Paragraph, ...] = ()
            if texto:
                paragraphs = (Paragraph(css_class=block_type, text=_clean_text(texto)),)

            if paragraphs:
                version = Version(
                    norm_id=norma_id,
                    publication_date=fecha,
                    effective_date=fecha,
                    paragraphs=paragraphs,
                )
                blocks.append(
                    Block(
                        id=id_parte or f"{norma_id}-{block_type}",
                        block_type=block_type,
                        title=title,
                        versions=(version,),
                    )
                )

            # Recurse into nested EstructurasFuncionales
            nested = ef.find(_tag("EstructurasFuncionales"))
            if nested is not None:
                blocks.extend(self._parse_estructuras(nested, norma_id))

        return blocks


# ── Metadata Parser ──


# Map BCN Tipo values to rank strings
RANK_MAP: dict[str, str] = {
    "Ley": "ley",
    "Código": "codigo",
    "Decreto con Fuerza de Ley": "dfl",
    "Decreto Ley": "dl",
    "Decreto": "decreto",
    "Decreto Supremo": "decreto_supremo",
    "Tratado Internacional": "tratado",
    "Ley Orgánica Constitucional": "ley_organica_constitucional",
    "Ley de Quórum Calificado": "ley_quorum_calificado",
    "Ley Interpretativa": "ley_interpretativa",
    "Resolución": "resolucion",
    "Reglamento": "reglamento",
    "Mensaje": "mensaje",
}


class CLMetadataParser(MetadataParser):
    """Parses BCN XML metadata into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse the XML response for a norm into NormMetadata.

        norm_id is the BCN idNorma (numeric string).
        """
        root = ET.fromstring(data)

        # Root attributes
        derogado = root.get("derogado", "no derogado")

        # Identificador
        ident = root.find(_tag("Identificador"))
        fecha_pub_str = ident.get("fechaPublicacion", "") if ident is not None else ""
        fecha_prom_str = ident.get("fechaPromulgacion", "") if ident is not None else ""
        pub_date = _parse_date(fecha_pub_str) or _parse_date(fecha_prom_str) or date(1900, 1, 1)

        # Tipo and Numero from TiposNumeros
        tipo = ""
        numero = ""
        if ident is not None:
            tn = ident.find(f"{_tag('TiposNumeros')}/{_tag('TipoNumero')}")
            if tn is not None:
                tipo = _find_text(tn, _tag("Tipo"))
                numero = _find_text(tn, _tag("Numero"))

        # Organismo
        organismo = ""
        if ident is not None:
            organismo = _find_text(ident, f"{_tag('Organismos')}/{_tag('Organismo')}")

        # Metadatos
        meta = root.find(_tag("Metadatos"))
        titulo = _find_text(meta, _tag("TituloNorma")) if meta is not None else ""

        # NombresUsoComun — preferred short title
        short_title = ""
        if meta is not None:
            short_title = _find_text(meta, f"{_tag('NombresUsoComun')}/{_tag('NombreUsoComun')}")

        # Materias
        subjects: tuple[str, ...] = ()
        if meta is not None:
            materias = meta.find(_tag("Materias"))
            if materias is not None:
                subjects = tuple(
                    m.text.strip() for m in materias.findall(_tag("Materia")) if m.text
                )

        # Rank mapping
        rank_str = RANK_MAP.get(tipo, "otro")

        # Status
        status = (
            NormStatus.REPEALED
            if "derogado" not in derogado or derogado == "derogado"
            else NormStatus.IN_FORCE
        )
        # "no derogado" → IN_FORCE, "derogado" → REPEALED
        if derogado == "no derogado":
            status = NormStatus.IN_FORCE
        else:
            status = NormStatus.REPEALED

        # Source URL
        source_url = f"https://www.leychile.cl/Navegar?idNorma={norm_id}"

        return NormMetadata(
            title=titulo or f"{tipo} {numero}".strip(),
            short_title=short_title or titulo or "",
            identifier=f"CL-{norm_id}",
            country="cl",
            rank=Rank(rank_str),
            publication_date=pub_date,
            status=status,
            department=organismo,
            source=source_url,
            subjects=subjects,
        )


def _map_tipo_parte(tipo_parte: str) -> str:
    """Map BCN tipoParte to a generic block_type string."""
    mapping = {
        "Artículo": "articulo",
        "Capítulo": "capitulo",
        "Título": "titulo",
        "Párrafo": "parrafo",
        "Disposición Transitoria": "transitoria",
        "Libro": "libro",
        "Sección": "seccion",
    }
    return mapping.get(tipo_parte, "otro")
