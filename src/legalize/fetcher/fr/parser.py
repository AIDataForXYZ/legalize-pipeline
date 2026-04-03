"""Parsers LEGI — consolidated text and metadata from the LEGI database.

Parses the combined XML built by LEGIClient.get_text()
(<legi_combined> format) and the metadata from the structure file.

Reference: Archéo-Lex (github.com/Legilibre/Archeo-Lex) for the
XML structure of the LEGI dump.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from lxml import etree

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
# LEGI NATURE → Rank mapping
# ─────────────────────────────────────────────

_NATURE_TO_RANK: dict[str, Rank] = {
    "CODE": Rank.CODE,
    "CONSTITUTION": Rank.CONSTITUTION_FR,
    "LOI": Rank.LOI,
    "LOI ORGANIQUE": Rank.LOI_ORGANIQUE,
    "LOI_ORGANIQUE": Rank.LOI_ORGANIQUE,
    "ORDONNANCE": Rank.ORDONNANCE,
    "DECRET": Rank.DECRET,
    "DÉCRET": Rank.DECRET,
}

# ─────────────────────────────────────────────
# Section niv → CSS class for markdown mapping
# ─────────────────────────────────────────────

_SECTION_NIV_CSS: dict[str, str] = {
    "1": "titulo_tit",  # ## heading
    "2": "capitulo_tit",  # ### heading
    "3": "seccion",  # #### heading
    "4": "seccion",  # #### heading (sub-section)
}


# ─────────────────────────────────────────────
# Date helpers (same pattern as xml_parser._parse_date)
# ─────────────────────────────────────────────


def _parse_date_legi(date_str: str) -> date | None:
    """Parses LEGI dates. Returns None for sentinels and invalid dates.

    Actual format of the LEGI dump (verified with 2026-03-27 data):
    - Dates in YYYY-MM-DD format (ISO 8601), e.g.: "2008-07-24"
    - Sentinel: "2999-01-01" (= indefinite validity) → None
    - Also accepts YYYYMMDD (some LEGI sources use this format)
    - Years > 2100 → None (covers sentinel 2999 and invalid future dates)
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    if not date_str or date_str in ("99999999", "2999-01-01"):
        return None
    try:
        # Try ISO 8601 first (actual dump format)
        if "-" in date_str:
            parsed = date.fromisoformat(date_str)
        elif len(date_str) >= 8:
            # Fallback YYYYMMDD (just in case)
            parsed = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        else:
            return None
        if parsed.year > 2100:
            return None
        return parsed
    except ValueError:
        logger.debug("Could not parse date: %s", date_str)
        return None


# ─────────────────────────────────────────────
# Text extraction from CONTENU
# ─────────────────────────────────────────────


def _extract_text_legi(element: etree._Element) -> str:
    """Extracts text from a LEGI element, including inline sub-elements.

    Actual format of CONTENU in the LEGI dump (verified):
    - <p> with text and inline elements
    - <b>, <i>, <strong>, <em> → Markdown bold/italic
    - <br/> → newline
    - <blockquote> → indented text (common in articles that modify others)
    - <div> → container (treat as block)
    - <a> → text only (no link)
    - <sup> → plain text (note numbers)
    - <table>, <tr>, <td> → text separated by pipes
    """
    parts: list[str] = []

    if element.text:
        parts.append(element.text)

    for child in element:
        tag = etree.QName(child.tag).localname if isinstance(child.tag, str) else ""
        tag_lower = tag.lower()

        if tag_lower in ("b", "strong"):
            inner = _extract_text_legi(child)
            if inner.strip():
                parts.append(f"**{inner.strip()}**")
        elif tag_lower in ("i", "em"):
            inner = _extract_text_legi(child)
            if inner.strip():
                parts.append(f"*{inner.strip()}*")
        elif tag_lower == "br":
            parts.append("\n")
        elif tag_lower == "blockquote":
            inner = _extract_text_legi(child)
            if inner.strip():
                # Indent each line of the blockquote
                quoted = "\n".join(f"> {line}" for line in inner.strip().splitlines())
                parts.append(f"\n{quoted}\n")
        elif tag_lower == "div":
            inner = _extract_text_legi(child)
            if inner.strip():
                parts.append(f"\n{inner.strip()}\n")
        elif tag_lower in ("sup", "sub"):
            inner = _extract_text_legi(child)
            if inner.strip():
                parts.append(inner.strip())
        elif tag_lower == "table":
            # Simplify tables to plain text
            inner = _extract_text_legi(child)
            if inner.strip():
                parts.append(f"\n{inner.strip()}\n")
        else:
            # <a>, <span>, <font>, etc. → extract text only
            inner = _extract_text_legi(child)
            if inner:
                parts.append(inner)

        if child.tail:
            parts.append(child.tail)

    return "".join(parts)


def _extract_contenu_paragraphs(article_el: etree._Element) -> list[Paragraph]:
    """Extracts paragraphs from the CONTENU of a LEGI article.

    Generates a paragraph with class 'articulo' for the article number,
    followed by paragraphs with class 'parrafo' for the content.
    """
    contenu = article_el.find("CONTENU")
    if contenu is None:
        return []

    paragraphs: list[Paragraph] = []
    num = article_el.get("num", "")
    if num:
        paragraphs.append(Paragraph(css_class="articulo", text=f"Article {num}"))

    for p_el in contenu.iter("p"):
        text = _extract_text_legi(p_el)
        if text.strip():
            paragraphs.append(Paragraph(css_class="parrafo", text=text.strip()))

    # If CONTENU has no <p> children, try direct text
    if not paragraphs or (len(paragraphs) == 1 and num):
        direct_text = _extract_text_legi(contenu)
        if direct_text.strip():
            paragraphs.append(Paragraph(css_class="parrafo", text=direct_text.strip()))

    return paragraphs


# ─────────────────────────────────────────────
# Combined text parser → Block
# ─────────────────────────────────────────────


def _parse_legi_combined(data: bytes) -> list[Block]:
    """Parses the combined XML from LEGIClient.get_text() into Blocks.

    Input format: <legi_combined id="LEGITEXTXXX">
      <META>...</META>
      <elements>
        <section id="..." titre="..." niv="1" debut="..." fin="..." etat="..."/>
        <article id="..." cid="..." num="..." debut="..." fin="..." etat="...">
          <CONTENU><p>...</p></CONTENU>
          <source_modif id="..." date="..." nature="..."/>
        </article>
        ...
      </elements>
    </legi_combined>
    """
    root = etree.fromstring(data)
    elements = root.find("elements")
    if elements is None:
        return []

    # First pass: group articles by cid
    articles_by_cid: dict[str, list[etree._Element]] = {}
    for el in elements:
        if el.tag == "article":
            cid = el.get("cid") or el.get("id")
            articles_by_cid.setdefault(cid, []).append(el)

    # Second pass: iterate in document order
    blocks: list[Block] = []
    seen_cids: set[str] = set()

    for el in elements:
        if el.tag == "section":
            block = _parse_section_block(el)
            if block is not None:
                blocks.append(block)

        elif el.tag == "article":
            cid = el.get("cid") or el.get("id")
            if cid in seen_cids:
                continue
            seen_cids.add(cid)

            block = _parse_article_block(cid, articles_by_cid[cid])
            if block is not None:
                blocks.append(block)

    return blocks


def _parse_section_block(section_el: etree._Element) -> Block | None:
    """Creates a section Block (heading) from a <section> element."""
    title = section_el.get("titre", "")
    if not title:
        return None

    start_date = _parse_date_legi(section_el.get("debut", ""))
    if start_date is None:
        return None

    niv = section_el.get("niv", "1")
    css_class = _SECTION_NIV_CSS.get(niv, "seccion")

    versions: list[Version] = [
        Version(
            norm_id=section_el.get("id", ""),
            publication_date=start_date,
            effective_date=start_date,
            paragraphs=(Paragraph(css_class=css_class, text=title),),
        )
    ]

    # If the section was repealed, add an empty version at the end date
    status = section_el.get("etat", "")
    if status in ("ABROGE", "ABROGE_DIFF"):
        end_date = _parse_date_legi(section_el.get("fin", ""))
        if end_date is not None:
            versions.append(
                Version(
                    norm_id=section_el.get("id", ""),
                    publication_date=end_date,
                    effective_date=end_date,
                    paragraphs=(),
                )
            )

    return Block(
        id=section_el.get("id", ""),
        block_type="section",
        title=title,
        versions=tuple(versions),
    )


def _parse_article_block(cid: str, article_els: list[etree._Element]) -> Block | None:
    """Creates an article Block with all its historical versions."""
    versions: list[Version] = []
    max_end_date: date | None = None
    all_abrogated = True

    for art_el in sorted(article_els, key=lambda e: e.get("debut", "")):
        status = art_el.get("etat", "")
        start_date = _parse_date_legi(art_el.get("debut", ""))
        end_date = _parse_date_legi(art_el.get("fin", ""))

        if start_date is None:
            continue

        if status == "VIGUEUR":
            all_abrogated = False

        if end_date is not None:
            max_end_date = max(max_end_date, end_date) if max_end_date else end_date

        paragraphs = _extract_contenu_paragraphs(art_el)
        if not paragraphs:
            continue

        # Source of the modification (JORFTEXT of the modifying text)
        source = art_el.find("source_modif")
        norm_id = (source.get("id", "") if source is not None else "") or art_el.get("id", "")

        versions.append(
            Version(
                norm_id=norm_id,
                publication_date=start_date,
                effective_date=start_date,
                paragraphs=tuple(paragraphs),
            )
        )

    if not versions:
        return None

    # If all versions are repealed, add an empty version at the end
    if all_abrogated and max_end_date is not None:
        versions.append(
            Version(
                norm_id="",
                publication_date=max_end_date,
                effective_date=max_end_date,
                paragraphs=(),
            )
        )

    num = article_els[0].get("num", "")
    title = f"Article {num}" if num else cid

    return Block(
        id=cid,
        block_type="article",
        title=title,
        versions=tuple(versions),
    )


# ─────────────────────────────────────────────
# LEGI metadata parser → NormMetadata
# ─────────────────────────────────────────────


def _text_of(parent: etree._Element, tag: str) -> str:
    """Extracts the text of a sub-element, or '' if it does not exist."""
    el = parent.find(f".//{tag}")
    if el is not None and el.text:
        return el.text.strip()
    return ""


def _parse_status(status_str: str) -> NormStatus:
    """Converts LEGI ETAT to NormStatus."""
    status_upper = status_str.upper()
    if status_upper in ("ABROGE", "ABROGE_DIFF"):
        return NormStatus.REPEALED
    if status_upper == "MODIFIE":
        return NormStatus.PARTIALLY_REPEALED
    return NormStatus.IN_FORCE


def _build_legifrance_url(norm_id: str, nature: str) -> str:
    """Builds the Legifrance URL for a text."""
    if nature == "CODE":
        return f"https://www.legifrance.gouv.fr/codes/texte_lc/{norm_id}"
    return f"https://www.legifrance.gouv.fr/loda/id/{norm_id}"


def _short_title_fr(raw_title: str) -> str:
    """Generates a short title for French texts.

    "Code civil" → "Code civil"
    "Constitution du 4 octobre 1958" → "Constitution"
    "Loi n° 2024-123 du 1er mars 2024 relative à..." → "Loi n° 2024-123"
    """
    if not raw_title:
        return raw_title

    # Constitution → shorten
    if raw_title.lower().startswith("constitution"):
        return "Constitution"

    # Laws with number → truncate at "du" (the date)
    for prefix in ("Loi n°", "Loi organique n°", "Ordonnance n°", "Décret n°"):
        if raw_title.startswith(prefix):
            # Keep up to the number
            parts = raw_title.split(" du ", 1)
            return parts[0].strip()

    # Codes → return as-is (already short)
    return raw_title


def _parse_metadata_legi(xml_data: bytes, norm_id: str) -> NormMetadata:
    """Parses metadata from a LEGI structure file.

    Extracts information from META/META_COMMUN and META/META_SPEC.
    """
    root = etree.fromstring(xml_data)

    # Extract fields from META
    nature = _text_of(root, "NATURE")
    title = (
        _text_of(root, "TITRE_TEXTE")
        or _text_of(root, "TITREFULL")
        or _text_of(root, "TITRE")
        or norm_id
    )
    identifier = _text_of(root, "ID") or norm_id
    status_str = _text_of(root, "ETAT")

    # Dates — in the actual dump, DATE_PUBLI for codes is usually 2999-01-01
    # For codes, look for debut of the first VERSION
    pub_date_str = _text_of(root, "DATE_PUBLI")
    pub_date = _parse_date_legi(pub_date_str)
    if pub_date is None:
        pub_date = _parse_date_legi(_text_of(root, "DATE_TEXTE"))
    if pub_date is None:
        # Fallback: debut from LIEN_TXT in VERSIONS
        for lien_txt in root.iter("LIEN_TXT"):
            pub_date = _parse_date_legi(lien_txt.get("debut", ""))
            if pub_date is not None:
                break
    if pub_date is None:
        pub_date = _parse_date_legi(_text_of(root, "DATE_DEBUT"))
    if pub_date is None:
        raise ValueError(f"Could not extract publication date for {norm_id}")

    modif_date_str = _text_of(root, "DERNIERE_MODIFICATION")
    modif_date = _parse_date_legi(modif_date_str)

    # Rank
    rank = _NATURE_TO_RANK.get(nature, Rank.OTRO)

    # Autorite / Ministere as department
    department = _text_of(root, "AUTORITE") or _text_of(root, "MINISTERE") or ""

    # Source URL
    source_url = _build_legifrance_url(identifier, nature)

    short_title = _short_title_fr(title)

    # Extra fields: LEGI metadata not captured in core fields
    extra: list[tuple[str, str]] = []

    nor = _text_of(root, "NOR")
    if nor:
        extra.append(("nor", nor))

    date_texte = _text_of(root, "DATE_TEXTE")
    enactment_date = _parse_date_legi(date_texte)
    if enactment_date:
        extra.append(("enactment_date", enactment_date.isoformat()))

    jorf = _text_of(root, "JORF")
    if jorf:
        extra.append(("jorf", jorf))

    numero = _text_of(root, "NUM") or _text_of(root, "NUMERO")
    if numero:
        extra.append(("official_number", numero))

    origine = _text_of(root, "ORIGINE_PUBLI")
    if origine:
        extra.append(("official_journal", origine))

    return NormMetadata(
        title=title,
        short_title=short_title,
        identifier=identifier,
        country="fr",
        rank=rank,
        publication_date=pub_date,
        status=_parse_status(status_str),
        department=department,
        source=source_url,
        last_modified=modif_date,
        extra=tuple(extra),
    )


# ─────────────────────────────────────────────
# Public classes (TextParser/MetadataParser interface)
# ─────────────────────────────────────────────


class LEGITextParser(TextParser):
    """Parses the combined LEGI XML into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        return _parse_legi_combined(data)


class LEGIMetadataParser(MetadataParser):
    """Parses metadata from a LEGI structure file."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        return _parse_metadata_legi(data, norm_id)
