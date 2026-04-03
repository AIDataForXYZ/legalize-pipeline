"""Parser for Austrian RIS XML documents and API metadata.

The RIS XML format uses the BKA namespace: http://www.bka.gv.at
Each NOR document represents one paragraph/article.
The API JSON metadata groups NOR entries by Gesetzesnummer.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any
from xml.etree import ElementTree as ET

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import (
    Block,
    NormStatus,
    NormMetadata,
    Paragraph,
    Rank,
    Version,
)

NS = {"r": "http://www.bka.gv.at"}

# Map RIS Typ codes to Rank values
RIS_TYP_TO_RANGO: dict[str, str] = {
    "BVG": "bundesverfassungsgesetz",
    "G": "bundesgesetz",
    "V": "verordnung",
    "K": "kundmachung",
    "E": "erlass",
    "Vertrag": "staatsvertrag",
}

_SKIP_CT = frozenset(
    {
        "kurztitel",
        "kundmachungsorgan",
        "typ",
        "artikel_anlage",
        "ikra",
        "akra",
        "index",
        "schlagworte",
        "geaendert",
        "gesnr",
        "doknr",
        "adoknr",
        "langtitel",
        "aenderung",
        "anmerkung",
    }
)

# Standalone date paragraphs (DD.MM.YYYY) in the Norm header entry have ct=""
# instead of ct="ikra"/"akra" — detect and skip them.
_DATE_ONLY_RE = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{4}$")


def _parse_date(s: str) -> date | None:
    """Parse YYYY-MM-DD or DD.MM.YYYY date strings."""
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _strip_html(s: str) -> str:
    """Strip HTML tags and remove trailing publication reference (StF: BGBl...)."""
    text = re.sub(r"<[^>]+>", "", s).strip()
    # Titles often have "StF: BGBl. Nr. 43/1975" appended after a <br/> tag
    text = re.sub(r"\s*StF:.*$", "", text).strip()
    return text


# Tags to skip entirely (headers, footers, layout artifacts from Word)
_SKIP_TAGS = {"kzinhalt", "fzinhalt", "layoutdaten", "feld"}

# Absatz typ values that are page headers/footers (not content)
_SKIP_TYP = {"kz", "fz"}

# Metadata headings to filter out (compared case-insensitively)
_SKIP_HEADINGS = frozenset(
    h.lower()
    for h in (
        "Kurztitel",
        "Kundmachungsorgan",
        "Inkrafttretensdatum",
        "Außerkrafttretensdatum",
        "Text",
        "Beachte",
        "Schlagworte",
        "§/Artikel/Anlage",
        "Langtitel",
        "Typ",
        "Änderung",
        "Index",
        "Gesetzesnummer",
        "Dokumentnummer",
        "Alte Dokumentnummer",
        "Zuletzt aktualisiert am",
        "Anmerkung",
    )
)


def _tag(el: ET.Element) -> str:
    """Strip namespace prefix from an element tag."""
    t = el.tag
    return t.split("}")[-1] if "}" in t else t


def _extract_text(el: ET.Element) -> str:
    """Extract text from an element, preserving inline formatting as Markdown.

    Handles: <i> → *italic*, <u> → _underline_, <super> → <sup>,
    <gldsym>/<span>/<n> → passthrough, <nbsp> → space.
    Skips: <feld>, <tab>, <abstand>, <layoutdaten>, <binary>, <src>.
    """
    parts: list[str] = []
    if el.text:
        parts.append(el.text)
    for child in el:
        ctag = _tag(child)
        child_text = _extract_text(child)
        if ctag == "i" and child_text:
            parts.append(f"*{child_text}*")
        elif ctag == "u" and child_text:
            parts.append(f"_{child_text}_")
        elif ctag == "super" and child_text:
            parts.append(f"<sup>{child_text}</sup>")
        elif ctag == "nbsp":
            parts.append("\u00a0")
        elif ctag in ("abstand", "feld", "layoutdaten", "tab", "binary", "src"):
            pass
        else:
            # gldsym, span, n, symbol, and unknown tags: passthrough text
            parts.append(child_text)
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def _cell_text(td: ET.Element) -> str:
    """Extract all visible text from a table cell as a single line."""
    cell_parts: list[str] = []
    for el in td.iter():
        tag = _tag(el)
        if tag == "absatz":
            ct = el.get("ct", "")
            if ct in _SKIP_CT:
                continue
            text = _extract_text(el).strip()
            if text:
                cell_parts.append(text)
        elif tag == "listelem":
            sym_el = el.find("{http://www.bka.gv.at}symbol")
            if sym_el is None:
                sym_el = el.find("symbol")
            sym = (_extract_text(sym_el).strip() + " ") if sym_el is not None else ""
            body_parts: list[str] = []
            if el.text and el.text.strip():
                body_parts.append(el.text.strip())
            for child in el:
                if _tag(child) != "symbol":
                    body_parts.append(_extract_text(child).strip())
                if child.tail and child.tail.strip():
                    body_parts.append(child.tail.strip())
            body = " ".join(p for p in body_parts if p)
            if body:
                cell_parts.append(f"{sym}{body}")
        elif tag == "schlussteil":
            text = _extract_text(el).strip()
            if text:
                cell_parts.append(text)
    text = " ".join(cell_parts)
    # Escape pipes for Markdown tables and collapse whitespace
    return re.sub(r"\s+", " ", text).replace("|", "\\|").strip()


def _table_to_markdown(table_el: ET.Element) -> str:
    """Convert a <table> XML element to a Markdown pipe table."""
    rows: list[list[str]] = []
    for child in table_el:
        if _tag(child) != "tr":
            continue
        cells = [_cell_text(td) for td in child if _tag(td) == "td"]
        if cells:
            rows.append(cells)

    if not rows:
        return ""

    # Normalize column count
    max_cols = max(len(row) for row in rows)
    for row in rows:
        while len(row) < max_cols:
            row.append("")

    lines = []
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join("---" for _ in range(max_cols)) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def _elem_to_paragraphs(nutzdaten: ET.Element) -> list[Paragraph]:
    """Convert RIS XML nutzdaten into a list of Paragraph objects.

    Filters out:
    - kzinhalt/fzinhalt (page headers/footers with "Bundesrecht konsolidiert")
    - absatz typ="kz"/typ="fz" (header/footer text)
    - metadata preamble (ct in _SKIP_CT: kurztitel, kundmachungsorgan, etc.)
    - feld elements (Word merge fields like page numbers)

    Preserves:
    - Inline formatting: <i> → *italic*, <u> → _underline_, <super> → <sup>
    - Tables: <table>/<tr>/<td> → Markdown pipe tables
    - Lists: <listelem> with symbols
    """
    paragraphs: list[Paragraph] = []

    # Collect elements inside <table> so we skip them in the main loop
    table_descendants: set[int] = set()
    for tbl in nutzdaten.iter():
        if _tag(tbl) == "table":
            for desc in tbl.iter():
                table_descendants.add(id(desc))

    for el in nutzdaten.iter():
        tag = _tag(el)

        # Skip descendants of table elements (handled when we hit <table>)
        if id(el) in table_descendants and tag != "table":
            continue

        # Skip header/footer/layout elements entirely
        if tag in _SKIP_TAGS:
            continue

        ct = el.get("ct", "")
        typ = el.get("typ", "")

        # Skip metadata fields (already extracted via API)
        if ct in _SKIP_CT:
            continue

        # Skip page header/footer paragraphs
        if typ in _SKIP_TYP:
            continue

        if tag == "table":
            md_table = _table_to_markdown(el)
            if md_table:
                paragraphs.append(Paragraph(css_class="table", text=md_table))

        elif tag == "ueberschrift":
            text = _extract_text(el).strip()
            if text and text.lower() not in _SKIP_HEADINGS:
                paragraphs.append(Paragraph(css_class=f"heading_{typ or 'g1'}", text=text))

        elif tag == "absatz":
            text = _extract_text(el).strip()
            # Skip standalone date paragraphs (Norm header has ct="" for dates)
            if text and not _DATE_ONLY_RE.match(text):
                paragraphs.append(Paragraph(css_class=typ or "abs", text=text))

        elif tag == "listelem":
            sym_el = el.find("r:symbol", NS)
            sym = (_extract_text(sym_el).strip() + " ") if sym_el is not None else ""
            parts: list[str] = []
            if el.text and el.text.strip():
                parts.append(el.text.strip())
            for child in el:
                if _tag(child) != "symbol":
                    t = _extract_text(child).strip()
                    if t:
                        parts.append(t)
                if child.tail and child.tail.strip():
                    parts.append(child.tail.strip())
            body = " ".join(p for p in parts if p)
            if body:
                paragraphs.append(Paragraph(css_class="listelem", text=f"{sym}{body}"))

        elif tag == "schlussteil":
            text = _extract_text(el).strip()
            if text:
                paragraphs.append(Paragraph(css_class="schlussteil", text=text))

    return paragraphs


class RISTextParser(TextParser):
    """Parses RIS XML documents (one or more NOR paragraphs) into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse NOR XML(s) into Block objects.

        Handles both single NOR documents and combined documents
        (wrapped in <combined_nor_documents> by the client).
        """
        text = data.decode("utf-8", errors="replace")

        # Combined document from get_text (multiple NOR XMLs)
        if "<combined_nor_documents" in text:
            return self._parse_combined(data)

        # Single NOR document
        return self._parse_single(data)

    def _parse_single(self, data: bytes) -> list[Any]:
        """Parse a single NOR XML into one Block."""
        root = ET.fromstring(data)
        nutzdaten = root.find(".//r:nutzdaten", NS)
        if nutzdaten is None:
            return []

        nor_id = self._extract_ct(nutzdaten, "doknr") or "unknown"
        para_label = self._extract_ct(nutzdaten, "artikel_anlage") or nor_id
        ikra_str = self._extract_ct(nutzdaten, "ikra")
        pub_date = _parse_date(ikra_str) or date(1900, 1, 1)

        paragraphs = _elem_to_paragraphs(nutzdaten)

        version = Version(
            norm_id=nor_id,
            publication_date=pub_date,
            effective_date=pub_date,
            paragraphs=tuple(paragraphs),
        )

        return [Block(id=nor_id, block_type="paragraph", title=para_label, versions=(version,))]

    def _parse_combined(self, data: bytes) -> list[Any]:
        """Parse combined NOR documents into multiple Blocks."""
        import re

        text = data.decode("utf-8", errors="replace")
        blocks = []

        # Extract individual RIS documents from the combined wrapper
        for match in re.finditer(r"(<risdok[^>]*>.*?</risdok>)", text, re.DOTALL):
            doc_xml = match.group(1).encode("utf-8")
            try:
                blocks.extend(self._parse_single(doc_xml))
            except ET.ParseError:
                continue

        return blocks

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform points from RIS XML.

        Full reform history requires cross-referencing the Novellen endpoint
        (a separate API call per Gesetz). Returns empty list for now.
        """
        text = data.decode("utf-8", errors="replace")
        if "<combined_nor_documents" in text:
            blocks = self._parse_combined(data)
            from legalize.transformer.xml_parser import extract_reforms

            return extract_reforms(blocks)

        root = ET.fromstring(data)
        nutzdaten = root.find(".//r:nutzdaten", NS)
        if nutzdaten is None:
            return []
        return []

    @staticmethod
    def _extract_ct(nutzdaten: ET.Element, ct_value: str) -> str:
        """Extract text of an absatz with a specific ct attribute."""
        for el in nutzdaten.findall(".//r:absatz", NS):
            if el.get("ct") == ct_value:
                return "".join(el.itertext()).strip()
        return ""


class RISMetadataParser(MetadataParser):
    """Parses RIS API JSON metadata into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse the JSON API response for a Gesetzesnummer into NormMetadata.

        norm_id is the Gesetzesnummer (e.g. '10002333').
        """
        api_data = json.loads(data)
        refs = api_data["OgdSearchResult"]["OgdDocumentResults"].get("OgdDocumentReference", [])
        if isinstance(refs, dict):
            refs = [refs]

        # Prefer the Norm (header) entry; fall back to first entry
        norm_ref = next(
            (
                r
                for r in refs
                if r["Data"]["Metadaten"]["Bundesrecht"]["BrKons"].get("Dokumenttyp") == "Norm"
            ),
            refs[0] if refs else None,
        )
        if not norm_ref:
            raise ValueError(f"No metadata found for Gesetzesnummer {norm_id}")

        br = norm_ref["Data"]["Metadaten"]["Bundesrecht"]
        brkons = br["BrKons"]
        allgemein = norm_ref["Data"]["Metadaten"].get("Allgemein", {})

        kurztitel = br.get("Kurztitel", "").strip()
        titel = _strip_html(br.get("Titel", kurztitel))

        # Normalize Typ — handle compound types like "Vertrag – Schweiz"
        typ_raw = brkons.get("Typ", "")
        typ_key = typ_raw.split("\u2013")[0].split("-")[0].strip()
        rango_str = RIS_TYP_TO_RANGO.get(typ_key, "sonstige")

        ikra = _parse_date(brkons.get("Inkrafttretensdatum", ""))
        akra = _parse_date(brkons.get("Ausserkrafttretensdatum", ""))
        estado = NormStatus.REPEALED if akra else NormStatus.IN_FORCE

        geaendert = _parse_date(allgemein.get("Geaendert", ""))
        eli_url = br.get("Eli", "") or brkons.get("GesamteRechtsvorschriftUrl", "")
        bgbl = brkons.get("Kundmachungsorgan", "")

        indizes = brkons.get("Indizes", {})
        if isinstance(indizes, dict):
            items = indizes.get("item", [])
            subjects: tuple[str, ...] = (items,) if isinstance(items, str) else tuple(items)
        else:
            subjects = ()

        # Collect keywords (Schlagworte) from all paragraph entries
        keywords: set[str] = set()
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            try:
                sw = ref["Data"]["Metadaten"]["Bundesrecht"]["BrKons"].get("Schlagworte", "")
            except (KeyError, TypeError):
                continue
            if sw:
                for kw in sw.split(","):
                    kw = kw.strip()
                    if kw:
                        keywords.add(kw)

        # Build country-specific extra fields
        extra: list[tuple[str, str]] = []

        if kurztitel:
            extra.append(("short_title", kurztitel))

        stammnorm_organ = brkons.get("StammnormPublikationsorgan", "").strip()
        stammnorm_nr = brkons.get("StammnormBgblnummer", "").strip()
        if stammnorm_organ and stammnorm_nr:
            extra.append(("official_journal", f"{stammnorm_organ} {stammnorm_nr}"))
        if stammnorm_nr:
            extra.append(("bgbl_number", stammnorm_nr))

        if akra:
            extra.append(("repeal_date", akra.isoformat()))

        novellen_nr = brkons.get("NovellenBgblnummer", "").strip()
        novellen_bez = brkons.get("NovellenBeziehung", "").strip()
        if novellen_nr:
            extra.append(("amendment_bgbl", novellen_nr))
        if novellen_bez:
            extra.append(("amendment_relation", novellen_bez))

        aenderung = brkons.get("Aenderung", "").replace("\r\n", " ").replace("\n", " ").strip()
        if aenderung:
            extra.append(("amendment_note", aenderung))

        anmerkung_raw = brkons.get("Anmerkung", "").replace("\r\n", " ").replace("\n", " ")
        anmerkung = _strip_html(anmerkung_raw)
        if anmerkung:
            extra.append(("annotation", anmerkung))

        if subjects:
            extra.append(("subjects", "; ".join(subjects)))

        if keywords:
            extra.append(("keywords", "; ".join(sorted(keywords))))

        consolidated_url = brkons.get("GesamteRechtsvorschriftUrl", "").strip()
        if consolidated_url and consolidated_url != eli_url:
            extra.append(("consolidated_url", consolidated_url))

        gesetzesnummer = brkons.get("Gesetzesnummer", "").strip()
        if gesetzesnummer:
            extra.append(("gesetzesnummer", gesetzesnummer))

        return NormMetadata(
            title=titel,
            short_title=kurztitel,
            identifier=f"AT-{norm_id}",
            country="at",
            rank=Rank(rango_str),
            publication_date=ikra or date(1900, 1, 1),
            status=estado,
            department="BKA (Bundeskanzleramt)",
            source=eli_url,
            last_modified=geaendert,
            subjects=subjects,
            summary=bgbl,
            extra=tuple(extra),
        )
