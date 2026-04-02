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
    }
)


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
    return re.sub(r"<[^>]+>", "", s).strip()


# Tags to skip entirely (headers, footers, layout artifacts from Word)
_SKIP_TAGS = {"kzinhalt", "fzinhalt", "layoutdaten", "feld"}

# Absatz typ values that are page headers/footers (not content)
_SKIP_TYP = {"kz", "fz"}


def _elem_to_paragraphs(nutzdaten: ET.Element) -> list[Paragraph]:
    """Convert RIS XML nutzdaten into a list of Paragraph objects.

    Filters out:
    - kzinhalt/fzinhalt (page headers/footers with "Bundesrecht konsolidiert")
    - absatz typ="kz"/typ="fz" (header/footer text)
    - metadata preamble (ct in _SKIP_CT: kurztitel, kundmachungsorgan, etc.)
    - feld elements (Word merge fields like page numbers)

    Keeps everything else: actual law text (ct="text" or no ct).
    """
    paragraphs: list[Paragraph] = []

    for el in nutzdaten.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag

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

        if tag == "ueberschrift":
            # Skip metadata headings (e.g., "Kurztitel", "Text", "Kundmachungsorgan")
            text = "".join(el.itertext()).strip()
            if text and text not in (
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
                "Alte Dokumentnummer",
            ):
                paragraphs.append(Paragraph(css_class=f"heading_{typ or 'g1'}", text=text))

        elif tag == "absatz":
            text = "".join(el.itertext()).strip()
            if text:
                paragraphs.append(Paragraph(css_class=typ or "abs", text=text))

        elif tag == "listelem":
            sym_el = el.find("r:symbol", NS)
            sym = ("".join(sym_el.itertext()).strip() + " ") if sym_el is not None else ""
            parts: list[str] = []
            if el.text and el.text.strip():
                parts.append(el.text.strip())
            for child in el:
                ctag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if ctag != "symbol":
                    parts.append("".join(child.itertext()).strip())
            body = " ".join(p for p in parts if p)
            if body:
                paragraphs.append(Paragraph(css_class="listelem", text=f"{sym}{body}"))

        elif tag == "schlussteil":
            text = "".join(el.itertext()).strip()
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
            notes=bgbl,
        )
