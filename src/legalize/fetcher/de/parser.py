"""Parser for German gesetze-im-internet.de gii-norm XML.

Format: Custom XML (gii-norm DTD v1.01)
Each ZIP contains one XML file with all <norm> elements for a law.
First <norm> = law-level metadata. Subsequent <norm>s = articles/sections.

Structure per <norm>:
  <metadaten>
    <jurabk>GG</jurabk>
    <enbez>Art 1</enbez>           ← article number
    <titel>Menschenwürde</titel>   ← article title (optional)
    <gliederungseinheit>...</>     ← section heading (if structural)
  </metadaten>
  <textdaten>
    <text format="XML">
      <Content>
        <P>(1) Die Würde...</P>
        <P>(2) Das Deutsche Volk...</P>
      </Content>
    </text>
  </textdaten>
"""

from __future__ import annotations

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

# Map common law type patterns to rank
_RANK_PATTERNS: list[tuple[str, str]] = [
    ("grundgesetz", "grundgesetz"),
    ("verordnung", "rechtsverordnung"),
    ("bekanntmachung", "bekanntmachung"),
    ("satzung", "satzung"),
]


def _parse_gii_date(s: str | None) -> date | None:
    """Parse GII date format (YYYYMMDD or YYYY-MM-DD)."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _text_content(el: ET.Element) -> str:
    """Extract all text, collapsing whitespace."""
    return " ".join("".join(el.itertext()).split())


def _infer_rank(title: str, jurabk: str) -> str:
    """Infer normative rank from law title and abbreviation."""
    if jurabk.upper() == "GG" or title.lower().startswith("grundgesetz"):
        return "grundgesetz"
    lower = (title + " " + jurabk).lower()
    for pattern, rank in _RANK_PATTERNS:
        if pattern in lower:
            return rank
    return "bundesgesetz"


class GIITextParser(TextParser):
    """Parses gii-norm XML into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse the full gii-norm XML into Blocks.

        First <norm> is law-level metadata (skipped here — handled by metadata parser).
        Structural <norm>s (with gliederungseinheit) become section headings.
        Article <norm>s (with enbez) become article blocks.
        """
        root = ET.fromstring(data)
        norms = root.findall(".//norm")
        blocks: list[Block] = []

        # Extract builddate for version dating
        builddate = root.get("builddate", "")
        pub_date = _parse_gii_date(builddate) or date.today()

        for norm in norms[1:]:  # Skip first norm (law-level metadata)
            meta = norm.find("metadaten")
            if meta is None:
                continue

            gliederung = meta.find("gliederungseinheit")
            enbez = meta.find("enbez")

            if gliederung is not None:
                block = self._parse_section(gliederung, norm, pub_date)
                if block:
                    blocks.append(block)
            elif enbez is not None:
                block = self._parse_article(meta, norm, pub_date)
                if block:
                    blocks.append(block)

        return blocks

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform info from standangabe metadata."""
        root = ET.fromstring(data)
        norms = root.findall(".//norm")
        reforms = []

        if norms:
            meta = norms[0].find("metadaten")
            if meta is not None:
                for stand in meta.findall("standangabe"):
                    kommentar = stand.find("standkommentar")
                    if kommentar is not None and kommentar.text:
                        reforms.append({"note": kommentar.text.strip()})

        return reforms

    def _parse_section(
        self, gliederung: ET.Element, norm: ET.Element, pub_date: date
    ) -> Block | None:
        """Parse a structural heading norm into a Block."""
        bez = gliederung.find("gliederungsbez")
        titel = gliederung.find("gliederungstitel")
        bez_text = bez.text.strip() if bez is not None and bez.text else ""
        titel_text = titel.text.strip() if titel is not None and titel.text else ""
        title = f"{bez_text} {titel_text}".strip() if bez_text else titel_text

        if not title:
            return None

        doknr = norm.get("doknr", "")
        heading_para = Paragraph(css_class="titulo", text=title)
        version = Version(
            norm_id=doknr,
            publication_date=pub_date,
            effective_date=pub_date,
            paragraphs=(heading_para,),
        )
        return Block(id=doknr, block_type="section", title=title, versions=(version,))

    def _parse_article(self, meta: ET.Element, norm: ET.Element, pub_date: date) -> Block | None:
        """Parse an article norm into a Block."""
        enbez = meta.find("enbez")
        titel = meta.find("titel")
        enbez_text = enbez.text.strip() if enbez is not None and enbez.text else ""
        titel_text = _text_content(titel) if titel is not None else ""
        title = f"{enbez_text} {titel_text}".strip() if enbez_text else titel_text

        doknr = norm.get("doknr", "")

        # Parse text content
        paragraphs: list[Paragraph] = []
        if title:
            paragraphs.append(Paragraph(css_class="articulo", text=title))

        content = norm.find(".//Content")
        if content is not None:
            paragraphs.extend(self._parse_content(content))

        if not paragraphs:
            return None

        version = Version(
            norm_id=doknr,
            publication_date=pub_date,
            effective_date=pub_date,
            paragraphs=tuple(paragraphs),
        )
        return Block(id=doknr, block_type="article", title=title, versions=(version,))

    def _parse_content(self, content: ET.Element) -> list[Paragraph]:
        """Parse <Content> children into Paragraph objects."""
        paragraphs: list[Paragraph] = []

        for child in content:
            tag = child.tag
            text = _text_content(child)

            if not text:
                continue

            if tag == "P":
                paragraphs.append(Paragraph(css_class="abs", text=text))
            elif tag in ("DL", "DT", "DD"):
                paragraphs.append(Paragraph(css_class="definition", text=text))
            elif tag == "table":
                # Tables: extract row text
                for row in child.findall(".//row"):
                    row_text = _text_content(row)
                    if row_text:
                        paragraphs.append(Paragraph(css_class="table_row", text=row_text))
            elif tag == "pre":
                paragraphs.append(Paragraph(css_class="pre", text=text))
            else:
                # B, I, F, SP, kommentar, etc.
                paragraphs.append(Paragraph(css_class="abs", text=text))

        return paragraphs


class GIIMetadataParser(MetadataParser):
    """Parses gii-norm XML first <norm> into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse the law-level metadata from the first <norm>.

        norm_id is the URL slug (e.g., "gg", "bgb").
        """
        root = ET.fromstring(data)
        norms = root.findall(".//norm")
        if not norms:
            raise ValueError(f"No norms found for {norm_id}")

        meta = norms[0].find("metadaten")
        if meta is None:
            raise ValueError(f"No metadata in first norm for {norm_id}")

        jurabk = (meta.findtext("jurabk") or norm_id).strip()
        langue = (meta.findtext("langue") or meta.findtext("kurzue") or jurabk).strip()
        kurzue = (meta.findtext("kurzue") or "").strip()

        # Date
        ausfertigung = meta.find("ausfertigung-datum")
        date_str = ausfertigung.text if ausfertigung is not None else None
        publication_date = _parse_gii_date(date_str) or date(1900, 1, 1)

        # Fundstelle (gazette reference)
        periodikum = meta.findtext("fundstelle/periodikum") or ""
        zitstelle = meta.findtext("fundstelle/zitstelle") or ""
        bgbl_ref = f"{periodikum} {zitstelle}".strip()

        # Standangabe (amendment status)
        stand_kommentar = ""
        for stand in meta.findall("standangabe"):
            kommentar = stand.findtext("standkommentar")
            if kommentar:
                stand_kommentar = kommentar.strip()

        # Document number (BJNR...)
        doknr = root.get("doknr", "")

        rank_str = _infer_rank(langue, jurabk)

        # Build identifier: use jurabk (abbreviation) as it's unique and stable
        identifier = jurabk.upper().replace(" ", "-")

        extra: list[tuple[str, str]] = []
        if doknr:
            extra.append(("doknr", doknr))
        extra.append(("slug", norm_id))
        if stand_kommentar:
            extra.append(("stand", stand_kommentar))

        return NormMetadata(
            title=langue,
            short_title=kurzue or jurabk,
            identifier=identifier,
            country="de",
            rank=Rank(rank_str),
            publication_date=publication_date,
            status=NormStatus.IN_FORCE,
            department="BMJ (Bundesministerium der Justiz)",
            source=f"https://www.gesetze-im-internet.de/{norm_id}/",
            summary=bgbl_ref,
            extra=tuple(extra),
        )
