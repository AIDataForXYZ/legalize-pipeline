"""Justice Canada XML parser for federal acts and regulations.

Parses the custom Justice Canada XML format (Statute/Regulation DTD) into
the generic Block / NormMetadata model. Handles sections, subsections,
paragraphs, tables, definitions, formulas, schedules, and preambles.

Adapted from legalize-ca-pipeline/legalize_ca/fetchers/federal.py.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from lxml import etree

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import Block, NormMetadata, NormStatus, Paragraph, Rank, Version

# Namespace for lims attributes.
LIMS_NS = "http://justice.gc.ca/lims"
XML_NS = "http://www.w3.org/XML/1998/namespace"

# Control characters to strip (C0/C1 minus tab, LF, CR).
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")


def _clean(text: str) -> str:
    """Normalize whitespace and strip control characters."""
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = _CTRL_RE.sub("", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_date(date_str: str) -> date | None:
    """Parse dates in YYYY-MM-DD or YYYYMMDD format."""
    if not date_str:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = re.match(r"(\d{4})(\d{2})(\d{2})", date_str)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# Inline text extraction (recursive, preserves emphasis/definitions/refs)
# ---------------------------------------------------------------------------


def _inline_text(el: etree._Element) -> str:
    """Recursively extract text with inline Markdown formatting."""
    parts: list[str] = []

    if el.text:
        parts.append(el.text)

    for child in el:
        tag = etree.QName(child).localname if isinstance(child.tag, str) else ""
        child_text = _inline_text(child)

        if tag in ("Emphasis", "DefinedTermEn", "DefinedTermFr"):
            if child_text.strip():
                parts.append(f"*{child_text.strip()}*")
        elif tag == "DefinitionRef":
            if child_text.strip():
                parts.append(f"*{child_text.strip()}*")
        elif tag in ("XRefExternal", "AmendedText"):
            parts.append(child_text)
        elif tag in ("FormulaTerm", "FormulaText", "FormulaDefinition", "FormulaConnector"):
            parts.append(child_text)
        else:
            parts.append(child_text)

        if child.tail:
            parts.append(child.tail)

    return "".join(parts).strip()


# ---------------------------------------------------------------------------
# Table parsing
# ---------------------------------------------------------------------------


def _table_to_markdown(table_group: etree._Element) -> str:
    """Convert a <TableGroup> containing XHTML-like table to Markdown pipe table."""
    rows: list[list[str]] = []

    for tr in table_group.iter():
        tag = etree.QName(tr).localname if isinstance(tr.tag, str) else ""
        if tag != "row":
            continue
        cells: list[str] = []
        for entry in tr:
            entry_tag = etree.QName(entry).localname if isinstance(entry.tag, str) else ""
            if entry_tag != "entry":
                continue
            text = _clean(_inline_text(entry)).replace("|", "\\|")
            cells.append(text)
        if cells:
            rows.append(cells)

    if not rows:
        return ""

    max_cols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < max_cols:
            r.append("")

    lines = []
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join("---" for _ in range(max_cols)) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section / subsection parsing
# ---------------------------------------------------------------------------


def _parse_section(section: etree._Element) -> list[Paragraph]:
    """Parse a <Section> element into a list of Paragraphs."""
    paragraphs: list[Paragraph] = []

    # Section heading.
    marginal = section.find("MarginalNote")
    label = section.find("Label")
    heading_parts: list[str] = []
    if label is not None and label.text:
        heading_parts.append(label.text.strip())
    if marginal is not None:
        heading_parts.append(_clean(_inline_text(marginal)))
    if heading_parts:
        paragraphs.append(Paragraph(css_class="articulo", text=" ".join(heading_parts)))

    # Direct Text children.
    for text_el in section.findall("./Text"):
        txt = _clean(_inline_text(text_el))
        if txt:
            paragraphs.append(Paragraph(css_class="parrafo", text=txt))

    # Subsections.
    for subsec in section.findall("./Subsection"):
        paragraphs.extend(_parse_subsection(subsec))

    # Section-level Introduction.
    for intro in section.findall("./Introduction"):
        for text_el in intro.findall("./Text"):
            txt = _clean(_inline_text(text_el))
            if txt:
                paragraphs.append(Paragraph(css_class="parrafo", text=txt))

    # Section-level Items.
    for item in section.findall("./Item"):
        paragraphs.extend(_parse_item(item, indent=0))

    # Section-level Formula.
    for formula in section.findall("./Formula"):
        formula_text = _parse_formula(formula)
        if formula_text:
            paragraphs.append(Paragraph(css_class="pre", text=formula_text))

    # Definitions.
    for defn in section.findall("./Definition"):
        paragraphs.extend(_parse_definition(defn))

    return paragraphs


def _parse_subsection(subsec: etree._Element) -> list[Paragraph]:
    """Parse a <Subsection> element."""
    paragraphs: list[Paragraph] = []
    label = subsec.find("Label")
    label_text = _clean(label.text or "") if label is not None else ""

    # Subsection text.
    for text_el in subsec.findall("./Text"):
        txt = _clean(_inline_text(text_el))
        if txt:
            prefix = f"**{label_text}** " if label_text else ""
            paragraphs.append(Paragraph(css_class="parrafo", text=f"{prefix}{txt}"))

    # Paragraphs within subsection.
    for para in subsec.findall("./Paragraph"):
        paragraphs.extend(_parse_paragraph(para))

    # ContinuedParagraph.
    for cp in subsec.findall("./ContinuedParagraph"):
        for text_el in cp.findall("./Text"):
            txt = _clean(_inline_text(text_el))
            if txt:
                paragraphs.append(Paragraph(css_class="parrafo", text=txt))

    # ContinuedSubparagraph at subsection level.
    for csp in subsec.findall("./ContinuedSubparagraph"):
        for text_el in csp.findall("./Text"):
            txt = _clean(_inline_text(text_el))
            if txt:
                paragraphs.append(Paragraph(css_class="parrafo", text=txt))

    # Items.
    for item in subsec.findall("./Item"):
        paragraphs.extend(_parse_item(item, indent=0))

    # Formulas.
    for formula in subsec.findall("./Formula"):
        formula_text = _parse_formula(formula)
        if formula_text:
            paragraphs.append(Paragraph(css_class="pre", text=formula_text))

    return paragraphs


def _parse_paragraph(para: etree._Element) -> list[Paragraph]:
    """Parse a <Paragraph> element (level below subsection)."""
    paragraphs: list[Paragraph] = []
    label = para.find("Label")
    label_text = _clean(label.text or "") if label is not None else ""

    for text_el in para.findall("./Text"):
        txt = _clean(_inline_text(text_el))
        if txt:
            prefix = f"**{label_text}** " if label_text else ""
            paragraphs.append(Paragraph(css_class="parrafo", text=f"{prefix}{txt}"))

    # Subparagraphs.
    for subpara in para.findall("./Subparagraph"):
        paragraphs.extend(_parse_subparagraph(subpara))

    # ContinuedSubparagraph within paragraph.
    for csp in para.findall("./ContinuedSubparagraph"):
        for text_el in csp.findall("./Text"):
            txt = _clean(_inline_text(text_el))
            if txt:
                paragraphs.append(Paragraph(css_class="parrafo", text=txt))

    return paragraphs


def _parse_subparagraph(subpara: etree._Element) -> list[Paragraph]:
    """Parse a <Subparagraph> element."""
    paragraphs: list[Paragraph] = []
    label = subpara.find("Label")
    label_text = _clean(label.text or "") if label is not None else ""

    for text_el in subpara.findall("./Text"):
        txt = _clean(_inline_text(text_el))
        if txt:
            prefix = f"**{label_text}** " if label_text else ""
            paragraphs.append(Paragraph(css_class="parrafo", text=f"{prefix}{txt}"))

    # Clauses.
    for clause in subpara.findall("./Clause"):
        paragraphs.extend(_parse_clause(clause))

    # ContinuedSubparagraph within subparagraph.
    for csp in subpara.findall("./ContinuedSubparagraph"):
        for text_el in csp.findall("./Text"):
            txt = _clean(_inline_text(text_el))
            if txt:
                paragraphs.append(Paragraph(css_class="parrafo", text=txt))

    return paragraphs


def _parse_clause(clause: etree._Element) -> list[Paragraph]:
    """Parse a <Clause> element."""
    paragraphs: list[Paragraph] = []
    label = clause.find("Label")
    label_text = _clean(label.text or "") if label is not None else ""

    for text_el in clause.findall("./Text"):
        txt = _clean(_inline_text(text_el))
        if txt:
            prefix = f"**{label_text}** " if label_text else ""
            paragraphs.append(Paragraph(css_class="parrafo", text=f"{prefix}{txt}"))

    # Subclauses.
    for subclause in clause.findall("./Subclause"):
        sc_label = subclause.find("Label")
        sc_label_text = _clean(sc_label.text or "") if sc_label is not None else ""
        for text_el in subclause.findall("./Text"):
            txt = _clean(_inline_text(text_el))
            if txt:
                prefix = f"**{sc_label_text}** " if sc_label_text else ""
                paragraphs.append(Paragraph(css_class="parrafo", text=f"{prefix}{txt}"))

    return paragraphs


def _parse_item(item: etree._Element, indent: int = 0) -> list[Paragraph]:
    """Parse an <Item> (list item)."""
    paragraphs: list[Paragraph] = []
    label = item.find("Label")
    label_text = _clean(label.text or "") if label is not None else ""

    for text_el in item.findall("./Text"):
        txt = _clean(_inline_text(text_el))
        if txt:
            prefix = f"**{label_text}** " if label_text else "- "
            paragraphs.append(Paragraph(css_class="list_item", text=f"{prefix}{txt}"))

    return paragraphs


def _parse_formula(formula: etree._Element) -> str:
    """Parse a <Formula> element into code-formatted text."""
    parts: list[str] = []
    for child in formula:
        tag = etree.QName(child).localname if isinstance(child.tag, str) else ""
        if tag in ("FormulaTerm", "FormulaText", "FormulaDefinition", "FormulaConnector"):
            txt = _clean(_inline_text(child))
            if txt:
                parts.append(txt)
    return "`" + " ".join(parts) + "`" if parts else ""


def _parse_definition(defn: etree._Element) -> list[Paragraph]:
    """Parse a <Definition> block."""
    paragraphs: list[Paragraph] = []
    term = defn.find("DefinedTermEn")
    if term is not None:
        term_text = _clean(_inline_text(term))
        if term_text:
            paragraphs.append(Paragraph(css_class="firma_rey", text=term_text))
    for text_el in defn.findall("./Text"):
        txt = _clean(_inline_text(text_el))
        if txt:
            paragraphs.append(Paragraph(css_class="parrafo", text=txt))
    return paragraphs


# ---------------------------------------------------------------------------
# Body-level parsing (Parts, Divisions, Headings, Schedules)
# ---------------------------------------------------------------------------


def _parse_body(body: etree._Element) -> list[Paragraph]:
    """Parse the <Body> of an Act/Regulation into paragraphs."""
    paragraphs: list[Paragraph] = []

    for child in body:
        tag = etree.QName(child).localname if isinstance(child.tag, str) else ""

        if tag == "Heading":
            title_text = child.find("TitleText")
            if title_text is not None:
                txt = _clean(_inline_text(title_text))
                if txt:
                    level = child.get("level", "1")
                    css = "titulo_tit" if level == "1" else "capitulo_tit"
                    paragraphs.append(Paragraph(css_class=css, text=txt))

        elif tag == "Section":
            paragraphs.extend(_parse_section(child))

        elif tag == "Part":
            # Part heading.
            plabel = child.find("Label")
            ptitle = child.find("TitleText")
            heading_parts: list[str] = []
            if plabel is not None and plabel.text:
                heading_parts.append(plabel.text.strip())
            if ptitle is not None:
                heading_parts.append(_clean(_inline_text(ptitle)))
            if heading_parts:
                paragraphs.append(Paragraph(css_class="titulo_tit", text=" - ".join(heading_parts)))
            # Recurse into Part body.
            paragraphs.extend(_parse_body(child))

        elif tag == "Division":
            dlabel = child.find("Label")
            dtitle = child.find("TitleText")
            heading_parts = []
            if dlabel is not None and dlabel.text:
                heading_parts.append(dlabel.text.strip())
            if dtitle is not None:
                heading_parts.append(_clean(_inline_text(dtitle)))
            if heading_parts:
                paragraphs.append(
                    Paragraph(css_class="capitulo_tit", text=" - ".join(heading_parts))
                )
            paragraphs.extend(_parse_body(child))

        elif tag == "Oath":
            oath_text = _clean(_inline_text(child))
            if oath_text:
                paragraphs.append(Paragraph(css_class="parrafo", text=f"> *{oath_text}*"))

        elif tag == "List":
            for item in child.findall(".//Item"):
                paragraphs.extend(_parse_item(item))

        elif tag == "Schedule":
            sched_label = child.find("Label")
            if sched_label is not None and sched_label.text:
                paragraphs.append(Paragraph(css_class="titulo_tit", text=sched_label.text.strip()))
            paragraphs.extend(_parse_body(child))

        elif tag == "TableGroup":
            md_table = _table_to_markdown(child)
            if md_table:
                paragraphs.append(Paragraph(css_class="table", text=md_table))

    return paragraphs


# ---------------------------------------------------------------------------
# TextParser
# ---------------------------------------------------------------------------


class CATextParser(TextParser):
    """Parses Justice Canada XML into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse the full act/regulation XML into a list of Block objects.

        Each act/regulation becomes a single Block (the entire document).
        """
        root = etree.fromstring(data)

        # Publication date from pit-date.
        pub_date = _parse_date(root.get(f"{{{LIMS_NS}}}pit-date", "")) or date.today()

        # Body.
        body_el = root.find(".//Body")
        paragraphs: list[Paragraph] = []

        if body_el is not None:
            paragraphs = _parse_body(body_el)

        # Preamble (before Body).
        preamble_el = root.find(".//Preamble")
        if preamble_el is not None:
            preamble_text = _clean(_inline_text(preamble_el))
            if preamble_text:
                paragraphs.insert(0, Paragraph(css_class="parrafo", text=f"> {preamble_text}"))

        # Schedules at root level (outside Body).
        for sched in root.findall("Schedule"):
            sched_label = sched.find("ScheduleFormHeading")
            if sched_label is not None:
                heading_text = _clean(_inline_text(sched_label))
                if heading_text:
                    paragraphs.append(Paragraph(css_class="titulo_tit", text=heading_text))
            elif sched.find("Label") is not None:
                label_text = _clean(sched.find("Label").text or "")
                if label_text:
                    paragraphs.append(Paragraph(css_class="titulo_tit", text=label_text))
            # Parse schedule body content.
            paragraphs.extend(_parse_body(sched))
            # Tables directly in schedule.
            for tg in sched.findall("TableGroup"):
                md_table = _table_to_markdown(tg)
                if md_table:
                    paragraphs.append(Paragraph(css_class="table", text=md_table))

        if not paragraphs:
            return []

        version = Version(
            norm_id="body",
            publication_date=pub_date,
            effective_date=pub_date,
            paragraphs=tuple(paragraphs),
        )
        return [
            Block(
                id="body",
                block_type="article",
                title="",  # Title comes from metadata, not the body.
                versions=(version,),
            )
        ]


# ---------------------------------------------------------------------------
# MetadataParser
# ---------------------------------------------------------------------------


class CAMetadataParser(MetadataParser):
    """Extracts NormMetadata from Justice Canada XML."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        root = etree.fromstring(data)
        root_tag = etree.QName(root).localname if isinstance(root.tag, str) else root.tag

        # Determine rank from root tag.
        is_act = root_tag in ("Statute", "Act")
        rank_str = "act" if is_act else "regulation"

        # Title.
        short_title_el = root.find(".//ShortTitle")
        long_title_el = root.find(".//LongTitle")
        title = ""
        if short_title_el is not None:
            title = _clean(_inline_text(short_title_el))
        if not title and long_title_el is not None:
            title = _clean(_inline_text(long_title_el))

        # Identifier from norm_id (e.g., "eng/acts/A-1" → "A-1").
        parts = norm_id.split("/")
        file_id = parts[-1] if parts else norm_id

        # Sanitize identifier: replace filesystem-unsafe characters.
        identifier = file_id.replace("/", "-").replace(" ", "-")

        # Dates.
        pit_date = _parse_date(root.get(f"{{{LIMS_NS}}}pit-date", ""))
        last_amended = _parse_date(root.get(f"{{{LIMS_NS}}}lastAmendedDate", ""))
        inforce_start = _parse_date(root.get(f"{{{LIMS_NS}}}inforce-start-date", ""))
        pub_date = pit_date or last_amended or date.today()

        # Status.
        in_force_attr = root.get("in-force", "yes")
        status = NormStatus.IN_FORCE if in_force_attr == "yes" else NormStatus.REPEALED

        # Department / enabling authority.
        enabling = root.find(".//EnablingAuthority")
        department = "Parliament of Canada"
        if enabling is not None:
            enabling_text = _clean(_inline_text(enabling))
            if enabling_text:
                department = enabling_text

        # Source URL.
        lang = "eng"
        category = "acts" if is_act else "regulations"
        if "/" in norm_id:
            lang = parts[0] if len(parts) >= 1 else "eng"
            category = parts[1] if len(parts) >= 2 else category
        source_url = f"https://laws-lois.justice.gc.ca/{lang}/{category}/{file_id}/"

        # Extra metadata.
        extra: list[tuple[str, str]] = []
        if last_amended:
            extra.append(("last_amended", last_amended.isoformat()))
        if inforce_start:
            extra.append(("inforce_start", inforce_start.isoformat()))

        current_date = root.get(f"{{{LIMS_NS}}}current-date", "")
        if current_date:
            extra.append(("consolidation_date", current_date))

        has_prev = root.get("hasPreviousVersion", "")
        if has_prev:
            extra.append(("has_previous_version", has_prev))

        bill_origin = root.get("bill-origin", "")
        if bill_origin:
            extra.append(("bill_origin", bill_origin))

        bill_type = root.get("bill-type", "")
        if bill_type:
            extra.append(("bill_type", bill_type))

        fid = root.get(f"{{{LIMS_NS}}}fid", "")
        if fid:
            extra.append(("fid", fid))

        # Language.
        xml_lang = root.get(f"{{{XML_NS}}}lang", "")
        if xml_lang:
            extra.append(("lang", xml_lang))

        return NormMetadata(
            title=title or identifier,
            short_title=title or identifier,
            identifier=identifier,
            country="ca",
            rank=Rank(rank_str),
            publication_date=pub_date,
            status=status,
            department=department,
            source=source_url,
            extra=tuple(extra),
        )
