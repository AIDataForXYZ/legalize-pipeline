"""Markdown generation from legislative blocks.

Converts the Block/Version/Paragraph structure from BOE XML
into Markdown that mirrors the legal hierarchy.

Refactored 2026-04-22 (RESEARCH-ES-v2.md):
- Full CSS-class map covering libro/parte/titulo/cap/seccion/subseccion/
  articulo/anexo/apendice/disposiciones/firmas
- Blockquote rendering for cita/cita_con_pleca family
- Sangrado paragraphs keep their indentation level
- Dedicated pass-through for "table" and "image" CSS classes emitted by
  the XML parser when it meets a <table> or <img> element
- nota_pie rendered as an indented styled paragraph so the legislative
  audit trail survives
"""

from __future__ import annotations

import re
from datetime import date
from typing import Callable

from legalize.models import Block, NormMetadata, Paragraph
from legalize.transformer.frontmatter import render_frontmatter
from legalize.transformer.xml_parser import get_block_at_date


# ─────────────────────────────────────────────
# Mexican fraccion / apartado normalisation
# ─────────────────────────────────────────────

# Matches a paragraph that starts with a Roman numeral (I–XIX covers the
# vast majority of Mexican federal article fracciones) followed by a single
# space and an uppercase letter — the signature of a fraccion rendered
# without a period separator, e.g. "I Pudieren verse perjudicadas…".
# Group 1 = the Roman numeral, Group 2 = the rest of the text.
#
# The pattern is intentionally narrow:
#   - requires the numeral to be at the very start of the string
#   - the character immediately after must be a single space (not "." or ")")
#   - the word after the space must start with an uppercase accented letter
#     (A-ZÁÉÍÓÚÑ) so mid-sentence occurrences are not matched
#   - stray lone numerals ("I" with no body) are excluded via \S requirement
_FRACCION_BARE_RE = re.compile(
    r"^(I{1,3}|IV|VI{0,3}|IX|XI{0,3}|XIV|XV|XVI{0,3}|XIX|XX)\s+([A-ZÁÉÍÓÚÜÑ]\S)",
    re.UNICODE,
)

# Matches lowercase-letter apartados written as "a) ..." or "a.- ..." that
# should be formatted uniformly as list items.
# Group 1 = the letter label including punctuation, Group 2 = rest of text.
_APARTADO_LETTER_RE = re.compile(
    r"^([a-záéíóúüñ]\))\s+(\S)",
    re.UNICODE,
)


def _normalise_fraccion_text(text: str) -> str:
    """Add a period after a bare Roman-numeral fraccion label if missing.

    Transforms "I Pudieren verse…" → "I. Pudieren verse…".
    Leaves "I. Texto", "I.- Texto", "I) Texto" and mid-sentence occurrences
    unchanged.

    # TODO: cross-reference links — when a paragraph contains a phrase like
    # "el artículo 89 de esta Ley", emit a Markdown link pointing to the
    # heading anchor of that article within the current document.  Skipped
    # for now because distinguishing intra-law references from references to
    # other laws (e.g. "el artículo 89 de la Constitución") requires access
    # to the full heading set and reliable disambiguation heuristics.
    """
    m = _FRACCION_BARE_RE.match(text)
    if m:
        numeral = m.group(1)
        # Insert a period after the numeral and collapse the space
        return f"{numeral}. {text[len(numeral):].lstrip()}"
    return text


# ─────────────────────────────────────────────
# CSS class → Markdown mapping
# ─────────────────────────────────────────────

def _render_parrafo(text: str) -> str:
    """Render a body paragraph, normalising fraccion/apartado labels."""
    return f"{_normalise_fraccion_text(text)}\n"


_SIMPLE_CSS_MAP: dict[str, Callable[[str], str]] = {
    # --- structural headings (no pair) ---
    "libro_num": lambda t: f"# {t}\n",
    "parte_num": lambda t: f"# {t}\n",
    "titulo": lambda t: f"## {t}\n",
    "titulo_tit": lambda t: f"## {t}\n",
    "capitulo_tit": lambda t: f"### {t}\n",
    "seccion": lambda t: f"#### {t}\n",
    "seccion_tit": lambda t: f"#### {t}\n",
    "subseccion": lambda t: f"##### {t}\n",
    "subseccion_tit": lambda t: f"##### {t}\n",
    "articulo": lambda t: f"###### {t}\n",
    "anexo": lambda t: f"### {t}\n",
    "anexo_num": lambda t: f"## {t}\n",
    "apendice": lambda t: f"### {t}\n",
    "apendice_num": lambda t: f"## {t}\n",
    "disp_num": lambda t: f"## {t}\n",
    # --- legacy / pseudo-centred headings ---
    "centro_redonda": lambda t: f"### {t}\n",
    "centro_negrita": lambda t: f"# {t}\n",
    "centro_cursiva": lambda t: f"### *{t}*\n",
    # --- emphasis / indent helpers ---
    "cita": lambda t: f"> {t}\n",
    "cita_con_pleca": lambda t: f"> {t}\n",
    "cita_ley": lambda t: f"> {t}\n",
    "cita_art": lambda t: f"> {t}\n",
    "sangrado": lambda t: f"    {t}\n",
    "sangrado_2": lambda t: f"        {t}\n",
    "sangrado_articulo": lambda t: f"    {t}\n",
    # --- nota_pie: reform provenance — keep as quoted small text ---
    "nota_pie": lambda t: f"> <small>{t}</small>\n",
    "nota_pie_2": lambda t: f"> <small>{t}</small>\n",
    # --- signatories ---
    "firma_rey": lambda t: f"**{t}**\n",
    "firma_ministro": lambda t: f"**{t}**\n",
    "firma": lambda t: f"**{t}**\n",
    # --- synthetic classes emitted by the XML parser ---
    "image": lambda t: f"{t}\n",
    "list_item": lambda t: f"{t}\n",
    "pre": lambda t: f"```\n{t}\n```\n",
    # --- Mexican body paragraphs: normalise fraccion/apartado labels ---
    "parrafo": _render_parrafo,
    # --- generic fallbacks used by non-ES parsers (kept for back-compat) ---
    "h1": lambda t: f"# {t}\n",
    "h2": lambda t: f"## {t}\n",
    "h3": lambda t: f"### {t}\n",
    "h4": lambda t: f"#### {t}\n",
    "h5": lambda t: f"##### {t}\n",
    "h6": lambda t: f"###### {t}\n",
    "signature": lambda t: f"**{t}**\n",
    "preamble": lambda t: f"{t}\n",
    "formula": lambda t: f"{t}\n",
    "list": lambda t: f"{t}\n",
    "quote": lambda t: f"> {t}\n",
    "num": lambda t: f"{t}\n",
    # --- rendered tables pass through verbatim ---
    "table": lambda t: f"{t}\n",
    "table_row": lambda t: f"{t}\n",
}

# Paired classes: num + tit merge into one heading.
_PAIRED_CLASSES: dict[str, tuple[str, str]] = {
    "libro_num": ("libro_tit", "#"),
    "parte_num": ("parte_tit", "#"),
    "titulo_num": ("titulo_tit", "##"),
    "capitulo_num": ("capitulo_tit", "###"),
    "seccion_num": ("seccion_tit", "####"),
    "subseccion_num": ("subseccion_tit", "#####"),
    "anexo_num": ("anexo_tit", "##"),
    "apendice_num": ("apendice_tit", "##"),
    "disp_num": ("disp_tit", "##"),
}


def render_paragraphs(paragraphs: list[Paragraph] | tuple[Paragraph, ...]) -> str:
    """Convert a list of paragraphs to Markdown."""
    lines: list[str] = []
    plist = list(paragraphs)
    i = 0

    while i < len(plist):
        p = plist[i]
        css = p.css_class
        text = p.text

        # Paired class: <num> + <tit> → one heading
        if css in _PAIRED_CLASSES:
            tit_class, prefix = _PAIRED_CLASSES[css]
            if i + 1 < len(plist) and plist[i + 1].css_class == tit_class:
                lines.append(f"{prefix} {text}. {plist[i + 1].text}")
                lines.append("")
                i += 2
                continue
            lines.append(f"{prefix} {text}")
            lines.append("")
            i += 1
            continue

        formatter = _SIMPLE_CSS_MAP.get(css)
        if formatter is not None:
            rendered = formatter(text).rstrip("\n")
            if rendered:
                lines.append(rendered)
                lines.append("")
        else:
            # Unknown class — default to plain paragraph
            lines.append(text)
            lines.append("")

        i += 1

    return "\n".join(lines)


def render_norm_at_date(
    metadata: NormMetadata,
    blocks: list[Block] | tuple[Block, ...],
    target_date: date,
    include_all: bool = False,
) -> str:
    """Generate the complete Markdown for a norm at a given point in time."""
    parts: list[str] = []
    parts.append(render_frontmatter(metadata, target_date))

    title = metadata.title.rstrip(". ").strip()
    parts.append(f"# {title}\n\n")

    for block in blocks:
        version = get_block_at_date(block, target_date)

        if version is None and include_all and block.versions:
            version = min(block.versions, key=lambda v: v.publication_date)

        if version is None:
            continue

        md = render_paragraphs(version.paragraphs)
        if md.strip():
            parts.append(md)
            if not md.endswith("\n\n"):
                parts.append("\n")

    return "".join(parts).rstrip("\n") + "\n"
