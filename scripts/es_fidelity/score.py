"""Per-law fidelity scorer.

Renders each sampled law through the current parser, fetches BOE's official
HTML rendering, computes an 8-axis score, and emits a per-law defect file.

Usage:
    python -m scripts.es_fidelity.score --sample /tmp/es-audit/sample.txt --iter 1
"""

from __future__ import annotations

import argparse
import csv
import difflib
import re
import sys
from datetime import date
from pathlib import Path

from lxml import etree, html as lhtml

_SRC = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(_SRC))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.es_fidelity.common import (  # noqa: E402
    DEFECTS_DIR,
    LOG_CSV,
    SANDBOX,
    fetch_consolidada_html,
    fetch_diario_xml,
    fetch_metadatos_xml,
    fetch_texto_xml,
    strip_nbsp,
    word_seq,
)

from legalize.fetcher.es.metadata import parse_metadata  # noqa: E402
from legalize.transformer.markdown import render_norm_at_date  # noqa: E402
from legalize.transformer.xml_parser import parse_text_xml  # noqa: E402


def _count_in_xml(xml_bytes: bytes, tag: str) -> int:
    if not xml_bytes:
        return 0
    root = etree.fromstring(xml_bytes)
    return sum(1 for _ in root.iter(tag))


def _count_p_class(xml_bytes: bytes, css: str) -> int:
    if not xml_bytes:
        return 0
    root = etree.fromstring(xml_bytes)
    return sum(1 for p in root.iter("p") if p.get("class", "") == css)


_CHROME_CLASSES = {
    # BOE navigation/UI chrome that pollutes #textoxslt
    "linkSubir",
    "fuera",
    "formBOE",
    "gris",
    "redondeada",
    "lista",
    "pie_unico",
    "siempreSeVe",
    "subtitMostrado",
    "barraSuperior",
    "botonera",
    "marcadores",  # <div class="marcadores"> — TOC dropdown container
    "marcador-actual",
    "dropdown",
}
_CHROME_IDS = {
    "selector-marcador",
    "enlaces-linked",
    "masMenos",
}

# Text-level markers BOE injects between blocks (rendered in text_content
# but meaningless navigation). Example: "[Bloque 3: #codigocivil]".
_BLOQUE_MARKER_RE = re.compile(r"\[Bloque\s+\d+[^\]]*\]")


def _html_text_payload(html_bytes: bytes) -> str:
    """Extract the legal text block from BOE's HTML viewer.

    BOE wraps the consolidated text in <div id="textoxslt">. That div also
    contains ~3k lines of navigation chrome (linkSubir/fuera/formBOE table of
    contents, Subir links, etc.) which we strip before comparing.
    """
    if not html_bytes:
        return ""
    doc = lhtml.fromstring(html_bytes)
    nodes = doc.cssselect("#textoxslt") or doc.cssselect(".texto")
    if not nodes:
        return ""
    root = nodes[0]

    # Remove chrome subtrees in-place — one XPath per class/id.
    for klass in _CHROME_CLASSES:
        for el in root.xpath(f".//*[contains(concat(' ', @class, ' '), ' {klass} ')]"):
            parent = el.getparent()
            if parent is not None:
                parent.remove(el)
    for ident in _CHROME_IDS:
        for el in root.xpath(f".//*[@id='{ident}']"):
            parent = el.getparent()
            if parent is not None:
                parent.remove(el)
    # BOE uses <p class="bloque">[Bloque N: #anchor]</p> as a block-marker
    # paragraph. Remove only those <p>s, not the <div class="bloque"> wrappers
    # around the legal content.
    for el in root.xpath(".//p[contains(concat(' ', @class, ' '), ' bloque ')]"):
        parent = el.getparent()
        if parent is not None:
            parent.remove(el)

    text = root.text_content()
    # Strip any remaining "[Bloque N: ...]" text that leaked through
    text = _BLOQUE_MARKER_RE.sub(" ", text)
    return strip_nbsp(text)


def _count_in_html(html_bytes: bytes, selector: str) -> int:
    if not html_bytes:
        return 0
    doc = lhtml.fromstring(html_bytes)
    return len(doc.cssselect(selector))


def score_one(boe_id: str) -> dict:
    """Fetch, render, diff, and score one law."""
    row: dict = {
        "id": boe_id,
        "text_xml_bytes": 0,
        "html_bytes": 0,
        "blocks": 0,
        "paragraphs_md": 0,
        "paragraphs_xml": 0,
        "tables_xml": 0,
        "tables_md": 0,
        "img_xml": 0,
        "img_md": 0,
        "notas_xml": 0,
        "notas_md": 0,
        "citas_xml": 0,
        "citas_md": 0,
        "links_xml": 0,
        "links_md": 0,
        "sup_xml": 0,
        "sup_md": 0,
        "sub_xml": 0,
        "sub_md": 0,
        "libros_xml": 0,
        "libros_md": 0,
        "anexos_xml": 0,
        "anexos_md": 0,
        "text_ratio": 0.0,
        "defects": [],
        "error": "",
    }

    # Consolidated XML — the source we parse from today.
    texto_xml = fetch_texto_xml(boe_id)
    row["text_xml_bytes"] = len(texto_xml)
    if not texto_xml:
        row["error"] = "404 /legislacion-consolidada (non-consolidated norm)"
        row["defects"].append("NOT_IN_CONSOLIDATA")
        return row

    metadatos_xml = fetch_metadatos_xml(boe_id)
    diario_xml = fetch_diario_xml(boe_id)
    try:
        metadata = parse_metadata(metadatos_xml, boe_id, diario_xml=diario_xml or None)
    except Exception as e:
        row["error"] = f"metadata parse: {e!s}"
        row["defects"].append("METADATA_PARSE_FAIL")
        return row

    # Count source-side constructs
    row["tables_xml"] = _count_in_xml(texto_xml, "table")
    row["img_xml"] = _count_in_xml(texto_xml, "img")
    row["sup_xml"] = _count_in_xml(texto_xml, "sup")
    row["sub_xml"] = _count_in_xml(texto_xml, "sub")
    row["links_xml"] = _count_in_xml(texto_xml, "a")
    row["paragraphs_xml"] = _count_in_xml(texto_xml, "p")
    row["notas_xml"] = _count_p_class(texto_xml, "nota_pie") + _count_p_class(
        texto_xml, "nota_pie_2"
    )
    row["citas_xml"] = _count_p_class(texto_xml, "cita_con_pleca") + _count_p_class(
        texto_xml, "cita"
    )
    row["libros_xml"] = _count_p_class(texto_xml, "libro_num")
    row["anexos_xml"] = _count_p_class(texto_xml, "anexo_num")

    # Parse + render through current pipeline
    try:
        blocks = parse_text_xml(texto_xml)
        md = render_norm_at_date(metadata, blocks, date.today(), include_all=True)
    except Exception as e:
        row["error"] = f"parser/renderer: {e!s}"
        row["defects"].append("PARSER_FAIL")
        return row

    (SANDBOX / f"{boe_id}.md").write_text(md)
    row["blocks"] = len(blocks)
    row["paragraphs_md"] = md.count("\n") - md.count("\n\n")  # rough

    # Count rendered-side constructs
    # Count actual tables rather than pipe rows: any contiguous block of |-lines
    # is one table.
    in_table = False
    tables_md = 0
    for line in md.splitlines():
        if line.startswith("|"):
            if not in_table:
                tables_md += 1
                in_table = True
        else:
            in_table = False
    row["tables_md"] = tables_md
    row["img_md"] = md.count("![")
    # nota_pie is rendered as "> <small>...</small>" by the refactored parser
    row["notas_md"] = md.count("<small>")
    # citas are blockquote lines that DO NOT contain <small> (those are notas)
    row["citas_md"] = sum(
        1 for line in md.splitlines() if line.startswith("> ") and "<small>" not in line
    )
    row["links_md"] = md.count("](http")
    row["sup_md"] = md.count("<sup>")
    row["sub_md"] = md.count("<sub>")
    # Heading counts for LIBRO / ANEXO — look for occurrences in headings
    row["libros_md"] = sum(
        1 for line in md.splitlines() if line.startswith(("# LIBRO", "## LIBRO"))
    )
    row["anexos_md"] = sum(
        1 for line in md.splitlines() if line.startswith(("# ANEXO", "## ANEXO"))
    )

    # Text fidelity — compare normalised word sequences md vs BOE HTML.
    # Strip frontmatter from the MD (our own metadata, not legal text),
    # drop Markdown link URLs (text inside [...] is kept, URL inside (...) is
    # discarded so we don't pollute the token stream with host/path tokens
    # that do not appear in the BOE HTML text_content), and drop Markdown
    # image references entirely (they do not exist in BOE HTML either).
    import re as _re
    html = fetch_consolidada_html(boe_id)
    row["html_bytes"] = len(html)
    boe_text = _html_text_payload(html)
    md_body = md
    if md_body.startswith("---\n"):
        end = md_body.find("\n---\n", 4)
        if end > 0:
            md_body = md_body[end + 5 :]
    # Drop image references (BOE HTML shows them as <img>, text_content returns "")
    md_body = _re.sub(r"!\[[^\]]*\]\([^)]*\)", "", md_body)
    # Drop URL portion of [text](url) leaving just text
    md_body = _re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", md_body)
    # Drop HTML passthrough tags (they show as text in MD but empty in lxml text_content)
    md_body = _re.sub(r"</?(?:sup|sub|small|ins|del|span|font)[^>]*>", "", md_body)
    our_text = strip_nbsp(md_body)
    if boe_text:
        boe_words = word_seq(boe_text)
        our_words = word_seq(our_text)
        matcher = difflib.SequenceMatcher(None, boe_words, our_words, autojunk=False)
        row["text_ratio"] = round(matcher.ratio(), 4)

    # Defect detection
    if row["tables_xml"] > 0 and row["tables_md"] == 0:
        row["defects"].append("TABLES_DROPPED")
    if row["notas_xml"] > 0 and row["notas_md"] == 0:
        row["defects"].append("NOTAS_DROPPED")
    if row["citas_xml"] > 0 and row["citas_md"] == 0:
        row["defects"].append("CITAS_FLATTENED")
    if row["libros_xml"] > 0 and row["libros_md"] == 0:
        row["defects"].append("LIBROS_UNSTYLED")
    # Anexos can be called "MODELOS", "TABLAS", "DISPOSICIONES ADICIONALES" etc.
    # in the title text, so we only flag ANEXOS_UNSTYLED when the XML has
    # anexo_num paragraphs AND the MD has NO "## " headings at all in the tail
    # of the document (rough heuristic).
    if row["anexos_xml"] > 0:
        tail = md.splitlines()[-max(30, len(md.splitlines()) // 5):]
        if not any(line.startswith("## ") for line in tail):
            row["defects"].append("ANEXOS_UNSTYLED")
    if row["img_xml"] > 0 and row["img_md"] == 0:
        row["defects"].append("IMAGES_DROPPED")
    if row["sup_xml"] > 0 and row["sup_md"] == 0:
        row["defects"].append("SUP_FLATTENED")
    if row["sub_xml"] > 0 and row["sub_md"] == 0:
        row["defects"].append("SUB_FLATTENED")
    if row["links_xml"] > 0 and row["links_md"] < row["links_xml"] * 0.5:
        row["defects"].append("LINKS_LOST")
    if row["text_ratio"] < 0.99 and boe_text:
        row["defects"].append(f"TEXT_RATIO_{int(row['text_ratio'] * 100)}")
    if metadata.title in ("test", "") or str(metadata.publication_date) == "2000-01-01":
        row["defects"].append("METADATA_PLACEHOLDER")

    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=str, required=True)
    ap.add_argument("--iter", type=int, required=True)
    args = ap.parse_args()

    sample_path = Path(args.sample)
    if not sample_path.exists():
        print(f"sample file not found: {sample_path}", file=sys.stderr)
        return 1

    ids = [ln.split("\t")[0] for ln in sample_path.read_text().splitlines() if ln.strip()]
    rows = []
    fieldnames: list[str] = []

    for i, boe_id in enumerate(ids, 1):
        print(f"[{i}/{len(ids)}] {boe_id}", file=sys.stderr)
        row = score_one(boe_id)
        row["iter"] = args.iter
        row["defects_str"] = ",".join(row["defects"])
        row.pop("defects")
        rows.append(row)
        if not fieldnames:
            fieldnames = list(row.keys())

        # Per-law defect file
        if row["defects_str"]:
            (DEFECTS_DIR / f"iter{args.iter}-{boe_id}.md").write_text(
                _defect_markdown(row)
            )

    # Append to running CSV
    exists = LOG_CSV.exists()
    with LOG_CSV.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)

    # One-line stdout summary
    for r in rows:
        print(
            f"{r['id']:22s} tbl {r['tables_md']}/{r['tables_xml']}  "
            f"not {r['notas_md']}/{r['notas_xml']}  "
            f"cit {r['citas_md']}/{r['citas_xml']}  "
            f"img {r['img_md']}/{r['img_xml']}  "
            f"lnk {r['links_md']}/{r['links_xml']}  "
            f"txt {r['text_ratio']:.3f}  {r['defects_str']}"
        )
    return 0


def _defect_markdown(row: dict) -> str:
    lines = [f"# Fidelity defects — {row['id']}", ""]
    for k, v in row.items():
        if k in ("defects_str", "error"):
            continue
        lines.append(f"- **{k}**: {v}")
    lines.append("")
    lines.append(f"**Defects:** `{row['defects_str']}`")
    if row.get("error"):
        lines.append("")
        lines.append(f"**Error:** {row['error']}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
