"""Generic HTML/XML table → Markdown pipe-table renderer.

Shared across country parsers. Accepts any <table>/<tr>/<td>/<th> subtree
(lowercase OR uppercase tags) and emits a well-formed Markdown pipe table.
Handles rowspan/colspan by repeating cell content into the expanded grid.

Cell content is extracted via a caller-supplied inline extractor so that
sup/sub/bold/italic/links survive into the cell text.
"""

from __future__ import annotations

from typing import Callable

from lxml import etree


def _cells_of(tr) -> list[tuple[etree._Element, int, int]]:
    out: list[tuple[etree._Element, int, int]] = []
    for child in tr:
        tag = (child.tag or "").lower() if isinstance(child.tag, str) else ""
        if tag not in ("td", "th"):
            continue
        colspan = int(child.get("colspan") or child.get("COLSPAN") or 1)
        rowspan = int(child.get("rowspan") or child.get("ROWSPAN") or 1)
        out.append((child, colspan, rowspan))
    return out


def render_table(
    table_el: etree._Element,
    cell_extractor: Callable[[etree._Element], str],
) -> str:
    """Render a table subtree to a Markdown pipe table.

    Args:
        table_el: lxml element whose local-name is 'table' (any case).
        cell_extractor: function that turns a <td>/<th> element into the
            flat string that should appear inside the pipe cell. Must
            already handle inline formatting (bold/italic/sup/sub) and
            escape `|` characters.

    Returns:
        Markdown pipe table as a single string (no trailing newline) —
        empty string if the table has no cells.
    """
    # Detect <thead> for header row — fall back to first row otherwise
    head_row_idx = -1
    raw_rows: list[list[tuple[str, int, int]]] = []
    for i, tr in enumerate(table_el.iter()):
        tag = (tr.tag or "").lower() if isinstance(tr.tag, str) else ""
        if tag != "tr":
            continue
        cells = _cells_of(tr)
        if not cells:
            continue
        # Is this row under a <thead>?
        anc = tr.getparent()
        while anc is not None and isinstance(anc.tag, str):
            if anc.tag.lower() == "thead":
                head_row_idx = len(raw_rows)
                break
            anc = anc.getparent()
        raw_rows.append([(cell_extractor(cell), cs, rs) for cell, cs, rs in cells])

    if not raw_rows:
        return ""

    # Expand rowspan/colspan into a 2D grid
    expanded: list[list[str]] = []
    pending: dict[int, tuple[str, int]] = {}
    for row in raw_rows:
        out_row: list[str] = []
        col = 0
        idx = 0
        while idx < len(row) or col in pending:
            if col in pending:
                text, remaining = pending[col]
                out_row.append(text)
                if remaining > 1:
                    pending[col] = (text, remaining - 1)
                else:
                    del pending[col]
                col += 1
                continue
            text, colspan, rowspan = row[idx]
            for _ in range(colspan):
                out_row.append(text)
                if rowspan > 1:
                    pending[col] = (text, rowspan - 1)
                col += 1
            idx += 1
        expanded.append(out_row)

    # Pad to max width
    width = max(len(r) for r in expanded)
    for r in expanded:
        while len(r) < width:
            r.append("")

    header = expanded[0] if head_row_idx < 0 else expanded[head_row_idx]
    body = [r for i, r in enumerate(expanded) if r is not header]

    # If no thead and first row looks like a data row, still promote it as header
    lines = ["| " + " | ".join(_clean(c) for c in header) + " |"]
    lines.append("| " + " | ".join("---" for _ in range(width)) + " |")
    for r in body:
        lines.append("| " + " | ".join(_clean(c) for c in r) + " |")
    return "\n".join(lines)


def _clean(text: str) -> str:
    """Cell text cleanup — single-line, pipe-escaped."""
    text = text.replace("\r", " ").replace("\n", " ").strip()
    text = text.replace("|", "\\|")
    while "  " in text:
        text = text.replace("  ", " ")
    return text
