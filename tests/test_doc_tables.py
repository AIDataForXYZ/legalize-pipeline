"""Tests for Word 97-2003 (.doc) table parsing into Markdown pipe tables.

Covers two table storage formats found in Diputados DOC files:

1. **Inline multi-row**: a single \\r-paragraph stores all rows separated by
   ``\\x07\\x07`` (e.g. ``Header\\x07Col2\\x07\\x071996\\x0712.50\\x07\\x07``).
   Already handled before this feature; tested for completeness.

2. **Multi-paragraph rows**: each row is its own \\r-paragraph.  The first row
   has ``\\x07`` cell separators but does NOT start with ``\\x07\\x07``.
   Subsequent rows start with ``\\x07\\x07`` (the row-end mark of the previous
   row).  The pre-pass in ``_extract_doc_paragraphs`` must group these into one
   combined segment and pass it to ``_word_table_to_markdown``.
"""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, patch

import pytest

from legalize.fetcher.mx.parser import (
    _diputados_doc_blocks,
    _extract_doc_paragraphs,
    _word_table_to_markdown,
)
from legalize.transformer.markdown import render_paragraphs


# ── _word_table_to_markdown unit tests ─────────────────────────────────────


def test_inline_multi_row_table():
    """An inline \\x07\\x07-separated table renders as a full pipe table."""
    segment = (
        "Ejercicio\x07Por ciento\x07\x07"
        "1996\x0712.50\x07\x07"
        "1997\x0712.50\x07\x07"
        "1998\x0712.50\x07\x07"
        "1999\x0710.00\x07\x07"
    )
    result = _word_table_to_markdown(segment)
    assert result is not None
    lines = result.splitlines()
    # Header row
    assert "Ejercicio" in lines[0]
    assert "Por ciento" in lines[0]
    # Separator row
    assert lines[1] == "| --- | --- |"
    # Data rows
    assert "1996" in result
    assert "12.50" in result
    assert "1999" in result
    assert "10.00" in result
    # All rows accounted for
    data_rows = [l for l in lines if l.startswith("| ") and "---" not in l]
    assert len(data_rows) == 5  # 1 header + 4 data rows


def test_empty_cell_rendered_as_space():
    """Empty cells render as a single space so the table is valid Markdown."""
    # First cell empty, second cell has content
    segment = "Col1\x07\x07Col2\x07data\x07\x07"
    result = _word_table_to_markdown(segment)
    assert result is not None
    # Empty cell should be "|   |" not "||"
    assert "|   |" in result or "| " in result
    # Verify it does not contain "||" (adjacent pipes with nothing)
    assert "||" not in result


def test_binary_garbage_returns_none():
    """A BEL segment containing only binary garbage returns None."""
    garbage = "\x07bjbj\x07OJQJ\x07\x07"
    result = _word_table_to_markdown(garbage)
    assert result is None


def test_single_column_table():
    """Single-column tables render correctly (1 pipe per row)."""
    segment = "Sección\x07\x07Primera\x07\x07Segunda\x07\x07"
    result = _word_table_to_markdown(segment)
    assert result is not None
    assert "Sección" in result
    assert "Primera" in result


# ── _extract_doc_paragraphs: multi-paragraph row grouping ──────────────────


def _make_mock_ole(raw_text: str) -> MagicMock:
    """Return a mock OleFileIO that serves raw_text as the WordDocument stream."""
    mock_stream = MagicMock()
    mock_stream.read.return_value = raw_text.encode("latin-1")
    mock_ole = MagicMock()
    mock_ole.__enter__ = lambda s: s
    mock_ole.__exit__ = MagicMock(return_value=False)
    mock_ole.exists.return_value = True
    mock_ole.openstream.return_value = mock_stream
    return mock_ole


def test_multi_paragraph_rows_grouped_into_one_table():
    """Consecutive \\r-paragraphs forming a table are merged into one pipe table.

    Format: row1 has ``\\x07`` separators but no ``\\x07\\x07`` prefix;
    rows 2..N start with ``\\x07\\x07``.
    """
    raw = (
        "Artículo 1o. Texto del artículo.\r"
        "d \x07= Definición de la variable d.\r"
        "\x07\x07m \x07= Definición de la variable m.\r"
        "\x07\x07n \x07= Definición de la variable n.\r"
        "Párrafo siguiente.\r"
    )
    with patch("olefile.OleFileIO", return_value=_make_mock_ole(raw)):
        paras = _extract_doc_paragraphs(b"fake")

    table_paras = [p for p in paras if p.startswith("| ")]
    assert len(table_paras) == 1, f"Expected 1 table, got {len(table_paras)}"

    table = table_paras[0]
    lines = table.splitlines()
    # d (header) + separator + m + n = 4 lines
    assert len(lines) == 4
    # First row is the header (d)
    assert "= Definición de la variable d." in lines[0]
    # Separator row
    assert "---" in lines[1]
    # Data rows: m, n
    assert "= Definición de la variable m." in lines[2]
    assert "= Definición de la variable n." in lines[3]


def test_multi_paragraph_rows_not_merged_with_non_table_paragraph():
    """The table grouping stops as soon as a paragraph without \\x07\\x07 is found."""
    raw = (
        "Artículo 1o. Texto del artículo.\r"
        "a \x07= Variable a.\r"
        "\x07\x07b \x07= Variable b.\r"
        "Parrafo sin tabla, rompe la secuencia.\r"
        "c \x07= Otra tabla separada.\r"
        "\x07\x07d \x07= Otra variable.\r"
    )
    with patch("olefile.OleFileIO", return_value=_make_mock_ole(raw)):
        paras = _extract_doc_paragraphs(b"fake")

    table_paras = [p for p in paras if p.startswith("| ")]
    # Two separate tables, separated by the non-table paragraph
    assert len(table_paras) == 2


def test_inline_table_unaffected_by_multi_paragraph_pre_pass():
    """An inline \\x07\\x07-separated table is not altered by the pre-pass."""
    raw = (
        "Artículo 1o. Define los porcentajes.\r"
        "Ejercicio\x07Por ciento\x07\x071996\x0712.50\x07\x071997\x0712.50\x07\x07\r"
        "Texto posterior.\r"
    )
    with patch("olefile.OleFileIO", return_value=_make_mock_ole(raw)):
        paras = _extract_doc_paragraphs(b"fake")

    table_paras = [p for p in paras if p.startswith("| ")]
    assert len(table_paras) == 1
    table = table_paras[0]
    assert "Ejercicio" in table
    assert "1996" in table
    assert "1997" in table


# ── Block builder: table paragraphs get css_class="table" ──────────────────


def _make_doc_envelope(raw_text: str) -> dict:
    """Build a minimal Diputados DOC envelope for _diputados_doc_blocks."""
    return {
        "source": "diputados",
        "norm_id": "DIP-TEST",
        "abbrev": "TEST",
        "title": "Ley de Prueba",
        "rank": "ley",
        "publication_date": "2020-01-01",
        "doc_b64": base64.b64encode(b"fake").decode("ascii"),
    }


def test_table_paragraph_gets_table_css_class():
    """Pipe-table paragraphs are stored with css_class='table' in the block."""
    raw = (
        "Artículo 1o. Este artículo define variables.\r"
        "d \x07= Valor de la variable d.\r"
        "\x07\x07m \x07= Valor de la variable m.\r"
        "Texto de seguimiento.\r"
    )
    with patch("olefile.OleFileIO", return_value=_make_mock_ole(raw)):
        blocks = _diputados_doc_blocks(_make_doc_envelope(raw))

    article_blocks = [b for b in blocks if b.block_type == "article"]
    assert article_blocks, "No article blocks emitted"

    all_paras = article_blocks[0].versions[0].paragraphs
    table_paras = [p for p in all_paras if p.css_class == "table"]
    assert len(table_paras) == 1
    assert "= Valor de la variable d." in table_paras[0].text
    assert "= Valor de la variable m." in table_paras[0].text


def test_table_paragraph_not_merged_with_preceding_body():
    """The table paragraph is NOT joined with the preceding body text.

    If "Artículo Xo. Texto." has its body-rest text in pending_body_lines,
    and then a table paragraph arrives, the flush must happen BEFORE the table
    is emitted — they must not be concatenated.
    """
    raw = (
        "Artículo 1o. Este artículo define variables.\r"
        "d \x07= Valor de la variable d.\r"
        "\x07\x07m \x07= Valor de la variable m.\r"
        "Texto de seguimiento.\r"
    )
    with patch("olefile.OleFileIO", return_value=_make_mock_ole(raw)):
        blocks = _diputados_doc_blocks(_make_doc_envelope(raw))

    article_blocks = [b for b in blocks if b.block_type == "article"]
    assert article_blocks

    all_paras = article_blocks[0].versions[0].paragraphs
    # No paragraph should contain both body text AND pipe-table syntax
    for p in all_paras:
        if "| " in p.text:
            # Table cell syntax must not be mixed with leading body text
            assert p.text.startswith("| "), (
                f"Table content mixed into body paragraph: {p.text[:100]!r}"
            )


def test_table_inline_tarifa_css_class_table():
    """An inline \\x07\\x07-format table inside an article gets css_class='table'."""
    raw = (
        "Artículo 1o. Define los porcentajes.\r"
        "La siguiente tabla aplica:\r"
        "Ejercicio\x07Por ciento\x07\x071996\x0712.50\x07\x071997\x0712.50\x07\x07\r"
        "Los contribuyentes deben cumplir.\r"
    )
    with patch("olefile.OleFileIO", return_value=_make_mock_ole(raw)):
        blocks = _diputados_doc_blocks(_make_doc_envelope(raw))

    article_blocks = [b for b in blocks if b.block_type == "article"]
    assert article_blocks

    all_paras = article_blocks[0].versions[0].paragraphs
    table_paras = [p for p in all_paras if p.css_class == "table"]
    assert len(table_paras) == 1
    assert "Ejercicio" in table_paras[0].text
    assert "1996" in table_paras[0].text


# ── Markdown rendering of table paragraphs ─────────────────────────────────


def test_table_paragraph_renders_as_pipe_table_in_markdown():
    """A Paragraph with css_class='table' renders as valid Markdown pipe table."""
    from legalize.models import Paragraph

    table_text = (
        "| Ejercicio | Por ciento |\n"
        "| --- | --- |\n"
        "| 1996 | 12.50 |\n"
        "| 1999 | 10.00 |"
    )
    p = Paragraph(css_class="table", text=table_text)
    rendered = render_paragraphs([p])
    assert "| Ejercicio | Por ciento |" in rendered
    assert "| --- | --- |" in rendered
    assert "| 1996 | 12.50 |" in rendered


# ── Integration: CFF.doc tarifa table from HTTP cache ──────────────────────


@pytest.mark.skipif(
    not __import__("pathlib").Path(".cache/http_cache.sqlite").exists(),
    reason="HTTP cache not available",
)
def test_cff_formula_variables_grouped_into_table():
    """CFF.doc: the UDI formula variable definitions are grouped into a table.

    In the CFF.doc file, the formula variables 'd', 'm', 'UDId,m' etc. are
    stored as consecutive \\r-paragraphs where rows 2..N start with \\x07\\x07.
    After the fix they must appear as one multi-row pipe table, not as
    individual 1-row tables.
    """
    import requests_cache

    s = requests_cache.CachedSession(".cache/http_cache.sqlite", backend="sqlite")
    resp = s.get("https://www.diputados.gob.mx/LeyesBiblio/doc/CFF.doc")
    if not resp.from_cache:
        pytest.skip("CFF.doc not in cache")

    paras = _extract_doc_paragraphs(resp.content)

    # Find the UDI formula table (contains row for 'd' and 'm').
    # After the fix it should be a single paragraph with multiple rows.
    udi_tables = [
        p for p in paras
        if p.startswith("| ") and "UDI" in p
    ]
    assert udi_tables, "UDI formula variable table not found"

    # The first such table should have at least 4 rows (d, m, UDId,m, UDId-1,m)
    first_table = udi_tables[0]
    lines = first_table.splitlines()
    data_rows = [l for l in lines if l.startswith("| ") and "---" not in l]
    assert len(data_rows) >= 4, (
        f"Expected at least 4 rows in UDI formula table, got {len(data_rows)}"
    )


@pytest.mark.skipif(
    not __import__("pathlib").Path(".cache/http_cache.sqlite").exists(),
    reason="HTTP cache not available",
)
def test_cff_tax_tarifa_table_still_correct():
    """CFF.doc: the Ejercicio/Por ciento tarifa table still renders correctly.

    This is the canonical example from the bug report — must render as a
    4-row × 2-col pipe table, not as merged prose.
    """
    import requests_cache

    s = requests_cache.CachedSession(".cache/http_cache.sqlite", backend="sqlite")
    resp = s.get("https://www.diputados.gob.mx/LeyesBiblio/doc/CFF.doc")
    if not resp.from_cache:
        pytest.skip("CFF.doc not in cache")

    paras = _extract_doc_paragraphs(resp.content)

    tarifa_tables = [
        p for p in paras
        if p.startswith("| ") and "Ejercicio" in p and "Por ciento" in p
    ]
    assert tarifa_tables, "Tarifa table not found in CFF paragraphs"

    table = tarifa_tables[0]
    assert "1996" in table
    assert "12.50" in table
    assert "1999" in table
    assert "10.00" in table

    # Must NOT be merged prose — "Ejercicio" and "1996" must be on separate lines
    lines = table.splitlines()
    ejercicio_line = next((l for l in lines if "Ejercicio" in l), None)
    assert ejercicio_line is not None
    year_line = next((l for l in lines if "1996" in l), None)
    assert year_line is not None
    assert ejercicio_line != year_line, "Ejercicio and 1996 must be on separate rows"
