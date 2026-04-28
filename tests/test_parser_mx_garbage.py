"""Regression test for MX parser binary-garbage filter.

Loads the corpus of 152 garbage lines extracted from the closed-PR cleanup
(``tests/fixtures/mx/garbage/closed_pr_corpus.txt``) and asserts that every
line is recognised as garbage by the parser's tail-trim entry points.  Also
asserts that a small set of known-good Spanish legislative sentences are NOT
flagged (anti-false-positive).

This test guards against regressions in ``_is_binary_garbage``,
``_TAIL_PARAGRAPH_GARBAGE_RE``, and ``_is_garbage_table_row`` — the three
helpers that together define the "is this paragraph stylesheet/binary noise?"
verdict at the document tail.
"""

from pathlib import Path

import pytest

from legalize.fetcher.mx.parser import (
    _TAIL_PARAGRAPH_GARBAGE_RE,
    _is_binary_garbage,
    _is_garbage_table_row,
)

FIXTURE = Path(__file__).parent / "fixtures" / "mx" / "garbage" / "closed_pr_corpus.txt"


def _is_tail_garbage(line: str) -> bool:
    """Mirror the parser's tail-trim verdict.

    Matches the predicate used by ``_truncate_tail_blob._is_tail_garbage`` and
    by the final micro-trim pass — any of the three helpers returning True
    means the paragraph would be dropped at the document tail.
    """
    if _is_binary_garbage(line):
        return True
    if _TAIL_PARAGRAPH_GARBAGE_RE.search(line):
        return True
    if _is_garbage_table_row(line):
        return True
    return False


def _load_corpus() -> list[str]:
    return [
        line.rstrip("\n")
        for line in FIXTURE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_closed_pr_corpus_fully_filtered():
    """Every line in the closed-PR garbage corpus is recognised as garbage."""
    corpus = _load_corpus()
    assert len(corpus) == 152, f"expected 152 corpus lines, got {len(corpus)}"
    survivors = [line for line in corpus if not _is_tail_garbage(line)]
    assert survivors == [], (
        f"{len(survivors)} corpus lines were not recognised as garbage:\n"
        + "\n".join(f"  - {s!r}" for s in survivors[:10])
    )


@pytest.mark.parametrize(
    "line",
    [
        # Article body and headings.
        "Artículo 1o. Las personas son libres y tienen derecho a la dignidad.",
        "Artículo 2o.- Los derechos humanos son universales.",
        "Artículo Primero. Esta ley entrará en vigor al día siguiente.",
        # Section headings (all-caps).
        "TÍTULO PRIMERO",
        "CAPÍTULO ÚNICO",
        "CAPÍTULO PRIMERO",
        # Promulgation block.
        "En cumplimiento de lo dispuesto por la fracción I del Artículo 89.",
        "Ciudad de México, a 5 de febrero de 1917.- Rúbrica.",
        # Reform stamp.
        "Última Reforma DOF 17-02-2024",
        # Fracciones with tab separator (parser's convention for indent).
        "I.\tLos derechos humanos reconocidos.",
        "II.\tEl interés superior del menor.",
        "a)\tLos servicios públicos.",
        "1.\tDe la Federación.",
        # Markdown table content (real cells with Spanish words).
        "| Tabla 1 | Resumen | Estado |",
        "| Activo | Vigente | Federal |",
        # Short legitimate fragments that appeared in synthetic table-render tests.
        "Col1",
        "Header",
        "Col1 Col2 Col3",
        # Common short prose.
        "México, D.F., a 7 de junio de 2024",
        "la cual se publicó",
        "es el órgano competente",
        "inscríbase en el Diario Oficial",
    ],
)
def test_legitimate_text_not_flagged(line: str):
    """Anti-false-positive: real Spanish legislative text passes the filter."""
    assert not _is_tail_garbage(line), (
        f"legitimate text was flagged as garbage: {line!r}"
    )
