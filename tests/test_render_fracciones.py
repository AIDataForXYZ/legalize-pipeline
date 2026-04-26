"""Tests for fraccion / apartado rendering normalisation.

Bug: Roman-numeral fracciones arrived as bare prose, e.g.
    "I Pudieren verse perjudicadas..."
Fix: renderer adds a period after the numeral so the output reads
    "I. Pudieren verse perjudicadas..."

Scope: renderer-side only (markdown.py).  Parser is unchanged.
"""
from __future__ import annotations

import pytest

from legalize.transformer.markdown import _normalise_fraccion_text, render_paragraphs
from legalize.models import Paragraph


# ── _normalise_fraccion_text unit tests ───────────────────────────────────────


@pytest.mark.parametrize("raw, expected", [
    # Standard fracciones without a period — must gain one
    ("I Pudieren verse perjudicadas las personas que hayan",
     "I. Pudieren verse perjudicadas las personas que hayan"),
    ("II Reciban requerimientos de información o documentación",
     "II. Reciban requerimientos de información o documentación"),
    ("III Otros supuestos que determine la ley",
     "III. Otros supuestos que determine la ley"),
    ("IV Las autoridades competentes",
     "IV. Las autoridades competentes"),
    ("V El Estado garantizará",
     "V. El Estado garantizará"),
    ("IX Derecho a la salud",
     "IX. Derecho a la salud"),
    ("XIV La ley determinará",
     "XIV. La ley determinará"),
    ("XIX El Congreso tendrá facultad",
     "XIX. El Congreso tendrá facultad"),
])
def test_fraccion_bare_gains_period(raw, expected):
    assert _normalise_fraccion_text(raw) == expected


@pytest.mark.parametrize("text", [
    # Already has a period — must be left alone
    "I. Pudieren verse perjudicadas",
    "II. Reciban requerimientos",
    # Already has a dash-separator
    "I.- Texto del inciso",
    # Already has a parenthesis separator
    "I) Texto del inciso",
    # Stray lone numeral with no body — skip (no uppercase after space)
    "I",
    "II",
    # Mid-sentence reference — "en el artículo X" starts with lowercase
    "en el artículo I de esta ley",
    # Plain prose that starts with a word that happens to be an abbreviation
    "Indicadores de calidad del servicio",
    # Lowercase apartado — handled separately, not by this function
    "a) Texto del apartado",
])
def test_fraccion_already_punctuated_or_not_a_fraccion(text):
    assert _normalise_fraccion_text(text) == text


# ── render_paragraphs integration: parrafo css class ─────────────────────────


def _make_parrafo(text: str) -> Paragraph:
    return Paragraph(css_class="parrafo", text=text)


def test_render_paragraphs_adds_period_to_roman_fraccion():
    """render_paragraphs must output 'I. Texto' not 'I Texto' for bare fracciones."""
    paras = [
        _make_parrafo("Las personas tienen derecho a:"),
        _make_parrafo("I Pudieren verse perjudicadas las personas que hayan"),
        _make_parrafo("II Reciban requerimientos de información"),
        _make_parrafo("III Otros supuestos que determine la ley"),
    ]
    md = render_paragraphs(paras)
    assert "I. Pudieren verse perjudicadas" in md
    assert "II. Reciban requerimientos" in md
    assert "III. Otros supuestos" in md
    # The bare forms must NOT appear in the output
    assert "I Pudieren" not in md
    assert "II Reciban" not in md
    assert "III Otros" not in md


def test_render_paragraphs_leaves_already_punctuated_alone():
    """Paragraphs already formatted 'I. ...' must be emitted verbatim."""
    paras = [
        _make_parrafo("I. La libertad de expresión es inviolable."),
        _make_parrafo("II. La libertad de reunión."),
    ]
    md = render_paragraphs(paras)
    assert "I. La libertad de expresión" in md
    assert "II. La libertad de reunión" in md
    # No double period
    assert "I.. " not in md
    assert "II.. " not in md


def test_render_paragraphs_does_not_touch_non_fraccion_prose():
    """Ordinary prose must pass through unchanged."""
    paras = [
        _make_parrafo("En los Estados Unidos Mexicanos todas las personas gozarán."),
        _make_parrafo("Esta ley entrará en vigor al día siguiente de su publicación."),
    ]
    md = render_paragraphs(paras)
    assert "En los Estados Unidos Mexicanos" in md
    assert "Esta ley entrará en vigor" in md


def test_render_paragraphs_lowercase_apartado_unchanged():
    """Lower-case 'a) ...' apartados pass through without modification by this fix.

    They were already being split into their own paragraphs by the parser;
    the renderer emits them verbatim (no period-insertion logic for them).
    """
    paras = [
        _make_parrafo("a) Cosa uno"),
        _make_parrafo("b) Cosa dos"),
    ]
    md = render_paragraphs(paras)
    assert "a) Cosa uno" in md
    assert "b) Cosa dos" in md


def test_render_paragraphs_nota_pie_unaffected():
    """nota_pie paragraphs (reform stamps) must be rendered as blockquotes, untouched."""
    paras = [
        Paragraph(css_class="nota_pie", text="Párrafo reformado DOF 04-12-2006"),
    ]
    md = render_paragraphs(paras)
    assert "> <small>Párrafo reformado DOF 04-12-2006</small>" in md


# ── End-to-end via _diputados_doc_block_run + render ─────────────────────────


def _doc_block_run(paragraphs: list[str]):
    """Drive DOC block builder with synthetic paragraphs and return blocks."""
    import base64
    from legalize.fetcher.mx import parser as mx_parser

    real = mx_parser._extract_doc_paragraphs
    mx_parser._extract_doc_paragraphs = lambda _b: paragraphs
    try:
        envelope = {
            "source": "diputados",
            "source_format": "doc",
            "norm_id": "DIP-TEST",
            "abbrev": "TEST",
            "title": "Ley de Prueba",
            "rank": "ley",
            "publication_date": "2020-01-01",
            "last_reform_date": "2024-06-15",
            "doc_url": "https://example.test/TEST.doc",
            "doc_b64": base64.b64encode(b"\xd0\xcf\x11\xe0stub").decode("ascii"),
        }
        return mx_parser._diputados_doc_blocks(envelope)
    finally:
        mx_parser._extract_doc_paragraphs = real


def test_doc_fracciones_render_with_period_end_to_end():
    """Full pipeline: DOC fracciones without period render with period in Markdown."""
    from legalize.transformer.markdown import render_paragraphs

    blocks = _doc_block_run([
        "Artículo 3o.- Las autoridades educativas tomarán medidas tendientes a:",
        "I Pudieren verse perjudicadas las personas de escasos recursos",
        "II Reciban requerimientos de información o documentación",
        "III Otros supuestos que determine la ley en la materia",
    ])

    art_blocks = [b for b in blocks if b.block_type == "article"]
    assert len(art_blocks) == 1

    md = render_paragraphs(art_blocks[0].versions[0].paragraphs)

    # BEFORE the fix these would appear as bare "I Pudieren…", "II Reciban…"
    # AFTER the fix they must have a period
    assert "I. Pudieren verse perjudicadas" in md
    assert "II. Reciban requerimientos" in md
    assert "III. Otros supuestos" in md

    # Bare forms must be absent
    assert "I Pudieren" not in md
    assert "II Reciban" not in md
    assert "III Otros" not in md
