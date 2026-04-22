"""Tests for transformer/anchor.py — anchor hint parsing and markdown
resolution over Stage-A-formatted documents."""

from __future__ import annotations


from legalize.transformer.anchor import (
    Anchor,
    parse_anchor_from_hint,
    resolve_anchor,
    resolve_anchor_from_hint,
)


# A synthetic Stage-A-shaped base Markdown, small enough for tests but
# covering the heading levels and apartado/letra formats we actually
# encounter in the corpus.
BASE_MARKDOWN = """\
---
title: "Fixture Law"
---

# LIBRO I

## TÍTULO I

### CAPÍTULO PRIMERO

###### Artículo 1. Ámbito de aplicación.

La presente Ley se aplicará a todos los sujetos indicados.

###### Artículo 5. Recursos económicos.

1. Los recursos economicos estaran compuestos por:

   a) Las cuotas de afiliacion.

   b) Los productos de las actividades propias.

   c) Las donaciones recibidas.

2. Tambien se incluiran las subvenciones publicas.

3. Los recursos extraordinarios se regularan aparte.

###### Artículo 10. Disposición final.

Entrada en vigor al dia siguiente.

## Disposición adicional primera.

Las referencias normativas se entenderan actualizadas.

## Disposición final tercera.

El Gobierno aprobara un reglamento en el plazo de seis meses.

## Anexo I

Contenido del anexo uno.
"""


# ──────────────────────────────────────────────────────────
# parse_anchor_from_hint
# ──────────────────────────────────────────────────────────


def test_parse_articulo_apartado_letra() -> None:
    a = parse_anchor_from_hint("la letra c) del apartado 2 del articulo 5")
    assert a.articulo == "5"
    assert a.apartado == "2"
    assert a.letra == "c"


def test_parse_articulo_with_ordinal_marker() -> None:
    """BOE formats articles as '5.º' / '5.ª' — the parser strips those."""
    a = parse_anchor_from_hint("el articulo 5.º de la Ley 37/1992")
    assert a.articulo == "5"


def test_parse_articulo_with_bis_ter() -> None:
    a = parse_anchor_from_hint("se anade el articulo 61 bis")
    # We keep "bis" attached because it's part of the article identifier.
    assert a.articulo is not None and "bis" in a.articulo.lower()


def test_parse_disposicion_compound() -> None:
    a = parse_anchor_from_hint("la disposicion adicional primera")
    assert a.disposicion == "adicional primera"


def test_parse_norma_circular_bde() -> None:
    """Circulares del BdE use 'norma 67' as top-level structural unit."""
    a = parse_anchor_from_hint("la letra b) del apartado 6 de la norma 67")
    assert a.norma == "67"
    assert a.apartado == "6"
    assert a.letra == "b"


def test_parse_anexo_roman() -> None:
    a = parse_anchor_from_hint("el apartado 3 del anexo II")
    assert a.anexo == "II"
    assert a.apartado == "3"


def test_parse_empty_hint_returns_empty_anchor() -> None:
    assert parse_anchor_from_hint("").is_empty
    assert parse_anchor_from_hint("texto sin señales").is_empty


# ──────────────────────────────────────────────────────────
# resolve_anchor — happy paths
# ──────────────────────────────────────────────────────────


def test_resolve_to_whole_articulo() -> None:
    pos = resolve_anchor(BASE_MARKDOWN, Anchor(articulo="5"))
    assert pos is not None
    assert pos.kind == "articulo"
    assert "Artículo 5." in pos.content
    # Must NOT include the following article.
    assert "Artículo 10." not in pos.content


def test_resolve_to_apartado_within_articulo() -> None:
    pos = resolve_anchor(BASE_MARKDOWN, Anchor(articulo="5", apartado="2"))
    assert pos is not None
    assert pos.kind == "apartado"
    assert "subvenciones publicas" in pos.content
    # Must not bleed into apartado 3 nor include apartado 1.
    assert "Los recursos extraordinarios" not in pos.content
    assert "cuotas de afiliacion" not in pos.content


def test_resolve_to_letra_within_apartado() -> None:
    pos = resolve_anchor(BASE_MARKDOWN, Anchor(articulo="5", apartado="1", letra="b"))
    assert pos is not None
    assert pos.kind == "letra"
    assert "productos de las actividades propias" in pos.content
    # Letras a and c must NOT be included.
    assert "cuotas de afiliacion" not in pos.content
    assert "donaciones recibidas" not in pos.content


def test_resolve_to_disposicion() -> None:
    pos = resolve_anchor(BASE_MARKDOWN, Anchor(disposicion="adicional primera"))
    assert pos is not None
    assert pos.kind == "disposicion"
    assert "referencias normativas" in pos.content
    assert "Gobierno aprobara" not in pos.content  # that's disp. final


def test_resolve_disposicion_does_not_leak_into_final() -> None:
    pos = resolve_anchor(BASE_MARKDOWN, Anchor(disposicion="final tercera"))
    assert pos is not None
    assert "Gobierno aprobara" in pos.content
    assert "referencias normativas" not in pos.content


def test_resolve_to_anexo() -> None:
    pos = resolve_anchor(BASE_MARKDOWN, Anchor(anexo="I"))
    assert pos is not None
    assert "Contenido del anexo uno" in pos.content


# ──────────────────────────────────────────────────────────
# resolve_anchor — failure modes
# ──────────────────────────────────────────────────────────


def test_resolve_returns_none_for_empty_anchor() -> None:
    assert resolve_anchor(BASE_MARKDOWN, Anchor()) is None


def test_resolve_returns_none_for_nonexistent_articulo() -> None:
    assert resolve_anchor(BASE_MARKDOWN, Anchor(articulo="999")) is None


def test_resolve_returns_none_for_nonexistent_apartado() -> None:
    assert resolve_anchor(BASE_MARKDOWN, Anchor(articulo="5", apartado="99")) is None


def test_resolve_returns_none_for_nonexistent_letra() -> None:
    assert resolve_anchor(BASE_MARKDOWN, Anchor(articulo="5", apartado="1", letra="z")) is None


def test_resolve_returns_none_on_heading_ambiguity() -> None:
    """When a heading appears twice (copy-paste artefact, pseudo-structural
    duplication), resolution must decline rather than guess."""
    md = BASE_MARKDOWN + "\n\n###### Artículo 5. Duplicado accidental.\n\nTexto raro.\n"
    assert resolve_anchor(md, Anchor(articulo="5")) is None


# ──────────────────────────────────────────────────────────
# Ordinal apartado styles
# ──────────────────────────────────────────────────────────


def test_resolve_apartado_with_ordinal_word_leader() -> None:
    """BOE sometimes numbers apartados as 'Uno.' / 'Dos.' instead of '1.' /
    '2.'. The resolver must match both shapes; the anchor is normalised to
    digits by parse_anchor_from_hint."""
    md = """\
###### Artículo 3. Subvenciones.

Uno. El Estado otorgara subvenciones anuales.

Dos. Dichas subvenciones se distribuiran en funcion del numero de escaños.

Tres. Las subvenciones seran compatibles.
"""
    pos = resolve_anchor(md, Anchor(articulo="3", apartado="2"))
    assert pos is not None
    assert "Dichas subvenciones" in pos.content
    assert "compatibles" not in pos.content


def test_resolve_apartado_via_word_in_hint() -> None:
    """If the anchor_hint says 'apartado dos', it should still match
    'Dos.' — or '2.'."""
    md = """\
###### Artículo 3. Subvenciones.

1. Primera.

2. Segunda.

3. Tercera.
"""
    pos = resolve_anchor_from_hint(md, "apartado dos del articulo 3")
    assert pos is not None
    assert "Segunda." in pos.content
    assert "Primera." not in pos.content


# ──────────────────────────────────────────────────────────
# Convenience entry
# ──────────────────────────────────────────────────────────


def test_resolve_anchor_from_hint_happy_path() -> None:
    pos = resolve_anchor_from_hint(
        BASE_MARKDOWN,
        "la letra a) del apartado 1 del articulo 5",
    )
    assert pos is not None
    assert "cuotas de afiliacion" in pos.content
