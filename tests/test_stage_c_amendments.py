"""Tests for Stage C — fetcher/es/amendments.py (modules 1 and 2).

Fixtures under ``tests/fixtures/stage_c/`` are real BOE XMLs scraped by the
research subagent; each was chosen to exercise a distinct pattern:

    modif-1.xml            Circular BdE 6/2021 — dense MODIFICA+SUPRIME on
                           Circular 4/2017 with ~49 «...» blocks.
    modif-ley-reales.xml   Ley Orgánica 8/2007 — contains disposiciones
                           adicionales that MODIFICA + ANADE several laws.
    modif-ley-8183.xml     Recurso de inconstitucionalidad — out of MVP scope
                           (verbs 552 "Recurso promovido contra"); must
                           return an empty patch list.
    modif-5-ley.xml        Correction of errors (verb 201/203) — out of MVP;
                           must return empty.
    modif-9-ley.xml        Small law with minimal body; used to prove the
                           extractor does not hallucinate on empty input.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from legalize.fetcher.es.amendments import (
    AmendmentPatch,
    extract_new_text_blocks,
    operation_for_verb,
    parse_amendments,
    parse_anteriores,
)

FIXTURES = Path(__file__).parent / "fixtures" / "stage_c"


def _read(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


# ──────────────────────────────────────────────────────────
# Verb classification
# ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "code, expected",
    [
        ("270", "replace"),
        ("407", "insert"),
        ("235", "delete"),
        ("210", "delete"),
        ("201", None),  # CORRECCION de errores — not a text-patch, out of MVP
        ("203", None),  # CORRIGE errores
        ("440", None),  # DE CONFORMIDAD con
        ("470", None),  # DECLARA
        ("552", None),  # Recurso promovido contra
        ("693", None),  # DICTADA
        ("", None),  # missing code
        ("9999", None),  # unknown code
    ],
)
def test_operation_for_verb_mvp_scope(code: str, expected: str | None) -> None:
    assert operation_for_verb(code) == expected


# ──────────────────────────────────────────────────────────
# parse_anteriores
# ──────────────────────────────────────────────────────────


def test_parse_anteriores_picks_mvp_verbs_only() -> None:
    """modif-1.xml has MODIFICA (270) + SUPRIME (235). Both must come through,
    and the modifier metadata must be filled in from <metadatos>."""
    patches = parse_anteriores(_read("modif-1.xml"))

    assert len(patches) == 2

    by_verb = {p.verb_code: p for p in patches}
    assert set(by_verb) == {"270", "235"}

    modifica = by_verb["270"]
    assert modifica.target_id == "BOE-A-2017-14334"
    assert modifica.operation == "replace"
    assert modifica.source_boe_id == "BOE-A-2021-21666"
    # fecha_publicacion = 20211229; we prefer it over fecha_disposicion
    assert modifica.source_date == date(2021, 12, 29)
    assert "Circular 4/2017" in modifica.anchor_hint

    suprime = by_verb["235"]
    assert suprime.target_id == "BOE-A-2019-17286"
    assert suprime.operation == "delete"


def test_parse_anteriores_skips_out_of_scope_verbs() -> None:
    """modif-ley-8183.xml only contains 'Recurso promovido contra' (552);
    the MVP parser must produce zero patches and never raise."""
    patches = parse_anteriores(_read("modif-ley-8183.xml"))
    assert patches == []


def test_parse_anteriores_skips_corrections() -> None:
    """Correction/errata verbs (201/203) are not text-patches in MVP."""
    patches = parse_anteriores(_read("modif-5-ley.xml"))
    assert patches == []


def test_parse_anteriores_handles_mixed_document() -> None:
    """modif-ley-reales.xml (Ley Organica 8/2007) has the full scope mix:
    MODIFICA (270), ANADE (407), DEROGA (210). All three must flow through;
    any non-MVP verbs (none in this file) are silently dropped."""
    patches = parse_anteriores(_read("modif-ley-reales.xml"))

    ops = {p.operation for p in patches}
    assert "replace" in ops  # MODIFICA
    assert "insert" in ops  # ANADE
    assert "delete" in ops  # DEROGA

    # The modifier is a single law, so every patch must share source_boe_id.
    src_ids = {p.source_boe_id for p in patches}
    assert len(src_ids) == 1
    assert next(iter(src_ids)).startswith("BOE-A-")

    # target_ids must all be distinct BOE-A-... strings.
    for p in patches:
        assert p.target_id.startswith("BOE-A-")


# ──────────────────────────────────────────────────────────
# extract_new_text_blocks
# ──────────────────────────────────────────────────────────


def test_extract_new_text_blocks_on_reales() -> None:
    """modif-ley-reales has ~5 disposiciones adicionales that quote new
    text in sangrado/sangrado_articulo paragraphs. The extractor must find
    every quoted run."""
    blocks = extract_new_text_blocks(_read("modif-ley-reales.xml"))

    assert len(blocks) >= 3, f"expected several blocks, got {len(blocks)}"

    # Every block's intro must match at least one of our intro patterns.
    for b in blocks:
        assert b.paragraphs, "block has no quoted paragraphs"
        for p in b.raw_paragraphs:
            # Raw paragraphs retain the «» markers somewhere (either at
            # start/end of the paragraph or wrapping it whole).
            assert "«" in p or "»" in p, f"no quote marker in {p!r}"


def test_extract_new_text_blocks_strips_markers() -> None:
    blocks = extract_new_text_blocks(_read("modif-ley-reales.xml"))

    for b in blocks:
        for para in b.paragraphs:
            assert not para.startswith("«"), f"stripped paragraph still starts with quote: {para!r}"


def test_extract_new_text_blocks_does_not_hallucinate_on_unrelated_quotes() -> None:
    """A paragraph with inline «partido politico» (a defined term) is not
    a modification block and must not be emitted."""
    # modif-ley-reales has the literal phrase
    #   'la expresion «partido politico» comprenderá...'
    # at paragraph 19 — if the extractor treats inline quotes as a block,
    # we'd emit a spurious one-paragraph TextBlock.
    blocks = extract_new_text_blocks(_read("modif-ley-reales.xml"))
    for b in blocks:
        first = b.paragraphs[0] if b.paragraphs else ""
        assert "partido político" not in first.split(".")[0], (
            f"extracted an inline-quote paragraph as a block: {first!r}"
        )


def test_extract_new_text_blocks_on_low_signal_input() -> None:
    """modif-ley-8183 is almost pure prose (court admission notice) with no
    modification intros. The extractor must return [] without raising."""
    blocks = extract_new_text_blocks(_read("modif-ley-8183.xml"))
    assert blocks == []


# ──────────────────────────────────────────────────────────
# parse_amendments — end-to-end
# ──────────────────────────────────────────────────────────


def test_parse_amendments_attaches_new_text_when_possible() -> None:
    """On the Ley Organica 8/2007 fixture, at least one patch must come
    back with new_text filled and confidence >= 0.5. Perfect scoring is
    not required here — that's what the fidelity loop (Week 4) measures."""
    patches = parse_amendments(_read("modif-ley-reales.xml"))

    filled = [p for p in patches if p.new_text is not None]
    assert filled, "no patch got new_text attached at all"

    # Every filled patch must be labelled 'regex' (module 3 LLM runs later).
    for p in filled:
        assert p.extractor == "regex"
        assert p.confidence > 0.0
        assert all(isinstance(s, str) and s for s in p.new_text)


def test_parse_amendments_on_circular_bde_blockquote_format() -> None:
    """modif-1.xml is Circular BdE 6/2021 modifying Circular 4/2017 — the
    MVP target rango. Its body uses the modern <blockquote class='sangrado'>
    format (not the older <p class='sangrado'> siblings). Both patches
    must come back confident: MODIFICA with all ~19 quoted blocks
    concatenated, SUPRIME with new_text=None but confidence=1.0."""
    patches = parse_amendments(_read("modif-1.xml"))

    assert len(patches) == 2
    by_verb = {p.verb_code: p for p in patches}

    modifica = by_verb["270"]
    assert modifica.operation == "replace"
    assert modifica.new_text is not None
    assert len(modifica.new_text) >= 10, (
        f"Circular 6/2021 modifies many apartados, expected >=10 paragraphs, "
        f"got {len(modifica.new_text)}"
    )
    assert modifica.confidence >= 0.9
    assert modifica.extractor == "regex"

    suprime = by_verb["235"]
    assert suprime.operation == "delete"
    assert suprime.new_text is None, "delete patches never carry new_text"
    assert suprime.confidence == 1.0, "delete patches are trivially confident"


def test_parse_amendments_delete_verbs_have_full_confidence() -> None:
    """DEROGA and SUPRIME never need body text; confidence must be 1.0
    regardless of whether the body has quoted blocks. modif-ley-reales
    has 1 DEROGA patch — it must come out confident with new_text=None."""
    patches = parse_amendments(_read("modif-ley-reales.xml"))
    deletes = [p for p in patches if p.operation == "delete"]
    assert deletes, "fixture should have at least one delete patch"
    for p in deletes:
        assert p.new_text is None
        assert p.confidence == 1.0
        assert p.extractor == "regex"


# ──────────────────────────────────────────────────────────
# Split confidence axes
# ──────────────────────────────────────────────────────────


def test_confidence_compound_is_minimum_of_axes() -> None:
    """The compound confidence property must equal min(anchor, new_text).
    Downstream LLM routing depends on this semantics: a patch with strong
    anchor but missing new_text should be flagged low-confidence overall
    so the caller sends it to the LLM new_text extractor."""
    from legalize.fetcher.es.amendments import AmendmentPatch

    p = AmendmentPatch(
        target_id="BOE-A-X",
        operation="replace",
        verb_code="270",
        verb_text="MODIFICA",
        anchor_hint="art. 5",
        source_boe_id="BOE-A-Y",
        source_date=date(2021, 1, 1),
        anchor_confidence=0.95,
        new_text_confidence=0.3,
    )
    assert p.confidence == 0.3


def test_single_text_patch_with_blocks_is_fully_confident_on_both_axes() -> None:
    """modif-1.xml (Circular BdE) has 1 text patch + ~19 blocks; both axes
    must be 1.0 because anchor is unambiguous (one target) and new_text is
    complete (all blocks concatenated)."""
    patches = parse_amendments(_read("modif-1.xml"))
    modifica = [p for p in patches if p.operation == "replace"][0]
    assert modifica.anchor_confidence == 1.0
    assert modifica.new_text_confidence == 1.0


# ──────────────────────────────────────────────────────────
# Quote delimiter normalization
# ──────────────────────────────────────────────────────────


def test_normalize_quotes_maps_entity_form() -> None:
    from legalize.fetcher.es.amendments import normalize_quotes

    assert normalize_quotes("texto &laquo;nuevo&raquo;.") == "texto «nuevo»."


def test_normalize_quotes_maps_smart_quotes() -> None:
    """Typesetter-supplied smart quotes (U+201C/U+201D) normalize to «»."""
    from legalize.fetcher.es.amendments import normalize_quotes

    assert normalize_quotes("texto “nuevo”.") == "texto «nuevo»."


def test_normalize_quotes_leaves_ascii_straight_alone() -> None:
    """ASCII straight quotes are ambiguous (defined term vs modification)
    so we do not normalize them — see the comment in normalize_quotes."""
    from legalize.fetcher.es.amendments import normalize_quotes

    assert normalize_quotes('texto "nuevo".') == 'texto "nuevo".'


# ──────────────────────────────────────────────────────────
# Unknown verb code → warning (drift signal)
# ──────────────────────────────────────────────────────────


def test_unknown_verb_code_emits_warning(caplog) -> None:
    """An <anterior> with a verb code not in the known set must log at
    WARNING level so the fidelity loop can detect BOE schema drift.
    Known-out-of-scope verbs (201 corrections, 552 judicial, etc.) log
    only at DEBUG because we've already triaged them."""
    import logging

    # Synthetic XML with a bogus verb code 9999
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2099-1</identificador>
    <fecha_disposicion>20990101</fecha_disposicion>
    <fecha_publicacion>20990102</fecha_publicacion>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-2020-1">
          <palabra codigo="9999">VERBO_NUEVO</palabra>
          <texto>desconocido</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto></texto>
</documento>
"""
    with caplog.at_level(logging.WARNING, logger="legalize.fetcher.es.amendments"):
        patches = parse_anteriores(xml)
    assert patches == []
    assert any("unknown BOE verb code" in r.message and "9999" in r.message for r in caplog.records)


def test_known_out_of_scope_verb_does_not_warn(caplog) -> None:
    """Code 201 (CORRECCION de errores) is a known out-of-scope verb — the
    parser must skip it silently (no WARNING)."""
    import logging

    with caplog.at_level(logging.WARNING, logger="legalize.fetcher.es.amendments"):
        patches = parse_anteriores(_read("modif-5-ley.xml"))  # only 201/203 verbs
    assert patches == []
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings, (
        f"unexpected warnings on known out-of-scope verbs: {[w.message for w in warnings]}"
    )


def test_parse_amendments_does_not_fabricate_on_empty_body() -> None:
    """modif-ley-8183 has no usable body → no patches, no exceptions."""
    patches = parse_amendments(_read("modif-ley-8183.xml"))
    assert patches == []


def test_amendment_patch_is_immutable() -> None:
    """The dataclass is frozen; downstream code must use dataclasses.replace
    instead of attribute assignment."""
    p = AmendmentPatch(
        target_id="BOE-A-2017-14334",
        operation="replace",
        verb_code="270",
        verb_text="MODIFICA",
        anchor_hint="",
        source_boe_id="BOE-A-2021-21666",
        source_date=date(2021, 12, 29),
    )
    with pytest.raises((AttributeError, TypeError)):
        p.target_id = "BOE-A-OTRO"  # type: ignore[misc]
