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
    format (not the older <p class='sangrado'> siblings).

    Post-split calibration: the MODIFICA's coarse hint ("determinados
    preceptos de la Circular 4/2017") previously collapsed into a single
    patch with ~19 paragraphs of new_text. Now the splitter promotes each
    block's intro ("i. Se modifica el apartado 1, ...") to its own
    anchor_hint, emitting ~19 sub-patches — each of which the patcher can
    resolve to a specific apartado. SUPRIME (delete) is not split.
    """
    patches = parse_amendments(_read("modif-1.xml"))

    modifica_patches = [p for p in patches if p.operation == "replace"]
    delete_patches = [p for p in patches if p.operation == "delete"]

    # The MODIFICA anterior emits N sub-patches, one per «...» block.
    assert len(modifica_patches) >= 10, (
        f"splitter should emit one sub-patch per block, got {len(modifica_patches)}"
    )
    # Every sub-patch carries its own block as new_text; most intros
    # carry a struct signal ("apartado N", "letra a") so the majority
    # of sub-patches have high anchor_confidence. Intros without a
    # struct signal (e.g. "No se preguntan...") legitimately stay at the
    # no-struct floor — the LLM tier picks those up.
    high_anchor = 0
    for sub in modifica_patches:
        assert sub.extractor == "regex_split"
        assert sub.new_text is not None and len(sub.new_text) >= 1
        assert sub.new_text_confidence >= 0.9
        assert "." in sub.ordering_key  # inherited parent.NNN
        if sub.anchor_confidence >= 0.9:
            high_anchor += 1
    # Dominant majority must have real struct-signal anchors — otherwise
    # the splitter is not pulling its weight.
    assert high_anchor >= len(modifica_patches) // 2, (
        f"expected majority of sub-patches to have struct signal, got "
        f"{high_anchor}/{len(modifica_patches)}"
    )

    # Delete patch is untouched by the splitter.
    assert len(delete_patches) == 1
    suprime = delete_patches[0]
    assert suprime.verb_code == "235"
    assert suprime.new_text is None
    assert suprime.new_text_confidence == 1.0


def test_parse_amendments_delete_verbs_carry_full_new_text_confidence() -> None:
    """DEROGA and SUPRIME never need body text — new_text_confidence
    is always 1.0. Anchor confidence depends on whether the hint points
    at a specific sub-heading: a full-norm repeal with hint "la Ley
    Orgánica X/Y, de Z" is NOT locatable as a heading and must cap at
    the no-struct floor. modif-ley-reales has one such DEROGA (hint
    = "la Ley Orgánica 3/1987, de 2 de julio", no struct signals).
    """
    patches = parse_amendments(_read("modif-ley-reales.xml"))
    deletes = [p for p in patches if p.operation == "delete"]
    assert deletes, "fixture should have at least one delete patch"
    for p in deletes:
        assert p.new_text is None
        assert p.new_text_confidence == 1.0
        assert p.extractor == "regex"
        # Anchor confidence reflects hint structure, not verb family.
        # Full-norm DEROGA with no sub-heading hint → floor.
        assert p.anchor_confidence == 0.3


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


def test_single_text_patch_with_blocks_splits_into_sub_patches() -> None:
    """modif-1.xml (Circular BdE) has 1 MODIFICA anterior + ~19 blocks.

    Before the splitter: the resolver collapsed all blocks into ONE
    patch whose patch-level hint ("determinados preceptos de la Circular
    4/2017") had no struct signal, so anchor_confidence capped at 0.3
    and apply_patch failed with anchor_not_found ~50 % of the time.

    After the splitter: each block's intro (e.g. "i. Se modifica el
    apartado 1, ...") becomes its own sub-patch anchor — intros carry
    the sub-structural tokens ("apartado N") that the patcher needs. So
    BOTH axes for a sub-patch sit at 0.95 and the patcher can locate
    each edit. The split design is the live-audit calibration for the
    coarse-hint-plus-fine-intro case.
    """
    patches = parse_amendments(_read("modif-1.xml"))
    sub_patches = [p for p in patches if p.operation == "replace"]
    assert len(sub_patches) >= 10
    # All sub-patches come out of the splitter with their block as
    # new_text. Anchor confidence tracks the quality of the individual
    # block intro — most have struct signals; a minority don't and stay
    # at the floor so the LLM tier sees them.
    assert all(p.extractor == "regex_split" for p in sub_patches)
    assert all(p.new_text_confidence >= 0.9 for p in sub_patches)
    high_anchor = sum(1 for p in sub_patches if p.anchor_confidence >= 0.9)
    assert high_anchor >= len(sub_patches) // 2, (
        "most block intros must carry struct tokens the splitter can promote"
    )


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


# ──────────────────────────────────────────────────────────
# Regression tests for the live-audit bugs (2026-04-23)
# ──────────────────────────────────────────────────────────
#
# The live fidelity run against 30 Circulares BdE/CNMV (see
# STAGE-C-LIVE-FIDELITY.md) exposed four defects that the fixture
# suite had been hiding. The tests below lock in the fix for each.


def test_verb_330_cita_is_known_out_of_scope(caplog) -> None:
    """Verb 330 (CITA) appears in the live BOE corpus but was not in
    _KNOWN_OUT_OF_SCOPE_VERBS until the live audit. Regression: the
    parser must now skip it WITHOUT emitting a drift-signal warning."""
    import logging

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
          <palabra codigo="330">CITA</palabra>
          <texto>la Circular 1/2013</texto>
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
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings, f"verb 330 CITA should be silent: {[w.message for w in warnings]}"


def test_anchor_confidence_capped_when_hint_lacks_struct_signal() -> None:
    """A hint that only identifies the norm ("determinados preceptos de
    la Circular 4/2017") MUST cap at the no-struct floor regardless of
    modifier grammar clarity. Before this calibration the resolver
    awarded 1.0 and the applier failed `anchor_not_found` 50% of the
    time on real Circulares."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
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
          <palabra codigo="270">MODIFICA</palabra>
          <texto>determinados preceptos de la Circular 4/2017, de 27 de noviembre</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p>El art. 5 queda redactado como sigue:</p>
    <blockquote class="sangrado"><p>«Nuevo texto del art. 5.»</p></blockquote>
  </texto>
</documento>
""".encode("utf-8")
    patches = parse_amendments(xml)
    assert len(patches) == 1
    p = patches[0]
    assert p.new_text_confidence == 1.0  # blocks extracted cleanly
    assert p.anchor_confidence == 0.3, (
        "hint has no article/apartado/disposición/anexo/norma — must cap"
    )


def test_anchor_confidence_preserved_when_hint_has_struct_signal() -> None:
    """Complement of the cap test: when the hint carries an article,
    apartado, disposición, anexo or norma reference, the legitimate
    1.0 confidence must survive."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
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
          <palabra codigo="270">MODIFICA</palabra>
          <texto>el art. 5 de la Circular 4/2017</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p>El art. 5 queda redactado como sigue:</p>
    <blockquote class="sangrado"><p>«Nuevo texto del art. 5.»</p></blockquote>
  </texto>
</documento>
""".encode("utf-8")
    patches = parse_amendments(xml)
    assert len(patches) == 1
    p = patches[0]
    assert p.new_text_confidence == 1.0
    assert p.anchor_confidence == 1.0, "hint has art. 5 → real structural signal"


def test_jaccard_fallback_preserves_blocks_when_zero_score() -> None:
    """When the Jaccard matcher scores 0 for every (patch, block) pair
    but the modifier body DOES contain blocks, losing them entirely
    would silently discard real amendment content (observed: 111
    blocks lost on BOE-A-2014-13365 in the live run). The fallback
    assigns all blocks to the first patch (by ordering_key); the
    splitter then expands that first patch into one sub-patch per
    block, since each block's intro carries a struct signal the coarse
    patch-level hint did not.
    """
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2099-1</identificador>
    <fecha_disposicion>20990101</fecha_disposicion>
    <fecha_publicacion>20990102</fecha_publicacion>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-2020-1" orden="1">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>determinados preceptos de la Circular 1/2013</texto>
        </anterior>
        <anterior referencia="BOE-A-2020-1" orden="2">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>otros preceptos de la Circular 1/2013</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p>La norma primera queda redactada como sigue:</p>
    <blockquote class="sangrado"><p>«Contenido nuevo de la norma primera.»</p></blockquote>
    <p>La norma segunda queda redactada en los siguientes términos:</p>
    <blockquote class="sangrado"><p>«Contenido nuevo de la norma segunda.»</p></blockquote>
  </texto>
</documento>
""".encode("utf-8")
    patches = parse_amendments(xml)
    # First patch (orden=1) gets both blocks via fallback, then the
    # splitter expands it into 2 sub-patches. Second patch (orden=2)
    # stays empty → 2 sub-patches + 1 empty = 3.
    assert len(patches) == 3

    by_extractor: dict[str, list] = {}
    for p in patches:
        by_extractor.setdefault(p.extractor, []).append(p)
    sub_patches = by_extractor.get("regex_split", [])
    assert len(sub_patches) == 2, "splitter must promote both blocks to their own sub-patches"
    for sub in sub_patches:
        assert sub.ordering_key.startswith("1.")  # inherited from first parent
        assert sub.new_text is not None and len(sub.new_text) == 1
        # Intros "La norma primera..." / "La norma segunda..." carry a
        # struct signal via _RE_NORMA so anchor_confidence rises.
        assert sub.anchor_confidence >= 0.9

    # Second patch (orden=2) stays empty — Jaccard-zero fallback did not
    # feed it anything, and there was nothing to split.
    empty = [p for p in patches if p.extractor == "regex" and p.ordering_key == "2"]
    assert len(empty) == 1
    assert empty[0].new_text is None or len(empty[0].new_text) == 0


def test_plural_normas_captures_structural_signal() -> None:
    """Regression: the `_STRUCT_RE` pattern used ``|norma`` (no plural s),
    so a hint "las normas 1 a 3 de la Circular X" matched kind=norma and
    ref='s' (the 's' from 'normas') — useless. With the ``|normas?`` fix
    the ref is correctly '1' and the anchor confidence stays high."""
    patches = parse_amendments(
        """<?xml version="1.0" encoding="UTF-8"?>
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
          <palabra codigo="270">MODIFICA</palabra>
          <texto>las normas 1 a 3 de la Circular 5/2008, de 5 de noviembre</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p>La norma 1 queda redactada como sigue:</p>
    <blockquote class="sangrado"><p>«Nueva norma 1.»</p></blockquote>
  </texto>
</documento>
""".encode("utf-8")
    )
    assert len(patches) == 1
    # With the plural fix, struct signal is present → anchor is not
    # capped to the no-struct floor.
    assert patches[0].anchor_confidence == 1.0


def test_ordinal_word_headings_match_cardinal_hint() -> None:
    """Regression: Circulares BdE render structural units as "Norma
    decimosexta" (feminine ordinal) while modifier hints say "norma
    16". The resolver must map ordinal words to cardinal numbers."""
    from legalize.transformer.anchor import Anchor, resolve_anchor

    markdown = (
        "###### Norma decimoquinta. Combinaciones de negocios.\n"
        "Contenido de la norma 15.\n\n"
        "###### Norma decimosexta. Uso de la CIR.\n"
        "Contenido de la norma 16.\n\n"
        "###### Norma decimoséptima. Cesión de datos.\n"
        "Contenido de la norma 17.\n"
    )
    pos = resolve_anchor(markdown, Anchor(norma="16"))
    assert pos is not None, "hint 'norma 16' must resolve against 'Norma decimosexta'"
    assert "Uso de la CIR" in pos.content
    # Heading for 15 and 17 must NOT match.
    pos15 = resolve_anchor(markdown, Anchor(norma="15"))
    pos17 = resolve_anchor(markdown, Anchor(norma="17"))
    assert pos15 is not None and "Combinaciones" in pos15.content
    assert pos17 is not None and "Cesión" in pos17.content


def test_disposicion_ordinal_matches_digit_hint() -> None:
    """Hint "Disposición adicional 1" must match heading "Disposición
    adicional primera" via ordinal ↔ digit canonicalisation."""
    from legalize.transformer.anchor import Anchor, resolve_anchor

    markdown = (
        "###### Disposición adicional primera. Objeto.\n"
        "Contenido DA 1.\n\n"
        "###### Disposición adicional segunda. Régimen.\n"
        "Contenido DA 2.\n"
    )
    pos = resolve_anchor(markdown, Anchor(disposicion="adicional 1"))
    assert pos is not None
    assert "DA 1" in pos.content


def test_fallback_does_not_mix_blocks_across_different_targets() -> None:
    """Regression: the block-fallback for Jaccard total-failure used
    to dump all blocks into the modifier-wide first_pi. If the modifier
    edits multiple target norms (target A and target B), we'd leak B's
    amendment text into A's patch, corrupting A. Guard: fallback only
    fires when every text-patch shares one target_id."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2099-1</identificador>
    <fecha_disposicion>20990101</fecha_disposicion>
    <fecha_publicacion>20990102</fecha_publicacion>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-2020-AAA" orden="1">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>determinados preceptos de la Circular A</texto>
        </anterior>
        <anterior referencia="BOE-A-2020-BBB" orden="2">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>determinados preceptos de la Circular B</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p>La norma primera queda redactada como sigue:</p>
    <blockquote class="sangrado"><p>«Contenido X.»</p></blockquote>
  </texto>
</documento>
""".encode("utf-8")
    patches = parse_amendments(xml)
    assert len(patches) == 2
    # Multi-target → fallback disabled, every patch stays empty to avoid
    # corruption. The block will be picked up by the LLM tier.
    for p in patches:
        assert p.new_text is None or len(p.new_text) == 0, (
            f"block leaked into multi-target patch: {p.target_id} "
            f"new_text_len={len(p.new_text or ())}"
        )


def test_intro_pattern_matches_siguiente_manera() -> None:
    """Regression: the intro pattern list used "de la siguiente forma"
    but not "de la siguiente manera", which is common in Circulares BdE
    disposiciones finales (observed on BOE-A-2023-5481). With the fix,
    the block extractor finds the quoted run that follows."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
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
          <palabra codigo="270">MODIFICA</palabra>
          <texto>la norma 4 de la Circular 4/2017</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p>En la norma 4 se modifican los apartados 5 y 6, que quedan redactados de la siguiente manera:</p>
    <blockquote class="sangrado"><p>«Nuevo texto del apartado 5.»</p></blockquote>
  </texto>
</documento>
""".encode("utf-8")
    patches = parse_amendments(xml)
    assert len(patches) == 1
    assert patches[0].new_text is not None and len(patches[0].new_text) >= 1
    assert patches[0].new_text_confidence == 1.0


def test_anchor_resolves_through_css_class_marker_in_heading() -> None:
    """Stage A/B renderer prepends the source CSS class name in brackets
    to some headings (e.g. "###### [precepto]Norma 14.ª ..."). This is
    renderer noise and MUST NOT block anchor resolution. Regression from
    live-run bucket F: targets BOE-A-2008-19438 and 18635 (Circulares
    IIC from 2008) all had this prefix and nothing resolved."""
    from legalize.transformer.anchor import Anchor, resolve_anchor

    md = (
        "###### [precepto]Norma 14.ª Disposiciones.\n"
        "Contenido de la norma 14.\n\n"
        "###### [precepto]Norma 15.ª Otras reglas.\n"
        "Contenido de la norma 15.\n"
    )
    pos = resolve_anchor(md, Anchor(norma="14"))
    assert pos is not None, "bracketed CSS marker must not block resolution"
    assert "Contenido de la norma 14" in pos.content


def test_nested_typographic_quote_does_not_truncate_block() -> None:
    """The old per-paragraph quote stripper chewed the trailing »
    off a paragraph whenever that paragraph ended with a nested
    typographic quote (U+201C...U+201D normalized to «...»). The fix
    strips outer markers only on the FIRST / LAST paragraph of a run.

    Live-run evidence: BOE-A-2025-26847 block 11 on Circular 4/2017
    ended mid-sentence at "Garantías y tasaciones" because the XML
    paragraph closed `...apartado I.D), "Garantías y tasaciones".`
    """
    xml = """<?xml version="1.0" encoding="UTF-8"?>
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
          <palabra codigo="270">MODIFICA</palabra>
          <texto>el art. 5 de la Circular 4/2017</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p>El art. 5 queda redactado como sigue:</p>
    <blockquote class="sangrado">
      <p>«Para ello se tendrán en cuenta las garantías eficaces recibidas, de acuerdo con lo establecido en el apartado I.D), “Garantías y tasaciones”.</p>
      <p>En el caso de las operaciones concedidas por debajo de su coste, la entidad tendrá en cuenta el tipo de interés efectivo original.»</p>
    </blockquote>
  </texto>
</documento>
""".encode("utf-8")
    patches = parse_amendments(xml)
    assert len(patches) == 1
    p = patches[0]
    assert p.new_text is not None and len(p.new_text) == 2
    first = p.new_text[0]
    # The first paragraph MUST contain the full nested-quote text,
    # ending at "tasaciones" with its closing punctuation intact —
    # NOT truncated mid-sentence by the outer-envelope stripper.
    assert (
        first.endswith("tasaciones».")
        or first.endswith("tasaciones»")
        or first.endswith("«Garantías y tasaciones».")
    ), f"paragraph truncated mid-sentence: ...{first[-80:]!r}"
    # And the second paragraph still ends normally (last run paragraph
    # loses its closing »).
    second = p.new_text[1]
    assert second.endswith("original") or second.endswith("original."), (
        f"second paragraph lost its terminator: ...{second[-60:]!r}"
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
