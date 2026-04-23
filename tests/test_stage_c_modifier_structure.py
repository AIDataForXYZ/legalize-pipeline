"""Unit + integration tests for fetcher/es/modifier_structure.py.

Covers:
- extract_modifier_sections on multi-target, single-target, and
  disposición-transitoria-heavy modifier XMLs.
- match_sections_to_patches substring matching + ambiguity policy.
- End-to-end: the integration into _attach_text_blocks correctly assigns
  blocks to the right patch for a real multi-target modifier (bucket J).
"""

from __future__ import annotations

from datetime import date

from legalize.fetcher.es.amendments import (
    AmendmentPatch,
    _iter_body,
    _load_root,
    extract_new_text_blocks,
    parse_amendments,
)
from legalize.fetcher.es.modifier_structure import (
    ModifierSection,
    blocks_in_section,
    extract_modifier_sections,
    match_sections_to_patches,
)


# ──────────────────────────────────────────────────────────
# Fixture builders — build minimal XML matching BOE diario shape
# ──────────────────────────────────────────────────────────


def _multi_target_modifier_xml() -> bytes:
    """A modifier that amends two Circulares in two articulo sections,
    followed by a non-amending disposición transitoria. Mirrors the
    BOE-A-2018-17880 shape from the live cache."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2099-0001</identificador>
    <fecha_publicacion>20990101</fecha_publicacion>
    <fecha_disposicion>20990101</fecha_disposicion>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-T-A" orden="10">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>determinados preceptos de la Circular 4/2017</texto>
        </anterior>
        <anterior referencia="BOE-A-T-B" orden="20">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>la norma 3 de la Circular 1/2013</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p class="articulo">Norma 1. Modificacion de la Circular 4/2017, de 27 de noviembre.</p>
    <p class="parrafo">Se introducen las siguientes modificaciones en la Circular 4/2017:</p>
    <p class="parrafo_2">a) En la norma 1, se modifica el apartado 4, que queda redactado como sigue:</p>
    <p class="sangrado_2">\xc2\xab4. Texto nuevo para la norma 1 apartado 4.\xc2\xbb</p>
    <p class="parrafo_2">b) En la norma 2, se modifica el apartado 7, que queda redactado como sigue:</p>
    <p class="sangrado_2">\xc2\xab7. Texto nuevo para la norma 2 apartado 7.\xc2\xbb</p>

    <p class="articulo">Norma 2. Modificacion de la Circular 1/2013, de 24 de mayo.</p>
    <p class="parrafo">Se introducen las siguientes modificaciones en la Circular 1/2013:</p>
    <p class="parrafo_2">a) En la norma 3, se modifica el apartado 1, que queda redactado como sigue:</p>
    <p class="sangrado_2">\xc2\xabApartado nuevo para la Circular 1/2013 norma 3.\xc2\xbb</p>

    <p class="articulo">Disposicion transitoria primera. Aplicacion gradual.</p>
    <p class="parrafo">El texto de esta disposicion no modifica ningun target.</p>
  </texto>
</documento>
"""


def _single_target_no_sections_xml() -> bytes:
    """A classic single-target modifier without per-target articulo
    sections — the path that should still go through the Jaccard logic."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2099-0002</identificador>
    <fecha_publicacion>20990101</fecha_publicacion>
    <fecha_disposicion>20990101</fecha_disposicion>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-T-S" orden="10">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>el apartado 2 de la norma 67 de la Circular 4/2017</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p class="parrafo">En la norma 67, se modifica el apartado 2, que queda redactado como sigue:</p>
    <p class="sangrado_2">\xc2\xab2. Nuevo apartado 2 de la norma 67.\xc2\xbb</p>
  </texto>
</documento>
"""


def _no_amending_sections_xml() -> bytes:
    """A modifier whose articulo sections are all Disposición headings —
    extract_modifier_sections must return an empty list so the legacy
    Jaccard path owns the blocks."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos>
    <identificador>BOE-A-2099-0003</identificador>
    <fecha_publicacion>20990101</fecha_publicacion>
    <fecha_disposicion>20990101</fecha_disposicion>
  </metadatos>
  <analisis>
    <referencias>
      <anteriores>
        <anterior referencia="BOE-A-T-N" orden="10">
          <palabra codigo="270">MODIFICA</palabra>
          <texto>el apartado 2 de la Ley 35/2006</texto>
        </anterior>
      </anteriores>
    </referencias>
  </analisis>
  <texto>
    <p class="articulo">Disposicion final unica. Entrada en vigor.</p>
    <p class="parrafo">Esta ley entra en vigor el dia siguiente al de su publicacion.</p>
  </texto>
</documento>
"""


# ──────────────────────────────────────────────────────────
# extract_modifier_sections
# ──────────────────────────────────────────────────────────


def test_extract_sections_two_targets_plus_disposicion() -> None:
    sections = extract_modifier_sections(_multi_target_modifier_xml())
    assert len(sections) == 2
    assert sections[0].target_identifier == "Circular 4/2017"
    assert sections[1].target_identifier == "Circular 1/2013"
    # The "Disposicion transitoria" header must NOT produce a third
    # section — it closes section 2 and is then dropped.
    assert all("Disposicion" not in s.title for s in sections)


def test_extract_sections_returns_empty_when_no_amending_articulo() -> None:
    # Single-target modifier with no articulo-level split → empty list.
    assert extract_modifier_sections(_single_target_no_sections_xml()) == []
    # Modifier whose only articulo is a Disposición → empty list.
    assert extract_modifier_sections(_no_amending_sections_xml()) == []


def test_extract_sections_ranges_match_iter_body_indexing() -> None:
    xml = _multi_target_modifier_xml()
    sections = extract_modifier_sections(xml)
    items = _iter_body(_load_root(xml))

    # Each section's start index must point at a _BodyItem whose flat text
    # carries the articulo title (sanity for the downstream intro_index
    # comparison).
    for s in sections:
        start_idx = s.item_range[0]
        assert 0 <= start_idx < len(items)
        assert items[start_idx].css_class == "articulo"
        assert "Modificacion" in items[start_idx].flat


# ──────────────────────────────────────────────────────────
# match_sections_to_patches
# ──────────────────────────────────────────────────────────


def _patch(target: str, hint: str) -> AmendmentPatch:
    return AmendmentPatch(
        target_id=target,
        operation="replace",
        verb_code="270",
        verb_text="MODIFICA",
        anchor_hint=hint,
        source_boe_id="BOE-A-2099-0001",
        source_date=date(2099, 1, 1),
    )


def test_match_sections_to_patches_substring_hit() -> None:
    sections = [
        ModifierSection("Norma 1. ...", "Circular 4/2017", (0, 10)),
        ModifierSection("Norma 2. ...", "Circular 1/2013", (10, 20)),
    ]
    patches = [
        _patch("BOE-A-T-A", "determinados preceptos de la Circular 4/2017"),
        _patch("BOE-A-T-B", "la norma 3 de la Circular 1/2013"),
    ]
    # Section 0 matches patch 0; section 1 matches patch 1.
    assert match_sections_to_patches(sections, patches) == {0: 0, 1: 1}


def test_match_sections_to_patches_no_match_when_identifier_absent() -> None:
    sections = [ModifierSection("Norma 1. ...", "Circular 99/9999", (0, 10))]
    patches = [_patch("BOE-A-T", "la norma 1 de la Circular 4/2017")]
    assert match_sections_to_patches(sections, patches) == {}


def test_match_sections_to_patches_first_match_wins_on_duplicate_hint() -> None:
    sections = [ModifierSection("Norma 1. ...", "Circular 4/2017", (0, 10))]
    patches = [
        _patch("BOE-A-T-A", "la Circular 4/2017"),
        _patch("BOE-A-T-B", "la Circular 4/2017"),  # same hint
    ]
    # First patch claims the section; second stays unassigned.
    assert match_sections_to_patches(sections, patches) == {0: 0}


# ──────────────────────────────────────────────────────────
# blocks_in_section
# ──────────────────────────────────────────────────────────


def test_blocks_in_section_uses_intro_index() -> None:
    xml = _multi_target_modifier_xml()
    sections = extract_modifier_sections(xml)
    blocks = extract_new_text_blocks(xml)

    s0_blocks = blocks_in_section(blocks, sections[0])
    s1_blocks = blocks_in_section(blocks, sections[1])

    # Section 1 (Circular 4/2017) has two blocks; section 2 (Circular
    # 1/2013) has one. Disposición tail carries no quoted blocks.
    assert len(s0_blocks) == 2
    assert len(s1_blocks) == 1
    # A block index belongs to at most one section.
    assert not (set(s0_blocks) & set(s1_blocks))


# ──────────────────────────────────────────────────────────
# End-to-end: parse_amendments now splits by section
# ──────────────────────────────────────────────────────────


def test_parse_amendments_splits_multi_target_modifier_correctly() -> None:
    xml = _multi_target_modifier_xml()
    patches = parse_amendments(xml)

    # After sectioning + per-block splitting: target A's 2-block patch is
    # expanded into 2 sub-patches (one per block); target B's single block
    # stays as one patch. Total = 3.
    by_target: dict[str, list] = {}
    for p in patches:
        by_target.setdefault(p.target_id, []).append(p)
    assert set(by_target) == {"BOE-A-T-A", "BOE-A-T-B"}
    assert len(by_target["BOE-A-T-A"]) == 2  # split into sub-patches per block
    assert len(by_target["BOE-A-T-B"]) == 1

    # Target A sub-patches: one carries apartado 4, the other apartado 7.
    a_texts = [" ".join(p.new_text or ()) for p in by_target["BOE-A-T-A"]]
    a_hints = [p.anchor_hint for p in by_target["BOE-A-T-A"]]
    joined_a_all = " | ".join(a_texts + a_hints)
    assert any("apartado 4" in h for h in a_hints)  # sub-patch hint came from block intro
    assert any("apartado 7" in h for h in a_hints)
    assert any("apartado 4" in t for t in a_texts)  # new_text carries the right block
    assert any("apartado 7" in t for t in a_texts)
    assert "Circular 1/2013" not in joined_a_all  # no cross-contamination

    # Target B: single block, single patch (no split).
    p_b = by_target["BOE-A-T-B"][0]
    assert p_b.new_text is not None
    joined_b = " ".join(p_b.new_text)
    assert "Circular 1/2013 norma 3" in joined_b
    assert "apartado 4" not in joined_b  # no cross-contamination
    assert "apartado 7" not in joined_b


def test_parse_amendments_single_target_still_uses_legacy_path() -> None:
    """Regression guard: the section-split path only fires for multi-target
    modifiers with 2+ amending sections. A single-target modifier should
    behave identically to before — all blocks collapse into the one
    patch via Pass 2a."""
    xml = _single_target_no_sections_xml()
    patches = parse_amendments(xml)
    assert len(patches) == 1
    p = patches[0]
    assert p.new_text is not None
    assert "Nuevo apartado 2" in " ".join(p.new_text)
