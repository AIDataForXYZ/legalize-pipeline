"""Structural splitter for BOE modifier bodies.

The existing amendment parser treats the modifier body as a flat list of
``(intro, «...»-block)`` pairs, then leans on Jaccard similarity between
each patch's ``anchor_hint`` and every block's intro to decide which
block belongs to which patch. That fails on multi-target modifiers
("omnibus" BOE dispositions that edit Circular X AND Circular Y in the
same document): the hints name the TARGET CIRCULAR but the intros name
the internal element ("En la norma 4..."), so no structural tokens
overlap and Jaccard hits zero.

Observation from 16 multi-target modifiers in the live cache
(``/tmp/stage-c-live/modifiers``): the BOE writers always group per
target under a top-level ``<p class="articulo">`` whose title reads::

    Norma 1. Modificación de la Circular 4/2017, de 27 de noviembre, ...

Everything under that heading (up to the next ``articulo``) amends the
named target. This module turns that observation into a parser: it
walks ``<texto>`` with the same index model ``_iter_body`` uses — so the
resulting ``item_range`` values are directly comparable to
``TextBlock.intro_index`` — and returns one ``ModifierSection`` per
section that amends an external target. Disposición transitoria /
final sections are skipped: those introduce new provisions in the
modifier itself, not amendments to the targets.

Downstream ``_attach_text_blocks`` consumes the sections to restrict
block-to-patch assignment to a single target, reducing the multi-target
case to a sequence of single-target cases — which the existing Pass
2a code already handles cleanly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lxml import etree

if TYPE_CHECKING:
    from legalize.fetcher.es.amendments import TextBlock


# ──────────────────────────────────────────────────────────
# Patterns
# ──────────────────────────────────────────────────────────


# "Norma 1. Modificación de la Circular 4/2017, de 27 de noviembre, ..."
# "Artículo 1. Modificación de la Ley 35/2006, del IRPF."
# "Norma primera. Modificación de la Circular 1/2013..."
# "Norma 3. Modificación del Real Decreto 439/2007..."
#
# We accept both "de la" and "del" (for Real Decreto, Reglamento). Name
# kinds are kept to the forms that appear in the BOE corpus — adding a
# new kind here is cheap but should be evidence-driven (i.e. do not add
# "Resolución" unless we actually see it).
_TARGET_KIND = (
    r"Circular"
    r"|Ley(?:\s+Org[aá]nica)?"
    r"|Real\s+Decreto(?:\s+Legislativo|\s+Ley)?"
    r"|Reglamento"
    r"|Orden(?:\s+Ministerial)?"
    r"|Decreto"
)

_SECTION_TITLE_RE = re.compile(
    r"(?:Norma|Art[ií]culo)\s+\S+\.\s+"
    r"(?:Modificaci[oó]n|Derogaci[oó]n|Correcci[oó]n)(?:\s+de\s+errores)?"
    r"\s+(?:de\s+la|del|de)\s+"
    r"(?P<kind>" + _TARGET_KIND + r")\s+"
    r"(?P<ref>[\w./\-]+)",
    re.IGNORECASE,
)

# Short-circuit: a section title that *opens* a Disposición is NEVER
# amending an external target. It introduces new text the modifier itself
# is adding to the legal system. Skipping these prevents false sections.
_SKIP_TITLE_RE = re.compile(
    r"^(Disposici[oó]n|T[ií]tulo|Cap[ií]tulo|Secci[oó]n|Preámbulo)\b",
    re.IGNORECASE,
)


# ──────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ModifierSection:
    """One top-level division of a modifier body that amends a single
    external target.

    ``item_range`` indexes into the list returned by
    ``amendments._iter_body`` — the same indexing ``TextBlock.intro_index``
    uses. A block ``b`` belongs to this section iff
    ``item_range[0] <= b.intro_index < item_range[1]``.

    ``target_identifier`` is the raw "kind + ref" substring as it appears
    in the title (e.g. ``"Circular 4/2017"`` or ``"Ley 35/2006"``).
    Callers match it against ``AmendmentPatch.anchor_hint`` via a
    case-insensitive substring search. The raw form is what the
    anchor_hint carries verbatim, so we don't try to canonicalize.
    """

    title: str
    target_identifier: str
    item_range: tuple[int, int]


# ──────────────────────────────────────────────────────────
# Public entry points
# ──────────────────────────────────────────────────────────


def extract_modifier_sections(xml_data: bytes | str | etree._Element) -> list[ModifierSection]:
    """Return the ordered list of target-bound sections inside ``<texto>``.

    Sections are delimited by ``<p class="articulo">`` elements whose text
    matches the "X. Modificación de la Y Z/NNNN, ..." pattern. Each
    section's ``item_range`` uses the same indexing as
    ``amendments._iter_body`` so it is directly comparable with
    ``TextBlock.intro_index``.

    Modifiers without a recognisable per-target articulo structure
    (e.g. a simple "Se modifica la Ley X." intro followed by a blockquote)
    return an empty list — callers fall through to the legacy Jaccard path.
    """
    # Deferred import to avoid a circular dependency: amendments imports
    # this module during its own parsing pass (see parse_amendments).
    from legalize.fetcher.es.amendments import _iter_body, _load_root

    if isinstance(xml_data, etree._Element):
        root = xml_data
    else:
        root = _load_root(xml_data)

    items = _iter_body(root)
    if not items:
        return []

    sections: list[ModifierSection] = []
    pending: tuple[str, str, int] | None = None  # (title, target, start_idx)

    for idx, item in enumerate(items):
        if not _is_section_header(item):
            continue
        target = _extract_target_identifier(item.flat)
        # Close the pending section at this boundary regardless of whether
        # THIS item opens a new amending section. A "Disposición transitoria"
        # heading is not an amendment, but it still terminates the previous
        # section (the amendment ends there).
        if pending is not None:
            title, prev_target, start_idx = pending
            sections.append(
                ModifierSection(
                    title=title,
                    target_identifier=prev_target,
                    item_range=(start_idx, idx),
                )
            )
            pending = None

        if target is None:
            continue  # non-amending heading (Disposición, etc.) — skip

        pending = (item.flat, target, idx)

    if pending is not None:
        title, target, start_idx = pending
        sections.append(
            ModifierSection(
                title=title,
                target_identifier=target,
                item_range=(start_idx, len(items)),
            )
        )

    return sections


def match_sections_to_patches(
    sections: list[ModifierSection],
    patches: list,  # list[AmendmentPatch] — typed loosely to avoid circular import
) -> dict[int, int]:
    """Bind each section to at most one patch by substring-matching the
    section's ``target_identifier`` against every patch's ``anchor_hint``.

    Returns ``{section_index: patch_index}``. A section with no match is
    absent from the dict — the caller keeps its blocks unassigned and
    lets the legacy Jaccard path (or LLM tier) handle them.

    Ambiguity policy: when multiple patches carry the same identifier
    (rare — not observed in the live cache), the first one in document
    order wins. We could tighten this with the section's ordinal ("Norma
    1" vs "Norma 2") but the added complexity has no evidence in the
    cached corpus yet; keep it simple until a counter-example appears.
    """
    out: dict[int, int] = {}
    used_patches: set[int] = set()
    for si, section in enumerate(sections):
        ident = _normalize_identifier(section.target_identifier)
        if not ident:
            continue
        for pi, patch in enumerate(patches):
            if pi in used_patches:
                continue
            hint_norm = _normalize_identifier(getattr(patch, "anchor_hint", "") or "")
            if ident in hint_norm:
                out[si] = pi
                used_patches.add(pi)
                break
    return out


def blocks_in_section(blocks: list["TextBlock"], section: ModifierSection) -> list[int]:
    """Indices into ``blocks`` whose ``intro_index`` falls inside the
    section's ``item_range``. Returns the list in document order."""
    lo, hi = section.item_range
    return [bi for bi, b in enumerate(blocks) if lo <= b.intro_index < hi]


# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────


def _is_section_header(item) -> bool:
    """True when ``item`` is a top-level ``<p class="articulo">`` whose
    title carries the "X. <kind> de la Y Z/NNNN" shape.

    We DON'T reject Disposición headings here — the caller uses them to
    close a previous section, they just don't open a new one. Rejection
    happens via ``_extract_target_identifier`` returning None.
    """
    if getattr(item, "kind", "") != "p":
        return False
    if getattr(item, "css_class", "") != "articulo":
        return False
    flat = getattr(item, "flat", "") or ""
    if not flat:
        return False
    # Fast reject: a Disposición/Título heading is never a new amending
    # section. We still return True so the caller closes the prior one.
    if _SKIP_TITLE_RE.match(flat):
        return True
    return bool(_SECTION_TITLE_RE.search(flat))


def _extract_target_identifier(title: str) -> str | None:
    """Return ``"<kind> <ref>"`` for an amending section title, or None.

    The returned string is the raw BOE form ("Circular 4/2017",
    "Ley 35/2006") — NOT canonicalized — because that is what the
    corresponding ``AmendmentPatch.anchor_hint`` embeds verbatim. A
    canonicalization step here would only make the substring match
    harder.
    """
    if not title or _SKIP_TITLE_RE.match(title):
        return None
    m = _SECTION_TITLE_RE.search(title)
    if not m:
        return None
    kind = " ".join(m.group("kind").split())  # collapse whitespace in compound kinds
    ref = m.group("ref")
    return f"{kind} {ref}"


def _normalize_identifier(s: str) -> str:
    """Lowercase + whitespace-collapse for substring comparison.

    Does NOT strip punctuation: "4/2017" and "4-2017" stay distinct on
    purpose, since BOE uses "/" exclusively for norm numbering and a
    mismatch here is signal (probably a typo worth flagging)."""
    return " ".join(s.lower().split())
