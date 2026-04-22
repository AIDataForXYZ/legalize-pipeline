"""Stage C — reconstruct reform history for non-consolidated BOE norms.

MVP scope: Circulares BdE + CNMV. Reference: PLAN-STAGE-C.md.

This module handles the *first two* pipeline stages:

    1. parse_anteriores(xml) -> list[AmendmentPatchRaw]
       Walk <analisis>/<referencias>/<anteriores>/<anterior> of a modifying
       norm. Each <anterior> names a target BOE-ID, the verb code
       (270 MODIFICA / 407 ANADE / 235 SUPRIME / 210 DEROGA / ...),
       and a free-text anchor hint describing what is being changed.

    2. extract_new_text_blocks(xml) -> list[TextBlock]
       Walk the modifier's <texto> body and extract every consecutive
       run of quoted paragraphs («...») preceded by a redaction intro
       ("queda redactado como sigue:", "con la siguiente redaccion:",
       "Se anade ... con el siguiente texto:", ...). Each run yields a
       TextBlock with the intro sentence and the list of new paragraphs.

Together, parse_amendments() joins the two: for each raw patch whose verb
is in the MVP set, it picks the most likely TextBlock (by anchor similarity
over the intro sentence) and fills new_text. Patches that cannot be
confidently filled stay with new_text=None and confidence<0.9 — the
downstream pipeline falls back to the LLM (module 3) or to a commit-pointer
[reforma] (see PLAN-STAGE-C.md, "Politica de fallback").
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from lxml import etree

from legalize.fetcher._text import decode_utf8, scrub_control

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────
# Verb classification
# ──────────────────────────────────────────────────────────

Operation = Literal["replace", "insert", "delete"]

# BOE "codigo" attribute inside <palabra>. Stable across the corpus.
# MVP: only the four structural-modification verbs are supported.
# Corrections (201/203), judicial acts (470/552/693), affirmations
# (440 "de conformidad") and relational references (600-series) are
# out of scope for text reconstruction; they return None here and the
# caller emits a commit-pointer reform instead.
_VERB_TO_OPERATION: dict[str, Operation] = {
    "270": "replace",  # MODIFICA
    "407": "insert",  # ANADE
    "235": "delete",  # SUPRIME
    "210": "delete",  # DEROGA
}


def operation_for_verb(code: str) -> Operation | None:
    """Return the canonical operation for a BOE verb code, or None if the
    verb is out of Stage C MVP scope."""
    return _VERB_TO_OPERATION.get(code)


# ──────────────────────────────────────────────────────────
# Dataclasses
# ──────────────────────────────────────────────────────────


Extractor = Literal["regex", "llm_parse", "llm_verify_correct", "none"]
# "regex"               → fully deterministic extraction
# "llm_parse"           → LLM parse_difficult_case produced this patch from scratch
# "llm_verify_correct"  → LLM verify() said the regex patch was wrong and returned
#                         a correction. Useful to separate populations in the
#                         fidelity loop: a regression in regex vs a regression in
#                         verifier calibration have different diagnoses.


@dataclass(frozen=True)
class AmendmentPatch:
    """A structured modification of one norm by another.

    Two orthogonal confidence axes:

    - `anchor_confidence`   — how sure we are the anchor (target+position) is
      correct. High when <anterior> has exactly one matching TextBlock with a
      strong Jaccard overlap, or when there's only one patch in scope (so the
      target is unambiguous).
    - `new_text_confidence` — how sure we are the extracted new_text is the
      literal text the modifier intends. High when a «...» block was attached;
      zero when no block matched. For delete verbs this is always 1.0 (no text
      is needed).

    Keeping them separate lets the LLM stage make a tighter ask: "I have a
    solid anchor but missing new_text, find only the text" vs "parse from
    scratch". Downstream `confidence` (compound) is `min(anchor, new_text)`
    so existing callers still get the worst-axis score.
    """

    target_id: str  # BOE-A-... being modified
    operation: Operation  # replace|insert|delete
    verb_code: str  # raw <palabra codigo=>
    verb_text: str  # raw <palabra> text (for logs)
    anchor_hint: str  # <texto> of the <anterior>
    source_boe_id: str  # BOE-A-... of the modifier
    source_date: date  # fecha_disposicion of modifier
    new_text: tuple[str, ...] | None = None
    anchor_confidence: float = 0.0
    new_text_confidence: float = 0.0
    extractor: Extractor = "none"
    ordering_key: str = ""  # "orden" attr, for stable sort

    @property
    def confidence(self) -> float:
        """Compound confidence = worst of the two axes. Callers that need a
        single number (legacy code, logs) use this; routing logic should
        consult the axes directly."""
        return min(self.anchor_confidence, self.new_text_confidence)


@dataclass(frozen=True)
class TextBlock:
    """A «...»-quoted run found in the modifier body, together with its
    introducing sentence. The intro carries the anchor hint we match
    against <anterior><texto>."""

    intro: str  # the sentence that precedes the block
    paragraphs: tuple[str, ...]  # inner text of each «...» paragraph, stripped
    raw_paragraphs: tuple[str, ...]  # same but with the «» markers kept
    # body-offset hints for later anchor matching (indices over body items):
    intro_index: int
    block_start_index: int  # first quoted body item
    block_end_index: int  # last quoted body item (inclusive)


# ──────────────────────────────────────────────────────────
# <anteriores> parser (module 1)
# ──────────────────────────────────────────────────────────


def _load_root(xml_data: bytes | str) -> etree._Element:
    """Parse the BOE XML with the same hygiene Stage A uses."""
    if isinstance(xml_data, str):
        text = xml_data
    else:
        text = decode_utf8(xml_data)
    text = scrub_control(text)
    parser = etree.XMLParser(recover=True, huge_tree=True, remove_blank_text=False)
    return etree.fromstring(text.encode("utf-8"), parser=parser)


def _parse_boe_date(s: str | None) -> date | None:
    """YYYYMMDD → date. Returns None when missing/unparseable."""
    if not s:
        return None
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        return None
    try:
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    except ValueError:
        return None


def _text_of(parent: etree._Element | None, tag: str) -> str:
    if parent is None:
        return ""
    el = parent.find(tag)
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _modifier_identity(root: etree._Element) -> tuple[str, date | None]:
    """Extract the modifier's BOE-ID and disposition date from <metadatos>."""
    meta = root.find("metadatos")
    if meta is None:
        return "", None
    src_id = _text_of(meta, "identificador")
    # fecha_disposicion is the legal date; fecha_publicacion is the BOE day
    # (usually 1-2 days later). For reform chronology we want the latter
    # because that is the day the change becomes public. Fall back to
    # fecha_disposicion when missing.
    src_date = _parse_boe_date(_text_of(meta, "fecha_publicacion")) or _parse_boe_date(
        _text_of(meta, "fecha_disposicion")
    )
    return src_id, src_date


# Verb codes that Stage C has classified as out-of-scope but are known to
# appear in the BOE corpus. When we see one of these, log at DEBUG and
# carry on — they produce commit-pointers downstream. Anything NOT on this
# list and NOT in _VERB_TO_OPERATION is a "drift signal" and is counted
# separately so the fidelity loop can alert on BOE schema changes
# (e.g. new verbs introduced for EU-directive transpositions, which was
# the case at least twice since 2012).
_KNOWN_OUT_OF_SCOPE_VERBS: frozenset[str] = frozenset(
    {
        "201",  # CORRECCION de errores
        "203",  # CORRIGE errores
        "440",  # DE CONFORMIDAD con
        "470",  # DECLARA (sentencia TC)
        "552",  # Recurso promovido contra
        "693",  # DICTADA (auto TC)
        "260",  # COMPLEMENTA
        "287",  # TRANSPONE
        "630",  # COMPETENCIA
        "690",  # AUTO
        "691",  # RECURSO
        "694",  # SENTENCIA
        "695",  # PROVIDENCIA
        # leave room for more; do NOT remove entries without checking the
        # fidelity loop for regressions.
    }
)


def parse_anteriores(xml_data: bytes | str) -> list[AmendmentPatch]:
    """Extract raw AmendmentPatch rows from a modifier's <anteriores>.

    Verbs outside the MVP set (corrections, judicial acts, references) are
    skipped with a debug log. The caller can still recover them by walking
    the XML directly; we deliberately filter here to keep Stage C focused.

    Unknown verb codes (not in _VERB_TO_OPERATION and not in
    _KNOWN_OUT_OF_SCOPE_VERBS) are logged at WARNING level so the fidelity
    loop can alert on BOE schema drift. See the "drift signal" metric in
    scripts/es_fidelity_c/report.py.
    """
    root = _load_root(xml_data)
    src_id, src_date = _modifier_identity(root)

    ants = root.find(".//analisis/referencias/anteriores")
    if ants is None:
        return []

    patches: list[AmendmentPatch] = []
    for ant in ants.findall("anterior"):
        target_id = (ant.get("referencia") or "").strip()
        if not target_id.startswith("BOE-"):
            continue

        palabra = ant.find("palabra")
        verb_code = (palabra.get("codigo") if palabra is not None else "") or ""
        verb_text = (palabra.text or "").strip() if palabra is not None else ""

        op = operation_for_verb(verb_code)
        if op is None:
            if verb_code and verb_code not in _KNOWN_OUT_OF_SCOPE_VERBS:
                logger.warning(
                    "unknown BOE verb code=%s verb=%r target=%s (modifier=%s) "
                    "— possible schema drift, review _KNOWN_OUT_OF_SCOPE_VERBS",
                    verb_code,
                    verb_text,
                    target_id,
                    src_id,
                )
            else:
                logger.debug(
                    "out-of-scope verb code=%s verb=%r target=%s",
                    verb_code,
                    verb_text,
                    target_id,
                )
            continue

        anchor_hint = _text_of(ant, "texto")

        if src_date is None:
            # Every modifier we actually fetch has a date. If it is missing
            # the XML is broken and we abort the patch rather than fabricate.
            logger.warning(
                "modifier %s has no disposition date; dropping patch to %s", src_id, target_id
            )
            continue

        patches.append(
            AmendmentPatch(
                target_id=target_id,
                operation=op,
                verb_code=verb_code,
                verb_text=verb_text,
                anchor_hint=anchor_hint,
                source_boe_id=src_id,
                source_date=src_date,
                ordering_key=ant.get("orden", ""),
            )
        )

    return patches


# ──────────────────────────────────────────────────────────
# «...» extractor (module 2)
# ──────────────────────────────────────────────────────────


# Quote-delimiter normalization. BOE XML has used at least five distinct
# quote conventions across decades:
#
#   «...»  (U+00AB / U+00BB)          — post-2000 standard
#   &laquo;...&raquo;                  — XML entity form, older docs
#   "..." (U+201C / U+201D)           — when typesetter used smart quotes
#   "..." (ASCII straight)            — pre-2010 plain text drops
#   "..." (sometimes with inner „")   — Germanic-influenced typographers
#
# We normalize to «...» at the text-iteration boundary so the downstream
# regex only ever sees one shape. Without this, a silent-failure class
# kicks in: the amendment shows up in <anteriores> but the regex finds no
# «...» block and emits a commit-pointer for no good reason.
_QUOTE_NORMALIZATION: tuple[tuple[str, str], ...] = (
    ("&laquo;", "«"),
    ("&raquo;", "»"),
    ("“", "«"),  # LEFT DOUBLE QUOTATION MARK
    ("”", "»"),  # RIGHT DOUBLE QUOTATION MARK
    ("„", "«"),  # DOUBLE LOW-9 (Germanic)
    ("‟", "»"),  # DOUBLE HIGH-REVERSED-9
)


def normalize_quotes(text: str) -> str:
    """Map the various BOE quote conventions to the canonical «...» pair.

    ASCII ``"..."`` is NOT mapped: a paired-straight-quote detector needs
    state (opening vs closing) and BOE uses straight quotes legitimately
    inside e.g. code-like content. We prefer the false-negative (miss a
    rare pre-2010 Circular with straight quotes) over the false-positive
    (mistake an inline term for a modification block).
    """
    for src, dst in _QUOTE_NORMALIZATION:
        text = text.replace(src, dst)
    return text


# Paragraph classes that the BOE uses for *quoted content*. A run of
# consecutive paragraphs with any of these classes, where the first starts
# with « and the last ends with », is treated as a single TextBlock.
_QUOTED_CLASSES: frozenset[str] = frozenset(
    {
        "sangrado",
        "sangrado_2",
        "sangrado_3",
        "sangrado_articulo",
        "cita",
        "cita_con_pleca",
        "cita_ley",
        "cita_art",
    }
)

# Intro phrases. We match case-insensitively on raw text of the preceding
# paragraph. The list is deliberately conservative — a false positive would
# attribute the quoted block to the wrong patch.
_INTRO_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"queda\s+redactad[oa]s?\s+(?:como\s+sigue|en\s+los?\s+siguientes?\s+t[eé]rminos|"
        r"del\s+siguiente\s+modo|de\s+la\s+siguiente\s+forma)",
        r"con\s+la\s+siguiente\s+redacci[oó]n",
        r"pasa\s+a\s+tener\s+la\s+siguiente\s+redacci[oó]n",
        r"tendr[aá]\s+la\s+siguiente\s+redacci[oó]n",
        r"con\s+el\s+siguiente\s+(?:texto|tenor)",
        r"(?:en\s+los?\s+)?siguientes?\s+t[eé]rminos",
        r"que\s+queda\s+redactad[oa]",
        r"se\s+a[nñ]ade[^\.]{0,200}?(?:con\s+el\s+siguiente\s+texto|con\s+la\s+siguiente\s+redacci[oó]n|:\s*$)",
        r"se\s+sustituy[eo][^\.]{0,200}?por\s*:\s*$",
    )
)

# Body items: the direct children of <texto> we care about for Stage C.
# <p>           → single-paragraph item
# <blockquote>  → multi-paragraph item (BOE wraps quoted runs in blockquote
#                 starting around 2018; earlier docs use <p class='sangrado'>
#                 siblings instead). We unify both by treating a blockquote
#                 as ONE body item with multiple inner paragraphs.
# Everything else (<table>, <img>, <pre>, <ol>/<ul>) terminates any in-
# progress run — they never carry amendment text in practice.


@dataclass(frozen=True)
class _BodyItem:
    """Normalized unit of modifier body: one logical paragraph-run.

    For a plain <p>, `paragraphs` is a single-element tuple.
    For a <blockquote>, `paragraphs` is one entry per child <p>.
    `kind` is "p" or "blockquote" — the callers use this to distinguish a
    single-paragraph intro from a multi-paragraph quoted run.
    """

    kind: Literal["p", "blockquote"]
    css_class: str
    paragraphs: tuple[str, ...] = field(default_factory=tuple)

    @property
    def flat(self) -> str:
        return " ".join(self.paragraphs)


def _iter_body(root: etree._Element) -> list[_BodyItem]:
    """Return the ordered list of body items under <texto>.

    We walk *direct* children only (plus one level for blockquote). That
    keeps the index model simple and sidesteps the footgun of counting the
    same content twice under nested elements. Empty items are dropped — a
    blank paragraph carries no anchor signal and would confuse run grouping.
    """
    texto = root.find("texto")
    if texto is None:
        return []

    items: list[_BodyItem] = []
    for child in texto:
        if not isinstance(child.tag, str):
            continue
        tag = etree.QName(child.tag).localname
        css = child.get("class", "") or ""

        if tag == "p":
            flat = " ".join("".join(child.itertext()).split()).strip()
            flat = normalize_quotes(flat)
            if flat:
                items.append(_BodyItem("p", css, (flat,)))
            continue

        if tag == "blockquote":
            inner: list[str] = []
            for p in child.iter():
                if not isinstance(p.tag, str):
                    continue
                if etree.QName(p.tag).localname != "p":
                    continue
                flat = " ".join("".join(p.itertext()).split()).strip()
                flat = normalize_quotes(flat)
                if flat:
                    inner.append(flat)
            if inner:
                items.append(_BodyItem("blockquote", css, tuple(inner)))
            continue

        # Non-paragraph structure. We don't emit it, but we also don't want
        # to break run-continuity silently: BOE occasionally places a stray
        # <img> (e.g. a signature mark) between an intro and its quoted run.
        # The run-collector treats "no emission" the same as a gap, which
        # is the safe default.

    return items


def _is_quoted(item: _BodyItem) -> bool:
    """True when `item` is a quoted fragment — either by CSS class or by
    leading «.

    For <blockquote> we trust the CSS class (the enclosing tag already
    signals quotation). For <p> we require either the CSS class OR a
    leading « in the flat text.
    """
    if item.kind == "blockquote":
        return item.css_class in _QUOTED_CLASSES or any(
            p.lstrip().startswith("«") for p in item.paragraphs
        )
    if item.css_class in _QUOTED_CLASSES:
        return True
    # Fall-through: an unclassed <p> that wraps the full paragraph in «..».
    flat = item.flat
    return flat.lstrip().startswith("«")


def _is_intro(flat: str) -> bool:
    if not flat:
        return False
    if not any(p.search(flat) for p in _INTRO_PATTERNS):
        return False
    # An intro typically ends with ":" to introduce the quoted block.
    # We accept both ":" and short matches without it (some sources drop it).
    return True


def extract_new_text_blocks(xml_data: bytes | str) -> list[TextBlock]:
    """Walk the modifier body and find every (intro, «...» run) pair.

    A "run" is either a single <blockquote> (modern BOE format) or a
    sequence of consecutive <p class='sangrado*|cita*'> siblings (older
    BOE format, pre-2018). Both collapse into the same TextBlock shape.
    """
    root = _load_root(xml_data)
    items = _iter_body(root)

    blocks: list[TextBlock] = []
    i = 0
    n = len(items)
    while i < n:
        cur = items[i]

        # An intro is always a bare <p> with a redaction trigger phrase.
        # blockquotes and sangrado <p>s can't be intros — they ARE the
        # content. Requiring kind=="p" + non-quoted css prevents classifying
        # a quoted paragraph as an intro when it happens to contain one of
        # the trigger phrases verbatim.
        if cur.kind != "p" or cur.css_class in _QUOTED_CLASSES:
            i += 1
            continue
        if not _is_intro(cur.flat):
            i += 1
            continue

        # Collect the following run. Modern: one blockquote. Older: a chain
        # of quoted <p>s. We allow either.
        j = i + 1
        run_items: list[_BodyItem] = []
        while j < n and _is_quoted(items[j]):
            run_items.append(items[j])
            j += 1
            # Modern layout: one <blockquote> is enough; we stop as soon as
            # the next item is not quoted. Older layout: multiple <p> in a
            # row. The loop handles both because blockquote follow-ups are
            # rare in practice.

        if run_items:
            raw: list[str] = []
            stripped: list[str] = []
            for ri in run_items:
                for p in ri.paragraphs:
                    raw.append(p)
                    stripped.append(_strip_quote_markers(p))
            blocks.append(
                TextBlock(
                    intro=cur.flat,
                    paragraphs=tuple(stripped),
                    raw_paragraphs=tuple(raw),
                    intro_index=i,
                    block_start_index=i + 1,
                    block_end_index=j - 1,
                )
            )
            i = j
        else:
            i += 1

    return blocks


def _strip_quote_markers(s: str) -> str:
    """Remove leading « and trailing » (and optional trailing period inside),
    keeping internal angle quotes (which BOE uses legitimately for nested
    titles like «Boletin Oficial del Estado»)."""
    s = s.strip()
    if s.startswith("«"):
        s = s[1:]
    if s.endswith("»"):
        s = s[:-1]
    elif s.endswith("».") or s.endswith("»,") or s.endswith("»;"):
        s = s[:-2]
    return s.strip()


# ──────────────────────────────────────────────────────────
# Anchor-hint → TextBlock matcher
# ──────────────────────────────────────────────────────────


# Tokens inside an anchor hint that carry signal. Two families, combined:
# 1. Structural: "artículo 5", "art. 5", "apartado 2", "letra c)",
#    "párrafo tercero", "disposición adicional primera", "anexo II".
# 2. Norm identity: "Ley 37/1992", "Real Decreto 439/2007", "Circular 4/2017".
#    This is often the strongest anchor because a single modifier typically
#    edits several articles of the SAME target norm — matching on the law
#    identity disambiguates which patch owns which quoted block.

_STRUCT_RE = re.compile(
    r"(?P<kind>"
    r"art[ií]culos?|arts?\."
    r"|apartad[oa]s?|apdos?\."
    r"|letra|letras"
    r"|p[aá]rrafo|p[aá]rrafos"
    r"|disposici[oó]n(?:\s+adicional|\s+transitoria|\s+final|\s+derogatoria)?"
    r"|anexos?"
    r"|norma"
    r")"
    r"\s*(?P<ref>[\w\.\-ºª]+)",
    re.IGNORECASE,
)

# Norm identity: "Ley 35/2006" / "Real Decreto 439/2007" / "Circular 4/2017"
# The slash-numbering is the canonical BOE identifier format. We keep the
# numbering as-is; years get normalized to their trailing 4 digits.
_NORM_IDENT_RE = re.compile(
    r"\b(?P<kind>"
    r"Ley(?:\s+Org[aá]nica)?|Real\s+Decreto(?:-ley|\s+Legislativo)?|"
    r"Decreto(?:-ley)?|Orden(?:\s+Ministerial)?|Circular|Instrucci[oó]n|"
    r"Reglamento|Resoluci[oó]n"
    r")"
    r"\s+(?P<num>\d+)\s*/\s*(?P<year>\d{4})\b",
    re.IGNORECASE,
)


def _extract_signals(text: str) -> set[str]:
    """Pull anchor signals from free text as a set of lowercased tokens.

    Two token families produced:
      - "struct:{kind}:{ref}" for article/apartado/letra/... mentions
      - "norm:{kind}:{num}/{year}" for "Ley 37/1992"-style identifiers
      - "ord:{word}" for ordinal words ("primera", "segundo", ...)

    Returns an empty set when nothing usable is present; callers treat that
    as "low signal" and score via fallback.
    """
    if not text:
        return set()
    signals: set[str] = set()

    for m in _STRUCT_RE.finditer(text):
        kind = m.group("kind").lower().rstrip(".").rstrip("s")
        # "arts." -> "art", "apdos." -> "apdo", keep dot-stripped.
        # normalize "artículo"/"articulo"/"art" all to "art".
        if kind.startswith("art"):
            kind = "art"
        elif kind.startswith("apartado") or kind.startswith("apdo"):
            kind = "apartado"
        elif kind.startswith("parrafo") or kind.startswith("párrafo"):
            kind = "parrafo"
        elif kind.startswith("disposicion") or kind.startswith("disposición"):
            # keep the sub-type so "disposición adicional primera" doesn't
            # collide with "disposición final primera"
            kind = kind.replace("ó", "o").replace(" ", "_")
        elif kind.startswith("letra"):
            kind = "letra"
        elif kind.startswith("anexo"):
            kind = "anexo"
        elif kind.startswith("norma"):
            kind = "norma"
        ref = m.group("ref").lower().rstrip(".,;:)").replace("º", "").replace("ª", "")
        signals.add(f"struct:{kind}:{ref}")

    for m in _NORM_IDENT_RE.finditer(text):
        kind = re.sub(r"\s+", "_", m.group("kind").lower())
        signals.add(f"norm:{kind}:{m.group('num')}/{m.group('year')}")

    for ord_word in _ORDINAL_WORDS:
        if re.search(rf"\b{ord_word}\b", text, re.IGNORECASE):
            signals.add(f"ord:{ord_word}")

    return signals


_ORDINAL_WORDS: tuple[str, ...] = (
    "primera",
    "segunda",
    "tercera",
    "cuarta",
    "quinta",
    "sexta",
    "septima",
    "séptima",
    "octava",
    "novena",
    "decima",
    "décima",
    "undecima",
    "undécima",
    "duodecima",
    "duodécima",
    "primero",
    "segundo",
    "tercero",
    "cuarto",
    "quinto",
    "sexto",
    "septimo",
    "séptimo",
    "octavo",
    "noveno",
)


def _score_match(anchor_hint: str, block_intro: str) -> float:
    """Return a similarity score in [0,1] between an <anterior><texto> and
    a TextBlock intro. Based on overlap of structural signals, not bare
    string distance — string distance would be dominated by boilerplate."""
    a = _extract_signals(anchor_hint)
    b = _extract_signals(block_intro)
    if not a or not b:
        # Nothing to hang similarity on. Return a neutral, low score so
        # that downstream prefers a TextBlock with explicit signal overlap
        # when one exists.
        return 0.1
    inter = a & b
    if not inter:
        return 0.0
    # Jaccard on the signals.
    return len(inter) / len(a | b)


def _attach_text_blocks(
    patches: list[AmendmentPatch],
    blocks: list[TextBlock],
) -> list[AmendmentPatch]:
    """Attach TextBlocks to AmendmentPatches.

    Policy:
      - delete verbs (SUPRIME/DEROGA) never need body text; they keep
        new_text=None and get confidence=1.0 straight away.
      - When only one replace/insert patch exists in this modifier, ALL
        TextBlocks belong to it (a modifier that edits only one target
        often contains many "...apartado X queda redactado: «...»"
        stanzas — one per apartado). Paragraphs are concatenated in
        document order; the final patch confidence is 1.0.
      - When multiple replace/insert patches exist, we fall back to greedy
        Jaccard matching on anchor signals. Tie-breaker: the patch-block
        pair with the highest score wins; the remaining assignments run
        on what's left.

    Returns a fresh list; never mutates inputs.
    """
    if not patches:
        return patches

    out: list[AmendmentPatch] = list(patches)

    # Pass 1: delete patches don't consume blocks and are trivially confident
    # on both axes (anchor comes from <anterior>, new_text is not applicable).
    delete_idx: list[int] = []
    text_idx: list[int] = []
    for i, p in enumerate(patches):
        if p.operation == "delete":
            delete_idx.append(i)
        else:
            text_idx.append(i)

    for i in delete_idx:
        out[i] = _replace(
            out[i],
            anchor_confidence=1.0,
            new_text_confidence=1.0,
            extractor="regex",
        )

    if not text_idx or not blocks:
        # Text-patches without body blocks: the anchor may still be clear
        # (single patch → target unambiguous) but new_text is missing.
        if text_idx and len(text_idx) == 1 and not blocks:
            i = text_idx[0]
            out[i] = _replace(
                out[i],
                anchor_confidence=1.0,
                new_text_confidence=0.0,
                extractor="regex",
            )
        return out

    # Pass 2a: exactly one text-patch → all blocks collapse into it. Anchor
    # is trivially unambiguous (one <anterior>), new_text is strong
    # (concatenation of every quoted run in the body).
    if len(text_idx) == 1:
        i = text_idx[0]
        joined_stripped: list[str] = []
        for b in blocks:
            joined_stripped.extend(b.paragraphs)
        out[i] = _replace(
            out[i],
            new_text=tuple(joined_stripped),
            anchor_confidence=1.0,
            new_text_confidence=1.0,
            extractor="regex",
        )
        return out

    # Pass 2b: multiple text-patches → greedy Jaccard matcher. Each block
    # lands on the patch whose anchor_hint shares the most structural
    # signals with the block's intro sentence.
    scored: list[tuple[float, int, int]] = []
    for pi in text_idx:
        for bi, b in enumerate(blocks):
            s = _score_match(patches[pi].anchor_hint, b.intro)
            scored.append((s, pi, bi))
    scored.sort(reverse=True)

    # Accumulate blocks per patch in document order. A patch may collect
    # several blocks (omnibus modifier). A block is consumed at most once.
    assigned: dict[int, list[int]] = {pi: [] for pi in text_idx}
    used_blocks: set[int] = set()
    for score, pi, bi in scored:
        if bi in used_blocks:
            continue
        if score <= 0.0:
            continue
        assigned[pi].append(bi)
        used_blocks.add(bi)

    for pi, block_indices in assigned.items():
        if not block_indices:
            # Patch landed with no blocks assigned: anchor is ambiguous in
            # this multi-patch context AND we have no text. Both axes low;
            # caller will route to LLM or commit-pointer.
            out[pi] = _replace(
                out[pi],
                anchor_confidence=0.3,
                new_text_confidence=0.0,
                extractor="regex",
            )
            continue
        block_indices.sort()  # document order
        paragraphs: list[str] = []
        best_score = 0.0
        for bi in block_indices:
            paragraphs.extend(blocks[bi].paragraphs)
            best_score = max(best_score, _score_match(patches[pi].anchor_hint, blocks[bi].intro))
        # Anchor confidence tracks how tightly the patch and its block(s)
        # agree on structural signals. New_text confidence is high when we
        # did extract text; when extraction is empty it is 0.
        anchor_conf = 0.95 if best_score >= 0.95 else best_score
        new_text_conf = 0.95 if paragraphs else 0.0
        out[pi] = _replace(
            out[pi],
            new_text=tuple(paragraphs),
            anchor_confidence=anchor_conf,
            new_text_confidence=new_text_conf,
            extractor="regex",
        )

    return out


def _replace(p: AmendmentPatch, **kwargs) -> AmendmentPatch:
    # Light wrapper: frozen dataclass replace without importing dataclasses
    # at module top (keeps the imports section focused on what the module
    # exposes). Same semantics as dataclasses.replace.
    from dataclasses import replace as _r

    return _r(p, **kwargs)


# ──────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────


def parse_amendments(xml_data: bytes | str) -> list[AmendmentPatch]:
    """End-to-end parse of a modifier XML.

    Returns every MVP-scoped patch (MODIFICA/ANADE/SUPRIME/DEROGA) with
    new_text attached when the body «...» extractor found a confident match.
    Patches with new_text=None and/or confidence<0.9 are the input for the
    LLM fallback (module 3). Callers MUST sort by (source_date, ordering_key)
    before applying; this module preserves the XML document order, which is
    not a chronological guarantee when a single modifier edits multiple
    targets interleaved.
    """
    patches = parse_anteriores(xml_data)
    if not patches:
        return patches
    blocks = extract_new_text_blocks(xml_data)
    return _attach_text_blocks(patches, blocks)
