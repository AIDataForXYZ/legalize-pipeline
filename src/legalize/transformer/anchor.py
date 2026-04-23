"""Anchor resolution over Stage-A-produced Markdown (PLAN-STAGE-C.md §W3).

An Anchor is a structural location within a norm: "the letter c) of
apartado 2 of article 5", or "disposición adicional primera". An
AmendmentPatch's anchor_hint is the raw free-text description from
BOE's <anterior><texto>; here we parse it into structural fields and
walk the base Markdown tree to find the exact byte range the patch
should operate on.

Two public entry points:

    parse_anchor_from_hint(hint) -> Anchor
        Turns "art. 5.2.c) de la Ley 37/1992" into
        Anchor(articulo="5", apartado="2", letra="c", ...).
        Pure string parsing — no side effects, no filesystem.

    resolve_anchor(markdown, anchor) -> Position | None
        Walks the Stage A Markdown heading hierarchy (# libro / ##
        título|anexo|disposición / ### capítulo / #### sección /
        ##### subsección / ###### artículo) plus the in-body apartado /
        letra numbering (1. / 2. / Uno. / Dos. / a) / b)) to find the
        exact line range matching the anchor. Returns None when:
         - the anchor is too under-specified to locate a unique region, OR
         - the target heading does not exist in the base (e.g. a patch
           that adds an artículo 5 bis to a norm that doesn't have one
           yet — that's an insert case; the caller requests the PARENT
           anchor instead).
        The patcher calls it and, on None, falls back to commit-pointer.

Design constraints honoured here:

  - NEVER invent text. We return byte ranges pointing at existing
    content; the patcher substitutes/deletes that exact slice.
  - Two matches is worse than zero: ambiguous resolution returns None.
    Better to emit a commit-pointer than to patch the wrong article.
  - No LLM. The LLM pipeline already resolved anchors upstream; if it
    couldn't, the patch never reaches the anchor resolver at all.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────
# Anchor dataclass + hint parser
# ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Anchor:
    """Structural location within a Stage-A Markdown norm.

    Fields are deliberately optional. The resolver walks from coarsest
    (libro) to finest (párrafo), using whatever fields are set. More
    fields = more specific match; fewer fields = broader region.

    Two special compound fields:
     - `disposicion`: e.g. ``"adicional primera"``, ``"final tercera"``.
       BOE encodes these as a single stretch "Disposición adicional primera"
       so we keep them compound rather than splitting into type + ordinal.
     - `norma`: used by Circulares del Banco de España whose top-level
       structural unit is ``Norma 1.`` rather than ``Artículo 1.``.
    """

    libro: str | None = None
    parte: str | None = None
    titulo: str | None = None
    capitulo: str | None = None
    seccion: str | None = None
    subseccion: str | None = None
    articulo: str | None = None  # "5" / "5 bis" / "5.º"
    norma: str | None = None  # Circulares BdE
    disposicion: str | None = None  # "adicional primera", "final tercera"
    anexo: str | None = None  # "I" / "1"
    apartado: str | None = None  # "1" / "Uno" / "primero"
    letra: str | None = None  # "a"
    parrafo: str | None = None  # "primero" / "1"

    @property
    def is_empty(self) -> bool:
        return all(getattr(self, f.name) is None for f in self.__dataclass_fields__.values())


# Precompiled patterns for hint parsing. Capture the numeric/ordinal ref
# separately so we can normalise it.

# Articulo: "art. 5" "artículo 5.º" "arts. 9 y 10" "artículo 5 bis"
#
# Accepts both the long form ("artículo" / "articulos") and the short
# abbreviated form ("art." / "arts."). BOE uses both interchangeably;
# a hint like "un art. 61 bis a la Ley 35/2006" must parse.
_RE_ARTICULO = re.compile(
    r"(?:art[ií]culos?|arts?\.)\s+(?P<ref>\d+\s*(?:bis|ter|quater)?(?:\.[º°ª]?)?)",
    re.IGNORECASE,
)

# Norma (Circulares BdE): "norma 67" "Norma 3ª" "normas 1 a 3"
#
# Plural form ``normas`` is deliberately included because BOE modifier
# hints frequently enumerate several ("las normas 1, 2 y 5..."). Before
# this addition, "normas 1" was skipped and the hint fell back to the
# bucket-A no-struct path with a 0.3 confidence cap — 9 live-run
# patches were lost to this single-character omission.
_RE_NORMA = re.compile(r"\bnormas?\s+(?P<ref>\d+\s*(?:bis)?)", re.IGNORECASE)

# Apartado: "apartado 2" "apartado uno" "apartado primero" "apdo. 3"
_RE_APARTADO = re.compile(
    r"(?:apartad[oa]s?|apdos?\.?)\s+(?P<ref>[\wÁÉÍÓÚáéíóúñ\-]+)",
    re.IGNORECASE,
)

# Letra: "letra c)" "letras a) y b)" "la letra c"
_RE_LETRA = re.compile(r"letras?\s+(?P<ref>[a-z])\)?", re.IGNORECASE)

# Párrafo: "párrafo primero" "párrafo 3"
_RE_PARRAFO = re.compile(r"p[aá]rrafos?\s+(?P<ref>[\wÁÉÍÓÚáéíóúñ\-]+)", re.IGNORECASE)

# Disposición: must capture type + ordinal. "Disposición adicional primera"
_RE_DISPOSICION = re.compile(
    r"disposici[oó]n\s+(?P<type>adicional|transitoria|final|derogatoria)"
    r"(?:\s+(?P<ordinal>[\wÁÉÍÓÚáéíóúñ\-]+))?",
    re.IGNORECASE,
)

# Anexo: "anexo I" "anexo 1" "anexo III"
_RE_ANEXO = re.compile(r"\banexos?\s+(?P<ref>[IVXLCDM\d]+)", re.IGNORECASE)

# Libro / título / capítulo / sección / subsección with roman or arabic ref.
_RE_LIBRO = re.compile(r"\blibro\s+(?P<ref>[IVXLCDM\d]+)", re.IGNORECASE)
_RE_TITULO = re.compile(r"\bt[ií]tulo\s+(?P<ref>[IVXLCDM\d]+)", re.IGNORECASE)
_RE_CAPITULO = re.compile(
    r"\bcap[ií]tulo\s+(?P<ref>[IVXLCDM\d]+|PRIMERO|SEGUNDO|TERCERO|CUARTO|QUINTO|SEXTO|S[EÉ]PTIMO|OCTAVO|NOVENO|D[EÉ]CIMO)",
    re.IGNORECASE,
)
_RE_SECCION = re.compile(r"\bsecci[oó]n\s+(?P<ref>\d+[ªa]?|[IVXLCDM]+)", re.IGNORECASE)


def _normalize_ref(ref: str) -> str:
    """Strip ordinal markers and whitespace: '5.º' -> '5', '1ª' -> '1'.

    Order matters: remove the ordinal characters before re-stripping
    punctuation. Otherwise '5.º' becomes '5.' (rstrip finds '.', then
    º removal doesn't re-trigger rstrip).
    """
    cleaned = ref.strip().replace("º", "").replace("°", "").replace("ª", "")
    return cleaned.strip().rstrip(".").strip()


def parse_anchor_from_hint(hint: str) -> Anchor:
    """Extract structural fields from a free-text anchor description.

    Rules:
      - Only the FIRST match of each kind is kept. "arts. 9 y 10" picks
        up "9"; the caller can split hints with " y " / " ; " beforehand
        if they need each reference separately.
      - Matches are case-insensitive; refs are normalised (5.º → 5,
        1ª → 1).
      - Disposición is kept compound ("adicional primera") so the
        resolver can match the full heading line verbatim.
    """
    if not hint:
        return Anchor()

    libro = _take(_RE_LIBRO, hint)
    parte = None  # rare; not parsed in MVP
    titulo = _take(_RE_TITULO, hint)
    capitulo = _take(_RE_CAPITULO, hint)
    seccion = _take(_RE_SECCION, hint)
    articulo = _take(_RE_ARTICULO, hint)
    norma = _take(_RE_NORMA, hint)
    apartado = _take(_RE_APARTADO, hint)
    letra = _take(_RE_LETRA, hint)
    parrafo = _take(_RE_PARRAFO, hint)
    anexo = _take(_RE_ANEXO, hint)

    disposicion: str | None = None
    m = _RE_DISPOSICION.search(hint)
    if m:
        t = m.group("type").lower()
        ordinal = m.group("ordinal")
        disposicion = f"{t} {ordinal.lower()}" if ordinal else t

    return Anchor(
        libro=libro,
        parte=parte,
        titulo=titulo,
        capitulo=capitulo,
        seccion=seccion,
        subseccion=None,
        articulo=articulo,
        norma=norma,
        disposicion=disposicion,
        anexo=anexo,
        apartado=apartado,
        letra=letra.lower() if letra else None,
        parrafo=parrafo,
    )


def _take(pattern: re.Pattern[str], text: str) -> str | None:
    """First capture group, normalised. None when no match."""
    m = pattern.search(text)
    if not m:
        return None
    return _normalize_ref(m.group("ref"))


# ──────────────────────────────────────────────────────────
# Position dataclass + markdown tree walk
# ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Position:
    """A contiguous range inside a Markdown document.

    Ranges are half-open over lines (line_start inclusive, line_end
    exclusive), mirroring Python slice semantics. `content` is the
    exact substring the patcher will replace/delete — we carry it so
    the caller can run its own literal-presence check before writing.
    """

    line_start: int
    line_end: int  # exclusive
    content: str  # the slice the patcher acts on
    kind: str  # "articulo" | "norma" | "disposicion" | "anexo"
    # | "apartado" | "letra" | "parrafo"

    def with_content(self, new_content: str) -> "Position":
        from dataclasses import replace

        return replace(self, content=new_content)


# Heading levels in Stage A output. Matches markdown.py's CSS map.
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


@dataclass(frozen=True)
class _HeadingNode:
    """One heading in the Markdown's outline. Lines cover the heading
    itself plus its body up to (but not including) the next heading of
    equal-or-higher level."""

    level: int  # 1..6
    text: str  # the heading text (stripped of '#'s)
    line_no: int  # 0-indexed; the '# …' line itself
    body_start: int  # line after the heading
    body_end: int  # exclusive; next equal-or-higher heading or EOF

    @property
    def content_start(self) -> int:
        return self.line_no

    @property
    def content_end(self) -> int:
        return self.body_end


def _parse_heading_outline(lines: list[str]) -> list[_HeadingNode]:
    """Emit a flat list of heading nodes. The outline tree is implicit
    via (level, order); callers walk it with level-based stacking."""
    raw: list[tuple[int, int, str]] = []
    for i, line in enumerate(lines):
        m = _HEADING_RE.match(line)
        if not m:
            continue
        level = len(m.group(1))
        raw.append((level, i, m.group(2).strip()))

    # Compute each heading's body_end: the next heading of <= level, or EOF.
    n = len(lines)
    out: list[_HeadingNode] = []
    for idx, (level, line_no, text) in enumerate(raw):
        end = n
        for next_level, next_line, _ in raw[idx + 1 :]:
            if next_level <= level:
                end = next_line
                break
        out.append(
            _HeadingNode(
                level=level,
                text=text,
                line_no=line_no,
                body_start=line_no + 1,
                body_end=end,
            )
        )
    return out


# ──────────────────────────────────────────────────────────
# Heading matchers
# ──────────────────────────────────────────────────────────


# CSS class markers that Stage A/B renderer prepends to headings:
#   "[precepto]Norma 14.ª …"  "[capítulo_num]CAPÍTULO PRIMERO"  etc.
# These are an artefact of the Markdown renderer and MUST NOT prevent
# anchor resolution. We strip them before running the per-kind matcher.
_CSS_MARKER_RE = re.compile(r"^\s*\[[\wÁÉÍÓÚáéíóúñ_\-]+\]\s*")


def _strip_css_marker(heading_text: str) -> str:
    """Remove the leading [css_class] tag the renderer prepends, if any.

    Leaves the rest of the heading untouched (case, punctuation, inner
    marks like º/ª). Callers should pass the already-lowercased or
    canonical-ised text to their regex matchers."""
    return _CSS_MARKER_RE.sub("", heading_text)


def _canonical_num(token: str) -> str:
    """Normalise a heading number / hint ref to a common form so
    "decimosexta" and "16" compare equal.

    - strips ordinal markers (º/°/ª) and trailing punctuation,
    - maps Spanish ordinal / cardinal words to their digit form,
    - lowercases the result.

    Roman numerals are left as-is because Anexos routinely use them
    ("Anexo I", "Anexo III") and converting would introduce collisions.
    """
    s = _normalize_ref(token).lower()
    return _ORDINAL_WORDS_CARDINAL.get(s, s)


def _match_articulo(heading_text: str, ref: str) -> bool:
    """True when the heading text is 'Artículo N' (optionally with title)."""
    text = _strip_css_marker(heading_text)
    m = re.match(
        r"^art[ií]culo\s+(?P<n>\S+(?:\s*(?:bis|ter|quater))?)",
        text,
        re.IGNORECASE,
    )
    if not m:
        return False
    return _canonical_num(m.group("n")) == _canonical_num(ref)


def _match_norma(heading_text: str, ref: str) -> bool:
    """True when heading is 'Norma N' / 'Norma primera' for the requested
    ref. Normalises ordinal words ↔ digits — Circulares del Banco de
    España render Normas as "Norma decimosexta" while modifier hints
    use "norma 16". Also tolerates the leading [css_class] marker the
    renderer prepends to some headings."""
    text = _strip_css_marker(heading_text)
    m = re.match(r"^norma\s+(?P<n>\S+(?:\s*bis)?)", text, re.IGNORECASE)
    if not m:
        return False
    return _canonical_num(m.group("n")) == _canonical_num(ref)


def _match_disposicion(heading_text: str, ref: str) -> bool:
    """True when heading is a disposición matching the anchor's type+ordinal.

    `ref` is the compound string from parse_anchor_from_hint:
      "adicional primera" / "final tercera" / "transitoria segunda"

    We canonicalise the ordinal part so "1" in the hint matches "primera"
    in the heading (and vice-versa). "única" is its own ordinal and does
    NOT map to a number — callers that want to target a "disposición
    adicional única" must use that word literally in the hint.
    """
    h = _strip_css_marker(heading_text).lower()
    if "disposici" not in h:
        return False
    parts = ref.lower().split(maxsplit=1)
    if len(parts) < 2:
        # Bare type ("adicional") with no ordinal: substring match is fine.
        return parts[0] in h
    d_type, ord_part = parts[0], parts[1]
    if d_type not in h:
        return False
    # Extract the ordinal token from the heading's own "disposición
    # {type} {ordinal}" shape and compare canonically.
    m = re.search(
        rf"disposici[oó]n\s+{d_type}\s+(?P<ord>\S+)",
        h,
        re.IGNORECASE,
    )
    if not m:
        return False
    return _canonical_num(m.group("ord").rstrip(".,;:)")) == _canonical_num(ord_part)


def _match_anexo(heading_text: str, ref: str) -> bool:
    text = _strip_css_marker(heading_text)
    m = re.match(r"^anexo\s+(?P<n>[IVXLCDM\d]+)", text, re.IGNORECASE)
    if not m:
        return False
    return _canonical_num(m.group("n")) == _canonical_num(ref)


# ──────────────────────────────────────────────────────────
# In-body apartado / letra matching
# ──────────────────────────────────────────────────────────


# Apartado leaders at start of paragraph:
#   "1. " "2. " ... (cardinal)
#   "Uno. " "Dos. " ... (ordinal word)
#   "Primero. " (ordinal)
#   "I. " "II. " (roman, less common)
_ORDINAL_WORDS_CARDINAL: dict[str, str] = {
    # Cardinal words (used in "Apartado uno", "Apartado dos", …)
    "uno": "1",
    "dos": "2",
    "tres": "3",
    "cuatro": "4",
    "cinco": "5",
    "seis": "6",
    "siete": "7",
    "ocho": "8",
    "nueve": "9",
    "diez": "10",
    "once": "11",
    "doce": "12",
    "trece": "13",
    "catorce": "14",
    "quince": "15",
    "dieciseis": "16",
    "dieciséis": "16",
    "diecisiete": "17",
    "dieciocho": "18",
    "diecinueve": "19",
    "veinte": "20",
    # Masculine ordinals ("apartado primero", "párrafo segundo", …)
    "primero": "1",
    "segundo": "2",
    "tercero": "3",
    "cuarto": "4",
    "quinto": "5",
    "sexto": "6",
    "septimo": "7",
    "séptimo": "7",
    "octavo": "8",
    "noveno": "9",
    "decimo": "10",
    "décimo": "10",
    # Feminine ordinals. Circulares del Banco de España render their
    # top-level structural units as "Norma primera", "Norma segunda",
    # …, "Norma decimosexta". Modifier hints on the other hand use
    # cardinal numbers: "la norma 16 de la Circular 1/2013". Without
    # this mapping the resolver compared "Norma decimosexta" heading
    # text against "16" and returned None. Observed: ~14 live-run
    # patches in bucket F were lost to this single omission.
    "primera": "1",
    "segunda": "2",
    "tercera": "3",
    "cuarta": "4",
    "quinta": "5",
    "sexta": "6",
    "septima": "7",
    "séptima": "7",
    "octava": "8",
    "novena": "9",
    "decima": "10",
    "décima": "10",
    "undecima": "11",
    "undécima": "11",
    "duodecima": "12",
    "duodécima": "12",
    "decimotercera": "13",
    "decimocuarta": "14",
    "decimoquinta": "15",
    "decimosexta": "16",
    "decimoseptima": "17",
    "decimoséptima": "17",
    "decimoctava": "18",
    "decimonovena": "19",
    "vigesima": "20",
    "vigésima": "20",
    "vigesimoprimera": "21",
    "vigesimosegunda": "22",
    "vigesimotercera": "23",
    "vigesimocuarta": "24",
    "vigesimoquinta": "25",
    "vigesimosexta": "26",
    "vigesimoseptima": "27",
    "vigesimoséptima": "27",
    "vigesimoctava": "28",
    "vigesimonovena": "29",
    "trigesima": "30",
    "trigésima": "30",
    "trigesimoprimera": "31",
    "trigesimosegunda": "32",
    "trigesimotercera": "33",
    "trigesimocuarta": "34",
    "trigesimoquinta": "35",
    "cuadragesima": "40",
    "cuadragésima": "40",
    "quincuagesima": "50",
    "quincuagésima": "50",
    "sexagesima": "60",
    "sexagésima": "60",
    "septuagesima": "70",
    "septuagésima": "70",
    "octogesima": "80",
    "octogésima": "80",
    "nonagesima": "90",
    "nonagésima": "90",
    "centesima": "100",
    "centésima": "100",
    # Masculine equivalents for non-norma contexts ("Capítulo
    # decimoctavo", etc. — rare but observed on some legacy Leyes).
    "undecimo": "11",
    "undécimo": "11",
    "duodecimo": "12",
    "duodécimo": "12",
    "decimotercero": "13",
    "decimocuarto": "14",
    "decimoquinto": "15",
    "decimosexto": "16",
    "decimoseptimo": "17",
    "decimoséptimo": "17",
    "decimoctavo": "18",
    "decimonoveno": "19",
    "vigesimo": "20",
    "vigésimo": "20",
    # "única" / "único" collapse to their own literal token. Disposiciones
    # únicas exist as a bare ordinal; we keep them as text so the
    # structured matcher compares hint↔heading literally instead of
    # mapping to a digit (there is no numeric equivalent).
    "única": "unica",
    "unica": "unica",
    "único": "unico",
    "unico": "unico",
}

_RE_APARTADO_LEADER = re.compile(
    r"^\s*"
    r"(?:"
    r"(?P<num>\d+)\.\s"  # "1. "
    r"|(?P<word>[\wÁÉÍÓÚáéíóúñ]+)\.\s"  # "Uno. " "Primero. "
    r")"
)

_RE_LETRA_LEADER = re.compile(r"^\s*(?P<ch>[a-z])\)\s", re.IGNORECASE)


def _paragraph_leader_apartado(line: str) -> str | None:
    """Return the normalised apartado number a line starts with, or None.

    "1. blah"          → "1"
    "Uno. blah"        → "1"  (mapped via _ORDINAL_WORDS_CARDINAL)
    "Primero. blah"    → "1"
    "blah"             → None
    """
    m = _RE_APARTADO_LEADER.match(line)
    if not m:
        return None
    num = m.group("num")
    if num is not None:
        return num
    word = (m.group("word") or "").lower()
    # Only accept words we recognise — avoids matching random "Este." or
    # "España." as if they were apartado leaders.
    return _ORDINAL_WORDS_CARDINAL.get(word)


def _paragraph_leader_letra(line: str) -> str | None:
    m = _RE_LETRA_LEADER.match(line)
    return m.group("ch").lower() if m else None


def _find_apartado_range(
    lines: list[str],
    body_start: int,
    body_end: int,
    apartado_ref: str,
) -> tuple[int, int] | None:
    """Locate the apartado block with normalised number == apartado_ref
    inside [body_start, body_end). Returns (start_line, end_line) or None.

    End_line is the line right before the next apartado leader (or body_end).
    """
    apartado_ref = apartado_ref.lower()
    # Normalise words-as-apartados ("primero") to digits upfront.
    apartado_ref = _ORDINAL_WORDS_CARDINAL.get(apartado_ref, apartado_ref)

    starts: list[tuple[int, str]] = []
    for i in range(body_start, body_end):
        lead = _paragraph_leader_apartado(lines[i])
        if lead is not None:
            starts.append((i, lead))

    for idx, (line_no, num) in enumerate(starts):
        if num == apartado_ref:
            # End is next apartado leader, or body_end.
            end = body_end
            if idx + 1 < len(starts):
                end = starts[idx + 1][0]
            return line_no, end
    return None


def _find_letra_range(
    lines: list[str],
    start: int,
    end: int,
    letra_ref: str,
) -> tuple[int, int] | None:
    """Locate the "x) ..." line inside [start, end). Returns its line range.

    The letra block ends at the next letra leader OR at the next apartado
    leader OR at `end`, whichever comes first.
    """
    letra_ref = letra_ref.lower()
    starts: list[tuple[int, str]] = []
    for i in range(start, end):
        lead = _paragraph_leader_letra(lines[i])
        if lead is not None:
            starts.append((i, lead))

    for idx, (line_no, ch) in enumerate(starts):
        if ch == letra_ref:
            stop = end
            if idx + 1 < len(starts):
                stop = starts[idx + 1][0]
            # Also cap at the next apartado, if any.
            for j in range(line_no + 1, stop):
                if _paragraph_leader_apartado(lines[j]) is not None:
                    stop = j
                    break
            return line_no, stop
    return None


# ──────────────────────────────────────────────────────────
# resolve_anchor — the public entry point
# ──────────────────────────────────────────────────────────


def resolve_anchor(markdown: str, anchor: Anchor) -> Position | None:
    """Locate `anchor` inside `markdown`. Returns the tightest Position
    we can match, or None when the anchor cannot be uniquely resolved.

    Resolution rules:

      - Find the top-level section (artículo / norma / disposición /
        anexo). If none is set, we cannot resolve — return None.
      - Find exactly one matching heading. Zero matches → None.
        Two or more matches → None (ambiguity is worse than missing).
      - If apartado is set, narrow to that apartado range.
      - If letra is set, narrow further to the letra's line range.
      - If nothing beyond the top-level section is set, return the
        whole section range.

    The `kind` field on the returned Position is the finest unit we
    actually narrowed to.
    """
    if anchor.is_empty:
        return None

    lines = markdown.splitlines()
    outline = _parse_heading_outline(lines)

    # Find top-level section. We try in the order: articulo, norma,
    # disposicion, anexo. Only ONE top-level should be set per anchor.
    section: _HeadingNode | None = None
    kind = "unknown"
    if anchor.articulo:
        section, kind = (
            _find_unique(outline, lambda h: _match_articulo(h.text, anchor.articulo)),
            "articulo",
        )
    elif anchor.norma:
        section, kind = _find_unique(outline, lambda h: _match_norma(h.text, anchor.norma)), "norma"
    elif anchor.disposicion:
        section, kind = (
            _find_unique(outline, lambda h: _match_disposicion(h.text, anchor.disposicion)),
            "disposicion",
        )
    elif anchor.anexo:
        section, kind = _find_unique(outline, lambda h: _match_anexo(h.text, anchor.anexo)), "anexo"

    if section is None:
        return None

    # If no finer anchor, return the whole section.
    if not anchor.apartado and not anchor.letra and not anchor.parrafo:
        return _position_for_lines(lines, section.content_start, section.content_end, kind)

    # Narrow to apartado.
    span: tuple[int, int] = (section.body_start, section.body_end)
    if anchor.apartado:
        rng = _find_apartado_range(lines, section.body_start, section.body_end, anchor.apartado)
        if rng is None:
            return None
        span = rng
        kind = "apartado"

    # Narrow to letra.
    if anchor.letra:
        rng = _find_letra_range(lines, span[0], span[1], anchor.letra)
        if rng is None:
            return None
        span = rng
        kind = "letra"

    return _position_for_lines(lines, span[0], span[1], kind)


def _find_unique(
    outline: list[_HeadingNode],
    predicate,
) -> _HeadingNode | None:
    """Return the single heading matching predicate, or None if 0 or >1."""
    hits = [h for h in outline if predicate(h)]
    if len(hits) != 1:
        if len(hits) > 1:
            logger.debug("anchor matched %d headings; treating as ambiguous", len(hits))
        return None
    return hits[0]


def _position_for_lines(
    lines: list[str],
    start: int,
    end: int,
    kind: str,
) -> Position:
    content = "\n".join(lines[start:end])
    return Position(line_start=start, line_end=end, content=content, kind=kind)


# ──────────────────────────────────────────────────────────
# Convenience: one-shot
# ──────────────────────────────────────────────────────────


def resolve_anchor_from_hint(markdown: str, hint: str) -> Position | None:
    """Shortcut for callers that just want to pass the raw hint text."""
    return resolve_anchor(markdown, parse_anchor_from_hint(hint))
