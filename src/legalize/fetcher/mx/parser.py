"""Mexico parser — multi-source.

Bytes returned by ``MXClient`` are JSON envelopes (``{source, norm_id, …}``)
that wrap whatever raw payload came back from the portal — for Diputados,
either the consolidated PDF (legacy) or the Word 97-2003 .doc file (default).
The parser dispatches on the envelope's ``source`` field and, within Diputados,
on ``source_format`` (``"doc"`` or ``"pdf"``) to the right per-format helper.

Implemented today: Diputados (single-snapshot text + index-based metadata).
Other sources still raise ``NotImplementedError``.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import re
import unicodedata
from datetime import date
from typing import Any

import pdfplumber

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import Block, NormMetadata, NormStatus, Paragraph, Rank, Reform, Version

logger = logging.getLogger(__name__)

_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")

# Article-heading detector. Captures the article number/ordinal AND any text
# that follows on the same visual line (PDF wraps the title with body).
# Examples that match:
#   "Artículo 1o. En los Estados Unidos Mexicanos…"
#   "Artículo 4o.- La mujer y el hombre son iguales…"
#   "ARTÍCULO 123. Toda persona tiene derecho al trabajo…"
#   "Artículo Único.- Se reforma…"
_ARTICULO_RE = re.compile(
    r"^(Art[íi]culo|ART[ÍI]CULO)\s+"
    r"(?P<num>\d+(?:\s*[ºo°])?|[Úú]nico|Primero|Segundo|Tercero|Cuarto|Quinto|Sexto|S[ée]ptimo|Octavo|Noveno|D[ée]cimo|PRIMERO|SEGUNDO|TERCERO|CUARTO|QUINTO|SEXTO|S[ÉE]PTIMO|OCTAVO|NOVENO|D[ÉE]CIMO)"
    r"\s*(?P<sep>[.\-]+)?\s*"
    r"(?P<rest>.*)$",
)

# Matches an article number that is a NUMERIC ordinal (e.g. "1o", "2", "23o", "1 o").
# Used to distinguish the issuing decree's word-ordinal articles (PRIMERO, SEGUNDO…)
# from the main law's numeric articles (1o., 2., 3o.-).
_NUMERIC_ARTICULO_NUM_RE = re.compile(r"^\d")

# Matches a WORD ordinal article number (PRIMERO, SEGUNDO … or mixed-case equivalents).
# Used to detect the issuing-decree head articles that precede the main law.
_WORD_ORDINAL_NUM_RE = re.compile(
    r"^(?:PRIMERO|SEGUNDO|TERCERO|CUARTO|QUINTO|SEXTO|S[ÉE]PTIMO|OCTAVO|NOVENO|D[ÉE]CIMO"
    r"|Primero|Segundo|Tercero|Cuarto|Quinto|Sexto|S[ée]ptimo|Octavo|Noveno|D[ée]cimo)$",
    re.IGNORECASE,
)

# Section / chapter / title headings — for grouping.
# Requirements to avoid matching prose mid-sentence:
#   1. Must start with the exact keyword in ALL-CAPS (title-case prose like
#      "Capítulo, el reglamento…" is already excluded by the uppercase anchor).
#   2. After optional whitespace, must be followed ONLY by an ordinal word,
#      Roman numeral, digit, ALL-CAPS text, or end-of-line.  A comma
#      immediately after the keyword always means prose — reject it.
_SECTION_SUFFIX = (
    r"(?:\s+"
    r"(?:"
    r"[IVXLC]+[.\-]?"                                    # Roman numeral (I, II, IV…)
    r"|[0-9]+"                                           # arabic number
    r"|(?:PRIM|SEG|TERC|CUART|QUINT|SEXT|S[ÉE]PT|OCT|NOV|D[ÉE]C)\w*"  # ordinal stems
    r"|[A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ\s\d.\-]*"               # ALL-CAPS text
    r")"
    r")?\s*$"
)
_TITULO_RE = re.compile(r"^T[ÍI]TULO" + _SECTION_SUFFIX)
_CAPITULO_RE = re.compile(r"^CAP[ÍI]TULO" + _SECTION_SUFFIX)
_SECCION_RE = re.compile(r"^SECCI[ÓO]N" + _SECTION_SUFFIX)
_LIBRO_RE = re.compile(r"^LIBRO" + _SECTION_SUFFIX)
_TRANSITORIOS_RE = re.compile(
    r"^\s*ART[ÍI]CULOS?\s+TRANSITORIOS?(?:\s+DE\s+DECRETOS?\s+DE\s+REFORMA)?\s*$",
    re.IGNORECASE,
)

# Matches the boundary heading that switches us into decreto-tail mode.
# Once seen, every subsequent "Artículo …" is a transitorio of a reform decree
# and must NOT be parsed as a main-law article heading.
_DECRETO_TAIL_TRIGGER_RE = re.compile(
    r"^\s*ART[ÍI]CULOS?\s+TRANSITORIOS?\s+DE\s+DECRETOS?\s+DE\s+REFORMA\s*$",
    re.IGNORECASE,
)

# Matches individual reform-decree intro lines, e.g.:
#   "DECRETO por el que se reforman los artículos 65 y 66…"
# These become sub-section headings inside the decreto-tail context.
_DECRETO_HEADER_RE = re.compile(
    r"^\s*DECRETO\b",
    re.IGNORECASE,
)

# Inside decreto-tail, "TRANSITORIOS" and "TRANSITORIO" headings are
# sub-headings within a decree block, not the main ARTÍCULOS TRANSITORIOS
# section heading that the outer parser handles.
_DECRETO_TRANSITORIOS_RE = re.compile(
    r"^\s*TRANSITORIOS?\s*$",
    re.IGNORECASE,
)

# Reform-provenance stamps Diputados injects after each amended fragment:
#   "Párrafo reformado DOF 04-12-2006, 10-06-2011"
#   "Artículo reformado DOF 14-08-2001"
#   "Fracción adicionada DOF 12-04-2019"
#   "Apartado A reformado DOF …"
#   "Reforma DOF 14-08-2001: Derogó del artículo…"     ← bare Reforma prefix (DOC only)
#   "Denominación del Capítulo reformada DOF …"         ← heading-level stamp (DOC only)
#   "Base reformada DOF …" / "Numeral reformado DOF …"  ← sub-article unit stamps
# Tagged as nota_pie so the renderer emits them as quoted small text instead
# of being mistaken for actual law text.
_REFORM_STAMP_RE = re.compile(
    r"^\s*(?:"
    # classic unit-level stamps
    r"p[áa]rrafo|art[íi]culo|fracci[óo]n|inciso|apartado|"
    r"subinciso|secci[óo]n|cap[íi]tulo|t[íi]tulo|fe\s+de\s+erratas|"
    # additional DOC-only stamps
    r"reforma|denominaci[óo]n|base|numeral|encabezado"
    r")\b"
    r"[\s\S]*?\bDOF\s+\d{2}-\d{2}-\d{4}",
    re.IGNORECASE,
)

# Sub-article structure detectors. The Mexican federal style nests articles
# into Apartados (single capital letter), then fracciones (Roman numeral),
# then incisos (lowercase letter). PDF text-extraction does not preserve
# blank lines between these, so we use the leading marker to force a
# paragraph break.
_APARTADO_RE = re.compile(r"^[A-Z]\.\s+\S")
_FRACCION_RE = re.compile(
    # Matches fracciones both with explicit punctuation (I. / I- / I) / i.)
    # and with the bare "I<space><UppercaseLetter>" form that Diputados DOC
    # files sometimes use (e.g. "I Pudieren verse perjudicadas…").
    # The bare form requires an uppercase letter after the space so that
    # mid-sentence Roman references are not mistakenly split.
    r"^(?:[IVX]+|[ivx]+)(?:[.\-\)]\s+\S|\s+[A-ZÁÉÍÓÚÜÑ]\S)",
    re.UNICODE,
)
_INCISO_RE = re.compile(r"^[a-z]\)\s+\S")


# ── Module helpers ─────────────────────────────────────────────────────


def _decode_envelope(data: bytes) -> dict[str, Any]:
    """Decode the JSON envelope produced by MXClient. Raises on malformed data."""
    try:
        envelope = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"MX parser expected JSON envelope, got: {exc}") from exc
    if not isinstance(envelope, dict) or "source" not in envelope:
        raise ValueError("MX parser envelope is missing 'source' key")
    return envelope


def _clean_line(text: str) -> str:
    """Normalize whitespace and strip control chars for one line."""
    text = unicodedata.normalize("NFC", text)
    text = _CONTROL_RE.sub("", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_pdf_text(pdf_bytes: bytes) -> list[str]:
    """Return one string per page of the PDF."""
    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages.append(text)
    return pages


_PAGINATION_RE = re.compile(r"^(?:p[áa]gina\s+)?\d+\s+de\s+\d+$", re.IGNORECASE)
_DIPUTADOS_BOILERPLATE_RE = re.compile(
    r"(c[áa]mara de diputados|congreso de la uni[óo]n|"
    r"secretar[íi]a general|secretar[íi]a de servicios parlamentarios|"
    r"centro de documentaci[óo]n)",
    re.IGNORECASE,
)

# DOF page-header lines injected at the end of some Word DOC files.
# Pattern: "(Primera/Segunda/… Sección) [TAB] DIARIO OFICIAL [TAB] <weekday> …"
# Unlike _DIPUTADOS_BOILERPLATE_RE (which uses re.search), this is matched
# against the full paragraph using re.search for the combined pattern.
_DOF_PAGE_HEADER_RE = re.compile(
    r"(?:primera|segunda|tercera|cuarta|quinta|sexta)\s+secci[óo]n\s*[)\]]\s+diario\s+oficial",
    re.IGNORECASE,
)
_LAST_REFORM_FOOTER_RE = re.compile(
    r"^[ÚU]ltima\s+[Rr]eforma\s+(?:DOF|publicada)", re.IGNORECASE
)

# Matches a DOF keyword followed by one or more comma-separated DD-MM-YYYY dates.
# Example: "DOF 04-12-2006, 10-06-2011" → captures "04-12-2006, 10-06-2011".
# Used to extract individual date strings from the multi-date capture group.
_DOF_DATE_RE = re.compile(r"\bDOF\s+((?:\d{2}-\d{2}-\d{4}(?:,\s*)?)+)", re.IGNORECASE)
# Sub-pattern that extracts individual DD-MM-YYYY dates from a DOF date group.
_DATE_TOKEN_RE = re.compile(r"\d{2}-\d{2}-\d{4}")

# Maps keywords in reform stamps to canonical commit types.
# Priority: explicit adición/derogación/erratas keywords override the default "reforma".
_STAMP_TYPE_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bfe\s+de\s+erratas\b", re.IGNORECASE), "fe_de_erratas"),
    (re.compile(r"\bderogad[ao]\b", re.IGNORECASE), "derogacion"),
    (re.compile(r"\badicionad[ao]\b", re.IGNORECASE), "adicion"),
]
_STAMP_DEFAULT_TYPE = "reforma"


# ── DOC text extraction ────────────────────────────────────────────────

# Matches OLE2/Word internal field codes that leak into the text stream,
# e.g. "PAGE", "NUMPAGES406", "EMBED Word.Picture.8 …".
_DOC_FIELD_RE = re.compile(r"^(?:PAGE|NUMPAGES\S*|EMBED\s+\S+)", re.IGNORECASE)

# Matches Word 97 style-sheet / field-code tokens that bleed verbatim into
# the WordDocument text stream.  These originate from:
#   • OLE2 document summary / FIB header  ("bjbj" is a fixed signature word)
#   • Word style-sheet XML references     (OJQJ, CJOJQJ, mH sH, CJPJaJ, ^JaJ …)
#   • Conditional-format IF field codes   ($If, IfF4, IfF42, Faöf4 …)
#   • Field code delimiters / cell refs   (Qkd<hex>, gd<name>, $$Ifa$ …)
#   • Word picture-layout cell refs       (dð¤ = d + eth U+00F0 + U+00A4 …)
# Any paragraph that contains at least one of these tokens is field-code
# garbage and must be dropped.  The patterns are deliberately narrow (word
# boundaries, specific sub-strings) so that ordinary Spanish prose that
# happens to contain a dollar sign or a capital letter run is not affected.
_WORD_FIELD_CODE_RE = re.compile(
    r"bjbj"                    # OLE2 Word document signature (always garbage)
    r"|OJ[PQ]J"                # Word style-sheet marker: OJQJ or OJPJQJ variants
    r"|\bIfF\d"                # Conditional field code: IfF4, IfF42, IfFM …
    r"|\$If\^"                 # Conditional-format cell reference: $If^
    r"|\$\$Ifa\$"              # Conditional-format array ref: $$Ifa$
    r"|\$\$If[A-Z]"            # Conditional-format field block: $$IfF…, $$IfT… (any suffix)
    r"|\$%@[A-Z]"              # TOC-style garbage delimiter: $%@A, $%@B …
    r"|\$!`!\w\$"              # Field-code cell reference: $!`!a$, $!`!b$ …
    r"|Faöf4"                  # Filter field code suffix (öf4 is diagnostic)
    r"|[Qç]kd[A-Za-z0-9$ì\xaa]"  # Field-code delimiter token (Qkd / çkd + next char)
    r"|[^\x00-\x7f]kd[^\x00-\x7f]"  # Non-ASCII + kd + non-ASCII (variant field-code delimiter)
    r"|gd[A-Za-z0-9\[{<_#àÁ¿·Ë;ï¢þ³ô¶úÀ-ÿ~\-]"  # Named range ref in Word style sheet dump
    r"|mH\s+sH"                # Word paragraph-spacing attribute (mH<ws>sH).
                               # NOTE: no \b anchors — this token always appears
                               # glued to other style codes (e.g. CJmH\tsH) so
                               # \b before 'm' would never match inside that cluster.
    r"|CJPJaJ"                 # Word character-style code: CJK + Para + AllJustify
    r"|d\xf0\xa4"              # Word picture-cell reference token (d + eth + ¤)
    # ── Trailing Word stylesheet / drawing-object tokens (Bug 1 extension) ────
    # These appear in the binary tail of DOC files after the last legitimate
    # text paragraph.  They represent inline character-style property strings
    # that the Word binary format stores after the document text stream.
    r"|\d?CJ\^J"               # CJ^J / 5CJ^J — Word char-style "CJK + no kerning"
    r"|CJPJ[\^]]?aJ"           # CJPJ^JaJ / CJPJ]aJ / CJPJaJ variants (ParagraphJustify)
    r"|CJPJ\^J"                # CJPJ^J without trailing aJ (property-only form)
    r"|B\*CJ"                  # B*CJ — Word "bold + CJK" boolean attribute
    r"|CJaJ"                   # CJaJ — bare CJK + AllJustify (no word-boundary needed)
    r"|CJh[A-Za-z0-9!]?"        # CJh — Word CJK-handle stub (h = handle prefix);
                               # matches CJh, CJhm, CJh§, etc.
                               # never appears in Spanish prose.
    r"|CJ\dh"                  # CJ<digit>h — Word CJK-handle with numeric suffix
                               # e.g. CJ1h, CJ7h as in bCJ1h, hO~ãCJ7h»Ãh
    r"|nH\s*tH"                # nH tH — Word "no-hyphenation + Thai" flag cluster
    # ── OLE2 drawing-object coordinate indicators ───────────────────────────────
    # U+00A4 (¤ currency sign) and U+00A6 (¦ broken bar) appear in Word drawing-
    # object coordinate dumps but essentially never in authentic Spanish legislation.
    r"|[\xa4\xa6]"             # ¤ / ¦ — drawing-object coordinate artifact bytes
    # U+00A0 (non-breaking space) combined with ASCII punctuation not used in
    # Spanish legislative prose.  \xa0&, \xa0/, \xa0[, \xa0! etc. are Word
    # internal reference / coordinate artifacts; legitimate uses of \xa0 in
    # Spanish text are always surrounded by regular words, not punctuation.
    r"|\xa0[!-/:-@\[-`{-~]|[!-/:-@\[-`{-~]\xa0"
    # Alternating uppercase-ASCII + non-ASCII sequence of 3+ pairs that contains
    # at least one non-Spanish non-ASCII char.  Covers coordinate dumps like
    # "U¾U¿UV\tVúVûVÿVWW" (Word drawing-object cell dump).
    r"|(?:[A-Z][^\x00-\x7fáéíóúÁÉÍÓÚñÑ]){3,}"
)

# Characters that are valid in ordinary Spanish legislative text (including
# Windows-1252 extended Latin and common punctuation).  Everything else in
# the 0x80-0xFF range is OLE2/field-code artefact.
_SPANISH_HIGHBYTE_RE = re.compile(
    r"[áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãõÃÕñÑçÇ"
    r"¡¿«»—–‘’""°ºª"
    r"·«»]"
)

# 3+ consecutive identical non-ASCII chars — indicates OLE2 / TOC table dump.
# Legitimate Spanish text never repeats the same accented letter 3 times in a row.
_REPEAT_NONASCII_RE = re.compile(r"([^\x00-\x7f])\1{2,}")

# 3+ consecutive repetitions of the same NON-ASCII 2-char pair — Word style-sheet
# comparison-table artifact (e.g. ïáïáïáïá, òáòáòáòá, ÎÎÎÎ is caught by
# _REPEAT_NONASCII_RE, but alternating-pair dumps require this check).
_REPEAT_PAIR_NONASCII_RE = re.compile(r"([^\x00-\x7f][^\x00-\x7f])\1{3,}")

# 3+ consecutive repetitions of the same 4-char non-ASCII sequence — long-period
# Word style-sheet table dumps that escape the 2-char pair check.  Example:
# ``üõüíüõüíüõüí…`` (period 4: ü, õ, ü, í) repeated dozens of times.  Legitimate
# Spanish text never repeats a 4-char non-ASCII sequence three times in a row.
_REPEAT_QUAD_NONASCII_RE = re.compile(
    r"([^\x00-\x7f][^\x00-\x7f][^\x00-\x7f][^\x00-\x7f])\1{2,}"
)

# Matches a sequence of 3+ alphabetic characters (used to detect "real words" in a
# paragraph).  Combined with _ROMAN_NUMERAL_WORD_RE below this lets us distinguish
# legitimate prose from short OLE2 / field-code garbage that escaped the other checks.
_REAL_WORD_ALPHA_RE = re.compile(r"[A-Za-záéíóúÁÉÍÓÚñÑüÜ]{3,}")

# Pure Roman numeral word (all I/V/X/L/C/D/M, any case).  Used to exclude Roman
# numeral tokens like "VIII" from the "real word" count — "I A VIII." is garbage
# even though "VIII" is 4 alphabetic characters.
_ROMAN_NUMERAL_WORD_RE = re.compile(r"^[IVXLCDMivxlcdm]+$")

# Curated set of all-uppercase Spanish words that legitimately appear in
# legislative text (section headings, gazette stamps, signatory titles).  Used
# by ``_is_spanish_word`` to admit short all-caps headings while rejecting
# random all-caps stylesheet dumps like ``XYfTU`` or ``HDEFIJ``.
_LEGAL_ALL_CAPS_WORDS: frozenset[str] = frozenset({
    "ARTICULO", "ARTÍCULO", "ARTICULOS", "ARTÍCULOS",
    "TITULO", "TÍTULO", "TITULOS", "TÍTULOS",
    "CAPITULO", "CAPÍTULO", "CAPITULOS", "CAPÍTULOS",
    "SECCION", "SECCIÓN", "SECCIONES", "LIBRO", "LIBROS",
    "DECRETO", "DECRETA", "DECRETOS", "ANEXO", "ANEXOS",
    "TRANSITORIO", "TRANSITORIOS", "CONSIDERANDO", "CONSIDERANDOS",
    "UNICO", "ÚNICO", "PRIMERO", "PRIMERA", "SEGUNDO", "SEGUNDA",
    "TERCERO", "TERCERA", "CUARTO", "CUARTA", "QUINTO", "QUINTA",
    "SEXTO", "SEXTA", "SEPTIMO", "SÉPTIMO", "OCTAVO", "NOVENO",
    "DECIMO", "DÉCIMO", "REFORMA", "REFORMAS", "REFORMADO", "REFORMADA",
    "PUBLICADA", "PUBLICADO", "DOF", "DIPUTADOS", "SENADO", "SENADORES",
    "MEXICO", "MÉXICO", "MEXICANOS", "MEXICANAS", "FEDERAL", "FEDERALES",
    "PRESIDENTE", "PRESIDENTA", "SECRETARIO", "SECRETARIA",
    "CONSTITUCION", "CONSTITUCIÓN", "CONSTITUCIONALES",
    "REPUBLICA", "REPÚBLICA", "ESTADOS", "UNIDOS",
    "LEY", "LEYES", "CODIGO", "CÓDIGO", "REGLAMENTO",
    "FRACCION", "FRACCIÓN", "INCISO", "PARRAFO", "PÁRRAFO",
    "GENERAL", "GENERALES", "NACIONAL", "NACIONALES",
    # Signatory titles & common Spanish honorifics in scanned signature blocks.
    "LICENCIADO", "LICENCIADA", "DOCTOR", "DOCTORA", "MAESTRO", "MAESTRA",
    "INGENIERO", "INGENIERA", "ARQUITECTO", "ARQUITECTA",
    "RUBRICA", "RÚBRICA", "RUBRICAS", "RÚBRICAS",
    "GOBERNADOR", "GOBERNADORA", "GOBERNACION", "GOBERNACIÓN",
    "DIPUTADO", "DIPUTADA", "SENADOR", "SENADORA",
    "MINISTRO", "MINISTRA", "PROCURADOR", "PROCURADORA",
    "SUPREMA", "CORTE", "JUSTICIA", "NACION", "NACIÓN",
    "ACUERDOS", "CERTIFICA", "AUDITORIA", "AUDITORÍA",
})


def _is_spanish_word(word: str) -> bool:
    """Return True when ``word`` looks like a real Spanish word (not a Word
    stylesheet handle fragment or a random alphabet sweep).

    Tiered acceptance:
    - Pure Roman numeral tokens (``I``, ``VIII``) are rejected.
    - Words containing a Word stylesheet handle substring are rejected.
    - Strictly increasing alphabet sweeps (``efgij``, ``BCDEF``) are rejected.
    - Words with NO vowel are rejected.
    - 3-4 char words: accepted as long as they have a vowel (covers headings
      like ``Col``, ``Ley``, ``DOF``, ``ART``, abbreviations like ``IMSS``).
    - 5+ char curated all-caps headings (``ARTÍCULO``, ``CAPÍTULO``, …) accepted.
    - 5+ char other words: must follow Spanish casing (all-lowercase OR
      Capitalized + rest lowercase) AND contain ≥ 2 vowels.  This rejects
      mixed-case stylesheet dumps (``XYfTU``, ``WwhdÚCJ``, ``aCDEL``, ``Ífjop``).
    """
    word = word.rstrip(".")
    if not word or len(word) < 3:
        return False
    if _ROMAN_NUMERAL_WORD_RE.match(word):
        return False
    if _WORD_HANDLE_TOKENS_RE.search(word):
        return False
    lw = word.lower()
    # Vowel requirement — every Spanish word has at least one.
    if not any(c in "aeiouáéíóúü" for c in lw):
        return False
    # Strictly increasing alphabet sweep — applies to all lengths ≥ 3.  Real
    # Spanish words don't sweep the alphabet (``abc``, ``bcd``, ``efg``,
    # ``ghi``, ``efgij``, ``BCDEF`` are all stylesheet alphabet dumps).
    is_sweep = all(
        lw[i].isalpha() and lw[i + 1].isalpha()
        and 0 <= ord(lw[i + 1]) - ord(lw[i]) <= 2
        for i in range(len(lw) - 1)
    )
    if is_sweep:
        return False
    upper = word.upper()
    if len(word) < 5:
        # Short words (3-4 chars): only accept curated all-caps headings
        # (``LEY``, ``DOF``) or 4-char Spanish tokens that are NOT all-caps
        # ASCII handle fragments.  This rejects 3-4 char stylesheet noise
        # like ``CJh``, ``aJh``, ``IJÍ``, ``UVh``, ``EFT`` while still
        # admitting ``Ley``, ``Para``, ``Esta``, ``Esto``, ``Pena``, ``Esto``,
        # ``Mes``, ``año`` etc. via the criteria below:
        #   - all-lowercase Spanish word, OR
        #   - Capitalized + lowercase rest, OR
        #   - in the curated all-caps list.
        if upper in _LEGAL_ALL_CAPS_WORDS:
            return True
        if word.isupper():
            return False
        if word.islower() or word[1:].islower():
            return True
        return False
    if upper in _LEGAL_ALL_CAPS_WORDS:
        return True
    if word.isupper():
        # All-caps Spanish words must appear in the curated list; otherwise
        # treat as stylesheet noise.  Proper-name signatory blocks
        # (``RAFAEL COELLO CETINA``) are accepted because the surrounding
        # ``LICENCIADO`` / ``RUBRICA`` / etc. matches keep ``has_real_word``
        # True at the paragraph level even when individual proper-name tokens
        # don't qualify on their own.
        return False
    # Casing — Spanish words are either all-lowercase or Capitalized + rest lowercase.
    if not (word.islower() or word[1:].islower()):
        return False
    # Vowel content — ≥ 2 lowercase / accented vowels for 5+ char words.
    vowel_count = sum(1 for c in lw if c in "aeiouáéíóú")
    if vowel_count < 2:
        return False
    return True

# Word stylesheet character-style handle tokens that bleed into the text stream.
# Each match here is diagnostic on its own when it appears in a short paragraph —
# legitimate Spanish prose never contains the exact substrings ``CJ\^J``,
# ``CJUV``, ``CJUa``, ``5aJ``, ``aJh``, ``hf_``, ``mHnHu`` etc.
_WORD_HANDLE_TOKENS_RE = re.compile(
    r"CJ\\\^J"            # CJ\^J — char-style "CJK + no kerning" with backslash escape
    r"|CJ\\aJ"            # CJ\aJ — char-style with backslash separator
    r"|CJUV"              # CJUV — Word "CJK + Underline + Vertical" handle
    r"|CJUa"              # CJUa — variant of CJUV with AllJustify
    r"|CJU\b"             # bare CJU at token boundary
    r"|CJOJ"              # CJOJ — CJK + Outer + Justify
    r"|\^Jh"              # ^Jh — caret-J handle suffix
    r"|0J[Uja]"           # 0JU / 0Jj / 0Ja — "no-kerning, justify" prefixes
    r"|5aJ"               # 5aJ — char-style cluster
    r"|aJh[A-Za-z\xc0-\xff_]"  # aJh<id> — handle reference
    r"|hf_h"              # hf_h — Word style-sheet handle reference
    r"|mHnHu"             # mHnHu — Word "no-hyphenation" attribute
    r"|JU\b"              # JU at token boundary
    r"|J\\\^J"            # J\^J — split form of CJ\^J
)

# Catches "sequential single-key byte runs": one letter immediately followed by
# 4+ fragments that all start with the same letter and have only short
# non-alphabetic gaps.  Examples from the closed-PR corpus:
#   uuu$u%u&u2u5uBuKuSuXub...
#   jvjwj{j|jîjïjòjój3k4k...   (key changes mid-run; we match each run)
#   hyOh5aJhúENhA=5aJ           (key 'h')
#   B%B/B0BHBRBSBT
# A "fragment" is up to 3 non-alpha chars.  Five+ repetitions of the same key
# letter in this pattern is never legitimate Spanish prose.
_SEQUENTIAL_KEY_RUN_RE = re.compile(
    r"([A-Za-z])(?:[^A-Za-z\s]{0,3}\1){4,}"
)


def _is_binary_garbage(text: str) -> bool:
    """Return True when a paragraph is OLE2 binary data or Word field-code garbage.

    Six independent signals trigger a True verdict:

    1. **Field-code tokens** — the paragraph contains at least one of the
       diagnostic Word/OLE2 tokens captured by ``_WORD_FIELD_CODE_RE``
       (``bjbj``, ``OJQJ``, ``$If^``, ``Faöf4``, ``Qkd…``, ``gd…``,
       ``mH sH``).  These are unambiguous: they never appear in authentic
       legislative Spanish prose.

    2. **Embedded newline** — the paragraph (already split on ``\\r``) contains
       a ``\\n`` (U+000A linefeed) character.  In the Word binary text stream
       ``\\r`` is the paragraph separator; ``\\n`` only appears inside OLE2
       table / TOC binary data that bleeds into the text stream.  Legitimate
       legislative paragraphs never contain embedded newlines after the
       ``\\r``-split.  (Tabs ``\\t`` are NOT filtered here because Diputados
       uses them as fraccion/apartado indent separators, e.g. ``I.\\tTexto…``.)

    3. **Non-Spanish high-byte majority** — the paragraph has more than 8
       bytes above U+007F and fewer than 25 % of those bytes are valid
       Spanish extended-Latin / punctuation characters.  This catches the
       OLE2 FIB header block (which always starts the WordDocument stream)
       and any embedded-object binary that slipped through the control-char
       strip.

    4. **Repeated non-ASCII character** — 4 or more consecutive occurrences
       of the same non-ASCII character (e.g. ``ôôôô``, ``ØØØØØ``).
       Legitimate Spanish legislative text never repeats an accented letter
       that many times in a row; this pattern is exclusive to OLE2 / Word
       TOC table cell dumps.

    5. **Repeated non-ASCII 2-char pair** — 4 or more consecutive repetitions
       of the same 2-char non-ASCII pair (e.g. ``ïáïáïáïá``, ``òáòáòáòá``).
       Produced by Word style-sheet comparison tables; never in Spanish prose.

    6. **Short paragraph with no recognisable Spanish word** — the paragraph
       is ≤ 200 characters AND does not contain a single token of 3 or more
       alphabetic characters that is not a pure Roman numeral (I/V/X/L/C/D/M).
       This catches residual OLE2 field-code artifacts that begin with an
       ASCII prefix that looks like a fracción marker (e.g. ``I J m!!!¦"È"É"…``
       or ``I A VIII.``) but have no real Spanish prose words.  Legitimate
       fracciones like ``I Pudieren verse perjudicadas…`` survive because
       "Pudieren" is 8 alphabetic characters.  Section headings survive because
       "ARTÍCULO", "TÍTULO", "CAPÍTULO", etc. are all ≥ 6 non-Roman-numeral
       alphabetic characters.

    Short paragraphs (≤4 chars) bypass the high-byte check but are still
    tested for field-code tokens and embedded newlines.
    """
    if not text:
        return True
    # Markdown pipe-table paragraphs (produced by _word_table_to_markdown) are
    # multi-line strings that start with "| ".  They are never binary garbage;
    # skip all checks for them.
    if text.startswith("| "):
        return False
    # Signal 1: explicit field-code / OLE2 token.
    if _WORD_FIELD_CODE_RE.search(text):
        return True
    # Signal 2: embedded newline — never present in legitimate legislative text.
    if "\n" in text:
        return True
    # Signal 3: high non-ASCII fraction with few Spanish accented chars.
    if len(text) > 4:
        non_ascii_chars = [c for c in text if ord(c) > 0x7F]
        if len(non_ascii_chars) > 8:
            spanish_count = sum(
                1 for c in non_ascii_chars if _SPANISH_HIGHBYTE_RE.match(c)
            )
            if spanish_count / len(non_ascii_chars) < 0.25:
                return True
    # Signal 4: repeated non-ASCII character run — 4+ identical non-ASCII bytes in a row.
    # Legitimate Spanish text never repeats the same accented char 4 times consecutively.
    # Garbage TOC / style-sheet dumps do (e.g. ôôôôôôôô, öööööö, ÎÎÎÎÎÎÎÎ).
    if _REPEAT_NONASCII_RE.search(text):
        return True
    # Signal 5: repeating non-ASCII 2-char pair — 3+ consecutive repetitions of the same
    # two non-ASCII chars (e.g. ïáïáïáïá, òáòáòáòá).  This pattern is produced by
    # Word style-sheet comparison table dumps and never appears in Spanish legislative text.
    if _REPEAT_PAIR_NONASCII_RE.search(text):
        return True
    # Signal 5b: repeating non-ASCII 4-char sequence (e.g. ``üõüíüõüíüõüí…``).
    # Same provenance as signal 5 but a longer period.
    if _REPEAT_QUAD_NONASCII_RE.search(text):
        return True
    # Signal 6: paragraph with no recognisable Spanish word AND either short or
    # dominated by numeric/symbol sequences.
    # A "real word" is 3+ consecutive alphabetic characters that are NOT a pure Roman
    # numeral (sequences of I/V/X/L/C/D/M only).  Real fracciones always contain at
    # least one such word; OLE2 field-code artifacts do not.
    # The length threshold is raised to 400 to catch coordinate-dump paragraphs
    # that exceed 200 chars but still contain no real Spanish words.
    #
    # For lines ≤ 80 chars we additionally require that the "real word" be ≥ 5
    # alphabetic chars and NOT itself look like a Word stylesheet handle (e.g.
    # ``CJh``, ``aJh``, ``JKT``, ``RST``, ``IJÍÎ`` are 3-char alpha runs that the
    # original signal accepted, but they are stylesheet token fragments, not
    # Spanish words).  Real legislative prose has at least one ≥ 5-letter word
    # (``de``/``la`` are too short to count, but ``Artículo``, ``vigor``,
    # ``Mexico`` etc. are present in every legitimate paragraph).
    if len(text) <= 400:
        has_real_word = False
        # Short paragraphs (≤ 80 chars) require a strict Spanish word — see
        # ``_is_spanish_word``.  Longer paragraphs only need any 3+ alpha
        # non-Roman token, because long stylesheet dumps are already filtered
        # by signal 3 (high-byte ratio) and signal 8 (sequential key run).
        require_strict = len(text) <= 80
        for m in _REAL_WORD_ALPHA_RE.finditer(text):
            word = m.group().rstrip(".")
            if require_strict:
                if _is_spanish_word(word):
                    has_real_word = True
                    break
            else:
                if _ROMAN_NUMERAL_WORD_RE.match(word):
                    continue
                has_real_word = True
                break
        if not has_real_word:
            return True

    # Signal 7: Word stylesheet handle-token cluster.  Two or more occurrences of
    # any handle token in a single paragraph are diagnostic; one occurrence is
    # diagnostic in a short (≤ 80 char) paragraph.  These tokens (``CJ\^J``,
    # ``CJUV``, ``5aJ``, ``aJh``, ``hf_``, ``mHnHu`` etc.) never appear in
    # authentic Spanish legislative prose.
    handle_hits = len(_WORD_HANDLE_TOKENS_RE.findall(text))
    if handle_hits >= 2:
        return True
    if handle_hits >= 1 and len(text) <= 80:
        return True

    # Signal 8: sequential single-key byte run — one letter repeated 5+ times
    # with only short non-alpha gaps between repetitions.  Produced by Word
    # binary coordinate / handle-table dumps; never appears in Spanish prose.
    if _SEQUENTIAL_KEY_RUN_RE.search(text):
        return True

    return False


def _is_garbage_table_row(line: str) -> bool:
    """Return True when ``line`` is a single Markdown pipe-table row whose cells
    contain only Word stylesheet / OLE2 garbage.

    Used by the tail-truncation passes and by the audit script.  This is
    intentionally separate from ``_is_binary_garbage`` (which exempts pipe-
    table rows wholesale, since legitimate multi-line tables produced by
    ``_word_table_to_markdown`` are also pipe-table rows).  We only call this
    helper at the document tail, where surviving garbage tables — produced by
    Word's binary footer that bleeds into the text stream — can be safely
    dropped because they are never the closing content of a real law.
    """
    s = line.strip()
    if not s.startswith("|") or not s.endswith("|"):
        return False
    # Pure separator row (``| --- | --- |``) is a structural row — garbage when
    # appearing on its own at the tail (no surrounding header / data).
    inner = s[1:-1]
    cells = [c.strip() for c in inner.split("|")]
    if all(re.fullmatch(r"-+", c) for c in cells if c):
        return True
    # If no cell contains a real Spanish word AND at least one cell contains
    # binary-garbage signals, treat the row as garbage.
    has_word = False
    has_garbage_signal = False
    for c in cells:
        if not c:
            continue
        for m in _REAL_WORD_ALPHA_RE.finditer(c):
            if _is_spanish_word(m.group()):
                has_word = True
                break
        if has_word:
            break
        # Garbage signals inside a cell.
        if (
            _WORD_FIELD_CODE_RE.search(c)
            or _WORD_HANDLE_TOKENS_RE.search(c)
            or _TAIL_PARAGRAPH_GARBAGE_RE.search(c)
            or _is_binary_garbage(c)
        ):
            has_garbage_signal = True
    return has_garbage_signal and not has_word


def _word_table_to_markdown(raw_segment: str) -> str | None:
    """Convert a Word binary table segment to a Markdown pipe table.

    Word 97-2003 stores table cells with BEL (U+0007) as the cell separator
    and a double-BEL (``\\x07\\x07``) as the row terminator.  A segment that
    contains BEL characters is interpreted as one or more table rows.

    Each row is a sequence of cells terminated by ``\\x07\\x07``.  Within a
    row, cells are separated by single ``\\x07``.

    Returns a Markdown pipe-table string (multi-line), or ``None`` if the
    segment contains no BEL characters, is not a valid table, or the cells
    do not contain recognisable Spanish text.
    """
    if "\x07" not in raw_segment:
        return None

    # Split into rows: double-BEL terminates each row.
    row_texts = re.split(r"\x07\x07", raw_segment)
    rows: list[list[str]] = []
    for row_text in row_texts:
        # Each remaining single-BEL separates cells within the row.
        cells = row_text.split("\x07")
        # Strip control chars and whitespace from each cell.
        cells = [_CONTROL_RE.sub("", c).strip() for c in cells]
        # Drop rows that are entirely empty (row separator artifacts).
        if any(c for c in cells):
            rows.append(cells)

    if not rows:
        return None

    # Validity check: the table must contain natural-language content.
    # Reject the table if:
    #  (a) the first row's combined text is itself binary garbage, OR
    #  (b) any cell in the first row contains a Word field-code token, OR
    #  (c) all cells combined contain only Diputados boilerplate text (header/
    #      footer templates that the Word file appends after the main body).
    # This distinguishes real table segments (which have Spanish prose in
    # at least one cell) from OLE2 binary / drawing-object segments that
    # also contain BEL bytes.
    first_row_cells = rows[0]
    first_row_text = " ".join(first_row_cells)
    # Strip remaining control chars before checking.
    first_row_text_clean = _CONTROL_RE.sub("", first_row_text).strip()
    if not first_row_text_clean:
        return None
    if _is_binary_garbage(first_row_text_clean):
        return None
    # Additionally reject if any cell contains a Word field-code token —
    # drawing-object cells often pass the Spanish-word check when a short
    # real word happens to be embedded in the code string.
    if any(_WORD_FIELD_CODE_RE.search(c) for c in first_row_cells):
        return None
    # Reject Diputados boilerplate header/footer tables (e.g. the page-header
    # template that appears after the last real paragraph in some DOC files).
    all_cells_text = " ".join(c for r in rows for c in r)
    if _DIPUTADOS_BOILERPLATE_RE.search(all_cells_text):
        return None
    if _LAST_REFORM_FOOTER_RE.search(all_cells_text):
        return None

    # Normalise row widths so all rows have the same column count.
    max_cols = max(len(r) for r in rows)
    rows = [r + [""] * (max_cols - len(r)) for r in rows]

    # Build pipe-table lines.
    def _row_line(cells: list[str]) -> str:
        return "| " + " | ".join(c if c else " " for c in cells) + " |"

    lines = [_row_line(rows[0])]
    # Separator row (required by Markdown spec).
    lines.append("| " + " | ".join("---" for _ in range(max_cols)) + " |")
    for row in rows[1:]:
        lines.append(_row_line(row))
    return "\n".join(lines)


# Minimum consecutive garbage paragraphs at the document tail that trigger
# the "tail blob" truncation.  Five is chosen to avoid cutting legitimate
# isolated binary artifacts that sometimes appear mid-document.
_TAIL_BLOB_THRESHOLD = 5

# Broader tail-garbage detector used in both _truncate_tail_blob and the final
# micro-trim.  Only applied to paragraphs that are SHORT (≤ 120 chars) or
# known to be in a binary tail context.  More permissive than _is_binary_garbage
# to catch patterns that pass the main filter but are clearly garbage at the
# end of a document.
_TAIL_PARAGRAPH_GARBAGE_RE = re.compile(
    # Word stylesheet property token clusters (various forms)
    r"[A-Z][a-z]?[A-Z]\^[A-Z]"   # e.g. CJ^J, CJ^JaJ-style sub-tokens
    r"|CJaJ|CJPJ|B\*CJ|nHtH"
    r"|CJh"                        # CJh — Word CJK-handle stub
    r"|\dCJ"                       # 5CJ, 6CJ … numeric CJK style prefix
    r"|^h[0-9(A-Z\xc0-\xff]"       # h9(, hJg, hÏTú … style-sheet ref starts
    r"|^\xb4"                      # ´CJh, ´5CJ … acute-accent garbage prefix
    r"|gd[^\s,;.]"                 # gd named-range refs
    # Dense numeric/symbolic coordinate sequences (OLE2 binary coordinate dumps)
    r"|(?:[A-Z]{1,2}\d+){3,}"
    # Alternating letter + non-letter/non-space pairs — lowered to 3 for tail context
    # (catches VúVûVÿ / U¾U¿ style Word drawing-object coordinate cells)
    r"|(?:[A-Za-z][^A-Za-z\s]){3,}"
    # Non-ASCII ordinal indicator (ª) repeated
    r"|\xaa{2,}|(?:[^\x00-\x7f]\xaa){2,}"
    # $& prefix (OLE2 named-range cell reference start)
    r"|^\$[&%!]"
    # WWXX / JJJJ style repeated uppercase-2-char pairs (graphic coords)
    r"|([A-Z]{2,3})\1{2,}"
    # BBB / CCC / DDD / ddd / ggg — 3+ identical letters
    r"|([A-Za-z])\2{2,}"
    # Dense sequences of non-ASCII chars outside Spanish range (6+ consecutive)
    r"|[^\x00-\x7fáéíóúÁÉÍÓÚñÑüÜ]{6,}"
    # OLE2 drawing-coordinate strings: non-ASCII char alternating with digit/punct
    r"|(?:[^\x00-\x7f][!\"#$%&'()*+,\-./0-9:;<=>?]){4,}"
    # Backtick characters — never appear in Spanish legislative text
    r"|`{2,}|[`\xad].*`"
    # Non-breaking space (\xa0) adjacent to ASCII punctuation
    r"|\xa0[!-/:-@\[-`{-~]|[!-/:-@\[-`{-~]\xa0"
    # Degree sign (\xb0 / °) immediately followed by non-breaking space (\xa0).
    # In Spanish legislative text ° appears in ordinals ("1°") followed by a
    # regular space or no space; °\xa0 only appears in Word binary coordinate
    # dumps.
    r"|\xb0\xa0"
    # Word stylesheet handle tokens — see _WORD_HANDLE_TOKENS_RE for full list.
    r"|CJ\\\^J|CJ\\aJ|CJUV|CJUa|CJOJ|0J[Uja]|5aJ|aJh|hf_h|mHnHu|\^Jh"
    # Sequential single-key byte run (see _SEQUENTIAL_KEY_RUN_RE).
    r"|([A-Za-z])(?:[^A-Za-z\s]{0,3}\3){4,}"
    # Pure-separator markdown table row at the tail (``| --- | --- |``).
    # Acceptable mid-document but never as the closing line of a real document.
    r"|^\|(?:\s*-+\s*\|)+\s*$"
    # 3+ TAB characters interleaved with short tokens — Word style-sheet
    # tab-separated coordinate dump (e.g. ``pqráúÝ\tí\tî\tü\t$¡``).
    r"|(?:\t[^\t\n]{1,3}){3,}"
    # Single non-ASCII uppercase letter followed by 4 ASCII lowercase letters
    # then non-ASCII — Word handle reference (``Ífjop``, ``Üàpqr…``-style).
    # The pattern requires the surrounding chars to be high-byte to avoid
    # false-positives on legitimate accented words like ``Última``.
    r"|[\xc0-\xff][\xc0-\xff][A-Z]{1,2}[a-z]{1,4}[\xc0-\xff]"
    # Word conditional-format / named-range fragment: ``$Ifa$gd``, ``$Ifa$``,
    # ``Ifa$gd``.  These are field-code residuals that escape the wider
    # ``\$\$Ifa\$`` pattern when only a single ``$`` survives the encoding.
    r"|\$Ifa\$|Ifa\$gd|\bgd$"
    # ASCII letter run sandwiched between high-byte / punct bursts within a
    # short window — produces sequences like ``Xõeopq}ú3`` or
    # ``wx³Íáþ0Daz{`` where 3-4 letter pseudo-words are framed by binary
    # noise on both sides.
    r"|[\xc0-\xff][a-z]{2,4}[!-/{-~\xc0-\xff]\d"
    # 3-5 ASCII lowercase letters followed by ASCII punct then a high-byte
    # char (``eopq}ú``).
    r"|[a-z]{3,5}[!-/{-~][\xc0-\xff]"
    # High-byte char immediately followed by digit + uppercase + 2-3 lowercase
    # then a punctuation/high-byte char (``þ0Daz{``).
    r"|[\xc0-\xff]\d[A-Z][a-z]{2,3}[!-/{-~\xc0-\xff]"
)


def _truncate_tail_blob(paragraphs: list[str]) -> list[str]:
    """Drop any trailing binary-garbage blob from the paragraph list.

    After all per-paragraph filters have run, some DOC files still end with
    a block of Word stylesheet / drawing-object bytes that escaped the main
    filter.  These paragraphs look like ``h9(CJ^JaJh)^``, ``zz"z'z1z…``,
    ``´5CJ\\aJh``, etc.

    Strategy
    --------
    1. Scan the last ``_TAIL_BLOB_WINDOW`` paragraphs.
    2. Classify each paragraph as "tail-garbage" using a broader heuristic
       (looser than ``_is_binary_garbage`` — see ``_is_tail_garbage``).
    3. Find the last paragraph that is definitively NOT tail-garbage.
    4. If the blob after that paragraph is at least ``_TAIL_BLOB_THRESHOLD``
       paragraphs long, truncate at that position.

    The "last non-garbage" approach is used (rather than a strict contiguous
    run) because garbage paragraphs sometimes alternate with un-caught garbage
    paragraphs of different shapes.

    A paragraph is considered "tail-garbage" when:
    - ``_is_binary_garbage`` returns True, OR
    - it is short (≤ 120 chars) AND contains a known Word style-property
      token OR starts with a garbage-prefix pattern.
    """
    # Mexican legislative documents that end cleanly never have a binary blob
    # before the last paragraph.  All real text ends with a promulgation block
    # ("En cumplimiento de lo dispuesto…") or a signatory line ("Ciudad de
    # México, a …- Rúbrica.").  These always contain Spanish long words.
    _SIGNATORY_RE = re.compile(
        r"cumplimiento|Rúbrica|R\xfabrica|firman|firmas?"
        r"|[Pp]residenta?|[Ss]ecretari[oa]"
        r"|Ciudad de M\xe9xico|Ciudad de Mexico",
        re.IGNORECASE,
    )

    n = len(paragraphs)
    if n < _TAIL_BLOB_THRESHOLD:
        return paragraphs

    def _is_tail_garbage(text: str) -> bool:
        if _is_binary_garbage(text):
            return True
        if len(text) <= 120 and _TAIL_PARAGRAPH_GARBAGE_RE.search(text):
            return True
        if _is_garbage_table_row(text):
            return True
        return False

    # Look back through the last _TAIL_BLOB_WINDOW paragraphs.
    _TAIL_BLOB_WINDOW = 50
    window_start = max(0, n - _TAIL_BLOB_WINDOW)

    # Count garbage vs. good paragraphs in the window.
    window = paragraphs[window_start:]
    garbage_count = sum(1 for pp in window if _is_tail_garbage(pp))
    good_count = len(window) - garbage_count

    # If the window is dominated by garbage (> 60%), scan backward to find the
    # last definitely-good paragraph and truncate there.
    if good_count == 0 or garbage_count / len(window) > 0.6:
        last_good = -1
        for i in range(n - 1, window_start - 1, -1):
            if not _is_tail_garbage(paragraphs[i]):
                last_good = i
                break
        if last_good < 0:
            return paragraphs[:window_start]
        tail_length = n - (last_good + 1)
        if tail_length >= _TAIL_BLOB_THRESHOLD:
            return paragraphs[: last_good + 1]
        return paragraphs

    # Otherwise use the strict contiguous-run approach:
    # find the last paragraph that is definitely NOT garbage.
    last_good = -1
    for i in range(n - 1, window_start - 1, -1):
        if not _is_tail_garbage(paragraphs[i]):
            last_good = i
            break

    if last_good < 0:
        return paragraphs[:window_start]

    tail_length = n - (last_good + 1)
    # Use a lower threshold here than _TAIL_BLOB_THRESHOLD: even a single
    # garbage paragraph after the last real paragraph must be cut.  Mexican
    # federal laws always end with a promulgation block or signatory line —
    # any trailing garbage is unambiguous.
    if tail_length >= 1:
        return paragraphs[: last_good + 1]
    return paragraphs


def _truncate_repeated_short_tail(paragraphs: list[str]) -> list[str]:
    """Remove trailing runs of short-garbage paragraphs that escaped the main filters.

    Word 97-2003 stylesheet property strings (e.g. ``Jáh``, ``UIh``, ``bCJh``,
    ``CJh§\\h~7``) are short Word-internal style handles.  They slip through
    ``_is_binary_garbage`` because they are too short to trigger signal 3 and
    contain enough letters to satisfy signal 6.

    A paragraph is treated as a short-garbage candidate when:
    - It is ≤ 10 characters long, AND
    - It does NOT look like a legitimate derogation marker: it is either
      longer than 2 chars, or it contains at least one non-ASCII character
      (guards against cutting lone "." / ".." / "I" / "A" etc. which are
      valid in Mexican federal legislation).

    Strategy: starting from the last paragraph, walk backward as long as each
    paragraph is a short-garbage candidate.  If the resulting contiguous run
    is ≥ 5 paragraphs, truncate before it.  The threshold of 5 is chosen so
    that isolated short-token artifacts mid-document (which can legitimately
    appear after reform stamps) are not affected — only the repeated tail blob.
    """
    _MAX_LEN = 20
    _MIN_RUN = 5
    _TAIL_WINDOW = 40

    n = len(paragraphs)
    if n < _MIN_RUN:
        return paragraphs

    window_start = max(0, n - _TAIL_WINDOW)

    def _is_short_garbage_candidate(text: str) -> bool:
        if len(text) > _MAX_LEN:
            return False
        # Allow lone "." and ".." (derogation markers) and single ASCII letters.
        if len(text) <= 2 and all(ord(c) <= 0x7F for c in text):
            return False
        return True

    # Walk backward from the end through the tail window.
    cut_at = n
    for i in range(n - 1, window_start - 1, -1):
        if _is_short_garbage_candidate(paragraphs[i]):
            cut_at = i
        else:
            break

    if cut_at == n:
        return paragraphs  # no contiguous short-garbage at the tail

    tail_length = n - cut_at
    if tail_length < _MIN_RUN:
        return paragraphs  # run too short to be confident it is garbage

    return paragraphs[:cut_at]


def _extract_doc_paragraphs(doc_bytes: bytes) -> list[str]:
    """Extract plain-text paragraphs from a Word 97-2003 (.doc) OLE2 file.

    The WordDocument stream stores the full document text with ``\\r``
    (0x0D) as the paragraph separator.  We decode as latin-1 (Diputados
    uses Windows-1252, a superset of latin-1), strip C0/C1 control chars,
    normalize to NFC Unicode, split on ``\\r``, and discard:
      - empty paragraphs
      - OLE2 binary-garbage paragraphs (field-code tokens or non-Spanish
        high-byte majority — see ``_is_binary_garbage``)
      - Word field codes (PAGE, NUMPAGES, EMBED …)
      - Diputados institutional boilerplate / "Última Reforma" footer lines

    Word 97-2003 tables are detected by the presence of BEL (\\x07) cell
    separators and converted to Markdown pipe tables inline.
    """
    try:
        import olefile
    except ImportError as exc:
        raise ImportError(
            "olefile is required for .doc parsing. "
            "Install it with: uv add olefile"
        ) from exc

    ole = olefile.OleFileIO(io.BytesIO(doc_bytes))
    try:
        if not ole.exists("WordDocument"):
            raise ValueError("OLE2 file has no 'WordDocument' stream — not a valid .doc")
        raw = ole.openstream("WordDocument").read()
    finally:
        ole.close()

    # Decode as latin-1 (Windows-1252 is a superset; errors='replace' is a
    # safety net for stray bytes outside the BMP).
    # NOTE: do NOT strip control chars yet — we need BEL (0x07) for table
    # detection in the loop below.
    raw_text = raw.decode("latin-1", errors="replace")

    # Pre-pass: group consecutive multi-paragraph table rows into combined
    # segments before the per-paragraph loop.
    #
    # Word 97-2003 stores table rows in two ways:
    #   1. Inline: a single \r-paragraph contains multiple rows separated by
    #      \x07\x07 (e.g. "Header\x07Col2\x07\x07Row1\x07Val1\x07\x07…").
    #      _word_table_to_markdown handles these already.
    #   2. Multi-paragraph: each row is its own \r-paragraph.  The first row
    #      has \x07 cell separators but does NOT start with \x07\x07.
    #      Subsequent rows start with \x07\x07 (the row-end mark from the
    #      previous row) followed by cell content.
    #
    # This pre-pass detects runs of multi-paragraph rows (first para has \x07
    # but no \x07\x07 prefix; subsequent paras start with \x07\x07) and
    # concatenates them into a single inline-format segment that
    # _word_table_to_markdown can handle.
    raw_paras = raw_text.split("\r")
    merged_paras: list[str] = []
    i = 0
    while i < len(raw_paras):
        p = raw_paras[i]
        # Detect the start of a multi-paragraph table: paragraph has \x07
        # but does NOT start with \x07\x07 (not already a continuation row).
        if "\x07" in p and not p.startswith("\x07\x07"):
            # Collect continuation rows: subsequent paragraphs that start
            # with \x07\x07.
            run = [p]
            j = i + 1
            while j < len(raw_paras) and raw_paras[j].startswith("\x07\x07"):
                run.append(raw_paras[j])
                j += 1
            if len(run) > 1:
                # Multiple rows found — reconstruct as a single inline-format
                # segment: strip the leading \x07\x07 from continuation rows
                # (those bytes were the prior row's terminator in the split
                # format) and rejoin using \x07\x07 as row separator.
                segments = [run[0]] + [r.lstrip("\x07") for r in run[1:]]
                merged_paras.append("\x07\x07".join(segments) + "\x07\x07")
            else:
                merged_paras.append(p)
            i = j
        else:
            merged_paras.append(p)
            i += 1

    paragraphs: list[str] = []
    for raw_para in merged_paras:
        # Handle Word table rows BEFORE stripping control characters.
        # BEL (U+0007) is the Word cell separator; a paragraph that contains
        # at least one BEL is a table row (or part of a multi-row table block).
        if "\x07" in raw_para:
            table_md = _word_table_to_markdown(raw_para)
            if table_md:
                # Emit the whole pipe table as a single paragraph.  The block
                # builder treats it as one body paragraph, and the Markdown
                # renderer passes it through verbatim.
                paragraphs.append(table_md)
            # Whether or not we got a valid table, skip the normal
            # per-character-class processing for this segment.
            continue

        # Strip C0/C1 control characters from non-table segments.
        para = _CONTROL_RE.sub("", raw_para)
        para = unicodedata.normalize("NFC", para).strip()
        if not para:
            continue
        if _is_binary_garbage(para):
            continue
        if _DOC_FIELD_RE.match(para):
            continue
        if _DIPUTADOS_BOILERPLATE_RE.search(para):
            continue
        if _LAST_REFORM_FOOTER_RE.match(para):
            continue
        if _DOF_PAGE_HEADER_RE.search(para):
            continue
        paragraphs.append(para)

    # Trim trailing single-character artifacts (lone letters, underscores, etc.)
    # that are not period/full-stop markers.  OLE2 binary data at the end of
    # the WordDocument stream sometimes leaves 1-char relics after the main
    # text that pass all paragraph-level checks.  Legitimate single-char
    # paragraphs in Mexican federal law are only derogation dots (".") or
    # double-dots ("..").
    while paragraphs and len(paragraphs[-1]) == 1 and paragraphs[-1] not in {".", ","}:
        paragraphs.pop()

    # Tail-blob truncation: drop any trailing contiguous run of binary-garbage
    # paragraphs that slipped through the per-paragraph filter.
    paragraphs = _truncate_tail_blob(paragraphs)

    # Repeated-short-tail truncation: drop trailing runs of identical very
    # short paragraphs (≤ 10 chars).  Covers stylesheet handle tokens like
    # "Jáh" that contain one Spanish accent and therefore pass _is_binary_garbage
    # signal 3 and signal 6.  Three or more consecutive identical short
    # paragraphs at the document end are never legitimate legislative text.
    paragraphs = _truncate_repeated_short_tail(paragraphs)

    # Second-pass tail-blob truncation: after the repeated-short-tail step
    # removes "anchor" paragraphs (e.g. "Jáh") that were blocking the first
    # pass, re-run to drop the exposed shorter garbage tail.
    paragraphs = _truncate_tail_blob(paragraphs)

    # Final micro-trim: remove any remaining artifacts at the tail that are
    # clearly not legitimate legislative text.  Two criteria:
    #
    # 1. Very-short (≤ 5-char) items that are not derogation dots or fracción
    #    markers.  Covers edge cases like "DEFRq" (Word style-name stub, 5 chars,
    #    pure ASCII) that slip through the binary-garbage and tail-blob filters.
    # 2. Items up to 50 chars that are flagged by _is_binary_garbage when isolated
    #    at the tail.  This catches residual OLE2 coordinate/style-dump fragments
    #    whose garbage nature was masked by surrounding context during the window
    #    scan but is clear once all neighbours are removed.  We limit this to the
    #    last 3 paragraphs to avoid over-cutting near legitimate short text.
    #
    # Legitimate ≤ 5-char tail paragraphs in Mexican legislation:
    #   "."  ".."  — derogation dots
    #   "I."  "II." etc. — very short fracción markers
    _LEGIT_SHORT = {".", "..", "I.", "II.", "III.", "IV.", "V."}
    while (
        paragraphs
        and len(paragraphs[-1]) <= 5
        and paragraphs[-1] not in _LEGIT_SHORT
    ):
        paragraphs.pop()

    # Pass 2: drop up to 3 tail paragraphs that the broader _TAIL_PARAGRAPH_GARBAGE_RE
    # considers junk when evaluated in isolation.  This catches residual OLE2 / Word
    # coordinate-dump fragments (e.g. "U¾U¿UV\tVúVûVÿVWW", "ijkº»Éæ…hm") whose
    # garbage nature was masked by surrounding context during the window scan but is
    # clear once all neighbours have been removed.  We check both _is_binary_garbage
    # (broad) and _TAIL_PARAGRAPH_GARBAGE_RE (tail-specific) with the ≤ 120-char
    # guard to avoid false positives on any long paragraph that survives to this point.
    for _ in range(3):
        if not paragraphs:
            break
        last = paragraphs[-1]
        is_junk = (
            _is_binary_garbage(last)
            or (len(last) <= 120 and _TAIL_PARAGRAPH_GARBAGE_RE.search(last))
            or _is_garbage_table_row(last)
        )
        if is_junk:
            paragraphs.pop()
        else:
            break

    return paragraphs


def _split_into_lines(pages: list[str]) -> list[str | None]:
    """Flatten pages into lines, preserving paragraph breaks as ``None``.

    Diputados PDFs repeat institutional banners on every page (the document
    title in all caps, "Cámara de Diputados…" footers, pagination markers,
    "Última Reforma DOF…" stamps). We drop them. Blank lines from the PDF
    survive as ``None`` so the block builder can use them as paragraph
    separators (otherwise every visual line wrap looks like a new para).
    Page boundaries are forced into ``None`` so a paragraph never silently
    spans pages of different content.
    """
    # First pass: count line frequencies so per-page chrome can be detected
    # even when the static patterns above don't catch it.
    counts: dict[str, int] = {}
    for page_text in pages:
        seen: set[str] = set()
        for raw in page_text.splitlines():
            line = _clean_line(raw)
            if line and line not in seen:
                seen.add(line)
                counts[line] = counts.get(line, 0) + 1
    repeating_chrome = {
        line for line, count in counts.items() if len(pages) > 2 and count >= 3
    }

    out: list[str | None] = []
    for page_idx, page_text in enumerate(pages):
        last_emitted_blank = True  # collapse leading blanks per page
        for raw in page_text.splitlines():
            line = _clean_line(raw)
            if not line:
                if not last_emitted_blank:
                    out.append(None)
                    last_emitted_blank = True
                continue
            if _PAGINATION_RE.match(line):
                continue
            if _LAST_REFORM_FOOTER_RE.match(line):
                continue
            if _DIPUTADOS_BOILERPLATE_RE.search(line):
                continue
            if line in repeating_chrome:
                continue
            out.append(line)
            last_emitted_blank = False
        # Force a paragraph break at every page boundary.
        if page_idx < len(pages) - 1 and not last_emitted_blank:
            out.append(None)

    return out


def _articulo_id(token: str) -> str:
    """Normalize an article identifier into a slug (e.g. '1o' → '1o', 'Único' → 'unico')."""
    t = token.lower().strip()
    t = re.sub(r"\s+[ºo°]$", "o", t)
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return t.replace(" ", "")


def _article_heading_text(num: str, sep: str | None) -> str:
    """Render a clean article heading from the captured number + separator."""
    head = f"Artículo {num.strip()}"
    if sep:
        sep = sep.strip()
        # Preserve the punctuation style the source used so the heading reads
        # naturally (some articles use ".-", others just ".").
        if sep.startswith("."):
            head += sep
        else:
            head += f" {sep}"
    else:
        head += "."
    return head


def _diputados_blocks(envelope: dict[str, Any]) -> list[Block]:
    """Build Block/Version trees from a Diputados PDF envelope.

    Single-snapshot: each Block has exactly one Version dated to the law's
    most recent DOF reform (the date in the Markdown frontmatter and on
    the resulting git commit). Real reform-by-reform history requires DOF
    integration and lives behind that adapter.
    """
    pdf_b64 = envelope.get("pdf_b64")
    if not pdf_b64:
        raise ValueError("Diputados envelope is missing 'pdf_b64'")
    pdf_bytes = base64.b64decode(pdf_b64)

    norm_id = envelope["norm_id"]
    pub_date = date.fromisoformat(envelope["publication_date"])
    last_reform = envelope.get("last_reform_date")
    effective_date = (
        date.fromisoformat(last_reform) if last_reform else pub_date
    )

    pages = _extract_pdf_text(pdf_bytes)
    line_stream = _split_into_lines(pages)

    blocks: list[Block] = []
    article_seq = 0  # used to disambiguate repeated "Artículo Único" in transitorios

    # Once we see "ARTÍCULOS TRANSITORIOS DE DECRETOS DE REFORMA" we switch into
    # decreto-tail mode.  In this mode _ARTICULO_RE matches are NOT parsed as
    # main-law article headings; they are body paragraphs within their decree block.
    in_decreto_tail: bool = False

    # State for the article currently being built
    current_article_id: str | None = None
    current_article_title: str | None = None
    current_article_paragraphs: list[Paragraph] = []
    pending_body_lines: list[str] = []
    pending_kind: str | None = None  # "body" | "stamp"

    def flush_pending_paragraph() -> None:
        """Merge accumulated lines into a single paragraph and tag it."""
        nonlocal pending_kind
        if not pending_body_lines:
            pending_kind = None
            return
        text = " ".join(pending_body_lines).strip()
        pending_body_lines.clear()
        kind = pending_kind or "body"
        pending_kind = None
        if not text or current_article_id is None:
            return
        css = "nota_pie" if kind == "stamp" else "parrafo"
        current_article_paragraphs.append(Paragraph(css_class=css, text=text))

    def flush_article() -> None:
        nonlocal current_article_id, current_article_title, current_article_paragraphs
        flush_pending_paragraph()
        if current_article_id is None:
            return
        blocks.append(
            Block(
                id=current_article_id,
                block_type="article",
                title=current_article_title or current_article_id,
                versions=(
                    Version(
                        norm_id=norm_id,
                        publication_date=pub_date,
                        effective_date=effective_date,
                        paragraphs=tuple(current_article_paragraphs),
                    ),
                ),
            )
        )
        current_article_id = None
        current_article_title = None
        current_article_paragraphs = []

    section_counter = 0

    def emit_section(css_class: str, text: str) -> None:
        nonlocal section_counter
        flush_article()
        section_counter += 1
        blocks.append(
            Block(
                id=f"sec-{section_counter}",
                block_type="section",
                title=text,
                versions=(
                    Version(
                        norm_id=norm_id,
                        publication_date=pub_date,
                        effective_date=effective_date,
                        paragraphs=(Paragraph(css_class=css_class, text=text),),
                    ),
                ),
            )
        )

    for entry in line_stream:
        if entry is None:
            # PDF blank line / page break → end the running paragraph
            flush_pending_paragraph()
            continue

        line = entry

        # Check for decreto-tail trigger FIRST (before general _TRANSITORIOS_RE
        # so we can set the flag and still emit the section heading).
        if _DECRETO_TAIL_TRIGGER_RE.match(line):
            in_decreto_tail = True
            emit_section("titulo_tit", line)
            continue

        # In decreto-tail mode: handle DECRETO headings and sub-headings specially.
        if in_decreto_tail:
            if _DECRETO_HEADER_RE.match(line):
                # Each "DECRETO por el que…" line becomes a sub-section heading
                # that groups the following transitorios.
                emit_section("seccion_tit", line)
                continue
            if _DECRETO_TRANSITORIOS_RE.match(line):
                # "TRANSITORIOS" / "TRANSITORIO" within a decree block becomes a
                # sub-heading (not the outer main-law transitorios section).
                emit_section("subseccion_tit", line)
                continue
            # All other lines in decreto-tail: fall through to normal body-text
            # accumulation below, but skip the _ARTICULO_RE article-heading branch.
            # Stamps, fracciones, and apartados are still handled as usual.
            if current_article_id is None:
                # Attach to the last section block as a body paragraph by creating
                # a synthetic article container so text is not silently dropped.
                article_seq += 1
                current_article_id = f"decreto-body-{article_seq}"
                current_article_title = ""
                current_article_paragraphs = []
            is_stamp = bool(_REFORM_STAMP_RE.match(line))
            if pending_body_lines:
                switching_kind = (
                    (pending_kind == "stamp" and not is_stamp)
                    or (pending_kind == "body" and is_stamp)
                )
                if switching_kind:
                    flush_pending_paragraph()
            if is_stamp:
                pending_kind = "stamp"
            elif pending_kind is None:
                pending_kind = "body"
            pending_body_lines.append(line)
            continue

        if _TRANSITORIOS_RE.match(line):
            emit_section("titulo_tit", line)
            continue
        if _LIBRO_RE.match(line) or _TITULO_RE.match(line):
            emit_section("titulo_tit", line)
            continue
        if _CAPITULO_RE.match(line):
            emit_section("capitulo_tit", line)
            continue
        if _SECCION_RE.match(line):
            emit_section("seccion", line)
            continue

        m = _ARTICULO_RE.match(line)
        if m:
            flush_article()
            article_seq += 1
            num = m.group("num")
            sep = m.group("sep")
            rest = m.group("rest").strip()
            slug = _articulo_id(num)
            # Disambiguate repeated "Artículo Único" entries in transitorios
            # so each gets a unique block id (otherwise frontmatter slugs collide).
            current_article_id = f"art-{slug}-{article_seq}"
            current_article_title = _article_heading_text(num, sep)
            current_article_paragraphs = [
                Paragraph(css_class="articulo", text=current_article_title)
            ]
            if rest:
                # The article's first body line was on the same visual line as
                # the heading. Seed the pending paragraph with it.
                pending_body_lines.append(rest)
            continue

        if current_article_id is None:
            # Free-form preamble (decree title, promulgation block) — drop.
            continue

        is_stamp = bool(_REFORM_STAMP_RE.match(line))
        is_sub_marker = bool(
            _APARTADO_RE.match(line)
            or _FRACCION_RE.match(line)
            or _INCISO_RE.match(line)
        )

        # Force a paragraph break when:
        #   - the running paragraph is a stamp and we just hit body text (or vice versa)
        #   - the new line is a sub-article marker (Apartado / fracción / inciso)
        if pending_body_lines:
            switching_kind = (
                (pending_kind == "stamp" and not is_stamp)
                or (pending_kind == "body" and is_stamp)
            )
            if switching_kind or is_sub_marker:
                flush_pending_paragraph()

        if is_stamp:
            pending_kind = "stamp"
        elif pending_kind is None:
            pending_kind = "body"
        pending_body_lines.append(line)

    flush_article()
    return blocks


def _diputados_doc_blocks(envelope: dict[str, Any]) -> list[Block]:
    """Build Block/Version trees from a Diputados DOC envelope.

    Works identically to ``_diputados_blocks`` but consumes the raw
    ``doc_b64`` bytes through ``_extract_doc_paragraphs`` instead of
    pdfplumber.  DOC paragraphs are already cleanly separated by ``\\r``
    so there is no need for the PDF-specific ``_split_into_lines`` page-
    merging logic; we feed the paragraphs directly into the same block-
    builder state machine.
    """
    doc_b64 = envelope.get("doc_b64")
    if not doc_b64:
        raise ValueError("Diputados DOC envelope is missing 'doc_b64'")
    doc_bytes = base64.b64decode(doc_b64)

    norm_id = envelope["norm_id"]
    pub_date = date.fromisoformat(envelope["publication_date"])
    last_reform = envelope.get("last_reform_date")
    effective_date = (
        date.fromisoformat(last_reform) if last_reform else pub_date
    )

    paragraphs_raw = _extract_doc_paragraphs(doc_bytes)

    # Pre-scan: determine whether any numeric article (1o., 2., etc.) exists in
    # the document.  If so, any leading word-ordinal articles (PRIMERO, SEGUNDO…)
    # belong to the issuing decree, not to the main law body.
    _has_numeric_article = any(
        (m2 := _ARTICULO_RE.match(p2)) is not None
        and _NUMERIC_ARTICULO_NUM_RE.match(m2.group("num"))
        for p2 in paragraphs_raw
    )

    blocks: list[Block] = []
    article_seq = 0

    # Once we see "ARTÍCULOS TRANSITORIOS DE DECRETOS DE REFORMA" we switch into
    # decreto-tail mode.  In this mode _ARTICULO_RE matches are NOT parsed as
    # main-law article headings; they are body paragraphs within their decree block.
    in_decreto_tail: bool = False

    # True while we are processing the issuing-decree head articles (PRIMERO,
    # SEGUNDO, etc.) that precede the main-law body.  Only set when the first
    # article of the document is word-ordinal AND numeric articles exist later.
    # Cleared as soon as we encounter the first numeric article.
    in_issuing_decree: bool = False
    _issuing_decree_section_emitted: bool = False

    current_article_id: str | None = None
    current_article_title: str | None = None
    current_article_paragraphs: list[Paragraph] = []
    pending_body_lines: list[str] = []
    pending_kind: str | None = None  # "body" | "stamp"

    def flush_pending_paragraph() -> None:
        nonlocal pending_kind
        if not pending_body_lines:
            pending_kind = None
            return
        text = " ".join(pending_body_lines).strip()
        pending_body_lines.clear()
        kind = pending_kind or "body"
        pending_kind = None
        if not text or current_article_id is None:
            return
        # Markdown pipe-table paragraphs (produced by _word_table_to_markdown)
        # start with "| " — assign the "table" css_class so they render verbatim.
        if kind == "body" and text.startswith("| "):
            css = "table"
        elif kind == "stamp":
            css = "nota_pie"
        else:
            css = "parrafo"
        current_article_paragraphs.append(Paragraph(css_class=css, text=text))

    def flush_article() -> None:
        nonlocal current_article_id, current_article_title, current_article_paragraphs
        flush_pending_paragraph()
        if current_article_id is None:
            return
        blocks.append(
            Block(
                id=current_article_id,
                block_type="article",
                title=current_article_title or current_article_id,
                versions=(
                    Version(
                        norm_id=norm_id,
                        publication_date=pub_date,
                        effective_date=effective_date,
                        paragraphs=tuple(current_article_paragraphs),
                    ),
                ),
            )
        )
        current_article_id = None
        current_article_title = None
        current_article_paragraphs = []

    section_counter = 0

    def emit_section(css_class: str, text: str) -> None:
        nonlocal section_counter
        flush_article()
        section_counter += 1
        blocks.append(
            Block(
                id=f"sec-{section_counter}",
                block_type="section",
                title=text,
                versions=(
                    Version(
                        norm_id=norm_id,
                        publication_date=pub_date,
                        effective_date=effective_date,
                        paragraphs=(Paragraph(css_class=css_class, text=text),),
                    ),
                ),
            )
        )

    for para in paragraphs_raw:
        # Check for decreto-tail trigger FIRST (before general _TRANSITORIOS_RE
        # so we can set the flag and still emit the section heading).
        if _DECRETO_TAIL_TRIGGER_RE.match(para):
            in_decreto_tail = True
            emit_section("titulo_tit", para)
            continue

        # In decreto-tail mode: handle DECRETO headings and sub-headings specially.
        if in_decreto_tail:
            if _DECRETO_HEADER_RE.match(para):
                # Each "DECRETO por el que…" line becomes a sub-section heading
                # that groups the following transitorios.
                emit_section("seccion_tit", para)
                continue
            if _DECRETO_TRANSITORIOS_RE.match(para):
                # "TRANSITORIOS" / "TRANSITORIO" within a decree block becomes a
                # sub-heading (not the outer main-law transitorios section).
                emit_section("subseccion_tit", para)
                continue
            # All other lines in decreto-tail: accumulate as body text.
            # Stamps are still tagged as nota_pie; _ARTICULO_RE is intentionally
            # NOT tested here so ordinal article lines stay as plain paragraphs.
            if current_article_id is None:
                # Attach to the last section block by creating a synthetic article
                # container so the text is not silently dropped.
                article_seq += 1
                current_article_id = f"decreto-body-{article_seq}"
                current_article_title = ""
                current_article_paragraphs = []
            # Pipe-table paragraphs: emit directly (same logic as general path).
            if para.startswith("| "):
                flush_pending_paragraph()
                current_article_paragraphs.append(Paragraph(css_class="table", text=para))
                continue
            is_stamp = bool(_REFORM_STAMP_RE.match(para))
            if pending_body_lines:
                switching_kind = (
                    (pending_kind == "stamp" and not is_stamp)
                    or (pending_kind == "body" and is_stamp)
                )
                if switching_kind:
                    flush_pending_paragraph()
            if is_stamp:
                pending_kind = "stamp"
            elif pending_kind is None:
                pending_kind = "body"
            pending_body_lines.append(para)
            # DOC paragraphs are already complete units — flush body immediately.
            if not is_stamp:
                flush_pending_paragraph()
            continue

        if _TRANSITORIOS_RE.match(para):
            emit_section("titulo_tit", para)
            continue
        if _LIBRO_RE.match(para) or _TITULO_RE.match(para):
            emit_section("titulo_tit", para)
            continue
        if _CAPITULO_RE.match(para):
            emit_section("capitulo_tit", para)
            continue
        if _SECCION_RE.match(para):
            emit_section("seccion", para)
            continue

        m = _ARTICULO_RE.match(para)
        if m:
            num = m.group("num")
            sep = m.group("sep")
            rest = m.group("rest").strip()
            is_word_ordinal = bool(_WORD_ORDINAL_NUM_RE.match(num))
            is_numeric = bool(_NUMERIC_ARTICULO_NUM_RE.match(num))

            # Issuing-decree detection (law-START).
            # If the very first ARTICLE of this document is word-ordinal AND
            # the law contains numeric articles later, the word-ordinal articles
            # are part of the issuing decree, not the main law.
            # "First article" means current_article_id is None and no previous
            # art- block has been emitted (section headings don't count).
            _no_article_emitted = not any(
                b.block_type == "article" for b in blocks
            )
            if (
                not in_issuing_decree
                and not _issuing_decree_section_emitted
                and is_word_ordinal
                and _has_numeric_article
                and current_article_id is None
                and _no_article_emitted
            ):
                # Emit an issuing-decree section heading before the first decree article.
                in_issuing_decree = True
                emit_section("decreto_tit", "Decreto que expide esta Ley")
                _issuing_decree_section_emitted = True

            if in_issuing_decree and is_numeric:
                # First numeric article encountered — the issuing decree is over.
                in_issuing_decree = False

            if in_issuing_decree:
                # Word-ordinal article inside the issuing-decree prefix — treat as
                # body text, NOT as a main-law article heading (same pattern as
                # decreto-tail handling for transitorios of reform decrees).
                if current_article_id is None:
                    article_seq += 1
                    current_article_id = f"decreto-head-{article_seq}"
                    current_article_title = ""
                    current_article_paragraphs = []
                # Include the article heading line itself as body text so it reads as
                # a plain paragraph (e.g. "Artículo PRIMERO.- Se expide la Ley X…").
                heading_text = _article_heading_text(num, sep)
                body_line = f"{heading_text} {rest}".strip() if rest else heading_text
                flush_pending_paragraph()
                pending_body_lines.append(body_line)
                pending_kind = "body"
                flush_pending_paragraph()
                continue

            flush_article()
            article_seq += 1
            slug = _articulo_id(num)
            current_article_id = f"art-{slug}-{article_seq}"
            current_article_title = _article_heading_text(num, sep)
            current_article_paragraphs = [
                Paragraph(css_class="articulo", text=current_article_title)
            ]
            if rest:
                # The article's first body paragraph was on the same DOC paragraph
                # as the heading.  Seed the pending paragraph with it and mark
                # kind explicitly so it won't be merged with a following stamp.
                pending_body_lines.append(rest)
                pending_kind = "body"
            continue

        if current_article_id is None:
            # Free-form preamble (decree title, promulgation block) — drop.
            continue

        # Pipe-table paragraphs produced by _word_table_to_markdown start with
        # "| ".  They must be emitted as standalone paragraphs so they are not
        # joined with preceding body text by flush_pending_paragraph's " ".join.
        # Force-flush any accumulated content first, then emit the table
        # directly into current_article_paragraphs.
        if para.startswith("| "):
            flush_pending_paragraph()
            if current_article_id is not None:
                current_article_paragraphs.append(Paragraph(css_class="table", text=para))
            continue

        is_stamp = bool(_REFORM_STAMP_RE.match(para))
        is_sub_marker = bool(
            _APARTADO_RE.match(para)
            or _FRACCION_RE.match(para)
            or _INCISO_RE.match(para)
        )

        # Force a paragraph break when:
        #   - the running paragraph is a stamp and we just hit body text (or vice versa)
        #   - the new line is a sub-article marker (Apartado / fracción / inciso)
        # In DOC mode each source paragraph is already its own unit so we flush
        # on every new paragraph rather than waiting for a blank separator.
        if pending_body_lines:
            switching_kind = (
                (pending_kind == "stamp" and not is_stamp)
                or (pending_kind == "body" and is_stamp)
            )
            if switching_kind or is_sub_marker:
                flush_pending_paragraph()

        if is_stamp:
            pending_kind = "stamp"
        elif pending_kind is None:
            pending_kind = "body"
        pending_body_lines.append(para)

        # Each DOC paragraph is already complete — flush immediately so that
        # body paragraphs don't merge.  We do not flush stamp paragraphs here
        # so that multi-date stamps ("DOF 04-12-2006, 10-06-2011") can collect
        # if they ever span two raw paragraphs (uncommon, but defensive).
        if not is_stamp:
            flush_pending_paragraph()

    flush_article()
    return blocks


def _diputados_metadata(envelope: dict[str, Any], norm_id: str) -> NormMetadata:
    title = envelope["title"]
    pub_date = date.fromisoformat(envelope["publication_date"])
    last_reform = envelope.get("last_reform_date")
    last_modified = (
        date.fromisoformat(last_reform) if last_reform else None
    )
    rank = Rank(envelope.get("rank") or "ley")

    pdf_url = envelope.get("pdf_url", "")
    doc_url = envelope.get("doc_url", "")
    # source points to the DOC when available (primary format), else falls back to PDF.
    source_url = doc_url or pdf_url

    extra: list[tuple[str, str]] = [
        ("source_name", "diputados"),
        ("abbrev", envelope["abbrev"]),
    ]
    if pdf_url:
        extra.append(("pdf_url", pdf_url))
    if doc_url:
        extra.append(("doc_url", doc_url))
    if last_reform:
        extra.append(("last_reform_dof", last_reform))

    return NormMetadata(
        title=title,
        short_title=title.split(",")[0][:120].strip(),
        identifier=norm_id,
        country="mx",
        rank=rank,
        publication_date=pub_date,
        status=NormStatus.IN_FORCE,
        department="Cámara de Diputados",
        source=source_url,
        last_modified=last_modified,
        pdf_url=pdf_url or None,
        extra=tuple(extra),
    )


# ── DOF reform extraction ──────────────────────────────────────────────


def _stamp_commit_type(text: str) -> str:
    """Infer a commit-type string from the wording of a Diputados reform stamp.

    Returns one of: "reforma", "adicion", "derogacion", "fe_de_erratas".
    """
    for pattern, commit_type in _STAMP_TYPE_MAP:
        if pattern.search(text):
            return commit_type
    return _STAMP_DEFAULT_TYPE


def _extract_dof_reforms_from_blocks(
    blocks: list[Block],
    norm_id: str,
    pub_date: date,
) -> list[Reform]:
    """Scan nota_pie paragraphs in blocks and return one Reform per unique DOF date.

    Each ``nota_pie`` paragraph (reform stamp) carries one or more DOF dates,
    e.g. ``"Párrafo reformado DOF 04-12-2006, 10-06-2011"``.  We collect every
    unique date, determine the dominant commit type for that date across all stamps,
    record which block IDs were mentioned in stamps for that date, and return one
    :class:`~legalize.models.Reform` per date sorted chronologically.

    The *first* Reform is dated to the law's original ``publication_date`` so the
    bootstrap commit corresponds to the law's enactment (not the first reform).
    Subsequent reforms are real DOF amendment events.

    Reform ``norm_id``s use the pattern ``{norm_id}-DOF-{YYYY-MM-DD}`` so each
    produces a unique ``Source-Id`` trailer and idempotency checks work correctly.
    """
    # date → (set of block_ids, list of inferred commit types)
    date_blocks: dict[date, list[str]] = {}
    date_types: dict[date, list[str]] = {}

    for block in blocks:
        block_id = block.id
        for version in block.versions:
            for para in version.paragraphs:
                if para.css_class != "nota_pie":
                    continue
                text = para.text
                # _DOF_DATE_RE captures the date group after each "DOF" keyword.
                # Each group may contain multiple comma-separated dates, e.g.
                # "04-12-2006, 10-06-2011".  Extract individual date tokens.
                date_groups = _DOF_DATE_RE.findall(text)
                if not date_groups:
                    continue
                raw_dates = [
                    token
                    for group in date_groups
                    for token in _DATE_TOKEN_RE.findall(group)
                ]
                if not raw_dates:
                    continue
                commit_type = _stamp_commit_type(text)
                for raw_date in raw_dates:
                    try:
                        day, month, year = raw_date.split("-")
                        stamp_date = date(int(year), int(month), int(day))
                    except (ValueError, TypeError):
                        logger.debug("Could not parse DOF date %r in %s", raw_date, norm_id)
                        continue
                    if stamp_date not in date_blocks:
                        date_blocks[stamp_date] = []
                    if block_id not in date_blocks[stamp_date]:
                        date_blocks[stamp_date].append(block_id)
                    date_types.setdefault(stamp_date, []).append(commit_type)

    if not date_blocks:
        # No stamps found — return a single bootstrap reform at publication date.
        return [
            Reform(
                date=pub_date,
                norm_id=norm_id,
                affected_blocks=(),
            )
        ]

    reforms: list[Reform] = []

    # Bootstrap reform: the law's original publication (before any DOF amendments).
    # Always first, dated to pub_date.
    reforms.append(
        Reform(
            date=pub_date,
            norm_id=norm_id,
            affected_blocks=(),
        )
    )

    # One reform per unique DOF date (skipping the publication date itself if it
    # appears in stamps — it would duplicate the bootstrap).
    for stamp_date in sorted(date_blocks.keys()):
        if stamp_date == pub_date:
            # Absorb into the bootstrap reform's affected_blocks rather than
            # emitting a duplicate reform for the same date.
            reforms[0] = Reform(
                date=reforms[0].date,
                norm_id=reforms[0].norm_id,
                affected_blocks=tuple(date_blocks[stamp_date]),
            )
            continue
        reforms.append(
            Reform(
                date=stamp_date,
                norm_id=f"{norm_id}-DOF-{stamp_date.isoformat()}",
                affected_blocks=tuple(date_blocks[stamp_date]),
            )
        )

    return reforms


# ── Public parser classes ──────────────────────────────────────────────


class MXTextParser(TextParser):
    """Parse Mexican consolidated text. Dispatches per source via envelope."""

    def parse_text(self, data: bytes) -> list[Any]:
        envelope = _decode_envelope(data)
        if envelope["source"] == "diputados":
            # Dispatch on source_format: "doc" (default) or "pdf" (legacy/fallback).
            fmt = envelope.get("source_format", "doc")
            if fmt == "doc":
                return _diputados_doc_blocks(envelope)
            return _diputados_blocks(envelope)
        raise NotImplementedError(
            f"MX text parser not wired for source '{envelope['source']}'."
        )

    def extract_reforms(self, data: bytes) -> list[Reform]:
        """Extract per-DOF-date reform timeline from consolidated Diputados text.

        Scans every ``nota_pie`` paragraph for ``DOF DD-MM-YYYY`` stamp patterns
        and groups them by date.  Each unique date becomes one
        :class:`~legalize.models.Reform` entry so ``legalize commit`` can produce
        one git commit per DOF reform event.

        Only implemented for the Diputados source.  Other sources fall through to
        the generic block-version extractor.
        """
        envelope = _decode_envelope(data)
        if envelope["source"] != "diputados":
            # Generic fallback for non-Diputados sources.
            from legalize.transformer.xml_parser import extract_reforms as _generic
            return _generic(self.parse_text(data))

        norm_id = envelope["norm_id"]
        pub_date_str = envelope.get("publication_date", "")
        try:
            pub_date = date.fromisoformat(pub_date_str)
        except (ValueError, TypeError):
            pub_date = date(1900, 1, 1)

        blocks = self.parse_text(data)
        return _extract_dof_reforms_from_blocks(blocks, norm_id, pub_date)


class MXMetadataParser(MetadataParser):
    """Parse Mexican norm metadata. Dispatches per source via envelope."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        envelope = _decode_envelope(data)
        if envelope["source"] == "diputados":
            return _diputados_metadata(envelope, norm_id)
        raise NotImplementedError(
            f"MX metadata parser not wired for source '{envelope['source']}'."
        )
