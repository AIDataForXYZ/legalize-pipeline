"""LLM client for difficult amendment cases (PLAN-STAGE-C.md §module 3).

Design goals (enforced by the user's "save money" policy):

  - Regex path is the default. This client is ONLY invoked on patches with
    confidence < 0.9. Delete verbs never reach it.
  - Disk cache keyed by prompt hash: the same ambiguous case never costs
    twice. Re-running the fidelity loop after a parser tweak only bills
    new cache-misses.
  - Minimalist prompts: ~400 chars of anchor-context + the modifier
    excerpt, not the full base Markdown. Keeps input tokens ~500 not
    ~5000.
  - Model escalation ladder, cheapest first:
        openai/gpt-oss-20b   ($0.10/$0.50 per MTok)
        openai/gpt-oss-120b  ($0.15/$0.75)
        llama-3.3-70b-versatile ($0.59/$0.79)
    A call fails over to the next rung only if the cheaper one returns
    confidence < 0.8 or an invalid JSON shape.
  - Two entry points:
        parse_difficult_case -- produce an AmendmentPatch from scratch
        verify               -- spot-check a regex-produced patch and,
                                if wrong, return a corrected one

Backends:
  - groq   (OpenAI-compatible REST at api.groq.com/openai/v1)
  - ollama (local, OpenAI-compatible wrapper at localhost:11434/v1)

Both speak the same Chat Completions schema so we use the same HTTP code
path with different base URLs + auth.

Test strategy: unit tests stub the transport with the `responses` library;
they never make a live call. A separate smoke script (scripts/groq_smoke.py)
hits the real API with GROQ_API_KEY when we want to validate behaviour
end-to-end — that is opt-in and never runs in CI.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, replace as dc_replace
from datetime import date
from pathlib import Path
from typing import Literal

import requests

from legalize.fetcher.es.amendments import AmendmentPatch, Operation, operation_for_verb
from legalize.transformer.anchor import Anchor

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────
# Config + errors
# ──────────────────────────────────────────────────────────


Backend = Literal["groq", "ollama"]

# Model ladder. The "escalation" idea (cheapest-first with retry) was
# deferred out of MVP after an independent review: the optimization is
# worth ~$0.20 on 1200 calls but costs hours of debugging complexity
# (silent short-circuits when the cheap rung is confidently wrong, mixed
# extractor provenance in the fidelity loop). We keep the tuple interface
# so Phase 3 can re-introduce escalation with real data, but the MVP
# default is a single model.
#
# gpt-oss-120b strikes the best quality/cost for Spanish legal text
# ($0.15/$0.75 per MTok, ~500ms/call on Groq). Stage C MVP budget is
# ~$0.60 total at this model — noise at our scale.
GROQ_ESCALATION: tuple[str, ...] = ("openai/gpt-oss-120b",)

OLLAMA_ESCALATION: tuple[str, ...] = ("qwen2.5:32b-instruct",)


class LLMError(RuntimeError):
    """Raised when every model in the escalation ladder fails. The caller
    should emit a commit-pointer fallback, never invent text."""


@dataclass(frozen=True)
class LLMConfig:
    """Runtime settings for AmendmentLLM.

    Defaults are tuned for the MVP: Groq backend, zero temperature, disk
    cache under ``.cache/llm/`` so the fidelity loop is repeatable."""

    backend: Backend = "groq"
    api_key: str | None = None
    base_url: str | None = None  # overrides default endpoint
    escalation: tuple[str, ...] = ()  # empty → backend default
    temperature: float = 0.0
    max_tokens: int = 800
    cache_dir: Path | None = None  # None → no cache
    timeout_s: float = 30.0
    # Confidence below which we escalate to the next model rung.
    rung_threshold: float = 0.8

    def resolved_escalation(self) -> tuple[str, ...]:
        if self.escalation:
            return self.escalation
        return GROQ_ESCALATION if self.backend == "groq" else OLLAMA_ESCALATION

    def resolved_base_url(self) -> str:
        if self.base_url:
            return self.base_url
        return {
            "groq": "https://api.groq.com/openai/v1",
            "ollama": "http://localhost:11434/v1",
        }[self.backend]

    def resolved_api_key(self) -> str | None:
        if self.api_key:
            return self.api_key
        # Groq requires a key; Ollama does not (local, no auth).
        return os.environ.get("GROQ_API_KEY") if self.backend == "groq" else None


@dataclass(frozen=True)
class VerifyResult:
    """Outcome of a spot-check on a regex-produced patch.

    verdict:
      - "ok"      → the patch is coherent with the modifier text; apply it.
      - "wrong"   → the patch is wrong; corrected_patch contains the fix.
      - "unable"  → the model can't decide (low confidence). Caller must
                    fall back to commit-pointer, NOT apply the patch.
    """

    verdict: Literal["ok", "wrong", "unable"]
    corrected_patch: AmendmentPatch | None = None
    reason: str = ""
    model_used: str = ""


# ──────────────────────────────────────────────────────────
# JSON schema for structured output
# ──────────────────────────────────────────────────────────


# Shape the model MUST return when parsing a difficult case. Kept small so
# 20B-class models have no trouble satisfying it.
_PARSE_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": ["operation", "anchor", "new_text", "confidence", "reason"],
    "properties": {
        "operation": {"enum": ["replace", "insert", "delete"]},
        "anchor": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "article": {"type": ["string", "null"]},
                "section": {"type": ["string", "null"]},
                "subsection": {"type": ["string", "null"]},
                "letter": {"type": ["string", "null"]},
                "ordinal": {"type": ["string", "null"]},
                "free_text": {"type": ["string", "null"]},
            },
        },
        "new_text": {
            "type": ["array", "null"],
            "items": {"type": "string"},
            "description": "Paragraphs to insert/replace with. null for delete ops.",
        },
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "reason": {"type": "string", "maxLength": 280},
    },
}


_VERIFY_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "confidence", "reason"],
    "properties": {
        "verdict": {"enum": ["ok", "wrong", "unable"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "reason": {"type": "string", "maxLength": 280},
        "corrected": {
            "anyOf": [{"type": "null"}, _PARSE_SCHEMA],
        },
    },
}


# ──────────────────────────────────────────────────────────
# Prompts
# ──────────────────────────────────────────────────────────


_SYS_PARSE = (
    "Eres un asistente juridico especializado en legislacion espanola del BOE. "
    "Te paso (1) un fragmento del texto base de una norma, (2) un extracto del "
    "cuerpo de otra norma que la modifica, y (3) una descripcion del ancla. "
    "Tu tarea: devolver UN SOLO JSON.\n\n"
    "ESQUEMA EXACTO (todos los campos obligatorios, sin anadir otros):\n"
    "{\n"
    '  "operation": "replace" | "insert" | "delete",   // EN INGLES, literal\n'
    '  "anchor": {"article": null | string, "section": null | string,\n'
    '             "subsection": null | string, "letter": null | string,\n'
    '             "ordinal": null | string, "free_text": null | string},\n'
    '  "new_text": null | [string, string, ...],   // null si operation=delete\n'
    '  "confidence": 0.0..1.0,\n'
    '  "reason": string (max 280 chars)\n'
    "}\n\n"
    "MAPEO VERBO → operation:\n"
    '  MODIFICA     → "replace"\n'
    '  AÑADE/ANADE  → "insert"\n'
    '  SUPRIME      → "delete"\n'
    '  DEROGA       → "delete"\n\n'
    "REGLAS ESTRICTAS:\n"
    "- NO INVENTES TEXTO. Si no encuentras el texto nuevo literal entre «...» "
    "  en el extracto modificador, devuelve new_text=null y confidence<0.5.\n"
    "- Si operation=delete, new_text debe ser null.\n"
    "- El valor de operation es SIEMPRE uno de los tres strings en ingles; "
    "  nunca devuelvas el verbo en espanol.\n"
    "- Si no estas 95% seguro, baja confidence. El pipeline prefiere un "
    "  commit-pointer a un patch erroneo."
)


_SYS_VERIFY = (
    "Eres un asistente juridico que revisa modificaciones de normas BOE. "
    "Te paso (1) el texto base (fragmento), (2) el extracto del cuerpo "
    "modificador, (3) el ancla propuesta y (4) el new_text propuesto. "
    "Tu tarea: decidir si el patch es coherente.\n\n"
    "ESQUEMA EXACTO:\n"
    "{\n"
    '  "verdict": "ok" | "wrong" | "unable",   // EN INGLES literal\n'
    '  "confidence": 0.0..1.0,\n'
    '  "reason": string (max 280 chars),\n'
    '  "corrected": null | {misma estructura que parse: operation, anchor, new_text, confidence, reason}\n'
    "}\n\n"
    "REGLAS:\n"
    '- verdict="ok" si el new_text propuesto aparece literal en el extracto modificador y el anchor es correcto.\n'
    '- verdict="wrong" si el new_text NO aparece literal o el anchor es incorrecto. Incluye corrected solo si puedes dar una version correcta; en caso contrario corrected=null.\n'
    '- verdict="unable" si no puedes decidir con seguridad.\n'
    '- El valor de operation dentro de corrected debe ser "replace"|"insert"|"delete" en ingles.\n'
    "- NUNCA inventes texto que no este en el extracto modificador."
)


def _build_parse_prompt(
    base_context: str,
    modifier_excerpt: str,
    anchor_hint: str,
    operation_hint: str,
) -> str:
    return (
        f"OPERACION SUGERIDA: {operation_hint}\n"
        f"DESCRIPCION DEL ANCLA: {anchor_hint}\n\n"
        f"TEXTO BASE (fragmento):\n---\n{base_context}\n---\n\n"
        f"EXTRACTO DEL CUERPO MODIFICADOR:\n---\n{modifier_excerpt}\n---\n\n"
        f"Devuelve UN SOLO JSON con la estructura {{operation, anchor, new_text, "
        f"confidence, reason}}. Nada mas."
    )


def _build_verify_prompt(
    base_context: str,
    modifier_excerpt: str,
    anchor_hint: str,
    proposed_new_text: list[str] | None,
    operation: str,
) -> str:
    nt = "null" if proposed_new_text is None else json.dumps(proposed_new_text, ensure_ascii=False)
    return (
        f"OPERACION: {operation}\n"
        f"ANCHOR: {anchor_hint}\n"
        f"NEW_TEXT PROPUESTO: {nt}\n\n"
        f"TEXTO BASE (fragmento):\n---\n{base_context}\n---\n\n"
        f"EXTRACTO DEL CUERPO MODIFICADOR:\n---\n{modifier_excerpt}\n---\n\n"
        "Devuelve UN SOLO JSON con {verdict, confidence, reason, corrected?}."
    )


# ──────────────────────────────────────────────────────────
# Structured-edit extraction (session 3 redesign)
# ──────────────────────────────────────────────────────────
#
# The parse_difficult_case / verify pair kept the anchor as a free-text
# string, which the downstream patcher then had to re-parse with brittle
# regexes (parse_anchor_from_hint). That roundtrip lost the anchor detail
# the LLM had clearly resolved. The structured-edit path below returns
# the anchor fields directly (as the Anchor dataclass the patcher already
# consumes), and packs multiple edits into one call so a modifier with N
# sub-patches costs one LLM round-trip instead of N.


@dataclass(frozen=True)
class StructuredEdit:
    """An LLM-produced edit ready to feed ``apply_patch_structured`` directly.

    Fields mirror what apply_patch actually needs — no more roundtripping
    through string hints.

    - ``anchor``       already a parsed ``Anchor`` object; resolver consumes it.
    - ``operation``    one of replace / insert / delete.
    - ``old_text``     literal quote of the region we expect to replace. Used
                       by the length-sanity gate and for provenance logs; does
                       NOT drive resolution (the anchor does).
    - ``new_text``     tuple of paragraphs or None for delete ops.
    - ``confidence``   model's self-reported confidence in THIS edit, independent
                       of other edits in the same batch.
    - ``reason``       free-text rationale; kept for fidelity logs.
    - ``patch_index``  optional pointer back to the AmendmentPatch this edit
                       resolves, so the dispatcher can correlate one LLM call
                       across multiple patches.
    """

    anchor: Anchor
    operation: Operation
    old_text: str
    new_text: tuple[str, ...] | None
    confidence: float = 0.0
    reason: str = ""
    patch_index: int | None = None


# JSON schema for the multi-edit response. We allow the model to return an
# empty list (if nothing can be resolved) so the caller can degrade to
# regex instead of us faking confidence.
_EXTRACT_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": ["edits"],
    "properties": {
        "edits": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["operation", "anchor", "new_text", "confidence", "reason"],
                "properties": {
                    "patch_index": {"type": ["integer", "null"]},
                    "operation": {"enum": ["replace", "insert", "delete"]},
                    "anchor": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "articulo": {"type": ["string", "null"]},
                            "norma": {"type": ["string", "null"]},
                            "apartado": {"type": ["string", "null"]},
                            "letra": {"type": ["string", "null"]},
                            "parrafo": {"type": ["string", "null"]},
                            "disposicion": {"type": ["string", "null"]},
                            "anexo": {"type": ["string", "null"]},
                            "libro": {"type": ["string", "null"]},
                            "titulo": {"type": ["string", "null"]},
                            "capitulo": {"type": ["string", "null"]},
                            "seccion": {"type": ["string", "null"]},
                        },
                    },
                    "old_text": {"type": ["string", "null"]},
                    "new_text": {
                        "type": ["array", "null"],
                        "items": {"type": "string"},
                    },
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "reason": {"type": "string", "maxLength": 280},
                },
            },
        },
    },
}


_SYS_EXTRACT = (
    "Eres un asistente juridico especializado en legislacion espanola del BOE. "
    "Recibes: (1) un fragmento del texto base de una norma en Markdown, (2) el "
    "cuerpo completo de otra norma que la modifica, (3) una lista de pistas de "
    "ancla (hints) — una por sub-modificacion — cada una con su verbo BOE.\n\n"
    "Tu tarea: devolver UN SOLO JSON con una lista 'edits', una entrada por hint. "
    "Cada edit debe ubicar el punto exacto en la norma base y el texto nuevo "
    "literal que aparece en el cuerpo modificador (entre comillas «...»).\n\n"
    "ESQUEMA EXACTO:\n"
    "{\n"
    '  "edits": [\n'
    "    {\n"
    '      "patch_index": integer,             // indice de la pista en la lista de entrada\n'
    '      "operation": "replace"|"insert"|"delete",  // EN INGLES, literal\n'
    '      "anchor": {                         // todos los campos son opcionales; usa solo los relevantes\n'
    '        "articulo": null | string,        // "5" | "5 bis" | "12"\n'
    '        "norma": null | string,           // "16" (Circulares BdE)\n'
    '        "apartado": null | string,        // "2" | "uno" | "primero"\n'
    '        "letra": null | string,           // "a" | "c"\n'
    '        "parrafo": null | string,         // "primero" | "3"\n'
    '        "disposicion": null | string,     // "adicional primera" | "final tercera"\n'
    '        "anexo": null | string,           // "I" | "III" | "1"\n'
    '        "libro": null | string,\n'
    '        "titulo": null | string,\n'
    '        "capitulo": null | string,\n'
    '        "seccion": null | string\n'
    "      },\n"
    '      "old_text": string | null,          // cita literal del texto base que se sustituye (para gate)\n'
    '      "new_text": [string, ...] | null,   // parrafos literales entre «»; null si operation=delete\n'
    '      "confidence": 0.0..1.0,\n'
    '      "reason": string (max 280)\n'
    "    },\n"
    "    ...\n"
    "  ]\n"
    "}\n\n"
    "MAPEO VERBO BOE → operation:\n"
    '  MODIFICA / 270 / 407 → "replace"\n'
    '  AÑADE / ANADE / 210  → "insert"\n'
    '  SUPRIME / 235 / DEROGA → "delete"\n\n'
    "REGLAS ESTRICTAS:\n"
    "- NO INVENTES TEXTO. Si no encuentras el new_text literal entre «...» en "
    "  el cuerpo modificador, pon new_text=null y confidence<0.5.\n"
    "- Si operation=delete, new_text=null SIEMPRE.\n"
    "- Usa SOLO los campos del anchor que realmente identifican la ubicacion; "
    "  deja null los demas. Un anchor bueno para una Circular BdE suele ser "
    "  {norma, apartado, letra}; para una Ley suele ser {articulo, apartado, letra}.\n"
    "- El 'patch_index' DEBE coincidir con el indice 0-based de la pista en la "
    "  lista de entrada. Si una pista no es resoluble, incluyela en 'edits' con "
    "  confidence=0 en lugar de omitirla.\n"
    "- Prefiero commit-pointer a patch erroneo: si dudas, baja confidence."
)


def _build_extract_prompt(
    base_context: str,
    modifier_body: str,
    hints: list[tuple[str, str]],  # [(anchor_hint, verb_code), ...]
) -> str:
    hint_lines: list[str] = []
    for idx, (hint, verb) in enumerate(hints):
        hint_lines.append(f"  [{idx}] verb={verb or '?'}  hint={hint!r}")
    hints_block = "\n".join(hint_lines) if hint_lines else "  (lista vacia)"
    return (
        f"PISTAS DE ANCLA (una por sub-modificacion):\n"
        f"{hints_block}\n\n"
        f"TEXTO BASE (fragmento, Markdown):\n---\n{base_context}\n---\n\n"
        f"CUERPO MODIFICADOR (completo):\n---\n{modifier_body}\n---\n\n"
        f"Devuelve UN SOLO JSON con {{edits: [...]}} — una entrada por hint, "
        f"en el mismo orden. Nada mas."
    )


_ANCHOR_FIELDS: tuple[str, ...] = (
    "articulo",
    "norma",
    "apartado",
    "letra",
    "parrafo",
    "disposicion",
    "anexo",
    "libro",
    "titulo",
    "capitulo",
    "seccion",
)


# Law-identifier pattern ("168/2025", "8/2015", "27/2014") — BOE's
# canonical citation form. The model keeps shoving this into the
# ``norma`` field even though our schema reserves ``norma`` for
# Circular-BdE internal units (Norma 1, Norma 2, …). Detect and drop.
_LAW_IDENT_RE = re.compile(r"^\s*\d+\s*/\s*\d{4}\s*$")

# Combined article reference ("96.2", "32.1.a") that should be split into
# articulo + apartado (+ optional letra). Captures the parts so we can
# repack them into the correct fields.
_ARTICLE_COMPOUND_RE = re.compile(
    r"^\s*(?P<art>\d+\s*(?:bis|ter|quater)?)"
    r"(?:\s*\.\s*(?P<ap>\d+))?"
    r"(?:\s*\.\s*(?P<letra>[a-z]))?\s*$",
    re.IGNORECASE,
)

# "disposición transitoria cuarta.6" / "adicional primera.3" — the model
# appends the apartado to the ordinal with a literal dot. Separate.
_DISPOSICION_COMPOUND_RE = re.compile(
    r"^\s*(?P<disp>[\wÁÉÍÓÚáéíóúñ]+\s+[\wÁÉÍÓÚáéíóúñ]+)"
    r"\s*\.\s*(?P<ap>\d+)\s*$",
    re.IGNORECASE,
)


def _anchor_from_dict(raw: dict | None) -> Anchor:
    """Build an Anchor from the LLM's JSON payload.

    Beyond trimming/nulling empty strings we run three LLM-quirk fixes
    uncovered by session-3 live diagnostics:

      1. ``norma`` carrying a law identifier like ``"168/2025"`` is
         dropped — ``norma`` in our Anchor is reserved for the internal
         structural unit of a Circular BdE ("Norma 1", "Norma 67"),
         not for the modified law's BOE citation. The model confuses
         the two.
      2. ``articulo`` given as ``"N.M"`` is split into articulo + apartado
         (and ``"N.M.letra"`` into articulo + apartado + letra). This
         is an extremely common shorthand on BOE amendments but breaks
         ``_match_articulo`` because the regex expects just the article
         number.
      3. Empty strings → None, so downstream resolution can treat them
         as "not supplied" instead of matching on "".
    """
    if not isinstance(raw, dict):
        return Anchor()
    fields: dict[str, str | None] = {}
    for name in _ANCHOR_FIELDS:
        v = raw.get(name)
        if isinstance(v, str):
            v = v.strip()
            fields[name] = v or None
        else:
            fields[name] = None

    # Fix 1: drop law-identifier misuse in ``norma``.
    if fields.get("norma") and _LAW_IDENT_RE.match(fields["norma"] or ""):
        fields["norma"] = None

    # Fix 2: split compound articulo ref. We only overwrite apartado /
    # letra when they were NOT explicitly provided by the LLM.
    art = fields.get("articulo")
    if art:
        m = _ARTICLE_COMPOUND_RE.match(art)
        if m and m.group("ap"):
            fields["articulo"] = m.group("art").strip()
            if not fields.get("apartado"):
                fields["apartado"] = m.group("ap")
            if m.group("letra") and not fields.get("letra"):
                fields["letra"] = m.group("letra").lower()

    # Fix 2b: same pattern on disposicion. "transitoria cuarta.6" →
    # disposicion="transitoria cuarta" + apartado="6".
    disp = fields.get("disposicion")
    if disp:
        dm = _DISPOSICION_COMPOUND_RE.match(disp)
        if dm:
            fields["disposicion"] = dm.group("disp").strip()
            if not fields.get("apartado"):
                fields["apartado"] = dm.group("ap")

    # Normalise letra to lowercase for the resolver.
    if fields.get("letra"):
        fields["letra"] = (fields["letra"] or "").lower() or None

    return Anchor(**fields)


# ──────────────────────────────────────────────────────────
# AmendmentLLM
# ──────────────────────────────────────────────────────────


class AmendmentLLM:
    """HTTP client for OpenAI-compatible Chat Completions endpoints.

    Thread-safety: each method call creates its own requests.Session via
    the module-level `requests` calls, so instances are reusable across
    threads. The disk cache is per-process; multi-process runs can share
    the same cache_dir safely (file writes are atomic via write-rename).
    """

    def __init__(self, config: LLMConfig | None = None) -> None:
        self.cfg = config or LLMConfig()
        if self.cfg.cache_dir:
            self.cfg.cache_dir.mkdir(parents=True, exist_ok=True)

    # ── public API ────────────────────────────────────────

    def parse_difficult_case(
        self,
        *,
        base_context: str,
        modifier_excerpt: str,
        anchor_hint: str,
        operation_hint: str,
        target_id: str,
        source_boe_id: str,
        source_date: date,
        verb_code: str = "",
        verb_text: str = "",
        ordering_key: str = "",
    ) -> AmendmentPatch:
        """Ask the LLM to construct a structured patch from ambiguous input.

        Returns an AmendmentPatch with extractor="llm". Confidence is what
        the model reports; callers typically require >= 0.95 before
        applying. If every model in the escalation ladder fails or the
        JSON is invalid, raises LLMError so the caller can fall back to a
        commit-pointer reform.
        """
        prompt = _build_parse_prompt(base_context, modifier_excerpt, anchor_hint, operation_hint)
        result = self._call_with_escalation(
            system=_SYS_PARSE,
            user=prompt,
            schema=_PARSE_SCHEMA,
        )
        model_used, parsed = result

        # Defensive: the ladder guarantees a valid schema, but we validate
        # the semantically important fields explicitly.
        op = parsed.get("operation")
        if op not in ("replace", "insert", "delete"):
            raise LLMError(f"invalid operation from {model_used}: {op!r}")

        new_text_raw = parsed.get("new_text")
        new_text: tuple[str, ...] | None
        if new_text_raw is None:
            new_text = None
        elif isinstance(new_text_raw, list) and all(isinstance(x, str) for x in new_text_raw):
            new_text = tuple(x for x in new_text_raw if x)
            if not new_text:
                new_text = None
        else:
            raise LLMError(f"invalid new_text shape from {model_used}: {new_text_raw!r}")

        conf = float(parsed.get("confidence", 0.0))

        # Sanity: delete never carries text. If the model disagrees, trust
        # the verb, not the model.
        if op == "delete":
            new_text = None

        # Operation coherence with the verb hint. If the verb is MODIFICA
        # but the model returned delete, that is suspicious — we clamp the
        # confidence so the caller falls back to a commit pointer.
        if verb_code:
            expected = operation_for_verb(verb_code)
            if expected and expected != op:
                logger.warning(
                    "LLM returned op=%s but verb_code=%s expected %s; clamping confidence",
                    op,
                    verb_code,
                    expected,
                )
                conf = min(conf, 0.4)

        return AmendmentPatch(
            target_id=target_id,
            operation=op,  # type: ignore[arg-type]
            verb_code=verb_code,
            verb_text=verb_text,
            anchor_hint=anchor_hint,
            source_boe_id=source_boe_id,
            source_date=source_date,
            new_text=new_text,
            # Both axes collapse to the model's own confidence when the
            # LLM constructed the patch from scratch — it owns both the
            # anchor decision and the text extraction.
            anchor_confidence=conf,
            new_text_confidence=conf if op != "delete" else 1.0,
            extractor="llm_parse",
            ordering_key=ordering_key,
        )

    def extract_edits_from_modifier(
        self,
        *,
        base_context: str,
        modifier_body: str,
        hints: list[tuple[str, str]],
    ) -> list[StructuredEdit]:
        """One LLM call for a whole group of sub-patches sharing a modifier.

        ``hints`` is a list of (anchor_hint, verb_code) pairs in the order
        the patches live in the AmendmentPatch list; the model is asked to
        return one edit per hint at the matching 0-based ``patch_index``.

        Cached identically to parse_difficult_case: the full prompt hash is
        the key, so re-running the fidelity loop on the same modifier is
        free after the first call.

        Returns an empty list when the model returns no usable edits (all
        confidences below the configured rung threshold, malformed JSON,
        etc.) — never raises for an empty result. Transport-level errors
        still raise ``LLMError`` so the dispatcher can fall back to regex.
        """
        if not hints:
            return []
        prompt = _build_extract_prompt(base_context, modifier_body, hints)
        # Budget scales with number of edits: each edit carries an anchor,
        # old_text and 1+ new_text paragraphs. 4K tokens comfortably fits
        # 10 edits with full Spanish prose; we scale linearly for larger
        # groups while staying well below Groq's 32K-token cap.
        max_tokens = max(4000, 400 * len(hints))
        model_used, parsed = self._call_with_escalation(
            system=_SYS_EXTRACT,
            user=prompt,
            schema=_EXTRACT_SCHEMA,
            # confidence of the envelope is not a single number here —
            # individual edits carry their own. Skip the rung threshold
            # check at envelope level; we filter per-edit below.
            bypass_rung_threshold=True,
            max_tokens=max_tokens,
        )
        raw_edits = parsed.get("edits")
        if not isinstance(raw_edits, list):
            logger.warning("LLM %s returned non-list edits: %r", model_used, raw_edits)
            return []

        out: list[StructuredEdit] = []
        for item in raw_edits:
            if not isinstance(item, dict):
                continue
            op = item.get("operation")
            if op not in ("replace", "insert", "delete"):
                continue
            new_text_raw = item.get("new_text")
            if new_text_raw is None:
                new_text: tuple[str, ...] | None = None
            elif isinstance(new_text_raw, list):
                paragraphs = tuple(x for x in new_text_raw if isinstance(x, str) and x.strip())
                new_text = paragraphs if paragraphs else None
            else:
                continue
            if op == "delete":
                new_text = None
            old_text_raw = item.get("old_text")
            old_text = old_text_raw if isinstance(old_text_raw, str) else ""
            conf_raw = item.get("confidence", 0.0)
            try:
                conf = float(conf_raw)
            except (TypeError, ValueError):
                conf = 0.0
            conf = max(0.0, min(1.0, conf))
            reason = item.get("reason") or ""
            idx_raw = item.get("patch_index")
            patch_index = idx_raw if isinstance(idx_raw, int) else None
            anchor = _anchor_from_dict(item.get("anchor"))
            out.append(
                StructuredEdit(
                    anchor=anchor,
                    operation=op,  # type: ignore[arg-type]
                    old_text=old_text,
                    new_text=new_text,
                    confidence=conf,
                    reason=reason if isinstance(reason, str) else "",
                    patch_index=patch_index,
                )
            )
        return out

    def verify(
        self,
        *,
        patch: AmendmentPatch,
        base_context: str,
        modifier_excerpt: str,
    ) -> VerifyResult:
        """Spot-check a regex-produced patch. Cheap (~300 tokens roundtrip).

        The verifier only runs on patches we're about to APPLY — skip this
        for delete verbs (they're trivially correct) and for patches below
        the "worth verifying" threshold (send those to parse_difficult_case
        directly instead).
        """
        if patch.operation == "delete":
            return VerifyResult(
                verdict="ok", reason="delete verbs need no text", model_used="skipped"
            )

        prompt = _build_verify_prompt(
            base_context=base_context,
            modifier_excerpt=modifier_excerpt,
            anchor_hint=patch.anchor_hint,
            proposed_new_text=list(patch.new_text) if patch.new_text else None,
            operation=patch.operation,
        )
        model_used, parsed = self._call_with_escalation(
            system=_SYS_VERIFY,
            user=prompt,
            schema=_VERIFY_SCHEMA,
        )
        verdict = parsed.get("verdict")
        if verdict not in ("ok", "wrong", "unable"):
            raise LLMError(f"invalid verdict from {model_used}: {verdict!r}")
        reason = parsed.get("reason", "")

        corrected_patch: AmendmentPatch | None = None
        if verdict == "wrong":
            corrected = parsed.get("corrected")
            if corrected:
                new_text_raw = corrected.get("new_text")
                corrected_new_text: tuple[str, ...] | None
                if new_text_raw is None:
                    corrected_new_text = None
                elif isinstance(new_text_raw, list):
                    corrected_new_text = tuple(x for x in new_text_raw if isinstance(x, str) and x)
                    if not corrected_new_text:
                        corrected_new_text = None
                else:
                    corrected_new_text = None

                corrected_op = corrected.get("operation") or patch.operation
                if corrected_op not in ("replace", "insert", "delete"):
                    corrected_op = patch.operation
                cconf = float(corrected.get("confidence", 0.0))
                corrected_patch = dc_replace(
                    patch,
                    operation=corrected_op,
                    new_text=corrected_new_text,
                    anchor_confidence=cconf,
                    new_text_confidence=cconf if corrected_op != "delete" else 1.0,
                    # Distinct from "llm_parse": this patch started as a
                    # regex candidate the verifier disagreed with. Keeping
                    # the provenance separate lets the fidelity loop see
                    # whether verifier-corrections agree with the base
                    # text as often as from-scratch parses.
                    extractor="llm_verify_correct",
                )

        return VerifyResult(
            verdict=verdict,  # type: ignore[arg-type]
            corrected_patch=corrected_patch,
            reason=reason,
            model_used=model_used,
        )

    # ── transport ─────────────────────────────────────────

    def _call_with_escalation(
        self,
        *,
        system: str,
        user: str,
        schema: dict,
        bypass_rung_threshold: bool = False,
        max_tokens: int | None = None,
    ) -> tuple[str, dict]:
        """Try each model in the ladder until one returns a valid JSON.

        Returns (model_used, parsed_json). Raises LLMError if every rung
        fails.

        Cache semantics (hardened post-review):

        - Key = hash(system || user || model_ladder). If we change the
          ladder or the prompt, the cache is invalidated — no stale
          answers from a different question.
        - Payload = {model, response_id, groq_model_reported, parsed}.
          Groq silently updates weights under the same alias (e.g. a
          `gpt-oss-120b` response today may not match the same alias
          tomorrow). We cannot pin weights, but we can OBSERVE drift by
          recording the response_id + the model name Groq echoes back. A
          separate script can flag cache entries whose metadata changed
          between runs even though the cached parse is reused.
        """
        cache_key = _prompt_hash(system, user, self.cfg.resolved_escalation())
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached["model"], cached["parsed"]

        errors: list[str] = []
        for model in self.cfg.resolved_escalation():
            try:
                parsed, meta = self._call_once(
                    model=model,
                    system=system,
                    user=user,
                    schema=schema,
                    max_tokens=max_tokens,
                )
            except LLMError as e:
                errors.append(f"{model}: {e}")
                continue

            if not bypass_rung_threshold:
                conf = parsed.get("confidence")
                if isinstance(conf, (int, float)) and conf < self.cfg.rung_threshold:
                    errors.append(f"{model}: confidence {conf:.2f} below rung threshold")
                    continue

            self._cache_put(
                cache_key,
                {
                    "model": model,
                    "parsed": parsed,
                    "response_id": meta.get("response_id", ""),
                    "groq_model_reported": meta.get("model_reported", ""),
                },
            )
            return model, parsed

        raise LLMError("all models failed: " + "; ".join(errors))

    def _call_once(
        self,
        *,
        model: str,
        system: str,
        user: str,
        schema: dict,
        max_tokens: int | None = None,
    ) -> tuple[dict, dict]:
        """One HTTP round-trip. Returns (parsed_json, response_meta) where
        response_meta carries the opaque response_id + the model Groq
        echoed back — both are used for drift observability."""
        api_key = self.cfg.resolved_api_key()
        if self.cfg.backend == "groq" and not api_key:
            raise LLMError("GROQ_API_KEY not set")

        url = f"{self.cfg.resolved_base_url()}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        effective_max_tokens = max_tokens if max_tokens is not None else self.cfg.max_tokens
        payload: dict = {
            "model": model,
            "temperature": self.cfg.temperature,
            "max_tokens": effective_max_tokens,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "response_format": {"type": "json_object"},
        }

        # Single retry on 429 — Groq TPM limits are easy to trip when we
        # ship full base_markdown + long modifier_body on every call. The
        # error body includes a precise "try again in Xs" hint which we
        # honour up to 30s. Any other non-200 bubbles up as LLMError.
        attempts = 0
        while True:
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=self.cfg.timeout_s)
            except requests.RequestException as e:
                raise LLMError(f"network error to {model}: {e}") from e

            if resp.status_code == 429 and attempts == 0:
                attempts += 1
                wait_s = _parse_retry_after(resp)
                logger.info(
                    "Groq 429 rate-limit for %s; sleeping %.1fs before retry",
                    model,
                    wait_s,
                )
                import time

                time.sleep(wait_s)
                continue
            break

        if resp.status_code != 200:
            body = resp.text[:500]
            raise LLMError(f"{model} HTTP {resp.status_code}: {body}")

        try:
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, ValueError) as e:
            raise LLMError(f"{model} unexpected response shape: {e}") from e

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            raise LLMError(f"{model} non-JSON content: {content[:200]!r}") from e

        if not isinstance(parsed, dict):
            raise LLMError(f"{model} returned non-object JSON: {type(parsed).__name__}")

        missing = set(schema.get("required", [])) - set(parsed.keys())
        if missing:
            raise LLMError(f"{model} missing required keys: {sorted(missing)}")

        response_meta = {
            "response_id": data.get("id", ""),
            "model_reported": data.get("model", ""),
        }
        return parsed, response_meta

    # ── cache ─────────────────────────────────────────────

    def _cache_path(self, key: str) -> Path | None:
        if not self.cfg.cache_dir:
            return None
        return self.cfg.cache_dir / f"{key}.json"

    def _cache_get(self, key: str) -> dict | None:
        path = self._cache_path(key)
        if not path or not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _cache_put(self, key: str, value: dict) -> None:
        path = self._cache_path(key)
        if not path:
            return
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)


# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────


def _parse_retry_after(resp) -> float:
    """Pull the suggested backoff out of a Groq 429 response. Groq puts it
    inside the JSON body as 'try again in 5.42568s' (sometimes with ms).
    When missing, fall back to a 10s default; we cap at 30s to avoid
    stalling the fidelity loop on a noisy-neighbour event."""
    import re as _re

    try:
        body = resp.json()
        msg = body.get("error", {}).get("message", "")
    except ValueError:
        msg = resp.text or ""
    m = _re.search(r"try again in ([\d.]+)\s*(m?s)", msg)
    if m:
        value = float(m.group(1))
        if m.group(2) == "ms":
            value = value / 1000.0
        return max(0.5, min(30.0, value))
    header = resp.headers.get("Retry-After")
    if header:
        try:
            return max(0.5, min(30.0, float(header)))
        except ValueError:
            pass
    return 10.0


def _prompt_hash(system: str, user: str, ladder: tuple[str, ...] = ()) -> str:
    """Stable cache key: blake2b of (system || user || ladder).

    The ladder is part of the key so that changing it (e.g. swapping
    gpt-oss-120b for a reasoning model) invalidates the cache. The
    alternative — a model-independent key — would happily serve a 20b
    answer when a 120b was requested, which is exactly the class of bug
    the fidelity loop is supposed to catch.
    """
    h = hashlib.blake2b(digest_size=16)
    h.update(system.encode("utf-8"))
    h.update(b"\x00")
    h.update(user.encode("utf-8"))
    h.update(b"\x00")
    h.update("|".join(ladder).encode("utf-8"))
    return h.hexdigest()


def build_anchor_context(base_markdown: str, anchor_hint: str, window: int = 400) -> str:
    """Extract a ~`window`-char fragment of `base_markdown` around where
    the anchor likely lives. Callers use this to keep prompts small.

    Strategy: search the base for the first occurrence of any signal token
    from the anchor hint (article number, law identifier, ...). Return a
    window around that position. When no token matches, return the first
    `window` characters — better than no context.
    """
    if not base_markdown:
        return ""
    if not anchor_hint:
        return base_markdown[:window]

    # Pull candidate anchors: "articulo 5", "Ley 37/1992", etc. Reuse the
    # signal regexes from the amendments module so we stay consistent.
    from legalize.fetcher.es.amendments import _NORM_IDENT_RE, _STRUCT_RE

    candidates: list[str] = []
    for m in _NORM_IDENT_RE.finditer(anchor_hint):
        candidates.append(m.group(0))
    for m in _STRUCT_RE.finditer(anchor_hint):
        candidates.append(m.group(0))

    for c in candidates:
        idx = base_markdown.lower().find(c.lower())
        if idx >= 0:
            start = max(0, idx - window // 2)
            end = min(len(base_markdown), idx + window // 2)
            return base_markdown[start:end]

    return base_markdown[:window]
