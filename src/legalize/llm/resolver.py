"""Claude-session resolver for hard amendment cases.

When a Stage C bootstrap accumulates BATCH_SIZE hard cases, the driver
hands off to this module. Two execution modes:

  1. Sub-agent dispatch (preferred during bootstrap when a Claude Code
     session is driving the pipeline). The driver calls resolve_batch()
     which builds a structured prompt, spawns an Explore sub-agent with
     it, and writes the returned resolutions back to the queue. This is
     the flow the user signed off on ("cuando haya 30 me ejecutas a ti
     mismo").

  2. Manual mode (no orchestrating Claude). Used for post-hoc recovery
     of a queue left over from a headless run. The user opens Claude
     Code in the project, invokes the /resolve-stage-c-batch slash
     command, I read the batch file directly with the Read tool,
     resolve each case, and write resolutions.jsonl with Write.

This module only owns the PROMPT CONSTRUCTION and RESPONSE PARSING; it
does not assume which interpreter actually runs the reading-comprehension
step. That keeps it testable without invoking any LLM.

Daily-update runs use Groq only and never enter this module; Claude is
exclusively a bootstrap-phase tool.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

from legalize.llm.queue import CaseResolution, PendingCase, PendingCaseQueue

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────
# Prompt construction
# ──────────────────────────────────────────────────────────


_RESOLVER_SYSTEM_PROMPT = """\
Eres el resolvedor de casos dificiles del pipeline Stage C del proyecto Legalize.

Se te pasa un lote de N casos. Cada caso representa una modificacion del BOE espanol
que el regex y los modelos Groq no han podido resolver con confianza. Tu tarea:
leer cada caso, entender que modifica (ancla) y con que texto (new_text), y
devolver una resolucion estructurada.

REGLAS ESTRICTAS:

- NO INVENTAR TEXTO. Si el modifier_excerpt no contiene el texto nuevo entre «...»
  o algun equivalente explicito, devuelve new_text=null y confidence=0. El
  pipeline prefiere un commit-pointer a un patch incorrecto.
- operation debe ser uno de: "replace" | "insert" | "delete" (en ingles).
  Si el caso es delete, new_text=null.
- anchor_confidence: 0.0-1.0 — tu certeza de que identificaste el punto exacto
  que se modifica dentro del base_context.
- new_text_confidence: 0.0-1.0 — tu certeza de que el new_text es literal y
  completo. Para delete, siempre 1.0.
- Si no puedes resolver con al menos 0.85 en ambos ejes, devuelve una resolucion
  con confidence 0 y reason explicando el obstaculo.
- reason: una frase corta (max 200 chars) explicando por que llegaste a esa
  decision. Es lo que aparecera en los fidelity logs.

FORMATO DE RESPUESTA:

Devuelves UN SOLO JSON con la estructura:

{
  "resolutions": [
    {
      "case_id": "<id del caso>",
      "operation": "replace" | "insert" | "delete",
      "new_text": ["parrafo 1", "parrafo 2"] | null,
      "anchor_confidence": 0.0-1.0,
      "new_text_confidence": 0.0-1.0,
      "reason": "frase corta"
    },
    ...
  ]
}

Una entrada por caso recibido. No omitas casos; si no puedes resolver uno,
devuelvelo con confidence 0 y reason explicando.
"""


def build_resolver_prompt(cases: Iterable[PendingCase]) -> str:
    """Format cases as a single prompt body ready to be sent to the
    Claude resolver (sub-agent or direct session). Keeps each case
    self-contained so the resolver can process them in any order."""
    blocks: list[str] = []
    for i, case in enumerate(cases, 1):
        blocks.append(
            f"## CASO {i}  (case_id: {case.case_id})\n\n"
            f"- target_id: {case.target_id}\n"
            f"- source_boe_id: {case.source_boe_id}\n"
            f"- source_date: {case.source_date}\n"
            f"- operation_hint: {case.operation}  (verb {case.verb_code} — {case.verb_text})\n"
            f"- anchor_hint (del <anterior><texto>): {case.anchor_hint}\n\n"
            f"### base_context\n\n"
            f"```markdown\n{case.base_context}\n```\n\n"
            f"### modifier_excerpt\n\n"
            f"```markdown\n{case.modifier_excerpt}\n```\n"
        )
    return "\n".join(blocks)


def build_full_prompt(cases: list[PendingCase]) -> str:
    """System + user prompt, joined. Used when invoking the Explore
    sub-agent which takes a single prompt string."""
    user = build_resolver_prompt(cases)
    return _RESOLVER_SYSTEM_PROMPT + "\n\n---\n\n" + f"LOTE DE {len(cases)} CASOS:\n\n" + user


# ──────────────────────────────────────────────────────────
# Response parsing
# ──────────────────────────────────────────────────────────


def parse_resolutions_json(raw: str) -> list[CaseResolution]:
    """Extract CaseResolution entries from a resolver response.

    The resolver may return either a bare JSON object with "resolutions"
    or the object wrapped in a fenced code block. We tolerate both and
    ignore any trailing prose. Malformed entries are skipped with a log
    warning — better to leave a case pending than to poison the queue.
    """
    text = raw.strip()

    # Strip a markdown code fence if present.
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    # Find the outer JSON object. The resolver is instructed to return
    # a single object, but if it accidentally adds a preamble we can
    # still recover by scanning for the first '{'.
    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace < 0 or last_brace < 0 or last_brace < first_brace:
        raise ValueError("no JSON object found in resolver response")
    payload = text[first_brace : last_brace + 1]

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as e:
        raise ValueError(f"resolver returned malformed JSON: {e}") from e

    if not isinstance(data, dict) or "resolutions" not in data:
        raise ValueError("resolver response missing 'resolutions' key")

    out: list[CaseResolution] = []
    for entry in data.get("resolutions", []):
        if not isinstance(entry, dict):
            logger.warning("skipping non-dict resolution entry: %s", entry)
            continue
        cid = entry.get("case_id")
        if not cid or not isinstance(cid, str):
            logger.warning("skipping resolution with no case_id: %s", entry)
            continue
        op = entry.get("operation")
        if op not in ("replace", "insert", "delete"):
            logger.warning("skipping resolution with invalid operation: %s", entry)
            continue
        nt = entry.get("new_text")
        if nt is not None:
            if not isinstance(nt, list) or not all(isinstance(x, str) for x in nt):
                logger.warning("skipping resolution with invalid new_text: %s", entry)
                continue
            nt = tuple(x for x in nt if x)
            if not nt:
                nt = None
        if op == "delete":
            nt = None
        out.append(
            CaseResolution(
                case_id=cid,
                operation=op,
                new_text=nt,
                anchor_confidence=float(entry.get("anchor_confidence", 0.0)),
                new_text_confidence=float(
                    entry.get("new_text_confidence", 1.0 if op == "delete" else 0.0)
                ),
                reason=str(entry.get("reason", ""))[:280],
                resolver="claude_code",
            )
        )
    return out


# ──────────────────────────────────────────────────────────
# Queue integration
# ──────────────────────────────────────────────────────────


def ingest_resolutions(queue: PendingCaseQueue, resolutions: list[CaseResolution]) -> int:
    """Write every resolution to the queue. Returns the count that was
    actually new (not already present)."""
    existing = set(queue.resolutions().keys())
    new_count = 0
    for r in resolutions:
        if r.case_id in existing:
            continue
        queue.record_resolution(r)
        new_count += 1
    return new_count


def load_batch(path: Path) -> list[PendingCase]:
    """Read a frozen batch file (JSONL) back into PendingCase instances.
    Used both by the resolver and by tests."""
    out: list[PendingCase] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            out.append(PendingCase(**d))
    return out
