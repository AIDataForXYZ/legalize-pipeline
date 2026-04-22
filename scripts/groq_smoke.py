"""Live smoke test for src/legalize/llm/amendment_parser.py.

NOT a pytest — it actually calls Groq with the real API key. Run it
manually when changing the LLM integration or when validating a new
model. Reads the key from (in order):

    1. $GROQ_API_KEY
    2. ./groq_api_key (file in CWD)
    3. ~/.groq_api_key (dotfile in HOME)

Usage:
    uv run python scripts/groq_smoke.py
    uv run python scripts/groq_smoke.py --model openai/gpt-oss-120b
    uv run python scripts/groq_smoke.py --no-verify   # skip verifier phase

Exits 0 on success, 1 on any failure. Prints per-call timing + token
usage so you can estimate cost before batch runs.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.llm.amendment_parser import AmendmentLLM, LLMConfig, build_anchor_context


def _load_key() -> str:
    if k := os.environ.get("GROQ_API_KEY"):
        return k
    for candidate in (ROOT / "groq_api_key", Path.home() / ".groq_api_key"):
        if candidate.exists():
            return candidate.read_text(encoding="utf-8").strip()
    print("error: GROQ_API_KEY not found (env var or groq_api_key file)", file=sys.stderr)
    sys.exit(1)


# Synthetic but realistic Stage C scenario: Circular BdE 6/2021 edits
# Circular 4/2017 by replacing the letter c) of apartado 6 of norma 67.
BASE_CONTEXT = """\
###### Norma 67. Estados individuales reservados.

6. Los estados financieros reservados individuales se envian en los plazos siguientes:

a) Estados FI 1 a FI 45: antes del dia 15 del mes siguiente.
b) Importe en libros bruto: conforme a lo dispuesto en la norma 12.
c) El estado FI 40, que deberan remitir todas las entidades.
d) Los estados FI 41 a FI 45 se confeccionaran aplicando los criterios...
"""


MODIFIER_EXCERPT = """\
ii. Se modifica la letra c) del apartado 6, que queda redactada del siguiente modo:

«c) El estado FI 40, que habran de remitir las entidades que no tengan que enviar
estados consolidados reservados conforme a lo dispuesto en la norma 68, salvo que
formen parte de un grupo bajo supervision consolidada del Banco de Espana.»
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=None,
                    help="Force a specific model (default: escalation ladder)")
    ap.add_argument("--no-verify", action="store_true",
                    help="Skip the verify() phase")
    ap.add_argument("--cache-dir", default=str(ROOT / ".cache" / "llm"),
                    help="Cache directory (default: .cache/llm)")
    args = ap.parse_args()

    api_key = _load_key()

    escalation: tuple[str, ...] = () if not args.model else (args.model,)
    cfg = LLMConfig(
        backend="groq",
        api_key=api_key,
        cache_dir=Path(args.cache_dir),
        escalation=escalation,
    )
    llm = AmendmentLLM(cfg)

    print(f"▶ parse_difficult_case (ladder: {cfg.resolved_escalation()})")
    t0 = time.monotonic()
    try:
        patch = llm.parse_difficult_case(
            base_context=build_anchor_context(BASE_CONTEXT, "letra c) apartado 6 norma 67"),
            modifier_excerpt=MODIFIER_EXCERPT,
            anchor_hint="la letra c) del apartado 6 de la norma 67",
            operation_hint="MODIFICA",
            target_id="BOE-A-2017-14334",
            source_boe_id="BOE-A-2021-21666",
            source_date=date(2021, 12, 29),
            verb_code="270",
            verb_text="MODIFICA",
        )
    except Exception as e:
        print(f"  ✗ failed: {e}")
        return 1
    dt = time.monotonic() - t0
    print(f"  ✓ {dt*1000:.0f} ms")
    print(f"    operation   = {patch.operation}")
    print(f"    confidence  = {patch.confidence:.2f}")
    print(f"    extractor   = {patch.extractor}")
    nt_preview = (patch.new_text[0][:100] + "...") if patch.new_text else "None"
    print(f"    new_text[0] = {nt_preview!r}")

    if patch.confidence < 0.95:
        print("  ! confidence below 0.95 — caller would fall back to commit-pointer")

    if args.no_verify:
        return 0

    print("\n▶ verify (correct patch — should return verdict=ok)")
    t0 = time.monotonic()
    try:
        result = llm.verify(
            patch=patch,
            base_context=build_anchor_context(BASE_CONTEXT, "letra c) apartado 6 norma 67"),
            modifier_excerpt=MODIFIER_EXCERPT,
        )
    except Exception as e:
        print(f"  ✗ failed: {e}")
        return 1
    dt = time.monotonic() - t0
    print(f"  ✓ {dt*1000:.0f} ms  verdict={result.verdict}  model={result.model_used}")
    print(f"    reason: {result.reason[:120]!r}")

    print("\n▶ verify (fabricated patch — should return verdict=wrong)")
    bad_patch = AmendmentPatch(
        target_id="BOE-A-2017-14334",
        operation="replace",
        verb_code="270",
        verb_text="MODIFICA",
        anchor_hint="la letra c) del apartado 6 de la norma 67",
        source_boe_id="BOE-A-2021-21666",
        source_date=date(2021, 12, 29),
        new_text=("Texto fabricado que NO aparece en el extracto modificador.",),
        confidence=0.7,
        extractor="regex",
    )
    t0 = time.monotonic()
    result = llm.verify(
        patch=bad_patch,
        base_context=build_anchor_context(BASE_CONTEXT, "letra c) apartado 6 norma 67"),
        modifier_excerpt=MODIFIER_EXCERPT,
    )
    dt = time.monotonic() - t0
    print(f"  ✓ {dt*1000:.0f} ms  verdict={result.verdict}  model={result.model_used}")
    print(f"    reason: {result.reason[:120]!r}")
    if result.verdict != "wrong":
        print("  ! expected 'wrong' for a fabricated patch — verifier may be too lax")

    print("\nOK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
