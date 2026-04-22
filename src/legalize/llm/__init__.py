"""LLM helpers for the pipeline.

Stage C uses this package for the ambiguous-amendment fallback (module 3 of
PLAN-STAGE-C.md). The regex extractor in fetcher/es/amendments.py resolves
~70% of cases deterministically; the remaining ~30% go through AmendmentLLM
here. Delete verbs (SUPRIME/DEROGA) and single-block modifiers never call
this package — the cost/latency budget stays mostly zero.
"""

from legalize.llm.amendment_parser import (
    AmendmentLLM,
    LLMConfig,
    LLMError,
    VerifyResult,
)
from legalize.llm.queue import (
    CaseResolution,
    CaseTier,
    PendingCase,
    PendingCaseQueue,
    case_id_for,
    classify_case,
)
from legalize.llm.resolver import (
    build_full_prompt,
    build_resolver_prompt,
    ingest_resolutions,
    load_batch,
    parse_resolutions_json,
)

__all__ = [
    "AmendmentLLM",
    "LLMConfig",
    "LLMError",
    "VerifyResult",
    # queue
    "CaseResolution",
    "CaseTier",
    "PendingCase",
    "PendingCaseQueue",
    "case_id_for",
    "classify_case",
    # resolver
    "build_full_prompt",
    "build_resolver_prompt",
    "ingest_resolutions",
    "load_batch",
    "parse_resolutions_json",
]
