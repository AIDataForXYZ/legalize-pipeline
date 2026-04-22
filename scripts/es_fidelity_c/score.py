"""Stage C fidelity scoring (PLAN-STAGE-C.md §W4).

This module consumes the pipeline's output — AmendmentPatches + their
PatchResults — and aggregates it into the metrics the fidelity loop
needs to triage iterations:

  - Coverage: what % of scope-relevant patches produced a usable mutation.
  - Confidence distribution: split by the two axes (anchor / new_text).
  - Case-tier routing: how many patches went regex-only vs short/medium/
    hard, so we can see whether the dispatcher thresholds are right.
  - Per-verb breakdown: MODIFICA vs AÑADE vs SUPRIME vs DEROGA success
    rates. DEROGA should be near-100% (no text extraction needed);
    MODIFICA is the hardest.
  - Gate failure breakdown: when a patch can't be applied, WHICH gate
    rejected it (anchor_not_found is a regex-resolver problem;
    length_mismatch is an LLM-truncation problem; each has different
    remediation).

Output is a FidelityReport dataclass. scripts/es_fidelity_c/report.py
renders it to CSV + Markdown for human review and trend tracking.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable

from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.llm.queue import classify_case
from legalize.transformer.patcher import PatchResult, PatchStatus


VERB_LABEL = {
    "270": "MODIFICA",
    "407": "ANADE",
    "235": "SUPRIME",
    "210": "DEROGA",
}


@dataclass(frozen=True)
class PatchRecord:
    """One row in the fidelity log: patch metadata + dispatch tier +
    apply result. All optional-unless-meaningful so a patch that never
    reached the patcher still fits (tier="dispatched" + result=None)."""

    modifier_id: str            # BOE-ID of the modifier this patch came from
    target_id: str
    verb_code: str
    operation: str
    anchor_confidence: float
    new_text_confidence: float
    extractor: str
    tier: str                   # classify_case() output
    apply_status: str | None    # PatchResult.status when apply was attempted
    apply_reason: str | None    # PatchResult.reason


@dataclass(frozen=True)
class FidelityReport:
    """Aggregated metrics over a set of PatchRecords.

    Percentages are computed against the MVP-scoped patch count (the
    denominator for "coverage"). Patches dropped during parse_anteriores
    because their verb was out of scope never appear in records — they
    are counted separately in out_of_scope_count for visibility.
    """

    sample_size: int                    # modifier XMLs processed
    total_patches: int                  # patches produced by parse_amendments
    out_of_scope_count: int             # verbs filtered at parse_anteriores stage

    # Coverage: % of total_patches we consider "ready to apply" at each stage.
    regex_ready_pct: float              # extractor=regex AND new_text filled (or delete)
    llm_bound_pct: float                # tier in {short, medium} (needs Groq)
    claude_bound_pct: float             # tier == "hard"

    # Apply stage — only meaningful when a base_markdown was supplied.
    apply_attempted: int
    apply_status_counts: dict[str, int] = field(default_factory=dict)

    # Verb-level roll-up.
    per_verb_total: dict[str, int] = field(default_factory=dict)
    per_verb_regex_filled: dict[str, int] = field(default_factory=dict)

    # Tier routing distribution.
    tier_counts: dict[str, int] = field(default_factory=dict)

    # Confidence axis histograms (10 buckets: [0.0, 0.1), ..., [0.9, 1.0]).
    anchor_confidence_buckets: list[int] = field(default_factory=lambda: [0] * 10)
    new_text_confidence_buckets: list[int] = field(default_factory=lambda: [0] * 10)

    @property
    def regex_only_coverage(self) -> float:
        """Share of all scope patches that could be committed with zero
        LLM spend. The number that matters for the "save money" policy."""
        return self.regex_ready_pct

    @property
    def applied_share(self) -> float:
        """Share of attempted applies that reached status == 'applied'.
        0 when no base was supplied."""
        if self.apply_attempted == 0:
            return 0.0
        return self.apply_status_counts.get("applied", 0) / self.apply_attempted


# ──────────────────────────────────────────────────────────
# Aggregation
# ──────────────────────────────────────────────────────────


def _bucket(value: float) -> int:
    """Map confidence in [0, 1] to a 10-bucket index. Clamped."""
    if value >= 1.0:
        return 9
    if value < 0:
        return 0
    return int(value * 10)


def build_report(
    records: Iterable[PatchRecord],
    *,
    sample_size: int,
    out_of_scope_count: int = 0,
) -> FidelityReport:
    """Fold a stream of PatchRecords into a FidelityReport."""
    records = list(records)

    total = len(records)
    tier_counts: Counter[str] = Counter(r.tier for r in records)
    apply_counts: Counter[str] = Counter(
        r.apply_status for r in records if r.apply_status is not None
    )
    apply_attempted = sum(apply_counts.values())

    regex_ready = sum(
        1 for r in records
        if r.extractor == "regex"
        and (r.operation == "delete" or r.new_text_confidence >= 0.9)
        and r.anchor_confidence >= 0.9
    )
    llm_bound = sum(1 for r in records if r.tier in ("short", "medium"))
    claude_bound = sum(1 for r in records if r.tier == "hard")

    per_verb_total: Counter[str] = Counter(r.verb_code for r in records)
    per_verb_filled: Counter[str] = Counter(
        r.verb_code for r in records
        if r.extractor == "regex"
        and (r.operation == "delete" or r.new_text_confidence >= 0.9)
    )

    anchor_buckets = [0] * 10
    new_text_buckets = [0] * 10
    for r in records:
        anchor_buckets[_bucket(r.anchor_confidence)] += 1
        new_text_buckets[_bucket(r.new_text_confidence)] += 1

    denom = max(1, total)
    return FidelityReport(
        sample_size=sample_size,
        total_patches=total,
        out_of_scope_count=out_of_scope_count,
        regex_ready_pct=regex_ready / denom,
        llm_bound_pct=llm_bound / denom,
        claude_bound_pct=claude_bound / denom,
        apply_attempted=apply_attempted,
        apply_status_counts=dict(apply_counts),
        per_verb_total=dict(per_verb_total),
        per_verb_regex_filled=dict(per_verb_filled),
        tier_counts=dict(tier_counts),
        anchor_confidence_buckets=anchor_buckets,
        new_text_confidence_buckets=new_text_buckets,
    )


# ──────────────────────────────────────────────────────────
# Per-patch record builder
# ──────────────────────────────────────────────────────────


def record_from_patch(
    patch: AmendmentPatch,
    *,
    modifier_excerpt_len: int,
    apply_result: PatchResult | None = None,
) -> PatchRecord:
    """Build a PatchRecord from a parsed AmendmentPatch + optional patcher
    result. Callers run apply_patch themselves and pass the outcome in;
    this function contains no I/O."""
    tier = classify_case(
        anchor_confidence=patch.anchor_confidence,
        new_text_confidence=patch.new_text_confidence,
        modifier_excerpt_len=modifier_excerpt_len,
    )
    return PatchRecord(
        modifier_id=patch.source_boe_id,
        target_id=patch.target_id,
        verb_code=patch.verb_code,
        operation=patch.operation,
        anchor_confidence=patch.anchor_confidence,
        new_text_confidence=patch.new_text_confidence,
        extractor=patch.extractor,
        tier=tier,
        apply_status=apply_result.status if apply_result else None,
        apply_reason=apply_result.reason if apply_result else None,
    )
