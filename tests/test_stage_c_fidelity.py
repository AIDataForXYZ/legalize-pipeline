"""Tests for scripts/es_fidelity_c/ — scoring + reporting.

We don't shell out to run.py here (that's the smoke path, exercised
manually and captured into STATUS-STAGE-C.md). These tests pin the
aggregation logic so regressions in the scorer are caught in CI even
when the runner isn't invoked."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from legalize.fetcher.es.amendments import AmendmentPatch  # noqa: E402
from legalize.transformer.patcher import PatchResult  # noqa: E402
from scripts.es_fidelity_c.report import render_markdown_summary  # noqa: E402
from scripts.es_fidelity_c.score import (  # noqa: E402
    build_report,
    record_from_patch,
)


def _patch(
    *,
    operation: str = "replace",
    verb: str = "270",
    anchor_conf: float = 0.0,
    new_text_conf: float = 0.0,
    new_text: tuple[str, ...] | None = None,
    extractor: str = "regex",
    anchor_hint: str = "art. 1",
) -> AmendmentPatch:
    return AmendmentPatch(
        target_id="BOE-A-T",
        operation=operation,  # type: ignore[arg-type]
        verb_code=verb,
        verb_text="x",
        anchor_hint=anchor_hint,
        source_boe_id="BOE-A-S",
        source_date=date(2021, 1, 1),
        new_text=new_text,
        anchor_confidence=anchor_conf,
        new_text_confidence=new_text_conf,
        extractor=extractor,  # type: ignore[arg-type]
    )


# ──────────────────────────────────────────────────────────
# record_from_patch
# ──────────────────────────────────────────────────────────


def test_record_tier_classification_is_regex_only_when_both_axes_high() -> None:
    p = _patch(anchor_conf=0.95, new_text_conf=0.95, new_text=("x",))
    rec = record_from_patch(p, modifier_excerpt_len=2000)
    assert rec.tier == "regex_only"


def test_record_tier_escalates_to_hard_for_big_modifier() -> None:
    p = _patch(anchor_conf=0.5, new_text_conf=0.5)
    rec = record_from_patch(p, modifier_excerpt_len=5000)
    assert rec.tier == "hard"


def test_record_carries_apply_result_when_provided() -> None:
    p = _patch(anchor_conf=0.95, new_text_conf=0.95, new_text=("x",))
    result = PatchResult(status="applied", new_markdown="after")
    rec = record_from_patch(p, modifier_excerpt_len=500, apply_result=result)
    assert rec.apply_status == "applied"


# ──────────────────────────────────────────────────────────
# build_report
# ──────────────────────────────────────────────────────────


def test_build_report_rolls_up_coverage_and_verbs() -> None:
    # Three records: one regex-ready MODIFICA, one delete DEROGA, one
    # low-confidence MODIFICA that lands in "hard".
    records = [
        record_from_patch(
            _patch(verb="270", anchor_conf=0.95, new_text_conf=0.95, new_text=("t",)),
            modifier_excerpt_len=500,
        ),
        record_from_patch(
            _patch(verb="210", operation="delete", anchor_conf=1.0, new_text_conf=1.0),
            modifier_excerpt_len=500,
        ),
        record_from_patch(
            _patch(verb="270", anchor_conf=0.3, new_text_conf=0.3),
            modifier_excerpt_len=5000,
        ),
    ]
    report = build_report(records, sample_size=3, out_of_scope_count=1)

    assert report.sample_size == 3
    assert report.total_patches == 3
    assert report.out_of_scope_count == 1

    # 2 of 3 are regex-ready (MODIFICA with both conf high + DEROGA delete)
    assert report.regex_ready_pct == pytest.approx(2 / 3, rel=1e-3)

    # Verb counts are in the roll-up.
    assert report.per_verb_total == {"270": 2, "210": 1}
    # DEROGA is always "filled" (delete doesn't need text).
    # The MODIFICA with high confidence counts as filled too.
    assert report.per_verb_regex_filled["210"] == 1
    assert report.per_verb_regex_filled["270"] == 1


def test_build_report_applied_share_is_zero_when_no_applies_attempted() -> None:
    records = [
        record_from_patch(
            _patch(anchor_conf=0.95, new_text_conf=0.95, new_text=("t",)), modifier_excerpt_len=500
        ),
    ]
    report = build_report(records, sample_size=1)
    assert report.apply_attempted == 0
    assert report.applied_share == 0.0


def test_build_report_applied_share_is_meaningful_when_applies_exist() -> None:
    applied = PatchResult(status="applied", new_markdown="after")
    failed = PatchResult(status="anchor_not_found", new_markdown="unchanged")
    records = [
        record_from_patch(
            _patch(anchor_conf=1.0, new_text_conf=1.0, new_text=("x",)),
            modifier_excerpt_len=500,
            apply_result=applied,
        ),
        record_from_patch(
            _patch(anchor_conf=0.5, new_text_conf=0.5),
            modifier_excerpt_len=500,
            apply_result=failed,
        ),
    ]
    report = build_report(records, sample_size=2)
    assert report.apply_attempted == 2
    assert report.applied_share == 0.5
    assert report.apply_status_counts == {"applied": 1, "anchor_not_found": 1}


def test_build_report_confidence_histograms_have_10_buckets() -> None:
    records = [
        record_from_patch(
            _patch(anchor_conf=0.05, new_text_conf=0.95, new_text=("x",)), modifier_excerpt_len=500
        ),
        record_from_patch(_patch(anchor_conf=0.95, new_text_conf=0.05), modifier_excerpt_len=500),
    ]
    report = build_report(records, sample_size=2)
    assert sum(report.anchor_confidence_buckets) == 2
    assert report.anchor_confidence_buckets[0] == 1  # 0.05 → bucket 0
    assert report.anchor_confidence_buckets[9] == 1  # 0.95 → bucket 9


def test_build_report_handles_empty_records() -> None:
    report = build_report([], sample_size=0)
    assert report.total_patches == 0
    assert report.regex_ready_pct == 0.0
    assert report.apply_attempted == 0


# ──────────────────────────────────────────────────────────
# render_markdown_summary
# ──────────────────────────────────────────────────────────


def test_render_summary_emits_expected_sections() -> None:
    records = [
        record_from_patch(
            _patch(verb="270", anchor_conf=0.95, new_text_conf=0.95, new_text=("t",)),
            modifier_excerpt_len=500,
        ),
    ]
    report = build_report(records, sample_size=1)
    summary = render_markdown_summary(report)

    for section in (
        "Stage C fidelity report",
        "Coverage",
        "Per-verb breakdown",
        "Case-tier routing",
        "Confidence histograms",
    ):
        assert section in summary, f"missing section {section!r}"


def test_render_summary_handles_empty_report_without_crashing() -> None:
    report = build_report([], sample_size=0)
    summary = render_markdown_summary(report)
    assert "Stage C fidelity report" in summary
