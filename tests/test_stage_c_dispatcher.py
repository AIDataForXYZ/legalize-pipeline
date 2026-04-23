"""Tier routing integration tests for llm/dispatcher.py.

Exercises the four classify_case branches end-to-end:

  regex_only → apply_patch directly (no LLM, no queue)
  short      → AmendmentLLM.parse_difficult_case (short modifier)
  medium     → AmendmentLLM.parse_difficult_case (mid-size modifier)
  hard       → PendingCaseQueue.enqueue; next run w/ resolution applies

The LLM is stubbed via a tiny fake class that records every call. Same
trick for the queue — we hit the real PendingCaseQueue but on a tmp_path
so the JSONL streams don't leak between tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pytest

from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.llm.dispatcher import dispatch_patch, extract_modifier_body_text
from legalize.llm.queue import (
    CaseResolution,
    PendingCaseQueue,
    case_id_for,
    classify_case,
)


BASE_MARKDOWN = (
    "# Circular 4/2017\n\n"
    "###### Norma 67. Estados individuales reservados.\n\n"
    "1. Las entidades remitiran los estados.\n\n"
    "2. Los estados se envian trimestralmente.\n"
)


# ──────────────────────────────────────────────────────────
# Fakes
# ──────────────────────────────────────────────────────────


@dataclass
class _FakeLLM:
    """Records calls; returns a patch with caller-supplied new_text."""

    new_text: tuple[str, ...] | None = ("Texto inyectado por el fake.",)
    operation: str = "replace"
    confidence: float = 0.95
    calls: list[dict] = field(default_factory=list)

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
        self.calls.append(
            {
                "base_context_len": len(base_context),
                "modifier_excerpt_len": len(modifier_excerpt),
                "anchor_hint": anchor_hint,
                "operation_hint": operation_hint,
            }
        )
        return AmendmentPatch(
            target_id=target_id,
            operation=self.operation,  # type: ignore[arg-type]
            verb_code=verb_code,
            verb_text=verb_text,
            anchor_hint=anchor_hint,
            source_boe_id=source_boe_id,
            source_date=source_date,
            new_text=self.new_text,
            anchor_confidence=self.confidence,
            new_text_confidence=self.confidence,
            extractor="llm_parse",
            ordering_key=ordering_key,
        )


# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────


def _make_patch(
    *,
    anchor_confidence: float,
    new_text_confidence: float,
    new_text: tuple[str, ...] | None = ("Los estados se envian mensualmente.",),
    operation: str = "replace",
    anchor_hint: str = "del apartado 2 de la norma 67",
) -> AmendmentPatch:
    return AmendmentPatch(
        target_id="BOE-A-2017-T",
        operation=operation,  # type: ignore[arg-type]
        verb_code="100",
        verb_text="MODIFICA",
        anchor_hint=anchor_hint,
        source_boe_id="BOE-A-2021-M",
        source_date=date(2021, 5, 1),
        new_text=new_text,
        anchor_confidence=anchor_confidence,
        new_text_confidence=new_text_confidence,
        extractor="regex",
        ordering_key="1",
    )


# ──────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────


def test_regex_only_tier_uses_apply_patch_no_llm_no_queue() -> None:
    patch = _make_patch(anchor_confidence=1.0, new_text_confidence=1.0)
    llm = _FakeLLM()

    result = dispatch_patch(
        BASE_MARKDOWN,
        patch,
        modifier_body="corto",
        llm=llm,
        queue=None,
        dry_run=True,
    )

    assert result.tier == "regex_only"
    assert llm.calls == []  # LLM must NOT be called in the regex_only tier
    assert result.patch_result is not None
    # status is "applied" (or "gate_failed" if the anchor doesn't resolve,
    # but the FakeLLM output was designed to succeed).
    assert result.status in ("applied", "gate_failed")


def test_short_tier_calls_llm_and_applies_enriched_patch() -> None:
    # Weak new_text_confidence + short modifier body → "short" tier.
    patch = _make_patch(
        anchor_confidence=0.5,
        new_text_confidence=0.0,
        new_text=None,  # regex couldn't extract text
    )
    llm = _FakeLLM(new_text=("Los estados se remitiran de forma mensual.",))
    short_body = "corto " * 20  # ~120 chars

    # Sanity: verify the tier we expect.
    assert (
        classify_case(
            anchor_confidence=0.5,
            new_text_confidence=0.0,
            modifier_excerpt_len=len(short_body),
        )
        == "short"
    )

    result = dispatch_patch(
        BASE_MARKDOWN,
        patch,
        modifier_body=short_body,
        llm=llm,
        queue=None,
        dry_run=True,
    )

    assert result.tier == "short"
    assert len(llm.calls) == 1
    # The enriched patch must carry the LLM's new_text, not the original None.
    assert result.patch.new_text == ("Los estados se remitiran de forma mensual.",)


def test_medium_tier_calls_llm_with_larger_excerpt() -> None:
    patch = _make_patch(anchor_confidence=0.6, new_text_confidence=0.0, new_text=None)
    llm = _FakeLLM()
    medium_body = "palabra " * 200  # ~1600 chars → medium

    assert (
        classify_case(
            anchor_confidence=0.6,
            new_text_confidence=0.0,
            modifier_excerpt_len=len(medium_body),
        )
        == "medium"
    )

    result = dispatch_patch(
        BASE_MARKDOWN,
        patch,
        modifier_body=medium_body,
        llm=llm,
        queue=None,
        dry_run=True,
    )

    assert result.tier == "medium"
    assert len(llm.calls) == 1
    assert llm.calls[0]["modifier_excerpt_len"] == len(medium_body)


def test_hard_tier_enqueues_without_calling_apply_patch(tmp_path: Path) -> None:
    patch = _make_patch(anchor_confidence=0.2, new_text_confidence=0.2, new_text=None)
    queue = PendingCaseQueue(tmp_path / "queue", batch_size=5)
    llm = _FakeLLM()
    long_body = "xxx " * 1000  # ~4000 chars → hard

    assert (
        classify_case(
            anchor_confidence=0.2,
            new_text_confidence=0.2,
            modifier_excerpt_len=len(long_body),
        )
        == "hard"
    )

    result = dispatch_patch(
        BASE_MARKDOWN,
        patch,
        modifier_body=long_body,
        llm=llm,
        queue=queue,
        dry_run=True,
    )

    assert result.tier == "hard"
    assert result.status == "queued"
    assert result.patch_result is None
    assert llm.calls == []  # hard cases bypass Groq, go straight to Claude queue
    pending = queue.pending_cases()
    assert len(pending) == 1
    assert pending[0].case_id == case_id_for(patch)


def test_hard_tier_applies_when_resolution_already_present(tmp_path: Path) -> None:
    patch = _make_patch(anchor_confidence=0.2, new_text_confidence=0.2, new_text=None)
    queue = PendingCaseQueue(tmp_path / "queue", batch_size=5)
    long_body = "xxx " * 1000

    cid = case_id_for(patch)
    queue.record_resolution(
        CaseResolution(
            case_id=cid,
            operation="replace",
            new_text=("Los estados se envian mensualmente.",),
            anchor_confidence=0.95,
            new_text_confidence=0.95,
            reason="resolved by Claude",
            resolver="claude_code",
        )
    )

    result = dispatch_patch(
        BASE_MARKDOWN,
        patch,
        modifier_body=long_body,
        llm=None,
        queue=queue,
        dry_run=True,
    )

    assert result.tier == "hard"
    assert result.status in ("resolved", "gate_failed")
    assert result.patch.extractor == "claude_code"
    assert result.patch.new_text == ("Los estados se envian mensualmente.",)


def test_llm_failure_falls_back_to_regex_path() -> None:
    """When the LLM raises LLMError, the dispatcher must NOT swallow the
    patch silently; it should fall back to applying the regex patch as-is
    (degraded mode) and mark status=llm_failed for observability."""
    from legalize.llm.amendment_parser import LLMError

    class _BrokenLLM(_FakeLLM):
        def parse_difficult_case(self, **kwargs):  # type: ignore[override]
            raise LLMError("simulated transport failure")

    patch = _make_patch(anchor_confidence=0.5, new_text_confidence=0.0)
    llm = _BrokenLLM()
    result = dispatch_patch(
        BASE_MARKDOWN,
        patch,
        modifier_body="corto",
        llm=llm,
        queue=None,
        dry_run=True,
    )

    assert result.status == "llm_failed"
    assert result.patch_result is not None  # regex fallback was tried


def test_daily_mode_routes_hard_tier_through_groq(tmp_path: Path) -> None:
    """In daily-cron mode the PendingCaseQueue is not an option (no Claude
    session to resolve), so the dispatcher must fall through to Groq even
    for hard cases. Without the flag the same patch goes to the queue."""
    patch = _make_patch(anchor_confidence=0.3, new_text_confidence=0.0, new_text=None)
    llm = _FakeLLM(new_text=("Texto extraido por Groq.",), confidence=0.92)
    queue = PendingCaseQueue(tmp_path / "q", batch_size=5)
    long_body = "xxx " * 2000

    assert (
        classify_case(
            anchor_confidence=0.3,
            new_text_confidence=0.0,
            modifier_excerpt_len=len(long_body),
        )
        == "hard"
    )

    # Without daily_mode → queued, no LLM call.
    result_bootstrap = dispatch_patch(
        BASE_MARKDOWN,
        patch,
        modifier_body=long_body,
        llm=llm,
        queue=queue,
        dry_run=True,
        daily_mode=False,
    )
    assert result_bootstrap.status == "queued"
    assert llm.calls == []

    # With daily_mode → Groq gets called, no enqueue.
    queue2 = PendingCaseQueue(tmp_path / "q2", batch_size=5)
    result_daily = dispatch_patch(
        BASE_MARKDOWN,
        patch,
        modifier_body=long_body,
        llm=llm,
        queue=queue2,
        dry_run=True,
        daily_mode=True,
    )
    assert result_daily.tier == "hard"
    assert result_daily.status in ("applied", "gate_failed")
    assert len(llm.calls) == 1
    assert queue2.pending_cases() == []  # nothing enqueued in daily mode


def test_extract_modifier_body_text_collapses_whitespace() -> None:
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <metadatos><identificador>BOE-A-TEST</identificador></metadatos>
  <texto>
    <p>Primer parrafo.</p>
    <p>Segundo   parrafo   con   espacios.</p>
  </texto>
</documento>
"""
    body = extract_modifier_body_text(xml)
    assert "Primer parrafo" in body
    assert "Segundo" in body


def test_extract_modifier_body_text_returns_empty_on_malformed_xml() -> None:
    assert extract_modifier_body_text(b"not xml <<<") == ""
    assert extract_modifier_body_text(b"") == ""


@pytest.mark.parametrize(
    "anchor_conf,new_text_conf,excerpt_len,expected_tier",
    [
        (1.0, 1.0, 100, "regex_only"),
        (0.5, 0.0, 500, "short"),
        (0.5, 0.0, 1500, "medium"),
        (0.2, 0.2, 4000, "hard"),
    ],
)
def test_classify_case_boundaries(
    anchor_conf: float, new_text_conf: float, excerpt_len: int, expected_tier: str
) -> None:
    """Guardrail test: if someone tunes the classifier thresholds, they
    must update this table too. Keeps live/fidelity numbers comparable
    across commits."""
    assert (
        classify_case(
            anchor_confidence=anchor_conf,
            new_text_confidence=new_text_conf,
            modifier_excerpt_len=excerpt_len,
        )
        == expected_tier
    )
