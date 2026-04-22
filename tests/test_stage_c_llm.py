"""Tests for src/legalize/llm/amendment_parser.py.

Every HTTP call is stubbed with the `responses` library so the suite
never touches the real Groq or Ollama endpoints. A separate smoke script
(scripts/groq_smoke.py) exercises the live API with GROQ_API_KEY and is
run manually, not in CI.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
import responses

from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.llm.amendment_parser import (
    GROQ_ESCALATION,
    AmendmentLLM,
    LLMConfig,
    LLMError,
    build_anchor_context,
)


GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


def _groq_ok(payload: dict, *, model: str = "openai/gpt-oss-20b") -> dict:
    """Shape of a successful Groq response with `payload` as JSON content."""
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 0,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": json.dumps(payload)},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 100, "completion_tokens": 30, "total_tokens": 130},
    }


def _make_llm(tmp_path: Path, **overrides) -> AmendmentLLM:
    cfg = LLMConfig(
        backend="groq",
        api_key="sk-test-fake",  # pragma: allowlist secret
        cache_dir=tmp_path / "cache",
        **overrides,
    )
    return AmendmentLLM(cfg)


# ──────────────────────────────────────────────────────────
# parse_difficult_case — happy path
# ──────────────────────────────────────────────────────────


@responses.activate
def test_parse_difficult_case_returns_structured_patch(tmp_path: Path) -> None:
    responses.add(
        responses.POST,
        GROQ_URL,
        json=_groq_ok(
            {
                "operation": "replace",
                "anchor": {
                    "article": "5",
                    "section": "2",
                    "letter": "c",
                    "ordinal": None,
                    "subsection": None,
                    "free_text": None,
                },
                "new_text": ["c) Nuevo texto de la letra c del apartado 2 del articulo 5."],
                "confidence": 0.95,
                "reason": "Extracto contiene «...»",
            }
        ),
        status=200,
    )

    llm = _make_llm(tmp_path)
    patch = llm.parse_difficult_case(
        base_context="...articulo 5.2.c) Texto anterior...",
        modifier_excerpt="Se modifica la letra c) del apartado 2 del articulo 5 por «c) Nuevo texto...».",
        anchor_hint="la letra c) del apartado 2 del articulo 5",
        operation_hint="MODIFICA",
        target_id="BOE-A-2017-14334",
        source_boe_id="BOE-A-2021-21666",
        source_date=date(2021, 12, 29),
        verb_code="270",
        verb_text="MODIFICA",
    )

    assert patch.operation == "replace"
    assert patch.new_text == ("c) Nuevo texto de la letra c del apartado 2 del articulo 5.",)
    assert patch.extractor == "llm"
    assert patch.confidence == 0.95
    assert patch.target_id == "BOE-A-2017-14334"


@responses.activate
def test_parse_difficult_case_clamps_confidence_when_verb_mismatches(tmp_path: Path) -> None:
    """If the verb says MODIFICA (replace) but the model returns delete,
    that's a hallucination red flag — confidence must be clamped so the
    caller prefers a commit-pointer over applying the patch."""
    responses.add(
        responses.POST,
        GROQ_URL,
        json=_groq_ok(
            {
                "operation": "delete",
                "anchor": {
                    "article": "5",
                    "section": None,
                    "letter": None,
                    "ordinal": None,
                    "subsection": None,
                    "free_text": None,
                },
                "new_text": None,
                "confidence": 0.99,
                "reason": "bogus",
            }
        ),
        status=200,
    )
    llm = _make_llm(tmp_path)
    patch = llm.parse_difficult_case(
        base_context="...",
        modifier_excerpt="...",
        anchor_hint="art. 5",
        operation_hint="MODIFICA",
        target_id="BOE-A-X",
        source_boe_id="BOE-A-Y",
        source_date=date(2021, 1, 1),
        verb_code="270",  # MODIFICA → expects replace
        verb_text="MODIFICA",
    )
    # Operation is preserved (the model said delete) but we clamp confidence.
    assert patch.operation == "delete"
    assert patch.confidence <= 0.4


@responses.activate
def test_parse_difficult_case_rejects_invalid_operation(tmp_path: Path) -> None:
    responses.add(
        responses.POST,
        GROQ_URL,
        json=_groq_ok(
            {
                "operation": "rewrite",  # not in enum
                "anchor": {
                    "article": None,
                    "section": None,
                    "letter": None,
                    "ordinal": None,
                    "subsection": None,
                    "free_text": None,
                },
                "new_text": None,
                "confidence": 0.9,
                "reason": "invalid",
            }
        ),
        status=200,
    )
    # Every rung will return this same garbage, so we should escalate and
    # eventually raise. To keep the test fast, cap the escalation to 1 rung.
    llm = _make_llm(tmp_path, escalation=("openai/gpt-oss-20b",))
    with pytest.raises(LLMError):
        llm.parse_difficult_case(
            base_context="",
            modifier_excerpt="",
            anchor_hint="",
            operation_hint="",
            target_id="X",
            source_boe_id="Y",
            source_date=date.today(),
        )


# ──────────────────────────────────────────────────────────
# Model escalation
# ──────────────────────────────────────────────────────────


@responses.activate
def test_escalates_to_next_model_on_low_confidence(tmp_path: Path) -> None:
    """First rung returns confidence below threshold; second rung clears it.
    The final patch must come from the second model."""

    # First call (20b): low confidence
    responses.add(
        responses.POST,
        GROQ_URL,
        json=_groq_ok(
            {
                "operation": "replace",
                "anchor": {
                    "article": "1",
                    "section": None,
                    "letter": None,
                    "ordinal": None,
                    "subsection": None,
                    "free_text": None,
                },
                "new_text": ["x"],
                "confidence": 0.5,
                "reason": "dudoso",
            },
            model="openai/gpt-oss-20b",
        ),
        status=200,
    )
    # Second call (120b): confident
    responses.add(
        responses.POST,
        GROQ_URL,
        json=_groq_ok(
            {
                "operation": "replace",
                "anchor": {
                    "article": "1",
                    "section": None,
                    "letter": None,
                    "ordinal": None,
                    "subsection": None,
                    "free_text": None,
                },
                "new_text": ["texto definitivo"],
                "confidence": 0.96,
                "reason": "ok",
            },
            model="openai/gpt-oss-120b",
        ),
        status=200,
    )

    llm = _make_llm(tmp_path, rung_threshold=0.8)
    patch = llm.parse_difficult_case(
        base_context="articulo 1",
        modifier_excerpt="se modifica el art. 1 por «texto definitivo»",
        anchor_hint="articulo 1",
        operation_hint="MODIFICA",
        target_id="BOE-A-X",
        source_boe_id="BOE-A-Y",
        source_date=date.today(),
    )
    assert patch.new_text == ("texto definitivo",)
    assert patch.confidence == 0.96
    # Two HTTP calls were made (escalation happened).
    assert len(responses.calls) == 2


@responses.activate
def test_every_model_fails_raises(tmp_path: Path) -> None:
    # Short ladder (2 models) always returns HTTP 500
    for _ in range(2):
        responses.add(responses.POST, GROQ_URL, json={"error": "oops"}, status=500)

    llm = _make_llm(
        tmp_path,
        escalation=("openai/gpt-oss-20b", "openai/gpt-oss-120b"),
    )
    with pytest.raises(LLMError):
        llm.parse_difficult_case(
            base_context="",
            modifier_excerpt="",
            anchor_hint="",
            operation_hint="",
            target_id="X",
            source_boe_id="Y",
            source_date=date.today(),
        )


# ──────────────────────────────────────────────────────────
# Cache
# ──────────────────────────────────────────────────────────


@responses.activate
def test_disk_cache_prevents_second_call(tmp_path: Path) -> None:
    responses.add(
        responses.POST,
        GROQ_URL,
        json=_groq_ok(
            {
                "operation": "replace",
                "anchor": {
                    "article": "1",
                    "section": None,
                    "letter": None,
                    "ordinal": None,
                    "subsection": None,
                    "free_text": None,
                },
                "new_text": ["first call"],
                "confidence": 0.95,
                "reason": "",
            }
        ),
        status=200,
    )

    llm = _make_llm(tmp_path)
    common = dict(
        base_context="articulo 1",
        modifier_excerpt="se modifica",
        anchor_hint="articulo 1",
        operation_hint="MODIFICA",
        target_id="BOE-A-X",
        source_boe_id="BOE-A-Y",
        source_date=date(2021, 1, 1),
    )

    p1 = llm.parse_difficult_case(**common)
    p2 = llm.parse_difficult_case(**common)

    assert p1.new_text == p2.new_text == ("first call",)
    assert len(responses.calls) == 1, "second call must be served from cache"
    # And the cache file exists on disk.
    cache_files = list((tmp_path / "cache").glob("*.json"))
    assert len(cache_files) == 1


# ──────────────────────────────────────────────────────────
# verify()
# ──────────────────────────────────────────────────────────


def test_verify_skips_delete_verbs(tmp_path: Path) -> None:
    """Delete patches never need verification; method returns ok without
    any HTTP activity."""
    llm = _make_llm(tmp_path)
    patch = AmendmentPatch(
        target_id="BOE-A-X",
        operation="delete",
        verb_code="210",
        verb_text="DEROGA",
        anchor_hint="la Ley X",
        source_boe_id="BOE-A-Y",
        source_date=date.today(),
    )
    result = llm.verify(patch=patch, base_context="any", modifier_excerpt="any")
    assert result.verdict == "ok"
    assert result.model_used == "skipped"


@responses.activate
def test_verify_returns_corrected_patch(tmp_path: Path) -> None:
    """When the LLM says wrong + provides a corrected structure, verify()
    must surface it as VerifyResult.corrected_patch."""
    responses.add(
        responses.POST,
        GROQ_URL,
        json=_groq_ok(
            {
                "verdict": "wrong",
                "confidence": 0.9,
                "reason": "el new_text propuesto no aparece literal",
                "corrected": {
                    "operation": "replace",
                    "anchor": {
                        "article": "5",
                        "section": "2",
                        "letter": "c",
                        "ordinal": None,
                        "subsection": None,
                        "free_text": None,
                    },
                    "new_text": ["texto correcto"],
                    "confidence": 0.95,
                    "reason": "",
                },
            }
        ),
        status=200,
    )

    llm = _make_llm(tmp_path)
    original = AmendmentPatch(
        target_id="BOE-A-X",
        operation="replace",
        verb_code="270",
        verb_text="MODIFICA",
        anchor_hint="letra c) del apartado 2 del articulo 5",
        source_boe_id="BOE-A-Y",
        source_date=date(2021, 1, 1),
        new_text=("texto propuesto INCORRECTO",),
        confidence=0.7,
        extractor="regex",
    )
    result = llm.verify(patch=original, base_context="base", modifier_excerpt="excerpt")

    assert result.verdict == "wrong"
    assert result.corrected_patch is not None
    assert result.corrected_patch.new_text == ("texto correcto",)
    assert result.corrected_patch.extractor == "llm"
    # target/source metadata is preserved from the original patch.
    assert result.corrected_patch.target_id == "BOE-A-X"
    assert result.corrected_patch.source_boe_id == "BOE-A-Y"


# ──────────────────────────────────────────────────────────
# Anchor context helper
# ──────────────────────────────────────────────────────────


def test_build_anchor_context_finds_law_identifier() -> None:
    base = "A" * 1000 + "Ley 37/1992, de 28 de diciembre, del IVA, articulo 20" + "B" * 1000
    ctx = build_anchor_context(base, anchor_hint="art. 20 de la Ley 37/1992", window=300)
    assert "Ley 37/1992" in ctx
    assert len(ctx) <= 300


def test_build_anchor_context_falls_back_to_head() -> None:
    base = "texto sin anclas identificables" * 100
    ctx = build_anchor_context(base, anchor_hint="referencia inexistente", window=200)
    assert len(ctx) == 200
    assert ctx == base[:200]


def test_build_anchor_context_empty_inputs() -> None:
    assert build_anchor_context("", "anything") == ""
    assert build_anchor_context("some text", "") == "some text"


# ──────────────────────────────────────────────────────────
# Config plumbing
# ──────────────────────────────────────────────────────────


def test_default_escalation_is_groq_cheapest_first() -> None:
    cfg = LLMConfig()
    assert cfg.resolved_escalation()[0] == "openai/gpt-oss-20b"
    assert cfg.resolved_escalation() == GROQ_ESCALATION


def test_ollama_backend_uses_local_url() -> None:
    cfg = LLMConfig(backend="ollama")
    assert cfg.resolved_base_url().startswith("http://localhost:11434")
    assert cfg.resolved_api_key() is None  # ollama has no auth
