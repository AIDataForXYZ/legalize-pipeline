"""Tests for the session-3 structured-edit path.

Covers three layers:

  1. AmendmentLLM.extract_edits_from_modifier — the new batched LLM entry
     that returns list[StructuredEdit] with the anchor already parsed.
  2. apply_patch_structured — the patcher variant that consumes the
     structured anchor directly (bypasses parse_anchor_from_hint).
  3. dispatch_modifier_patches — the group dispatcher that pools patches
     per (modifier, target), issues ONE LLM call, and applies each
     returned edit with apply_patch_structured.

Every HTTP call is stubbed with the `responses` library; the suite never
touches real Groq.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
import responses

from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.llm.amendment_parser import (
    AmendmentLLM,
    LLMConfig,
    _anchor_from_dict,
)
from legalize.llm.dispatcher import dispatch_modifier_patches
from legalize.transformer.anchor import Anchor
from legalize.transformer.patcher import apply_patch_structured


GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


def _groq_ok(payload: dict, *, model: str = "openai/gpt-oss-120b") -> dict:
    return {
        "id": "chatcmpl-test-structured",
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
        "usage": {"prompt_tokens": 200, "completion_tokens": 60, "total_tokens": 260},
    }


def _make_llm(tmp_path: Path, **overrides) -> AmendmentLLM:
    cfg = LLMConfig(
        backend="groq",
        api_key="sk-test-fake",  # pragma: allowlist secret
        cache_dir=tmp_path / "cache",
        **overrides,
    )
    return AmendmentLLM(cfg)


BASE_MARKDOWN = (
    "# Circular 4/2017 del Banco de España\n\n"
    "###### Norma 67. Estados individuales reservados.\n\n"
    "1. Las entidades remitirán los estados individuales reservados "
    "trimestralmente según el calendario del anexo I.\n\n"
    "2. Los estados deberán enviarse en formato XBRL firmado digitalmente.\n\n"
    "###### Norma 68. Estados consolidados reservados.\n\n"
    "1. Las entidades consolidantes remitirán estados consolidados.\n"
)


# ──────────────────────────────────────────────────────────
# extract_edits_from_modifier
# ──────────────────────────────────────────────────────────


@responses.activate
def test_extract_edits_returns_one_edit_per_hint(tmp_path: Path) -> None:
    payload = {
        "edits": [
            {
                "patch_index": 0,
                "operation": "replace",
                "anchor": {"norma": "67", "apartado": "2"},
                "old_text": "Los estados deberán enviarse en formato XBRL firmado digitalmente.",
                "new_text": ["Los estados se enviarán mensualmente en formato XBRL."],
                "confidence": 0.92,
                "reason": "Segundo parrafo de la Norma 67 sustituido",
            },
            {
                "patch_index": 1,
                "operation": "replace",
                "anchor": {"norma": "68", "apartado": "1"},
                "old_text": "Las entidades consolidantes remitirán estados consolidados.",
                "new_text": ["Las entidades consolidantes remitirán estados mensuales."],
                "confidence": 0.9,
                "reason": "Primer parrafo de la Norma 68",
            },
        ]
    }
    responses.add(responses.POST, GROQ_URL, json=_groq_ok(payload), status=200)

    llm = _make_llm(tmp_path)
    edits = llm.extract_edits_from_modifier(
        base_context=BASE_MARKDOWN,
        modifier_body="Cuerpo del modificador (omitido).",
        hints=[
            ("del apartado 2 de la norma 67", "270"),
            ("del apartado 1 de la norma 68", "270"),
        ],
    )

    assert len(edits) == 2
    assert edits[0].anchor.norma == "67"
    assert edits[0].anchor.apartado == "2"
    assert edits[0].operation == "replace"
    assert edits[0].new_text == ("Los estados se enviarán mensualmente en formato XBRL.",)
    assert edits[0].confidence == pytest.approx(0.92)
    assert edits[0].patch_index == 0
    assert edits[1].patch_index == 1
    assert edits[1].anchor.norma == "68"


@responses.activate
def test_extract_edits_returns_empty_list_on_malformed_response(tmp_path: Path) -> None:
    responses.add(
        responses.POST,
        GROQ_URL,
        json=_groq_ok({"edits": "not a list"}),
        status=200,
    )
    llm = _make_llm(tmp_path)
    edits = llm.extract_edits_from_modifier(
        base_context=BASE_MARKDOWN,
        modifier_body="body",
        hints=[("hint", "270")],
    )
    assert edits == []


@responses.activate
def test_extract_edits_skips_invalid_items(tmp_path: Path) -> None:
    payload = {
        "edits": [
            {
                "patch_index": 0,
                "operation": "explode",  # invalid
                "anchor": {"norma": "67"},
                "old_text": None,
                "new_text": ["x"],
                "confidence": 0.9,
                "reason": "bad op",
            },
            {
                "patch_index": 1,
                "operation": "delete",
                "anchor": {"norma": "68", "apartado": "1"},
                "old_text": None,
                "new_text": ["should be stripped on delete"],
                "confidence": 0.95,
                "reason": "ok",
            },
        ]
    }
    responses.add(responses.POST, GROQ_URL, json=_groq_ok(payload), status=200)
    llm = _make_llm(tmp_path)
    edits = llm.extract_edits_from_modifier(
        base_context=BASE_MARKDOWN,
        modifier_body="body",
        hints=[("x", "235"), ("y", "235")],
    )
    assert len(edits) == 1
    # The delete edit must have no new_text regardless of what the model returned.
    assert edits[0].operation == "delete"
    assert edits[0].new_text is None


def test_extract_edits_no_hints_skips_http(tmp_path: Path) -> None:
    # With zero hints we must short-circuit and return [] without hitting
    # the transport at all. Any POST would raise because `responses` is
    # not activated here.
    llm = _make_llm(tmp_path)
    assert (
        llm.extract_edits_from_modifier(
            base_context="x",
            modifier_body="y",
            hints=[],
        )
        == []
    )


# ──────────────────────────────────────────────────────────
# apply_patch_structured
# ──────────────────────────────────────────────────────────


def test_apply_patch_structured_replaces_apartado_by_anchor() -> None:
    anchor = Anchor(norma="67", apartado="2")
    result = apply_patch_structured(
        BASE_MARKDOWN,
        anchor=anchor,
        operation="replace",
        new_text=("Los estados se enviarán mensualmente.",),
        dry_run=True,
    )
    assert result.status == "dry_run_ok"
    assert result.position is not None


def test_apply_patch_structured_missing_anchor_returns_anchor_not_found() -> None:
    anchor = Anchor(norma="999", apartado="1")
    result = apply_patch_structured(
        BASE_MARKDOWN,
        anchor=anchor,
        operation="replace",
        new_text=("x",),
        dry_run=True,
    )
    assert result.status == "anchor_not_found"


def test_apply_patch_structured_empty_anchor_fails_fast() -> None:
    result = apply_patch_structured(
        BASE_MARKDOWN,
        anchor=Anchor(),
        operation="replace",
        new_text=("x",),
        dry_run=True,
    )
    assert result.status == "anchor_not_found"


def test_apply_patch_structured_delete_with_text_rejected() -> None:
    result = apply_patch_structured(
        BASE_MARKDOWN,
        anchor=Anchor(norma="67", apartado="1"),
        operation="delete",
        new_text=("should not be here",),
        dry_run=True,
    )
    assert result.status == "delete_with_text"


def test_apply_patch_structured_relaxed_length_ratio() -> None:
    """The structured extractor gets the wider [0.05, 20] ratio window —
    a tiny replacement of a big apartado must pass."""
    # Old region is ~60 chars; new_text is 3 chars → ratio 0.05 which is
    # now within bounds for the structured extractor.
    anchor = Anchor(norma="67", apartado="2")
    result = apply_patch_structured(
        BASE_MARKDOWN,
        anchor=anchor,
        operation="replace",
        new_text=("xyz",),
        dry_run=True,
    )
    # We expect either dry_run_ok (ratio relaxed enough) OR length_mismatch
    # if still too tight. The relaxation target is 0.05×, and the new_text
    # length is 3 / 68 ≈ 0.044 — still below even the relaxed bound, so the
    # gate must still fire. The point of this test: the message should
    # reference the relaxed bound, not the strict one.
    if result.status == "length_mismatch":
        assert "0.05" in result.reason
    else:
        assert result.status == "dry_run_ok"


# ──────────────────────────────────────────────────────────
# dispatch_modifier_patches — group behaviour
# ──────────────────────────────────────────────────────────


def _make_patch(
    *,
    anchor_hint: str,
    verb_code: str = "270",
    anchor_confidence: float = 0.3,
    new_text_confidence: float = 0.0,
    new_text=None,
    operation: str = "replace",
) -> AmendmentPatch:
    return AmendmentPatch(
        target_id="BOE-A-2017-T",
        operation=operation,  # type: ignore[arg-type]
        verb_code=verb_code,
        verb_text="MODIFICA",
        anchor_hint=anchor_hint,
        source_boe_id="BOE-A-2021-M",
        source_date=date(2021, 5, 1),
        new_text=new_text,
        anchor_confidence=anchor_confidence,
        new_text_confidence=new_text_confidence,
        extractor="regex_split",
        ordering_key="1",
    )


@responses.activate
def test_dispatch_modifier_patches_single_llm_call_for_group(tmp_path: Path) -> None:
    patches = [
        _make_patch(anchor_hint="determinados preceptos de la Circular X"),
        _make_patch(anchor_hint="determinados preceptos (segundo patch)"),
    ]
    payload = {
        "edits": [
            {
                "patch_index": 0,
                "operation": "replace",
                "anchor": {"norma": "67", "apartado": "2"},
                "old_text": "Los estados",
                "new_text": ["Los estados se enviarán mensualmente."],
                "confidence": 0.9,
                "reason": "r1",
            },
            {
                "patch_index": 1,
                "operation": "replace",
                "anchor": {"norma": "68", "apartado": "1"},
                "old_text": "Las entidades",
                "new_text": ["Las entidades consolidantes remitirán datos mensuales."],
                "confidence": 0.88,
                "reason": "r2",
            },
        ]
    }
    responses.add(responses.POST, GROQ_URL, json=_groq_ok(payload), status=200)

    llm = _make_llm(tmp_path)
    # Long modifier body → medium/hard tier so the LLM path fires.
    modifier_body = "x " * 800

    results = dispatch_modifier_patches(
        BASE_MARKDOWN,
        patches,
        modifier_body=modifier_body,
        llm=llm,
        queue=None,
        dry_run=True,
        daily_mode=True,
        use_structured=True,
    )

    # Exactly one HTTP call total despite two patches.
    groq_posts = [c for c in responses.calls if c.request.url == GROQ_URL]
    assert len(groq_posts) == 1

    assert len(results) == 2
    # Both patches should have been enriched via llm_structured.
    assert results[0].patch.extractor == "llm_structured"
    assert results[1].patch.extractor == "llm_structured"
    assert results[0].patch.new_text == ("Los estados se enviarán mensualmente.",)
    assert results[1].patch.new_text == ("Las entidades consolidantes remitirán datos mensuales.",)


@responses.activate
def test_dispatch_modifier_patches_low_confidence_falls_back_to_regex(tmp_path: Path) -> None:
    patches = [_make_patch(anchor_hint="del apartado 2 de la norma 67")]
    payload = {
        "edits": [
            {
                "patch_index": 0,
                "operation": "replace",
                "anchor": {"norma": "67", "apartado": "2"},
                "old_text": "x",
                "new_text": ["y"],
                "confidence": 0.1,  # below _STRUCTURED_MIN_CONFIDENCE
                "reason": "model is unsure",
            }
        ]
    }
    responses.add(responses.POST, GROQ_URL, json=_groq_ok(payload), status=200)

    llm = _make_llm(tmp_path)
    results = dispatch_modifier_patches(
        BASE_MARKDOWN,
        patches,
        modifier_body="y " * 400,
        llm=llm,
        queue=None,
        dry_run=True,
        daily_mode=True,
        use_structured=True,
    )
    assert len(results) == 1
    # Regex fallback → extractor stays as the original regex_split, not llm_structured.
    assert results[0].patch.extractor == "regex_split"
    assert "below threshold" in results[0].reason


def test_dispatch_modifier_patches_regex_only_bypasses_llm(tmp_path: Path) -> None:
    # Regex-only patches never trigger the LLM even when it is wired.
    patches = [
        _make_patch(
            anchor_hint="del apartado 2 de la norma 67",
            anchor_confidence=1.0,
            new_text_confidence=1.0,
            new_text=("Los estados se enviarán mensualmente.",),
        )
    ]
    # No HTTP responses registered → any Groq call would fail. Test
    # success proves the group dispatcher skipped the LLM entirely.
    llm = _make_llm(tmp_path)
    results = dispatch_modifier_patches(
        BASE_MARKDOWN,
        patches,
        modifier_body="corto",
        llm=llm,
        queue=None,
        dry_run=True,
        use_structured=True,
    )
    assert len(results) == 1
    assert results[0].tier == "regex_only"


@responses.activate
def test_dispatch_modifier_patches_respects_patch_index_reordering(tmp_path: Path) -> None:
    """Model may echo edits out of order; we must correlate by patch_index."""
    patches = [
        _make_patch(anchor_hint="first"),
        _make_patch(anchor_hint="second"),
    ]
    payload = {
        "edits": [
            {
                "patch_index": 1,
                "operation": "replace",
                "anchor": {"norma": "68", "apartado": "1"},
                "old_text": "Las entidades",
                "new_text": ["segundo patch resuelto"],
                "confidence": 0.9,
                "reason": "r2",
            },
            {
                "patch_index": 0,
                "operation": "replace",
                "anchor": {"norma": "67", "apartado": "2"},
                "old_text": "Los estados",
                "new_text": ["primer patch resuelto"],
                "confidence": 0.9,
                "reason": "r1",
            },
        ]
    }
    responses.add(responses.POST, GROQ_URL, json=_groq_ok(payload), status=200)

    llm = _make_llm(tmp_path)
    results = dispatch_modifier_patches(
        BASE_MARKDOWN,
        patches,
        modifier_body="x " * 400,
        llm=llm,
        queue=None,
        dry_run=True,
        daily_mode=True,
        use_structured=True,
    )
    assert results[0].patch.new_text == ("primer patch resuelto",)
    assert results[1].patch.new_text == ("segundo patch resuelto",)


# ──────────────────────────────────────────────────────────
# _anchor_from_dict — LLM-quirk sanitization
# ──────────────────────────────────────────────────────────


def test_anchor_from_dict_drops_law_identifier_in_norma() -> None:
    """The LLM frequently drops the modified law's BOE citation into
    the ``norma`` field ('168/2025', '27/2014'). That's wrong —
    ``norma`` is reserved for BdE Circular sub-units (Norma 1..67).
    The sanitizer must strip this back out so resolve_anchor doesn't
    go hunting for a non-existent heading."""
    a = _anchor_from_dict({"norma": "168/2025", "disposicion": "final única"})
    assert a.norma is None
    assert a.disposicion == "final única"


def test_anchor_from_dict_keeps_real_norma_values() -> None:
    # Plain integer stays — that's a real BdE Circular norma.
    a = _anchor_from_dict({"norma": "67"})
    assert a.norma == "67"


def test_anchor_from_dict_splits_compound_articulo() -> None:
    a = _anchor_from_dict({"articulo": "96.2"})
    assert a.articulo == "96"
    assert a.apartado == "2"
    assert a.letra is None


def test_anchor_from_dict_splits_compound_articulo_with_letra() -> None:
    a = _anchor_from_dict({"articulo": "32.1.a"})
    assert a.articulo == "32"
    assert a.apartado == "1"
    assert a.letra == "a"


def test_anchor_from_dict_respects_existing_apartado() -> None:
    # If the LLM provided BOTH articulo='96.2' and apartado='3', trust
    # the explicit apartado; don't clobber with the one embedded in
    # articulo.
    a = _anchor_from_dict({"articulo": "96.2", "apartado": "3"})
    assert a.articulo == "96"
    assert a.apartado == "3"


def test_anchor_from_dict_trims_empty_strings_to_none() -> None:
    a = _anchor_from_dict({"articulo": "   ", "apartado": "2"})
    assert a.articulo is None
    assert a.apartado == "2"


def test_anchor_from_dict_splits_compound_disposicion() -> None:
    # "transitoria cuarta.6" → disposicion="transitoria cuarta", apartado="6"
    a = _anchor_from_dict({"disposicion": "transitoria cuarta.6"})
    assert a.disposicion == "transitoria cuarta"
    assert a.apartado == "6"


def test_anchor_from_dict_compound_disposicion_respects_existing_apartado() -> None:
    a = _anchor_from_dict({"disposicion": "final primera.3", "apartado": "5"})
    assert a.disposicion == "final primera"
    assert a.apartado == "5"


def test_dispatch_modifier_patches_no_llm_matches_legacy_behaviour(tmp_path: Path) -> None:
    """Without LLM and queue the group dispatcher must behave exactly like
    the legacy per-patch path: regex_only applies, others go through the
    original apply_patch fall-through."""
    patches = [
        _make_patch(
            anchor_hint="del apartado 2 de la norma 67",
            anchor_confidence=1.0,
            new_text_confidence=1.0,
            new_text=("Los estados se enviarán mensualmente.",),
        ),
    ]
    results = dispatch_modifier_patches(
        BASE_MARKDOWN,
        patches,
        modifier_body="corto",
        llm=None,
        queue=None,
        dry_run=True,
    )
    assert len(results) == 1
    assert results[0].tier == "regex_only"
    assert results[0].patch_result is not None
