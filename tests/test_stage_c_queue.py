"""Tests for the Claude-resolver queue (src/legalize/llm/queue.py,
src/legalize/llm/resolver.py).

Covers: classification thresholds, queue idempotency, batch emission,
resolution round-trip, resolver prompt/response plumbing.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.llm.queue import (
    CaseResolution,
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


# ──────────────────────────────────────────────────────────
# classify_case
# ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "anchor, new_text, excerpt_len, expected",
    [
        (0.95, 0.95, 200, "regex_only"),  # both axes clean
        (0.95, 0.95, 5000, "regex_only"),  # size doesn't matter when regex nailed it
        (0.5, 0.5, 400, "short"),  # small excerpt + weak axis
        (0.5, 0.5, 1000, "medium"),  # medium excerpt, moderate axes
        (0.3, 0.3, 1400, "medium"),  # both weak but excerpt below 1500 → still medium
        (0.3, 0.3, 1800, "hard"),  # both VERY weak (<0.5) + excerpt > 1500 → hard
        (0.1, 0.1, 4500, "hard"),  # huge excerpt → always hard
        (0.1, 0.95, 4500, "hard"),  # anchor weak + huge excerpt → hard
    ],
)
def test_classify_case_routing(anchor, new_text, excerpt_len, expected):
    assert (
        classify_case(
            anchor_confidence=anchor,
            new_text_confidence=new_text,
            modifier_excerpt_len=excerpt_len,
        )
        == expected
    )


# ──────────────────────────────────────────────────────────
# case_id_for
# ──────────────────────────────────────────────────────────


def _make_patch(target="BOE-A-2017-14334", source="BOE-A-2021-21666", verb="270") -> AmendmentPatch:
    return AmendmentPatch(
        target_id=target,
        operation="replace",
        verb_code=verb,
        verb_text="MODIFICA",
        anchor_hint="art. 5, apartado 2",
        source_boe_id=source,
        source_date=date(2021, 12, 29),
    )


def test_case_id_for_is_deterministic():
    p = _make_patch()
    assert case_id_for(p) == case_id_for(p)


def test_case_id_for_differs_by_target_source_verb_or_anchor():
    base = _make_patch()
    assert case_id_for(base) != case_id_for(_make_patch(target="BOE-A-X"))
    assert case_id_for(base) != case_id_for(_make_patch(source="BOE-A-X"))
    assert case_id_for(base) != case_id_for(_make_patch(verb="407"))


# ──────────────────────────────────────────────────────────
# PendingCaseQueue basics
# ──────────────────────────────────────────────────────────


def _make_case(cid: str, target: str = "BOE-A-X") -> PendingCase:
    return PendingCase(
        case_id=cid,
        target_id=target,
        source_boe_id="BOE-A-Y",
        source_date="2021-12-29",
        operation="replace",
        verb_code="270",
        verb_text="MODIFICA",
        anchor_hint="art. 5",
        modifier_excerpt="se modifica el art. 5 por «texto nuevo»",
        base_context="###### Artículo 5\n\nTexto anterior del artículo 5.",
    )


def test_queue_enqueue_and_read(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path)
    q.enqueue(_make_case("abc"))
    q.enqueue(_make_case("def"))
    cases = q.pending_cases()
    assert [c.case_id for c in cases] == ["abc", "def"]


def test_queue_enqueue_is_idempotent(tmp_path: Path) -> None:
    """Re-enqueuing the same case_id is a no-op; callers don't need to dedupe."""
    q = PendingCaseQueue(tmp_path)
    q.enqueue(_make_case("abc"))
    q.enqueue(_make_case("abc"))  # duplicate
    q.enqueue(_make_case("abc"))  # triplicate
    assert [c.case_id for c in q.pending_cases()] == ["abc"]


def test_queue_resolution_round_trip(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path)
    q.enqueue(_make_case("abc"))

    assert q.unresolved_cases()  # before resolution, case is pending

    q.record_resolution(
        CaseResolution(
            case_id="abc",
            operation="replace",
            new_text=("texto resuelto",),
            anchor_confidence=0.95,
            new_text_confidence=0.95,
            reason="clean match in modifier excerpt",
        )
    )

    assert not q.unresolved_cases()
    res = q.resolutions()["abc"]
    assert res.new_text == ("texto resuelto",)
    assert res.anchor_confidence == 0.95


def test_queue_resolution_is_idempotent(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path)
    q.enqueue(_make_case("abc"))
    r = CaseResolution(
        case_id="abc",
        operation="replace",
        new_text=("first",),
        anchor_confidence=0.9,
        new_text_confidence=0.9,
    )
    q.record_resolution(r)
    # Second call with different content → silently ignored (first write wins)
    q.record_resolution(
        CaseResolution(
            case_id="abc",
            operation="replace",
            new_text=("OVERWRITE",),
            anchor_confidence=0.1,
            new_text_confidence=0.1,
        )
    )
    assert q.resolutions()["abc"].new_text == ("first",)


def test_apply_resolution_returns_none_when_pending(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path)
    case = _make_case("abc")
    q.enqueue(case)
    patch = _make_patch()
    assert q.apply_resolution(case, patch) is None


def test_apply_resolution_enriches_patch(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path)
    case = _make_case("abc")
    q.enqueue(case)
    q.record_resolution(
        CaseResolution(
            case_id="abc",
            operation="replace",
            new_text=("resolved",),
            anchor_confidence=0.96,
            new_text_confidence=0.96,
        )
    )
    patch = _make_patch()
    out = q.apply_resolution(case, patch)
    assert out is not None
    assert out.new_text == ("resolved",)
    assert out.anchor_confidence == 0.96
    assert out.extractor == "claude_code"
    # metadata from the original patch is preserved
    assert out.target_id == patch.target_id
    assert out.source_boe_id == patch.source_boe_id


# ──────────────────────────────────────────────────────────
# Batch emission
# ──────────────────────────────────────────────────────────


def test_ready_for_batch_flips_when_threshold_reached(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path, batch_size=3)
    q.enqueue(_make_case("a"))
    q.enqueue(_make_case("b"))
    assert not q.ready_for_batch()
    q.enqueue(_make_case("c"))
    assert q.ready_for_batch()


def test_emit_batch_freezes_unresolved(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path, batch_size=3)
    for cid in ("a", "b", "c"):
        q.enqueue(_make_case(cid))

    path = q.emit_batch()
    assert path is not None
    assert path.name == "batch_000.jsonl"
    # Batch file should contain all three cases
    assert len(path.read_text().strip().splitlines()) == 3


def test_emit_batch_does_not_reemit_already_batched(tmp_path: Path) -> None:
    """Calling emit_batch twice without new cases coming in returns None
    — we don't want to duplicate work for the resolver."""
    q = PendingCaseQueue(tmp_path, batch_size=2)
    q.enqueue(_make_case("a"))
    q.enqueue(_make_case("b"))
    p1 = q.emit_batch()
    p2 = q.emit_batch()
    assert p1 is not None and p2 is None


def test_emit_batch_closes_when_resolutions_come_in(tmp_path: Path) -> None:
    """When all cases in a batch get resolved, that batch stops being
    'open' — a NEW batch can be emitted over fresh unresolved cases."""
    q = PendingCaseQueue(tmp_path, batch_size=2)
    q.enqueue(_make_case("a"))
    q.enqueue(_make_case("b"))
    b0 = q.emit_batch()
    assert b0 and b0.name == "batch_000.jsonl"

    # Resolve everything in batch_000
    for cid in ("a", "b"):
        q.record_resolution(
            CaseResolution(
                case_id=cid,
                operation="replace",
                new_text=(f"text for {cid}",),
                anchor_confidence=0.9,
                new_text_confidence=0.9,
            )
        )

    # Two fresh cases come in
    q.enqueue(_make_case("c"))
    q.enqueue(_make_case("d"))
    b1 = q.emit_batch()
    assert b1 and b1.name == "batch_001.jsonl"
    # And batch_000 is no longer listed as open
    assert b0 not in q.open_batches()
    assert b1 in q.open_batches()


# ──────────────────────────────────────────────────────────
# Resolver prompt & response
# ──────────────────────────────────────────────────────────


def test_build_resolver_prompt_contains_all_cases(tmp_path: Path) -> None:
    cases = [_make_case("a"), _make_case("b", target="BOE-A-OTHER")]
    prompt = build_resolver_prompt(cases)
    assert "CASO 1" in prompt and "CASO 2" in prompt
    assert "BOE-A-X" in prompt and "BOE-A-OTHER" in prompt
    for c in cases:
        assert c.case_id in prompt
        assert c.modifier_excerpt in prompt


def test_build_full_prompt_has_system_and_user_sections() -> None:
    prompt = build_full_prompt([_make_case("a")])
    assert "Legalize" in prompt  # from system prompt
    assert "LOTE DE 1 CASOS" in prompt
    assert "CASO 1" in prompt
    assert "new_text" in prompt  # schema hint from system prompt


def test_parse_resolutions_json_happy_path() -> None:
    raw = json.dumps(
        {
            "resolutions": [
                {
                    "case_id": "abc",
                    "operation": "replace",
                    "new_text": ["nuevo texto"],
                    "anchor_confidence": 0.95,
                    "new_text_confidence": 0.95,
                    "reason": "direct match",
                },
            ]
        }
    )
    out = parse_resolutions_json(raw)
    assert len(out) == 1
    assert out[0].case_id == "abc"
    assert out[0].new_text == ("nuevo texto",)
    assert out[0].resolver == "claude_code"


def test_parse_resolutions_json_tolerates_code_fence() -> None:
    raw = (
        "```json\n"
        + json.dumps(
            {
                "resolutions": [
                    {
                        "case_id": "x",
                        "operation": "delete",
                        "new_text": None,
                        "anchor_confidence": 0.9,
                        "new_text_confidence": 1.0,
                        "reason": "deroga",
                    },
                ]
            }
        )
        + "\n```\n"
    )
    out = parse_resolutions_json(raw)
    assert len(out) == 1
    assert out[0].operation == "delete"
    assert out[0].new_text is None


def test_parse_resolutions_json_drops_invalid_entries() -> None:
    raw = json.dumps(
        {
            "resolutions": [
                {
                    "case_id": "good",
                    "operation": "replace",
                    "new_text": ["ok"],
                    "anchor_confidence": 0.9,
                    "new_text_confidence": 0.9,
                },
                {
                    "case_id": "bad_op",
                    "operation": "rewrite",
                    "new_text": None,
                    "anchor_confidence": 0.5,
                    "new_text_confidence": 0.5,
                },
                {
                    "operation": "replace",
                    "new_text": ["missing case_id"],
                    "anchor_confidence": 0.5,
                    "new_text_confidence": 0.5,
                },
            ]
        }
    )
    out = parse_resolutions_json(raw)
    assert len(out) == 1
    assert out[0].case_id == "good"


def test_parse_resolutions_json_forces_delete_new_text_to_none() -> None:
    """A resolver that returns operation=delete but provides new_text is
    contradicting itself. The verb wins: new_text goes to None."""
    raw = json.dumps(
        {
            "resolutions": [
                {
                    "case_id": "x",
                    "operation": "delete",
                    "new_text": ["ignored"],
                    "anchor_confidence": 0.9,
                    "new_text_confidence": 0.9,
                },
            ]
        }
    )
    out = parse_resolutions_json(raw)
    assert out[0].new_text is None


def test_parse_resolutions_json_errors_on_malformed() -> None:
    with pytest.raises(ValueError):
        parse_resolutions_json("not json at all")
    with pytest.raises(ValueError):
        parse_resolutions_json('{"not_resolutions": []}')


def test_ingest_resolutions_is_idempotent(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path)
    q.enqueue(_make_case("a"))
    resolutions = [
        CaseResolution(
            case_id="a",
            operation="replace",
            new_text=("t",),
            anchor_confidence=0.9,
            new_text_confidence=0.9,
        ),
    ]
    added_first = ingest_resolutions(q, resolutions)
    added_second = ingest_resolutions(q, resolutions)
    assert added_first == 1
    assert added_second == 0
    assert len(q.resolutions()) == 1


def test_load_batch_round_trips_a_batch_file(tmp_path: Path) -> None:
    q = PendingCaseQueue(tmp_path, batch_size=2)
    q.enqueue(_make_case("a"))
    q.enqueue(_make_case("b"))
    path = q.emit_batch()
    assert path is not None
    loaded = load_batch(path)
    assert [c.case_id for c in loaded] == ["a", "b"]
