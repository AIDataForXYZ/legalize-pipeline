"""Tier-based patch dispatcher (Stage C LLM integration).

This module sits between ``parse_amendments`` (which emits AmendmentPatch
candidates annotated with two confidence axes) and ``apply_patch`` (which
mutates markdown when gates pass). Its job is to decide, per patch, which
backend actually resolves the ambiguity:

    regex_only  → apply_patch directly (no external call)
    short       → Groq gpt-oss-20b via AmendmentLLM.parse_difficult_case
    medium      → Groq gpt-oss-120b via AmendmentLLM.parse_difficult_case
    hard        → enqueue into PendingCaseQueue; pick up resolution if
                  already present (idempotent reruns)

The dispatcher is deliberately thin: it never chains LLM calls, never
spawns sub-agents, never writes to disk outside the queue. That keeps
StageCDriver and the live fidelity runner on the same code path — both
just call ``dispatch_patch`` per patch and aggregate the results.

Sub-agent invocation (resolving a batch of hard cases) is an orchestration
concern handled by the caller, not here. The dispatcher reports when a
batch is ready via ``queue.ready_for_batch()``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace as dc_replace
from datetime import datetime, timezone
from typing import Literal

from lxml import etree

from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.llm.amendment_parser import (
    AmendmentLLM,
    LLMError,
    StructuredEdit,
    build_anchor_context,
)
from legalize.llm.queue import (
    CaseTier,
    PendingCase,
    PendingCaseQueue,
    case_id_for,
    classify_case,
)
from legalize.transformer.anchor import Position
from legalize.transformer.patcher import (
    PatchResult,
    PatchStatus,
    apply_at_position,
    apply_patch,
    apply_patch_structured,
)

logger = logging.getLogger(__name__)


DispatchStatus = Literal[
    "applied",  # apply_patch returned status applied/dry_run_ok
    "gate_failed",  # apply_patch returned a non-applied status (anchor miss etc.)
    "llm_failed",  # short/medium tier: LLM call errored; fell back to regex
    "queued",  # hard tier: case enqueued, awaiting Claude-session resolution
    "resolved",  # hard tier: resolution already in queue, patch enriched + applied
]


@dataclass(frozen=True)
class DispatchResult:
    """Outcome of dispatching one patch through the tier router."""

    tier: CaseTier
    status: DispatchStatus
    patch: AmendmentPatch  # possibly enriched by LLM / resolution
    patch_result: PatchResult | None  # None when queued (we never called apply_patch)
    reason: str = ""

    @property
    def applied(self) -> bool:
        return (
            self.status in ("applied", "resolved")
            and self.patch_result is not None
            and (self.patch_result.status in ("applied", "dry_run_ok"))
        )


def extract_modifier_body_text(xml_bytes: bytes | str) -> str:
    """Return the flat text of a modifier's ``<texto>`` element.

    Used both to size the case (classify_case thresholds) and as the
    ``modifier_excerpt`` input for LLM prompts. Whitespace is collapsed so
    the length is meaningful regardless of source indentation. Returns an
    empty string when ``<texto>`` is missing or malformed.
    """
    if not xml_bytes:
        return ""
    try:
        if isinstance(xml_bytes, str):
            root = etree.fromstring(xml_bytes.encode("utf-8"))
        else:
            root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return ""
    texto = root.find("texto")
    if texto is None:
        return ""
    return " ".join(texto.itertext()).strip()


def dispatch_patch(
    markdown: str,
    patch: AmendmentPatch,
    *,
    modifier_body: str,
    llm: AmendmentLLM | None,
    queue: PendingCaseQueue | None,
    dry_run: bool = False,
    daily_mode: bool = False,
) -> DispatchResult:
    """Route ``patch`` to the right backend, call it, and return the outcome.

    The caller owns the markdown state; on status ``applied`` / ``resolved``
    it should advance to ``patch_result.new_markdown`` before dispatching
    the next patch.

    ``llm`` or ``queue`` can be None; in that case the dispatcher falls
    back to the plain regex path (current pre-LLM behaviour). This is the
    same code path the first year of Stage C fidelity runs exercised, so
    regressions are easy to detect.

    ``daily_mode`` routes ``hard`` cases through Groq instead of enqueuing
    them for Claude. This matches the feedback_stage_c_llm_policy memory:
    the nightly cron cannot spawn a Claude sub-agent, so hard cases either
    resolve via Groq or degrade to a commit-pointer. Bootstrap runs leave
    this False so hard cases go to the Claude queue.
    """
    tier = classify_case(
        anchor_confidence=patch.anchor_confidence,
        new_text_confidence=patch.new_text_confidence,
        modifier_excerpt_len=len(modifier_body),
    )

    if tier == "regex_only":
        result = apply_patch(markdown, patch, dry_run=dry_run)
        return DispatchResult(
            tier=tier,
            status="applied" if result.status in ("applied", "dry_run_ok") else "gate_failed",
            patch=patch,
            patch_result=result,
            reason=result.reason,
        )

    if tier in ("short", "medium"):
        return _dispatch_llm(
            markdown=markdown,
            patch=patch,
            modifier_body=modifier_body,
            llm=llm,
            tier=tier,
            dry_run=dry_run,
        )

    # tier == "hard"
    if daily_mode and llm is not None:
        return _dispatch_llm(
            markdown=markdown,
            patch=patch,
            modifier_body=modifier_body,
            llm=llm,
            tier="hard",
            dry_run=dry_run,
        )
    return _dispatch_hard(
        markdown=markdown,
        patch=patch,
        modifier_body=modifier_body,
        queue=queue,
        dry_run=dry_run,
    )


# ──────────────────────────────────────────────────────────
# short / medium — Groq
# ──────────────────────────────────────────────────────────


def _dispatch_llm(
    *,
    markdown: str,
    patch: AmendmentPatch,
    modifier_body: str,
    llm: AmendmentLLM | None,
    tier: CaseTier,
    dry_run: bool,
) -> DispatchResult:
    """Call AmendmentLLM.parse_difficult_case to enrich the patch, then
    apply. When the LLM is unavailable (None) or errors out, fall back to
    applying the regex patch as-is so behaviour degrades gracefully."""
    if llm is None:
        # Regression-safe fallback: try the patch as-is.
        result = apply_patch(markdown, patch, dry_run=dry_run)
        return DispatchResult(
            tier=tier,
            status="applied" if result.status in ("applied", "dry_run_ok") else "gate_failed",
            patch=patch,
            patch_result=result,
            reason="llm disabled; regex fallback: " + result.reason,
        )

    base_context = build_anchor_context(markdown, patch.anchor_hint)
    try:
        enriched = llm.parse_difficult_case(
            base_context=base_context,
            modifier_excerpt=modifier_body,
            anchor_hint=patch.anchor_hint,
            operation_hint=patch.operation,
            target_id=patch.target_id,
            source_boe_id=patch.source_boe_id,
            source_date=patch.source_date,
            verb_code=patch.verb_code,
            verb_text=patch.verb_text,
            ordering_key=patch.ordering_key,
        )
    except LLMError as e:
        logger.warning("LLM %s tier failed for %s: %s", tier, patch.source_boe_id, e)
        result = apply_patch(markdown, patch, dry_run=dry_run)
        return DispatchResult(
            tier=tier,
            status="llm_failed",
            patch=patch,
            patch_result=result,
            reason=f"llm error: {e}; regex fallback status={result.status}",
        )

    result = apply_patch(markdown, enriched, dry_run=dry_run)
    return DispatchResult(
        tier=tier,
        status="applied" if result.status in ("applied", "dry_run_ok") else "gate_failed",
        patch=enriched,
        patch_result=result,
        reason=result.reason,
    )


# ──────────────────────────────────────────────────────────
# hard — queue + (if resolved) apply
# ──────────────────────────────────────────────────────────


def _dispatch_hard(
    *,
    markdown: str,
    patch: AmendmentPatch,
    modifier_body: str,
    queue: PendingCaseQueue | None,
    dry_run: bool,
) -> DispatchResult:
    """Enqueue the case. If a resolution already exists (from a prior
    sub-agent run), apply the resolved patch immediately — this is what
    makes reruns idempotent: once Claude resolves a batch, the next
    dispatch through the same case path applies without re-asking."""
    if queue is None:
        result = apply_patch(markdown, patch, dry_run=dry_run)
        return DispatchResult(
            tier="hard",
            status="applied" if result.status in ("applied", "dry_run_ok") else "gate_failed",
            patch=patch,
            patch_result=result,
            reason="queue disabled; regex fallback: " + result.reason,
        )

    cid = case_id_for(patch)

    # Short-circuit: already resolved in a prior run.
    resolution = queue.resolutions().get(cid)
    if resolution is not None:
        enriched = dc_replace(
            patch,
            operation=resolution.operation,
            new_text=resolution.new_text,
            anchor_confidence=resolution.anchor_confidence,
            new_text_confidence=resolution.new_text_confidence,
            extractor="claude_code",
        )
        result = apply_patch(markdown, enriched, dry_run=dry_run)
        return DispatchResult(
            tier="hard",
            status="resolved" if result.status in ("applied", "dry_run_ok") else "gate_failed",
            patch=enriched,
            patch_result=result,
            reason=f"resolved by {resolution.resolver}: {resolution.reason}",
        )

    # First time we see this case — enqueue for the Claude resolver.
    case = PendingCase(
        case_id=cid,
        target_id=patch.target_id,
        source_boe_id=patch.source_boe_id,
        source_date=patch.source_date.isoformat(),
        operation=patch.operation,
        verb_code=patch.verb_code,
        verb_text=patch.verb_text,
        anchor_hint=patch.anchor_hint,
        modifier_excerpt=modifier_body,
        base_context=build_anchor_context(markdown, patch.anchor_hint, window=1000),
        ordering_key=patch.ordering_key,
        enqueued_at=datetime.now(timezone.utc).isoformat(),
    )
    queue.enqueue(case)
    return DispatchResult(
        tier="hard",
        status="queued",
        patch=patch,
        patch_result=None,
        reason="hard case enqueued for Claude resolver",
    )


# ──────────────────────────────────────────────────────────
# Group dispatch — one LLM call per (source, target)
# ──────────────────────────────────────────────────────────


# Minimum self-reported confidence per StructuredEdit before we trust the
# LLM to bypass the regex fallback.
#
# Started at 0.6 in session 3, but the Groq cache shows the model
# routinely returns 0.3-0.5 with a correctly-populated structural
# anchor (articulo/norma/apartado/letra) — it is lowballing its own
# certainty on the text identification even when the structural
# resolution is sound. With threshold=0.6 we dropped 107 such usable
# anchor_only edits on the Leyes corpus alone; dropping to 0.4 reaches
# them and lets the downstream patcher gates (anchor_not_found,
# empty_anchor, length_mismatch) do the real filtering. Regex path
# rung_threshold stays at 0.8 — different population.
_STRUCTURED_MIN_CONFIDENCE = 0.4


def dispatch_modifier_patches(
    markdown: str,
    patches: list[AmendmentPatch],
    *,
    modifier_body: str,
    llm: AmendmentLLM | None,
    queue: PendingCaseQueue | None,
    dry_run: bool = False,
    daily_mode: bool = False,
    use_structured: bool = True,
) -> list[DispatchResult]:
    """Dispatch every patch for one (modifier, target) pair in a single batch.

    Difference vs per-patch ``dispatch_patch``:

      - Patches whose tier is ``regex_only`` are applied exactly as before.
      - Patches needing LLM help (short/medium, or hard when daily_mode is
        True) are POOLED: one call to
        ``AmendmentLLM.extract_edits_from_modifier`` resolves all of them.
        Each returned StructuredEdit is applied with
        ``apply_patch_structured`` so the LLM's anchor object goes
        straight to the resolver — the old roundtrip through
        ``parse_anchor_from_hint`` is bypassed.
      - Hard patches without daily_mode go to the Claude queue as before.

    This is the session-3 primary win: instead of one LLM call per patch
    with a weak string hint, we do one call per modifier group with the
    full base + modifier context, and the LLM returns the anchor in a
    form the resolver consumes directly.

    The returned list is in the same order as ``patches``, so callers can
    correlate outcomes by index. ``working_md`` evolves across patches;
    callers that need to preserve that state (StageCDriver) should pass
    the markdown AFTER each applied patch, but this function already
    threads the cumulative mutation internally — just advance your own
    copy from the last ``DispatchResult.patch_result.new_markdown``.
    """
    if not patches:
        return []

    # Fast path 1: caller has no LLM wiring. Fall through to the per-patch
    # dispatcher, keeping behaviour identical to pre-structured runs.
    if llm is None and queue is None:
        working = markdown
        out: list[DispatchResult] = []
        for p in patches:
            r = dispatch_patch(
                working,
                p,
                modifier_body=modifier_body,
                llm=llm,
                queue=queue,
                dry_run=dry_run,
                daily_mode=daily_mode,
            )
            out.append(r)
            if r.applied and r.patch_result is not None:
                working = r.patch_result.new_markdown
        return out

    # Bucket each patch by its routing decision.
    tiers: list[CaseTier] = [
        classify_case(
            anchor_confidence=p.anchor_confidence,
            new_text_confidence=p.new_text_confidence,
            modifier_excerpt_len=len(modifier_body),
        )
        for p in patches
    ]

    # Indices that go through the structured LLM batch call.
    llm_indices: list[int] = []
    for i, (p, t) in enumerate(zip(patches, tiers)):
        if t == "regex_only":
            continue
        if t == "hard" and not daily_mode:
            continue  # goes to the Claude queue
        if llm is None:
            continue  # no llm wired — falls through to regex
        llm_indices.append(i)

    # If we don't have a use_structured flag or there's nothing to batch,
    # degrade to per-patch dispatch (legacy path). This keeps tests and
    # daily-mode runs that haven't opted into the structured extract
    # working unchanged.
    if not use_structured or not llm_indices:
        working = markdown
        out = []
        for p in patches:
            r = dispatch_patch(
                working,
                p,
                modifier_body=modifier_body,
                llm=llm,
                queue=queue,
                dry_run=dry_run,
                daily_mode=daily_mode,
            )
            out.append(r)
            if r.applied and r.patch_result is not None:
                working = r.patch_result.new_markdown
        return out

    # One LLM call for every patch that needs resolution. We send a bigger
    # base window than per-patch so the model sees more structure around
    # the hints.
    hints = [(patches[i].anchor_hint, patches[i].verb_code) for i in llm_indices]
    base_context = _build_group_base_context(
        markdown, [patches[i].anchor_hint for i in llm_indices]
    )

    try:
        edits = llm.extract_edits_from_modifier(
            base_context=base_context,
            modifier_body=modifier_body,
            hints=hints,
        )
    except LLMError as e:
        logger.warning(
            "LLM extract_edits_from_modifier failed for %s/%s: %s",
            patches[0].source_boe_id,
            patches[0].target_id,
            e,
        )
        edits = []

    # Correlate edits back to patches by patch_index (preferred) or by
    # position in the llm_indices list (fallback when the model didn't
    # echo indices back).
    edits_by_index: dict[int, StructuredEdit] = {}
    for i, e in enumerate(edits):
        if e.patch_index is not None and 0 <= e.patch_index < len(llm_indices):
            edits_by_index[llm_indices[e.patch_index]] = e
        elif i < len(llm_indices):
            edits_by_index.setdefault(llm_indices[i], e)

    # Session-4 change: two-phase apply to avoid cascading errors.
    #
    # Phase 1 (dry-run) — every patch resolves against the SAME original
    # ``markdown``, so a bad patch that would match "accidentally
    # uniquely" after a prior mutation can never fire. We record the
    # resolved position for each patch that passes the gates.
    #
    # Phase 2 (apply) — take the successful resolutions and apply them
    # bottom-up (sorted by ``position.line_start`` DESC). Each mutation
    # shifts lines at and below its anchor, but those lines have
    # already been consumed; the remaining higher-up positions stay
    # valid.
    #
    # The function returns ``DispatchResult`` objects in the SAME order
    # as the input patches; only the internal apply ordering changes.
    # Each successfully-applied result carries the final cumulative
    # ``new_markdown`` so the caller can pick it up uniformly.

    resolved: list[
        tuple[
            int, Position | None, PatchResult, StructuredEdit | None, AmendmentPatch, CaseTier, str
        ]
    ] = []
    # Reusable placeholder when no LLM was used for this patch.
    for i, (patch, tier) in enumerate(zip(patches, tiers)):
        if tier == "regex_only":
            res = apply_patch(markdown, patch, dry_run=True)
            resolved.append((i, res.position, res, None, patch, tier, ""))
            continue

        if tier == "hard" and not daily_mode:
            # Claude queue — resolved later in a separate pass since we
            # never touch markdown for queued cases.
            resolved.append(
                (
                    i,
                    None,
                    PatchResult(status="anchor_not_found", new_markdown=markdown, reason=""),
                    None,
                    patch,
                    tier,
                    "__queue__",
                )
            )
            continue

        edit = edits_by_index.get(i)
        has_resolvable_signal = edit is not None and (
            not edit.anchor.is_empty or (edit.old_text and len(edit.old_text.strip()) >= 40)
        )
        if (
            edit is not None
            and edit.confidence >= _STRUCTURED_MIN_CONFIDENCE
            and has_resolvable_signal
        ):
            res = apply_patch_structured(
                markdown,
                anchor=edit.anchor,
                operation=edit.operation,
                new_text=edit.new_text,
                dry_run=True,
                extractor="llm_structured",
                old_text=edit.old_text,
            )
            resolved.append((i, res.position, res, edit, patch, tier, "__llm__"))
            continue

        # LLM produced nothing usable — dry-run regex.
        res = apply_patch(markdown, patch, dry_run=True)
        resolved.append((i, res.position, res, edit, patch, tier, "__llm_fallback__"))

    # Phase 2: apply the survivors bottom-up on a single evolving
    # working markdown. Sort by line_start DESC so earlier-line patches
    # apply last (when they apply, lower lines have already been
    # mutated in place, but their byte positions haven't shifted).
    appliable = [r for r in resolved if r[2].status == "dry_run_ok" and r[1] is not None]
    appliable.sort(key=lambda r: -r[1].line_start)  # type: ignore[union-attr]

    working = markdown
    applied_indices: set[int] = set()
    position_by_index: dict[int, Position] = {}
    for i, position, dry_res, edit, patch, tier, tag in appliable:
        if position is None:
            continue
        # Key move: apply using the pre-resolved Position (no
        # re-anchoring on the evolving ``working`` markdown). This is
        # what keeps bottom-up safe — any prior mutation in this
        # loop happened at HIGHER line numbers, so our line indices
        # remain valid.
        op = edit.operation if (edit is not None and tag == "__llm__") else patch.operation
        new_text = edit.new_text if (edit is not None and tag == "__llm__") else patch.new_text
        try:
            working = apply_at_position(
                working,
                position=position,
                operation=op,
                new_text=new_text,
            )
        except Exception as err:  # defensive: malformed position
            logger.warning("apply_at_position failed for patch %d: %s", i, err)
            continue
        applied_indices.add(i)
        position_by_index[i] = position
        # Bump the stored result to "applied/dry_run_ok" so the summary
        # agrees with the actual outcome.
        new_status: PatchStatus = "dry_run_ok" if dry_run else "applied"
        # Overwrite the resolved tuple in place so the returned list
        # carries the post-mutation state.
        for idx, rec in enumerate(resolved):
            if rec[0] == i:
                resolved[idx] = (
                    i,
                    position,
                    PatchResult(
                        status=new_status,
                        new_markdown=markdown if dry_run else working,
                        reason="gates passed (bottom-up batch)",
                        position=position,
                    ),
                    edit,
                    patch,
                    tier,
                    tag,
                )
                break

    # Build the returned list in original patch order.
    out: list[DispatchResult] = []
    for i, position, res, edit, patch, tier, tag in sorted(resolved, key=lambda r: r[0]):
        if tag == "__queue__":
            out.append(
                _dispatch_hard(
                    markdown=working,
                    patch=patch,
                    modifier_body=modifier_body,
                    queue=queue,
                    dry_run=dry_run,
                )
            )
            continue
        if tag == "__llm__" and edit is not None:
            enriched = _patch_from_structured(patch, edit)
            # Surface the final cumulative working markdown on every
            # applied result so callers can pick it up uniformly.
            patch_result = (
                PatchResult(
                    status=res.status,
                    new_markdown=working if i in applied_indices else markdown,
                    reason=res.reason,
                    position=position,
                )
                if i in applied_indices
                else res
            )
            out.append(
                DispatchResult(
                    tier=tier,
                    status="applied"
                    if patch_result.status in ("applied", "dry_run_ok")
                    else "gate_failed",
                    patch=enriched,
                    patch_result=patch_result,
                    reason=f"structured: {(edit.reason if edit else '') or patch_result.reason}",
                )
            )
            continue

        # regex or LLM-fallback-to-regex path.
        patch_result = (
            PatchResult(
                status=res.status,
                new_markdown=working if i in applied_indices else markdown,
                reason=res.reason,
                position=position,
            )
            if i in applied_indices
            else res
        )
        if tag == "__llm_fallback__":
            status_str: DispatchStatus = (
                "applied"
                if patch_result.status in ("applied", "dry_run_ok")
                else ("llm_failed" if edit is None else "gate_failed")
            )
            reason = (
                "LLM returned no edit for this patch; regex fallback: " + patch_result.reason
                if edit is None
                else f"LLM confidence {edit.confidence:.2f} below threshold; regex fallback: {patch_result.reason}"
            )
            out.append(
                DispatchResult(
                    tier=tier,
                    status=status_str,
                    patch=patch,
                    patch_result=patch_result,
                    reason=reason,
                )
            )
            continue
        out.append(
            DispatchResult(
                tier=tier,
                status="applied"
                if patch_result.status in ("applied", "dry_run_ok")
                else "gate_failed",
                patch=patch,
                patch_result=patch_result,
                reason=patch_result.reason,
            )
        )
    return out


_BASE_CONTEXT_SOFT_CAP = 24000
# 24K chars ~ 6K tokens for Spanish text — cheap on Groq gpt-oss-120b
# (128K context) and big enough that most BdE Circulares / short Leyes
# fit entirely. For longer Leyes we splice per-hint windows as before.


def _build_group_base_context(markdown: str, hints: list[str]) -> str:
    """Build a base-context window that covers every hint in the group.

    Previous behaviour used a ~1200-char window per hint, which worked for
    the synthetic fixtures but missed the mark on live BdE Circulares
    whose tables / anexos sit far away from the short prologue the window
    ended up centred on. session-3 live runs showed the model responding
    with "el fragmento base no contiene el modulo X" even when X did exist
    downstream in the Markdown.

    New strategy:
      1. If the whole document is <= ``_BASE_CONTEXT_SOFT_CAP`` chars,
         ship it verbatim. That removes the "model can't find the right
         section" failure mode at a modest token-cost bump.
      2. Otherwise splice per-hint windows with a wider radius (4000
         chars) and cap the total at the soft cap.
    """
    if not markdown:
        return ""
    if len(markdown) <= _BASE_CONTEXT_SOFT_CAP:
        return markdown
    if not hints:
        return markdown[:_BASE_CONTEXT_SOFT_CAP]

    slices: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for h in hints:
        ctx = build_anchor_context(markdown, h, window=4000)
        if not ctx:
            continue
        idx = markdown.find(ctx)
        if idx < 0:
            continue
        start = idx
        end = idx + len(ctx)
        start = markdown.rfind("\n", 0, start) + 1
        nxt = markdown.find("\n", end)
        end = nxt if nxt >= 0 else len(markdown)
        key = (start, end)
        if key in seen:
            continue
        seen.add(key)
        slices.append(key)

    if not slices:
        return markdown[:_BASE_CONTEXT_SOFT_CAP]

    slices.sort()
    merged: list[list[int]] = [list(slices[0])]
    for s, e in slices[1:]:
        if s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])

    pieces: list[str] = []
    total = 0
    for s, e in merged:
        chunk = markdown[s:e]
        if total + len(chunk) > _BASE_CONTEXT_SOFT_CAP:
            chunk = chunk[: max(0, _BASE_CONTEXT_SOFT_CAP - total)]
        pieces.append(chunk)
        total += len(chunk)
        if total >= _BASE_CONTEXT_SOFT_CAP:
            break
    return "\n\n[...]\n\n".join(p for p in pieces if p)


def _patch_from_structured(base: AmendmentPatch, edit: StructuredEdit) -> AmendmentPatch:
    """Return an AmendmentPatch mirroring the structured LLM edit so the
    fidelity log and downstream reporting carry the enriched provenance."""
    return dc_replace(
        base,
        operation=edit.operation,
        new_text=edit.new_text,
        anchor_confidence=edit.confidence,
        new_text_confidence=edit.confidence if edit.operation != "delete" else 1.0,
        extractor="llm_structured",
    )
