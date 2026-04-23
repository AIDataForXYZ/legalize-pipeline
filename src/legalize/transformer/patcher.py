"""Apply an AmendmentPatch to a base Markdown document (PLAN-STAGE-C.md §W3).

The patcher is the only module allowed to mutate a target norm's
Markdown. It is deliberately conservative: every patch is validated at
three gates before the mutation is emitted, and ANY gate failure
produces a skipped-patch result that the caller converts into a
commit-pointer reform rather than a text change.

Gates (in order):

  1. Anchor resolves uniquely. The anchor resolver returns Some(Position)
     or None; None → status="anchor_not_found".

  2. Literal-presence ("hash-check"). For replace/delete, the resolved
     Position must be non-empty. For replace, the declared new_text
     must be non-empty too. An empty resolved region means we would
     "replace nothing with something" or "delete nothing", both of
     which are almost always bugs — flagged as status="empty_anchor".

  3. Length sanity. A new_text that is absurdly shorter/longer than
     the region it replaces is a red flag (LLM truncation, regex
     false-positive). If the ratio is outside 0.1..10, we refuse and
     emit status="length_mismatch". The fidelity loop tunes these
     bounds if they prove too strict.

Commit-pointer fallback is NOT computed here. The patcher is a
mutation function; the committer decides what to do when it returns a
non-"applied" status. Keeping them separate lets the fidelity loop
measure each gate independently.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.transformer.anchor import Anchor, Position, parse_anchor_from_hint, resolve_anchor

logger = logging.getLogger(__name__)


PatchStatus = Literal[
    "applied",  # mutation succeeded
    "dry_run_ok",  # gates passed; no mutation written (dry_run=True)
    "anchor_not_found",  # resolve_anchor returned None
    "empty_anchor",  # resolved Position has no content
    "empty_new_text",  # replace/insert has no new_text
    "length_mismatch",  # new_text size ratio out of sanity bounds
    "delete_with_text",  # delete op was given new_text (contradiction)
    "unsupported_operation",  # the patch's operation string wasn't replace/insert/delete
]


@dataclass(frozen=True)
class PatchResult:
    """Outcome of a patch application attempt.

    When status == "applied" or "dry_run_ok", new_markdown holds the
    post-mutation document (or the would-be post-mutation document in
    dry-run mode). For any other status, new_markdown == input markdown.

    `reason` is a short human-readable explanation, mirroring the way
    LLMError carries per-rung diagnostics: the fidelity loop dumps it
    into the CSV log for per-patch triage.
    """

    status: PatchStatus
    new_markdown: str
    reason: str = ""
    position: Position | None = None


# ──────────────────────────────────────────────────────────
# Sanity bounds
# ──────────────────────────────────────────────────────────


# A new_text that is <10% or >10× the size of the region it replaces is
# almost certainly wrong (LLM truncation, wrong anchor). These bounds
# can be loosened per the fidelity loop's evidence.
_LENGTH_RATIO_MIN = 0.1
_LENGTH_RATIO_MAX = 10.0

# Relaxed bounds for regex_split sub-patches and LLM-structured edits: in
# both paths the "old region" we compare against is the entire structural
# unit (e.g. a full apartado), while the new_text is just the one
# paragraph being replaced. That makes the ratio naturally noisier —
# seeing a 0.05× or 15× ratio is common and usually benign. We keep a
# sanity cap so a truncated paragraph still trips the gate.
_LENGTH_RATIO_MIN_SPLIT = 0.05
_LENGTH_RATIO_MAX_SPLIT = 20.0

_RELAXED_EXTRACTORS: frozenset[str] = frozenset({"regex_split", "llm_structured"})

# When the old region is very short (< 40 chars — think "a)") the ratio
# check is too noisy to be useful. We skip it in that regime; the
# anchor-resolution step already took the care.
_SHORT_OLD_CHARS = 40


def _literal_old_text_position(markdown: str, old_text: str) -> Position | None:
    """Resolve by literal substring match.

    Only fires when:
      - ``old_text`` is at least 40 characters long (shorter strings like
        "a)" trip false positives far too often), AND
      - the snippet appears EXACTLY once in the base markdown — a
        duplicate is as good as ambiguous; better to emit commit-pointer
        than mutate the wrong region.

    The returned Position covers the set of whole lines that contain the
    match, so downstream mutation helpers (``_replace`` / ``_delete`` /
    ``_insert_after``) operate at line granularity as they do with
    heading-resolved anchors.
    """
    if not markdown or not old_text:
        return None
    needle = old_text.strip()
    if len(needle) < 40:
        return None
    if markdown.count(needle) != 1:
        return None
    idx = markdown.find(needle)
    line_start = markdown[:idx].count("\n")
    line_end = line_start + needle.count("\n") + 1
    lines = markdown.splitlines()
    if line_end > len(lines):
        line_end = len(lines)
    content = "\n".join(lines[line_start:line_end])
    return Position(
        line_start=line_start,
        line_end=line_end,
        content=content,
        kind="literal",
    )


def _ratio_bounds(extractor: str) -> tuple[float, float]:
    """Pick the (min, max) new/old-length ratio gate for this patch. Split
    sub-patches and structured-LLM edits get the looser bounds — see
    session-3 recap for the evidence behind the asymmetry."""
    if extractor in _RELAXED_EXTRACTORS:
        return _LENGTH_RATIO_MIN_SPLIT, _LENGTH_RATIO_MAX_SPLIT
    return _LENGTH_RATIO_MIN, _LENGTH_RATIO_MAX


# ──────────────────────────────────────────────────────────
# apply_patch — public entry
# ──────────────────────────────────────────────────────────


def apply_patch(
    markdown: str,
    patch: AmendmentPatch,
    *,
    dry_run: bool = False,
) -> PatchResult:
    """Apply `patch` to `markdown`.

    Returns a PatchResult. The caller decides whether to commit the
    result (on status == "applied") or emit a commit-pointer (on any
    other status). `dry_run=True` runs every gate but writes nothing;
    the fidelity loop uses this to measure per-gate failure rates
    without side effects.
    """
    # Short-circuit structural problems before touching the markdown.
    if patch.operation not in ("replace", "insert", "delete"):
        return PatchResult(
            status="unsupported_operation",
            new_markdown=markdown,
            reason=f"operation={patch.operation!r} not in (replace, insert, delete)",
        )

    if patch.operation == "delete" and patch.new_text:
        return PatchResult(
            status="delete_with_text",
            new_markdown=markdown,
            reason="delete ops must not carry new_text",
        )

    if patch.operation in ("replace", "insert") and not patch.new_text:
        return PatchResult(
            status="empty_new_text",
            new_markdown=markdown,
            reason="replace/insert requires new_text",
        )

    # Gate 1: anchor resolves.
    anchor = parse_anchor_from_hint(patch.anchor_hint)
    position = resolve_anchor(markdown, anchor)
    if position is None:
        return PatchResult(
            status="anchor_not_found",
            new_markdown=markdown,
            reason=f"no unique match for anchor_hint={patch.anchor_hint!r}",
        )

    # Gate 2: literal presence. For delete/replace we require a non-empty
    # region; for insert it is OK for the section body to be short — we
    # just need a parent to attach under.
    if patch.operation in ("replace", "delete") and not position.content.strip():
        return PatchResult(
            status="empty_anchor",
            new_markdown=markdown,
            reason="resolved anchor is empty; refusing to mutate",
            position=position,
        )

    # Gate 3: length sanity (replace only).
    if patch.operation == "replace":
        old_len = max(1, len(position.content.strip()))
        new_len = max(1, sum(len(p) for p in (patch.new_text or ())))
        if old_len >= _SHORT_OLD_CHARS:
            ratio = new_len / old_len
            ratio_min, ratio_max = _ratio_bounds(patch.extractor)
            if ratio < ratio_min or ratio > ratio_max:
                return PatchResult(
                    status="length_mismatch",
                    new_markdown=markdown,
                    reason=f"new_text/old ratio={ratio:.2f} outside [{ratio_min}, {ratio_max}]",
                    position=position,
                )

    # All gates passed. Build the mutated document.
    if patch.operation == "replace":
        new_md = _replace(markdown, position, patch.new_text or ())
    elif patch.operation == "insert":
        new_md = _insert_after(markdown, position, patch.new_text or ())
    else:
        new_md = _delete(markdown, position)

    status: PatchStatus = "dry_run_ok" if dry_run else "applied"
    return PatchResult(
        status=status,
        new_markdown=markdown if dry_run else new_md,
        reason="gates passed",
        position=position,
    )


# ──────────────────────────────────────────────────────────
# apply_at_position — mutate with a pre-resolved Position (bottom-up)
# ──────────────────────────────────────────────────────────


def apply_at_position(
    markdown: str,
    *,
    position: Position,
    operation: Literal["replace", "insert", "delete"],
    new_text: tuple[str, ...] | None,
) -> str:
    """Apply a mutation given a pre-resolved ``Position``.

    Used by the group dispatcher's bottom-up apply pass: Phase 1
    resolves every patch's position against the ORIGINAL base
    markdown; Phase 2 then mutates bottom-up on an evolving working
    markdown WITHOUT re-resolving (which would re-anchor against the
    now-partially-mutated document and defeat the point of
    bottom-up ordering).

    No gates — the caller has already passed them in the dry-run step.
    Returns the mutated markdown.
    """
    if operation == "replace":
        return _replace(markdown, position, new_text or ())
    if operation == "insert":
        return _insert_after(markdown, position, new_text or ())
    return _delete(markdown, position)


# ──────────────────────────────────────────────────────────
# apply_patch_structured — LLM-produced edits (bypasses parse_anchor_from_hint)
# ──────────────────────────────────────────────────────────


def apply_patch_structured(
    markdown: str,
    *,
    anchor: Anchor,
    operation: Literal["replace", "insert", "delete"],
    new_text: tuple[str, ...] | None,
    dry_run: bool = False,
    extractor: str = "llm_structured",
    old_text: str = "",
) -> PatchResult:
    """Apply an LLM-produced structured edit directly.

    Unlike ``apply_patch`` which re-parses a free-text hint via
    ``parse_anchor_from_hint``, this entry consumes the ``Anchor`` object
    the LLM has already resolved. Same gates, same PatchResult shape —
    only the resolution step is short-circuited.

    ``extractor`` tags which path produced this edit (default
    ``"llm_structured"``) so the length-sanity gate picks the relaxed
    ratio bounds and the fidelity log records provenance.

    ``old_text`` — when the Anchor can't be resolved through the heading
    tree (empty or ambiguous), fall back to a literal substring search of
    this snippet in the base markdown. Only triggers when the snippet is
    non-empty, at least 40 chars (short strings are too likely to match
    by accident), and appears exactly once. This unblocks the common BdE
    case where a modification targets a table cell / sub-module whose
    path cannot be expressed in our Anchor dataclass.
    """
    # Shape gates, identical to apply_patch.
    if operation not in ("replace", "insert", "delete"):
        return PatchResult(
            status="unsupported_operation",
            new_markdown=markdown,
            reason=f"operation={operation!r} not in (replace, insert, delete)",
        )

    if operation == "delete" and new_text:
        return PatchResult(
            status="delete_with_text",
            new_markdown=markdown,
            reason="delete ops must not carry new_text",
        )

    if operation in ("replace", "insert") and not new_text:
        return PatchResult(
            status="empty_new_text",
            new_markdown=markdown,
            reason="replace/insert requires new_text",
        )

    # Gate 1: anchor resolves. Try the structural anchor first, then fall
    # back to a literal old_text match when the anchor is empty or missed.
    position: Position | None = None
    if not anchor.is_empty:
        position = resolve_anchor(markdown, anchor)

    if position is None:
        position = _literal_old_text_position(markdown, old_text)

    if position is None:
        if anchor.is_empty:
            return PatchResult(
                status="anchor_not_found",
                new_markdown=markdown,
                reason="structured anchor is empty and old_text fallback did not match",
            )
        return PatchResult(
            status="anchor_not_found",
            new_markdown=markdown,
            reason=f"no unique match for structured anchor={anchor!r}",
        )

    # Gate 2: literal presence.
    if operation in ("replace", "delete") and not position.content.strip():
        return PatchResult(
            status="empty_anchor",
            new_markdown=markdown,
            reason="resolved anchor is empty; refusing to mutate",
            position=position,
        )

    # Gate 3: length sanity (replace only). Use relaxed bounds.
    if operation == "replace":
        old_len = max(1, len(position.content.strip()))
        new_len = max(1, sum(len(p) for p in (new_text or ())))
        if old_len >= _SHORT_OLD_CHARS:
            ratio = new_len / old_len
            ratio_min, ratio_max = _ratio_bounds(extractor)
            if ratio < ratio_min or ratio > ratio_max:
                return PatchResult(
                    status="length_mismatch",
                    new_markdown=markdown,
                    reason=f"new_text/old ratio={ratio:.2f} outside [{ratio_min}, {ratio_max}]",
                    position=position,
                )

    if operation == "replace":
        new_md = _replace(markdown, position, new_text or ())
    elif operation == "insert":
        new_md = _insert_after(markdown, position, new_text or ())
    else:
        new_md = _delete(markdown, position)

    status: PatchStatus = "dry_run_ok" if dry_run else "applied"
    return PatchResult(
        status=status,
        new_markdown=markdown if dry_run else new_md,
        reason="gates passed (structured)",
        position=position,
    )


# ──────────────────────────────────────────────────────────
# Line-based mutation helpers
# ──────────────────────────────────────────────────────────


def _replace(markdown: str, pos: Position, new_text: tuple[str, ...]) -> str:
    """Replace the lines covered by `pos` with `new_text` (one paragraph
    per entry, separated by blank lines — matching Stage A formatting).

    Preserves the trailing newline style of the base document: if the
    original ended with '\\n', so does the result.
    """
    lines = markdown.split("\n")
    new_block = _format_paragraphs(new_text)
    new_lines = lines[: pos.line_start] + new_block + lines[pos.line_end :]
    return _join_preserving_trailer(markdown, new_lines)


def _insert_after(markdown: str, pos: Position, new_text: tuple[str, ...]) -> str:
    """Insert `new_text` immediately after the region covered by `pos`.

    For insert operations the anchor is the PARENT region (e.g. the
    artículo under which we add an apartado); we append after that
    region's last line.
    """
    lines = markdown.split("\n")
    new_block = _format_paragraphs(new_text)
    # Ensure a blank line between existing content and the new block,
    # but don't double it if the next line is already blank.
    insertion_point = pos.line_end
    prefix_blank: list[str] = []
    if insertion_point > 0 and lines[insertion_point - 1].strip():
        prefix_blank = [""]
    new_lines = lines[:insertion_point] + prefix_blank + new_block + lines[insertion_point:]
    return _join_preserving_trailer(markdown, new_lines)


def _delete(markdown: str, pos: Position) -> str:
    """Remove the lines covered by `pos`. Leaves a single blank line
    behind so surrounding sections stay visually separated."""
    lines = markdown.split("\n")
    new_lines = lines[: pos.line_start] + [""] + lines[pos.line_end :]
    # Collapse runs of more than one blank (the insertion above + any
    # pre-existing blank lines around the deleted block).
    new_lines = _collapse_blank_runs(new_lines)
    return _join_preserving_trailer(markdown, new_lines)


def _format_paragraphs(paragraphs: tuple[str, ...]) -> list[str]:
    """Render a tuple of paragraph strings as Markdown lines. One blank
    line between paragraphs; no leading / trailing blanks."""
    out: list[str] = []
    for i, p in enumerate(paragraphs):
        if i > 0:
            out.append("")
        # Paragraphs may themselves contain internal newlines (from
        # preserved blockquote content). Split them so our line index
        # stays honest for any downstream re-resolution.
        out.extend(p.split("\n"))
    return out


def _collapse_blank_runs(lines: list[str]) -> list[str]:
    """Fold runs of 2+ blank lines to a single blank line."""
    out: list[str] = []
    prev_blank = False
    for line in lines:
        blank = not line.strip()
        if blank and prev_blank:
            continue
        out.append(line)
        prev_blank = blank
    return out


def _join_preserving_trailer(original: str, new_lines: list[str]) -> str:
    joined = "\n".join(new_lines)
    if original.endswith("\n") and not joined.endswith("\n"):
        joined += "\n"
    return joined
