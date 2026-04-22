"""Persistent queue for amendment cases routed to the Claude Code session.

Motivation: the independent review and subsequent cost analysis concluded
that running every ambiguous case through Groq is cheap enough (~$15 for
all of Spain's non-consolidated corpus) but that routing the hardest
cases — those requiring genuine reading comprehension of long modifier
bodies — to Claude Code is strictly better on quality and has zero
incremental API cost for the user (already covered by the Claude
subscription).

Flow:

    Stage C pipeline                      Claude Code session
    ────────────────                      ───────────────────
    parse_amendments →                                ▲
    confidence check →                                │
    classify_case() →                                 │
      "short"  → Groq gpt-oss-20b → apply             │
      "medium" → Groq gpt-oss-120b → apply            │
      "hard"   → enqueue PendingCase ───── reads ─────┘
                       │
                       ▼ writes CaseResolution ─────→  back to queue
    (next run):                                         │
    read resolutions, apply as extractor="claude_code"←─┘

Each queue file is a pair of JSONL streams on disk:

    {queue_dir}/pending.jsonl       # one PendingCase per line, append-only
    {queue_dir}/resolved.jsonl      # one CaseResolution per line, append-only

Both append-only, so the queue survives interrupted runs. Resolution
lookup is O(N) but N rarely exceeds a few hundred hard cases per run.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from legalize.fetcher.es.amendments import AmendmentPatch, Operation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PendingCase:
    """One hard case awaiting Claude-session resolution.

    Carries exactly the context the resolver needs — no more, no less.
    Importantly, the full modifier body excerpt is persisted so the
    resolution can happen at any later time without re-fetching BOE.
    """

    case_id: str  # hash identifying this case uniquely
    target_id: str
    source_boe_id: str
    source_date: str  # ISO; json doesn't serialise date directly
    operation: Operation
    verb_code: str
    verb_text: str
    anchor_hint: str
    modifier_excerpt: str  # full <texto> of the modifier, quote-normalized
    base_context: str  # ~1000-char window of target base Markdown
    ordering_key: str = ""
    enqueued_at: str = ""  # ISO datetime for observability


@dataclass(frozen=True)
class CaseResolution:
    """Outcome written back by the Claude Code session for a PendingCase."""

    case_id: str
    operation: Operation
    new_text: tuple[str, ...] | None
    anchor_confidence: float
    new_text_confidence: float
    reason: str = ""  # free-text explanation, useful for fidelity logs
    resolver: Literal["claude_code", "skipped"] = "claude_code"


DEFAULT_BATCH_SIZE = 30
# The Claude-session resolver reads one batch at a time. 30 cases is the
# sweet spot for a single sub-agent call: small enough that the sub-agent
# fits in context with each case's excerpt (< 4 KB × 30 = 120 KB, well
# under Opus/Sonnet's context window), large enough that the fixed
# overhead per sub-agent invocation is amortized.


class PendingCaseQueue:
    """Append-only JSONL pair (pending + resolved) under a single directory,
    plus a rotating set of immutable batch files for the Claude resolver.

    Layout::

        {queue_dir}/
            pending.jsonl       append-only log of PendingCase entries
            resolved.jsonl      append-only log of CaseResolution entries
            batches/
                batch_000.jsonl
                batch_001.jsonl
                ...

    A batch is frozen (immutable) once emitted; the resolver reads the
    batch file, writes resolutions back to resolved.jsonl, and the
    pipeline picks them up on the next read. Idempotent everywhere:
    re-emitting a batch over already-queued cases is a no-op.

    Thread-safety: concurrent writers are safe because each line is
    self-delimited and file writes are atomic on POSIX for < PIPE_BUF
    bytes (typically 4096). Lines over that should be rare — our biggest
    case carries ~4 KB of modifier excerpt. If we ever exceed, callers
    should serialize via an external lock (fcntl) per queue_dir.
    """

    def __init__(
        self,
        queue_dir: Path,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.queue_dir = queue_dir
        self.batch_size = batch_size
        queue_dir.mkdir(parents=True, exist_ok=True)
        (queue_dir / "batches").mkdir(exist_ok=True)
        self._pending_path = queue_dir / "pending.jsonl"
        self._resolved_path = queue_dir / "resolved.jsonl"
        self._batches_dir = queue_dir / "batches"

    @property
    def pending_path(self) -> Path:
        return self._pending_path

    @property
    def resolved_path(self) -> Path:
        return self._resolved_path

    # ── enqueue / dequeue ─────────────────────────────────

    def enqueue(self, case: PendingCase) -> None:
        """Append a case to pending.jsonl. Idempotent by case_id: a case
        already present (even if already resolved) is a no-op."""
        if case.case_id in self._enqueued_ids():
            return
        with self._pending_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(case), ensure_ascii=False) + "\n")

    def pending_cases(self) -> list[PendingCase]:
        """All cases enqueued, in order. Includes already-resolved ones;
        call unresolved_cases() when you only want the work queue."""
        if not self._pending_path.exists():
            return []
        out: list[PendingCase] = []
        with self._pending_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("malformed pending.jsonl line; skipping: %s", line[:80])
                    continue
                out.append(PendingCase(**d))
        return out

    def unresolved_cases(self) -> list[PendingCase]:
        """Cases still awaiting resolution."""
        resolved = set(self.resolutions().keys())
        return [c for c in self.pending_cases() if c.case_id not in resolved]

    def _enqueued_ids(self) -> set[str]:
        return {c.case_id for c in self.pending_cases()}

    # ── resolve ───────────────────────────────────────────

    def record_resolution(self, resolution: CaseResolution) -> None:
        """Append a resolution. Idempotent by case_id: writing twice is
        a no-op (first write wins); callers don't need to dedupe."""
        if resolution.case_id in self.resolutions():
            return
        payload = asdict(resolution)
        # Tuples don't survive json round-trip with asdict → they become
        # lists. Keep as list on disk; materializer re-tuples on load.
        if resolution.new_text is not None:
            payload["new_text"] = list(resolution.new_text)
        with self._resolved_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def resolutions(self) -> dict[str, CaseResolution]:
        """Map of case_id -> CaseResolution, first-write-wins."""
        if not self._resolved_path.exists():
            return {}
        out: dict[str, CaseResolution] = {}
        with self._resolved_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("malformed resolved.jsonl line; skipping: %s", line[:80])
                    continue
                cid = d.get("case_id", "")
                if not cid or cid in out:
                    continue
                nt = d.get("new_text")
                nt_tuple = tuple(nt) if isinstance(nt, list) else None
                out[cid] = CaseResolution(
                    case_id=cid,
                    operation=d.get("operation", "replace"),
                    new_text=nt_tuple,
                    anchor_confidence=float(d.get("anchor_confidence", 0.0)),
                    new_text_confidence=float(d.get("new_text_confidence", 0.0)),
                    reason=d.get("reason", ""),
                    resolver=d.get("resolver", "claude_code"),
                )
        return out

    # ── integration with the pipeline ─────────────────────

    def apply_resolution(self, case: PendingCase, patch: AmendmentPatch) -> AmendmentPatch | None:
        """Given a base patch and its case, return the patch enriched with
        the resolved new_text + confidence — or None when the case is
        still pending."""
        res = self.resolutions().get(case.case_id)
        if res is None:
            return None
        from dataclasses import replace

        return replace(
            patch,
            operation=res.operation,
            new_text=res.new_text,
            anchor_confidence=res.anchor_confidence,
            new_text_confidence=res.new_text_confidence,
            extractor="claude_code",
        )

    # ── batches ───────────────────────────────────────────

    def ready_for_batch(self) -> bool:
        """True when the number of UNRESOLVED cases meets the batch
        threshold. The Stage C driver polls this after each modifier it
        processes, and when it flips True it triggers the Claude resolver."""
        return len(self.unresolved_cases()) >= self.batch_size

    def emit_batch(self) -> Path | None:
        """Freeze the current unresolved cases into a new batch file.

        Returns the path of the new batch, or None when there are no
        unresolved cases to emit. The batch file name is
        ``batch_NNN.jsonl`` with NNN = the next unused index. Cases that
        already appear in an earlier un-completed batch are NOT re-emitted
        (preventing duplicate work).
        """
        unresolved = self.unresolved_cases()
        if not unresolved:
            return None

        already_batched = self._cases_in_open_batches()
        fresh = [c for c in unresolved if c.case_id not in already_batched]
        if not fresh:
            return None

        idx = self._next_batch_index()
        path = self._batches_dir / f"batch_{idx:03d}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for c in fresh:
                f.write(json.dumps(asdict(c), ensure_ascii=False) + "\n")
        logger.info("emitted %s with %d cases", path.name, len(fresh))
        return path

    def open_batches(self) -> list[Path]:
        """List all batch files whose cases are not yet fully resolved.
        The Claude resolver works through these in order."""
        if not self._batches_dir.exists():
            return []
        resolved = set(self.resolutions().keys())
        out: list[Path] = []
        for path in sorted(self._batches_dir.glob("batch_*.jsonl")):
            ids = self._case_ids_in_batch(path)
            if ids - resolved:
                out.append(path)
        return out

    def _cases_in_open_batches(self) -> set[str]:
        ids: set[str] = set()
        for path in self.open_batches():
            ids |= self._case_ids_in_batch(path)
        return ids

    def _case_ids_in_batch(self, path: Path) -> set[str]:
        ids: set[str] = set()
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ids.add(json.loads(line)["case_id"])
                except (json.JSONDecodeError, KeyError):
                    continue
        return ids

    def _next_batch_index(self) -> int:
        existing = list(self._batches_dir.glob("batch_*.jsonl"))
        if not existing:
            return 0
        nums: list[int] = []
        for p in existing:
            try:
                nums.append(int(p.stem.split("_")[1]))
            except (IndexError, ValueError):
                continue
        return max(nums) + 1 if nums else 0


# ──────────────────────────────────────────────────────────
# Case classification
# ──────────────────────────────────────────────────────────


CaseTier = Literal["regex_only", "short", "medium", "hard"]


def classify_case(
    *,
    anchor_confidence: float,
    new_text_confidence: float,
    modifier_excerpt_len: int,
) -> CaseTier:
    """Route an AmendmentPatch to the right backend.

    - regex_only: both axes >= 0.9. Apply directly, no LLM.
    - short: at least one axis weak but modifier excerpt is small
      (< 800 chars). Cheap call to gpt-oss-20b suffices.
    - medium: modifier excerpt 800-3000 chars. Needs gpt-oss-120b.
    - hard: excerpt > 3000 chars or both axes very low. Routed to the
      Claude Code queue because these cases genuinely need reading
      comprehension across a long body.

    The thresholds are starting points; the fidelity loop (W4) will
    tune them against a measured corpus.
    """
    if anchor_confidence >= 0.9 and new_text_confidence >= 0.9:
        return "regex_only"

    if modifier_excerpt_len > 3000:
        return "hard"

    # Both axes very weak AND a sizeable excerpt → reading comprehension
    # territory, not just text extraction. Claude handles this better
    # than any model Groq currently exposes at the 20b/120b tier.
    if anchor_confidence < 0.5 and new_text_confidence < 0.5 and modifier_excerpt_len > 1500:
        return "hard"

    if modifier_excerpt_len < 800:
        return "short"

    return "medium"


def case_id_for(patch: AmendmentPatch) -> str:
    """Stable identifier so the queue is idempotent across reruns. The id
    is tied to (source_boe_id, target_id, verb_code, anchor_hint): same
    modification re-enqueued twice collapses to one entry."""
    import hashlib

    h = hashlib.blake2b(digest_size=12)
    h.update(patch.source_boe_id.encode())
    h.update(b"\x00")
    h.update(patch.target_id.encode())
    h.update(b"\x00")
    h.update(patch.verb_code.encode())
    h.update(b"\x00")
    h.update(patch.anchor_hint.encode())
    return h.hexdigest()
