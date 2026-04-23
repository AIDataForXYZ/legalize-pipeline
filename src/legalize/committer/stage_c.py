"""Stage C committer (PLAN-STAGE-C.md §W5).

Takes a non-consolidated BOE norm and produces its full reform history
as a git commit chain on disk:

    es/{target_id}.md
      [bootstrap]  Original diario BOE text
      [reforma]    Source-Id: <mod_1>   — first modification applied
      [reforma]    Source-Id: <mod_2>   — second modification applied
      ...

The driver glues together every Stage C module we built:

    parse_diario_xml (Stage B)
          ↓
    render_markdown  →  bootstrap commit
          ↓
    parse <posteriores>  →  list of modifier ids (sorted by date)
          ↓
    for each modifier:
      parse_amendments (fetcher/es/amendments)
           ↓
      classify_case + (optionally) AmendmentLLM / PendingCaseQueue
           ↓
      apply_patch (transformer/patcher)
           ↓
      git commit [reforma] or commit-pointer

Idempotency contract (the three xfail clauses in
tests/test_stage_c_idempotency.py):

  A. Bit-identical reruns. We get deterministic commit SHAs because:
     - author_date = BOE fecha_publicacion (no wall-clock sneaks in)
     - committer identity comes from config (no runner-specific drift)
     - the sort key (source_date, ordering_key, source_boe_id) is stable

  B. Detect-and-skip. Before each modifier, the driver asks
     ``GitRepo.has_commit_with_source_id(mod_id, target_id)``; existing
     commits are NEVER re-emitted. A partial rerun simply resumes.

  C. Commit-pointer stickiness. Once a pointer exists for (mod_id,
     target_id), the driver skips the modifier on rerun — same as any
     other committed reform. A later successful reconstruction cannot
     retroactively replace the pointer; the only way to upgrade is an
     explicit ``legalize reprocess --norm {id}`` that rewrites the
     whole file's history.

The driver never calls BOE directly; the caller injects a
``fetch_diario(id)`` callable. That keeps unit tests offline and lets
the CLI wire in rate limiting, caching, mirrors, etc.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Literal

from lxml import etree

from legalize.committer.git_ops import GitRepo
from legalize.fetcher.es.amendments import parse_amendments
from legalize.llm.amendment_parser import AmendmentLLM
from legalize.llm.dispatcher import (
    DispatchResult,
    dispatch_modifier_patches,
    extract_modifier_body_text,
)
from legalize.llm.queue import PendingCaseQueue
from legalize.transformer.patcher import PatchResult

logger = logging.getLogger(__name__)


FetchDiario = Callable[[str], bytes]
"""Callable (boe_id) -> raw XML bytes of /diario_boe/xml.php?id={boe_id}.
Injected by the caller so the driver never touches the network itself."""


# ──────────────────────────────────────────────────────────
# Result dataclasses
# ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ModifierOutcome:
    """How one modifier behaved when we tried to apply it."""

    modifier_id: str
    modifier_date: date
    status: Literal[
        "applied",  # at least one patch succeeded; real [reforma] commit
        "commit_pointer",  # every patch failed a gate; empty [reforma] commit
        "skipped_existing",  # prior run already committed this (idempotency)
        "skipped_queued",  # case was hard → queued for Claude, not yet resolved
    ]
    patch_results: tuple[PatchResult, ...] = ()
    sha: str = ""
    reason: str = ""


@dataclass(frozen=True)
class TargetResult:
    """Aggregated outcome of processing one target norm."""

    target_id: str
    bootstrap_status: Literal["committed", "existing", "skipped"]
    bootstrap_sha: str
    modifier_outcomes: tuple[ModifierOutcome, ...] = ()

    @property
    def applied_count(self) -> int:
        return sum(1 for m in self.modifier_outcomes if m.status == "applied")

    @property
    def pointer_count(self) -> int:
        return sum(1 for m in self.modifier_outcomes if m.status == "commit_pointer")

    @property
    def skipped_count(self) -> int:
        return sum(
            1 for m in self.modifier_outcomes if m.status in ("skipped_existing", "skipped_queued")
        )


# ──────────────────────────────────────────────────────────
# Driver
# ──────────────────────────────────────────────────────────


class StageCDriver:
    """Orchestrates Stage C per target norm. See module docstring."""

    def __init__(
        self,
        *,
        repo: GitRepo,
        fetch_diario: FetchDiario,
        country_dir: str = "es",
        author_name: str = "Legalize",
        author_email: str = "legalize@legalize.dev",
        queue: PendingCaseQueue | None = None,
        llm: AmendmentLLM | None = None,
        daily_mode: bool = False,
        use_structured_llm: bool = True,
    ) -> None:
        self.repo = repo
        self.fetch_diario = fetch_diario
        self.country_dir = country_dir
        self.author_name = author_name
        self.author_email = author_email
        self.queue = queue
        self.llm = llm
        self.daily_mode = daily_mode
        self.use_structured_llm = use_structured_llm
        repo.load_existing_commits()

    # ── public ────────────────────────────────────────────

    def process_target(self, target_id: str) -> TargetResult:
        """Full Stage C flow for one target norm. Idempotent: re-running
        skips any work already committed in a previous invocation.

        Raises:
            ValueError when fetch_diario returns no content or the XML
            is malformed beyond repair.
        """
        target_path = f"{self.country_dir}/{target_id}.md"

        # ── Bootstrap ──────────────────────────────────
        bootstrap_sha, bootstrap_status, base_md, metadata = self._bootstrap(
            target_id,
            target_path,
        )

        # ── Discover modifiers ─────────────────────────
        posteriores = self._discover_posteriores(metadata["_xml"], target_id)

        # ── Apply each modifier ────────────────────────
        outcomes: list[ModifierOutcome] = []
        current_md = base_md
        for mod_id, mod_date, mod_ordering in posteriores:
            outcome = self._apply_modifier(
                target_id=target_id,
                target_path=target_path,
                current_md=current_md,
                modifier_id=mod_id,
                modifier_date=mod_date,
                ordering_key=mod_ordering,
            )
            outcomes.append(outcome)
            # Only advance the working markdown on a successful apply;
            # commit-pointers preserve the prior state for the next
            # modifier's inputs.
            if outcome.status == "applied":
                current_md = self._read_target_file(target_path)

        return TargetResult(
            target_id=target_id,
            bootstrap_status=bootstrap_status,
            bootstrap_sha=bootstrap_sha,
            modifier_outcomes=tuple(outcomes),
        )

    # ── bootstrap ─────────────────────────────────────────

    def _bootstrap(
        self,
        target_id: str,
        target_path: str,
    ) -> tuple[str, Literal["committed", "existing", "skipped"], str, dict]:
        """Emit the bootstrap commit if missing; otherwise load the
        existing base markdown from disk."""
        # Idempotency: a bootstrap uses Source-Id == Norm-Id by convention.
        # If GitRepo already knows one, load the file from disk instead
        # of re-fetching.
        if self.repo.has_commit_with_source_id(target_id, norm_id=target_id):
            logger.info("bootstrap skipped (existing): %s", target_id)
            existing = self._read_target_file(target_path)
            return "", "existing", existing, {"_xml": self._fetch_cached(target_id)}

        xml_bytes = self.fetch_diario(target_id)
        if not xml_bytes:
            raise ValueError(f"fetch_diario returned empty for {target_id}")

        base_md, metadata = self._build_bootstrap_markdown(xml_bytes, target_id)

        if not self.repo.write_and_add(target_path, base_md):
            # File was identical to what was on disk (rare: re-running
            # after a crash that wrote the file but didn't commit). Just
            # commit so the Source-Id trailer lands.
            pass

        info = self._build_commit_info(
            commit_type="bootstrap",
            target_id=target_id,
            source_id=target_id,  # bootstrap self-references
            source_date=metadata["publication_date"],
            short_title=metadata["short_title"],
            content=base_md,
            target_path=target_path,
            subject_suffix=f"— versión original {metadata['publication_date'].year}",
            body=(
                f"Publicación original de {metadata['short_title']}.\n"
                f"\n"
                f"Norma: {target_id}\n"
                f"Fecha: {metadata['publication_date'].isoformat()}\n"
                f"Fuente: https://www.boe.es/diario_boe/xml.php?id={target_id}"
            ),
        )
        sha = self.repo.commit(info)
        logger.info("bootstrap committed: %s (%s)", target_id, sha[:8] if sha else "?")
        return sha or "", "committed", base_md, {"_xml": xml_bytes, **metadata}

    def _fetch_cached(self, target_id: str) -> bytes:
        """Helper for resumed runs: we need the target's XML to discover
        posteriores even when the bootstrap was committed previously."""
        return self.fetch_diario(target_id)

    def _build_bootstrap_markdown(
        self,
        xml_bytes: bytes,
        target_id: str,
    ) -> tuple[str, dict]:
        """Render the bootstrap Markdown from the diario XML. Import is
        local so tests that stub the pipeline don't require the full
        Stage A parse chain to be importable."""
        from legalize.transformer.markdown import render_paragraphs
        from legalize.transformer.xml_parser import parse_diario_xml

        blocks = parse_diario_xml(xml_bytes)
        parts: list[str] = []
        for block in blocks:
            if not block.versions:
                continue
            parts.append(render_paragraphs(list(block.versions[0].paragraphs)))
        body = "\n\n".join(p for p in parts if p).rstrip() + "\n"

        # Minimal metadata from <metadatos>. Full Stage A metadata enrichment
        # is out of scope for Stage C driver; callers that want rich
        # frontmatter can post-process.
        pub_date, short_title = self._extract_bootstrap_metadata(xml_bytes, target_id)
        return body, {"publication_date": pub_date, "short_title": short_title}

    def _extract_bootstrap_metadata(
        self,
        xml_bytes: bytes,
        target_id: str,
    ) -> tuple[date, str]:
        try:
            root = etree.fromstring(xml_bytes)
        except Exception as e:
            raise ValueError(f"could not parse diario XML for {target_id}: {e}") from e
        meta = root.find("metadatos")
        if meta is None:
            raise ValueError(f"no <metadatos> in diario XML for {target_id}")
        raw_date = (meta.findtext("fecha_publicacion") or "").strip()
        if len(raw_date) != 8 or not raw_date.isdigit():
            raise ValueError(f"invalid fecha_publicacion for {target_id}: {raw_date!r}")
        pub_date = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
        title = (meta.findtext("titulo") or target_id).strip()
        short_title = title.split(",")[0].strip() or target_id
        return pub_date, short_title

    # ── modifier application ──────────────────────────────

    def _apply_modifier(
        self,
        *,
        target_id: str,
        target_path: str,
        current_md: str,
        modifier_id: str,
        modifier_date: date,
        ordering_key: str,
    ) -> ModifierOutcome:
        # Idempotency clause B: already committed → skip.
        if self.repo.has_commit_with_source_id(modifier_id, target_id):
            logger.debug("modifier %s already committed; skipping", modifier_id)
            return ModifierOutcome(
                modifier_id=modifier_id,
                modifier_date=modifier_date,
                status="skipped_existing",
            )

        mod_xml = self.fetch_diario(modifier_id)
        if not mod_xml:
            return ModifierOutcome(
                modifier_id=modifier_id,
                modifier_date=modifier_date,
                status="commit_pointer",
                reason="fetch returned empty; emitting pointer",
                sha=self._emit_pointer(
                    target_id=target_id,
                    target_path=target_path,
                    modifier_id=modifier_id,
                    modifier_date=modifier_date,
                    content=current_md,
                    reason="modifier XML unavailable",
                ),
            )

        patches = [p for p in parse_amendments(mod_xml) if p.target_id == target_id]

        # Deterministic order within one modifier: by <anterior orden=".."/>
        # attribute, falling back to stable document order.
        patches.sort(key=lambda p: (p.ordering_key, p.anchor_hint))

        modifier_body = extract_modifier_body_text(mod_xml)

        working_md = current_md
        applied_any = False
        queued_any = False
        dispatches: list[DispatchResult] = list(
            dispatch_modifier_patches(
                working_md,
                patches,
                modifier_body=modifier_body,
                llm=self.llm,
                queue=self.queue,
                daily_mode=self.daily_mode,
                use_structured=self.use_structured_llm,
            )
        )
        # The group dispatcher threads the working markdown internally;
        # take the final state from the last applied patch in the batch.
        for dispatch in dispatches:
            if dispatch.applied:
                working_md = dispatch.patch_result.new_markdown  # type: ignore[union-attr]
                applied_any = True
            elif dispatch.status == "queued":
                queued_any = True

        results: list[PatchResult] = [
            d.patch_result for d in dispatches if d.patch_result is not None
        ]

        if not applied_any:
            # Queued-only modifiers are held back for a future rerun: once
            # the Claude resolver writes resolutions.jsonl, the next
            # StageCDriver invocation dispatches the same hard cases and
            # picks up the resolution (idempotent via case_id_for).
            if queued_any and not results:
                logger.info(
                    "modifier %s fully deferred to Claude queue (%d cases)",
                    modifier_id,
                    sum(1 for d in dispatches if d.status == "queued"),
                )
                return ModifierOutcome(
                    modifier_id=modifier_id,
                    modifier_date=modifier_date,
                    status="skipped_queued",
                    patch_results=(),
                    reason="all patches routed to Claude queue; awaiting resolution",
                )

            sha = self._emit_pointer(
                target_id=target_id,
                target_path=target_path,
                modifier_id=modifier_id,
                modifier_date=modifier_date,
                content=current_md,
                reason=self._summarize_reasons(results),
            )
            return ModifierOutcome(
                modifier_id=modifier_id,
                modifier_date=modifier_date,
                status="commit_pointer",
                patch_results=tuple(results),
                sha=sha,
                reason="no patch applied",
            )

        # Apply + commit real reforma.
        self.repo.write_and_add(target_path, working_md)
        info = self._build_commit_info(
            commit_type="reform",
            target_id=target_id,
            source_id=modifier_id,
            source_date=modifier_date,
            short_title=self._read_short_title(target_path) or target_id,
            content=working_md,
            target_path=target_path,
            subject_suffix=f"— {len([r for r in results if r.status == 'applied'])} patches",
            body=(
                f"Modificación aplicada por {modifier_id}.\n"
                f"\n"
                f"Norma: {target_id}\n"
                f"Disposición: {modifier_id}\n"
                f"Fecha: {modifier_date.isoformat()}\n"
                f"Patches aplicados: {sum(1 for r in results if r.status == 'applied')}/{len(results)}"
            ),
        )
        sha = self.repo.commit(info) or ""
        return ModifierOutcome(
            modifier_id=modifier_id,
            modifier_date=modifier_date,
            status="applied",
            patch_results=tuple(results),
            sha=sha,
        )

    # ── commit pointer (empty-content reform) ─────────────

    def _emit_pointer(
        self,
        *,
        target_id: str,
        target_path: str,
        modifier_id: str,
        modifier_date: date,
        content: str,
        reason: str,
    ) -> str:
        """Empty commit with the reform trailers. Honours idempotency
        clause C (stickiness): once emitted, this pointer survives
        subsequent reruns unchanged.

        Empty here means the WORKING TREE is untouched; we commit with
        --allow-empty via a tiny helper path on GitRepo (see below). The
        content argument is carried so if the caller changes the file
        elsewhere we at least check-consistency.
        """
        info = self._build_commit_info(
            commit_type="reform",
            target_id=target_id,
            source_id=modifier_id,
            source_date=modifier_date,
            short_title=self._read_short_title(target_path) or target_id,
            content=content,
            target_path=target_path,
            subject_suffix="— commit-pointer (texto no reconstruido)",
            body=(
                f"Modificación por {modifier_id} no reconstruible automáticamente.\n"
                f"\n"
                f"Norma: {target_id}\n"
                f"Disposición: {modifier_id}\n"
                f"Fecha: {modifier_date.isoformat()}\n"
                f"Motivo: {reason}\n"
                f"\n"
                f"Consulte el BOE: https://www.boe.es/diario_boe/xml.php?id={modifier_id}"
            ),
        )
        sha = self._commit_allow_empty(info) or ""
        logger.info("commit-pointer: %s → %s", modifier_id, sha[:8] if sha else "?")
        return sha

    def _commit_allow_empty(self, info) -> str | None:
        """Write an empty commit preserving the trailers. Uses the same
        env plumbing as GitRepo.commit but with --allow-empty."""
        from legalize.committer.message import format_commit_message

        message = format_commit_message(info)
        git_date = info.author_date if info.author_date >= date(1970, 1, 2) else date(1970, 1, 2)
        author_date = f"{git_date.isoformat()}T00:00:00"
        env = {
            "GIT_AUTHOR_DATE": author_date,
            "GIT_COMMITTER_DATE": author_date,
            "GIT_AUTHOR_NAME": info.author_name,
            "GIT_AUTHOR_EMAIL": info.author_email,
            "GIT_COMMITTER_NAME": self.repo._committer_name,
            "GIT_COMMITTER_EMAIL": self.repo._committer_email,
        }
        self.repo._run(["commit", "--allow-empty", "-m", message], env=env)
        sha = self.repo._run(["rev-parse", "HEAD"])
        # Update the in-memory idempotency cache.
        source_id = info.trailers.get("Source-Id", "")
        norm_id = info.trailers.get("Norm-Id", "")
        if source_id and norm_id and hasattr(self.repo, "_existing_commits"):
            self.repo._existing_commits.add((source_id, norm_id))
        return sha

    # ── helpers ────────────────────────────────────────────

    def _discover_posteriores(
        self,
        xml_bytes: bytes,
        target_id: str,
    ) -> list[tuple[str, date, str]]:
        """Extract (modifier_id, modifier_date, ordering_key) tuples from
        <analisis>/<referencias>/<posteriores>. Sorted for stable reruns
        by (date ASC, ordering_key ASC, modifier_id ASC).

        We don't have the modifier's own fecha_publicacion yet — we rely
        on the <posterior> element's attrs if present, else treat it as
        date(9999,12,31) so it sorts last and a second pass via
        fetch_diario can refine. For MVP the primary order is the
        modifier_id itself (monotonic within BOE).
        """
        try:
            root = etree.fromstring(xml_bytes)
        except Exception:
            return []
        posts = root.find(".//analisis/referencias/posteriores")
        if posts is None:
            return []

        raw: list[tuple[str, date, str]] = []
        for p in posts.findall("posterior"):
            mid = (p.get("referencia") or "").strip()
            if not mid.startswith("BOE-"):
                continue
            # BOE-A-YYYY-NNN carries year; derive a stable ordering date
            # from the ID so reruns produce the same order even before we
            # fetch the modifier. When we later fetch, we can refine, but
            # idempotency doesn't depend on it because the commit dates
            # come from the modifier's own fecha_publicacion at commit time.
            year = self._year_from_id(mid)
            mdate = date(year, 1, 1) if year else date(9999, 12, 31)
            orden = p.get("orden") or ""
            raw.append((mid, mdate, orden))

        raw.sort(key=lambda t: (t[1], t[2], t[0]))
        return raw

    @staticmethod
    def _year_from_id(boe_id: str) -> int | None:
        """BOE-A-YYYY-NNN → YYYY as int; None on malformed."""
        parts = boe_id.split("-")
        if len(parts) < 3:
            return None
        try:
            return int(parts[2])
        except ValueError:
            return None

    def _summarize_reasons(self, results: list[PatchResult]) -> str:
        if not results:
            return "no patches produced by parse_amendments"
        counts: dict[str, int] = {}
        for r in results:
            counts[r.status] = counts.get(r.status, 0) + 1
        parts = [f"{status}={count}" for status, count in sorted(counts.items())]
        return "; ".join(parts)

    def _build_commit_info(
        self,
        *,
        commit_type: Literal["bootstrap", "reform"],
        target_id: str,
        source_id: str,
        source_date: date,
        short_title: str,
        content: str,
        target_path: str,
        subject_suffix: str,
        body: str,
    ):
        from legalize.models import CommitInfo, CommitType

        ctype = CommitType.BOOTSTRAP if commit_type == "bootstrap" else CommitType.REFORM
        subject = f"[{ctype.value}] {short_title} {subject_suffix}".strip()
        trailers = {
            "Source-Id": source_id,
            "Source-Date": source_date.isoformat(),
            "Norm-Id": target_id,
        }
        return CommitInfo(
            commit_type=ctype,
            subject=subject,
            body=body,
            trailers=trailers,
            author_name=self.author_name,
            author_email=self.author_email,
            author_date=source_date,
            file_path=target_path,
            content=content,
        )

    def _read_target_file(self, target_path: str) -> str:
        p = Path(self.repo._path) / target_path
        if not p.exists():
            return ""
        return p.read_text(encoding="utf-8")

    def _read_short_title(self, target_path: str) -> str:
        """Peek at the committed file's first heading or frontmatter title."""
        content = self._read_target_file(target_path)
        for line in content.splitlines()[:20]:
            if line.startswith("# "):
                return line[2:].strip()
        return ""
