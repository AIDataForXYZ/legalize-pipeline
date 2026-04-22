"""Contract tests for Stage C rerun idempotency.

These tests define the contract the Stage C commit pipeline (W5) MUST
satisfy. They're written here so the contract is visible BEFORE we build
the patcher — the independent review flagged idempotency as the single
biggest under-specified area, and "write the rerun test before writing
the patcher" as the one concrete simplification that matters most.

The tests currently xfail; they will turn green as W5 lands. Do NOT skip
or remove them; they are the acceptance criteria for the commit pipeline.

The contract has three clauses:

    (A) Bit-identical rerun. Running Stage C twice on the same input
        produces the same git history: same commit SHAs, same order,
        same content. No fixup commits ever appear.

    (B) Detect-and-skip on partial reruns. If a previous run committed
        a [reforma] with trailer `Source-Id: BOE-A-X`, a subsequent run
        over the same target must detect that commit and skip
        re-applying the patch for BOE-A-X. New modifiers fetched between
        runs are applied as fresh [reforma] commits on top.

    (C) Commit-pointer stickiness. A [reforma] commit-pointer (emitted
        when regex+LLM couldn't reconstruct the text) does NOT get
        replaced on rerun, even if a later code change would have
        succeeded. The commit history is the legislative record; rewriting
        it to "fix" a past pointer corrupts integrity. The only way to
        promote a pointer to a real reform is an explicit
        `legalize reprocess --country es --norm BOE-A-X` command that
        rewrites the ENTIRE file's history from scratch.

Each clause has one test below.
"""

from __future__ import annotations

import pytest


pytestmark = pytest.mark.xfail(
    reason="W5 stage C committer not yet implemented — these are the contract",
    strict=True,
)


def test_rerun_produces_identical_commit_shas(tmp_path) -> None:
    """Clause (A): bit-identical reruns.

    Spec:
      - Setup: an empty git repo, a bootstrap XML fixture, a list of
        modifier XMLs.
      - Run 1: Stage C commits bootstrap + N reformas.
      - Delete the working tree (keep only .git).
      - Re-clone fresh; run Stage C again.
      - Assert: `git log --pretty=%H` is identical between the two runs.

    Why strict: our commit dates come from BOE fecha_publicacion, our
    author from config, our committer from config. Given identical
    inputs, the SHA must be deterministic.
    """
    pytest.fail("committer.stage_c not built yet")


def test_partial_rerun_skips_already_applied_modifiers(tmp_path) -> None:
    """Clause (B): detect-and-skip.

    Spec:
      - Start with a repo that already has [bootstrap] + [reforma] for
        modifiers M1 and M2, but not M3.
      - Re-run Stage C over all three modifiers.
      - Assert: no new commits for M1 or M2 (detected via
        `git log --grep "Source-Id: {M1_id}"`). A single new [reforma]
        lands for M3.
      - The pre-existing commits' SHAs remain unchanged.
    """
    pytest.fail("committer.stage_c not built yet")


def test_commit_pointer_is_not_replaced_on_rerun(tmp_path) -> None:
    """Clause (C): commit-pointer stickiness.

    Spec:
      - Run 1: modifier M1's patch cannot be reconstructed (simulate by
        feeding the pipeline a modifier with no «...» blocks and no LLM
        available). A commit-pointer [reforma] lands.
      - "Fix" the regex/LLM stub so that a rerun WOULD succeed.
      - Run 2: no fixup commit lands. The original pointer is preserved.
      - The only way to upgrade to a real reform is
        `legalize reprocess --country es --norm {target_id}`, which
        drops the whole file's history and rebuilds — tested separately.
    """
    pytest.fail("committer.stage_c not built yet")
