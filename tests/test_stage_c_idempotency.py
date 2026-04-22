"""Stage C rerun-idempotency contract tests.

Three clauses the driver MUST satisfy — previously xfail, now live after
W5 landed. See src/legalize/committer/stage_c.py for the implementation.

    (A) Bit-identical rerun. Running Stage C twice on the same input
        produces the same git history: same commit SHAs, same order,
        same content. No fixup commits ever appear.

    (B) Detect-and-skip. Modifiers already committed (identified via
        the Source-Id trailer) are NEVER re-applied on subsequent runs.

    (C) Commit-pointer stickiness. A pointer commit (emitted when no
        patch passed the gates) survives reruns unchanged. The only
        way to upgrade a pointer to a real reform is an explicit
        reprocess command that wipes + rebuilds the file's history.

Each clause has one acceptance test; extra smoke tests exercise the
moving parts (bootstrap, trailer format, allow-empty plumbing) so
regressions are caught close to source.

Fixtures: the existing modif-ley-reales.xml + base-irpf.xml pair is
used as a real Stage C scenario. modif-ley-reales ANADE-s artículo 61
bis to Ley 35/2006 (IRPF, BOE-A-2006-20764). The anchor doesn't resolve
cleanly (the article doesn't pre-exist → correct commit-pointer path),
which is exactly what clause C needs to validate.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


from legalize.committer.git_ops import GitRepo
from legalize.committer.stage_c import StageCDriver, TargetResult


FIXTURES = Path(__file__).parent / "fixtures" / "stage_c"
TARGET_ID = "BOE-A-2006-20764"  # Ley 35/2006 IRPF
MODIFIER_ID = "BOE-A-2007-19290"  # pretend: modif-ley-reales acts on IRPF


# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────


def _make_fetcher() -> "Callable":  # noqa: F821
    """Return a fetcher that maps BOE-IDs to our fixture XMLs, and injects
    a synthetic <posteriores> into the IRPF XML so the driver discovers
    the modifier without network access."""
    from lxml import etree

    base_bytes = (FIXTURES / "base-irpf.xml").read_bytes()

    root = etree.fromstring(base_bytes)
    analisis = root.find("analisis")
    if analisis is None:
        analisis = etree.SubElement(root, "analisis")
    refs = analisis.find("referencias")
    if refs is None:
        refs = etree.SubElement(analisis, "referencias")
    posts = refs.find("posteriores")
    if posts is None:
        posts = etree.SubElement(refs, "posteriores")
    # Clear existing posteriores for determinism + add our modifier.
    for child in list(posts):
        posts.remove(child)
    post = etree.SubElement(posts, "posterior")
    post.set("referencia", MODIFIER_ID)
    post.set("orden", "2015")
    palabra = etree.SubElement(post, "palabra")
    palabra.set("codigo", "407")
    palabra.text = "AÑADE"
    texto = etree.SubElement(post, "texto")
    texto.text = "un articulo 61 bis"

    patched_base = etree.tostring(root, encoding="utf-8", xml_declaration=True)

    modifier_bytes = (FIXTURES / "modif-ley-reales.xml").read_bytes()
    # modif-ley-reales's real <metadatos>/<identificador> says its own ID,
    # but our synthetic <posterior> names MODIFIER_ID. Rewrite the
    # identifier in the modifier payload too so parse_amendments agrees.
    mroot = etree.fromstring(modifier_bytes)
    mmeta = mroot.find("metadatos")
    if mmeta is not None:
        ident = mmeta.find("identificador")
        if ident is not None:
            ident.text = MODIFIER_ID
    patched_mod = etree.tostring(mroot, encoding="utf-8", xml_declaration=True)

    store = {TARGET_ID: patched_base, MODIFIER_ID: patched_mod}

    def fetcher(boe_id: str) -> bytes:
        return store[boe_id]

    return fetcher


def _make_driver(tmp_path: Path) -> StageCDriver:
    repo_path = tmp_path / "legalize-es"
    repo_path.mkdir(parents=True, exist_ok=True)
    repo = GitRepo(repo_path, committer_name="Legalize", committer_email="bot@legalize.dev")
    repo.init()
    return StageCDriver(repo=repo, fetch_diario=_make_fetcher())


def _git_log_shas(repo_path: Path) -> list[str]:
    out = subprocess.check_output(["git", "log", "--format=%H", "--reverse"], cwd=repo_path)
    return out.decode().strip().splitlines()


def _git_log_trailers(repo_path: Path) -> list[tuple[str, str]]:
    """Returns (source_id, norm_id) per commit, in chronological order."""
    fmt = "%(trailers:key=Source-Id,valueonly,separator=)%x09%(trailers:key=Norm-Id,valueonly,separator=)"
    out = subprocess.check_output(
        ["git", "log", "--reverse", f"--format={fmt}"],
        cwd=repo_path,
    ).decode()
    rows = []
    for line in out.splitlines():
        if "\t" in line:
            s, _, n = line.partition("\t")
            rows.append((s.strip(), n.strip()))
    return rows


# ──────────────────────────────────────────────────────────
# Clause A: bit-identical reruns
# ──────────────────────────────────────────────────────────


def test_clause_a_rerun_produces_identical_commit_shas(tmp_path: Path) -> None:
    # First run from scratch.
    d1 = _make_driver(tmp_path / "run1")
    d1.process_target(TARGET_ID)
    shas_1 = _git_log_shas(d1.repo._path)
    assert len(shas_1) >= 2  # at minimum: bootstrap + one modifier outcome

    # Second run in a fresh repo with IDENTICAL inputs.
    d2 = _make_driver(tmp_path / "run2")
    d2.process_target(TARGET_ID)
    shas_2 = _git_log_shas(d2.repo._path)

    assert shas_1 == shas_2, (
        f"SHAs diverged between identical runs:\nrun1: {shas_1}\nrun2: {shas_2}"
    )


# ──────────────────────────────────────────────────────────
# Clause B: detect-and-skip
# ──────────────────────────────────────────────────────────


def test_clause_b_partial_rerun_skips_already_applied_modifiers(tmp_path: Path) -> None:
    driver = _make_driver(tmp_path / "run")
    # First run lands bootstrap + modifier outcome.
    driver.process_target(TARGET_ID)
    shas_first = _git_log_shas(driver.repo._path)
    commit_count_first = len(shas_first)
    assert commit_count_first >= 2

    # Second invocation on the same repo: no new commits must land
    # because everything is already there.
    result_second: TargetResult = driver.process_target(TARGET_ID)
    shas_second = _git_log_shas(driver.repo._path)
    assert shas_first == shas_second, (
        f"partial rerun introduced new commits; was {commit_count_first}, now {len(shas_second)}"
    )

    # And the outcomes must report "skipped_existing" on the second run.
    assert result_second.bootstrap_status == "existing"
    for outcome in result_second.modifier_outcomes:
        assert outcome.status == "skipped_existing", (
            f"modifier {outcome.modifier_id} did not skip; got {outcome.status}"
        )


# ──────────────────────────────────────────────────────────
# Clause C: commit-pointer stickiness
# ──────────────────────────────────────────────────────────


def test_clause_c_commit_pointer_is_not_replaced_on_rerun(tmp_path: Path) -> None:
    """The modifier in our fixture ANADE-s artículo 61 bis to IRPF but
    does not name an existing parent → anchor_not_found → commit-pointer.
    On rerun the pointer must survive unchanged; no fixup commit lands."""
    driver = _make_driver(tmp_path / "run")
    first = driver.process_target(TARGET_ID)

    # Locate the pointer outcome.
    pointer_outcomes = [o for o in first.modifier_outcomes if o.status == "commit_pointer"]
    assert pointer_outcomes, "expected at least one commit-pointer given the ANADE anchor miss"
    pointer_sha = pointer_outcomes[0].sha
    assert pointer_sha, "pointer outcome carries no SHA"

    trailers_first = _git_log_trailers(driver.repo._path)

    # Second run on the SAME repo must not rewrite the pointer.
    driver2 = StageCDriver(repo=driver.repo, fetch_diario=_make_fetcher())
    second = driver2.process_target(TARGET_ID)

    trailers_second = _git_log_trailers(driver.repo._path)
    assert trailers_first == trailers_second, "commit-pointer trailers changed on rerun"

    # The pointer's SHA must still be in the log, unchanged.
    current_shas = _git_log_shas(driver.repo._path)
    assert pointer_sha in current_shas, "pointer SHA vanished between runs"

    # And the second run must have reported the pointer as skipped.
    assert all(o.status in ("skipped_existing", "skipped_queued") for o in second.modifier_outcomes)


# ──────────────────────────────────────────────────────────
# Smoke
# ──────────────────────────────────────────────────────────


def test_bootstrap_commit_carries_expected_trailers(tmp_path: Path) -> None:
    driver = _make_driver(tmp_path / "run")
    driver.process_target(TARGET_ID)
    trailers = _git_log_trailers(driver.repo._path)
    # First commit in reverse order is the bootstrap.
    source_id, norm_id = trailers[0]
    assert norm_id == TARGET_ID
    assert source_id == TARGET_ID  # bootstrap self-references


def test_driver_returns_modifier_outcomes(tmp_path: Path) -> None:
    driver = _make_driver(tmp_path / "run")
    result = driver.process_target(TARGET_ID)
    assert result.target_id == TARGET_ID
    assert result.bootstrap_status == "committed"
    assert result.modifier_outcomes  # at least one modifier discovered
    # Bootstrap SHA is populated and non-empty.
    assert result.bootstrap_sha
