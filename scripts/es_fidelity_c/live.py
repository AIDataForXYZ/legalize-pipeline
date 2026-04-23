"""Stage C live fidelity runner (PLAN-STAGE-C.md §W4 — production path).

Fetches real Circulares BdE / CNMV (non-consolidated) from the BOE
/diario_boe/xml.php endpoint and feeds them through the exact same
dry-run pipeline that `run.py` uses for fixtures. The goal is to
measure how the pipeline behaves on real production inputs — not on
hand-picked fixtures that were chosen to exercise specific code paths.

Discovery strategy (avoids querying an enormous sumario range):

  1. Walk BOE sumarios in a configurable date window (default: 2 years
     ending at today). Every sumario lists every disposition of the day.
  2. Pick items whose department is "BANCO DE ESPAÑA" or "COMISIÓN
     NACIONAL DEL MERCADO DE VALORES" and whose title starts with
     "Circular" or "Instrucción". Those are *modifier candidates* —
     Circulares that typically amend older ones.
  3. Fetch each modifier's diario XML and read <analisis>/<referencias>/
     <anteriores>. The anteriores (verb 270/407/235/210) point at the
     *target* norms we actually want to reconstruct.
  4. Deduplicate targets. For each target we fetch its own diario XML
     and read <posteriores> to enumerate the full modifier chain.

Cache layout (``/tmp/stage-c-live`` by default):

    sumarios/<YYYYMMDD>.xml        daily sumarios (skip weekends)
    targets/<BOE-ID>.xml           the reconstructable norm
    modifiers/<BOE-ID>.xml         each modifier in a target's chain
    discover.json                  {target_id: [modifier_ids]} index

CLI:

    uv run python scripts/es_fidelity_c/live.py \\
        --start 2024-01-01 --end 2026-04-23 \\
        --max-targets 30 \\
        --cache /tmp/stage-c-live

The runner is incremental: anything already cached is reused, so
re-running is cheap. Only the final fidelity report is regenerated
every time.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from lxml import etree

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from legalize.fetcher.es.amendments import parse_amendments, parse_anteriores  # noqa: E402
from legalize.llm.amendment_parser import AmendmentLLM, LLMConfig  # noqa: E402
from legalize.llm.dispatcher import (  # noqa: E402
    dispatch_modifier_patches,
    dispatch_patch,
    extract_modifier_body_text,
)
from legalize.llm.queue import PendingCaseQueue  # noqa: E402
from legalize.transformer.markdown import render_paragraphs  # noqa: E402
from legalize.transformer.patcher import apply_patch  # noqa: E402
from legalize.transformer.xml_parser import parse_diario_xml  # noqa: E402

from scripts.es_fidelity_c.score import build_report, record_from_patch  # noqa: E402
from scripts.es_fidelity_c.report import render_markdown_summary  # noqa: E402

logger = logging.getLogger("stage_c.live")


DEPARTMENT_WHITELIST = {
    "BANCO DE ESPAÑA",
    "COMISIÓN NACIONAL DEL MERCADO DE VALORES",
    "COMISION NACIONAL DEL MERCADO DE VALORES",
}

TITLE_PREFIXES = ("Circular", "Instrucción", "Instruccion")

USER_AGENT = "Legalize-Pipeline/StageC-Fidelity (https://legalize.dev)"
RATE_LIMIT_SECONDS = 0.35  # ~2.8 req/s


class BoeFetcher:
    """Minimal HTTP client with disk cache + polite rate limiting."""

    def __init__(self, cache_root: Path) -> None:
        self.cache_root = cache_root
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": USER_AGENT, "Accept": "application/xml"}
        )
        self._last_hit = 0.0

    def _wait(self) -> None:
        elapsed = time.monotonic() - self._last_hit
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_hit = time.monotonic()

    def sumario(self, d: date) -> bytes | None:
        path = self.cache_root / "sumarios" / f"{d:%Y%m%d}.xml"
        if path.exists():
            return path.read_bytes()
        url = f"https://www.boe.es/datosabiertos/api/boe/sumario/{d:%Y%m%d}"
        self._wait()
        try:
            r = self._session.get(url, timeout=30)
        except requests.RequestException as e:
            logger.warning("sumario %s fetch error: %s", d, e)
            return None
        if r.status_code == 404:
            # No sumario for this date (Sundays, some holidays).
            path.write_bytes(b"")
            return None
        if r.status_code != 200:
            logger.warning("sumario %s → HTTP %s", d, r.status_code)
            return None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(r.content)
        return r.content

    def diario(self, boe_id: str, subdir: str) -> bytes | None:
        path = self.cache_root / subdir / f"{boe_id}.xml"
        if path.exists():
            data = path.read_bytes()
            return data if data else None
        url = f"https://www.boe.es/diario_boe/xml.php?id={boe_id}"
        self._wait()
        try:
            r = self._session.get(url, timeout=30)
        except requests.RequestException as e:
            logger.warning("diario %s fetch error: %s", boe_id, e)
            return None
        if r.status_code != 200:
            logger.warning("diario %s → HTTP %s", boe_id, r.status_code)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"")
            return None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(r.content)
        return r.content


# ──────────────────────────────────────────────────────────
# Discovery
# ──────────────────────────────────────────────────────────


def iter_sumario_dates(start: date, end: date):
    """Walk business days (Mon-Sat) from start to end inclusive."""
    cur = start
    while cur <= end:
        if cur.weekday() != 6:  # skip Sundays
            yield cur
        cur += timedelta(days=1)


def extract_candidate_modifiers(
    xml_bytes: bytes,
    *,
    dept_whitelist: frozenset[str] | None = None,
    title_prefixes: tuple[str, ...] | None = None,
) -> list[str]:
    """Pick items whose department matches ``dept_whitelist`` and whose
    title begins with any prefix in ``title_prefixes``. Both filters are
    case-insensitive on title; department names come from BOE upper-case.

    Defaults preserve the original Circular BdE/CNMV behaviour so existing
    callers are not broken.
    """
    if not xml_bytes:
        return []
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return []

    depts = dept_whitelist if dept_whitelist is not None else DEPARTMENT_WHITELIST
    prefixes = title_prefixes if title_prefixes is not None else TITLE_PREFIXES

    hits: list[str] = []
    for dept in root.iter("departamento"):
        name = (dept.get("nombre") or "").strip().upper()
        if depts and name not in depts:
            continue
        for item in dept.iter("item"):
            title_el = item.find("titulo")
            id_el = item.find("identificador")
            if title_el is None or id_el is None:
                continue
            title = (title_el.text or "").strip()
            if prefixes and not title.startswith(prefixes):
                continue
            boe_id = (id_el.text or "").strip()
            if boe_id.startswith("BOE-A-"):
                hits.append(boe_id)
    return hits


def target_ids_from_modifier(xml_bytes: bytes) -> list[str]:
    """Read <anteriores> from a modifier XML and return the referenced
    target norm IDs whose verb is within Stage C scope.

    `parse_anteriores` already filters out-of-scope verbs for us."""
    if not xml_bytes:
        return []
    patches = parse_anteriores(xml_bytes)
    seen: set[str] = set()
    out: list[str] = []
    for p in patches:
        if p.target_id.startswith("BOE-A-") and p.target_id not in seen:
            seen.add(p.target_id)
            out.append(p.target_id)
    return out


def posteriores_from_target(xml_bytes: bytes) -> list[str]:
    """Read <posteriores> on the target's own XML and return modifier
    BOE IDs in document order. We do NOT dedupe here — a single
    modifier can appear with several verbs, but for fetch-once it's
    enough to dedupe downstream."""
    if not xml_bytes:
        return []
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return []
    out: list[str] = []
    posts = root.find(".//analisis/referencias/posteriores")
    if posts is None:
        return []
    for p in posts.findall("posterior"):
        mid = (p.get("referencia") or "").strip()
        if mid.startswith("BOE-A-") and mid not in out:
            out.append(mid)
    return out


def discover(
    fetcher: BoeFetcher,
    start: date,
    end: date,
    max_targets: int,
    *,
    dept_whitelist: frozenset[str] | None = None,
    title_prefixes: tuple[str, ...] | None = None,
) -> dict[str, list[str]]:
    """Full discovery pass. Returns {target_id: [modifier_ids]}."""
    index_path = fetcher.cache_root / "discover.json"
    if index_path.exists():
        saved = json.loads(index_path.read_text())
        if len(saved) >= max_targets:
            logger.info("discover cache hit: %d targets", len(saved))
            return saved
    else:
        saved = {}

    targets: dict[str, list[str]] = dict(saved)
    modifier_candidates: list[str] = []

    days = list(iter_sumario_dates(start, end))
    logger.info("scanning %d days between %s and %s", len(days), start, end)
    for d in days:
        xml = fetcher.sumario(d)
        modifier_candidates.extend(
            extract_candidate_modifiers(
                xml,
                dept_whitelist=dept_whitelist,
                title_prefixes=title_prefixes,
            )
        )

    # Dedupe modifiers; walk newest first so we maximise chance of
    # finding targets with rich <posteriores>.
    seen: set[str] = set()
    unique_mods = []
    for m in reversed(modifier_candidates):
        if m not in seen:
            seen.add(m)
            unique_mods.append(m)

    logger.info("modifier candidates: %d unique", len(unique_mods))

    for mod_id in unique_mods:
        if len(targets) >= max_targets:
            break
        mod_xml = fetcher.diario(mod_id, "modifiers")
        tids = target_ids_from_modifier(mod_xml)
        if not tids:
            continue
        for tid in tids:
            if len(targets) >= max_targets:
                break
            if tid in targets:
                continue
            t_xml = fetcher.diario(tid, "targets")
            if not t_xml:
                continue
            posteriores = posteriores_from_target(t_xml)
            if not posteriores:
                continue
            targets[tid] = posteriores
            logger.info("target %-20s with %d posteriores", tid, len(posteriores))

    index_path.write_text(json.dumps(targets, indent=2, ensure_ascii=False))
    return targets


# ──────────────────────────────────────────────────────────
# Fidelity dry-run on live data
# ──────────────────────────────────────────────────────────


def _render_base_markdown(xml_bytes: bytes) -> str | None:
    if not xml_bytes:
        return None
    blocks = parse_diario_xml(xml_bytes)
    if not blocks:
        return None
    parts: list[str] = []
    for block in blocks:
        if not block.versions:
            continue
        parts.append(render_paragraphs(list(block.versions[0].paragraphs)))
    return "\n\n".join(p for p in parts if p) or None


def _modifier_excerpt_len(xml_bytes: bytes) -> int:
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return len(xml_bytes)
    texto = root.find("texto")
    if texto is None:
        return 0
    return len(" ".join(texto.itertext()))


def fidelity_dry_run(
    fetcher: BoeFetcher,
    targets: dict[str, list[str]],
    *,
    llm: AmendmentLLM | None = None,
    queue: PendingCaseQueue | None = None,
    daily_mode: bool = False,
    use_structured: bool = False,
    cumulative: bool = True,
):
    """Dry-run apply_patch on every (target, modifier) pair in the
    discovery index. Returns (report, records, raw_apply_log).

    When ``cumulative=True`` (default, session-4 fix) we thread the
    working markdown through each modifier in chronological order, just
    like ``StageCDriver`` does in production. Previously each
    (target, modifier) pair was evaluated against the ORIGINAL bootstrap
    markdown, which undercounted coverage drastically: a modifier that
    references ``articulo 20`` of a law whose original bootstrap only
    had 11 articulos always missed, even when a prior reform had added
    articulo 20. This flag flips off the old per-modifier isolation
    behaviour so the numbers reflect real chain state.

    The internal calls pass ``dry_run=False`` so the patcher returns the
    post-mutation markdown (we need it to seed the next modifier). We
    never write to disk, so this is still a measurement-only tool."""
    records = []
    raw_log: list[dict] = []
    out_of_scope_total = 0

    # Cache base (original bootstrap) markdown per target — used as the
    # starting state for the per-target modifier chain.
    base_cache: dict[str, str | None] = {}
    # When cumulative, we additionally carry the working markdown per
    # target and mutate it as modifiers land.
    working_by_target: dict[str, str] = {}

    for target_id, modifier_ids in targets.items():
        t_xml = fetcher.diario(target_id, "targets")
        if t_xml is None:
            continue
        if target_id not in base_cache:
            base_cache[target_id] = _render_base_markdown(t_xml)
        base_md0 = base_cache.get(target_id)
        if base_md0 is None:
            continue
        working_by_target[target_id] = base_md0

        # Sort modifiers chronologically by BOE-A-YYYY-NNN so cumulative
        # state evolves in the same order StageCDriver uses in prod.
        # document order is usually but not always chronological.
        def _mod_sort_key(mid: str) -> tuple[int, int]:
            parts = mid.split("-")
            try:
                year = int(parts[2])
                num = int(parts[3])
            except (IndexError, ValueError):
                return (9999, 9999999)
            return (year, num)

        sorted_modifier_ids = sorted(set(modifier_ids), key=_mod_sort_key)
        for mod_id in sorted_modifier_ids:
            mod_xml = fetcher.diario(mod_id, "modifiers")
            if mod_xml is None:
                continue
            raw_patches = parse_anteriores(mod_xml)
            patches = parse_amendments(mod_xml)

            try:
                root = etree.fromstring(mod_xml)
                anteriores = root.findall(".//analisis/referencias/anteriores/anterior")
                out_of_scope_total += max(0, len(anteriores) - len(raw_patches))
            except Exception:
                pass

            excerpt_len = _modifier_excerpt_len(mod_xml)
            modifier_body = extract_modifier_body_text(mod_xml)

            # Filter to patches for THIS target so the group dispatcher
            # sees a single (modifier, target) bucket at a time.
            target_patches = [p for p in patches if p.target_id == target_id]
            if not target_patches:
                continue

            # The patch extractor used a stable ordering_key from the
            # modifier XML, so sorting here keeps dispatches in the same
            # order StageCDriver would use in production.
            target_patches.sort(key=lambda p: (p.ordering_key, p.anchor_hint))

            base_md = (
                working_by_target[target_id] if cumulative else base_md0
            )

            # In cumulative mode we want the mutation back in new_markdown.
            # dry_run=False is safe because nothing hits disk from this path.
            internal_dry = not cumulative

            dispatches: list = []
            if llm is None and queue is None:
                working = base_md
                for patch in target_patches:
                    ar = apply_patch(working, patch, dry_run=internal_dry)
                    if ar.status in ("applied", "dry_run_ok"):
                        working = ar.new_markdown
                    records.append(record_from_patch(
                        patch,
                        modifier_excerpt_len=excerpt_len,
                        apply_result=ar,
                    ))
                    raw_log.append({
                        "target_id": target_id,
                        "modifier_id": mod_id,
                        "verb_code": patch.verb_code,
                        "operation": patch.operation,
                        "extractor": patch.extractor,
                        "anchor_confidence": patch.anchor_confidence,
                        "new_text_confidence": patch.new_text_confidence,
                        "apply_status": ar.status,
                        "apply_reason": ar.reason,
                        "dispatch_tier": "",
                        "dispatch_status": "",
                    })
                if cumulative:
                    working_by_target[target_id] = working
                continue

            if use_structured:
                dispatches = list(dispatch_modifier_patches(
                    base_md,
                    target_patches,
                    modifier_body=modifier_body,
                    llm=llm,
                    queue=queue,
                    dry_run=internal_dry,
                    daily_mode=daily_mode,
                    use_structured=True,
                ))
            else:
                working = base_md
                for patch in target_patches:
                    d = dispatch_patch(
                        working,
                        patch,
                        modifier_body=modifier_body,
                        llm=llm,
                        queue=queue,
                        dry_run=internal_dry,
                        daily_mode=daily_mode,
                    )
                    if d.applied and d.patch_result is not None:
                        working = d.patch_result.new_markdown
                    dispatches.append(d)

            # Thread the cumulative state forward — take the last applied
            # mutation from the batch.
            if cumulative:
                working = base_md
                for d in dispatches:
                    if d.applied and d.patch_result is not None:
                        working = d.patch_result.new_markdown
                working_by_target[target_id] = working

            for d in dispatches:
                enriched_patch = d.patch
                apply_result = d.patch_result
                records.append(record_from_patch(
                    enriched_patch,
                    modifier_excerpt_len=excerpt_len,
                    apply_result=apply_result,
                ))
                raw_log.append({
                    "target_id": target_id,
                    "modifier_id": mod_id,
                    "verb_code": enriched_patch.verb_code,
                    "operation": enriched_patch.operation,
                    "extractor": enriched_patch.extractor,
                    "anchor_confidence": enriched_patch.anchor_confidence,
                    "new_text_confidence": enriched_patch.new_text_confidence,
                    "apply_status": apply_result.status if apply_result else None,
                    "apply_reason": apply_result.reason if apply_result else None,
                    "dispatch_tier": d.tier,
                    "dispatch_status": d.status,
                })

    report = build_report(
        records,
        sample_size=len(targets),
        out_of_scope_count=out_of_scope_total,
    )
    return report, records, raw_log


# ──────────────────────────────────────────────────────────
# LLM wiring
# ──────────────────────────────────────────────────────────


def _load_groq_key() -> str | None:
    """Look for the Groq API key in (1) env var, (2) repo-local file,
    (3) ~/.groq_api_key. Returns None when none of the three are present —
    the caller prints a clear message and exits."""
    import os

    if k := os.environ.get("GROQ_API_KEY"):
        return k
    for candidate in (ROOT / "groq_api_key", Path.home() / ".groq_api_key"):
        if candidate.exists():
            return candidate.read_text(encoding="utf-8").strip()
    return None


def _build_llm_and_queue(
    *,
    cache_root: Path,
    groq_model: str,
    queue_dir: Path,
) -> tuple[AmendmentLLM, PendingCaseQueue]:
    """Construct the Groq client + the Claude-resolver queue.

    Cache goes under ``<cache_root>/llm-cache`` so that reruns of the
    fidelity loop are cheap once the prompts have been seen. The queue
    JSONL streams live at ``queue_dir``.
    """
    key = _load_groq_key()
    if not key:
        raise SystemExit(
            "--with-llm requires GROQ_API_KEY (env, ./groq_api_key or ~/.groq_api_key)"
        )
    cfg = LLMConfig(
        backend="groq",
        api_key=key,
        escalation=(groq_model,),
        cache_dir=cache_root / "llm-cache",
    )
    llm = AmendmentLLM(cfg)
    queue = PendingCaseQueue(queue_dir)
    return llm, queue


# ──────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01",
                    help="Sumario walk start date (YYYY-MM-DD).")
    ap.add_argument("--end", default=date.today().isoformat(),
                    help="Sumario walk end date (YYYY-MM-DD, inclusive).")
    ap.add_argument("--max-targets", type=int, default=30,
                    help="Stop discovery once we have this many targets.")
    ap.add_argument("--cache", default="/tmp/stage-c-live",
                    help="Cache directory root.")
    ap.add_argument("--csv", default=None,
                    help="CSV path (default: <cache>/fidelity-log.csv).")
    ap.add_argument("--report", default=None,
                    help="Markdown report path (default: <cache>/fidelity-report.md).")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument(
        "--with-llm",
        action="store_true",
        help="Route short/medium cases through Groq and hard cases to the Claude queue.",
    )
    ap.add_argument(
        "--groq-model",
        default="openai/gpt-oss-120b",
        help="Groq model for the single-rung escalation ladder (default gpt-oss-120b).",
    )
    ap.add_argument(
        "--queue-dir",
        default=None,
        help="PendingCaseQueue root (default: <cache>/queue).",
    )
    ap.add_argument(
        "--daily-mode",
        action="store_true",
        help=(
            "Route hard tier through Groq too (daily-cron policy). "
            "Without this, hard cases are queued for Claude resolution."
        ),
    )
    ap.add_argument(
        "--use-structured",
        action="store_true",
        help=(
            "Use dispatch_modifier_patches with the LLM structured-edit "
            "extract (session 3 redesign). One call per modifier group, "
            "anchor bypasses parse_anchor_from_hint."
        ),
    )
    ap.add_argument(
        "--no-cumulative",
        action="store_true",
        help=(
            "Measure each modifier against the ORIGINAL bootstrap (legacy "
            "pre-session-4 behaviour). Default is cumulative: modifiers "
            "apply in chronological order and each one sees the state "
            "produced by prior modifiers, matching StageCDriver production."
        ),
    )
    ap.add_argument(
        "--dept",
        action="append",
        default=None,
        help=(
            "Department name to include in discovery (repeatable). Matched "
            "case-insensitively against the UPPER-case form BOE uses. "
            "Default: BANCO DE ESPAÑA + CNMV."
        ),
    )
    ap.add_argument(
        "--any-dept",
        action="store_true",
        help="Disable the department filter entirely (any issuer).",
    )
    ap.add_argument(
        "--title-prefix",
        action="append",
        default=None,
        help=(
            "Title prefix to include (repeatable, case-sensitive match "
            "against item <titulo>). Default: Circular/Instrucción."
        ),
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    cache = Path(args.cache)
    csv_path = Path(args.csv) if args.csv else cache / "fidelity-log.csv"
    report_path = Path(args.report) if args.report else cache / "fidelity-report.md"
    fetcher = BoeFetcher(cache)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    # ── 1. Discovery ──────────────────────────────────────
    if args.any_dept:
        dept_filter: frozenset[str] | None = frozenset()  # empty → disable filter
    elif args.dept:
        dept_filter = frozenset(d.strip().upper() for d in args.dept if d and d.strip())
    else:
        dept_filter = None  # use default whitelist
    title_filter = (
        tuple(p for p in args.title_prefix if p) if args.title_prefix else None
    )
    if dept_filter:
        logger.info("department filter: %s", sorted(dept_filter))
    if title_filter:
        logger.info("title prefix filter: %s", list(title_filter))
    targets = discover(
        fetcher,
        start,
        end,
        args.max_targets,
        dept_whitelist=dept_filter,
        title_prefixes=title_filter,
    )
    if not targets:
        logger.error("no targets discovered — widen the date range or check connectivity")
        return 1

    # ── 2. Fidelity dry-run ───────────────────────────────
    llm = None
    queue = None
    if args.with_llm:
        llm, queue = _build_llm_and_queue(
            cache_root=cache,
            groq_model=args.groq_model,
            queue_dir=Path(args.queue_dir) if args.queue_dir else cache / "queue",
        )
        logger.info(
            "LLM routing enabled — model=%s queue=%s",
            args.groq_model,
            queue.queue_dir,
        )
    report, records, raw_log = fidelity_dry_run(
        fetcher,
        targets,
        llm=llm,
        queue=queue,
        daily_mode=args.daily_mode,
        use_structured=args.use_structured,
        cumulative=not args.no_cumulative,
    )

    # ── 3. Outputs ────────────────────────────────────────
    import csv
    cache.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "modifier_id", "target_id", "verb_code", "operation",
            "anchor_conf", "new_text_conf", "extractor", "tier",
            "apply_status", "apply_reason",
        ])
        for r in records:
            w.writerow([
                r.modifier_id, r.target_id, r.verb_code, r.operation,
                f"{r.anchor_confidence:.2f}", f"{r.new_text_confidence:.2f}",
                r.extractor, r.tier,
                r.apply_status or "", r.apply_reason or "",
            ])

    (cache / "fidelity-raw.json").write_text(
        json.dumps(raw_log, indent=2, ensure_ascii=False)
    )

    md = render_markdown_summary(report)
    header = (
        f"# Stage C live fidelity — {date.today():%Y-%m-%d}\n\n"
        f"- Window: {args.start} → {args.end}\n"
        f"- Targets discovered: **{len(targets)}**\n"
        f"- Modifiers indexed: **{sum(len(v) for v in targets.values())}**\n"
        f"- Cache: `{cache}`\n\n"
        f"---\n\n"
    )
    report_path.write_text(header + md + "\n")
    print(header + md)
    print(f"\nCSV  → {csv_path}", file=sys.stderr)
    print(f"JSON → {cache / 'fidelity-raw.json'}", file=sys.stderr)
    print(f"MD   → {report_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
