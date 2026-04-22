"""Stage C fidelity runner (PLAN-STAGE-C.md §W4).

Feeds a collection of modifier XMLs through the pipeline we have so far
and produces a FidelityReport. Deliberately dry-run: no git commits,
no external API calls (Groq/Claude queue paths are *classified* here
but never actually invoked — the goal is to measure how the REGEX stage
alone performs, and how many patches would need LLM/Claude attention).

Two usage modes:

  1. Smoke / development: feed the fixtures under
     tests/fixtures/stage_c/ to validate the scoring pipeline produces
     numbers at all, and to set a baseline we can regress-check
     against when tweaking regex patterns.

  2. Production fidelity run: feed a sample of ~500 real Circulares
     BdE/CNMV fetched via fetcher/es/client. This is W4's real payload
     and requires network + disk cache. Out of scope for this module;
     a companion `discover.py` will handle it when we run against the
     live BOE API.

CLI:

    uv run python scripts/es_fidelity_c/run.py [fixture_dir]

Exits 0. Writes ``/tmp/stage-c-fidelity-log.csv`` + prints a Markdown
summary to stdout.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))  # so `scripts.es_fidelity_c.*` is importable

from legalize.fetcher.es.amendments import parse_amendments, parse_anteriores  # noqa: E402
from legalize.transformer.markdown import render_paragraphs  # noqa: E402
from legalize.transformer.patcher import apply_patch  # noqa: E402
from legalize.transformer.xml_parser import parse_diario_xml  # noqa: E402

from scripts.es_fidelity_c.score import (  # noqa: E402
    FidelityReport,
    PatchRecord,
    build_report,
    record_from_patch,
)


def _render_base_markdown(xml_bytes: bytes) -> str | None:
    """Render a Stage-A-compatible Markdown for the target norm, used as
    the base against which we dry-run apply_patch. Returns None when the
    XML does not yield any block. Concatenates the first version of each
    block — enough for anchor resolution in Stage C dry-run."""
    blocks = parse_diario_xml(xml_bytes)
    if not blocks:
        return None
    parts: list[str] = []
    for block in blocks:
        if not block.versions:
            continue
        version = block.versions[0]
        parts.append(render_paragraphs(list(version.paragraphs)))
    return "\n\n".join(p for p in parts if p)


def run_on_fixtures(
    fixture_dir: Path,
    *,
    target_index_path: Path | None = None,
) -> tuple[FidelityReport, list[PatchRecord]]:
    """Run the fidelity loop over every ``modif-*.xml`` in fixture_dir.

    If target_index_path is provided, it must be a JSON mapping
    {target_boe_id: fixture_filename} so we can dry-run apply_patch
    against the right base. Missing targets fall through silently
    (apply_result=None) — the scorer still captures the parse stage.
    """
    import json

    modifier_paths = sorted(fixture_dir.glob("modif-*.xml"))
    targets: dict[str, Path] = {}
    if target_index_path and target_index_path.exists():
        raw = json.loads(target_index_path.read_text())
        for tid, fname in raw.items():
            path = fixture_dir / fname
            if path.exists():
                targets[tid] = path

    records: list[PatchRecord] = []
    out_of_scope_total = 0

    base_md_cache: dict[str, str | None] = {}

    for mpath in modifier_paths:
        data = mpath.read_bytes()
        # The parser filters out-of-scope verbs at parse_anteriores; we
        # measure that drop here so the report can show corpus shape.
        raw_patches = parse_anteriores(data)
        patches = parse_amendments(data)

        # Count anteriores entries that got filtered. parse_anteriores
        # already dropped them so we can't see them directly; but from
        # the fixture we know the shape: count <anterior> elements in
        # the XML vs patches returned.
        from lxml import etree
        try:
            root = etree.fromstring(data)
            anteriores = root.findall(".//analisis/referencias/anteriores/anterior")
            out_of_scope_total += max(0, len(anteriores) - len(raw_patches))
        except Exception:
            pass

        excerpt_len = _modifier_excerpt_len(data)

        for patch in patches:
            apply_result = None
            target_path = targets.get(patch.target_id)
            if target_path is not None:
                base_md = base_md_cache.get(patch.target_id)
                if base_md is None:
                    base_md = _render_base_markdown(target_path.read_bytes())
                    base_md_cache[patch.target_id] = base_md
                if base_md is not None:
                    apply_result = apply_patch(base_md, patch, dry_run=True)

            records.append(record_from_patch(
                patch,
                modifier_excerpt_len=excerpt_len,
                apply_result=apply_result,
            ))

    report = build_report(
        records,
        sample_size=len(modifier_paths),
        out_of_scope_count=out_of_scope_total,
    )
    return report, records


def _modifier_excerpt_len(xml_bytes: bytes) -> int:
    """Rough byte count of the modifier <texto> body — used by the case
    classifier to route short vs medium vs hard."""
    try:
        from lxml import etree
        root = etree.fromstring(xml_bytes)
        texto = root.find("texto")
        if texto is None:
            return 0
        return len(" ".join(texto.itertext()))
    except Exception:
        return len(xml_bytes)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "fixture_dir",
        nargs="?",
        default=str(ROOT / "tests" / "fixtures" / "stage_c"),
        help="Directory of modifier XMLs to process (default: test fixtures).",
    )
    ap.add_argument("--targets", default=None,
                    help="Optional JSON mapping {target_id: fixture_filename} "
                         "for dry-run apply_patch. Without it, only the parse "
                         "+ classify stages are measured.")
    ap.add_argument("--csv", default="/tmp/stage-c-fidelity-log.csv")
    args = ap.parse_args()

    fixture_dir = Path(args.fixture_dir)
    targets_path = Path(args.targets) if args.targets else None

    report, records = run_on_fixtures(fixture_dir, target_index_path=targets_path)

    # CSV log — one row per patch.
    import csv
    with open(args.csv, "w", encoding="utf-8", newline="") as f:
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

    # Markdown summary to stdout.
    from scripts.es_fidelity_c.report import render_markdown_summary
    print(render_markdown_summary(report))
    print(f"\nCSV log → {args.csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
