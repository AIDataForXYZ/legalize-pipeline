"""Render a FidelityReport to human-readable Markdown.

Stdout output used by `scripts/es_fidelity_c/run.py`. The format tracks
the dimensions PLAN-STAGE-C.md listed as exit criteria, so iterations
can be compared side by side (paste in to STATUS-STAGE-C.md as a table).
"""

from __future__ import annotations

from scripts.es_fidelity_c.score import VERB_LABEL, FidelityReport


def _pct(x: float) -> str:
    return f"{x * 100:.1f}%"


def render_markdown_summary(report: FidelityReport) -> str:
    lines: list[str] = []

    lines.append("# Stage C fidelity report")
    lines.append("")
    lines.append(f"- Sample size (modifier XMLs): **{report.sample_size}**")
    lines.append(f"- MVP-scoped patches produced: **{report.total_patches}**")
    lines.append(f"- Out-of-scope entries (correcciones, judicial, ...): {report.out_of_scope_count}")
    lines.append("")

    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- **regex-ready**: {_pct(report.regex_ready_pct)}  "
                 "_(both axes >= 0.9, no LLM needed)_")
    lines.append(f"- LLM-bound (short/medium): {_pct(report.llm_bound_pct)}")
    lines.append(f"- Claude-queue (hard): {_pct(report.claude_bound_pct)}")
    lines.append("")

    if report.apply_attempted:
        lines.append("## Apply stage (dry-run against base_markdown)")
        lines.append("")
        lines.append(f"- Attempted: {report.apply_attempted}")
        lines.append(f"- Applied OK: {_pct(report.applied_share)}")
        lines.append("")
        lines.append("| status | count |")
        lines.append("|---|---:|")
        for status, count in sorted(
            report.apply_status_counts.items(), key=lambda x: -x[1]
        ):
            lines.append(f"| {status} | {count} |")
        lines.append("")
    else:
        lines.append("## Apply stage")
        lines.append("")
        lines.append("_No base_markdown provided; skipped._")
        lines.append("")

    lines.append("## Per-verb breakdown")
    lines.append("")
    lines.append("| verb | total | regex-filled | fill rate |")
    lines.append("|---|---:|---:|---:|")
    for code in sorted(report.per_verb_total):
        total = report.per_verb_total[code]
        filled = report.per_verb_regex_filled.get(code, 0)
        rate = filled / total if total else 0
        label = VERB_LABEL.get(code, code)
        lines.append(f"| {label} ({code}) | {total} | {filled} | {_pct(rate)} |")
    lines.append("")

    lines.append("## Case-tier routing")
    lines.append("")
    lines.append("| tier | count |")
    lines.append("|---|---:|")
    for tier in ("regex_only", "short", "medium", "hard"):
        count = report.tier_counts.get(tier, 0)
        lines.append(f"| {tier} | {count} |")
    lines.append("")

    lines.append("## Confidence histograms (10 buckets, 0.0-1.0)")
    lines.append("")
    lines.append(_histogram_line("anchor_conf", report.anchor_confidence_buckets))
    lines.append(_histogram_line("new_text_conf", report.new_text_confidence_buckets))
    lines.append("")

    return "\n".join(lines)


def _histogram_line(name: str, buckets: list[int]) -> str:
    max_count = max(buckets) if buckets else 0
    if max_count == 0:
        bars = " ".join("." * 10)
        return f"- {name}: {bars}"
    bars: list[str] = []
    for count in buckets:
        ratio = count / max_count
        h = int(round(ratio * 8))
        bars.append(["·", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"][h])
    total = sum(buckets)
    return f"- {name:15s} [{''.join(bars)}]  ({total} patches)"
