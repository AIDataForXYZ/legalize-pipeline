"""Aggregate defects across all iterations run so far.

Usage:
    python -m scripts.es_fidelity.report --iter 1 > /tmp/es-audit/iteration-1-report.md
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(_SRC))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.es_fidelity.common import LOG_CSV  # noqa: E402

DEFECT_LABELS = {
    "NOT_IN_CONSOLIDATA": "not in /legislacion-consolidada (covered by Stage B)",
    "METADATA_PARSE_FAIL": "metadata parse failed",
    "PARSER_FAIL": "parse_text_xml crash",
    "TABLES_DROPPED": "tables entirely dropped from .md",
    "NOTAS_DROPPED": "nota_pie footnotes dropped",
    "CITAS_FLATTENED": "cita_con_pleca rendered as plain paragraph",
    "LIBROS_UNSTYLED": "libro_num/tit not a heading",
    "ANEXOS_UNSTYLED": "anexo_num/tit not a heading",
    "IMAGES_DROPPED": "images not linked to BOE CDN (policy §11)",
    "SUP_FLATTENED": "<sup> flattened, semantics lost",
    "SUB_FLATTENED": "<sub> flattened, semantics lost",
    "LINKS_LOST": "<a href> stripped",
    "METADATA_PLACEHOLDER": 'frontmatter has "test" or 2000-01-01 placeholder',
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--iter", type=int, required=True)
    ap.add_argument("--out", type=str, default=None)
    args = ap.parse_args()

    if not LOG_CSV.exists():
        print("No log yet. Run score.py first.", file=sys.stderr)
        return 1

    with LOG_CSV.open() as f:
        rows = list(csv.DictReader(f))

    this_iter = [r for r in rows if int(r["iter"]) == args.iter]
    if not this_iter:
        print(f"No data for iter {args.iter}.", file=sys.stderr)
        return 1

    total = len(this_iter)
    clean = [r for r in this_iter if not r["defects_str"]]
    defect_counter: Counter[str] = Counter()
    for r in this_iter:
        for d in r["defects_str"].split(","):
            if d:
                defect_counter[d.split("_TEXT_RATIO_")[0]] += 1  # keep one TEXT_RATIO bucket

    out_lines = [
        f"# Fidelity iteration {args.iter} — report",
        "",
        f"Sample size: **{total}** laws. Clean: **{len(clean)}** ({len(clean) * 100 // total if total else 0}%).",
        "",
        "## Top defect classes",
        "",
        "| Defect | Laws affected | % | Meaning |",
        "|---|---|---|---|",
    ]

    for defect, n in defect_counter.most_common():
        pct = int(n * 100 / total)
        label = DEFECT_LABELS.get(defect, defect)
        out_lines.append(f"| `{defect}` | {n} | {pct}% | {label} |")

    out_lines += ["", "## Per-law detail", "", "| ID | Text ratio | Tables | Notas | Citas | Img | Libros | Anexos | Defects |", "|---|---|---|---|---|---|---|---|---|"]

    for r in sorted(this_iter, key=lambda x: float(x["text_ratio"] or 0)):
        defects = r["defects_str"] or "CLEAN"
        out_lines.append(
            f"| {r['id']} | {r['text_ratio']} | "
            f"{r['tables_md']}/{r['tables_xml']} | "
            f"{r['notas_md']}/{r['notas_xml']} | "
            f"{r['citas_md']}/{r['citas_xml']} | "
            f"{r['img_md']}/{r['img_xml']} | "
            f"{r['libros_md']}/{r['libros_xml']} | "
            f"{r['anexos_md']}/{r['anexos_xml']} | "
            f"{defects} |"
        )

    # Progression if multiple iterations exist
    by_iter = Counter(int(r["iter"]) for r in rows)
    if len(by_iter) > 1:
        out_lines += ["", "## Iteration progression", "", "| Iter | Laws | Clean | Top defect |", "|---|---|---|---|"]
        for it in sorted(by_iter):
            sub = [r for r in rows if int(r["iter"]) == it]
            sub_clean = sum(1 for r in sub if not r["defects_str"])
            sub_defects: Counter[str] = Counter()
            for r in sub:
                for d in r["defects_str"].split(","):
                    if d:
                        sub_defects[d] += 1
            top = sub_defects.most_common(1)
            top_str = f"{top[0][0]} ({top[0][1]})" if top else "—"
            out_lines.append(f"| {it} | {len(sub)} | {sub_clean} | {top_str} |")

    out = "\n".join(out_lines)
    if args.out:
        Path(args.out).write_text(out)
    else:
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
