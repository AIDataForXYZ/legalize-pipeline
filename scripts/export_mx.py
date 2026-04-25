#!/usr/bin/env python3
"""Render every parsed MX norm in the data cache to ``exports/mx/{id}.md``.

Hackathon convenience: the legalize engine ordinarily writes country output
into a sibling git repo (``../countries/mx``) so each country has its own
public history. While we're iterating on the parser, it's more useful to
have the rendered Markdown live next to the source so a single PR shows
both the parser change and its effect on the laws.

This script renders only — it does not touch git. Re-run after every
parser change; diff the output as part of code review.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from legalize.config import load_config  # noqa: E402
from legalize.storage import load_norma_from_json  # noqa: E402
from legalize.transformer.markdown import render_norm_at_date  # noqa: E402


def main() -> int:
    config = load_config(str(REPO_ROOT / "config.yaml"))
    cc = config.get_country("mx")
    json_dir = Path(cc.data_dir) / "json"
    if not json_dir.exists():
        print(f"No JSON cache at {json_dir}. Run `legalize fetch -c mx ...` first.")
        return 1

    out_dir = REPO_ROOT / "exports" / "mx"
    out_dir.mkdir(parents=True, exist_ok=True)

    rendered = 0
    for json_file in sorted(json_dir.glob("*.json")):
        norm = load_norma_from_json(json_file)
        markdown = render_norm_at_date(
            norm.metadata,
            norm.blocks,
            norm.metadata.publication_date,
            include_all=True,
        )
        md_path = out_dir / f"{norm.metadata.identifier}.md"
        md_path.write_text(markdown)
        rendered += 1
        print(f"  {md_path.relative_to(REPO_ROOT)}: {len(markdown):>9,} chars")

    print(f"\nRendered {rendered} laws to exports/mx/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
