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

import dataclasses
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from legalize.config import load_config  # noqa: E402
from legalize.models import NormMetadata  # noqa: E402
from legalize.storage import load_norma_from_json  # noqa: E402
from legalize.transformer.markdown import render_norm_at_date  # noqa: E402


def _gazette_pdf_page(last_reform_dof: str) -> str:
    """Return the DOF gazette index URL for the given ISO date string."""
    d = date.fromisoformat(last_reform_dof)
    return (
        f"https://www.diariooficial.gob.mx/index_100.php"
        f"?year={d.year}&month={d.month:02d}&day={d.day:02d}#gsc.tab=0"
    )


def _enrich_mx_metadata(metadata: NormMetadata) -> NormMetadata:
    """Inject MX-specific frontmatter fields into the metadata.

    - Sets jurisdiction="federal" for all Diputados laws.
    - Promotes last_reform_dof from extra to a standalone extra entry (keeping
      it but ensuring gazette_pdf_page is derived and added alongside it).
    - Adds gazette_pdf_page derived from last_reform_dof.
    - Removes last_reform_dof from extra to avoid duplication since it is now
      rendered explicitly via the top-level extra entries below.
    """
    # Rebuild extra: strip last_reform_dof (we re-add it + gazette_pdf_page at end)
    last_reform_dof: str | None = None
    filtered_extra: list[tuple[str, str]] = []
    for key, value in metadata.extra:
        if key == "last_reform_dof":
            last_reform_dof = value
        else:
            filtered_extra.append((key, value))

    # Re-append last_reform_dof and gazette_pdf_page at the end of extra
    if last_reform_dof:
        filtered_extra.append(("last_reform_dof", last_reform_dof))
        filtered_extra.append(("gazette_pdf_page", _gazette_pdf_page(last_reform_dof)))

    # Prepend classification extras (gov_organ, entidad_federativa) so they
    # appear immediately after jurisdiction in the frontmatter.
    classification_extra: list[tuple[str, str]] = [
        ("gov_organ", "congreso_federal"),
        ("entidad_federativa", "na"),
    ]

    return dataclasses.replace(
        metadata,
        jurisdiction="federal",
        extra=tuple(classification_extra + filtered_extra),
    )


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
        metadata = _enrich_mx_metadata(norm.metadata)
        target_date = metadata.last_modified or metadata.publication_date
        markdown = render_norm_at_date(
            metadata,
            norm.blocks,
            target_date,
            include_all=True,
        )
        md_path = out_dir / f"{metadata.identifier}.md"
        md_path.write_text(markdown)
        rendered += 1
        print(f"  {md_path.relative_to(REPO_ROOT)}: {len(markdown):>9,} chars")

    print(f"\nRendered {rendered} laws to exports/mx/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
