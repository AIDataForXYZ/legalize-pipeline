"""Build per-target validation payloads for 3-subagent audit.

Reads the live cache at /tmp/stage-c-live, picks N targets, and for
each one emits a JSON bundle under /tmp/stage-c-live/validation/:

  {
    target_id, target_xml_head (first 2000 bytes),
    base_markdown_head (first 3000 chars),
    modifiers: [
      {
        modifier_id, patches: [ {verb_code, operation, anchor_hint,
        anchor_confidence, new_text_confidence, new_text_preview,
        extractor, apply_status, apply_reason} ]
      }
    ],
    aggregate: {total, applied, anchor_not_found, empty_new_text, hard}
  }

These JSONs are the input each subagent audits. Keeping bytes bounded
(head slices only) keeps the subagent context small while preserving
enough context to judge precision / invented-text / anchor quality.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from legalize.fetcher.es.amendments import parse_amendments  # noqa: E402
from legalize.transformer.markdown import render_paragraphs  # noqa: E402
from legalize.transformer.patcher import apply_patch  # noqa: E402
from legalize.transformer.xml_parser import parse_diario_xml  # noqa: E402


DEFAULT_TARGETS = [
    "BOE-A-2022-666",      # recent, 2 mods
    "BOE-A-2013-5720",     # Circular 1/2013 BdE, 13 mods (omnibus chain)
    "BOE-A-2017-14334",    # Circular 4/2017 BdE (famous), 8 mods
    "BOE-A-2021-6049",     # 3 mods
    "BOE-A-2010-13162",    # 2010, 6 mods
]


def render_base_markdown(xml_bytes: bytes) -> str:
    blocks = parse_diario_xml(xml_bytes)
    parts = []
    for block in blocks:
        if not block.versions:
            continue
        parts.append(render_paragraphs(list(block.versions[0].paragraphs)))
    return "\n\n".join(p for p in parts if p)


def build_payload(target_id: str, modifier_ids: list[str], cache: Path) -> dict:
    target_xml_path = cache / "targets" / f"{target_id}.xml"
    if not target_xml_path.exists() or target_xml_path.stat().st_size == 0:
        raise RuntimeError(f"target XML missing: {target_id}")
    target_xml = target_xml_path.read_bytes()
    base_md = render_base_markdown(target_xml)

    modifiers = []
    status_counts: Counter[str] = Counter()

    for mod_id in modifier_ids:
        mod_xml_path = cache / "modifiers" / f"{mod_id}.xml"
        if not mod_xml_path.exists() or mod_xml_path.stat().st_size == 0:
            continue
        mod_xml = mod_xml_path.read_bytes()
        patches = [p for p in parse_amendments(mod_xml) if p.target_id == target_id]

        patch_rows = []
        for patch in patches:
            result = apply_patch(base_md, patch, dry_run=True) if base_md else None
            status = result.status if result else None
            if status:
                status_counts[status] += 1
            patch_rows.append({
                "verb_code": patch.verb_code,
                "operation": patch.operation,
                "anchor_hint": patch.anchor_hint,
                "anchor_confidence": patch.anchor_confidence,
                "new_text_preview": (patch.new_text or "")[:500],
                "new_text_length": len(patch.new_text or ""),
                "new_text_confidence": patch.new_text_confidence,
                "extractor": patch.extractor,
                "apply_status": status,
                "apply_reason": result.reason if result else None,
            })
        modifiers.append({
            "modifier_id": mod_id,
            "patches": patch_rows,
            "patch_count": len(patch_rows),
        })

    return {
        "target_id": target_id,
        "target_xml_head": target_xml[:2000].decode("utf-8", errors="replace"),
        "target_xml_bytes": len(target_xml),
        "base_markdown_head": base_md[:3000],
        "base_markdown_chars": len(base_md),
        "modifier_count": len(modifiers),
        "modifiers": modifiers,
        "aggregate_status_counts": dict(status_counts),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="/tmp/stage-c-live")
    ap.add_argument("--out", default="/tmp/stage-c-live/validation")
    ap.add_argument("--targets", nargs="*", default=DEFAULT_TARGETS)
    args = ap.parse_args()

    cache = Path(args.cache)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    index_path = cache / "discover.json"
    discover = json.loads(index_path.read_text()) if index_path.exists() else {}

    manifest = {"targets": []}
    for target_id in args.targets:
        if target_id not in discover:
            print(f"  MISS {target_id} not in discover.json", file=sys.stderr)
            continue
        try:
            payload = build_payload(target_id, discover[target_id], cache)
        except RuntimeError as e:
            print(f"  FAIL {target_id}: {e}", file=sys.stderr)
            continue
        path = out / f"{target_id}.json"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        manifest["targets"].append({
            "target_id": target_id,
            "path": str(path),
            "modifiers": payload["modifier_count"],
            "status_counts": payload["aggregate_status_counts"],
        })
        print(f"  OK   {target_id} — {payload['modifier_count']} mods, "
              f"{sum(payload['aggregate_status_counts'].values())} patches")

    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    print(f"\nmanifest → {out / 'manifest.json'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
