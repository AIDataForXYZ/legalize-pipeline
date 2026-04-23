"""Diagnose Stage C live failures: classify each failing patch by a
family of root causes so we can rank programmatic fixes by expected
coverage before writing more code.

Input:  /tmp/stage-c-live/fidelity-log.csv  (from live.py)
        /tmp/stage-c-live/targets/*.xml, /tmp/stage-c-live/modifiers/*.xml
Output: structured classification to stdout + JSON dump at
        /tmp/stage-c-live/diagnosis.json
"""

from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from legalize.fetcher.es.amendments import (  # noqa: E402
    _extract_signals,
    _has_structural_signal,
    extract_new_text_blocks,
    parse_amendments,
)
from legalize.transformer.anchor import parse_anchor_from_hint, resolve_anchor  # noqa: E402
from legalize.transformer.markdown import render_paragraphs  # noqa: E402
from legalize.transformer.xml_parser import parse_diario_xml  # noqa: E402


CACHE = Path("/tmp/stage-c-live")


def render_base(target_id: str) -> str | None:
    p = CACHE / "targets" / f"{target_id}.xml"
    if not p.exists() or p.stat().st_size == 0:
        return None
    blocks = parse_diario_xml(p.read_bytes())
    parts = [
        render_paragraphs(list(b.versions[0].paragraphs))
        for b in blocks if b.versions
    ]
    return "\n\n".join(x for x in parts if x) or None


def modifier_xml(mod_id: str) -> bytes | None:
    p = CACHE / "modifiers" / f"{mod_id}.xml"
    if not p.exists() or p.stat().st_size == 0:
        return None
    return p.read_bytes()


def hint_for(mod_id: str, target_id: str, verb_code: str) -> str:
    mxml = modifier_xml(mod_id)
    if not mxml:
        return ""
    for patch in parse_amendments(mxml):
        if patch.target_id == target_id and patch.verb_code == verb_code:
            return patch.anchor_hint
    return ""


# ──────────────────────────────────────────────────────────
# Classifiers
# ──────────────────────────────────────────────────────────


def classify_anchor_failure(hint: str, base_md: str | None) -> dict:
    """For an anchor_not_found patch: diagnose whether fix is possible."""
    signals = _extract_signals(hint)
    struct_signals = [s for s in signals if s.startswith("struct:")]
    norm_signals = [s for s in signals if s.startswith("norm:")]

    has_struct = bool(struct_signals)
    anchor = parse_anchor_from_hint(hint)

    bucket = "unknown"
    evidence = ""

    if not has_struct:
        # No structural signal at all → inherently ambiguous.
        if norm_signals:
            bucket = "A. norm-only hint (inherently ambiguous)"
            evidence = f"signals={sorted(signals)}"
        else:
            bucket = "B. empty/boilerplate hint"
            evidence = f"hint=…{hint[:80]!r}"
    else:
        # Struct signal present. Does the resolver parse something from it?
        if anchor.is_empty:
            bucket = "C. struct signal but parse_anchor_from_hint returns empty"
            evidence = f"struct_signals={struct_signals}"
        elif base_md is None:
            bucket = "D. target markdown unavailable"
        else:
            # The anchor has fields; the resolver could not find them.
            # Classify by which field the anchor has.
            fields: list[str] = []
            if anchor.articulo:
                fields.append(f"articulo={anchor.articulo}")
            if anchor.norma:
                fields.append(f"norma={anchor.norma}")
            if anchor.disposicion:
                fields.append(f"disposicion={anchor.disposicion}")
            if anchor.anexo:
                fields.append(f"anexo={anchor.anexo}")
            if anchor.apartado:
                fields.append(f"apartado={anchor.apartado}")
            if anchor.letra:
                fields.append(f"letra={anchor.letra}")

            # Check the base markdown for hints the resolver could find.
            # This tells us if the target heading actually exists.
            heading_clues = _find_heading_clues(base_md, anchor)
            if heading_clues:
                bucket = "E. struct signal + heading exists but resolver misses"
                evidence = f"anchor=[{', '.join(fields)}] headings_present={heading_clues}"
            else:
                bucket = "F. struct signal but heading NOT in target markdown"
                evidence = f"anchor=[{', '.join(fields)}]"

    return {"bucket": bucket, "evidence": evidence, "hint": hint}


def _find_heading_clues(base_md: str, anchor) -> list[str]:
    """Look for possible matching headings in the base markdown."""
    clues: list[str] = []
    heading_re = re.compile(r"^#{1,6}\s+(.+)$", re.MULTILINE)
    headings = heading_re.findall(base_md)

    if anchor.articulo:
        for h in headings:
            if re.match(rf"^art[ií]culo\s+{re.escape(anchor.articulo)}\b", h, re.IGNORECASE):
                clues.append(h[:60])
    if anchor.norma:
        for h in headings:
            if re.match(rf"^norma\s+{re.escape(anchor.norma)}\b", h, re.IGNORECASE):
                clues.append(h[:60])
    if anchor.anexo:
        for h in headings:
            if re.match(rf"^anexo\s+{re.escape(anchor.anexo)}\b", h, re.IGNORECASE):
                clues.append(h[:60])
    if anchor.disposicion:
        for h in headings:
            if "disposici" in h.lower() and anchor.disposicion.lower() in h.lower():
                clues.append(h[:60])
    return clues[:3]


def classify_empty_failure(hint: str, mod_id: str, target_id: str) -> dict:
    """For an empty_new_text patch: does the XML have blocks that we missed?"""
    mxml = modifier_xml(mod_id)
    if not mxml:
        return {"bucket": "X. modifier XML missing"}

    from lxml import etree
    try:
        root = etree.fromstring(mxml)
    except Exception:
        return {"bucket": "Y. malformed XML"}

    # Count raw « » pairs in the <texto>.
    texto = root.find("texto")
    if texto is None:
        return {"bucket": "G. no <texto> element"}
    raw = "".join(texto.itertext())

    num_open = raw.count("«") + raw.count("“")
    num_close = raw.count("»") + raw.count("”")

    # Count <blockquote> + <p class="sangrado*|cita*"> siblings.
    blockquotes = len(texto.findall(".//blockquote"))
    sangrado_ps = sum(1 for p in texto.iter("p")
                      if (p.get("class", "") or "").startswith(("sangrado", "cita")))

    # Try our parser on the full XML.
    try:
        blocks = extract_new_text_blocks(mxml)
    except Exception as e:
        return {"bucket": "Z. parser crashed", "err": str(e)}

    evidence = (f"«:{num_open} »:{num_close} <blockquote>:{blockquotes} "
                f"<p sangrado>:{sangrado_ps} parser_blocks:{len(blocks)}")

    if num_open == 0 and num_close == 0 and blockquotes == 0 and sangrado_ps == 0:
        bucket = "H. guidance-only modifier (no quoted content)"
    elif len(blocks) == 0 and (blockquotes > 0 or sangrado_ps > 0):
        bucket = "I. quoted content exists but parser found zero blocks"
    elif len(blocks) > 0:
        # The parser found blocks but they didn't attach to THIS patch.
        # Usually: multi-patch modifier where target_id collected no block.
        bucket = "J. blocks exist but none assigned to this patch"
    else:
        bucket = "K. other — needs manual review"

    return {"bucket": bucket, "evidence": evidence, "hint": hint}


# ──────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────


def main() -> int:
    log = CACHE / "fidelity-log.csv"
    out = CACHE / "diagnosis.json"

    rows = list(csv.DictReader(log.open()))
    print(f"loaded {len(rows)} patches from {log}")

    base_cache: dict[str, str | None] = {}
    diagnosis = {"anchor_not_found": [], "empty_new_text": [], "length_mismatch": []}
    bucket_counts: dict[str, Counter] = defaultdict(Counter)

    for r in rows:
        status = r["apply_status"]
        target_id = r["target_id"]
        mod_id = r["modifier_id"]
        verb = r["verb_code"]

        if target_id not in base_cache:
            base_cache[target_id] = render_base(target_id)
        base_md = base_cache[target_id]

        hint = hint_for(mod_id, target_id, verb)

        if status == "anchor_not_found":
            d = classify_anchor_failure(hint, base_md)
            d.update({"modifier_id": mod_id, "target_id": target_id, "verb": verb})
            diagnosis["anchor_not_found"].append(d)
            bucket_counts["anchor_not_found"][d["bucket"]] += 1
        elif status == "empty_new_text":
            d = classify_empty_failure(hint, mod_id, target_id)
            d.update({"modifier_id": mod_id, "target_id": target_id, "verb": verb})
            diagnosis["empty_new_text"].append(d)
            bucket_counts["empty_new_text"][d["bucket"]] += 1
        elif status == "length_mismatch":
            d = {"hint": hint, "reason": r["apply_reason"]}
            d.update({"modifier_id": mod_id, "target_id": target_id, "verb": verb})
            diagnosis["length_mismatch"].append(d)
            bucket_counts["length_mismatch"]["(length)"] += 1

    print()
    for status, counts in bucket_counts.items():
        print(f"## {status} ({sum(counts.values())} total)")
        for bucket, n in counts.most_common():
            pct = n / sum(counts.values()) * 100
            print(f"   {n:>3}  ({pct:5.1f}%)  {bucket}")
        print()

    # Show 3 examples per bucket for manual review.
    print("### Examples per bucket (up to 3 each) ###\n")
    for status, items in diagnosis.items():
        by_bucket: dict[str, list[dict]] = defaultdict(list)
        for x in items:
            by_bucket[x.get("bucket", "(no bucket)")].append(x)
        for bucket, examples in by_bucket.items():
            print(f"-- {status} :: {bucket} --")
            for ex in examples[:3]:
                mid = ex.get("modifier_id", "?")
                tid = ex.get("target_id", "?")
                print(f"   {mid} → {tid}  {ex.get('evidence', '')}")
                if ex.get("hint"):
                    print(f"     hint: {ex['hint'][:120]!r}")
            print()

    out.write_text(json.dumps(diagnosis, indent=2, ensure_ascii=False))
    print(f"full diagnosis → {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
