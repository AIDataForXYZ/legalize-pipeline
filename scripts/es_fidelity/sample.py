"""Stratified sample of BOE IDs for the fidelity loop.

Usage:
    python -m scripts.es_fidelity.sample --n 20 --seed 42 > /tmp/es-audit/sample.txt
"""

from __future__ import annotations

import argparse
import random
import sys
from collections import defaultdict
from pathlib import Path

# Bootstrap path for when invoked via `python scripts/es_fidelity/sample.py`
_SRC = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(_SRC))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.es_fidelity.common import CatalogEntry, load_catalog

# Rangos we care about in iteration 1 — all present in the consolidated catalog.
# Non-consolidated rangos (most Circulares, many Órdenes) are covered in Stage B
# of the refactor (see RESEARCH-ES-v2.md §3).
RANGOS_SCOPE = {
    "Constitución",
    "Ley Orgánica",
    "Ley",
    "Real Decreto Legislativo",
    "Real Decreto-ley",
    "Real Decreto",
    "Orden",
    "Resolución",
    "Circular",
    "Instrucción",
    "Acuerdo Internacional",
    "Ley Foral",
    "Decreto Legislativo",
    "Decreto-ley",
    "Decreto",
}


def stratify(entries: list[CatalogEntry]) -> dict[tuple[str, str, str], list[CatalogEntry]]:
    """Group by (rango, ambito, decade)."""
    groups: dict[tuple[str, str, str], list[CatalogEntry]] = defaultdict(list)
    for e in entries:
        if e.rango not in RANGOS_SCOPE:
            continue
        key = (e.rango, e.ambito, e.decade)
        groups[key].append(e)
    return groups


def sample(n: int, seed: int = 42, exclude: set[str] | None = None) -> list[CatalogEntry]:
    """Round-robin pick across non-empty strata until n IDs are chosen."""
    exclude = exclude or set()
    entries = load_catalog()
    entries = [e for e in entries if e.boe_id and e.boe_id not in exclude]
    groups = stratify(entries)
    rng = random.Random(seed)

    keys = list(groups.keys())
    rng.shuffle(keys)
    # Pin a few "must-always-include" landmarks so regressions on the
    # flagship laws always surface.
    must_have = [
        "BOE-A-1978-31229",  # Constitución
        "BOE-A-1889-4763",  # Código Civil
        "BOE-A-1995-25444",  # Código Penal
        "BOE-A-2003-23186",  # LGT
        "BOE-A-2006-20764",  # TR IRPF (tablas)
        "BOE-A-2015-10565",  # LPAC
        "BOE-A-2015-11704",  # Código de Comercio 1885 (clasificado)
    ]
    chosen: list[CatalogEntry] = []
    by_id = {e.boe_id: e for e in entries}
    for pinned in must_have:
        if pinned in by_id and pinned not in exclude:
            chosen.append(by_id[pinned])

    i = 0
    while len(chosen) < n and i < len(keys) * 10:
        key = keys[i % len(keys)]
        bucket = groups[key]
        if bucket:
            pick = rng.choice(bucket)
            bucket.remove(pick)
            if pick.boe_id not in {c.boe_id for c in chosen}:
                chosen.append(pick)
        i += 1

    return chosen[:n]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=str, default="/tmp/es-audit/sample.txt")
    args = ap.parse_args()

    picks = sample(args.n, args.seed)
    lines = [f"{e.boe_id}\t{e.rango}\t{e.ambito}\t{e.decade}\t{e.title}" for e in picks]
    Path(args.out).write_text("\n".join(lines))
    for ln in lines:
        print(ln)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
