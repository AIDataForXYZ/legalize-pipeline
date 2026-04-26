#!/usr/bin/env python3
"""Scan rendered MX exports for known parser-quality issues.

Run after a `legalize fetch -c mx --all` + `scripts/export_mx.py` pass to
confirm the parser is producing clean output. Each check below is a known
issue category we have hit in the past; the script counts affected files
and prints a few sample lines so the next agent has reproducible inputs.

Exit 0 when clean, exit 1 when any bucket is non-zero. Suitable for CI.

Usage:
    uv run python scripts/audit_mx.py            # check all
    uv run python scripts/audit_mx.py --strict   # also fail on non-fatal
                                                 # warnings (very short
                                                 # files, etc.)
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
EXPORTS_DIR = REPO_ROOT / "exports" / "mx"


# Each check returns a list of (file_path, line_no, sample_excerpt).
def check_word_field_codes(text: str) -> list[tuple[int, str]]:
    """Word/OLE2 field-code or TOC-table garbage that escaped the parser filter."""
    pattern = re.compile(
        r"Fa[öo]f4|\$\$IfF|\$%@[A-Z]|ôôôô|ÖÿÿÖÿÿ|OJ[PQ]J|mH\s+sH"
    )
    hits: list[tuple[int, str]] = []
    for i, line in enumerate(text.splitlines(), start=1):
        if pattern.search(line):
            hits.append((i, line[:120]))
    return hits


def check_short_garbage_lines(text: str) -> list[tuple[int, str]]:
    """Single-letter-token soup that masquerades as Spanish prose.

    Matches lines that start like a Roman numeral or letter, are short
    (<200 chars), and consist mostly of 1-2 character tokens with high
    non-ASCII density that is NOT standard Spanish accents.
    """
    spanish_hi = set("áéíóúüñÁÉÍÓÚÜÑ¿¡«»—–“”‘’°ºª·")
    hits: list[tuple[int, str]] = []
    for i, line in enumerate(text.splitlines(), start=1):
        if len(line) > 200 or len(line) < 6:
            continue
        if not re.match(r"^[IVXLCDM]+\s+[A-Za-zÁ-Úá-ú¦]", line):
            continue
        # Token analysis after the leading numeral.
        rest = line.split(None, 1)[1] if " " in line else ""
        if not rest:
            continue
        tokens = re.split(r"[\s,;]+", rest)
        short_tokens = sum(1 for t in tokens if len(t) <= 2)
        non_spanish_hi = sum(1 for c in rest if not c.isascii() and c not in spanish_hi)
        if short_tokens >= 3 and non_spanish_hi >= 3:
            hits.append((i, line[:120]))
    return hits


def check_pdf_source(text: str) -> list[tuple[int, str]]:
    """Frontmatter `source:` should point to .doc, never .pdf."""
    hits: list[tuple[int, str]] = []
    for i, line in enumerate(text.splitlines()[:30], start=1):
        if re.match(r'^source:\s*"[^"]*\.pdf"', line):
            hits.append((i, line))
    return hits


def check_issuing_decree_at_start(text: str) -> list[tuple[int, str]]:
    """Articles labeled PRIMERO/SEGUNDO/etc. that come BEFORE any numeric Article 1.

    These usually indicate the issuing decree is being rendered as if it were
    main-law articles. Returns empty when word-ordinal articles exist after a
    numeric one (legitimate transitorios) or when the law uses word ordinals
    throughout (no numeric ones).
    """
    word_ord_re = re.compile(r"^###### Artículo (PRIMERO|SEGUNDO|TERCERO|CUARTO|QUINTO|SEXTO|SÉPTIMO|OCTAVO|NOVENO|DÉCIMO)\.\-")
    num_art_re = re.compile(r"^###### Artículo \d+[oa]?\.\-?")
    first_word_ord_line = None
    first_num_line = None
    for i, line in enumerate(text.splitlines(), start=1):
        if first_num_line is None and num_art_re.match(line):
            first_num_line = i
        if first_word_ord_line is None and word_ord_re.match(line):
            first_word_ord_line = i
        if first_num_line is not None and first_word_ord_line is not None:
            break
    if (
        first_word_ord_line is not None
        and first_num_line is not None
        and first_word_ord_line < first_num_line
    ):
        return [(first_word_ord_line, "issuing-decree word-ordinal article precedes Article 1")]
    return []


def check_repeated_pdf_artifacts(text: str) -> list[tuple[int, str]]:
    """Page-footer leakage from PDF source extraction (we should be off PDF now)."""
    hits: list[tuple[int, str]] = []
    for i, line in enumerate(text.splitlines(), start=1):
        if re.search(r"P[áa]gina\s+\d+\s+de\s+\d+", line):
            hits.append((i, line[:120]))
    return hits


CHECKS = {
    "field_codes": (check_word_field_codes, True),  # (fn, fatal)
    "short_garbage_lines": (check_short_garbage_lines, True),
    "pdf_source_url": (check_pdf_source, True),
    "issuing_decree_at_start": (check_issuing_decree_at_start, True),
    "pdf_page_footer": (check_repeated_pdf_artifacts, True),
}


def audit(strict: bool = False) -> int:
    """Run all checks across exports/mx/*.md. Return non-zero if any fatal hit."""
    files = sorted(EXPORTS_DIR.glob("*.md"))
    if not files:
        print(f"No exports found in {EXPORTS_DIR}", file=sys.stderr)
        return 2

    print(f"Auditing {len(files)} files in {EXPORTS_DIR}\n")

    bucket_totals: dict[str, list[tuple[Path, int, str]]] = {k: [] for k in CHECKS}

    for f in files:
        text = f.read_text()
        for name, (fn, _fatal) in CHECKS.items():
            for line_no, excerpt in fn(text):
                bucket_totals[name].append((f, line_no, excerpt))

    fatal_hits = 0
    print(f"{'CHECK':<28} {'FILES':>6} {'HITS':>6}  STATUS")
    print("-" * 72)
    for name, (_fn, fatal) in CHECKS.items():
        hits = bucket_totals[name]
        files_affected = len({h[0] for h in hits})
        status = "OK" if not hits else ("FAIL" if fatal else "WARN")
        if hits and fatal:
            fatal_hits += 1
        print(f"{name:<28} {files_affected:>6} {len(hits):>6}  {status}")

    print()
    for name, hits in bucket_totals.items():
        if not hits:
            continue
        print(f"=== {name} — {len(hits)} hits across {len({h[0] for h in hits})} files ===")
        # Sample up to 5 hits per bucket.
        for f, ln, ex in hits[:5]:
            print(f"  {f.relative_to(REPO_ROOT)}:{ln}  {ex}")
        if len(hits) > 5:
            print(f"  ... and {len(hits) - 5} more")
        print()

    if fatal_hits:
        print(f"AUDIT FAILED — {fatal_hits} fatal categor{'y' if fatal_hits == 1 else 'ies'} have hits")
        return 1
    print("AUDIT PASSED — exports/mx/ is clean")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="Reserved for future warning-level checks.")
    args = parser.parse_args()
    raise SystemExit(audit(strict=args.strict))
