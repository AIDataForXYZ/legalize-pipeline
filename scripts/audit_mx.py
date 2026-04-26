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

try:
    import yaml as _yaml_mod
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

REPO_ROOT = Path(__file__).resolve().parents[1]
EXPORTS_DIR = REPO_ROOT / "exports" / "mx"


# Each check returns a list of (file_path, line_no, sample_excerpt).
def check_word_field_codes(text: str) -> list[tuple[int, str]]:
    """Word/OLE2 field-code or TOC-table garbage that escaped the parser filter."""
    pattern = re.compile(
        r"Fa[öo]f4|\$\$If[A-Z]|\$%@[A-Z]|ôôôô|ÖÿÿÖÿÿ|OJ[PQ]J|mH\s+sH"
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


def check_tail_binary_blob(text: str) -> list[tuple[int, str]]:
    """Word stylesheet / drawing-object bytes that should have been truncated.

    Inspects the last 200 lines of the exported Markdown for paragraphs that
    match the known patterns of Word character-style property strings (CJ^J,
    CJPJ]aJ, B*CJ, CJaJ) or other binary-coordinate blob patterns.  Any hit
    here means the tail-blob truncation in the parser missed something.

    Only the last 200 lines are checked: mid-document garbage that escaped
    the per-paragraph filter is reported by ``check_word_field_codes``; this
    check is specifically for the TRAILING blob that the truncation step is
    responsible for removing.
    """
    _TAIL_BLOB_RE = re.compile(
        r"\d?CJ\^J"          # 5CJ^J / CJ^J
        r"|CJ\s*PJ\s*\]?\s*aJ"  # CJPJ]aJ / CJPJ^JaJ
        r"|B\*CJ"             # B*CJ boolean attribute
        r"|CJaJ"              # bare CJaJ
        r"|nH\s*tH"           # nH tH flag
    )
    lines = text.splitlines()
    last_200 = lines[-200:] if len(lines) > 200 else lines
    offset = max(0, len(lines) - 200)
    hits: list[tuple[int, str]] = []
    for j, line in enumerate(last_200, start=offset + 1):
        if _TAIL_BLOB_RE.search(line):
            hits.append((j, line[:120]))
    return hits


def check_repeated_short_tail(text: str) -> list[tuple[int, str]]:
    """Trailing run of a repeated short line — catches Jáh-style Word handle garbage.

    Inspects the last 30 lines of the file.  If the most-frequently-repeated
    distinct line in that window is ≤ 30 chars AND appears ≥ 5 times, the file
    fails.  This mirrors the criterion that triggered the Reg_Senado.doc bug:
    10 consecutive 'Jáh' lines at the file end.
    """
    lines = text.splitlines()
    tail = [ln.strip() for ln in lines[-30:]] if len(lines) >= 30 else [ln.strip() for ln in lines]
    # Count occurrences of each distinct non-empty line in the tail window.
    from collections import Counter
    counts: Counter[str] = Counter(ln for ln in tail if ln)
    if not counts:
        return []
    most_common_line, most_common_count = counts.most_common(1)[0]
    if most_common_count >= 5 and len(most_common_line) <= 30:
        # Report the last occurrence line number.
        all_lines = text.splitlines()
        last_line_no = 0
        for i, ln in enumerate(all_lines, start=1):
            if ln.strip() == most_common_line:
                last_line_no = i
        return [(last_line_no, f"repeated tail: {most_common_line!r} x{most_common_count}")]
    return []


def check_frontmatter_completeness(text: str) -> list[tuple[int, str]]:
    """Validate YAML frontmatter for required keys and sanity checks.

    Required keys: title, identifier, country, rank, jurisdiction, gov_organ,
    entidad_federativa, publication_date, last_updated, status, source,
    department, pdf_url, doc_url, source_name, abbrev.

    Conditionally required: if last_reform_dof is present, gazette_pdf_page
    must also be present (and vice versa).

    Sanity checks: dates in ISO YYYY-MM-DD, source ends in .doc (not .pdf),
    country == 'mx'.
    """
    REQUIRED_KEYS = [
        "title", "identifier", "country", "rank", "jurisdiction", "gov_organ",
        "entidad_federativa", "publication_date", "last_updated", "status",
        "source", "department", "pdf_url", "doc_url", "source_name", "abbrev",
    ]
    DATE_KEYS = {"publication_date", "last_updated", "last_reform_dof"}
    _ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    hits: list[tuple[int, str]] = []

    # Extract frontmatter block.
    fm_match = re.match(r"^---\n(.*?)\n---", text, re.DOTALL)
    if not fm_match:
        hits.append((1, "missing YAML frontmatter block"))
        return hits

    fm_text = fm_match.group(1)
    fm_start_line = 1  # frontmatter starts at line 1 (the opening ---)

    # Parse YAML.
    if _HAS_YAML:
        try:
            fm = _yaml_mod.safe_load(fm_text) or {}
        except Exception as exc:
            hits.append((1, f"YAML parse error: {exc}"))
            return hits
    else:
        # Minimal fallback: extract key: "value" pairs with regex.
        fm: dict = {}
        for m in re.finditer(r'^(\w+):\s*"?([^"\n]*)"?\s*$', fm_text, re.MULTILINE):
            fm[m.group(1)] = m.group(2).strip()

    # Required key presence.
    for key in REQUIRED_KEYS:
        if key not in fm or fm[key] in (None, "", []):
            hits.append((fm_start_line, f"missing required frontmatter key: {key!r}"))

    # Conditional pair: last_reform_dof ↔ gazette_pdf_page.
    has_reform = bool(fm.get("last_reform_dof"))
    has_gazette = bool(fm.get("gazette_pdf_page"))
    if has_reform and not has_gazette:
        hits.append((fm_start_line, "last_reform_dof present but gazette_pdf_page missing"))
    if has_gazette and not has_reform:
        hits.append((fm_start_line, "gazette_pdf_page present but last_reform_dof missing"))

    # Sanity: ISO date format.
    for dk in DATE_KEYS:
        val = fm.get(dk)
        if val and not _ISO_DATE_RE.match(str(val)):
            hits.append((fm_start_line, f"{dk} is not ISO YYYY-MM-DD: {val!r}"))

    # Sanity: source ends in .doc, not .pdf.
    source = fm.get("source", "")
    if source and str(source).endswith(".pdf"):
        hits.append((fm_start_line, f"source points to .pdf, expected .doc: {source!r}"))

    # Sanity: country == 'mx'.
    country = fm.get("country", "")
    if country and str(country) != "mx":
        hits.append((fm_start_line, f"country is {country!r}, expected 'mx'"))

    return hits


CHECKS = {
    "field_codes": (check_word_field_codes, True),  # (fn, fatal)
    "short_garbage_lines": (check_short_garbage_lines, True),
    "pdf_source_url": (check_pdf_source, True),
    "issuing_decree_at_start": (check_issuing_decree_at_start, True),
    "pdf_page_footer": (check_repeated_pdf_artifacts, True),
    "tail_binary_blob": (check_tail_binary_blob, True),
    "repeated_short_tail": (check_repeated_short_tail, True),
    "frontmatter_completeness": (check_frontmatter_completeness, True),
}

# ── JSON-backed reform-count sanity check ─────────────────────────────

JSON_DIR = REPO_ROOT / "countries" / "data-mx" / "json"
_DOF_STAMP_RE = re.compile(r"\bDOF\s+\d{2}-\d{2}-\d{4}")


def check_reform_count_sanity_for_file(md_path: Path) -> list[tuple[int, str]]:
    """Compare DOF stamps in rendered Markdown with reform count in JSON.

    Rules:
    - If the MD body contains at least one ``DOF DD-MM-YYYY`` stamp AND the
      corresponding JSON has reforms == 1, the parser failed to extract
      reforms → FAIL.
    - If the number of unique DOF dates visible in the MD body is more than
      3× the number of JSON reforms, likely an extraction error → FAIL.
      (A factor of 3 is used because multi-date stamps like
      ``DOF 04-12-2006, 10-06-2011`` count as two dates in the text but only
      two reform entries, and the mismo date can appear multiple times in
      stamps for different paragraphs.)

    Skips gracefully when the corresponding JSON does not exist.
    """
    import json as _json

    stem = md_path.stem
    json_path = JSON_DIR / f"{stem}.json"
    if not json_path.exists():
        return []

    try:
        data = _json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []

    n_reforms = len(data.get("reforms", []))

    # Count unique DOF dates appearing in the MD body.
    text = md_path.read_text(encoding="utf-8")
    # Skip the frontmatter block.
    body = re.sub(r"^---.*?---\s*", "", text, count=1, flags=re.DOTALL)
    dof_dates_in_body = set(_DOF_STAMP_RE.findall(body))
    n_stamps = len(dof_dates_in_body)

    hits: list[tuple[int, str]] = []

    if n_stamps > 0 and n_reforms == 1:
        hits.append((
            0,
            f"reform_count_sanity: {stem} has {n_stamps} DOF stamp date(s) "
            f"in body but JSON reforms=1 — parser did not extract reforms",
        ))
    elif n_stamps > 0 and n_reforms > 1 and n_stamps > n_reforms * 3:
        hits.append((
            0,
            f"reform_count_sanity: {stem} has {n_stamps} unique DOF dates "
            f"in body but only {n_reforms} reforms in JSON "
            f"(ratio {n_stamps / n_reforms:.1f}×) — possible extraction gap",
        ))

    return hits


def audit(strict: bool = False) -> int:
    """Run all checks across exports/mx/*.md. Return non-zero if any fatal hit."""
    files = sorted(EXPORTS_DIR.glob("*.md"))
    if not files:
        print(f"No exports found in {EXPORTS_DIR}", file=sys.stderr)
        return 2

    print(f"Auditing {len(files)} files in {EXPORTS_DIR}\n")

    bucket_totals: dict[str, list[tuple[Path, int, str]]] = {k: [] for k in CHECKS}
    # reform_count_sanity is file-path-aware, so handled separately.
    reform_hits: list[tuple[Path, int, str]] = []

    for f in files:
        text = f.read_text()
        for name, (fn, _fatal) in CHECKS.items():
            for line_no, excerpt in fn(text):
                bucket_totals[name].append((f, line_no, excerpt))
        # Run the JSON-backed reform-count check (needs the file path).
        for line_no, excerpt in check_reform_count_sanity_for_file(f):
            reform_hits.append((f, line_no, excerpt))

    # Build the display table: standard checks + reform_count_sanity.
    all_check_names = list(CHECKS.keys()) + ["reform_count_sanity"]
    all_bucket_totals: dict[str, list[tuple[Path, int, str]]] = {
        **bucket_totals,
        "reform_count_sanity": reform_hits,
    }
    all_fatal: dict[str, bool] = {name: fatal for name, (_fn, fatal) in CHECKS.items()}
    all_fatal["reform_count_sanity"] = True  # reform extraction failures are fatal

    fatal_hits = 0
    print(f"{'CHECK':<28} {'FILES':>6} {'HITS':>6}  STATUS")
    print("-" * 72)
    for name in all_check_names:
        hits = all_bucket_totals[name]
        fatal = all_fatal[name]
        files_affected = len({h[0] for h in hits})
        status = "OK" if not hits else ("FAIL" if fatal else "WARN")
        if hits and fatal:
            fatal_hits += 1
        print(f"{name:<28} {files_affected:>6} {len(hits):>6}  {status}")

    print()
    for name in all_check_names:
        hits = all_bucket_totals[name]
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
