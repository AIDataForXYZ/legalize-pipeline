#!/usr/bin/env python3
"""Pre-commit hook: verify all registered countries dispatch correctly."""

import sys

from legalize.countries import (
    get_client_class,
    get_discovery_class,
    get_metadata_parser,
    get_text_parser,
    supported_countries,
)

errors = []

for code in supported_countries():
    c = get_client_class(code)
    if not hasattr(c, "create"):
        errors.append(f"{code}: {c.__name__} missing create()")
    if not hasattr(c, "get_text"):
        errors.append(f"{code}: {c.__name__} missing get_text()")
    if not hasattr(c, "get_metadata"):
        errors.append(f"{code}: {c.__name__} missing get_metadata()")

    t = get_text_parser(code)
    if not hasattr(t, "parse_text"):
        errors.append(f"{code}: {type(t).__name__} missing parse_text()")

    m = get_metadata_parser(code)
    if not hasattr(m, "parse"):
        errors.append(f"{code}: {type(m).__name__} missing parse()")

    d = get_discovery_class(code)
    if not hasattr(d, "discover_all"):
        errors.append(f"{code}: {d.__name__} missing discover_all()")

if errors:
    print("Country dispatch errors:")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)

print(f"All {len(supported_countries())} countries dispatch OK")
