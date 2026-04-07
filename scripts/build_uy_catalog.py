"""One-shot helper to build a Uruguay law-number catalog by probing IMPO in parallel.

The serial discovery loop in `IMPODiscovery._discover_leyes` is the bottleneck
of the UY bootstrap (~4 sec/number due to HTTP latency + rate limiting).
This script does the same probing in parallel with N workers and writes the
result to `<data_dir>/catalog.json`. Subsequent calls to
`IMPODiscovery._discover_leyes` will read from this catalog instead of
re-probing.

Usage:
    python scripts/build_uy_catalog.py [--start 9000] [--end 20500] [--workers 16]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from legalize.fetcher.uy.discovery import _year_candidates

BASE_URL = "https://www.impo.com.uy"
USER_AGENT = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize)"

log = logging.getLogger("build_uy_catalog")


def _is_json_response(content: bytes) -> bool:
    head = content.lstrip()[:1]
    return head == b"{"


def probe_one(session: requests.Session, num: int, last_year_hint: int | None) -> tuple[int, str | None]:
    """Try candidate years for a single law number, return (num, "leyes/N-Y") or (num, None)."""
    candidates = _year_candidates(num)
    if last_year_hint is not None and last_year_hint not in candidates:
        candidates = [last_year_hint, *candidates]
    elif last_year_hint is not None:
        candidates = [last_year_hint, *(c for c in candidates if c != last_year_hint)]

    for year in candidates:
        url = f"{BASE_URL}/bases/leyes/{num}-{year}?json=true"
        try:
            r = session.get(url, timeout=20)
        except requests.RequestException as exc:
            log.debug("error %s on %d-%d: %s", num, num, year, exc)
            continue
        if r.status_code != 200:
            continue
        if _is_json_response(r.content):
            return (num, f"leyes/{num}-{year}")
    return (num, None)


def build_catalog(start: int, end: int, workers: int, out_path: Path) -> None:
    log.info("Probing leyes/%d..%d with %d workers", start, end, workers)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sessions = [requests.Session() for _ in range(workers)]
    for s in sessions:
        s.headers["User-Agent"] = USER_AGENT

    found: dict[int, str] = {}
    last_year_hint: int | None = None
    started = time.time()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for i, num in enumerate(range(start, end + 1)):
            session = sessions[i % workers]
            futures[pool.submit(probe_one, session, num, last_year_hint)] = num

        completed = 0
        for fut in as_completed(futures):
            completed += 1
            num, norm_id = fut.result()
            if norm_id:
                found[num] = norm_id
                # Update sticky hint from year of latest find
                last_year_hint = int(norm_id.rsplit("-", 1)[1])
            if completed % 200 == 0:
                elapsed = time.time() - started
                rate = completed / elapsed
                eta = (end - start - completed) / max(rate, 0.1)
                log.info(
                    "  %d/%d probed (%d hits) — %.1f/s, eta %.0fs",
                    completed,
                    end - start + 1,
                    len(found),
                    rate,
                    eta,
                )

    elapsed = time.time() - started
    log.info(
        "Done: %d/%d laws found in %.1fs (%.1f probes/s)",
        len(found),
        end - start + 1,
        elapsed,
        (end - start + 1) / elapsed,
    )

    norm_ids = [found[n] for n in sorted(found)]
    out_path.write_text(json.dumps(norm_ids, indent=2))
    log.info("Catalog written to %s (%d entries)", out_path, len(norm_ids))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=9000)
    parser.add_argument("--end", type=int, default=20500)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--out", default="../countries/data-uy/catalog.json")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    build_catalog(args.start, args.end, args.workers, Path(args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
