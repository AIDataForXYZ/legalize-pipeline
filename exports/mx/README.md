# exports/mx/

The rendered Markdown for Mexico's federal laws lives in the dedicated
country repository:

**https://github.com/AIDataForXYZ/legalize-mx**

That repo contains 316 laws as `mx/{ID}.md`, with one git commit per
DOF reform (3,282 commits total) — the canonical legislative record.

This directory only exists in the engine repo as a transient scratch
location: when iterating on the parser, `scripts/export_mx.py` writes
re-renders here for in-PR review, but the files are not committed.
After bootstrap to the country repo, this directory is intentionally
empty (this README aside).

To re-render locally:

```sh
uv run python scripts/export_mx.py
```
