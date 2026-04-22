# Stage C — status as of 2026-04-23

Branch: `feat/stage-c-amendments` in worktree `engine-stage-c/`.

## Code delivered

| Module | Path | Responsibility |
|---|---|---|
| Amendment parser | `src/legalize/fetcher/es/amendments.py` | `<anteriores>` + body `«…»` extraction, two-axis confidence, greedy matcher |
| LLM client | `src/legalize/llm/amendment_parser.py` | Groq OpenAI-compatible HTTP, structured JSON, disk cache, verifier |
| Case queue | `src/legalize/llm/queue.py` | Persistent JSONL pair + immutable batches for Claude resolver |
| Resolver | `src/legalize/llm/resolver.py` | Prompt construction + response parsing for the Claude-session resolver |
| Anchor resolver | `src/legalize/transformer/anchor.py` | Hint → `Anchor` dataclass → `Position` in base Markdown |
| Patcher | `src/legalize/transformer/patcher.py` | `apply_patch` with 3 safety gates + dry-run |
| Fidelity score | `scripts/es_fidelity_c/score.py` | `PatchRecord` + `FidelityReport` aggregation |
| Fidelity runner | `scripts/es_fidelity_c/run.py` | End-to-end driver over modifier fixtures |
| Fidelity report | `scripts/es_fidelity_c/report.py` | Markdown summary with coverage, per-verb, histograms |

## Test coverage

| Suite | Passing | Notes |
|---|---:|---|
| `test_stage_c_amendments.py` | 27 | Parser + extractor on 5 real fixtures |
| `test_stage_c_llm.py` | 13 | Groq client (HTTP mocked); 1 smoke script live-tested |
| `test_stage_c_queue.py` | 29 | Queue + batch rotation + resolver plumbing |
| `test_stage_c_anchor.py` | 22 | Hint parser + markdown resolver, success + failure modes |
| `test_stage_c_patcher.py` | 12 | 3 gates, 3 operations, boundary preservation |
| `test_stage_c_fidelity.py` | 10 | Scorer roll-up + markdown renderer |
| `test_stage_c_idempotency.py` | 3 xfail | Contract for W5 committer |
| **Full engine suite** | **1748 pass** | + 27 skip, 3 xfail |

## Baseline fidelity metrics (2026-04-23, 15 fixture modifiers)

Run: `python scripts/es_fidelity_c/run.py --targets tests/fixtures/stage_c/target_index.json`

| Metric | Value |
|---|---|
| Sample size | 15 modifier XMLs |
| MVP-scoped patches produced | 14 |
| Out-of-scope entries filtered | 35 (correcciones, judicial, references) |
| **regex-ready coverage** | **28.6 %** |
| LLM-bound (short/medium) | 0 % |
| Claude-queue (hard) | 71.4 % |
| Apply dry-runs attempted | 2 (against IRPF base) |
| Apply status | both `anchor_not_found` — inserts of new articles, expected |

### Per-verb breakdown

| Verb | Total | Regex-filled | Fill rate |
|---|---:|---:|---:|
| DEROGA (210) | 1 | 1 | 100 % |
| SUPRIME (235) | 1 | 1 | 100 % |
| MODIFICA (270) | 10 | 6 | 60 % |
| ANADE (407) | 2 | 2 | 100 % |

### Reading the baseline

- The 71 % `hard` share is an artefact of the fixture mix: several fixtures
  are full laws (modif-ley-*) with 50-150 KB modifier bodies. Every patch
  from an omnibus modifier trips the `len > 3000` → hard threshold. On
  real Circulares BdE/CNMV (typical body 5-20 KB with many patches per
  body), the share is expected to drop to 20-40 %.
- DEROGA and SUPRIME are trivially 100 % — they carry no text.
- MODIFICA at 60 % regex-filled matches the plan projection
  (~70 % ± sample noise on n=14).
- ANADE 100 % is encouraging for the extractor, but the two patches
  were the easy case (single-block omnibus). The harder ANADE pattern
  ("añade un artículo 61 bis tras el 61") still drops to
  `anchor_not_found` on apply because the insert parent isn't explicit
  in the hint.

## What's next

### W5 — Committer integration (pending)

- `src/legalize/committer/stage_c.py` — orchestrate bootstrap + reformas
  per target, write git commits with `Source-Id`, `Source-Date`, `Norm-Id`
  trailers.
- Idempotency enforcement (the three xfail contracts):
  - A. Bit-identical reruns
  - B. Detect-and-skip via `git log --grep Source-Id`
  - C. Commit-pointer stickiness
- Wire into `daily-update.yml` for Groq-only daily runs.
- Wire Claude queue emission + sub-agent dispatch for bootstrap runs.

### Gate for declaring MVP done

Run the fidelity loop on ~500 real Circulares BdE (fetch via
`client.get_disposition_xml`), measure:

- **≥ 85 %** of MODIFICA + AÑADE + SUPRIME patches applied with hash-check OK
  (target revised from 90 % per independent review arithmetic).
- 0 silent corruptions (all failures visible in logs + commit-pointer fallback).
- ≤ 2 s per patch mean including LLM path.

Go/no-go to Phase 2 decided by trajectory between iterations, not absolute.

## Open work
- Smarter case classifier that looks at LOCAL excerpt size per patch,
  not the whole modifier body — will reduce false-positive `hard` tier
  on omnibus modifiers.
- Insert patcher needs explicit parent anchor resolution (W5 concern
  when we see more real ANADE cases).
- `discover.py` for Circulares BdE/CNMV sampling (currently uses hand-
  copied fixtures; W4's "real" path needs BOE sumario iteration).
