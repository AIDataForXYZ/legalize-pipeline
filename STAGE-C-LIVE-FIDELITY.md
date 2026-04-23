# Stage C — live fidelity audit + programmatic fixes (2026-04-23)

First run against real BOE data (non-consolidated
`/diario_boe/xml.php`). Discovery window 2020-01-01 → 2025-12-31,
30 target Circulares BdE/CNMV, 100 modifiers, 82 MVP-scoped patches.

## Headline metrics — evolution

| Metric | Baseline | After programmatic fixes | Δ |
|---|---:|---:|---:|
| `dry_run_ok` applied | 4 / 82 (4.9 %) | **16 / 82 (19.5 %)** | **×4** |
| `anchor_not_found` | 41 | 27 | ↓ 14 |
| `empty_new_text` | 35 | 35 | ≈ |
| `length_mismatch` | 2 | 4 | ↑ 2 |
| `regex_ready` (honest) | 43.9 % fake | 15.9 % real | calibration fix |
| `hard` (Claude queue) | 56.1 % | 82.9 % | → LLM tier |

The apparent drop in `regex_ready` is the biggest win: the 43.9 %
baseline was falsely confident (anchor=1.0 on hints with no
structural signal, failing 50 % of the time downstream). 15.9 % is
the honest floor — every one of those patches actually applies.

## 9 programmatic fixes shipped

All on worktree `feat/stage-c-amendments`, not pushed. 11 regression
tests added. Suite: **1765 pass, 27 skip, 0 fail, 0 xfail**.

| # | File:area | What | Evidence source |
|---|---|---|---|
| 1 | `amendments._attach_text_blocks` | Cap `anchor_confidence` to 0.3 when hint has no structural signal | Agent B audit |
| 2 | `amendments._attach_text_blocks` | Fallback: when Jaccard scores 0 for all pairs but blocks exist, assign to first patch by ordering_key | Agent A audit |
| 3 | `amendments._strip_run_outer_markers` | Strip outer `«»` only on first/last paragraph of a run (preserve nested typographic quotes) | Agent A audit |
| 4 | `amendments._KNOWN_OUT_OF_SCOPE_VERBS` | Add verb 330 CITA | Live warnings |
| 5 | `amendments._STRUCT_RE` + `anchor._RE_NORMA` | Accept plural `normas?` | Diagnosis bucket C |
| 6 | `anchor._ORDINAL_WORDS_CARDINAL` + matchers | Feminine / compound ordinals (primera..centésima) ↔ cardinal numbers | Diagnosis bucket F |
| 7 | `amendments._attach_text_blocks` | Fallback guard: only fire when every text-patch shares target_id (no multi-target leakage) | Fix 2 safety |
| 8 | `amendments._INTRO_PATTERNS` | "de la siguiente manera / en la forma siguiente / con el tenor literal siguiente" | Diagnosis bucket I |
| 9 | `anchor._strip_css_marker` | Strip `[precepto]`, `[capítulo_num]`, … prefixes the renderer prepends to headings | Diagnosis bucket F residual |

## Remaining failures — 66 / 82, all beyond regex reach

Diagnosis (`scripts/es_fidelity_c/diagnose.py`):

| Bucket | Count | Programmatic viability |
|---|---:|---|
| **A. Norm-only hint** — "determinados preceptos de la Circular X" | 23 | ❌ Source literally does not say where to edit. Commit-pointer is the correct behaviour. |
| **J. Multi-patch same-target, blocks assigned to first** | 30 | ⚠️ Needs semantic understanding — the secondary patches have real amendment content in the modifier body but cannot be separated without reading the prose. **LLM territory.** |
| **F. Struct signal but heading absent from target** | 8 | 🤔 Case-by-case; marginal value. |
| **I. Parser finds 0 blocks despite « » in body** | 4 | 🤔 Rare structural variants (nueva circular con disposiciones finales modificativas). |
| **length_mismatch** | 4 | 🤔 Side-effect of Fix-2 fallback (large concatenation vs small old region). |
| **C / K residual** | 2 | 🤔 |

## Estimated ceiling

| Path | Expected applied | Effort |
|---|---:|---|
| Current state (no LLM) | 16 / 82 (19.5 %) | ✅ done |
| Squeeze F/I/length programmatically | +6–10 → 22–26 / 82 (~30 %) | 1–2 more days of targeted regex |
| **Integrate LLM queue with `StageCDriver`** | **+20–25 from bucket J → 36–45 / 82 (~50 %)** | **~1 day — the modules exist (`llm/queue.py`, `llm/resolver.py`, `llm/amendment_parser.py`), only the driver wiring is missing** |
| Commit-pointer everything else (bucket A) | applied% caps at ~50 % but 100 % of reforms are represented as either applied commit or pointer with BOE URL | no extra work |

User decision 2026-04-23: **stop regex extraction, proceed with LLM
integration**. Rationale: the next 6–10 programmatic patches would
each be case-specific and brittle; bucket J's 30 patches require
semantic understanding that regex cannot provide; the LLM modules
are already built and live-tested against Groq (`scripts/groq_smoke.py`).

## Artefacts

- Runner: `scripts/es_fidelity_c/live.py` (discovery + fetch + dry-run)
- Diagnosis: `scripts/es_fidelity_c/diagnose.py`
- Validation payload builder: `scripts/es_fidelity_c/prepare_validation.py`
- Live cache: `/tmp/stage-c-live/` (30 targets, 100 modifiers, CSVs, diagnosis.json)
- 5 per-target audit payloads: `/tmp/stage-c-live/validation/*.json`
- 3-subagent audit results (Agents A/B/C): embedded above

## What next (new session)

1. Integrate `PendingCaseQueue` + `AmendmentLLM` into `StageCDriver._apply_modifier`:
   - Call `classify_case(...)` per patch
   - `regex_only` → `apply_patch` directly (current behaviour)
   - `short` / `medium` → `AmendmentLLM.extract_new_text(...)` via Groq, then `apply_patch`
   - `hard` → queue into `PendingCaseQueue`, flush every 30 to a Claude sub-agent
2. Re-run `scripts/es_fidelity_c/live.py` with LLM integration enabled; measure applied %.
3. If applied % ≥ 50 %: propose PR + push in 1500-commit batches per `feedback_push_batches.md`.

## LLM wiring results (2026-04-23, session 2)

Wiring landed in `src/legalize/llm/dispatcher.py` + `StageCDriver` + `scripts/es_fidelity_c/live.py --with-llm [--daily-mode]`. 13 new integration tests (`tests/test_stage_c_dispatcher.py`) cover all four tiers. Suite: **1778 pass, 27 skip, 0 fail**.

Live rerun on the cached 30 Circulares (`/tmp/stage-c-live/`), `--with-llm --daily-mode`:

| metric | baseline (no LLM) | with LLM (daily) | Δ |
|---|---:|---:|---:|
| `dry_run_ok` applied | 16 / 82 (19.5 %) | 15 / 82 (18.3 %) | ≈ |
| regex_only tier | — | 13 / 82 | — |
| medium tier (Groq) | — | 1 / 82 | — |
| hard tier (Groq daily) | — | 68 / 82 | — |
| `dispatch_status = applied` | n/a | 9 | — |
| `dispatch_status = gate_failed` | n/a | 17 | — |
| `dispatch_status = llm_failed` | n/a | 56 | — |

**Cost**: 58 Groq gpt-oss-120b calls, total ≈ $0.05 USD (cached for future reruns).

**Root cause the LLM didn't help**:

1. 56/68 Groq calls return `confidence < 0.8` (rung_threshold). Groq is correctly honest: the source `<anterior><texto>` in bucket A (23 patches) literally does not contain enough information to identify an edit point, and in bucket J (30 patches) the multi-patch modifier body can't be split without strong domain cues.
2. `AmendmentLLM.parse_difficult_case` returns a structured `anchor` (article/section/letter/ordinal/free_text) but `AmendmentPatch.anchor_hint` is kept as the original input string. So even when the LLM CAN identify the right anchor, the downstream `parse_anchor_from_hint` still sees the weak hint.
3. Out of the 12 LLM-produced patches that reached the patcher, 9 applied (75 % yield once we trust the LLM), 17 hit `gate_failed` (anchor_not_found, length_mismatch) because of #2.

**Next moves to move the needle beyond 25 %**:

- A. Translate `LLMParseResponse.anchor` → a structured anchor object that the patcher consumes directly, bypassing `parse_anchor_from_hint`. Bucket J expected to respond.
- B. Lower the `rung_threshold` to 0.6 for hard tier (trust the patcher's gates more than Groq's self-reported confidence). Bucket F residual may respond.
- C. Actually run the Claude sub-agent resolver path (bootstrap mode, not daily) against one `batch_000.jsonl`. The current queue has 68 cases ready; flushing one batch gives concrete resolution data.
- D. Bucket A (23 patches, norm-only hint) remains commit-pointer by design — no programmatic or LLM fix is possible without hallucinating.

Artefacts from this session:

- `src/legalize/llm/dispatcher.py` — tier router
- `tests/test_stage_c_dispatcher.py` — 13 integration tests
- `/tmp/stage-c-live/fidelity-log.withllm.csv` — per-patch record
- `/tmp/stage-c-live/fidelity-raw.json` — dispatch_tier + dispatch_status per patch
- `/tmp/stage-c-live/llm-cache/*.json` — Groq responses (58 entries, cached)

## Structural parser + patch splitter (2026-04-23, session 2.5)

Reassessment after the LLM wiring: the LLM wasn't moving the needle because
the input we were feeding it was poor. The modifier body already carries
two strong structural signals we were ignoring:

1. **Per-target sections** — multi-target omnibus modifiers always wrap
   per-target amendments in `<p class="articulo">Norma N. Modificación
   de la Circular X/YYYY, ...</p>`. That headline binds every subsequent
   block to a single external target.
2. **Per-block intros** — each quoted run is preceded by a sentence like
   "a) En la norma 1, se modifica el apartado 4, ..." which carries the
   sub-structural tokens (norma/apartado/letra/ordinal) the patcher
   needs. We were collapsing all blocks into one patch and keeping only
   the coarse `<anterior><texto>` hint (often "determinados preceptos
   de la Circular X" — no struct signal).

Two programmatic additions, no LLM:

- NEW `src/legalize/fetcher/es/modifier_structure.py` — sections
  extractor + section→patch substring matcher
- NEW `_split_multi_block_patches` in `amendments.py` — when a patch
  carries ≥2 blocks, emit one sub-patch per block using `block.intro`
  as the anchor_hint
- NEW `tests/test_stage_c_modifier_structure.py` — 9 tests

**Metrics on the same 30-target live cache:**

| measure | baseline | +sectioning | +splitting |
|---|---:|---:|---:|
| anteriores with ≥1 applied | 16 / 82 (19.5 %) | 24 / 82 (29.3 %) | **28 / 82 (34.1 %)** |
| granular edits applied | 16 | 24 | **162** |
| API cost | $0 | $0 | $0 |

The headline jump isn't the 19.5 → 34.1 % at the anterior level — it's
the 6× growth in granular edits. Before, a single "applied" anterior
was one coarse-anchor replacement concatenating the text of ~19 blocks.
Now each of those 19 sub-edits lands at its specific apartado or stays
pending (LLM-bound) independently. A wrong anchor no longer corrupts
19 paragraphs worth of content.

Suite after both landings: **1787 pass, 27 skip, 0 fail**.

## Session 3 — structured-anchor LLM redesign (2026-04-23)

Session 2's LLM wiring produced zero delta because `AmendmentLLM.parse_difficult_case`
returned a structured anchor but the downstream `AmendmentPatch.anchor_hint` was
kept as the original string — `parse_anchor_from_hint` re-parsed a weak
hint, losing whatever the LLM had resolved. Session 3 fixes the contract:

- NEW `AmendmentLLM.extract_edits_from_modifier` returns
  `list[StructuredEdit]` in one call per (modifier, target) group. Each
  edit carries a pre-parsed `Anchor` object + `old_text` + `new_text`.
- NEW `apply_patch_structured(markdown, anchor=..., ...)` consumes the
  Anchor directly and adds a literal `old_text` fallback for cases
  whose internal structure (table cells, sub-modules) can't be expressed
  through the heading tree.
- NEW `dispatch_modifier_patches(...)` groups all patches for a
  (modifier, target) pair, makes ONE Groq call for the group, correlates
  edits back by `patch_index`, applies with `apply_patch_structured`.
  Cost: N-patches → 1 call instead of N.
- Transport hardening: 429 auto-retry with Groq-supplied backoff, per-call
  `max_tokens` override (structured extract uses 4K+, scaled by group size).
- Length-gate relaxation for regex_split + llm_structured extractors:
  `[0.1, 10]` → `[0.05, 20]`. Alone this moved 25 patches from
  `length_mismatch` to `dry_run_ok` on Leyes.

### Metrics — Circulares BdE (same 30-target live cache)

| measure | session 2.5 final | session 3 structured | Δ |
|---|---:|---:|---:|
| anteriores with ≥1 applied | 28 / 82 (34.1 %) | **31 / 82 (37.8 %)** | +3 (+3.7 pp) |
| granular edits applied | 162 | **168** | +6 |
| `llm_structured` extractor | 0 | **17** | new path live |
| `llm_failed` dispatch_status | 56 (session 2) | **25** | more LLM calls usable |
| unique LLM groups called | 47 | 47 | — |

Cost: ~43 Groq gpt-oss-120b calls, ≈ $0.15 USD (cached; reruns are free).

### Metrics — Leyes / Reales Decretos (same 30-target live cache)

| measure | session 2.5 final | session 3 conf≥0.6 | session 3 conf≥0.4 | Δ total |
|---|---:|---:|---:|---:|
| anteriores with ≥1 applied | 270 / 604 (44.7 %) | 280 / 604 (46.4 %) | **292 / 604 (48.3 %)** | +22 (+3.6 pp) |
| granular edits applied | 624 | 654 | **666** | +42 |
| `llm_structured` extractor | 0 | 41 | **89** | new path live |
| `length_mismatch` | 105 | 77 | **74** | quick-win relaxation |
| `empty_new_text` | 165 | 147 | **129** | +36 recovered |

Cost: ~260 Groq calls, ≈ $0.50 USD cached (reruns free).

### The 0.6 → 0.4 confidence gate change (diagnostic-driven)

Post-run inspection of `/tmp/stage-c-leyes/llm-cache/` (264 entries, 283
edits) revealed:

- 107 edits carry a non-null structural anchor but the model self-rated
  confidence < 0.6 — our gate was dropping them.
- The model is calibration-shy on `old_text` identification even when
  its structural resolution is sound. The downstream patcher gates
  (anchor_not_found, empty_anchor, length_mismatch) are stricter than
  the model's self-assessment.

Dropping the gate to 0.4 added 48 `llm_structured` edits on Leyes and
bumped anteriores coverage +1.9 pp. BdE held flat (all additional edits
landed in anteriores that were already applied via another patch). A
true bump on BdE needs either a richer `Anchor` model (table-cell /
sub-module paths) or a fuzzy `old_text` matcher for substrings shorter
than 40 chars.

## Session 4 — cumulative fidelity & honest numbers

The session-3 metrics were measured in **non-cumulative mode**: each
(target, modifier) pair was tested against the ORIGINAL bootstrap
markdown. That systematically over-reported coverage because ~30% of
live modifiers reference articulos/disposiciones that were added by
prior reforms and don't exist in the bootstrap. ``StageCDriver`` in
production does NOT work that way — it applies modifiers in
chronological order and each one sees the state produced by prior
modifiers.

Session 4 fixed ``scripts/es_fidelity_c/live.py`` to mirror the driver:
thread the working markdown through each target's modifier chain in
chronological order (by BOE-A-YYYY-NNN). A ``--no-cumulative`` flag
preserves the legacy behaviour for side-by-side diagnostics.

### Honest metrics (cumulative, chronological — matches production)

| measure | no LLM | structured LLM (session 3 all fixes) |
|---|---:|---:|
| Circulares BdE — anteriores | 27 / 82 (32.9 %) | **31 / 82 (37.8 %)** |
| Leyes/RDs — anteriores | 218 / 604 (36.1 %) | **261 / 604 (43.2 %)** |
| Leyes `anchor_not_found` | 648 | 642 |
| Leyes `applied`-status patches | 513 | 561 |

Structured-LLM uplift in the honest regime: **+7.1 pp on Leyes**
(+43 anteriores). BdE uplift is +4.9 pp.

### Why non-cumulative over-reported

The raw-log delta on Leyes:

| status | non-cumul (session 3) | cumul (session 4) | Δ |
|---|---:|---:|---:|
| applied / dry_run_ok | 675 | 561 | −114 |
| anchor_not_found | 498 | 642 | +144 |
| length_mismatch | 75 | 47 | −28 |
| empty_new_text | 129 | 127 | −2 |

The non-cumulative path counted 114 "applied" that in production never
would have — either because (a) the real chain state no longer had the
anchor, or (b) an earlier patch already consumed the same region.
Conversely 30 `length_mismatch` cases cleared up because after a prior
reform the replacement region was proportionally-sized.

### Remaining ceiling & next moves

The cumulative ceiling is bounded by CASCADING ERRORS: a patch whose
anchor resolves to the wrong region mutates the markdown in a way that
breaks the next patch's anchor. Cumulative `anchor_not_found` (642) is
+144 over non-cumulative precisely because of that.

Three directions, ordered by expected ROI:

1. **Stricter apply gate**: refuse LLM-structured edits when the
   resolved position.content has no recognisable apartado/letra marker
   near the start. Prevents "accidentally unique" matches that wreck
   surrounding structure.
2. **Rollback-aware batching**: for each modifier, gather dispatches in
   dry-run, require all patches to resolve, then apply. Partial
   applies on a single modifier are the main corruption source.
3. **Richer ``Anchor`` model** (table cells, módulo X.Y): unlocks
   BdE's unreachable ~45 % of patches. Big scope, low short-term ROI.

## Session 4.5 — bottom-up apply (tested, limited return)

Tried (and kept, as it's slightly safer): the group dispatcher now
resolves every patch against the ORIGINAL base in a dry-run phase,
sorts the survivors by ``position.line_start`` DESC, then mutates
bottom-up using a pre-resolved Position (via new
``apply_at_position``). This guarantees that a later patch's position
is still valid after an earlier mutation, because we mutate lower
document lines first.

Empirical delta on cumulative Leyes: 261 → 248 / 604 (barely moved).
Reason: within a single modifier patches rarely clash. The cascading
errors are CROSS-modifier — modifier M1's mutation invalidates M2's
anchors. Bottom-up only fixes within-modifier ordering.

Unsolved: the real uplift requires either an Anchor model that can
still locate moved sections (e.g. heading-text fuzzy match even after
surrounding reforms renumber articles) or a commit-pointer fallback
for chain-inconsistent modifiers rather than silent "anchor not
found". Both outside scope for session 4.

### Honest final numbers (cumulative, with ALL session-3+4 fixes)

| corpus | no LLM cumul | LLM + structured + sanitize + bottom-up |
|---|---:|---:|
| Circulares BdE | 27 / 82 (32.9 %) | **31 / 82 (37.8 %)** |
| Leyes/RDs | 218 / 604 (36.1 %) | **248 / 604 (41.1 %)** |

Net LLM uplift in honest (production-shaped) regime: **+5.0 pp on Leyes**,
**+4.9 pp on BdE**. Coverage is limited primarily by document-structure
evolution across modifiers, not by the LLM's quality. Suite: 1809 pass.

### What the LLM still can't fix (BdE)

Groq returns an anchor with every field null + confidence < 0.6 on most
bucket-A and bucket-J cases. The caches show the model's own reason:
"El fragmento base no contiene el módulo C.1" / "no puedo identificar la
ubicación sin más contexto". Two remaining blockers:

1. The target's internal structure (table cells, "módulo X.Y", numbered
   estado columns) is not representable in the current `Anchor`
   dataclass. Even if the model correctly identifies the region, we
   cannot build a resolvable anchor.
2. Many modifier bodies don't quote a long enough literal old_text to
   trigger the 40-char substring fallback — so even when the LLM
   correctly says "replace this table cell", we can't match.

Next unlock is NOT more LLM power but a richer Anchor model (or a pure
old_text-diff apply path that doesn't need structural anchoring).

Suite after session 3: **1801 pass, 27 skip, 0 fail** (+14 new tests
in `tests/test_stage_c_structured.py`).
