# Mexico (MX) — Multi-source Legal Corpus Research

> Status as of 2026-04-28: Mexico is wired as a multi-source scaffold.
> The Diputados federal consolidated-law path is implemented end-to-end and
> bootstrapped: 316 federal laws, 3,282 commits (one per DOF reform date),
> rendering from .doc (not PDF) with HTTP cache + rate-limit-on-miss-only.
> DOF, OJN, SJF, UNAM, and Justia are registered for routing but are not
> fetchable yet — DOF is the next planned wiring target so daily updates
> can begin.

## 0.1 Source Inventory

Mexico does not have one clean equivalent to Spain's BOE consolidated API or
Denmark's ELI XML endpoints. The implementation therefore uses prefixed IDs and
per-source adapters:

| Source | ID prefix | Kind | URL | Current status |
|---|---|---|---|---|
| Camara de Diputados, LeyesBiblio | `DIP` | `primary_legislation` | `https://www.diputados.gob.mx/LeyesBiblio` | Implemented for federal consolidated laws |
| Diario Oficial de la Federacion | `DOF` | `primary_legislation` | `https://www.dof.gob.mx` | Registered only |
| Orden Juridico Nacional | `OJN` | `primary_legislation` | `https://www.ordenjuridico.gob.mx` | Registered only; fixtures captured |
| Semanario Judicial de la Federacion | `SJF` | `case_law` | `https://sjf2.scjn.gob.mx` | Registered only; outside current norm model |
| UNAM Biblioteca Juridica Virtual | `UNAM` | `doctrine` | `https://biblio.juridicas.unam.mx/bjv` | Registered only; not a norm source |
| Justia Mexico | `JUSTIA` | `aggregator` | `https://mexico.justia.com` | Registered only; useful as fallback, not primary |

Official-source notes:

- Diputados LeyesBiblio publishes a federal-law index with PDF and DOC links,
  original DOF date, and latest reform date. The page itself states that the
  compilation is informational, and that the DOF is the official publication
  organ for federal laws, regulations, and decrees.
- DOF is the official gazette and must be the source of truth for daily updates,
  reform events, and original publication text.
- OJN aggregates federal, state, municipal, and international material. It is a
  candidate for state and municipal coverage, but should not replace DOF where
  DOF has the primary publication record.

Local TLS/network observations:

- `curl -I https://www.dof.gob.mx/` and
  `curl -I https://www.ordenjuridico.gob.mx/leyes2016.php` failed locally with
  certificate-chain verification errors.
- `curl -I https://www.diputados.gob.mx/LeyesBiblio/index.htm` timed out during
  one local probe. The saved fixture and search-engine crawl confirm the endpoint
  exists, but the client should keep conservative timeouts/retries.

## 0.2 Implemented Path: Diputados

Code:

- Client: `src/legalize/fetcher/mx/client.py`
- Discovery: `src/legalize/fetcher/mx/discovery.py`
- Parser: `src/legalize/fetcher/mx/parser.py`
- Tests: `tests/test_parser_mx.py`
- Config: `config.yaml`, key `mx`

Access pattern:

```text
GET https://www.diputados.gob.mx/LeyesBiblio/index.htm
GET https://www.diputados.gob.mx/LeyesBiblio/doc/{ABBREV}.doc        # primary
GET https://www.diputados.gob.mx/LeyesBiblio/pdf/{ABBREV}.pdf        # fallback
```

The current adapter parses the index into `DiputadosRow` records and emits IDs
as `DIP-{ABBREV}`. A fixture parse of `tests/fixtures/mx/diputados-index.html`
returns 316 rows; the live index has matched at the time of bootstrap.

Source format: the parser pulls .doc (Word 97-2003 OLE2) by default and falls
back to PDF only when the .doc URL is missing. The .doc path produces cleaner
text (no PDF-line-wrap heuristics, no page-chrome scraping) and is what feeds
the 3,282-commit bootstrap. PDF parsing is still wired but rarely used.

Implemented examples:

| ID | Title | Original DOF date | Latest reform date | PDF |
|---|---|---:|---:|---|
| `DIP-CPEUM` | Constitucion Politica de los Estados Unidos Mexicanos | 1917-02-05 | 2026-04-10 | `pdf/CPEUM.pdf` |
| `DIP-CCF` | Codigo Civil Federal | 1928-05-26 | 2025-11-14 | `pdf/CCF.pdf` |
| `DIP-CCom` | Codigo de Comercio | 1889-10-07 | 2026-02-18 | `pdf/CCom.pdf` |
| `DIP-CFF` | Codigo Fiscal de la Federacion | 1981-12-31 | 2026-04-09 | `pdf/CFF.pdf` |
| `DIP-LFT` | Ley Federal del Trabajo | 1970-04-01 | 2026-01-15 | `pdf/LFT.pdf` |

Metadata mapping:

| Source field | Legalize field |
|---|---|
| Index title | `title`, `short_title` |
| `DIP-{ABBREV}` | `identifier` |
| Original DOF date | `publication_date` |
| Latest reform date | `last_modified`, `extra.last_reform_dof` |
| Rank inferred from title | `rank` |
| Issuing/governing organ (Congreso vs Ejecutivo) | `extra.gov_organ` |
| Jurisdiction (federal/state/municipal) | `extra.jurisdiction` |
| Entidad federativa (when state-scoped) | `extra.entidad_federativa` |
| DOC URL (primary source) | `source`, `extra.doc_url` |
| PDF URL (fallback) | `extra.pdf_url` |
| Per-reform DOF page in gazette | `extra.gazette_pdf_page` |
| Source adapter name | `extra.source_name=diputados` |
| Abbreviation | `extra.abbrev` |

Output lives in the country repo `legalize-mx` (one file per law,
one commit per DOF reform date). The engine repo no longer ships rendered
exports; `exports/mx/` is empty by design and `scripts/export_mx.py` is
the in-PR re-render utility.

## 0.3 Formatting Inventory: Diputados .doc

The parser reads Word 97-2003 .doc files via `olefile` against the
`WordDocument` stream, decoded as latin-1, with a binary-garbage filter
that drops field codes (IF/FORMTEXT, $IfF, mH/sH, CJ^J, CJPJaJ), DOF page
headers, and trailing OLE remnants. Tables are extracted via Word cell
marks (`\x07`) and rendered as Markdown pipe tables.

Implemented normalization:

- Strip Word field codes and OLE binary remnants (~6 detection signals).
- Normalize Unicode to NFC and strip C0/C1 control characters.
- Preserve paragraph boundaries via Word paragraph marks.
- Detect article headings such as `Articulo 1o.`, `Articulo 4o.-`,
  `ARTICULO 123.`, and `Articulo Unico.-`.
- Detect section-like headings: `LIBRO`, `TITULO`, `CAPITULO`, `SECCION`,
  and `ARTICULOS TRANSITORIOS`.
- Detect sub-article markers: `A.`, Roman-numeral fracciones (with period
  insertion normalization), and `a)` incisos.
- Detect reform provenance stamps such as `Parrafo reformado DOF 04-12-2006`,
  tag them as `nota_pie`, AND extract them as DOF reform timeline events
  used to split a single law's history into per-reform commits.
- Render Word tables (CFF tarifa tables etc.) as Markdown pipe tables.
- Detect issuing-decree blocks at the start of laws and trim them
  consistently.

Audit script (`scripts/audit_mx.py`) gates regressions across 9 categories:
field codes, short garbage lines, PDF source URL leakage, issuing decree
present at start, PDF page footer leakage, tail binary blob, repeated
short tail, frontmatter completeness, reform-count sanity. All 8/8
content categories pass on the current 316-law corpus.

Known limitations:

- Per-reform commits are reconstructed from DOF stamps inside the .doc,
  not from real DOF text. Each reform commit is the consolidated text as
  of that reform date, not the diff that the gazette actually published.
- Images are dropped (counted in `extra.images_dropped`); we are not
  ingesting binary assets.
- Ranking is inferred from title keywords, not authoritative metadata.
- Footnotes and signatures still come through as plain paragraphs.

## 0.4 Fixtures

Current MX fixture coverage:

| Source | Fixture | Purpose |
|---|---|---|
| Diputados | `tests/fixtures/mx/diputados-index.html` | Federal consolidated-law index |
| DOF | `tests/fixtures/mx/dof/nota_5738985_reforma_constitucional.html` | Constitutional reform note |
| DOF | `tests/fixtures/mx/dof/nota_5746354_mf_fiscal_2025.html` | Fiscal miscellany note |
| DOF | `tests/fixtures/mx/dof/nota_5785957_decreto_reglamento.html` | Regulation decree note |
| DOF | `tests/fixtures/mx/dof/nota_5785960_acuerdo_fiscal.html` | Fiscal agreement note |
| OJN | `tests/fixtures/mx/ojn/fixture_federal_index.html` | Federal OJN index |
| OJN | `tests/fixtures/mx/ojn/fixture1_federal_codigo_civil.doc` | Federal code DOC |
| OJN | `tests/fixtures/mx/ojn/fixture2_federal_ley_trabajo.doc` | Federal labor-law DOC |
| OJN | `tests/fixtures/mx/ojn/fixture3_treaty_colombia_tlc.pdf` | Treaty PDF |
| OJN | `tests/fixtures/mx/ojn/fixture4_ficha_jalisco_constitucion.html` | State law detail page |
| OJN | `tests/fixtures/mx/ojn/fixture4_estatal_jalisco_constitucion.doc` | State constitution DOC |
| OJN | `tests/fixtures/mx/ojn/fixture5_ficha_ensenada_alcoholes.html` | Municipal law detail page |
| OJN | `tests/fixtures/mx/ojn/fixture5_municipal_ensenada_alcoholes.doc` | Municipal regulation DOC |

The DOF and OJN fixtures are currently untracked in this worktree; do not assume
they are available in a fresh checkout unless they are committed.

## 0.5 Version History

Diputados publishes current consolidated text and embeds reform-stamp
provenance as `DOF dd-mm-yyyy` markers next to amended fragments. We use
those stamps to reconstruct a per-reform timeline for each law: the
parser collects the unique reform dates, and the bootstrap commits a
snapshot of the consolidated text at each date in chronological order
(backdated via `GIT_AUTHOR_DATE`). The result is 3,282 commits across
316 federal laws — one per DOF reform date — pushed to
`AIDataForXYZ/legalize-mx`.

Caveat: this is a reconstruction, not the real text-as-of-date. Each
historical commit shows the text that the *current* consolidated .doc
attributes to that reform stamp, not the gazette-published text from
that day. Stamps prior to 1970-01-02 are clamped to 1970-01-02 because
GitHub's `git index-pack` rejects negative Unix timestamps; the accurate
date is preserved in the `Source-Date` trailer of the commit body.

History target after DOF lands:

1. Discover daily DOF notes.
2. Classify notes by legal effect: new law, reform decree, regulation, agreement,
   repeal, errata.
3. Link DOF reform notes back to affected `DIP-*` laws by title, abbreviation,
   and article references.
4. Reconstruct commit history from DOF events where the amendment text is
   machine-applicable.
5. Fall back to tracker commits for reforms that cannot be applied safely.

This matches the project pattern used elsewhere: Colombia ships single-snapshot
where no point-in-time text is available, while Denmark uses consolidated
full-text versions where the official source exposes the chain.

## 0.6 Scope

Current implemented scope:

- Federal primary legislation listed by Diputados LeyesBiblio.
- Index fixture count: 316 law rows; 316 laws bootstrapped end-to-end.
- 3,282 commits in `legalize-mx` (one per DOF reform date per law).
- Discovery yields `DIP-*` IDs only.
- Daily discovery is wired (workflow `daily-update-mx.yml`, Mon-Sat 09:00 UTC,
  ubuntu-latest) but `MXDiscovery.discover_daily()` is a stub that returns
  empty for all sources; daily runs are therefore green no-ops until DOF
  daily discovery is implemented.

Likely future scope:

- DOF: official daily federal publication and reform feed.
- OJN: federal/state/municipal/international full-text coverage where primary
  local sources are fragmented.
- SJF: case law and jurisprudencia, but this needs a separate case-law model
  before ingestion.
- UNAM: doctrine and historical/library material; useful for references, not
  committed as norms.
- Justia: aggregator fallback only, especially for state-level laws that OJN or
  local portals do not expose cleanly.

## 0.7 Implementation Clues From Other Countries

Patterns already copied or relevant:

- Colombia (`RESEARCH-CO.md`): single-snapshot model is acceptable when the
  source exposes reform references but not point-in-time full text.
- Denmark (`RESEARCH-DK.md`): when consolidated snapshots exist, model each
  official snapshot as a version and use source metadata to build history.
- Spain (`RESEARCH-ES-v2.md`): DOF should be treated like BOE daily XML: the
  gazette is the authoritative source for official publication and reforms, even
  when another portal has a convenient consolidated text.
- Latvia/Switzerland/Belgium parsers: table, footnote, link, and rich inline
  handling should be ported when MX moves beyond PDF-only extraction.

Concrete next steps:

1. Implement DOF daily discovery so `daily-update-mx.yml` produces real
   commits — the workflow is otherwise green-noop until then.
2. Commit the DOF/OJN fixtures (currently untracked under
   `tests/fixtures/mx/dof/` and `tests/fixtures/mx/ojn/`) before opening
   the upstream PR.
3. Implement DOF metadata/text parsing against the four saved `nota_*`
   pages.
4. Decide on OJN parser scope — only worth it if DOC quality there is
   comparable to Diputados, otherwise OJN duplicates federal coverage.
5. Add image and signature handling once we are ready to ingest binary
   assets; currently dropped + counted in `extra.images_dropped`.
6. Reconcile reconstructed reform commits with real DOF text once DOF
   text parsing lands — at that point we can replace approximated
   per-reform snapshots with gazette-accurate text.
