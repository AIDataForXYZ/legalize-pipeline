# Instructions: Converting a Mexican Federal Law to MD

## Source
All federal laws are listed at:
**https://www.diputados.gob.mx/LeyesBiblio/index.htm**

Each law has two downloadable formats linked from that page:
- **PDF** — `https://www.diputados.gob.mx/LeyesBiblio/pdf/[abbrev].pdf`
- **DOC** — `https://www.diputados.gob.mx/LeyesBiblio/doc/[abbrev].doc`

The `[abbrev]` is the short code used in the filename (e.g. `LAdua`, `LAgra`, `CCF`).

---

## Frontmatter

Every MD file starts with a YAML frontmatter block using these fields, in this order:

```yaml
---
title: "FULL LAW TITLE IN CAPS"
identifier: "DIP-[abbrev]"
country: "mx"
rank: "ley"
gov_organ: "congreso_federal"
entidad_federativa: "na"
jurisdiction: "federal"
publication_date: "DD-Mon-YYYY"
last_reform_dof: "DD-Mon-YYYY"
last_updated: "DD-Mon-YYYY"
status: "in_force"
source: "https://www.diputados.gob.mx/LeyesBiblio/index.htm"
source_name: "diputados"
department: "Cámara de Diputados"
pdf_url: "https://www.diputados.gob.mx/LeyesBiblio/pdf/[abbrev].pdf"
pdf_doc_#: [uuid]
doc_url: "https://www.diputados.gob.mx/LeyesBiblio/doc/[abbrev].doc"
doc_#: [uuid]
abbrev: "[abbrev]"
gazette_pdf_page: "https://www.diariooficial.gob.mx/index_100.php?year=YYYY&month=MM&day=DD#gsc.tab=0"
---
```

### Where each field comes from

| Field | Where to find it |
|---|---|
| `title` | Full official name of the law, as it appears in the PDF |
| `identifier` | Always `DIP-` followed by the abbrev |
| `rank` | `ley` for leyes, `codigo` for códigos — based on document type |
| `publication_date` | First page of the PDF: "Nueva Ley publicada en el DOF el..." |
| `last_reform_dof` | First page header of the PDF: "Última Reforma DOF DD-MM-YYYY" |
| `last_updated` | Same as `last_reform_dof` |
| `pdf_doc_#` | XMP metadata embedded in the PDF — look for the `DocumentID` field |
| `doc_#` | Internal metadata of the `.doc` file — there is one UUID inside it |
| `abbrev` | The filename code used in the PDF and DOC URLs |
| `gazette_pdf_page` | DOF gazette URL — use the date from `last_reform_dof` to build the URL |

### Notes
- `gov_organ`, `entidad_federativa`, `jurisdiction`, `source`, `source_name`, `department` are the same for all federal laws from this source.
- All dates use the format `DD-Mon-YYYY` (e.g. `15-Dec-1995`, `14-Nov-2025`).
- `gazette_pdf_page` lets the user look up the specific DOF issue where the last reform was published. Build the URL from the last reform date.

---

## Body Content

After the frontmatter, the document body follows this heading hierarchy:

```
# LAW TITLE

## Título/TITULO [number or name]         ← major divisions
##### [subtitle of that title]
### Capítulo [number]                      ← chapters
#### [subtitle of that chapter]
###### Artículo Xo.-                       ← articles

Article body text here.

> <small>Párrafo reformado DOF DD-MM-YYYY</small>   ← reform notes
```

### Heading levels
- `##` — Título (major division). Some laws write it as `TITULO` (all caps), others as `Título`.
- `###` — Capítulo
- `######` — Artículo. Always formatted as `Artículo Xo.-` (with the period-dash).

### Reform notes
Any line that records a DOF amendment belongs inside a `> <small>...</small>` block. These typically look like:
- `Párrafo reformado DOF DD-MM-YYYY`
- `Fracción adicionada DOF DD-MM-YYYY`
- `Artículo derogado DOF DD-MM-YYYY`
- `Denominación del Capítulo reformada DOF DD-MM-YYYY`

### Things to watch for
- **Roman numerals in fractions**: PDFs from this source sometimes encode capital `I` as lowercase `l` due to font issues. Double-check that `II`, `III`, `IV`, `IX` etc. are correct in the extracted text.
- **Page headers**: Every PDF page repeats a header with the law name, chamber info, and page number. These should not appear in the MD.
- **Preamble**: The first page contains a decree preamble (presidential promulgation text) before the law body begins. Include it as plain paragraphs before the first `## Título`.
