"""YAML frontmatter generation for norm Markdown files.

8 core fields (fixed order, all countries), then department/jurisdiction
if present, then country-specific extra fields from the source API.

  ---
  title: "Real Decreto Legislativo 2/2015..."
  identifier: "BOE-A-2015-11430"
  country: "es"
  rank: "real_decreto_legislativo"
  publication_date: "2015-10-24"
  last_updated: "2026-03-30"
  status: "in_force"
  source: "https://www.boe.es/eli/es/rdlg/2015/10/23/2"
  department: "Ministerio de Empleo y Seguridad Social"
  official_number: "2/2015"
  enactment_date: "2015-10-23"
  official_journal: "Boletín Oficial del Estado"
  journal_issue: "255"
  consolidation_status: "Finalizado"
  scope: "Estatal"
  ---
"""

from __future__ import annotations

from datetime import date

from legalize.models import NormMetadata, NormStatus


def render_frontmatter(metadata: NormMetadata, version_date: date) -> str:
    """Generates the YAML frontmatter block for a norm at a given date.

    Core fields first (fixed order), then department/jurisdiction,
    then extra fields from country-specific metadata.
    """
    clean_title = _clean_title(metadata.title)
    status = metadata.status.value if isinstance(metadata.status, NormStatus) else metadata.status

    lines = [
        "---",
        f'title: "{_escape_yaml(clean_title)}"',
        f'identifier: "{metadata.identifier}"',
        f'country: "{metadata.country}"',
        f'rank: "{metadata.rank}"',
    ]

    # Extract promoted extra fields that belong at specific positions in the
    # frontmatter (mx-specific: gov_organ/entidad_federativa near jurisdiction,
    # last_reform_dof near last_updated, gazette_pdf_page near source).
    # Remaining extras go at the end as usual.
    _promoted = {"gov_organ", "entidad_federativa", "last_reform_dof", "gazette_pdf_page"}
    extra_map: dict[str, str] = dict(metadata.extra)
    gov_organ = extra_map.get("gov_organ")
    entidad_federativa = extra_map.get("entidad_federativa")
    last_reform_dof = extra_map.get("last_reform_dof")
    gazette_pdf_page = extra_map.get("gazette_pdf_page")
    remaining_extra = [(k, v) for k, v in metadata.extra if k not in _promoted]

    if metadata.jurisdiction:
        lines.append(f'jurisdiction: "{metadata.jurisdiction}"')
    if gov_organ:
        lines.append(f'gov_organ: "{_escape_yaml(gov_organ)}"')
    if entidad_federativa:
        lines.append(f'entidad_federativa: "{_escape_yaml(entidad_federativa)}"')

    lines += [
        f'publication_date: "{metadata.publication_date.isoformat()}"',
        f'last_updated: "{version_date.isoformat()}"',
    ]
    if last_reform_dof:
        lines.append(f'last_reform_dof: "{_escape_yaml(last_reform_dof)}"')
    lines.append(f'status: "{status}"')
    lines.append(f'source: "{metadata.source}"')
    if gazette_pdf_page:
        lines.append(f'gazette_pdf_page: "{_escape_yaml(gazette_pdf_page)}"')

    if metadata.department:
        lines.append(f'department: "{_escape_yaml(metadata.department)}"')
    if metadata.pdf_url:
        lines.append(f'pdf_url: "{metadata.pdf_url}"')
    if metadata.subjects:
        subj_yaml = ", ".join(f'"{_escape_yaml(s)}"' for s in metadata.subjects)
        lines.append(f"subjects: [{subj_yaml}]")

    for key, value in remaining_extra:
        lines.append(f'{key}: "{_escape_yaml(value)}"')

    lines.append("---")
    lines.append("")

    return "\n".join(lines)


def _escape_yaml(text: str) -> str:
    """Escapes special characters for YAML double-quoted strings."""
    return text.replace("\\", "\\\\").replace('"', '\\"')


def _clean_title(raw_title: str) -> str:
    """Cleans the title: remove trailing period, normalize spaces."""
    return raw_title.rstrip(". ").strip()
