"""Tests for transformer/patcher.py — applying AmendmentPatches to a
base Markdown with hash-check + dry-run safety gates."""

from __future__ import annotations

from datetime import date


from legalize.fetcher.es.amendments import AmendmentPatch
from legalize.transformer.patcher import apply_patch


BASE_MARKDOWN = """\
###### Artículo 5. Recursos económicos.

1. Los recursos economicos estaran compuestos por:

   a) Las cuotas de afiliacion.

   b) Los productos de las actividades propias.

   c) Las donaciones recibidas.

2. Tambien se incluiran las subvenciones publicas.

3. Los recursos extraordinarios se regularan aparte.

###### Artículo 10. Disposición final.

Entrada en vigor al dia siguiente.
"""


def _patch(
    operation: str = "replace",
    anchor_hint: str = "la letra b) del apartado 1 del articulo 5",
    new_text: tuple[str, ...] | None = ("b) Nueva letra b con contenido sustituido.",),
    verb_code: str = "270",
) -> AmendmentPatch:
    return AmendmentPatch(
        target_id="BOE-A-X",
        operation=operation,  # type: ignore[arg-type]
        verb_code=verb_code,
        verb_text={"270": "MODIFICA", "407": "ANADE", "235": "SUPRIME", "210": "DEROGA"}[verb_code],
        anchor_hint=anchor_hint,
        source_boe_id="BOE-A-Y",
        source_date=date(2021, 12, 29),
        new_text=new_text,
        anchor_confidence=0.95,
        new_text_confidence=0.95,
        extractor="regex",
    )


# ──────────────────────────────────────────────────────────
# Happy paths
# ──────────────────────────────────────────────────────────


def test_replace_letra_writes_new_text() -> None:
    """Longest-standing Stage C case: replace a single letra. The
    resulting markdown must contain the new text and drop the old one.

    The old letra is 'b) Los productos de las actividades propias.' — a
    single short line. The new_text is similarly short. Length ratio
    gate skips because old_len < SHORT_OLD_CHARS (40)."""
    new = ("b) Nueva letra b con un texto lo bastante largo para ejercitar bien el gate.",)
    result = apply_patch(BASE_MARKDOWN, _patch(new_text=new))
    assert result.status == "applied"
    assert "Nueva letra b" in result.new_markdown
    assert "Los productos de las actividades propias" not in result.new_markdown
    # Neighbouring letras must survive untouched.
    assert "cuotas de afiliacion" in result.new_markdown
    assert "donaciones recibidas" in result.new_markdown


def test_delete_suprime_removes_letra() -> None:
    result = apply_patch(BASE_MARKDOWN, _patch(operation="delete", new_text=None, verb_code="235"))
    assert result.status == "applied"
    assert "Los productos de las actividades propias" not in result.new_markdown
    # Other letras remain.
    assert "cuotas de afiliacion" in result.new_markdown
    assert "donaciones recibidas" in result.new_markdown


def test_insert_anade_appends_after_parent() -> None:
    """ANADE adds a new letra after the parent apartado's last line."""
    patch = _patch(
        operation="insert",
        anchor_hint="el apartado 1 del articulo 5",
        new_text=("d) Nueva letra d añadida al final del apartado.",),
        verb_code="407",
    )
    result = apply_patch(BASE_MARKDOWN, patch)
    assert result.status == "applied"
    assert "Nueva letra d" in result.new_markdown
    # The three original letras must still be there, in order.
    md = result.new_markdown
    pos_a = md.index("cuotas")
    pos_b = md.index("productos")
    pos_c = md.index("donaciones")
    pos_d = md.index("Nueva letra d")
    assert pos_a < pos_b < pos_c < pos_d


def test_dry_run_passes_gates_without_mutating() -> None:
    new = ("b) Nueva letra b con un texto lo bastante largo para ejercitar el gate.",)
    result = apply_patch(BASE_MARKDOWN, _patch(new_text=new), dry_run=True)
    assert result.status == "dry_run_ok"
    # Nothing was written.
    assert result.new_markdown == BASE_MARKDOWN
    # But the position IS populated so callers can inspect.
    assert result.position is not None


# ──────────────────────────────────────────────────────────
# Safety gate rejections
# ──────────────────────────────────────────────────────────


def test_anchor_not_found_returns_base_unchanged() -> None:
    patch = _patch(anchor_hint="la letra z) del apartado 99 del articulo 999")
    result = apply_patch(BASE_MARKDOWN, patch)
    assert result.status == "anchor_not_found"
    assert result.new_markdown == BASE_MARKDOWN


def test_empty_new_text_rejects_replace() -> None:
    patch = _patch(new_text=None)
    result = apply_patch(BASE_MARKDOWN, patch)
    assert result.status == "empty_new_text"
    assert result.new_markdown == BASE_MARKDOWN


def test_delete_with_new_text_is_rejected() -> None:
    patch = _patch(operation="delete", verb_code="235", new_text=("should not be here",))
    result = apply_patch(BASE_MARKDOWN, patch)
    assert result.status == "delete_with_text"


def test_unsupported_operation_rejected() -> None:
    patch = AmendmentPatch(
        target_id="BOE-A-X",
        operation="rewrite",  # type: ignore[arg-type]
        verb_code="270",
        verb_text="?",
        anchor_hint="articulo 5",
        source_boe_id="BOE-A-Y",
        source_date=date(2021, 1, 1),
        new_text=("x",),
    )
    result = apply_patch(BASE_MARKDOWN, patch)
    assert result.status == "unsupported_operation"


def test_length_mismatch_rejects_over_long_replacement() -> None:
    """If new_text is > 10× the size of the region it replaces (and
    the region is large enough for the ratio check to be meaningful),
    the gate refuses. Prevents LLM-truncation false-positives from
    corrupting a whole article into one line."""
    # Build a synthetic base with a large apartado so the ratio check
    # actually fires (SHORT_OLD_CHARS threshold = 40).
    big_apartado = "1. " + ("Texto largo del apartado uno. " * 20)
    md = "###### Artículo 5.\n\n" + big_apartado + "\n\n2. Segundo apartado.\n"
    patch = _patch(
        operation="replace",
        anchor_hint="el apartado 1 del articulo 5",
        new_text=("x" * (len(big_apartado) * 15),),  # 15× the size
    )
    result = apply_patch(md, patch)
    assert result.status == "length_mismatch"


def test_length_mismatch_allows_normal_replacement() -> None:
    """A same-order-of-magnitude replacement must pass the gate."""
    big_apartado = "1. " + ("Texto del apartado uno. " * 20)
    md = "###### Artículo 5.\n\n" + big_apartado + "\n\n2. Segundo.\n"
    patch = _patch(
        operation="replace",
        anchor_hint="el apartado 1 del articulo 5",
        new_text=("1. " + ("Texto sustituido del apartado uno. " * 18),),
    )
    result = apply_patch(md, patch)
    assert result.status == "applied"


# ──────────────────────────────────────────────────────────
# Boundary preservation
# ──────────────────────────────────────────────────────────


def test_replace_preserves_trailing_newline() -> None:
    """Stage A Markdown always ends with \\n. Patcher must not drop it."""
    assert BASE_MARKDOWN.endswith("\n")
    patch = _patch(new_text=("b) Nueva letra b con un texto lo bastante largo para el gate.",))
    result = apply_patch(BASE_MARKDOWN, patch)
    assert result.status == "applied"
    assert result.new_markdown.endswith("\n")


def test_delete_does_not_leave_multiple_blank_lines() -> None:
    """Deletion collapses any run of 2+ blank lines in its aftermath."""
    patch = _patch(operation="delete", new_text=None, verb_code="235")
    result = apply_patch(BASE_MARKDOWN, patch)
    assert "\n\n\n" not in result.new_markdown
