"""Auto-detect table format type from structure and content.

Decision tree (priority order):
1. TRANSPOSED: first column has >=3 parameter names
2. QH_MATRIX: >=5 numeric ascending column headers
3. FLAT_TABLE: column classifier finds model + Q + H
4. CURVE_TABLE: Q(m³/h) pattern + model/H blocks in text
5. LIST_FORMAT: <=2 columns, >=5 rows, numbered items
6. UNKNOWN: none match
"""

import re
import logging
from typing import Optional

from pump_parser.models import TableType, ExtractedTable
from pump_parser.classifiers.column_classifier import (
    classify_columns,
    detect_transposed,
    detect_qh_matrix,
)

log = logging.getLogger("pump_parser.table_type")

# ─── Helpers ──────────────────────────────────────────────────────────────────

_PARAM_KEYWORDS = {
    "модель", "model", "modell", "modello", "modèle", "modelo",
    "расход", "подача", "flow", "delivery", "capacity", "portata", "débit", "caudal",
    "напор", "head", "prevalenza", "hauteur", "altura",
    "мощность", "power", "potenza", "puissance", "leistung", "potencia",
    "обороты", "speed", "rpm", "velocità", "vitesse",
    "тип", "type", "насос", "pump",
}

RE_Q_HEADER = re.compile(
    r"Q\s*[\(\[\{]?\s*(?:м[³3]\s*/\s*ч|m[³3]\s*/\s*h|m3\s*/\s*h|l/min|gpm|л/мин)",
    re.IGNORECASE,
)

RE_MODEL_LINE = re.compile(
    r"^[A-ZА-ЯЁ]{2,6}[\s\-]?\d{1,3}[\s\-/]\d{1,3}",
    re.MULTILINE,
)


def _is_numeric(s: str) -> bool:
    try:
        float(str(s).strip().replace(",", "."))
        return True
    except (ValueError, TypeError):
        return False


def _parse_float(s: str) -> Optional[float]:
    try:
        return float(str(s).strip().replace(",", "."))
    except (ValueError, TypeError):
        return None


def _is_ascending(values: list[float]) -> bool:
    """Check if values are generally ascending (allow small dips)."""
    if len(values) < 3:
        return False
    ascending_count = sum(1 for i in range(1, len(values)) if values[i] >= values[i - 1])
    return ascending_count >= len(values) * 0.7


# ─── Main Detector ────────────────────────────────────────────────────────────


def detect_table_type(
    table: Optional[ExtractedTable] = None,
    page_text: str = "",
    headers: Optional[list[str]] = None,
    rows: Optional[list[list[str]]] = None,
) -> tuple[TableType, float]:
    """Determine table format type from structure.

    Can accept either an ExtractedTable or raw headers+rows.

    Args:
        table: ExtractedTable object (if available)
        page_text: raw text from the page
        headers: column headers (alternative to table)
        rows: data rows (alternative to table)

    Returns:
        (TableType, confidence) tuple
    """
    if table is not None:
        headers = table.headers
        rows = table.rows

    has_table = headers is not None and rows is not None
    n_cols = len(headers) if headers else 0
    n_rows = len(rows) if rows else 0

    # ── 1. TRANSPOSED: first column has parameter names ──
    if has_table and n_rows >= 3 and n_cols >= 3:
        first_col_values = [str(rows[i][0]).lower().strip() for i in range(min(n_rows, 10))]
        param_matches = sum(
            1 for v in first_col_values
            if any(kw in v for kw in _PARAM_KEYWORDS)
        )
        if param_matches >= 3:
            log.debug("Table type: TRANSPOSED (%d param matches in first col)", param_matches)
            return TableType.TRANSPOSED, min(0.95, 0.6 + param_matches * 0.1)

        # Also use the classifier's detect_transposed
        if detect_transposed(headers, first_col_values):
            return TableType.TRANSPOSED, 0.80

    # ── 2. QH_MATRIX: many numeric ascending column headers ──
    if has_table and n_cols >= 6:
        # Check headers directly
        numeric_headers = []
        for i, h in enumerate(headers):
            v = _parse_float(h)
            if v is not None and v >= 0:
                numeric_headers.append((i, v))

        if len(numeric_headers) >= 5:
            values = [v for _, v in numeric_headers]
            if _is_ascending(values):
                log.debug("Table type: QH_MATRIX (%d numeric ascending headers)", len(numeric_headers))
                return TableType.QH_MATRIX, 0.90

        # Also try detect_qh_matrix from classifier (handles complex cases)
        matrix_info = detect_qh_matrix(headers, rows)
        if matrix_info is not None:
            log.debug("Table type: QH_MATRIX (via detect_qh_matrix)")
            return TableType.QH_MATRIX, 0.85

    # ── 3. FLAT_TABLE: column classifier finds model + Q/H ──
    if has_table and n_cols >= 3 and n_rows >= 2:
        classified = classify_columns(headers, rows, page_text)
        if classified["is_pump_table"]:
            col_map = classified["columns"]
            has_model = "model" in col_map
            has_q = "q_nom" in col_map
            has_h = "h_nom" in col_map
            if has_model and (has_q or has_h):
                confidence = 0.70
                if has_q and has_h:
                    confidence = 0.85
                if "power_kw" in col_map:
                    confidence = 0.90
                log.debug("Table type: FLAT_TABLE (cols: %s)", col_map)
                return TableType.FLAT_TABLE, confidence

    # ── 4. CURVE_TABLE: Q-row header in text + model/H blocks ──
    if RE_Q_HEADER.search(page_text):
        # Look for pattern: Q row followed by model lines with numbers
        model_count = len(RE_MODEL_LINE.findall(page_text))
        if model_count >= 2:
            log.debug("Table type: CURVE_TABLE (Q header + %d model lines)", model_count)
            return TableType.CURVE_TABLE, 0.80

        # Even without model patterns, Q header is a strong signal
        # Check for numeric rows after Q header
        lines = page_text.split("\n")
        numeric_lines = sum(1 for l in lines if len(re.findall(r"[\d]+[.,]?\d*", l)) >= 4)
        if numeric_lines >= 3:
            return TableType.CURVE_TABLE, 0.70

    # ── 5. LIST_FORMAT: sequential lines, no clear table ──
    if has_table and n_cols <= 2 and n_rows >= 5:
        log.debug("Table type: LIST_FORMAT (%d cols, %d rows)", n_cols, n_rows)
        return TableType.LIST_FORMAT, 0.60

    # Also check text for numbered list pattern without table
    if not has_table or n_rows < 2:
        numbered = re.findall(r"^\s*\d+[\.\)]\s+[A-ZА-ЯЁ]", page_text, re.MULTILINE)
        if len(numbered) >= 3:
            return TableType.LIST_FORMAT, 0.55

    # ── 6. UNKNOWN ──
    return TableType.UNKNOWN, 0.0


def detect_table_type_for_page(
    tables: list[ExtractedTable],
    page_text: str,
) -> tuple[TableType, float, Optional[ExtractedTable]]:
    """Detect table type for a page, trying all available tables.

    Returns:
        (TableType, confidence, best_table) — best_table may be None for CURVE/LIST
    """
    best_type = TableType.UNKNOWN
    best_conf = 0.0
    best_table = None

    # Try each extracted table
    for table in tables:
        ttype, conf = detect_table_type(table=table, page_text=page_text)
        if conf > best_conf:
            best_type = ttype
            best_conf = conf
            best_table = table

    # If no table gave result, try text-only detection
    if best_conf < 0.5:
        ttype, conf = detect_table_type(page_text=page_text)
        if conf > best_conf:
            best_type = ttype
            best_conf = conf
            best_table = None

    return best_type, best_conf, best_table
