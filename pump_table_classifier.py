#!/usr/bin/env python3
"""
Pump Table Classifier — Automatic column identification for pump catalog tables.

Supports 6 languages (RU/EN/DE/IT/FR/ES).
5-stage classification: header keywords → unit line → value inference → disambiguation → transpose.

Part of Universal Pump Parser v10.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any

from pump_validators import (
    parse_range_value, convert_q, convert_h, convert_p, calculate_efficiency,
)

log = logging.getLogger("pump_classifier")

# ─── Column Patterns (6 languages) ────────────────────────────────────────────

COLUMN_PATTERNS: Dict[str, Dict] = {
    "model": {
        "header_keywords": {
            "ru": ["модель", "тип", "насос", "наименование", "обозначение", "марка"],
            "en": ["model", "type", "pump", "designation", "name", "item"],
            "de": ["modell", "typ", "pumpe", "bezeichnung", "baureihe"],
            "it": ["modello", "tipo", "pompa", "denominazione"],
            "fr": ["modèle", "type", "pompe", "désignation"],
            "es": ["modelo", "tipo", "bomba", "denominación"],
        },
        "positive_patterns": [
            r"[A-ZА-ЯЁ]{2,}[\s\-]?\d+",            # CDM 32-5, КМГ 40
            r"[A-Z]{1,5}\d{1,3}[\-/]\d{1,3}",        # CR32-5, NK40/250
            r"[A-Z]{2,}\s+\d+[\-/]\d+[\-/]\d+",      # INL 32-12.5-18-1.1/2
        ],
        "negative_patterns": [
            r"^\d{6,10}$",                             # article number
            r"^[123]~\s?\d{3}\s?V",                    # voltage
            r"^DN\s?\d+$",                              # flange
            r"^IE[1-5]$",                               # energy class
            r"^IP\s?\d{2}$",                            # protection class
            r"^[A-F]$",                                 # insulation class
            r"^\d+\s*(кг|kg|мм|mm|°C)$",               # weight/dimension
            r"^(S1|S2|S3|S6)$",                         # duty mode
            r"^\d+(\.\d+)?\s*%$",                       # efficiency percent
        ],
    },
    "q_nom": {
        "header_keywords": {
            "ru": ["подача", "расход", "производительность", "объёмная подача"],
            "en": ["flow", "capacity", "delivery", "discharge", "flow rate"],
            "de": ["förderstrom", "fördermenge", "volumenstrom", "durchfluss"],
            "it": ["portata", "portata nominale"],
            "fr": ["débit", "débit nominal"],
            "es": ["caudal", "caudal nominal"],
        },
        "symbol": "Q",
        "value_range": [0.05, 50000],
    },
    "h_nom": {
        "header_keywords": {
            "ru": ["напор", "высота подачи", "давление нагнетания"],
            "en": ["head", "total head", "delivery head", "discharge head"],
            "de": ["förderhöhe", "druckhöhe", "gesamtförderhöhe"],
            "it": ["prevalenza", "altezza manometrica"],
            "fr": ["hauteur", "hauteur manométrique"],
            "es": ["altura", "altura manométrica"],
        },
        "symbol": "H",
        "value_range": [0.3, 3000],
    },
    "power_kw": {
        "header_keywords": {
            "ru": ["мощность", "мощн", "потребляемая мощность"],
            "en": ["power", "motor power", "rated power", "shaft power", "input power"],
            "de": ["leistung", "motorleistung", "nennleistung", "wellenleistung"],
            "it": ["potenza", "potenza motore", "potenza nominale"],
            "fr": ["puissance", "puissance moteur"],
            "es": ["potencia", "potencia motor"],
        },
        "symbol": "P",
        "value_range": [0.003, 2000],
    },
    "rpm": {
        "header_keywords": {
            "ru": ["обороты", "частота вращения", "скорость"],
            "en": ["speed", "rpm", "rotational speed", "rev/min"],
            "de": ["drehzahl", "nenndrehzahl"],
            "it": ["velocità", "giri/min"],
            "fr": ["vitesse", "tours/min"],
            "es": ["velocidad", "rpm"],
        },
        "valid_values": {960, 980, 1000, 1450, 1475, 1500, 1750, 2900, 2950, 3000, 3450, 3500, 3600},
        "value_range": [400, 7200],
    },
    "article": {
        "header_keywords": {
            "ru": ["артикул", "код", "каталожный номер", "номер заказа"],
            "en": ["article", "code", "part number", "order number", "catalog number", "item no"],
            "de": ["artikelnummer", "bestellnummer", "katalognummer"],
            "it": ["codice", "articolo", "numero d'ordine"],
        },
        "value_pattern": r"^\d{5,12}$",
    },
    "dn": {
        "header_keywords": {
            "ru": ["dn", "ду", "патрубок", "присоединение", "фланец"],
            "en": ["dn", "flange", "connection", "port size", "inlet", "outlet", "suction", "discharge"],
            "de": ["anschluss", "nennweite", "saugstutzen", "druckstutzen"],
        },
        "value_range": [10, 800],
    },
    "weight": {
        "header_keywords": {
            "ru": ["масса", "вес"],
            "en": ["weight", "mass", "net weight"],
            "de": ["gewicht", "masse"],
            "it": ["peso", "massa"],
        },
        "value_range": [0.5, 10000],
    },
    "npsh": {
        "header_keywords": {
            "ru": ["npsh", "кавитационный запас"],
            "en": ["npsh", "npshr", "npsh required"],
            "de": ["npsh", "haltedruckhöhe"],
        },
        "value_range": [0.1, 30],
    },
    "efficiency": {
        "header_keywords": {
            "ru": ["кпд", "эффективность"],
            "en": ["efficiency", "eta", "η"],
            "de": ["wirkungsgrad"],
            "it": ["rendimento"],
        },
        "value_range": [5, 96],
    },
    "stages": {
        "header_keywords": {
            "all": ["stages", "ступени", "stufen", "stadi"],
        },
        "value_range": [1, 100],
    },
}

# All Q/H/P unit keywords for detection in headers
ALL_UNIT_KEYWORDS = {
    "м³/ч", "m³/h", "m3/h", "m^3/h", "куб.м/ч",
    "м3/час", "м3 /час", "m3/час",  # PDF extraction variants
    "л/с", "l/s", "л/мин", "l/min", "gpm",
    "м", "m", "ft", "feet", "бар", "bar", "кпа", "kpa", "мпа", "mpa", "psi",
    "квт", "kw", "вт", "w", "hp", "л.с.", "cv", "ps",
    "об/мин", "rpm", "rev/min", "min⁻¹",
    "кг", "kg",
}

# NPSH / dimensional table negative keywords
NEGATIVE_TABLE_KEYWORDS = {"npsh", "кавитац", "габарит", "dimension", "abmessung",
                           "installation", "установк", "монтаж", "spare", "запчаст",
                           "accessories", "принадлежност", "zubehör"}

# Dimensional column headers (if table has these → likely NOT pump performance table)
DIMENSION_COLUMN_KEYWORDS = {"b", "l", "d", "a", "h1", "h2", "b1", "b2", "l1", "l2",
                             "d1", "d2", "a1", "a2"}


def _is_numeric(s: str) -> bool:
    """Check if a string can be parsed as a float."""
    try:
        s = str(s).strip().replace(',', '.')
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _parse_float(s: str) -> Optional[float]:
    """Parse float from string, handling commas and spaces."""
    if not s:
        return None
    s = str(s).strip().replace(',', '.').replace('\xa0', '').replace(' ', '')
    s = re.sub(r'\.$', '', s)
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _extract_unit_from_header(header: str) -> Optional[str]:
    """Extract measurement unit from header like 'Q (м³/ч)' or 'H, m'."""
    h = header.replace('\n', ' ').replace('\r', ' ').lower().strip()
    h = re.sub(r'\s+', ' ', h)

    def _find_best_unit(text: str) -> Optional[str]:
        """Find the longest matching unit in text (longer = more specific)."""
        best = None
        best_len = 0
        for unit in ALL_UNIT_KEYWORDS:
            u_lower = unit.lower()
            if u_lower in text and len(u_lower) > best_len:
                best = unit
                best_len = len(u_lower)
        return best

    # Check for unit in parentheses: "Q (м³/ч)" or "H [m]"
    m = re.search(r'[(\[](.*?)[)\]]', h)
    if m:
        found = _find_best_unit(m.group(1).strip())
        if found:
            return found
    # Check for unit after comma: "H, m" or "Q, m³/h"
    m = re.search(r',\s*(.+)$', h)
    if m:
        found = _find_best_unit(m.group(1).strip())
        if found:
            return found
    return None


# ─── Stage 1: Header Keyword Match ────────────────────────────────────────────

def _match_header_keywords(header: str) -> Optional[Tuple[str, int]]:
    """Match column header to field name using keywords.

    Returns (field_name, score) or None.
    """
    # Normalize newlines → spaces (PDF extraction breaks words across lines:
    # "Номинальная\nпроизводитель\nность" → "Номинальная производительность")
    h = header.replace('\n', ' ').replace('\r', ' ').lower().strip()
    # Collapse multiple spaces
    h = re.sub(r'\s+', ' ', h)
    if not h:
        return None

    # Remove units in parentheses for cleaner matching
    h_clean = re.sub(r'\s*[(\[].*?[)\]]\s*', ' ', h).strip()
    # Also create version with newlines simply removed (no space),
    # for cases where PDF breaks a word mid-syllable:
    # "производитель\nность" → "производительность" (not "производитель ность")
    h_joined = header.replace('\n', '').replace('\r', '').lower().strip()
    h_joined = re.sub(r'\s+', ' ', h_joined)
    h_joined_clean = re.sub(r'\s*[(\[].*?[)\]]\s*', ' ', h_joined).strip()

    best_field = None
    best_score = 0

    for field, info in COLUMN_PATTERNS.items():
        keywords = info.get("header_keywords", {})
        for lang, kw_list in keywords.items():
            for kw in kw_list:
                if kw.lower() in h_clean or kw.lower() in h_joined_clean:
                    score = len(kw)  # longer match = higher score
                    if score > best_score:
                        best_score = score
                        best_field = field

        # Also check symbol: "Q" → q_nom, "H" → h_nom, "P" → power_kw
        symbol = info.get("symbol", "")
        if symbol and re.match(rf'^{re.escape(symbol)}\b', h_clean, re.I):
            score = 5
            if score > best_score:
                best_score = score
                best_field = field

    if best_field:
        return (best_field, best_score)
    return None


# ─── Stage 3: Value-Based Inference ────────────────────────────────────────────

def _infer_from_values(values: List[str], header: str) -> Optional[str]:
    """Infer column type from cell values when header doesn't match."""
    if not values:
        return None

    # Count numeric vs string values
    numeric_vals = []
    string_vals = []
    for v in values:
        v_str = str(v).strip()
        if not v_str:
            continue
        f = _parse_float(v_str)
        if f is not None:
            numeric_vals.append(f)
        else:
            string_vals.append(v_str)

    total = len(numeric_vals) + len(string_vals)
    if total == 0:
        return None

    numeric_ratio = len(numeric_vals) / total

    # Model detection: mostly strings matching pump model patterns
    if numeric_ratio < 0.5 and len(string_vals) >= 2:
        model_positive = COLUMN_PATTERNS["model"]["positive_patterns"]
        model_negative = COLUMN_PATTERNS["model"]["negative_patterns"]
        pos_count = sum(1 for s in string_vals
                        if any(re.search(p, s) for p in model_positive))
        neg_count = sum(1 for s in string_vals
                        if any(re.match(p, s) for p in model_negative))
        if pos_count > len(string_vals) * 0.3 and neg_count < len(string_vals) * 0.5:
            return "model"

    # Article detection: all 5-12 digit numbers
    if numeric_ratio > 0.8 and len(numeric_vals) >= 2:
        article_count = sum(1 for v in values
                            if re.match(r'^\d{5,12}$', str(v).strip()))
        if article_count > len(values) * 0.7:
            return "article"

    # RPM detection: mostly common RPM values (≥80% must match to avoid false positives)
    if numeric_ratio > 0.7 and len(numeric_vals) >= 2:
        rpm_vals = COLUMN_PATTERNS["rpm"].get("valid_values", set())
        # Only count exact integers (not 2900.5 etc.)
        rpm_count = sum(1 for v in numeric_vals
                        if v == v and v == int(v) and int(v) in rpm_vals)
        if rpm_count > len(numeric_vals) * 0.8:
            return "rpm"

    return None


# ─── Stage 4: Disambiguation ──────────────────────────────────────────────────

def _is_dimension_table(classified_columns: Dict[str, str],
                        all_headers: List[str]) -> bool:
    """Check if this table is a dimensional/installation table, not performance."""
    headers_lower = {h.lower().strip() for h in all_headers}

    # If table title contains negative keywords
    # (caller should check this separately for page-level text)

    # If many dimension-like column headers present
    dim_count = sum(1 for h in headers_lower if h in DIMENSION_COLUMN_KEYWORDS)
    if dim_count >= 3:
        return True

    return False


def _is_npsh_table(all_headers: List[str], page_text: str = "") -> bool:
    """Check if this is an NPSH table (looks like pump table but isn't performance)."""
    combined = " ".join(all_headers).lower() + " " + page_text[:500].lower()
    return any(kw in combined for kw in NEGATIVE_TABLE_KEYWORDS)


def _disambiguate_h(classified: Dict[int, str], all_headers: List[str]) -> Dict[int, str]:
    """Resolve H=Head vs H=Height (габарит) ambiguity."""
    # If "h_nom" is classified AND dimension columns exist → probably Height not Head
    if "h_nom" not in classified.values():
        return classified

    # Check if the h_nom header explicitly says millimeters (dimensions, not head)
    h_nom_col = next((col for col, f in classified.items() if f == "h_nom"), None)
    if h_nom_col is not None and h_nom_col < len(all_headers):
        h_header = all_headers[h_nom_col].lower()
        if "mm" in h_header or "мм" in h_header:
            # H in mm is definitely a dimension, not head
            result = {col: f for col, f in classified.items() if f != "h_nom"}
            return result

    headers_lower = {h.lower().strip() for h in all_headers}
    dim_cols = sum(1 for h in headers_lower if h in DIMENSION_COLUMN_KEYWORDS)
    if dim_cols >= 2:
        # Remove h_nom classification — this is a dimension table
        result = {}
        for col_idx, field in classified.items():
            if field != "h_nom":
                result[col_idx] = field
        return result

    return classified


# ─── Stage 5: Transpose Detection ─────────────────────────────────────────────

def detect_transposed(headers: List[str], first_col_values: List[str]) -> bool:
    """Detect if table is transposed (parameters in rows, models in columns).

    Signs of transposed table:
    - First column contains parameter names (Модель, Мощность, Напор, ...)
    - Other columns contain numeric values
    """
    if not first_col_values:
        return False

    # Check if first column values match known parameter names
    param_keywords = set()
    for field_info in COLUMN_PATTERNS.values():
        for lang_kws in field_info.get("header_keywords", {}).values():
            param_keywords.update(kw.lower() for kw in lang_kws)

    match_count = sum(1 for v in first_col_values
                      if any(kw in str(v).lower() for kw in param_keywords))

    return match_count >= 2 and match_count > len(first_col_values) * 0.3


# ─── Selection Chart Detection ─────────────────────────────────────────────────

def detect_selection_chart(headers: List[str], index_values: List[str],
                           cell_values: List[List[str]]) -> bool:
    """Detect selection chart: rows=H, columns=Q, cells=model names.

    Selection chart has:
    - Row headers: all numeric (H values, descending)
    - Column headers: all numeric (Q values, ascending)
    - Cells: model names or empty
    """
    # Check column headers: mostly numeric
    numeric_headers = sum(1 for h in headers if _is_numeric(str(h)))
    if len(headers) < 3 or numeric_headers < len(headers) * 0.7:
        return False

    # Check row index: mostly numeric
    numeric_index = sum(1 for v in index_values if _is_numeric(str(v)))
    if len(index_values) < 3 or numeric_index < len(index_values) * 0.7:
        return False

    # Check cells: mostly strings or empty (not mostly numbers)
    total_cells = 0
    string_cells = 0
    empty_cells = 0
    for row in cell_values:
        for cell in row:
            total_cells += 1
            cell_str = str(cell).strip()
            if not cell_str or cell_str == "None" or cell_str == "nan":
                empty_cells += 1
            elif not _is_numeric(cell_str):
                string_cells += 1

    if total_cells == 0:
        return False

    # Selection chart: cells are model names (strings) or empty, not numbers
    return (string_cells + empty_cells) > total_cells * 0.5


# ─── Main Classifier ──────────────────────────────────────────────────────────

def classify_columns(headers: List[str],
                     data_rows: List[List[str]],
                     page_text: str = "") -> Dict[str, Any]:
    """Classify table columns to pump data fields.

    5-stage algorithm:
    1. Header keyword match
    2. Unit line check
    3. Value-based inference
    4. Disambiguation
    5. Transpose detection

    Args:
        headers: column header strings
        data_rows: list of rows, each row is list of cell strings
        page_text: optional page text for context

    Returns:
        {
            "columns": {col_idx: field_name, ...},
            "units": {field_name: unit_string, ...},
            "is_pump_table": bool,
            "is_transposed": bool,
            "is_selection_chart": bool,
            "data_rows": int,
            "numeric_ratio": float,
            "warnings": [str, ...],
        }
    """
    result = {
        "columns": {},
        "units": {},
        "is_pump_table": False,
        "is_transposed": False,
        "is_selection_chart": False,
        "data_rows": len(data_rows),
        "numeric_ratio": 0.0,
        "warnings": [],
    }

    if not headers or not data_rows:
        return result

    # ── Stage 1: Header keyword match ──
    col_assignments: Dict[int, Tuple[str, int]] = {}  # col_idx → (field, score)

    for col_idx, header in enumerate(headers):
        match = _match_header_keywords(header)
        if match:
            field, score = match
            col_assignments[col_idx] = (field, score)

        # Extract unit from header
        unit = _extract_unit_from_header(header)
        if unit:
            if match:
                result["units"][match[0]] = unit

    # ── Stage 2: Unit line check (row 0 or 1 might be units) ──
    if data_rows and len(data_rows) >= 1:
        first_row = data_rows[0]
        for col_idx, cell in enumerate(first_row):
            cell_str = str(cell).strip().lower()
            if cell_str in ALL_UNIT_KEYWORDS:
                # This row is a unit row, not data
                if col_idx in col_assignments:
                    field = col_assignments[col_idx][0]
                    result["units"][field] = cell_str

    # ── Stage 3: Value-based inference (for unclassified columns) ──
    classified_cols = {idx for idx in col_assignments}
    for col_idx in range(len(headers)):
        if col_idx in classified_cols:
            continue
        col_values = [row[col_idx] if col_idx < len(row) else ""
                      for row in data_rows]
        inferred = _infer_from_values(col_values, headers[col_idx])
        if inferred:
            col_assignments[col_idx] = (inferred, 1)

    # ── Stage 4: Disambiguation ──
    # Resolve duplicate field assignments (keep highest score)
    field_best: Dict[str, Tuple[int, int]] = {}  # field → (col_idx, score)
    for col_idx, (field, score) in col_assignments.items():
        if field not in field_best or score > field_best[field][1]:
            field_best[field] = (col_idx, score)

    classified = {col_idx: field for field, (col_idx, _) in field_best.items()}

    # Check for dimension table
    if _is_dimension_table(classified, headers):
        result["warnings"].append("Dimension table detected, skipping")
        return result

    # Check for NPSH table
    if _is_npsh_table(headers, page_text):
        result["warnings"].append("NPSH/accessory table detected, skipping")
        return result

    # H (Head) vs H (Height) disambiguation
    classified = _disambiguate_h(classified, headers)

    # P1 vs P2 check
    for col_idx, field in classified.items():
        if field == "power_kw":
            h = headers[col_idx].lower() if col_idx < len(headers) else ""
            if "p1" in h or "input" in h or "потребляемая" in h:
                result["warnings"].append(f"Column {col_idx} is P1 (input power), not P2 (shaft power)")
    # Also check original headers for P1 even if column was classified differently
    for col_idx, header in enumerate(headers):
        h = header.lower()
        if ("p1" in h or "input power" in h) and col_idx not in classified:
            result["warnings"].append(f"Header '{header}' suggests P1 (input power)")

    # ── Stage 5: Transpose detection ──
    first_col_vals = [row[0] if row else "" for row in data_rows]
    if detect_transposed(headers, first_col_vals):
        result["is_transposed"] = True
        result["warnings"].append("Transposed table detected")

    # Selection chart detection
    index_vals = first_col_vals
    cell_vals = [row[1:] if len(row) > 1 else [] for row in data_rows]
    if detect_selection_chart(headers[1:] if len(headers) > 1 else [],
                              index_vals, cell_vals):
        result["is_selection_chart"] = True

    # Build final result
    result["columns"] = {field: col_idx for col_idx, field in classified.items()}

    # Calculate numeric ratio
    total_cells = 0
    numeric_cells = 0
    for row in data_rows:
        for cell in row:
            total_cells += 1
            if _is_numeric(str(cell)):
                numeric_cells += 1
    result["numeric_ratio"] = numeric_cells / total_cells if total_cells > 0 else 0.0

    # Determine if this is a pump table
    result["is_pump_table"] = is_pump_table(result)

    return result


def is_pump_table(classified: Dict[str, Any]) -> bool:
    """Minimum criteria for 'this is a pump performance table'."""
    columns = classified.get("columns", {})
    n_rows = classified.get("data_rows", 0)
    numeric_ratio = classified.get("numeric_ratio", 0.0)

    has_model = "model" in columns
    has_q = "q_nom" in columns
    has_h = "h_nom" in columns

    # Must have: model + at least Q or H
    if not has_model:
        return False
    if not (has_q or has_h):
        return False
    # At least 2 data rows
    if n_rows < 2:
        return False
    # At least 30% numeric cells
    if numeric_ratio < 0.3:
        return False
    return True


# ─── DataFrame-to-Pumps Converter ─────────────────────────────────────────────

def dataframe_to_pump_dicts(classified: Dict[str, Any],
                            headers: List[str],
                            data_rows: List[List[str]]) -> List[Dict[str, Any]]:
    """Convert classified table data to pump dictionaries.

    Returns list of dicts with keys: model, q_nom, h_nom, power_kw, rpm, article, etc.
    """
    columns = classified.get("columns", {})
    units = classified.get("units", {})
    pumps = []

    for row in data_rows:
        pump = {}
        valid = True

        for field, col_idx in columns.items():
            if col_idx >= len(row):
                continue
            cell = str(row[col_idx]).strip()
            if not cell or cell.lower() in ("none", "nan", "-", "–", "—"):
                continue

            if field == "model":
                pump["model"] = cell
            elif field == "article":
                pump["article"] = cell
            elif field in ("q_nom", "h_nom", "power_kw", "weight", "npsh", "efficiency"):
                # Handle range values
                if re.search(r'[\d.,]+\s*[-–—]\s*[\d.,]+', cell):
                    vmin, vmax = parse_range_value(cell)
                    if field == "q_nom":
                        pump["q_min"] = vmin
                        pump["q_max"] = vmax
                        pump["q_nom"] = (vmin + vmax) / 2
                    elif field == "h_nom":
                        pump["h_min"] = vmin
                        pump["h_max"] = vmax
                        pump["h_nom"] = (vmin + vmax) / 2
                    else:
                        pump[field] = (vmin + vmax) / 2
                else:
                    v = _parse_float(cell)
                    if v is not None:
                        pump[field] = v
                    else:
                        valid = False
            elif field == "rpm":
                v = _parse_float(cell)
                if v is not None:
                    pump["rpm"] = int(v)
            elif field == "dn":
                v = _parse_float(cell)
                if v is not None:
                    pump["dn"] = int(v)
            elif field == "stages":
                v = _parse_float(cell)
                if v is not None:
                    pump["stages"] = int(v)

        # Must have at least model
        if "model" not in pump:
            continue

        # Apply unit conversions
        if "q_nom" in pump and "q_nom" in units:
            pump["q_nom"] = convert_q(pump["q_nom"], units["q_nom"])
        if "h_nom" in pump and "h_nom" in units:
            pump["h_nom"] = convert_h(pump["h_nom"], units["h_nom"])
        if "power_kw" in pump and "power_kw" in units:
            pump["power_kw"] = convert_p(pump["power_kw"], units["power_kw"])

        pumps.append(pump)

    return pumps


# ─── Inter-column Physics Check ────────────────────────────────────────────────

def score_column_assignment(data_rows: List[List[str]],
                            q_col: int, h_col: int, p_col: int) -> float:
    """Score a Q/H/P column assignment by calculating mean η.

    Returns distance from ideal η range [0.5, 0.7].
    Lower score = better assignment.
    """
    etas = []
    for row in data_rows:
        try:
            q = _parse_float(str(row[q_col])) if q_col < len(row) else None
            h = _parse_float(str(row[h_col])) if h_col < len(row) else None
            p = _parse_float(str(row[p_col])) if p_col < len(row) else None
            if q and h and p and q > 0 and h > 0 and p > 0:
                eta = calculate_efficiency(q, h, p)
                if eta is not None and 0.01 < eta < 2.0:
                    etas.append(eta)
        except (IndexError, TypeError, ValueError) as exc:
            log.debug(f"score_column_assignment row error: {exc}")
            continue

    if len(etas) < 2:
        return 999.0  # not enough data

    mean_eta = sum(etas) / len(etas)
    # Ideal range: 0.5-0.7
    if 0.5 <= mean_eta <= 0.7:
        return 0.0
    return abs(mean_eta - 0.6)  # distance from center of ideal range


# ─── Q-H Performance Matrix Detection & Parsing ─────────────────────────────

def detect_qh_matrix(headers: List[str], data_rows: List[List[str]]) -> Optional[Dict[str, Any]]:
    """Detect if table is a Q-H performance matrix.

    Common pump catalog format where:
    - Left columns: model name(s) + power (kW / HP)
    - Column headers from some point onwards: Q values (flow rates)
    - Cell values: H (head) at each Q point
    - Optionally row 0 has unit conversion row (l/min under m³/h)

    Example (Calpeda NM/NMS):
        B-NM          NM          P2/kW  HP  Q/m³/h  6.6   7.5   8.4   ...
                                              l/min   110   125   140   ...
        B-NM 32/12F   NM 32/12FE  0.55  0.75  H/m   12.5  12.5  12.0  ...

    Returns dict with matrix info if detected, None otherwise.
    """
    if len(headers) < 6 or len(data_rows) < 2:
        return None

    # Find where numeric Q-value columns start
    # Look for the first column header that is a plain number
    q_start_col = None
    q_values = []

    for col_idx in range(len(headers)):
        h = str(headers[col_idx]).replace(',', '.').replace('\n', ' ').strip()
        # Remove unit text like "m³/h", "Q", etc — we want pure numbers
        try:
            val = float(h)
            if 0.1 <= val <= 50000:  # reasonable Q range
                if q_start_col is None:
                    q_start_col = col_idx
                q_values.append((col_idx, val))
        except (ValueError, TypeError):
            if q_start_col is not None:
                # Gap in numeric headers — stop
                break

    # Need at least 4 consecutive numeric column headers
    num_count = len(q_values)
    if num_count < 4:
        # Bug 6 FIX: Headers may be auto-generated (Col0, Col1...) by PyMuPDF.
        # Try scanning first few data rows for Q values (FST format).
        # FST tables have 5+ label columns (MODEL, DN, Power...) before Q data starts.
        if num_count == 0 and len(data_rows) >= 3:
            # Collect ALL candidate Q rows (rows with 5+ ascending numerics)
            q_row_candidates = []  # [(scan_idx, run_start_col, run_vals, unit)]
            for scan_idx, scan_row in enumerate(data_rows[:5]):
                # Find longest run of consecutive ascending numeric values
                best_run_start = -1
                best_run_vals = []
                current_run_start = -1
                current_run_vals = []
                for col_idx in range(len(scan_row)):
                    cell = str(scan_row[col_idx]).replace(',', '.').strip()
                    if cell.lower() in ('nan', 'none', ''):
                        if len(current_run_vals) > len(best_run_vals):
                            best_run_start = current_run_start
                            best_run_vals = current_run_vals
                        current_run_vals = []
                        current_run_start = -1
                        continue
                    m = re.search(r'([\d.]+)\s*$', cell)
                    if m:
                        try:
                            v = float(m.group(1))
                            if 0 <= v <= 50000:
                                if current_run_start < 0:
                                    current_run_start = col_idx
                                current_run_vals.append(v)
                                continue
                        except (ValueError, TypeError):
                            pass
                    if len(current_run_vals) > len(best_run_vals):
                        best_run_start = current_run_start
                        best_run_vals = current_run_vals
                    current_run_vals = []
                    current_run_start = -1
                if len(current_run_vals) > len(best_run_vals):
                    best_run_start = current_run_start
                    best_run_vals = current_run_vals

                if len(best_run_vals) >= 5 and best_run_start >= 0:
                    is_ascending = all(best_run_vals[i] <= best_run_vals[i+1]
                                       for i in range(len(best_run_vals)-1))
                    if is_ascending:
                        # Detect unit for this row
                        row_text = ' '.join(str(c) for c in scan_row).lower()
                        unit = "unknown"
                        if 'm³/h' in row_text or 'м³/ч' in row_text or 'm3/h' in row_text or 'м3/ч' in row_text:
                            unit = "m3/h"
                        elif 'l/min' in row_text or 'л/мин' in row_text:
                            unit = "l/min"
                        elif 'l/s' in row_text or 'л/с' in row_text:
                            unit = "l/s"
                        elif 'gpm' in row_text:
                            unit = "gpm"
                        q_row_candidates.append((scan_idx, best_run_start, best_run_vals, unit))

            # Choose best Q row: prefer m³/h, then l/min, then last candidate
            if q_row_candidates:
                chosen = q_row_candidates[-1]  # default: last (closest to data)
                for cand in q_row_candidates:
                    if cand[3] == "m3/h":
                        chosen = cand
                        break
                scan_idx, best_run_start, best_run_vals, q_unit_found = chosen
                if q_unit_found == "unknown":
                    q_unit_found = "m3/h"
                # All rows up to and including the chosen Q row are skip rows
                skip_rows = list(range(scan_idx + 1))
                data_q_values = [(best_run_start + ci, qv)
                                 for ci, qv in enumerate(best_run_vals)]
                # Detect model/power columns (left of Q start)
                model_cols = [0]
                power_col = None
                hp_col = None
                for ci in range(best_run_start):
                    h_lower = str(headers[ci]).lower() if ci < len(headers) else ""
                    if any(kw in h_lower for kw in ('power', 'мощност', 'puissance',
                                                     'potencia', 'leistung', 'potenza')):
                        power_col = ci
                    elif 'hp' in h_lower or 'л.с' in h_lower:
                        hp_col = ci
                return {
                    "model_cols": model_cols,
                    "power_col": power_col,
                    "hp_col": hp_col,
                    "q_label_col": None,
                    "q_start_col": best_run_start,
                    "q_values": data_q_values,
                    "q_unit": q_unit_found,
                    "p_unit": "kw",
                    "unit_row_idx": None,
                    "skip_rows": skip_rows,
                }
        return None

    # Verify they're consecutive columns
    first_col = q_values[0][0]
    if not all(q_values[i][0] == first_col + i for i in range(len(q_values))):
        return None

    # Find model column(s) — leftmost non-numeric columns before Q values
    model_cols = []
    power_col = None
    hp_col = None
    q_label_col = None

    for col_idx in range(q_start_col):
        h = str(headers[col_idx]).lower().replace('\n', ' ').strip()
        # Detect specific column types
        if re.search(r'\bp2\b|\bpower\b|\bмощн|\bleistung|\bpotenza|\bpuissance', h):
            power_col = col_idx
        elif re.search(r'\bhp\b|\bл\.?\s*с\.?\b|\bcv\b', h):
            hp_col = col_idx
        elif re.search(r'\bq\b', h) and ('m' in h or '³' in h or 'l' in h):
            q_label_col = col_idx  # This is just a label column, not data
        elif re.search(r'^col\d+$', h):
            # Generic column — might be label column, check data
            pass
        elif not _is_numeric(h):
            model_cols.append(col_idx)

    # Need at least one model column
    if not model_cols:
        return None

    # Check that data rows have model names in model columns and numbers in Q columns
    model_count = 0
    numeric_data_count = 0
    skip_rows = []  # Bug 8 FIX: multiple unit/label rows (was single unit_row_idx)

    for row_idx, row in enumerate(data_rows):
        # Check for unit/label rows (l/min, m³/h, H=Head, etc.)
        row_text = ' '.join(str(c) for c in row).lower()
        if row_idx < 5 and re.search(
                r'l/min|л/мин|gpm|l/s|л/с|m³/h|м³/ч|m3/h|'
                r'head|напор|hauteur|prevalenza|förderhöhe|'
                r'h\s*=|h\s*\[|altitude|altura', row_text, re.I):
            skip_rows.append(row_idx)
            continue

        # Check for H label in a cell (e.g., "H\nm" or "H (m)")
        has_h_label = False
        for c in row[:q_start_col]:
            if re.search(r'\bH\b', str(c)):
                has_h_label = True

        # Check model name presence
        for mc in model_cols:
            if mc < len(row):
                val = str(row[mc]).strip()
                if val and val.lower() not in ('none', 'nan', '') and len(val) > 2:
                    model_count += 1
                    break

        # Check numeric data in Q columns
        for qc, _ in q_values:
            if qc < len(row):
                if _is_numeric(str(row[qc])):
                    numeric_data_count += 1
                    break

    # At least 2 rows with model names and numeric data
    if model_count < 2 or numeric_data_count < 2:
        return None

    # Determine Q unit from header context
    q_unit = "m3/h"  # default
    for col_idx in range(q_start_col):
        h = str(headers[col_idx]).lower().replace('\n', ' ')
        if 'l/min' in h or 'л/мин' in h:
            q_unit = "l/min"
        elif 'l/s' in h or 'л/с' in h:
            q_unit = "l/s"
        elif 'gpm' in h:
            q_unit = "gpm"
        elif 'm3/h' in h or 'm³/h' in h or 'м³/ч' in h:
            q_unit = "m3/h"

    # Check for kW in power header
    p_unit = "kw"
    if power_col is not None:
        h = str(headers[power_col]).lower().replace('\n', ' ')
        if 'w' in h and 'kw' not in h:
            p_unit = "w"

    return {
        "model_cols": model_cols,
        "power_col": power_col,
        "hp_col": hp_col,
        "q_label_col": q_label_col,
        "q_start_col": q_start_col,
        "q_values": q_values,  # list of (col_idx, q_value)
        "q_unit": q_unit,
        "p_unit": p_unit,
        "unit_row_idx": skip_rows[0] if skip_rows else None,  # compat
        "skip_rows": skip_rows,  # Bug 8 FIX: full list
    }


def parse_qh_matrix(headers: List[str], data_rows: List[List[str]],
                     matrix_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse a Q-H performance matrix into pump entries.

    Each data row becomes a pump with full Q-H curve.
    """
    model_cols = matrix_info["model_cols"]
    power_col = matrix_info["power_col"]
    hp_col = matrix_info["hp_col"]
    q_values_raw = matrix_info["q_values"]  # (col_idx, q_val) list
    q_unit = matrix_info["q_unit"]
    p_unit = matrix_info["p_unit"]
    # Bug 8 FIX: use skip_rows set (was single unit_row_idx)
    skip_rows_set = set(matrix_info.get("skip_rows", []))
    unit_row_idx = matrix_info.get("unit_row_idx")
    if unit_row_idx is not None:
        skip_rows_set.add(unit_row_idx)

    # Convert Q values from header to m³/h
    q_points_m3h = [convert_q(qv, q_unit) for _, qv in q_values_raw]

    pumps = []

    for row_idx, row in enumerate(data_rows):
        # Skip unit/label rows (Bug 8 FIX: check full set)
        if row_idx in skip_rows_set:
            continue

        # Check if this is a real data row (has model name + at least some H values)
        model_parts = []
        for mc in model_cols:
            if mc < len(row):
                val = str(row[mc]).strip()
                if val and val.lower() not in ('none', 'nan', ''):
                    model_parts.append(val)

        if not model_parts:
            continue

        # Get model name — use the longest/most informative one
        model = max(model_parts, key=len)

        # Skip if model looks like a unit/label row (Bug 7 FIX: expanded checks)
        model_stripped = model.strip()
        is_label = (
            model_stripped.upper() in ('H', 'M', 'FT', 'BAR', 'KPA', 'PSI', 'L/S', 'M3/H')
            or re.match(r'^(H\s*[=\[\(（]|л/|l/|ft\b|bar\b|kpa\b|м\.|m³|m3/|'
                        r'gpm\b|GPM\b|head|напор|hauteur|prevalenza)',
                        model_stripped, re.I)
            or (len(model_stripped) <= 2 and not model_stripped.isdigit())
        )
        if is_label:
            continue

        # Get power
        power_kw = 0.0
        if power_col is not None and power_col < len(row):
            pv = _parse_float(str(row[power_col]))
            if pv is not None:
                power_kw = convert_p(pv, p_unit)

        # If no power column but HP column exists, use HP
        if power_kw <= 0 and hp_col is not None and hp_col < len(row):
            pv = _parse_float(str(row[hp_col]))
            if pv is not None:
                power_kw = pv * 0.7457  # HP → kW

        # Extract H values at each Q point
        h_points = []
        valid_q_points = []
        for (col_idx, _), q_m3h in zip(q_values_raw, q_points_m3h):
            if col_idx < len(row):
                hv = _parse_float(str(row[col_idx]))
                if hv is not None and hv > 0:
                    h_points.append(hv)
                    valid_q_points.append(q_m3h)

        # Need at least 2 Q-H points for a valid entry
        if len(h_points) < 2:
            continue

        # Determine nominal Q and H at Best Efficiency Point (BEP)
        # BEP ≈ 60-75% of Q_max for centrifugal pumps
        q_max = max(valid_q_points)
        h_max = max(h_points)
        bep_q_target = q_max * 0.65
        # Find index closest to 65% of Q_max
        bep_idx = min(range(len(valid_q_points)),
                      key=lambda i: abs(valid_q_points[i] - bep_q_target))
        q_nom = valid_q_points[bep_idx]
        h_nom = h_points[bep_idx]

        pump = {
            "model": model,
            "q_nom": round(q_nom, 2),
            "h_nom": round(h_nom, 2),
            "power_kw": round(power_kw, 3),
            "q_points": [round(q, 2) for q in valid_q_points],
            "h_points": [round(h, 2) for h in h_points],
            "q_max": round(q_max, 2),
            "h_max": round(h_max, 2),
        }
        pumps.append(pump)

    return pumps


# ─── Spec Table Detection & Parsing (BM/CV transposed format) ─────────────

SPEC_ROW_KEYWORDS = {
    "q_nom": ["подача", "расход", "flow", "delivery", "portata", "débit",
              "förderstrom", "caudal", "capacity"],
    "h_max": ["напор", "head", "prevalenza", "hauteur", "förderhöhe",
              "altura", "давлен", "pressure"],
    "power": ["мощност", "power", "potenza", "puissance", "leistung",
              "potencia"],
    "efficiency": ["кпд", "эффективн", "efficiency", "rendimento",
                   "rendement", "wirkungsgrad", "η"],
    "rpm": ["оборот", "скорость", "speed", "rpm", "velocità", "vitesse"],
    "q_range": ["диапазон", "range", "campo", "plage", "bereich"],
}

RE_UNIT_IN_BRACKETS = re.compile(r'\[([^\]]+)\]')


def detect_spec_table(headers: List[str], data_rows: List[List[str]],
                      page_text: str = "") -> Optional[Dict[str, Any]]:
    """Detect transposed spec table: parameters in rows, models in columns.

    BM/CV format example:
        Col0                        | ВМ(N) 1 | ВМ(N) 3 | ...
        Номинальная подача [м3/ч]  | 1,6     | 3       | ...
        Максимальный напор [бар]   | 22      | 24      | ...

    Returns dict with spec_table info if detected, None otherwise.
    """
    if len(data_rows) < 3 or len(headers) < 3:
        return None

    # Check first column of data rows for parameter keywords
    first_col_values = []
    for row in data_rows:
        if row:
            first_col_values.append(str(row[0]).strip())
        else:
            first_col_values.append("")

    param_matches = {}  # row_idx → param_key
    for row_idx, val in enumerate(first_col_values):
        val_lower = val.lower()
        if not val_lower:
            continue
        for param_key, keywords in SPEC_ROW_KEYWORDS.items():
            if any(kw in val_lower for kw in keywords):
                # Avoid duplicates: keep first match for each param_key
                if param_key not in param_matches.values():
                    param_matches[row_idx] = param_key
                break

    # Need at least 2 parameter matches to be a spec table
    if len(param_matches) < 2:
        return None

    # Must have at least q_nom or h_max
    param_types = set(param_matches.values())
    if not ({"q_nom", "h_max"} & param_types):
        return None

    # Find model columns: columns 1+ that have non-empty headers or first-row values
    # that look like model names (contain letters + optional digits)
    model_cols = {}  # col_idx → model_name
    for col_idx in range(1, len(headers)):
        h = str(headers[col_idx]).strip()
        # Check header for model name
        if h and not h.lower().startswith("col") and len(h) > 1:
            model_cols[col_idx] = h
            continue
        # Check first data row (sometimes header row IS a model row)
        if data_rows and col_idx < len(data_rows[0]):
            cell = str(data_rows[0][col_idx]).strip()
            if cell and re.search(r'[A-ZА-ЯЁa-zа-яё]', cell) and len(cell) > 1:
                if not _is_numeric(cell):
                    model_cols[col_idx] = cell

    if len(model_cols) < 2:
        return None

    # Verify: most cells in model columns at param rows are numeric
    numeric_count = 0
    total_count = 0
    for row_idx in param_matches:
        if row_idx >= len(data_rows):
            continue
        for col_idx in model_cols:
            if col_idx < len(data_rows[row_idx]):
                cell = str(data_rows[row_idx][col_idx]).replace(',', '.').strip()
                total_count += 1
                if _is_numeric(cell) or re.match(r'[\d.,]+\s*[-–]\s*[\d.,]+', cell):
                    numeric_count += 1

    if total_count == 0 or numeric_count / total_count < 0.4:
        return None

    # Extract units from parameter names
    param_units = {}
    for row_idx in param_matches:
        val = first_col_values[row_idx]
        m = RE_UNIT_IN_BRACKETS.search(val)
        if m:
            param_units[row_idx] = m.group(1).strip().lower()

    return {
        "type": "spec_table",
        "param_rows": param_matches,
        "param_units": param_units,
        "model_cols": model_cols,
    }


def parse_spec_table(headers: List[str], data_rows: List[List[str]],
                     spec_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse transposed spec table into pump dicts.

    Returns SERIES-LEVEL data. Model names are series names
    (e.g. "ВМ(N) 1") not individual models ("BM 1-2A").
    Caller must handle series→models mapping.
    """
    param_units = spec_info.get("param_units", {})
    pumps = []

    for col_idx, model_name in spec_info["model_cols"].items():
        model_clean = model_name.replace('\n', ' ').strip()
        pump = {"model": model_clean, "_is_series_data": True}

        for row_idx, param_key in spec_info["param_rows"].items():
            if row_idx >= len(data_rows) or col_idx >= len(data_rows[row_idx]):
                continue
            raw = str(data_rows[row_idx][col_idx]).replace(',', '.').strip()
            unit = param_units.get(row_idx, "")

            if param_key == "q_range":
                q_min, q_max = parse_range_value(raw)
                pump["q_min"] = q_min
                pump["q_max"] = q_max
                continue

            try:
                m = re.match(r'([\d.]+)', raw)
                if not m:
                    continue
                val = float(m.group(1))

                if param_key == "q_nom":
                    pump["q_nom"] = convert_q(val, unit) if unit else val
                elif param_key == "h_max":
                    # Store as h_max_series, NOT h_nom (Bug 1: series max != per-model H)
                    pump["h_max_series"] = convert_h(val, unit) if unit else val
                elif param_key == "power":
                    pump["power_kw"] = convert_p(val, unit) if unit else val
                elif param_key == "efficiency":
                    pump["efficiency"] = val
                elif param_key == "rpm":
                    pump["rpm"] = int(val)
            except (ValueError, TypeError):
                pass

        if pump.get("q_nom", 0) > 0 or pump.get("h_max_series", 0) > 0:
            pumps.append(pump)
    return pumps


# ─── Selection Chart Parsing ────────────────────────────────────────────────

def parse_selection_chart(headers: List[str],
                          data_rows: List[List[str]]) -> List[Dict[str, Any]]:
    """Parse selection chart → pump operating points.

    Selection chart: rows=H (descending), cols=Q (ascending), cells=model names.
    """
    q_values = []
    for h in headers[1:]:
        try:
            q_values.append(float(str(h).replace(',', '.')))
        except (ValueError, TypeError):
            q_values.append(None)

    model_positions: Dict[str, List[Tuple[float, float]]] = {}
    for row in data_rows:
        h_val = None
        try:
            h_val = float(str(row[0]).replace(',', '.'))
        except (ValueError, TypeError):
            continue
        for col_idx, cell in enumerate(row[1:]):
            cell_str = str(cell).strip()
            if cell_str and cell_str.lower() not in ('none', 'nan', '', '-', '–'):
                if col_idx < len(q_values) and q_values[col_idx]:
                    model_positions.setdefault(cell_str, []).append(
                        (q_values[col_idx], h_val))

    pumps = []
    for model, points in model_positions.items():
        qs = [p[0] for p in points]
        hs = [p[1] for p in points]
        pumps.append({
            "model": model,
            "q_nom": round(sum(qs) / len(qs), 2),
            "h_nom": round(sum(hs) / len(hs), 2),
        })
    return pumps
