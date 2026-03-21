"""Automatic unit detection and conversion for pump parameters.

Detects units from table headers and page text, converts to standard:
  Q → m³/h, H → m, P → kW
"""

import re
import logging
from typing import Optional

log = logging.getLogger("pump_parser.unit_detector")

# ─── Unit Patterns (regex per unit per parameter) ─────────────────────────────

UNIT_PATTERNS: dict[str, dict[str, list[str]]] = {
    "q": {
        "m3/h": [r"м[³3]\s*/\s*ч", r"m[³3]\s*/\s*h", r"m3\s*/\s*h", r"mc/h",
                  r"куб\.?\s*м/ч", r"cbm/h", r"м3\s*/?\s*час"],
        "l/min": [r"л\s*/\s*мин", r"l\s*/\s*min", r"lpm"],
        "l/s": [r"л\s*/\s*с", r"l\s*/\s*s"],
        "gpm": [r"gpm", r"gal\s*/\s*min", r"usgpm"],
    },
    "h": {
        "m": [r"м\.?\s*в\.?\s*ст\.?", r"mwc", r"m\.?\s*w\.?\s*c\.?",
              r"метр", r"\bм\b", r"\bm\b"],
        "ft": [r"\bft\b", r"feet", r"фут"],
        "bar": [r"бар", r"\bbar\b"],
        "kpa": [r"кпа", r"\bkpa\b"],
    },
    "p": {
        "kw": [r"квт", r"\bkw\b"],
        "hp": [r"л\.?\s*с\.?", r"\bhp\b", r"\bcv\b"],
        "w": [r"\bвт\b", r"\bwatt\b"],
    },
}

# ─── Conversion Factors (to standard units) ──────────────────────────────────

CONVERSION_FACTORS: dict[str, dict[str, float]] = {
    "q": {  # → m³/h
        "m3/h": 1.0,
        "l/min": 0.06,
        "l/s": 3.6,
        "gpm": 0.2271,
    },
    "h": {  # → m
        "m": 1.0,
        "ft": 0.3048,
        "bar": 10.197,
        "kpa": 0.10197,
    },
    "p": {  # → kW
        "kw": 1.0,
        "hp": 0.7457,
        "w": 0.001,
    },
}


def _find_unit(text: str, param: str) -> Optional[str]:
    """Find the best matching unit for a parameter in text.

    Args:
        text: text to search (header, page text fragment)
        param: "q", "h", or "p"

    Returns:
        Unit key (e.g. "m3/h", "ft", "hp") or None
    """
    text_lower = text.lower().strip()
    if not text_lower:
        return None

    patterns = UNIT_PATTERNS.get(param, {})
    for unit_key, regexes in patterns.items():
        for rx in regexes:
            if re.search(rx, text_lower):
                return unit_key
    return None


def detect_units(headers: list[str], page_text: str = "") -> dict[str, str]:
    """Detect units for Q, H, P from table headers and page text.

    Checks headers first (more specific), falls back to page text.

    Returns:
        {"q": "m3/h", "h": "m", "p": "kw"} — only detected params included
    """
    result: dict[str, str] = {}

    # Combine all headers into one string for scanning
    headers_text = " ".join(headers).lower()

    for param in ("q", "h", "p"):
        # 1. Check individual headers (look for unit in parentheses/brackets)
        for header in headers:
            h = header.lower()
            # Check parenthesized units: "Q (м³/ч)" or "H [m]"
            m = re.search(r'[(\[](.*?)[)\]]', h)
            if m:
                unit = _find_unit(m.group(1), param)
                if unit:
                    result[param] = unit
                    break
            # Check after comma: "Q, m³/h"
            m = re.search(r',\s*(.+)$', h)
            if m:
                unit = _find_unit(m.group(1), param)
                if unit:
                    result[param] = unit
                    break

        if param in result:
            continue

        # 2. Check headers text as a whole
        unit = _find_unit(headers_text, param)
        if unit:
            result[param] = unit
            continue

        # 3. Check page text (first 2000 chars)
        unit = _find_unit(page_text[:2000], param)
        if unit:
            result[param] = unit

    return result


def convert_to_standard(value: float, from_unit: str, param: str) -> float:
    """Convert value from detected unit to standard (Q→m³/h, H→m, P→kW).

    Args:
        value: numeric value
        from_unit: unit key from detect_units (e.g. "l/min", "ft", "hp")
        param: "q", "h", or "p"

    Returns:
        Converted value in standard units
    """
    factors = CONVERSION_FACTORS.get(param, {})
    factor = factors.get(from_unit)
    if factor is None:
        log.warning("Unknown unit %r for param %r — returning as-is", from_unit, param)
        return value
    return value * factor


def detect_unit_from_text(text: str) -> dict[str, str]:
    """Detect all units from a block of text (e.g. page header area).

    Convenience wrapper for page-level detection without headers.
    """
    return detect_units([], text)
