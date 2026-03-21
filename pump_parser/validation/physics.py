#!/usr/bin/env python3
"""
Pump Validators — Physics validation, model name normalization, unit conversion.

Part of Universal Pump Parser v10.
"""

import re
import math
import logging
from typing import Tuple, Optional, List, Dict

log = logging.getLogger("pump_parser.physics")

# ─── Physical Constants ────────────────────────────────────────────────────────

RHO_WATER = 998.0    # kg/m³ at 20°C
G_GRAVITY = 9.81     # m/s²

# Standard IEC motor power sizes (kW)
IEC_MOTOR_SIZES = [
    0.06, 0.09, 0.12, 0.18, 0.25, 0.37, 0.55, 0.75,
    1.1, 1.5, 2.2, 3.0, 4.0, 5.5, 7.5, 11.0,
    15.0, 18.5, 22.0, 30.0, 37.0, 45.0, 55.0, 75.0,
    90.0, 110.0, 132.0, 160.0, 200.0, 250.0, 315.0, 355.0,
    400.0, 500.0, 630.0, 800.0, 1000.0,
]

# ─── Pump Type Detection ──────────────────────────────────────────────────────

PUMP_TYPES = {
    "centrifugal_single": {"q": [0.5, 5000], "h": [2, 200], "eta": [0.30, 0.92]},
    "centrifugal_multi":  {"q": [0.5, 500],  "h": [10, 2500], "eta": [0.40, 0.88]},
    "submersible_bore":   {"q": [0.5, 500],  "h": [10, 800],  "eta": [0.35, 0.80]},
    "submersible_drain":  {"q": [1, 2000],   "h": [2, 50],    "eta": [0.20, 0.75]},
    "sewage":             {"q": [1, 3000],   "h": [2, 60],    "eta": [0.15, 0.70]},
    "circulator":         {"q": [0.1, 50],   "h": [0.3, 15],  "eta": [0.05, 0.60]},
    "industrial":         {"q": [5, 50000],  "h": [5, 500],   "eta": [0.50, 0.93]},
}

# Keywords in model/series that indicate pump type
DRAIN_KEYWORDS = {"drain", "dw", "sewage", "грязев", "дренаж", "канализ", "фекал"}
CIRCULATOR_KEYWORDS = {"circul", "циркул", "ups", "wilo-star", "alpha"}
MULTI_KEYWORDS = {"cr", "cdl", "cdm", "cdmf", "bm", "bmn", "pv", "multi", "многоступ"}
BORE_KEYWORDS = {"sp", "sq", "скважин", "bore", "submers", "погруж"}


def guess_pump_type(q_nom: float, h_nom: float, power_kw: float,
                    model: str = "", series: str = "", stages: int = 0) -> str:
    """Determine pump type from parameters and naming for correct validation rules."""
    name_lower = (model + " " + series).lower()

    # Keyword-based detection (most reliable)
    if any(kw in name_lower for kw in DRAIN_KEYWORDS):
        return "submersible_drain"
    if any(kw in name_lower for kw in CIRCULATOR_KEYWORDS):
        return "circulator"
    if any(kw in name_lower for kw in BORE_KEYWORDS):
        return "submersible_bore"
    if any(kw in name_lower for kw in MULTI_KEYWORDS):
        return "centrifugal_multi"

    # Parameter-based fallback
    if h_nom < 15 and q_nom < 50 and power_kw < 0.5:
        return "circulator"
    if h_nom > 200 or stages > 5:
        return "centrifugal_multi"
    if "sewage" in name_lower or "фекал" in name_lower:
        return "sewage"
    return "centrifugal_single"


# ─── Unit Maps ─────────────────────────────────────────────────────────────────

Q_UNIT_MAP = {
    # unit → multiplier to convert to m³/h
    "м³/ч": 1.0, "m³/h": 1.0, "m3/h": 1.0, "m^3/h": 1.0,
    "м3/ч": 1.0, "куб.м/ч": 1.0, "cbm/h": 1.0,
    "м3/час": 1.0, "м3 /час": 1.0, "m3/час": 1.0,  # PDF extraction variants
    "л/с": 3.6, "l/s": 3.6,
    "л/мин": 0.06, "l/min": 0.06,
    "gpm": 0.2271, "usgpm": 0.2271,  # US gallons per minute
    "igpm": 0.2728,                    # Imperial gallons per minute
    "м³/мин": 60.0, "m³/min": 60.0,
    # PDF extraction artifacts (truncated units)
    "м": 1.0, "м/ч": 1.0, "m/h": 1.0,
    "куб.м./ч": 1.0, "куб. м/ч": 1.0, "куб м/ч": 1.0,
}

H_UNIT_MAP = {
    # unit → multiplier to convert to meters
    "м": 1.0, "m": 1.0, "м.в.ст": 1.0, "mwc": 1.0, "mws": 1.0,
    "ft": 0.3048, "feet": 0.3048,
    "бар": 10.197, "bar": 10.197, "atm": 10.33,
    "кпа": 0.10197, "kpa": 0.10197,
    "мпа": 101.97, "mpa": 101.97,
    "psi": 0.7031,
}

P_UNIT_MAP = {
    # unit → multiplier to convert to kW
    "квт": 1.0, "kw": 1.0,
    "вт": 0.001, "w": 0.001,
    "л.с.": 0.7457, "hp": 0.7457, "cv": 0.7355, "ps": 0.7355,
}


def convert_q(value: float, unit: str) -> float:
    """Convert flow value to m³/h."""
    key = unit.lower().strip()
    factor = Q_UNIT_MAP.get(key)
    if factor is None:
        log.warning("Unknown Q unit %r — assuming m³/h (factor=1.0)", unit)
        factor = 1.0
    return value * factor


def convert_h(value: float, unit: str) -> float:
    """Convert head value to meters."""
    key = unit.lower().strip()
    factor = H_UNIT_MAP.get(key)
    if factor is None:
        log.warning("Unknown H unit %r — assuming meters (factor=1.0)", unit)
        factor = 1.0
    return value * factor


def convert_p(value: float, unit: str) -> float:
    """Convert power value to kW."""
    key = unit.lower().strip()
    factor = P_UNIT_MAP.get(key)
    if factor is None:
        log.warning("Unknown P unit %r — assuming kW (factor=1.0)", unit)
        factor = 1.0
    return value * factor


# ─── Physics Validation ────────────────────────────────────────────────────────

def calculate_efficiency(q_m3h: float, h_m: float, power_kw: float) -> Optional[float]:
    """Calculate pump efficiency η = ρgQH / (P × 1000).
    Returns None if inputs are invalid."""
    if q_m3h <= 0 or h_m <= 0 or power_kw <= 0:
        return None
    p_hydraulic = RHO_WATER * G_GRAVITY * (q_m3h / 3600.0) * h_m / 1000.0
    eta = p_hydraulic / power_kw
    return eta


def validate_pump_physics(q_nom: float, h_nom: float, power_kw: float,
                          q_points: List[float] = None,
                          h_points: List[float] = None,
                          model: str = "", series: str = "",
                          stages: int = 0) -> Tuple[bool, str, float]:
    """Validate pump entry for physical sanity.

    Returns:
        (valid, reason, confidence_adjustment)
        valid: True if entry passes validation
        reason: explanation string
        confidence_adjustment: float to add to base confidence (-1.0 to 0.0)
    """
    q_points = q_points or []
    h_points = h_points or []

    # 1. Q > 0 required
    if q_nom <= 0:
        return False, "Q <= 0", 0.0

    # 2. H >= 0 (H=0 ok for drain pumps with no head)
    if h_nom < 0:
        return False, "H < 0", 0.0

    # 3. Determine pump type → validation rules
    pump_type = guess_pump_type(q_nom, h_nom, power_kw, model, series, stages)
    rules = PUMP_TYPES[pump_type]

    # 4. Range checks (2x tolerance for edge cases)
    if q_nom > rules["q"][1] * 2:
        return False, f"Q={q_nom} too large for {pump_type}", 0.0
    if h_nom > rules["h"][1] * 2:
        return False, f"H={h_nom} too large for {pump_type}", 0.0

    # 5. Efficiency check (only if Q, H, P all > 0)
    conf_adj = 0.0
    eta = calculate_efficiency(q_nom, h_nom, power_kw)
    if eta is not None:
        eta_min, eta_max = rules["eta"]
        if eta > 0.96:
            return False, f"η={eta:.2f} > 0.96 (physically impossible)", 0.0
        if eta > eta_max:
            conf_adj = -0.15  # suspiciously high efficiency
        if eta < eta_min * 0.5:
            conf_adj = -0.25  # suspiciously low efficiency
        if eta < 0.02:
            return False, f"η={eta:.3f} < 0.02 (garbage data)", 0.0

    # 6. Q-H curve validation: check only working range (Q > 20% Q_max)
    if len(q_points) >= 3 and len(h_points) >= 3:
        q_max = max(q_points)
        # Check Q is increasing
        for i in range(1, len(q_points)):
            if q_points[i] > q_max * 0.2:
                if q_points[i] <= q_points[i - 1]:
                    conf_adj -= 0.2
                    break
        # Check H trend (should generally decrease, allow for shutoff hump)
        work_h = [h for q, h in zip(q_points, h_points) if q > q_max * 0.2]
        if len(work_h) >= 3:
            rising_count = sum(1 for i in range(1, len(work_h))
                               if work_h[i] > work_h[i - 1] * 1.05)
            if rising_count > len(work_h) * 0.4:
                conf_adj -= 0.2  # H rising = suspicious but not fatal

    # 7. IEC motor check (not for circulators)
    if pump_type != "circulator" and power_kw > 0:
        closest = min(IEC_MOTOR_SIZES, key=lambda x: abs(x - power_kw))
        ratio = power_kw / closest if closest > 0 else 0
        if ratio < 0.75 or ratio > 1.35:
            conf_adj -= 0.1

    return True, "ok", conf_adj


def validate_entry_basic(model: str, q_nom: float, h_nom: float,
                         power_kw: float) -> Tuple[bool, str]:
    """Basic entry validation (backward-compatible with v9 validate_entry).

    Returns (ok, reason) tuple.
    """
    if not model or len(model) < 2:
        return False, "empty model"
    if q_nom < 0 or h_nom < 0:
        return False, "negative Q or H"
    if q_nom > 100000 or h_nom > 5000:
        return False, f"unrealistic Q={q_nom} H={h_nom}"
    if power_kw < 0 or power_kw > 20000:
        return False, f"bad power {power_kw}"
    if q_nom == 0 and h_nom == 0:
        return False, "both Q_nom and H_nom are zero"
    return True, "ok"


# ─── Model Name Normalization ──────────────────────────────────────────────────

def normalize_model_name(name: str) -> str:
    """Normalize model name for deduplication.

    CDM 32-5 == CDM32-5 == cdm 32-5 == CDM-32-5
    """
    s = name.strip().upper()
    # Map Cyrillic lookalikes to ASCII (for cross-catalog dedup)
    _CYR2ASCII = str.maketrans('АВЕКМНОРСТХ',
                               'ABEKMHOPCTX')
    s = s.translate(_CYR2ASCII)
    # Remove trailing frequency/phase markers before other processing
    s = re.sub(r'\s*(50|60)\s*HZ\s*$', '', s)
    s = re.sub(r'\s*\(IE[1-4]\)\s*$', '', s)
    # Strip trailing phase marker T after pole count: /4T → /4, /2T → /2
    s = re.sub(r'(/\d)T$', r'\1', s)
    # Normalize separators
    s = re.sub(r'\s*[-–—]\s*', '-', s)     # normalize dashes (with optional spaces)
    s = re.sub(r'\s*[/]\s*', '/', s)       # normalize slashes
    # Remove all remaining spaces and underscores so "CDM 32-5" == "CDM32-5" == "CDM_32-5"
    s = re.sub(r'[\s_]+', '', s)
    # Collapse letter-dash-digit so "CDM-32-5" == "CDM32-5"
    s = re.sub(r'([A-Z])-(\d)', r'\1\2', s)
    return s


# ─── Power Estimation ──────────────────────────────────────────────────────────

# Efficiency estimates by pump size
ETA_SMALL = 0.55    # Q < 10 m³/h
ETA_MEDIUM = 0.65   # 10 ≤ Q < 50 m³/h
ETA_LARGE = 0.72    # Q ≥ 50 m³/h


def estimate_power_hydraulic(q_m3h: float, h_m: float) -> float:
    """Estimate shaft power using hydraulic formula.

    P_shaft = ρ × g × Q × H / (η × 1000)
    Returns nearest standard IEC motor size (rounded up).
    """
    if q_m3h <= 0 or h_m <= 0:
        return 0.0
    if q_m3h < 10:
        eta = ETA_SMALL
    elif q_m3h < 50:
        eta = ETA_MEDIUM
    else:
        eta = ETA_LARGE

    q_m3s = q_m3h / 3600.0
    p_hydraulic = RHO_WATER * G_GRAVITY * q_m3s * h_m / 1000.0
    p_shaft = p_hydraulic / eta

    # Round UP to nearest IEC motor size
    for size in IEC_MOTOR_SIZES:
        if size >= p_shaft:
            return size
    return IEC_MOTOR_SIZES[-1]


# ─── Range Value Parsing ──────────────────────────────────────────────────────

# ─── Data Source Constants ─────────────────────────────────────────────────────

DS_CATALOG_CURVE = "catalog_curve"        # Q-H curve from catalog page
DS_CATALOG_NOMINAL = "catalog_nominal"    # Nominal point from catalog table
DS_ESTIMATED = "estimated"                # Derived from model name / stages
DS_UNIVERSAL_TABLE = "universal_table"    # Universal pipeline: standard table
DS_UNIVERSAL_QH = "universal_qh_matrix"  # Universal pipeline: Q-H matrix
DS_UNIVERSAL_TEXT = "universal_text"      # Universal pipeline: text regex
DS_GEMINI = "gemini_vision"               # Gemini Vision OCR
DS_GEMINI_UNVERIFIED = "gemini_unverified"  # Gemini result not cross-validated


def parse_range_value(s: str) -> Tuple[float, float]:
    """Parse range string like '5-80' or '32-19'.

    Returns (min_val, max_val) sorted ascending.
    For Italian format '32-19 m': stores as h_max=32 (at Q_min), h_min=19 (at Q_max).
    """
    if not s:
        return (0.0, 0.0)
    s = str(s).strip()
    # Normalize comma to dot
    s = s.replace(',', '.')
    # Remove units at the end
    s = re.sub(r'\s*(m³/h|m3/h|m/h|l/s|l/min|gpm|м³/ч|л/с|л/мин|m|м|ft|bar|бар|kw|квт|hp|kpa|кпа|psi)\s*$',
               '', s, flags=re.I)

    m = re.match(r'([\d.]+)\s*[-–—]\s*([\d.]+)', s)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        return (min(a, b), max(a, b))

    # Single value
    try:
        v = float(s)
        return (v, v)
    except (ValueError, TypeError):
        return (0.0, 0.0)
