"""Self-heal pipeline — auto-correct entries with physics violations.

Corrections applied (in order):
1. Swap Q/H if efficiency improves (common OCR/extraction mixup)
2. Fix unit errors: Q in l/min parsed as m³/h (÷60), H in bar parsed as m (×10.2)
3. Estimate missing power from Q, H using hydraulic formula
4. Snap power to nearest IEC motor size
5. Fix swapped Q_points/H_points in curves
"""

import logging
from typing import Optional

from pump_parser.models import PumpEntry
from pump_parser.validation.physics import (
    calculate_efficiency,
    estimate_power_hydraulic,
    validate_pump_physics,
    guess_pump_type,
    PUMP_TYPES,
    IEC_MOTOR_SIZES,
)

log = logging.getLogger("pump_parser.verification.self_heal")


def self_heal_entries(entries: list[PumpEntry]) -> tuple[list[PumpEntry], int]:
    """Apply auto-corrections to entries with physics violations.

    Returns:
        (entries, heal_count) — modified in-place, number of entries healed.
    """
    healed = 0
    for entry in entries:
        fixes = _heal_entry(entry)
        if fixes:
            healed += 1
            entry.warnings.append(f"Self-healed: {', '.join(fixes)}")
            log.debug("Healed %s: %s", entry.model, fixes)

    if healed:
        log.info("Self-heal: %d/%d entries corrected", healed, len(entries))

    return entries, healed


def _heal_entry(entry: PumpEntry) -> list[str]:
    """Try to fix a single entry. Returns list of fixes applied."""
    fixes = []

    # Skip entries with no data
    if entry.q_nom <= 0 and entry.h_nom <= 0:
        return fixes

    # 1. Try Q/H swap
    fix = _try_swap_qh(entry)
    if fix:
        fixes.append(fix)

    # 2. Try unit fix (Q looks like l/min, H looks like bar)
    fix = _try_fix_units(entry)
    if fix:
        fixes.append(fix)

    # 3. Estimate missing power
    fix = _try_estimate_power(entry)
    if fix:
        fixes.append(fix)

    # 4. Snap power to IEC size
    fix = _try_snap_power(entry)
    if fix:
        fixes.append(fix)

    # 5. Fix curve points
    fix = _try_fix_curve(entry)
    if fix:
        fixes.append(fix)

    return fixes


def _try_swap_qh(entry: PumpEntry) -> Optional[str]:
    """Swap Q and H if it produces better efficiency."""
    if entry.q_nom <= 0 or entry.h_nom <= 0 or entry.power_kw <= 0:
        return None

    eta_original = calculate_efficiency(entry.q_nom, entry.h_nom, entry.power_kw)
    eta_swapped = calculate_efficiency(entry.h_nom, entry.q_nom, entry.power_kw)

    if eta_original is None or eta_swapped is None:
        return None

    # Current is bad, swapped is good
    pump_type = guess_pump_type(entry.q_nom, entry.h_nom, entry.power_kw,
                                entry.model, entry.series)
    rules = PUMP_TYPES[pump_type]
    eta_min, eta_max = rules["eta"]

    original_ok = eta_min * 0.5 <= eta_original <= 0.96
    swapped_ok = eta_min * 0.5 <= eta_swapped <= 0.96

    if not original_ok and swapped_ok:
        entry.q_nom, entry.h_nom = entry.h_nom, entry.q_nom
        if entry.q_points and entry.h_points:
            entry.q_points, entry.h_points = entry.h_points, entry.q_points
        return f"swapped Q/H ({entry.h_nom}↔{entry.q_nom})"

    return None


def _try_fix_units(entry: PumpEntry) -> Optional[str]:
    """Fix common unit parsing errors."""
    if entry.q_nom <= 0 or entry.power_kw <= 0:
        return None

    eta = calculate_efficiency(entry.q_nom, entry.h_nom, entry.power_kw)
    if eta is not None and 0.10 <= eta <= 0.96:
        return None  # already reasonable

    # Try: Q was in l/min but parsed as m³/h
    if entry.q_nom > 30:
        q_fixed = entry.q_nom / 60.0
        if entry.h_nom > 0:
            eta_fixed = calculate_efficiency(q_fixed, entry.h_nom, entry.power_kw)
            if eta_fixed is not None and 0.10 <= eta_fixed <= 0.96:
                entry.q_nom = round(q_fixed, 2)
                if entry.q_points:
                    entry.q_points = [round(v / 60.0, 2) for v in entry.q_points]
                return f"Q ÷60 (was l/min)"

    # Try: H was in bar but parsed as meters (H suspiciously low)
    if 0 < entry.h_nom < 10 and entry.q_nom > 0:
        h_fixed = entry.h_nom * 10.2
        eta_fixed = calculate_efficiency(entry.q_nom, h_fixed, entry.power_kw)
        if eta_fixed is not None and 0.10 <= eta_fixed <= 0.96:
            entry.h_nom = round(h_fixed, 1)
            if entry.h_points:
                entry.h_points = [round(v * 10.2, 1) for v in entry.h_points]
            return f"H ×10.2 (was bar)"

    return None


def _try_estimate_power(entry: PumpEntry) -> Optional[str]:
    """Estimate power if missing but Q and H are present."""
    if entry.power_kw > 0:
        return None
    if entry.q_nom <= 0 or entry.h_nom <= 0:
        return None

    estimated = estimate_power_hydraulic(entry.q_nom, entry.h_nom)
    if estimated > 0:
        entry.power_kw = estimated
        return f"P estimated={estimated}kW"

    return None


def _try_snap_power(entry: PumpEntry) -> Optional[str]:
    """Snap power to nearest IEC motor size if close."""
    if entry.power_kw <= 0:
        return None

    closest = min(IEC_MOTOR_SIZES, key=lambda x: abs(x - entry.power_kw))
    ratio = entry.power_kw / closest if closest > 0 else 0

    # Only snap if within 15% of an IEC size and not already exact
    if 0.85 <= ratio <= 1.15 and entry.power_kw != closest:
        old = entry.power_kw
        entry.power_kw = closest
        return f"P snapped {old}→{closest}kW"

    return None


def _try_fix_curve(entry: PumpEntry) -> Optional[str]:
    """Fix swapped Q/H curve points."""
    if not entry.has_curve():
        return None

    q_pts = entry.q_points
    h_pts = entry.h_points

    # Check if Q is decreasing and H is increasing → swapped
    q_increasing = sum(1 for i in range(1, len(q_pts)) if q_pts[i] > q_pts[i-1])
    h_decreasing = sum(1 for i in range(1, len(h_pts)) if h_pts[i] < h_pts[i-1])
    q_decreasing = sum(1 for i in range(1, len(q_pts)) if q_pts[i] < q_pts[i-1])
    h_increasing = sum(1 for i in range(1, len(h_pts)) if h_pts[i] > h_pts[i-1])

    n = len(q_pts) - 1
    if n < 2:
        return None

    # Normal: Q goes up, H goes down (after shutoff region)
    # Swapped: Q goes down, H goes up
    if q_decreasing > n * 0.6 and h_increasing > n * 0.6:
        entry.q_points, entry.h_points = entry.h_points, entry.q_points
        return "curve Q/H swapped"

    return None
