"""Confidence calibration — refine entry confidence from multiple signals.

Combines signals:
1. Extractor base confidence (from extraction)
2. Physics validation result
3. Text grounding (model name found in page text)
4. Data completeness (Q, H, P, curve, article, RPM)
5. IEC motor match
6. Self-heal penalty (healed entries less trustworthy)
7. Cross-validation penalty (from Vision AI cross_validate)
"""

import logging

from pump_parser.models import PumpEntry
from pump_parser.validation.physics import (
    calculate_efficiency,
    guess_pump_type,
    validate_pump_physics,
    PUMP_TYPES,
    IEC_MOTOR_SIZES,
)

log = logging.getLogger("pump_parser.verification.confidence")

# Signal weights (sum to ~1.0)
W_COMPLETENESS = 0.25
W_PHYSICS = 0.30
W_GROUNDING = 0.20
W_IEC_MATCH = 0.10
W_EXTRACTOR = 0.15


def calibrate_confidence(
    entries: list[PumpEntry],
    page_texts: dict[int, str] | None = None,
) -> list[PumpEntry]:
    """Recalculate confidence for all entries using multiple signals.

    Args:
        entries: list of PumpEntry to calibrate
        page_texts: optional dict of page_num → page_text for grounding check

    Returns:
        Same entries with updated confidence values.
    """
    if not entries:
        return entries

    page_texts = page_texts or {}

    for entry in entries:
        new_conf = _calculate_confidence(entry, page_texts)
        old_conf = entry.confidence
        entry.confidence = round(new_conf, 3)

        if abs(new_conf - old_conf) > 0.1:
            log.debug(
                "%s: confidence %.2f → %.2f",
                entry.model, old_conf, new_conf,
            )

    return entries


def _calculate_confidence(entry: PumpEntry, page_texts: dict[int, str]) -> float:
    """Calculate calibrated confidence for a single entry."""
    scores = {}

    # 1. Completeness (0-1)
    scores["completeness"] = _score_completeness(entry)

    # 2. Physics (0-1)
    scores["physics"] = _score_physics(entry)

    # 3. Text grounding (0-1)
    scores["grounding"] = _score_grounding(entry, page_texts)

    # 4. IEC motor match (0-1)
    scores["iec"] = _score_iec_match(entry)

    # 5. Extractor base (0-1)
    scores["extractor"] = min(entry.confidence, 1.0)

    # Weighted sum
    conf = (
        scores["completeness"] * W_COMPLETENESS
        + scores["physics"] * W_PHYSICS
        + scores["grounding"] * W_GROUNDING
        + scores["iec"] * W_IEC_MATCH
        + scores["extractor"] * W_EXTRACTOR
    )

    # Penalties
    if any("Self-healed" in w for w in entry.warnings):
        conf *= 0.90  # healed entries slightly less trustworthy

    if any("not found in page text" in w for w in entry.warnings):
        conf *= 0.85  # model not grounded

    # Clamp
    return max(0.05, min(0.99, conf))


def _score_completeness(entry: PumpEntry) -> float:
    """Score data completeness (0-1)."""
    s = 0.0
    total = 0.0

    # Core fields (high weight)
    if entry.q_nom > 0:
        s += 3
    total += 3
    if entry.h_nom > 0:
        s += 3
    total += 3
    if entry.power_kw > 0:
        s += 2
    total += 2

    # Curve data (bonus)
    if entry.has_curve():
        s += 3
    total += 3

    # Optional fields
    if entry.rpm > 0:
        s += 1
    total += 1
    if entry.article:
        s += 1
    total += 1
    if entry.stages > 0:
        s += 0.5
    total += 0.5
    if entry.dn_suction > 0:
        s += 0.5
    total += 0.5

    return s / total if total > 0 else 0.0


def _score_physics(entry: PumpEntry) -> float:
    """Score physics validity (0-1)."""
    if entry.q_nom <= 0:
        return 0.2  # can't validate without Q

    ok, _, conf_adj = validate_pump_physics(
        entry.q_nom, entry.h_nom, entry.power_kw,
        entry.q_points, entry.h_points,
        entry.model, entry.series,
    )

    if not ok:
        return 0.1

    # Base 0.8, adjusted by physics confidence
    score = 0.8 + conf_adj
    # Bonus for good efficiency
    eta = calculate_efficiency(entry.q_nom, entry.h_nom, entry.power_kw)
    if eta is not None:
        pump_type = guess_pump_type(
            entry.q_nom, entry.h_nom, entry.power_kw,
            entry.model, entry.series,
        )
        rules = PUMP_TYPES[pump_type]
        eta_min, eta_max = rules["eta"]
        if eta_min <= eta <= eta_max:
            score += 0.2  # efficiency in normal range

    return max(0.0, min(1.0, score))


def _score_grounding(entry: PumpEntry, page_texts: dict[int, str]) -> float:
    """Score text grounding (0-1)."""
    page_text = page_texts.get(entry.source_page, "")
    if not page_text:
        return 0.5  # unknown — neutral

    text_upper = page_text.upper()
    model_upper = entry.model.strip().upper()

    if not model_upper:
        return 0.3

    # Model name in text
    if model_upper in text_upper:
        return 1.0

    # Without spaces
    if model_upper.replace(" ", "") in text_upper.replace(" ", ""):
        return 0.9

    return 0.2


def _score_iec_match(entry: PumpEntry) -> float:
    """Score IEC motor size match (0-1)."""
    if entry.power_kw <= 0:
        return 0.5  # unknown — neutral

    closest = min(IEC_MOTOR_SIZES, key=lambda x: abs(x - entry.power_kw))
    if entry.power_kw == closest:
        return 1.0

    ratio = entry.power_kw / closest if closest > 0 else 0
    if 0.95 <= ratio <= 1.05:
        return 0.9
    if 0.85 <= ratio <= 1.15:
        return 0.7
    if 0.75 <= ratio <= 1.35:
        return 0.5
    return 0.2
