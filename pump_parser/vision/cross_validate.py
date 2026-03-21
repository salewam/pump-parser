"""Cross-validation: verify Vision AI output against page text.

Catches hallucinations by checking:
1. Model names exist in page text
2. Numeric values appear in page text (fuzzy ±5%)
3. Adjusts confidence accordingly
"""

import re
import logging
from typing import Optional

from pump_parser.models import PumpEntry

log = logging.getLogger("pump_parser.vision.cross_validate")

# Confidence penalties
PENALTY_MODEL_NOT_FOUND = 0.5   # multiply confidence
PENALTY_NUMBERS_NOT_FOUND = 0.7


def cross_validate_entries(
    entries: list[PumpEntry],
    page_text: str,
) -> list[PumpEntry]:
    """Validate AI-extracted entries against page text.

    Adjusts confidence of entries based on text grounding.
    Returns the same entries list (modified in-place).
    """
    if not page_text.strip():
        return entries

    text_upper = page_text.upper()
    # Pre-extract all numbers from text for fast lookup
    text_numbers = _extract_all_numbers(page_text)

    for entry in entries:
        warnings = []

        # Check 1: model name in text
        model_found = _model_in_text(entry.model, text_upper)
        if not model_found:
            entry.confidence *= PENALTY_MODEL_NOT_FOUND
            warnings.append(f"Model '{entry.model}' not found in page text")

        # Check 2: numeric values in text
        values_to_check = []
        if entry.q_nom > 0:
            values_to_check.append(("Q", entry.q_nom))
        if entry.h_nom > 0:
            values_to_check.append(("H", entry.h_nom))
        if entry.power_kw > 0:
            values_to_check.append(("P", entry.power_kw))

        if values_to_check:
            found_count = sum(
                1 for _, v in values_to_check
                if _number_in_text(v, text_numbers)
            )
            if found_count == 0 and len(values_to_check) >= 2:
                entry.confidence *= PENALTY_NUMBERS_NOT_FOUND
                warnings.append("No numeric values found in page text")
            elif found_count < len(values_to_check):
                # Partial match — minor penalty
                ratio = found_count / len(values_to_check)
                entry.confidence *= (0.7 + 0.3 * ratio)

        # Check 3: curve points in text (spot check)
        if entry.has_curve():
            curve_found = sum(
                1 for v in entry.h_points[:3]
                if _number_in_text(v, text_numbers)
            )
            if curve_found == 0 and len(entry.h_points) >= 3:
                entry.confidence *= 0.8
                warnings.append("Curve values not grounded in text")

        if warnings:
            entry.warnings.extend(warnings)

        entry.confidence = round(max(entry.confidence, 0.05), 3)

    grounded = sum(1 for e in entries if not any("not found" in w for w in e.warnings))
    log.debug(
        "Cross-validation: %d/%d entries grounded in text",
        grounded, len(entries),
    )

    return entries


def _model_in_text(model: str, text_upper: str) -> bool:
    """Check if model name (or close variant) exists in page text."""
    model_clean = model.strip().upper()
    if not model_clean:
        return False

    # Exact match
    if model_clean in text_upper:
        return True

    # Try without spaces
    if model_clean.replace(" ", "") in text_upper.replace(" ", ""):
        return True

    # Try first significant part (e.g. "FVH1x2" from "FVH1x2/0.3(T)")
    parts = re.split(r'[/(\s]', model_clean)
    if parts and len(parts[0]) >= 3 and parts[0] in text_upper:
        return True

    return False


def _number_in_text(value: float, text_numbers: set[float], tolerance: float = 0.05) -> bool:
    """Check if a number appears in the set of text numbers (fuzzy ±5%)."""
    if value <= 0:
        return False

    for tn in text_numbers:
        if tn <= 0:
            continue
        if abs(tn - value) / max(value, 0.01) <= tolerance:
            return True
    return False


def _extract_all_numbers(text: str) -> set[float]:
    """Extract all numeric values from text."""
    nums = set()
    for m in re.finditer(r'[\d]+[.,]?\d*', text):
        s = m.group().replace(",", ".")
        try:
            nums.add(float(s))
        except ValueError:
            pass
    return nums
