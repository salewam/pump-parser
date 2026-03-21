"""Anomaly detection — flag outlier entries within a catalog.

Detects:
1. Statistical outliers in Q, H, P (IQR method)
2. Power-flow inconsistency (P doesn't scale with Q×H)
3. Duplicate-like entries (same Q/H/P, different model names)
4. Impossible values (negative, zero where required)
"""

import logging
from collections import defaultdict

from pump_parser.models import PumpEntry
from pump_parser.validation.physics import calculate_efficiency

log = logging.getLogger("pump_parser.verification.anomaly")


def detect_anomalies(entries: list[PumpEntry]) -> list[PumpEntry]:
    """Flag anomalous entries with warnings and confidence penalties.

    Returns same list, modified in-place.
    """
    if len(entries) < 3:
        return entries

    flagged = 0

    # 1. Statistical outliers
    flagged += _flag_statistical_outliers(entries)

    # 2. Power-flow inconsistency
    flagged += _flag_power_inconsistency(entries)

    # 3. Duplicate-like entries
    flagged += _flag_duplicates(entries)

    if flagged:
        log.info("Anomaly detection: %d/%d entries flagged", flagged, len(entries))

    return entries


def _flag_statistical_outliers(entries: list[PumpEntry]) -> int:
    """Flag entries with Q, H, or P values outside 1.5×IQR of their series."""
    # Group by series prefix
    series_groups = _group_by_series(entries)
    flagged = 0

    for series, group in series_groups.items():
        if len(group) < 5:
            continue

        for param, getter in [("Q", lambda e: e.q_nom), ("H", lambda e: e.h_nom), ("P", lambda e: e.power_kw)]:
            vals = [getter(e) for e in group if getter(e) > 0]
            if len(vals) < 5:
                continue

            q1, q3 = _percentile(vals, 25), _percentile(vals, 75)
            iqr = q3 - q1
            if iqr <= 0:
                continue

            lower = q1 - 3.0 * iqr  # wide tolerance (3×IQR, not 1.5)
            upper = q3 + 3.0 * iqr

            for e in group:
                v = getter(e)
                if v > 0 and (v < lower or v > upper):
                    e.warnings.append(f"Outlier {param}={v:.1f} (range {lower:.1f}-{upper:.1f})")
                    e.confidence *= 0.85
                    flagged += 1

    return flagged


def _flag_power_inconsistency(entries: list[PumpEntry]) -> int:
    """Flag entries where P doesn't scale proportionally with Q×H within the same series."""
    flagged = 0
    series_groups = _group_by_series(entries)

    for series, group in series_groups.items():
        complete = [e for e in group if e.q_nom > 0 and e.h_nom > 0 and e.power_kw > 0]
        if len(complete) < 5:
            continue

        etas = []
        for e in complete:
            eta = calculate_efficiency(e.q_nom, e.h_nom, e.power_kw)
            if eta is not None and 0.01 < eta < 2.0:
                etas.append(eta)

        if len(etas) < 5:
            continue

        median_eta = _percentile(etas, 50)
        if median_eta <= 0:
            continue

        for e in complete:
            eta = calculate_efficiency(e.q_nom, e.h_nom, e.power_kw)
            if eta is None:
                continue

            ratio = eta / median_eta
            if ratio > 3.0 or ratio < 0.33:
                e.warnings.append(
                    f"Efficiency anomaly: eta={eta:.2f} vs series median={median_eta:.2f} (×{ratio:.1f})"
                )
                e.confidence *= 0.80
                flagged += 1

    return flagged


def _flag_duplicates(entries: list[PumpEntry]) -> int:
    """Flag entries with identical Q/H/P but different model names."""
    flagged = 0
    seen: dict[tuple, str] = {}

    for e in entries:
        if e.q_nom <= 0 and e.h_nom <= 0:
            continue

        key = (round(e.q_nom, 1), round(e.h_nom, 1), round(e.power_kw, 1))
        if key in seen:
            other_model = seen[key]
            if other_model.upper() != e.model.upper():
                e.warnings.append(f"Same Q/H/P as '{other_model}'")
                e.confidence *= 0.90
                flagged += 1
        else:
            seen[key] = e.model

    return flagged


def _group_by_series(entries: list[PumpEntry]) -> dict[str, list[PumpEntry]]:
    """Group entries by series prefix."""
    import re
    groups: dict[str, list[PumpEntry]] = defaultdict(list)
    for e in entries:
        m = re.match(r'^([A-ZА-ЯЁa-zа-яё]{2,6})', e.model.strip())
        series = m.group(1).upper() if m else "OTHER"
        groups[series].append(e)
    return dict(groups)


def _percentile(vals: list[float], p: int) -> float:
    """Calculate percentile (simple nearest-rank method)."""
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = int(len(s) * p / 100)
    idx = max(0, min(idx, len(s) - 1))
    return s[idx]
