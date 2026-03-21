"""Cross-page validation — merge and enrich entries found on multiple pages.

Handles cases where:
1. Same model appears on multiple pages (table + graph, specs + curves)
2. Partial data on one page, complementary data on another
3. Series-level info (RPM, voltage) on one page, per-model data on others
"""

import logging
from collections import defaultdict

from pump_parser.models import PumpEntry
from pump_parser.validation.physics import normalize_model_name

log = logging.getLogger("pump_parser.verification.cross_page")


def cross_page_merge(entries: list[PumpEntry]) -> list[PumpEntry]:
    """Merge entries for the same model found on different pages.

    Strategy:
    - Group by normalized model name
    - For each group, merge into the best entry (highest quality)
    - Fill missing fields from other entries in the group

    Returns new list of merged entries.
    """
    if not entries:
        return entries

    # Group by normalized name
    groups: dict[str, list[PumpEntry]] = defaultdict(list)
    for e in entries:
        key = normalize_model_name(e.model)
        if key:
            groups[key].append(e)

    merged = []
    merge_count = 0

    for key, group in groups.items():
        if len(group) == 1:
            merged.append(group[0])
            continue

        # Pick best entry as base
        group.sort(key=_entry_completeness, reverse=True)
        best = group[0]

        # Merge data from other entries
        changed = False
        for other in group[1:]:
            if _merge_into(best, other):
                changed = True

        if changed:
            merge_count += 1
            best.warnings.append(
                f"Merged from {len(group)} pages: "
                + ", ".join(str(e.source_page) for e in group)
            )

        merged.append(best)

    if merge_count:
        log.info(
            "Cross-page merge: %d models merged, %d→%d entries",
            merge_count, len(entries), len(merged),
        )

    return merged


def enrich_from_series(entries: list[PumpEntry]) -> list[PumpEntry]:
    """Enrich entries with series-level data (RPM, voltage, phases).

    If most entries in a series share the same RPM/voltage/phases,
    fill missing values in other entries of that series.
    """
    if not entries:
        return entries

    # Group by series
    series_groups: dict[str, list[PumpEntry]] = defaultdict(list)
    for e in entries:
        series_key = _extract_series(e.model) or e.series or "unknown"
        series_groups[series_key].append(e)

    enriched = 0
    for series, group in series_groups.items():
        if len(group) < 2:
            continue

        # Find majority RPM
        rpms = [e.rpm for e in group if e.rpm > 0]
        if rpms:
            majority_rpm = max(set(rpms), key=rpms.count)
            for e in group:
                if e.rpm == 0:
                    e.rpm = majority_rpm
                    enriched += 1

        # Find majority voltage
        voltages = [e.voltage for e in group if e.voltage]
        if voltages:
            majority_v = max(set(voltages), key=voltages.count)
            for e in group:
                if not e.voltage:
                    e.voltage = majority_v

        # Find majority phases
        phases_list = [e.phases for e in group if e.phases > 0]
        if phases_list:
            majority_ph = max(set(phases_list), key=phases_list.count)
            for e in group:
                if e.phases == 0:
                    e.phases = majority_ph

    if enriched:
        log.debug("Enriched %d entries with series-level data", enriched)

    return entries


def _entry_completeness(e: PumpEntry) -> float:
    """Score how complete an entry's data is."""
    s = 0.0
    if e.q_nom > 0:
        s += 1
    if e.h_nom > 0:
        s += 1
    if e.power_kw > 0:
        s += 1
    if e.has_curve():
        s += 3
    if e.rpm > 0:
        s += 0.5
    if e.article:
        s += 0.5
    if e.stages > 0:
        s += 0.3
    if e.dn_suction > 0:
        s += 0.3
    s += e.confidence
    return s


def _merge_into(best: PumpEntry, other: PumpEntry) -> bool:
    """Merge missing fields from `other` into `best`. Returns True if anything changed."""
    changed = False

    # Fill missing nominal values
    if best.q_nom <= 0 and other.q_nom > 0:
        best.q_nom = other.q_nom
        changed = True
    if best.h_nom <= 0 and other.h_nom > 0:
        best.h_nom = other.h_nom
        changed = True
    if best.power_kw <= 0 and other.power_kw > 0:
        best.power_kw = other.power_kw
        changed = True

    # Prefer curve data
    if not best.has_curve() and other.has_curve():
        best.q_points = other.q_points
        best.h_points = other.h_points
        changed = True

    # Fill missing metadata
    if best.rpm == 0 and other.rpm > 0:
        best.rpm = other.rpm
        changed = True
    if not best.article and other.article:
        best.article = other.article
        changed = True
    if best.stages == 0 and other.stages > 0:
        best.stages = other.stages
        changed = True
    if best.dn_suction == 0 and other.dn_suction > 0:
        best.dn_suction = other.dn_suction
        changed = True
    if best.dn_discharge == 0 and other.dn_discharge > 0:
        best.dn_discharge = other.dn_discharge
        changed = True
    if not best.series and other.series:
        best.series = other.series
        changed = True
    if not best.voltage and other.voltage:
        best.voltage = other.voltage
        changed = True

    # Take higher confidence
    if other.confidence > best.confidence:
        best.confidence = other.confidence
        changed = True

    return changed


def _extract_series(model: str) -> str:
    """Extract series prefix from model name. E.g. 'CMI 25-2' → 'CMI'."""
    import re
    m = re.match(r'^([A-ZА-ЯЁa-zа-яё]{2,6})', model.strip())
    return m.group(1).upper() if m else ""
