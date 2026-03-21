"""Recipe generator — auto-create recipes from successful parses.

Triggered when:
- avg_confidence >= 0.8
- physics_pass_rate >= 0.8
- models >= 3

Captures from ParseResult:
- filename patterns from source filename
- series/manufacturer keywords from model names
- page signatures from data page text
- extractor type from dominant extractor
- RPM from entries
"""

import re
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path

from pump_parser.models import ParseResult, PumpEntry
from pump_parser.learning.recipe import (
    Recipe, MatchingConfig, ExtractionConfig, ValidationConfig,
)

log = logging.getLogger("pump_parser.learning.recipe_generator")

# Thresholds for auto-generation
MIN_CONFIDENCE = 0.8
MIN_PHYSICS_RATE = 0.8
MIN_MODELS = 3


def generate_recipe(
    pdf_path: str,
    result: ParseResult,
    pdf_hash: str = "",
    first_pages_text: str = "",
) -> Recipe | None:
    """Auto-generate a recipe from a successful parse.

    Returns Recipe if quality thresholds met, None otherwise.
    """
    entries = result.entries
    if len(entries) < MIN_MODELS:
        log.debug("Too few models (%d) to generate recipe", len(entries))
        return None

    if result.avg_confidence < MIN_CONFIDENCE:
        log.debug("Avg confidence %.2f below threshold", result.avg_confidence)
        return None

    if result.report and result.report.physics_pass_rate < MIN_PHYSICS_RATE:
        log.debug("Physics rate %.2f below threshold", result.report.physics_pass_rate)
        return None

    # Extract info from entries
    series = _extract_series(entries)
    manufacturer = _guess_manufacturer(pdf_path, first_pages_text)
    dominant_extractor = _dominant_extractor(result)
    rpms = _extract_rpms(entries)
    q_range, h_range, p_range = _extract_ranges(entries)

    # Build matching config
    filename = Path(pdf_path).name
    stem = Path(pdf_path).stem.lower()
    filename_patterns = _build_filename_patterns(stem, series)

    # Extract page signatures from first pages text
    signatures = _extract_signatures(first_pages_text)

    name = series[0] if series else stem[:20]
    recipe_id = Recipe.generate_id(manufacturer, name)

    recipe = Recipe(
        recipe_id=recipe_id,
        name=name,
        manufacturer=manufacturer,
        description=f"Auto-generated from {filename} ({len(entries)} models)",
        matching=MatchingConfig(
            filename_patterns=filename_patterns,
            manufacturer_keywords=[manufacturer] if manufacturer else [],
            series_keywords=series[:5],
            page_signatures=signatures[:5],
        ),
        extraction=ExtractionConfig(
            extractor_type=dominant_extractor,
            rpm_fixed=rpms[0] if len(rpms) == 1 else 0,
        ),
        validation=ValidationConfig(
            q_range=q_range,
            h_range=h_range,
            p_range=p_range,
            rpm_expected=rpms[:3],
        ),
        confidence=min(result.avg_confidence, 0.90),
        uses_count=1,
        success_count=1,
        created=datetime.utcnow().isoformat(),
        source_file=filename,
        source_hash=pdf_hash,
        auto_generated=True,
    )

    log.info(
        "Generated recipe '%s' (id=%s) from %s: %d models, extractor=%s",
        recipe.name, recipe.recipe_id, filename, len(entries), dominant_extractor,
    )
    return recipe


def _extract_series(entries: list[PumpEntry]) -> list[str]:
    """Extract unique series names, sorted by frequency."""
    counter = Counter(e.series for e in entries if e.series)
    return [s for s, _ in counter.most_common(10)]


def _guess_manufacturer(pdf_path: str, text: str) -> str:
    """Guess manufacturer from filename/text."""
    known = {
        "cnp": "CNP", "fancy": "CNP", "grundfos": "Grundfos",
        "wilo": "Wilo", "ksb": "KSB", "dab": "DAB",
        "ebara": "Ebara", "pedrollo": "Pedrollo", "lowara": "Lowara",
        "calpeda": "Calpeda",
    }
    combined = (Path(pdf_path).name + " " + text[:500]).lower()
    for key, name in known.items():
        if key in combined:
            return name
    return ""


def _dominant_extractor(result: ParseResult) -> str:
    """Get the extractor that produced the most entries."""
    if not result.report or not result.report.extractor_breakdown:
        return "flat_table"
    breakdown = result.report.extractor_breakdown
    return max(breakdown, key=breakdown.get)


def _extract_rpms(entries: list[PumpEntry]) -> list[int]:
    """Get unique RPM values from entries."""
    rpms = Counter(e.rpm for e in entries if e.rpm > 0)
    return [r for r, _ in rpms.most_common(3)]


def _extract_ranges(entries: list[PumpEntry]) -> tuple:
    """Extract Q/H/P ranges with 20% margin."""
    qs = [e.q_nom for e in entries if e.q_nom > 0]
    hs = [e.h_nom for e in entries if e.h_nom > 0]
    ps = [e.power_kw for e in entries if e.power_kw > 0]

    def _range(values, default_max):
        if not values:
            return (0.0, default_max)
        lo = min(values) * 0.8
        hi = max(values) * 1.2
        return (round(lo, 2), round(hi, 2))

    return _range(qs, 10000), _range(hs, 2500), _range(ps, 1000)


def _build_filename_patterns(stem: str, series: list[str]) -> list[str]:
    """Build filename match patterns."""
    patterns = []
    # Pattern from filename
    # Remove dates and version numbers
    clean = re.sub(r'[\d._-]{6,}', '*', stem)
    clean = re.sub(r'\*+', '*', clean)
    if clean and clean != '*':
        patterns.append(f"*{clean}*")

    # Patterns from series names
    for s in series[:3]:
        s_lower = s.lower()
        if len(s_lower) >= 2:
            patterns.append(f"*{s_lower}*")

    return list(dict.fromkeys(patterns))[:5]  # dedupe, max 5


def _extract_signatures(text: str) -> list[str]:
    """Extract distinctive text signatures from page text."""
    sigs = []
    # Look for header-like patterns
    patterns = [
        r'Q\s*(?:ном|nom)?\s*\[[^\]]+\]',
        r'H\s*(?:ном|nom)?\s*\[[^\]]+\]',
        r'P\s*2?\s*\[(?:кВт|kW)\]',
        r'(?:Модель|Model|Насос)\s*[|\s]',
        r'(?:Артикул|Article)\s*[|\s]',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            sigs.append(m.group().strip())

    return sigs
