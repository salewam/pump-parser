"""Type 5: Graph Reader extractor — reads Q-H curves via Vision AI.

Sends page image to Gemini, gets curve coordinates back,
cross-validates against page text, builds PumpEntry objects.
"""

import logging

from pump_parser.models import PumpEntry, ExtractedTable, ExtractionResult
from pump_parser.extractors.base import BaseExtractor
from pump_parser.vision.api import VisionAPI
from pump_parser.vision.prompts import get_prompt
from pump_parser.vision.cross_validate import cross_validate_entries

log = logging.getLogger("pump_parser.extractors.graph_reader")


class GraphReaderExtractor(BaseExtractor):
    """Extract pump data from graph/curve pages using Vision AI."""

    type_name = "graph_reader"

    def __init__(self, vision_api: VisionAPI | None = None):
        self._api = vision_api

    def extract(
        self,
        page_text: str,
        tables: list[ExtractedTable],
        page_num: int = 0,
        source_file: str = "",
        page_image: bytes | None = None,
    ) -> ExtractionResult:
        """Extract pump curves from a graph page image.

        Args:
            page_text: text from the page (for cross-validation)
            tables: extracted tables (usually empty for graph pages)
            page_num: page number
            source_file: PDF filename
            page_image: PNG bytes of the page render
        """
        if not page_image:
            return ExtractionResult(
                entries=[],
                score=0.0,
                extractor_type=self.type_name,
                page_num=page_num,
                warnings=["No page image provided for graph reading"],
            )

        if not self._api:
            return ExtractionResult(
                entries=[],
                score=0.0,
                extractor_type=self.type_name,
                page_num=page_num,
                warnings=["Vision API not configured"],
            )

        # Send image to Vision AI
        prompt = get_prompt("extract_graph")
        result = self._api.extract_data(page_image, prompt)

        if not result or not isinstance(result, dict):
            return ExtractionResult(
                entries=[],
                score=0.0,
                extractor_type=self.type_name,
                page_num=page_num,
                warnings=["Vision AI returned no data for graph"],
            )

        # Parse curves from AI response
        entries = _parse_graph_response(result, page_num, source_file)

        if not entries:
            return ExtractionResult(
                entries=[],
                score=0.0,
                extractor_type=self.type_name,
                page_num=page_num,
                warnings=["No curves extracted from graph"],
            )

        # Cross-validate against page text
        entries = cross_validate_entries(entries, page_text)

        log.info(
            "Graph reader: %d curves from page %d (API: $%.4f spent)",
            len(entries), page_num, self._api.budget.spent_usd,
        )

        return ExtractionResult(
            entries=entries,
            score=0.0,  # will be scored by base.score()
            extractor_type=self.type_name,
            page_num=page_num,
        )


def _parse_graph_response(
    data: dict,
    page_num: int,
    source_file: str,
) -> list[PumpEntry]:
    """Convert Vision AI graph response to PumpEntry list."""
    curves = data.get("curves", [])
    if not curves:
        return []

    # Unit conversion factors from AI response
    x_unit = data.get("x_axis", {}).get("unit", "m3/h").lower()
    y_unit = data.get("y_axis", {}).get("unit", "m").lower()

    q_factor = 1.0
    if "l/min" in x_unit:
        q_factor = 1.0 / 60.0
    elif "l/s" in x_unit:
        q_factor = 3.6

    h_factor = 1.0
    if "bar" in y_unit:
        h_factor = 10.2

    entries = []
    for curve in curves:
        model = str(curve.get("model", "")).strip()
        if not model:
            continue

        q_points_raw = curve.get("q_points", [])
        h_points_raw = curve.get("h_points", [])

        if not q_points_raw or not h_points_raw:
            continue

        # Convert to floats and apply unit conversion
        q_points = [_to_float(v) * q_factor for v in q_points_raw]
        h_points = [_to_float(v) * h_factor for v in h_points_raw]

        # Ensure same length
        min_len = min(len(q_points), len(h_points))
        q_points = q_points[:min_len]
        h_points = h_points[:min_len]

        if min_len < 3:
            continue

        # Get nominal values from AI or estimate
        q_nom = _to_float(curve.get("q_nom", 0)) * q_factor
        h_nom = _to_float(curve.get("h_nom", 0)) * h_factor

        # If AI didn't provide nominal, estimate at ~65% of max Q
        if q_nom <= 0 and q_points:
            target_q = max(q_points) * 0.65
            q_nom, h_nom = _interpolate_at(q_points, h_points, target_q)

        entry = PumpEntry(
            model=model,
            q_nom=round(q_nom, 2),
            h_nom=round(h_nom, 2),
            q_points=[round(v, 2) for v in q_points],
            h_points=[round(v, 2) for v in h_points],
            source_file=source_file,
            source_page=page_num,
            data_source="vision_graph",
            confidence=0.70,
        )
        entries.append(entry)

    return entries


def _interpolate_at(
    q_points: list[float],
    h_points: list[float],
    target_q: float,
) -> tuple[float, float]:
    """Linear interpolation to find H at target Q."""
    if not q_points or not h_points:
        return 0.0, 0.0

    # Find bracketing points
    for i in range(len(q_points) - 1):
        q1, q2 = q_points[i], q_points[i + 1]
        if q1 <= target_q <= q2 or q2 <= target_q <= q1:
            if abs(q2 - q1) < 0.001:
                return target_q, h_points[i]
            t = (target_q - q1) / (q2 - q1)
            h = h_points[i] + t * (h_points[i + 1] - h_points[i])
            return round(target_q, 2), round(h, 2)

    # Fallback: nearest point
    diffs = [abs(q - target_q) for q in q_points]
    idx = diffs.index(min(diffs))
    return q_points[idx], h_points[idx]


def _to_float(v) -> float:
    """Safe float conversion."""
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0
