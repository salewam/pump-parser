"""Base class for all pump data extractors."""

from abc import ABC, abstractmethod
import re
import logging

from pump_parser.models import PumpEntry, ExtractedTable, ClassifiedPage, ExtractionResult
from pump_parser.validation.physics import validate_pump_physics

log = logging.getLogger("pump_parser.extractors")


class BaseExtractor(ABC):
    """Abstract base for type-specific extractors."""

    type_name: str = "base"

    @abstractmethod
    def extract(
        self,
        page_text: str,
        tables: list[ExtractedTable],
        page_num: int = 0,
        source_file: str = "",
    ) -> ExtractionResult:
        """Extract pump entries from a page.

        Args:
            page_text: raw text from the page
            tables: extracted tables from the page
            page_num: 0-indexed page number
            source_file: PDF filename

        Returns:
            ExtractionResult with entries, score, and warnings
        """
        ...

    def score(self, result: ExtractionResult, page_text: str) -> float:
        """Score extraction quality (0-100).

        Scoring formula:
        - Quantity: min(count, 50) * 0.5
        - Completeness: % with Q>0 AND H>0 AND P>0, weight 30
        - Physics: % passing validation, weight 30
        - Grounding: % model names found in page text, weight 20
        - Curve bonus: entries with Q-H curves, capped at 15
        """
        entries = result.entries
        if not entries:
            return 0.0

        score = 0.0

        # Quantity
        score += min(len(entries), 50) * 0.5

        # Completeness
        complete = sum(1 for e in entries if e.q_nom > 0 and e.h_nom > 0 and e.power_kw > 0)
        score += (complete / len(entries)) * 30

        # Physics
        valid = 0
        for e in entries:
            ok, _, _ = validate_pump_physics(
                e.q_nom, e.h_nom, e.power_kw,
                e.q_points, e.h_points,
                e.model, e.series,
            )
            if ok:
                valid += 1
        score += (valid / len(entries)) * 30

        # Text grounding
        page_upper = page_text.upper()
        grounded = sum(1 for e in entries if e.model.upper() in page_upper)
        score += (grounded / len(entries)) * 20

        # Curve bonus
        curves = sum(1 for e in entries if e.has_curve())
        score += min(curves * 2, 15)

        return round(score, 1)
