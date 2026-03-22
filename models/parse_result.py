"""
Core dataclasses for the parse pipeline.
All stages produce and consume these types.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import re

# Import config for series orientation lookup
import sys
sys.path.insert(0, "/root/pump_parser")
from config import HORIZONTAL_SERIES, VERTICAL_SERIES, FLAGSHIP_SERIES


@dataclass
class PumpModelResult:
    """Single pump model with per-value confidence tracking."""
    model: str = ""
    series: str = ""
    q: float = 0.0          # flow rate, m³/h
    h: float = 0.0          # head, m
    kw: float = 0.0         # power, kW
    rpm: int = 2900          # rotation speed
    page_number: int = 0

    # Confidence per value (0.0 - 1.0)
    confidence_q: float = 0.0
    confidence_h: float = 0.0
    confidence_kw: float = 0.0

    # Source per value (docling, vlm, ocr, enrichment, selfcorrect)
    source_q: str = ""
    source_h: str = ""
    source_kw: str = ""

    @property
    def key(self) -> str:
        """Normalized dedup key: uppercase, no spaces."""
        k = self.model.upper().strip()
        k = re.sub(r"\s+", "", k)
        k = k.replace(",", ".")  # normalize comma decimal to dot
        k = k.replace("(", "").replace(")", "")  # remove parentheses
        # Normalize Cyrillic lookalikes
        for cyr, lat in [("\u041c", "M"), ("\u0412", "B"), ("\u0421", "C"),
                         ("\u0415", "E"), ("\u041d", "H"), ("\u041e", "O"),
                         ("\u0420", "P"), ("\u0422", "T"), ("\u0410", "A")]:
            k = k.replace(cyr, lat)
        return k

    @property
    def is_complete(self) -> bool:
        """True when all three key params are non-zero."""
        return self.q > 0 and self.h > 0 and self.kw > 0

    @property
    def confidence(self) -> float:
        """Average confidence across non-zero values."""
        vals = []
        if self.q > 0:
            vals.append(self.confidence_q)
        if self.h > 0:
            vals.append(self.confidence_h)
        if self.kw > 0:
            vals.append(self.confidence_kw)
        return round(sum(vals) / len(vals), 3) if vals else 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "model": self.model,
            "series": self.series,
            "q_nom": self.q,
            "h_nom": self.h,
            "power_kw": self.kw,
            "rpm": self.rpm,
            "confidence": self.confidence,
            "confidence_q": self.confidence_q,
            "confidence_h": self.confidence_h,
            "confidence_kw": self.confidence_kw,
            "page_number": self.page_number,
        }


@dataclass
class StageResult:
    """Output from a single pipeline stage."""
    models: List[PumpModelResult] = field(default_factory=list)
    pages_processed: int = 0
    source: str = ""         # "docling", "vlm", "ocr", "selfcorrect"
    errors: List[str] = field(default_factory=list)
    raw_tables: List[Any] = field(default_factory=list)
    page_data: Dict[int, Any] = field(default_factory=dict)  # page_num -> rendered PNG path or data


@dataclass
class ParseResult:
    """Final output of the full pipeline."""
    models: List[PumpModelResult] = field(default_factory=list)
    brand: str = "Unknown"
    brand_confidence: float = 0.0
    brand_source: str = ""
    series_detected: List[str] = field(default_factory=list)
    catalog_type: str = "PUMP"
    elapsed: float = 0.0
    stages_completed: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def total_models(self) -> int:
        return len(self.models)

    @property
    def complete_models(self) -> int:
        return sum(1 for m in self.models if m.is_complete)

    @property
    def completeness(self) -> float:
        """Percentage of models with all Q/H/kW filled."""
        if not self.models:
            return 0.0
        return round(self.complete_models / self.total_models * 100, 1)

    def to_base_format(self) -> List[Dict[str, Any]]:
        """Convert to BASE file format (backward-compatible with bot)."""
        result = []
        for m in self.models:
            s = m.series.upper()
            if s in HORIZONTAL_SERIES:
                orientation = "horizontal"
            elif s in VERTICAL_SERIES:
                orientation = "vertical"
            else:
                orientation = "vertical"
            result.append({
                "id": m.model,
                "kw": m.kw,
                "q": m.q,
                "head_m": m.h,
                "series": m.series,
                "orientation": orientation,
                "flagship": m.series.upper() in FLAGSHIP_SERIES,
                "brand": self.brand,
                "confidence": m.confidence,
            })
        return result
