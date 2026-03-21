"""Core data models for pump_parser."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


# ─── Enums ────────────────────────────────────────────────────────────────────

class PageType(Enum):
    DATA_TABLE = "data_table"
    DATA_GRAPH = "data_graph"
    COVER = "cover"
    TOC = "toc"
    DIMENSIONS = "dimensions"
    MODEL_CODE = "model_code"
    MODEL_RANGE = "model_range"
    MATERIALS = "materials"
    INSTALLATION = "installation"
    OTHER = "other"


class TableType(Enum):
    FLAT_TABLE = "flat_table"
    QH_MATRIX = "qh_matrix"
    CURVE_TABLE = "curve_table"
    TRANSPOSED = "transposed"
    LIST_FORMAT = "list_format"
    GRAPH = "graph"
    UNKNOWN = "unknown"


# ─── Core Data Models ────────────────────────────────────────────────────────


@dataclass
class PumpEntry:
    """Single pump model extracted from a catalog."""

    # Identity
    model: str = ""
    series: str = ""
    manufacturer: str = ""
    article: str = ""

    # Performance (standard units: m³/h, m, kW)
    q_nom: float = 0.0
    h_nom: float = 0.0
    power_kw: float = 0.0
    rpm: int = 0
    efficiency: float = 0.0

    # Q-H curve (>=3 points = full curve)
    q_points: list[float] = field(default_factory=list)
    h_points: list[float] = field(default_factory=list)

    # Physical
    dn_suction: int = 0
    dn_discharge: int = 0
    weight_kg: float = 0.0
    voltage: str = ""
    phases: int = 3
    stages: int = 0

    # Metadata
    source_file: str = ""
    source_page: int = 0
    data_source: str = ""
    confidence: float = 0.0
    recipe_id: str = ""
    warnings: list[str] = field(default_factory=list)

    def has_curve(self) -> bool:
        return len(self.q_points) >= 3 and len(self.h_points) >= 3

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PDFDocument:
    """Loaded PDF with metadata."""
    path: str = ""
    hash: str = ""
    num_pages: int = 0
    producer: str = ""
    creator: str = ""
    title: str = ""
    file_size_mb: float = 0.0
    is_scanned: bool = False

    _doc: object = field(default=None, repr=False)  # fitz.Document reference


@dataclass
class ClassifiedPage:
    """A page with its classification result."""
    page_num: int = 0
    page_type: PageType = PageType.OTHER
    text: str = ""
    confidence: float = 0.0
    table_type: Optional[TableType] = None


@dataclass
class ExtractedTable:
    """Raw table extracted from a page."""
    headers: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    bbox: tuple = ()       # (x0, y0, x1, y1)
    page_num: int = 0
    strategy_used: str = ""


@dataclass
class TextLine:
    """Single line of text with position info."""
    text: str = ""
    y: float = 0.0
    x0: float = 0.0
    font_size: float = 0.0
    is_bold: bool = False


@dataclass
class TextBlock:
    """Group of text lines forming a logical block."""
    lines: list[TextLine] = field(default_factory=list)
    bbox: tuple = ()
    is_table_like: bool = False


@dataclass
class ExtractionResult:
    """Result from a single extractor on a single page."""
    entries: list[PumpEntry] = field(default_factory=list)
    score: float = 0.0
    extractor_type: str = ""
    page_num: int = 0
    warnings: list[str] = field(default_factory=list)


@dataclass
class PageReport:
    """Parse report for a single page."""
    page_num: int = 0
    page_type: PageType = PageType.OTHER
    extractor_used: str = ""
    models_found: int = 0
    avg_confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)


@dataclass
class ParseReport:
    """Detailed parse quality report."""
    per_page: list[PageReport] = field(default_factory=list)
    total_models: int = 0
    avg_confidence: float = 0.0
    physics_pass_rate: float = 0.0
    extractor_breakdown: dict[str, int] = field(default_factory=dict)
    self_heal_applied: bool = False
    vision_ai_pages: int = 0
    vision_ai_cost_usd: float = 0.0


@dataclass
class ParseResult:
    """Complete result of parsing a PDF."""
    entries: list[PumpEntry] = field(default_factory=list)
    source: str = ""
    recipe_used: Optional[str] = None
    pages_processed: int = 0
    pages_skipped: int = 0
    extraction_time_s: float = 0.0
    avg_confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)
    report: Optional[ParseReport] = None

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "recipe_used": self.recipe_used,
            "pages_processed": self.pages_processed,
            "pages_skipped": self.pages_skipped,
            "extraction_time_s": round(self.extraction_time_s, 2),
            "avg_confidence": round(self.avg_confidence, 3),
            "total_models": len(self.entries),
            "warnings": self.warnings,
            "pumps": [e.to_dict() for e in self.entries],
        }
