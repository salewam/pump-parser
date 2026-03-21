"""Recipe data model — learned parse configuration for a catalog format.

A Recipe captures everything needed to parse a known PDF format:
- matching: how to recognize this format (filename, keywords, page signatures)
- extraction: which extractor to use, column mapping, patterns
- validation: expected value ranges for sanity checking
- quality: usage stats, confidence, success rate
"""

import json
import hashlib
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger("pump_parser.learning.recipe")


@dataclass
class MatchingConfig:
    """How to match an incoming PDF to this recipe."""

    # Filename patterns (fnmatch): ["*CMI*", "*cmi*"]
    filename_patterns: list[str] = field(default_factory=list)

    # Keywords that must appear in first N pages text
    manufacturer_keywords: list[str] = field(default_factory=list)

    # Series/model keywords: ["CMI", "NBS", "TD"]
    series_keywords: list[str] = field(default_factory=list)

    # Page signatures: text snippets that identify this format
    # e.g. ["Qном [м³/ч]", "Hном [м]", "P2 [кВт]"]
    page_signatures: list[str] = field(default_factory=list)

    # Language hint: "ru", "en", "de", "multi"
    language: str = ""

    # PDF producer/creator match
    producer_pattern: str = ""


@dataclass
class ExtractionConfig:
    """How to extract data from matched pages."""

    # Extractor type: "flat_table", "qh_matrix", "curve_table", "transposed", "list_parser"
    extractor_type: str = ""

    # Table extraction strategy: "auto", "fitz", "pdfplumber", "lines", "text"
    table_strategy: str = "auto"

    # Column mapping override: {"model": 0, "q_nom": 2, "h_nom": 3, "power_kw": 4}
    column_map: dict[str, int] = field(default_factory=dict)

    # Q header config for curve_table/qh_matrix
    q_header_pattern: str = ""

    # Model name regex pattern
    model_pattern: str = ""

    # RPM pattern or fixed value
    rpm_pattern: str = ""
    rpm_fixed: int = 0

    # Unit overrides: {"q": "m3/h", "h": "m", "p": "kw"}
    unit_overrides: dict[str, str] = field(default_factory=dict)

    # Page filter: which pages to process
    # "all", "data_only", or list of page numbers/ranges
    page_filter: str = "data_only"

    # Skip pages matching these types
    skip_page_types: list[str] = field(default_factory=list)


@dataclass
class ValidationConfig:
    """Expected value ranges for this catalog format."""

    q_range: tuple[float, float] = (0.0, 10000.0)
    h_range: tuple[float, float] = (0.0, 2500.0)
    p_range: tuple[float, float] = (0.0, 1000.0)
    rpm_expected: list[int] = field(default_factory=list)  # [1450, 2900]
    min_models_per_page: int = 0
    max_models_per_page: int = 200


@dataclass
class Recipe:
    """Complete parse recipe for a catalog format."""

    # Identity
    recipe_id: str = ""
    name: str = ""
    manufacturer: str = ""
    description: str = ""

    # Sub-configs
    matching: MatchingConfig = field(default_factory=MatchingConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)

    # Quality tracking
    confidence: float = 0.5
    uses_count: int = 0
    success_count: int = 0
    fail_count: int = 0
    consecutive_failures: int = 0
    last_used: str = ""
    created: str = ""

    # Source info
    source_file: str = ""        # original PDF that generated this recipe
    source_hash: str = ""        # SHA256 of that PDF
    auto_generated: bool = False

    def success_rate(self) -> float:
        if self.uses_count == 0:
            return 0.0
        return self.success_count / self.uses_count

    def record_use(self, success: bool) -> None:
        """Record a recipe usage result."""
        self.uses_count += 1
        self.last_used = datetime.utcnow().isoformat()
        if success:
            self.success_count += 1
            self.consecutive_failures = 0
        else:
            self.fail_count += 1
            self.consecutive_failures += 1

    def to_dict(self) -> dict:
        """Serialize to dict (JSON-compatible)."""
        d = asdict(self)
        # Convert tuples to lists for JSON
        d["validation"]["q_range"] = list(d["validation"]["q_range"])
        d["validation"]["h_range"] = list(d["validation"]["h_range"])
        d["validation"]["p_range"] = list(d["validation"]["p_range"])
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @classmethod
    def from_dict(cls, d: dict) -> "Recipe":
        """Deserialize from dict."""
        matching = MatchingConfig(**d.get("matching", {}))
        extraction = ExtractionConfig(**d.get("extraction", {}))

        val_d = d.get("validation", {})
        # Convert lists back to tuples
        for key in ("q_range", "h_range", "p_range"):
            if key in val_d and isinstance(val_d[key], list):
                val_d[key] = tuple(val_d[key])
        validation = ValidationConfig(**val_d)

        return cls(
            recipe_id=d.get("recipe_id", ""),
            name=d.get("name", ""),
            manufacturer=d.get("manufacturer", ""),
            description=d.get("description", ""),
            matching=matching,
            extraction=extraction,
            validation=validation,
            confidence=d.get("confidence", 0.5),
            uses_count=d.get("uses_count", 0),
            success_count=d.get("success_count", 0),
            fail_count=d.get("fail_count", 0),
            consecutive_failures=d.get("consecutive_failures", 0),
            last_used=d.get("last_used", ""),
            created=d.get("created", ""),
            source_file=d.get("source_file", ""),
            source_hash=d.get("source_hash", ""),
            auto_generated=d.get("auto_generated", False),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Recipe":
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def from_file(cls, path: str) -> "Recipe":
        with open(path, "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)

    @staticmethod
    def generate_id(manufacturer: str, name: str) -> str:
        """Generate deterministic recipe ID."""
        raw = f"{manufacturer.lower()}_{name.lower()}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]
