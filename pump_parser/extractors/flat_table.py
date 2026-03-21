"""Type 1: Flat Table extractor — one row per pump model.

Handles tables like:
  Model | Article | Q (m³/h) | H (m) | P2 (kW)
  CMI 1-20BT | 12345678 | 1.0 | 53 | 0.55

Generalized from v10 parse_nominal(). No hardcoded series names.
"""

import re
import logging
from typing import Optional

from pump_parser.models import PumpEntry, ExtractedTable, ExtractionResult
from pump_parser.extractors.base import BaseExtractor
from pump_parser.classifiers.column_classifier import (
    classify_columns,
    dataframe_to_pump_dicts,
)
from pump_parser.classifiers.unit_detector import detect_units, convert_to_standard

log = logging.getLogger("pump_parser.extractors.flat_table")

# Regex for extracting series from model name
RE_SERIES = re.compile(r'^([A-ZА-ЯЁa-zа-яё]{2,}(?:[\s\-]?[A-ZА-ЯЁa-zа-яё]+)?)', re.UNICODE)

# Fallback regex: Model Article Q H P in raw text (CMI/NBS format)
RE_NOMINAL = re.compile(
    r'([A-ZА-ЯЁ][A-ZА-ЯЁa-zа-яё]{1,5}[\s\-_][\d][\d/\-_.a-zA-Z()\s]{1,40}?)\s+'
    r'(\d{6,9})\s+'
    r'([\d,.]+)\s+'
    r'([\d,.]+)\s+'
    r'([\d,.]+)',
    re.MULTILINE,
)


def _parse_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = str(s).strip().replace(",", ".").replace("\xa0", "").replace(" ", "")
    s = re.sub(r'\.$', '', s)
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _extract_series(model: str) -> str:
    """Extract series name from model: 'CMI 1-20BT' → 'CMI'."""
    m = RE_SERIES.match(model.strip())
    if m:
        series = m.group(1).strip().rstrip("-")
        return series
    return ""


class FlatTableExtractor(BaseExtractor):
    """Extract pump data from flat (row-per-model) tables."""

    type_name = "flat_table"

    def extract(
        self,
        page_text: str,
        tables: list[ExtractedTable],
        page_num: int = 0,
        source_file: str = "",
    ) -> ExtractionResult:
        entries: list[PumpEntry] = []
        warnings: list[str] = []

        # Strategy 1: Use extracted tables with column classifier
        for table in tables:
            if len(table.rows) < 2 or len(table.headers) < 3:
                continue

            classified = classify_columns(table.headers, table.rows, page_text)
            if not classified["is_pump_table"]:
                continue

            pump_dicts = dataframe_to_pump_dicts(classified, table.headers, table.rows)
            units = classified.get("units", {})

            for pd in pump_dicts:
                model = pd.get("model", "").strip()
                if not model or len(model) < 2:
                    continue

                entry = PumpEntry(
                    model=model,
                    series=_extract_series(model),
                    article=pd.get("article", ""),
                    q_nom=pd.get("q_nom", 0.0),
                    h_nom=pd.get("h_nom", 0.0),
                    power_kw=pd.get("power_kw", 0.0),
                    rpm=pd.get("rpm", 0),
                    stages=pd.get("stages", 0),
                    dn_suction=pd.get("dn", 0),
                    weight_kg=pd.get("weight", 0.0),
                    efficiency=pd.get("efficiency", 0.0),
                    source_file=source_file,
                    source_page=page_num,
                    data_source=self.type_name,
                    confidence=0.85,
                )
                entries.append(entry)

            if entries:
                if classified.get("warnings"):
                    warnings.extend(classified["warnings"])

        # Strategy 2: Regex fallback on raw text (CMI/NBS format)
        if not entries:
            text_entries = self._extract_from_text(page_text, page_num, source_file)
            if text_entries:
                entries = text_entries
                warnings.append("Used regex fallback on raw text")

        # Deduplicate by model name within page
        seen = set()
        unique = []
        for e in entries:
            key = e.model.strip().upper()
            if key not in seen:
                seen.add(key)
                unique.append(e)
        entries = unique

        result = ExtractionResult(
            entries=entries,
            score=0.0,
            extractor_type=self.type_name,
            page_num=page_num,
            warnings=warnings,
        )
        result.score = self.score(result, page_text)
        return result

    def _extract_from_text(
        self,
        text: str,
        page_num: int,
        source_file: str,
    ) -> list[PumpEntry]:
        """Fallback: extract from raw text using regex (Model Article Q H P)."""
        entries = []
        seen = set()

        for m in RE_NOMINAL.finditer(text):
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            article = m.group(2)
            q = _parse_float(m.group(3))
            h = _parse_float(m.group(4))
            p = _parse_float(m.group(5))

            if q is None or h is None:
                continue
            key = (model, article)
            if key in seen:
                continue
            seen.add(key)

            entries.append(PumpEntry(
                model=model,
                series=_extract_series(model),
                article=article,
                q_nom=q,
                h_nom=h,
                power_kw=p or 0.0,
                source_file=source_file,
                source_page=page_num,
                data_source="flat_table_regex",
                confidence=0.75,
            ))

        return entries
