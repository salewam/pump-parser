"""Type 4: Transposed Table extractor — parameters in rows, models in columns.

Handles tables like:
  Параметр          | TG 32  | TG 40  | TG 50
  Расход, м³/ч      | 8-12,5 | 12,5-25| 16-50
  Напор, м           | 18-50  | 16-48  | 12-70
  Мощность, кВт      | 1,1-5,5| 1,1-7,5| 1,1-18,5

Uses detect_spec_table + parse_spec_table from column_classifier.
"""

import re
import logging
from typing import Optional

from pump_parser.models import PumpEntry, ExtractedTable, ExtractionResult
from pump_parser.extractors.base import BaseExtractor
from pump_parser.classifiers.column_classifier import (
    detect_spec_table,
    parse_spec_table,
    detect_transposed,
    classify_columns,
)

log = logging.getLogger("pump_parser.extractors.transposed")

RE_SERIES = re.compile(r'^([A-ZА-ЯЁa-zа-яё]{2,}(?:[\s\-]?[A-ZА-ЯЁa-zа-яё]+)?)', re.UNICODE)
RE_RPM = re.compile(r'(\d{4})\s*(?:об/мин|rpm)', re.IGNORECASE)

_PARAM_KEYWORDS = {
    "модель", "model", "расход", "подача", "flow", "capacity",
    "напор", "head", "мощность", "power", "обороты", "speed", "rpm",
    "тип", "type", "насос", "pump", "delivery", "prevalenza",
}


def _extract_series(model: str) -> str:
    m = RE_SERIES.match(model.strip())
    return m.group(1).strip().rstrip("-") if m else ""


def _parse_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = str(s).strip().replace(",", ".").replace("\xa0", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_range_mid(s: str) -> Optional[float]:
    """Parse range like '8-12,5' → midpoint 10.25, or single number."""
    s = str(s).strip().replace(",", ".").replace("\xa0", "")
    m = re.match(r'([\d.]+)\s*[-–—]\s*([\d.]+)', s)
    if m:
        lo = float(m.group(1))
        hi = float(m.group(2))
        return round((lo + hi) / 2, 2)
    return _parse_float(s)


class TransposedExtractor(BaseExtractor):
    """Extract pump data from transposed tables (params in rows, models in columns)."""

    type_name = "transposed"

    def extract(
        self,
        page_text: str,
        tables: list[ExtractedTable],
        page_num: int = 0,
        source_file: str = "",
    ) -> ExtractionResult:
        entries: list[PumpEntry] = []
        warnings: list[str] = []

        rpm_m = RE_RPM.search(page_text)
        rpm = int(rpm_m.group(1)) if rpm_m else 0

        for table in tables:
            if len(table.rows) < 3 or len(table.headers) < 3:
                continue

            # Strategy 1: detect_spec_table (handles BM/CV/TG/TL transposed format)
            spec_info = detect_spec_table(table.headers, table.rows, page_text)
            if spec_info:
                # Fix: if model names came from headers but row 0 has real model names,
                # replace them (e.g. header="Модели", row0="TG 32")
                model_cols = spec_info["model_cols"]
                if table.rows:
                    for col_idx in list(model_cols.keys()):
                        header_name = model_cols[col_idx]
                        # Check if header is generic (e.g. "Модели", empty, "Col1")
                        if header_name.lower() in ("модели", "models", "") or len(header_name) <= 1:
                            if col_idx < len(table.rows[0]):
                                row0_val = str(table.rows[0][col_idx]).strip()
                                if row0_val and len(row0_val) >= 2:
                                    model_cols[col_idx] = row0_val
                        # Also prefer row 0 if it has model-like names (letters+digits)
                        elif col_idx < len(table.rows[0]):
                            row0_val = str(table.rows[0][col_idx]).strip()
                            if row0_val and re.search(r'[A-ZА-ЯЁ].*\d', row0_val):
                                model_cols[col_idx] = row0_val

                pump_dicts = parse_spec_table(table.headers, table.rows, spec_info)
                for pd in pump_dicts:
                    model = pd.get("model", "").strip()
                    if not model or len(model) < 2:
                        continue

                    q_nom = pd.get("q_nom", 0.0)
                    h_nom = pd.get("h_max_series", 0.0)
                    power = pd.get("power_kw", 0.0)
                    entry_rpm = pd.get("rpm", rpm)

                    entries.append(PumpEntry(
                        model=model,
                        series=_extract_series(model),
                        q_nom=q_nom,
                        h_nom=h_nom,
                        power_kw=power,
                        rpm=entry_rpm,
                        source_file=source_file,
                        source_page=page_num,
                        data_source=self.type_name,
                        confidence=0.75,  # series-level data, less precise
                    ))
                if entries:
                    continue

            # Strategy 2: Manual transposed detection — first col has param names
            first_col = [str(table.rows[i][0]).lower().strip()
                         for i in range(min(len(table.rows), 15))]
            param_matches = sum(1 for v in first_col if any(kw in v for kw in _PARAM_KEYWORDS))

            if param_matches < 2:
                continue

            # Identify param rows
            q_row = h_row = p_row = rpm_row = None
            for idx, val in enumerate(first_col):
                if any(kw in val for kw in ("расход", "подача", "flow", "capacity", "delivery")):
                    q_row = idx
                elif any(kw in val for kw in ("напор", "head", "prevalenza")):
                    h_row = idx
                elif any(kw in val for kw in ("мощность", "power", "potenza", "leistung")):
                    p_row = idx
                elif any(kw in val for kw in ("обороты", "speed", "rpm", "частота вращения")):
                    rpm_row = idx

            if q_row is None and h_row is None:
                continue

            # Models are in column headers (or first data row)
            for col_idx in range(1, len(table.headers)):
                model = str(table.headers[col_idx]).strip()
                if not model or len(model) < 2:
                    continue

                q = 0.0
                h = 0.0
                p = 0.0
                r = rpm

                if q_row is not None and col_idx < len(table.rows[q_row]):
                    q = _parse_range_mid(str(table.rows[q_row][col_idx])) or 0.0
                if h_row is not None and col_idx < len(table.rows[h_row]):
                    h = _parse_range_mid(str(table.rows[h_row][col_idx])) or 0.0
                if p_row is not None and col_idx < len(table.rows[p_row]):
                    p = _parse_range_mid(str(table.rows[p_row][col_idx])) or 0.0
                if rpm_row is not None and col_idx < len(table.rows[rpm_row]):
                    rv = _parse_float(str(table.rows[rpm_row][col_idx]))
                    if rv and 800 <= rv <= 3600:
                        r = int(rv)

                if q <= 0 and h <= 0:
                    continue

                entries.append(PumpEntry(
                    model=model,
                    series=_extract_series(model),
                    q_nom=round(q, 2),
                    h_nom=round(h, 2),
                    power_kw=round(p, 2),
                    rpm=r,
                    source_file=source_file,
                    source_page=page_num,
                    data_source=self.type_name,
                    confidence=0.75,
                ))

        # Deduplicate
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
