"""Type 2: Q-H Matrix extractor — Q values in column headers, H values in cells.

Handles tables like:
  MODEL | DN | kW | HP | Amp | 0 | 5 | 10 | 15 | 20 | 25 | 30
  FVH1x2/0.3 | 25x25 | 0.25 | 0.33 | 0.7 | 17 | 16 | 15 | 11 | 6 | 2 | -

Generalized from v10 parse_cdlf_large() and parse_fst().
"""

import re
import logging
from typing import Optional

from pump_parser.models import PumpEntry, ExtractedTable, ExtractionResult
from pump_parser.extractors.base import BaseExtractor
from pump_parser.classifiers.column_classifier import detect_qh_matrix, parse_qh_matrix

log = logging.getLogger("pump_parser.extractors.qh_matrix")

# Series extraction from model name
RE_SERIES = re.compile(r'^([A-ZА-ЯЁa-zа-яё]{2,}(?:[\s\-]?[A-ZА-ЯЁa-zа-яё]+)?)', re.UNICODE)

# RPM from page text
RE_RPM = re.compile(r'n\s*[=:]\s*(\d{3,4})\s*(?:rpm|об/мин|l/min|min|1/min)', re.IGNORECASE)
RE_RPM_ALT = re.compile(r'(\d{4})\s*(?:rpm|об/мин)', re.IGNORECASE)
RE_FREQ = re.compile(r'(\d{2})\s*Hz', re.IGNORECASE)


def _extract_series(model: str) -> str:
    m = RE_SERIES.match(model.strip())
    return m.group(1).strip().rstrip("-") if m else ""


def _detect_rpm(page_text: str) -> int:
    """Detect RPM from page text."""
    m = RE_RPM.search(page_text)
    if m:
        rpm = int(m.group(1))
        if 800 <= rpm <= 3600:
            return rpm

    m = RE_RPM_ALT.search(page_text)
    if m:
        rpm = int(m.group(1))
        if 800 <= rpm <= 3600:
            return rpm

    # Infer from frequency
    m = RE_FREQ.search(page_text)
    if m:
        freq = int(m.group(1))
        if freq == 50:
            # Check for 2-pole or 4-pole hints
            if "2900" in page_text or "2950" in page_text or "2-pole" in page_text:
                return 2900
            return 1450  # default 4-pole
        elif freq == 60:
            return 3500

    return 0


class QHMatrixExtractor(BaseExtractor):
    """Extract pump data from Q-H matrix tables."""

    type_name = "qh_matrix"

    def extract(
        self,
        page_text: str,
        tables: list[ExtractedTable],
        page_num: int = 0,
        source_file: str = "",
    ) -> ExtractionResult:
        entries: list[PumpEntry] = []
        warnings: list[str] = []

        rpm = _detect_rpm(page_text)

        for table in tables:
            if len(table.headers) < 6 or len(table.rows) < 2:
                continue

            # Detect Q-H matrix structure
            matrix_info = detect_qh_matrix(table.headers, table.rows)
            if matrix_info is None:
                continue

            # Parse matrix into pump dicts
            pump_dicts = parse_qh_matrix(table.headers, table.rows, matrix_info)
            if not pump_dicts:
                continue

            q_unit = matrix_info.get("q_unit", "m3/h")
            log.debug("QH matrix: %d pumps, q_unit=%s, q_start=%d",
                       len(pump_dicts), q_unit, matrix_info["q_start_col"])

            for pd in pump_dicts:
                model = pd.get("model", "").strip()
                if not model or len(model) < 2:
                    continue

                q_points = pd.get("q_points", [])
                h_points = pd.get("h_points", [])
                q_nom = pd.get("q_nom", 0.0)
                h_nom = pd.get("h_nom", 0.0)
                power_kw = pd.get("power_kw", 0.0)

                entry = PumpEntry(
                    model=model,
                    series=_extract_series(model),
                    q_nom=q_nom,
                    h_nom=h_nom,
                    power_kw=power_kw,
                    rpm=rpm,
                    q_points=q_points,
                    h_points=h_points,
                    source_file=source_file,
                    source_page=page_num,
                    data_source=self.type_name,
                    confidence=0.85 if len(q_points) >= 3 else 0.70,
                )
                entries.append(entry)

        # Deduplicate by model name
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
