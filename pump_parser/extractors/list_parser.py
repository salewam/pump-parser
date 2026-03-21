"""Type 6: List/Manual extractor — sequential line parsing.

Handles formats where pump specs appear as sequential lines or
model-list tables with minimal structure, e.g.:

  TD32-14G/2    8    14    2900    0.75
  TD40-14G/2    8    14            0.75

Also handles numbered lists:
  1. TD32-14G/2 - Q=8 м³/ч, H=14 м, P=0.75 кВт

State machine approach:
- Scan for lines matching "MODEL  Q  H  [RPM]  [P]" patterns
- Accept table rows that look like model specs but weren't caught by other extractors
"""

import re
import logging
from typing import Optional

from pump_parser.models import PumpEntry, ExtractedTable, ExtractionResult
from pump_parser.extractors.base import BaseExtractor

log = logging.getLogger("pump_parser.extractors.list_parser")

RE_SERIES = re.compile(r'^([A-ZА-ЯЁa-zа-яё]{2,}(?:[\s\-]?[A-ZА-ЯЁa-zа-яё]+)?)', re.UNICODE)

# Model name pattern: starts with 2+ letters, has digits, may have /-()
RE_MODEL_NAME = re.compile(
    r'([A-ZА-ЯЁ][A-ZА-ЯЁa-zа-яё()]{1,10}[\s\-_]?\d[\d/\-_.a-zA-Zа-яА-ЯёЁ()]{0,30})',
)

# Line pattern: Model  Q  H  [RPM]  [P]  or  # Model Q H RPM P
RE_SPEC_LINE = re.compile(
    r'(?:^\d+\s*[.)]\s*)?'  # optional numbering
    r'([A-ZА-ЯЁ][A-ZА-ЯЁa-zа-яё()]{1,10}[\s\-_]?\d[\d/\-_.a-zA-Zа-яА-ЯёЁ()\s]{0,30}?)'
    r'\s+'
    r'([\d,.]+)\s+'     # Q
    r'([\d,.]+)'        # H
    r'(?:\s+([\d,.]+))?' # optional RPM or P
    r'(?:\s+([\d,.]+))?' # optional P
    r'(?:\s+([\d,.]+))?' # optional extra column
    ,
    re.MULTILINE,
)

RE_RPM = re.compile(r'(\d{4})\s*(?:об/мин|rpm)', re.IGNORECASE)

# Key=value inline patterns
RE_Q_INLINE = re.compile(r'Q\s*[=:]\s*([\d,.]+)\s*(?:м[³3]/ч|m[³3]/h)?', re.IGNORECASE)
RE_H_INLINE = re.compile(r'H\s*[=:]\s*([\d,.]+)\s*(?:м|m)?', re.IGNORECASE)
RE_P_INLINE = re.compile(r'P\s*[=:]\s*([\d,.]+)\s*(?:кВт|kW)?', re.IGNORECASE)


def _parse_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = str(s).strip().replace(",", ".").replace("\xa0", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _extract_series(model: str) -> str:
    m = RE_SERIES.match(model.strip())
    return m.group(1).strip().rstrip("-") if m else ""


class ListParserExtractor(BaseExtractor):
    """Extract pump data from list/manual format pages."""

    type_name = "list_parser"

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
        page_rpm = int(rpm_m.group(1)) if rpm_m else 0

        # Strategy 1: Parse from tables with simple structure (≤3 cols, many rows)
        for table in tables:
            if len(table.rows) < 3:
                continue
            if len(table.headers) <= 2 and len(table.rows) >= 5:
                # 1-2 column table: each row might have model + specs
                for row in table.rows:
                    row_text = " ".join(str(c) for c in row)
                    entry = self._parse_inline_spec(row_text, page_num, source_file, page_rpm)
                    if entry:
                        entries.append(entry)

        # Strategy 2: Parse from raw text line by line
        text_entries = self._extract_from_text(page_text, page_num, source_file, page_rpm)
        if text_entries:
            entries.extend(text_entries)

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

    def _extract_from_text(
        self,
        text: str,
        page_num: int,
        source_file: str,
        page_rpm: int,
    ) -> list[PumpEntry]:
        """Extract from raw text using regex line matching."""
        entries = []

        for m in RE_SPEC_LINE.finditer(text):
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            if len(model) < 3:
                continue

            val1 = _parse_float(m.group(2))
            val2 = _parse_float(m.group(3))
            val3 = _parse_float(m.group(4))
            val4 = _parse_float(m.group(5))
            val5 = _parse_float(m.group(6))

            if val1 is None or val2 is None:
                continue

            q = val1
            h = val2
            rpm = page_rpm
            power = 0.0

            # Heuristic: if val3 is 1000-3600 range, it's RPM
            if val3 is not None:
                if 800 <= val3 <= 3600:
                    rpm = int(val3)
                    if val4 is not None:
                        power = val4
                else:
                    power = val3

            # Validate basic ranges
            if q <= 0 or h <= 0:
                continue
            if q > 10000 or h > 1000:
                continue

            entries.append(PumpEntry(
                model=model,
                series=_extract_series(model),
                q_nom=round(q, 2),
                h_nom=round(h, 2),
                power_kw=power,
                rpm=rpm,
                source_file=source_file,
                source_page=page_num,
                data_source="list_text",
                confidence=0.70,
            ))

        return entries

    def _parse_inline_spec(
        self,
        text: str,
        page_num: int,
        source_file: str,
        page_rpm: int,
    ) -> Optional[PumpEntry]:
        """Parse inline specs like 'TD32-14G/2 Q=8 H=14 P=0.75'."""
        model_m = RE_MODEL_NAME.search(text)
        if not model_m:
            return None

        model = model_m.group(1).strip()
        q_m = RE_Q_INLINE.search(text)
        h_m = RE_H_INLINE.search(text)

        if not q_m or not h_m:
            return None

        q = _parse_float(q_m.group(1))
        h = _parse_float(h_m.group(1))
        if q is None or h is None or q <= 0 or h <= 0:
            return None

        p_m = RE_P_INLINE.search(text)
        power = _parse_float(p_m.group(1)) if p_m else 0.0

        return PumpEntry(
            model=model,
            series=_extract_series(model),
            q_nom=round(q, 2),
            h_nom=round(h, 2),
            power_kw=power or 0.0,
            rpm=page_rpm,
            source_file=source_file,
            source_page=page_num,
            data_source="list_inline",
            confidence=0.65,
        )
