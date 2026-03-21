"""Type 3: Curve Table extractor — shared Q-row + model/H blocks.

Handles formats like:
  Q (м3/ч):  0   3   4   5   6
  PV(n) 2-6   1.1   H (м)   69  65  53  45  18
  PV(n) 2-7   1.1            82  75  65.5 52  25

Also handles tabular variant where Q row and model/H data are in an ExtractedTable.

Generalized from v10 parse_cdm(), parse_chlf(), parse_cdlf(), parse_pv().
"""

import re
import logging
from typing import Optional

from pump_parser.models import PumpEntry, ExtractedTable, ExtractionResult
from pump_parser.extractors.base import BaseExtractor

log = logging.getLogger("pump_parser.extractors.curve_table")

# ─── Patterns ─────────────────────────────────────────────────────────────────

RE_Q_ROW = re.compile(
    r'Q\s*[\(\[]?\s*(?:м[³3]\s*/?\s*ч|m[³3]\s*/?\s*h|m3\s*/?\s*h|л/мин|l/min)',
    re.IGNORECASE,
)

RE_H_MARKER = re.compile(
    r'H\s*[\(\[]?\s*(?:м|m)\s*[\)\]]?',
    re.IGNORECASE,
)

RE_MODEL = re.compile(
    r'^([A-ZА-ЯЁ][A-ZА-ЯЁa-zа-яё()]{1,10}[\s\-]?\d[\d/\-_.a-zA-Zа-яА-ЯёЁ()\s]{0,30}?)\s*$',
    re.MULTILINE,
)

RE_SERIES = re.compile(r'^([A-ZА-ЯЁa-zа-яё]{2,}(?:[\s\-]?[A-ZА-ЯЁa-zа-яё]+)?)', re.UNICODE)

RE_RPM = re.compile(r'(\d{4})\s*(?:об/мин|rpm)', re.IGNORECASE)


def _parse_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = str(s).strip().replace(",", ".").replace("\xa0", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _extract_numbers(text: str) -> list[float]:
    """Extract all numbers from a text string."""
    nums = []
    for m in re.finditer(r'[\d]+[.,]?\d*', text):
        v = _parse_float(m.group())
        if v is not None:
            nums.append(v)
    return nums


def _extract_series(model: str) -> str:
    m = RE_SERIES.match(model.strip())
    return m.group(1).strip().rstrip("-") if m else ""


class CurveTableExtractor(BaseExtractor):
    """Extract pump data from curve-table format (shared Q-row + model/H blocks)."""

    type_name = "curve_table"

    def extract(
        self,
        page_text: str,
        tables: list[ExtractedTable],
        page_num: int = 0,
        source_file: str = "",
    ) -> ExtractionResult:
        entries: list[PumpEntry] = []
        warnings: list[str] = []

        # Detect RPM from page
        rpm_m = RE_RPM.search(page_text)
        rpm = int(rpm_m.group(1)) if rpm_m else 0

        # Strategy 1: Parse from extracted tables
        for table in tables:
            table_entries = self._extract_from_table(table, page_num, source_file, rpm)
            if table_entries:
                entries.extend(table_entries)

        # Strategy 2: Parse from raw text (state machine)
        if not entries:
            text_entries = self._extract_from_text(page_text, page_num, source_file, rpm)
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

    def _extract_from_table(
        self,
        table: ExtractedTable,
        page_num: int,
        source_file: str,
        rpm: int,
    ) -> list[PumpEntry]:
        """Extract from table where first row(s) contain Q and subsequent rows contain model + H."""
        if len(table.rows) < 3:
            return []

        entries = []

        # Find Q row: row containing Q header + numeric values
        q_row_idx = None
        q_values: list[float] = []
        q_start_col = 0

        for idx, row in enumerate(table.rows[:5]):
            row_text = " ".join(str(c) for c in row)
            if RE_Q_ROW.search(row_text):
                # Find where numbers start
                for ci, cell in enumerate(row):
                    v = _parse_float(str(cell))
                    if v is not None and v >= 0:
                        if not q_values:
                            q_start_col = ci
                        q_values.append(v)
                if len(q_values) >= 3:
                    q_row_idx = idx
                    break
                q_values = []

        # Also check headers
        if q_row_idx is None:
            header_text = " ".join(table.headers)
            if RE_Q_ROW.search(header_text):
                for ci, h in enumerate(table.headers):
                    v = _parse_float(str(h))
                    if v is not None and v >= 0:
                        if not q_values:
                            q_start_col = ci
                        q_values.append(v)
                if len(q_values) >= 3:
                    q_row_idx = -1  # headers are Q row

        if not q_values or len(q_values) < 3:
            return []

        # Parse model/H rows after Q row
        start = (q_row_idx + 1) if q_row_idx >= 0 else 0
        for row in table.rows[start:]:
            if len(row) < q_start_col + 2:
                continue

            # Skip H marker rows, unit rows
            row_text = " ".join(str(c) for c in row[:q_start_col])
            if RE_H_MARKER.match(row_text.strip()):
                continue

            # Find model name (leftmost non-empty non-numeric cell)
            model = ""
            power = 0.0
            for ci in range(min(q_start_col, len(row))):
                cell = str(row[ci]).strip()
                if not cell or cell.lower() in ("none", "nan", "-"):
                    continue
                v = _parse_float(cell)
                if v is not None:
                    # Could be power
                    if 0.03 <= v <= 500 and not model:
                        continue
                    if 0.03 <= v <= 500 and model:
                        power = v
                else:
                    # Text cell — likely model name
                    if len(cell) >= 3:
                        model = cell

            if not model:
                continue

            # Extract H values at Q columns
            h_points = []
            valid_q = []
            for qi, q_val in enumerate(q_values):
                ci = q_start_col + qi
                if ci < len(row):
                    hv = _parse_float(str(row[ci]))
                    if hv is not None and hv > 0:
                        h_points.append(hv)
                        valid_q.append(q_val)

            if len(h_points) < 2:
                continue

            # Find power in meta columns
            if power <= 0:
                for ci in range(min(q_start_col, len(row))):
                    cell = str(row[ci]).strip()
                    v = _parse_float(cell)
                    if v is not None and 0.03 <= v <= 500:
                        power = v
                        break

            # BEP nominal
            q_max = max(valid_q) if valid_q else 0
            bep_target = q_max * 0.65
            bep_idx = min(range(len(valid_q)), key=lambda i: abs(valid_q[i] - bep_target))

            entries.append(PumpEntry(
                model=model.replace("\n", " ").strip(),
                series=_extract_series(model),
                q_nom=round(valid_q[bep_idx], 2),
                h_nom=round(h_points[bep_idx], 2),
                power_kw=power,
                rpm=rpm,
                q_points=[round(q, 2) for q in valid_q],
                h_points=[round(h, 2) for h in h_points],
                source_file=source_file,
                source_page=page_num,
                data_source=self.type_name,
                confidence=0.85 if len(h_points) >= 3 else 0.65,
            ))

        return entries

    def _extract_from_text(
        self,
        text: str,
        page_num: int,
        source_file: str,
        rpm: int,
    ) -> list[PumpEntry]:
        """Extract from raw text using state machine.

        State machine:
        1. FIND_Q_ROW: look for Q header line with numbers
        2. PARSE_MODELS: parse model name + power + H values
        3. Back to FIND_Q_ROW when hitting next Q header or end
        """
        lines = text.split("\n")
        entries = []
        q_values: list[float] = []
        i = 0

        while i < len(lines):
            line = lines[i].strip()

            # Look for Q row
            if RE_Q_ROW.search(line):
                nums = _extract_numbers(line)
                if len(nums) >= 3:
                    q_values = nums
                    i += 1
                    continue
                # Q values might be on the next line
                if i + 1 < len(lines):
                    next_nums = _extract_numbers(lines[i + 1])
                    if len(next_nums) >= 3:
                        q_values = next_nums
                        i += 2
                        continue

            # If we have Q values, look for model/H blocks
            if q_values and line:
                # Check if this line has a model name
                # Model line pattern: ModelName  Power  H(м)  h1 h2 h3 h4...
                # or: ModelName
                #     Power   H(м)   h1 h2 h3...
                nums_in_line = _extract_numbers(line)

                # Skip pure H marker lines
                if RE_H_MARKER.match(line) and len(nums_in_line) < 3:
                    i += 1
                    continue

                # Try to parse as model line with H values
                # Extract non-numeric part as model name
                parts = re.split(r'\s{2,}', line)
                model = ""
                power = 0.0
                h_values: list[float] = []

                for part in parts:
                    part = part.strip()
                    if not part:
                        continue
                    v = _parse_float(part)
                    if v is None:
                        # Text — model name or H marker
                        if not RE_H_MARKER.match(part) and len(part) >= 3:
                            model = part
                    else:
                        if not model:
                            continue  # numbers before model name
                        if power <= 0 and 0.03 <= v <= 500 and not h_values:
                            power = v
                        else:
                            h_values.append(v)

                # If model found with enough H values matching Q length
                if model and len(h_values) >= 3:
                    # Trim H values to match Q length
                    h_trim = h_values[:len(q_values)]
                    q_trim = q_values[:len(h_trim)]

                    if len(h_trim) >= 3:
                        q_max = max(q_trim) if q_trim else 0
                        bep_target = q_max * 0.65
                        bep_idx = min(range(len(q_trim)), key=lambda j: abs(q_trim[j] - bep_target))

                        entries.append(PumpEntry(
                            model=model.replace("\n", " ").strip(),
                            series=_extract_series(model),
                            q_nom=round(q_trim[bep_idx], 2),
                            h_nom=round(h_trim[bep_idx], 2),
                            power_kw=power,
                            rpm=rpm,
                            q_points=[round(q, 2) for q in q_trim],
                            h_points=[round(h, 2) for h in h_trim],
                            source_file=source_file,
                            source_page=page_num,
                            data_source="curve_table_text",
                            confidence=0.80,
                        ))

            i += 1

        return entries
