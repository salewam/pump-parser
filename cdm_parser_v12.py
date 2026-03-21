#!/usr/bin/env python3
"""
CDM/CDMF Pump Data Parser - PyMuPDF VERSION V12
================================================
ИСПРАВЛЕНИЯ V12 (после регрессии V11):
1. MAX_KW увеличен до 200 кВт (CDM185/215 имеют до 160 кВт)
2. Возвращена проверка уменьшения Head (убирает мусорные данные)
3. Правило 4 полностью переписано - теперь только blacklist
4. Сохранены улучшения V11: float comparison, memory leak fix, constants

Установка: pip install PyMuPDF
"""

import fitz  # PyMuPDF
import json
import re
import sys
import os
import logging
import gc
import math
from typing import List, Dict, Optional, Tuple, Set, FrozenSet

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# КОНСТАНТЫ
# =============================================================================

DUPLICATE_RECORDS: FrozenSet[Tuple[str, float, float, float]] = frozenset({
    ("CDM95-2", 80.0, 50.5, 15.0), ("CDM95-2", 90.0, 47.0, 15.0),
    ("CDM95-2", 95.0, 45.0, 15.0), ("CDM95-2", 100.0, 42.8, 15.0),
    ("CDM95-2", 110.0, 37.5, 15.0), ("CDM95-2", 120.0, 31.0, 15.0),
    ("CDM95-3-2", 90.0, 57.5, 18.5), ("CDM95-3-2", 95.0, 54.0, 18.5),
    ("CDM95-3-2", 100.0, 50.0, 18.5), ("CDM95-3-2", 110.0, 41.5, 18.5),
    ("CDM95-3-2", 120.0, 31.5, 18.5),
    ("CDM95-3", 45.0, 87.0, 22.0), ("CDM95-3", 50.0, 86.0, 22.0),
    ("CDM95-3", 60.0, 83.5, 22.0), ("CDM95-3", 70.0, 80.5, 22.0),
    ("CDM95-3", 80.0, 76.5, 22.0), ("CDM95-3", 90.0, 71.0, 22.0),
    ("CDM95-3", 95.0, 68.0, 22.0), ("CDM95-3", 100.0, 64.5, 22.0),
    ("CDM95-3", 110.0, 56.5, 22.0), ("CDM95-3", 120.0, 47.0, 22.0),
    ("CDM95-4", 80.0, 102.5, 30.0), ("CDM95-4", 90.0, 95.0, 30.0),
    ("CDM95-4", 95.0, 91.0, 30.0), ("CDM95-4", 100.0, 86.5, 30.0),
    ("CDM95-4", 110.0, 76.0, 30.0), ("CDM95-4", 120.0, 63.5, 30.0),
    ("CDM95-6", 110.0, 112.0, 45.0), ("CDM95-6", 120.0, 92.0, 45.0),
    ("CDM95-7", 120.0, 104.0, 55.0),
    ("CDM95-8-2", 110.0, 133.0, 55.0), ("CDM95-8-2", 120.0, 105.0, 55.0),
    ("CDM150-5-1", 140.0, 99.0, 75.0), ("CDM150-5-1", 150.0, 93.5, 75.0),
    ("CDM150-5-1", 160.0, 87.0, 75.0), ("CDM150-5-1", 170.0, 80.0, 75.0),
})

# V12: Расширенный blacklist вместо правила 4
KNOWN_GARBAGE: FrozenSet[str] = frozenset({
    'CDM8-24', 'CDM10-29', 'CDM11-75', 'CDM11-110',
    'CDM45-120', 'CDM50-110', 'CDM60-160', 'CDM80- 180',
    'CDM125-9*',
    # Невалидные серии (артефакты парсинга)
    'CDM8', 'CDM11', 'CDM45', 'CDM50', 'CDM60', 'CDM80',
})

VALID_SERIES: FrozenSet[int] = frozenset({
    1, 3, 5, 10, 15, 20, 32, 42, 65, 85, 95, 120, 125, 150, 155, 185, 200, 215
})

INLET_DIAMETERS: FrozenSet[float] = frozenset({0.37, 0.75, 1.1, 1.5, 3.0, 4.0, 5.5, 11.0})
POWER_KW_VALUES: FrozenSet[float] = frozenset({
    4.0, 4.5, 5.5, 7.5, 11.0, 15.0, 18.5, 22.0, 30.0, 37.0, 45.0, 55.0, 75.0
})
OUTLET_D_VALUES: FrozenSet[float] = frozenset({1.0, 1.25, 1.5, 2.0, 2.5, 3.0})

# V12: ИСПРАВЛЕНО - диапазоны
MIN_KW = 0.1
MAX_KW = 200.0  # V12: Увеличено! CDM185/215 имеют до 160 кВт
MIN_Q = 0.0
MAX_Q = 500.0
MIN_HEAD = 0.0
MAX_HEAD = 250.0  # V12: Увеличено для больших насосов

FLOAT_TOLERANCE = 0.01


class CDMParserPyMuPDF:
    """ПАРСЕР CDM V12 - исправлена регрессия V11"""

    def __init__(self, pdf_path: str):
        if not pdf_path:
            raise ValueError("pdf_path не может быть пустым")
        self.pdf_path = pdf_path
        self.all_pumps: List[Dict] = []
        self.errors: List[str] = []

    def _float_in_set(self, value: float, values_set: FrozenSet[float]) -> bool:
        return any(math.isclose(value, v, rel_tol=FLOAT_TOLERANCE) for v in values_set)

    def _floats_equal(self, a: float, b: float) -> bool:
        return math.isclose(a, b, rel_tol=FLOAT_TOLERANCE)

    def parse_number(self, s: str) -> Optional[float]:
        if not s:
            return None
        s = str(s).strip()
        s = re.sub(r'(\d)\s+(\d)', r'\1\2', s)
        s = re.sub(r'(\d)\s+(\d)', r'\1\2', s)
        s = re.sub(r'(\d)\s+\.', r'\1.', s)
        s = re.sub(r'\.\s+(\d)', r'.\1', s)
        s = s.replace(',', '.')
        s = re.sub(r'[*\']', '', s)
        match = re.search(r'(\d+\.?\d*)', s)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
        return None

    def is_duplicate_record(self, record: Dict) -> bool:
        pump_id = record.get('id', '')
        q = record.get('q', 0)
        head = record.get('head_m', 0)
        kw = record.get('kw', 0)
        for dup_id, dup_q, dup_head, dup_kw in DUPLICATE_RECORDS:
            if (pump_id == dup_id and
                self._floats_equal(q, dup_q) and
                self._floats_equal(head, dup_head) and
                self._floats_equal(kw, dup_kw)):
                return True
        return False

    def is_garbage_pump(self, record: Dict) -> bool:
        """
        V12: Упрощённый фильтр мусора

        Изменения:
        - Убрано сложное правило 4 (вызывало регрессию)
        - Только blacklist + валидация серий + диапазоны
        """
        pump_id = record.get('id', '')
        q = record.get('q', 0)
        head = record.get('head_m', 0)
        kw = record.get('kw', 0)

        # Правило 1: ID с пробелами
        if ' ' in pump_id:
            return True

        # Правило 2: Blacklist
        if pump_id in KNOWN_GARBAGE:
            return True

        # Правило 3: Невалидные серии
        series_match = re.match(r'CDM[F]?(\d+)', pump_id)
        if series_match:
            series_num = int(series_match.group(1))
            if series_num not in VALID_SERIES:
                return True

        # V12: Правило 4 УДАЛЕНО (вызывало потерю валидных насосов)
        # Теперь полагаемся только на blacklist

        # Правило 5: Диапазоны (V12: расширены)
        if q < MIN_Q or q > MAX_Q:
            return True
        if head < MIN_HEAD or head > MAX_HEAD:
            return True
        if kw < MIN_KW or kw > MAX_KW:
            return True

        return False

    def is_metadata_record(self, record: Dict) -> bool:
        q = record.get('q', 0)
        head = record.get('head_m', 0)
        kw = record.get('kw', 0)
        pump_id = record.get('id', '')

        if self._float_in_set(q, INLET_DIAMETERS) and self._float_in_set(head, POWER_KW_VALUES):
            return True
        if self._floats_equal(q, head) and self._floats_equal(head, kw):
            return True
        if 'CDM95-3' in pump_id and self._floats_equal(q, 0.0):
            return True
        return False

    def quick_check_table(self, text_sample: str) -> Tuple[bool, Optional[str]]:
        if not text_sample or len(text_sample) < 50:
            return False, None

        sample = text_sample[:1500]
        sample_lower = sample.lower()

        table_keywords = ['таблица', 'table', 'характеристик', 'specifications',
                         'технические данные', 'technical data']
        pump_keywords = ['cdm', 'cdmf', 'cmh', 'насос', 'pump']

        has_table_header = any(kw in sample_lower for kw in table_keywords)
        has_pump_refs = any(kw in sample_lower for kw in pump_keywords)

        pump_id_patterns = [
            r'\d{2,3}[-−]\d+[*]?',
            r'CDM\s*\d{2,3}[-−]\d+',
            r'\d{2,3}[-−]\d+[-−][АВ]',
            r'\d{2,3}[-−]\d+[-−]\d+[АВ]',
            r'CDMF\d+\+CMH\d+-\d+',
        ]
        has_pump_ids = any(re.search(pattern, sample) for pattern in pump_id_patterns)

        param_patterns = [r'Q\s*\(м[3³]/ч\)', r'H\s*\(м\)', r'кВт|kW|Мощность|Power']
        has_params = any(re.search(pattern, sample, re.IGNORECASE) for pattern in param_patterns)

        numbers = re.findall(r'\d+[.,]?\d*', sample)
        has_many_numbers = len(numbers) > 10

        standard_match = has_table_header and has_pump_refs
        direct_match = has_pump_ids and has_params
        numeric_match = has_many_numbers and has_pump_refs and has_params

        if not (standard_match or direct_match or numeric_match):
            return False, None

        if re.search(r'CDMF\d+\+CMH\d+-\d+', sample):
            return True, "CDMF+CMH"
        if re.search(r'CDM/CDMF\d+', sample) or re.search(r'CDM\s*\d+', sample):
            return True, "CDM"
        if has_pump_ids:
            return True, "CDM"

        return False, None

    def parse_page_text(self, text: str, series_type: str) -> List[Dict]:
        lines = [l.strip() for l in text.split('\n') if l.strip()]

        q_values = []

        # Стратегия 1: Поиск после заголовка Q
        q_section = False
        for line in lines:
            if 'Q (м3/ч)' in line or 'Q (м³/ч)' in line:
                q_section = True
                continue
            if q_section:
                q = self.parse_number(line)
                if q is not None and MIN_Q <= q <= MAX_Q:
                    q_values.append(q)
                elif line and not line.replace(',', '.').replace('.', '').isdigit():
                    break

        # Стратегия 2: Поиск последовательности чисел
        if len(q_values) < 2:
            q_values = []
            potential_q_sequences = []
            current_sequence = []

            for i, line in enumerate(lines[:30]):
                if any(kw in line for kw in ['Таблица', 'характеристик', 'CDM', 'Модель', '©']):
                    if current_sequence:
                        potential_q_sequences.append(current_sequence)
                        current_sequence = []
                    continue

                q = self.parse_number(line)
                if q is not None and 10 <= q <= MAX_Q:
                    current_sequence.append(q)
                else:
                    if len(current_sequence) >= 2:
                        potential_q_sequences.append(current_sequence)
                    current_sequence = []

            if potential_q_sequences:
                q_values = max(potential_q_sequences, key=len)

        if not q_values or len(q_values) < 2:
            return []

        page_pumps = []
        skip_next_numbers = 0

        for i, line in enumerate(lines):
            if any(keyword in line for keyword in ['Outlet', 'Power', 'DN']):
                skip_next_numbers = 5

            line_clean = line.strip()
            id_pattern = r'\d+[-−\d АВЗабвзA-Za-z]*'

            model_match_with_prefix = re.match(rf'^(CDM[F]?\s*)({id_pattern})\*?$', line_clean)
            model_match_no_prefix = re.match(rf'^({id_pattern})\*?$', line_clean)

            if model_match_with_prefix:
                prefix = model_match_with_prefix.group(1).replace(' ', '')
                model_id = prefix + model_match_with_prefix.group(2).replace('−', '-')
            elif model_match_no_prefix:
                model_id = "CDM" + model_match_no_prefix.group(1).replace('−', '-')
            else:
                continue

            model_id = model_id.replace('З', '3')
            if line_clean.endswith('*'):
                model_id += '*'

            if i + 1 >= len(lines):
                continue

            kw = self.parse_number(lines[i + 1])
            if not kw or kw < MIN_KW or kw > MAX_KW:
                continue

            h_values = []
            start_idx = i + 2

            while start_idx < len(lines) and lines[start_idx] in ['Н', '(м)']:
                start_idx += 1

            for j in range(start_idx, min(start_idx + len(q_values) + 10, len(lines))):
                if skip_next_numbers > 0:
                    skip_next_numbers -= 1
                    continue

                line_text = lines[j]

                if any(skip in line_text for skip in ['Outlet', 'Power', 'кВт', 'Вт', 'DN', 'Rp']):
                    skip_next_numbers = 5
                    continue

                h = self.parse_number(line_text)

                if h is None:
                    if len(h_values) > 0 and line_text and not line_text.isspace():
                        break
                    continue

                if self._float_in_set(h, OUTLET_D_VALUES):
                    continue

                if h < 3 or h > 500:
                    continue

                # V12: ВОЗВРАЩЕНА проверка уменьшения Head
                # Это убирает мусорные данные
                if len(h_values) > 0:
                    last_h = h_values[-1]
                    if h > last_h + 10:
                        # Head вырос слишком сильно - вероятно это не данные таблицы
                        break

                h_values.append(h)

                if len(h_values) >= len(q_values):
                    break

            if len(h_values) >= 3:
                try:
                    series_str = model_id.replace('CDM', '').replace('CDMF', '').replace('*', '').split('-')[0]
                    series_str = ''.join(c for c in series_str if c.isdigit())
                    series_number = int(series_str) if series_str else None
                except (ValueError, IndexError):
                    series_number = None

                for idx, h in enumerate(h_values):
                    if idx < len(q_values):
                        q_val = q_values[idx]

                        if series_number is not None and self._floats_equal(q_val, h) and self._floats_equal(q_val, float(series_number)):
                            continue

                        record = {
                            "id": model_id,
                            "kw": round(kw, 2),
                            "q": q_val,
                            "head_m": round(h, 1)
                        }

                        if "-" not in record["id"]:
                            continue
                        if self.is_garbage_pump(record):
                            continue
                        if self.is_metadata_record(record):
                            continue
                        if self.is_duplicate_record(record):
                            continue

                        page_pumps.append(record)

        return page_pumps

    def parse(self) -> List[Dict]:
        self.all_pumps = []
        self.errors = []

        logger.info("="*60)
        logger.info("CDM PARSER V12 - исправлена регрессия V11")
        logger.info(f"Файл: {os.path.basename(self.pdf_path)}")
        logger.info("="*60)

        if not os.path.exists(self.pdf_path):
            raise FileNotFoundError(f"Файл не найден: {self.pdf_path}")
        if not os.path.isfile(self.pdf_path):
            raise IsADirectoryError(f"Путь указывает на директорию: {self.pdf_path}")

        pages_processed = 0
        pages_with_tables = 0
        pages_skipped = 0
        total_pages = 0

        try:
            with fitz.open(self.pdf_path) as doc:
                total_pages = len(doc)
                logger.info(f"Страниц в PDF: {total_pages}\n")

                for page_num in range(total_pages):
                    pages_processed += 1

                    try:
                        page = doc[page_num]
                        text = page.get_text()

                        if not text:
                            pages_skipped += 1
                            continue

                        has_table, series_type = self.quick_check_table(text)

                        if not has_table:
                            pages_skipped += 1
                            continue

                        pages_with_tables += 1
                        logger.info(f"[{page_num + 1}] Таблица {series_type}")

                        page_pumps = self.parse_page_text(text, series_type)

                        if page_pumps:
                            self.all_pumps.extend(page_pumps)
                            models = set(p['id'] for p in page_pumps)
                            logger.info(f"  {len(models)} моделей, {len(page_pumps)} записей")

                    except Exception as e:
                        error_msg = f"Страница {page_num + 1}: {e}"
                        logger.error(f"  Ошибка: {error_msg}")
                        self.errors.append(error_msg)

                    if page_num % 10 == 0:
                        gc.collect()

        except fitz.FileDataError as e:
            raise ValueError(f"Некорректный PDF файл: {e}")

        self.remove_duplicates()

        logger.info("\n" + "="*60)
        logger.info("ПАРСИНГ ЗАВЕРШЁН (V12)")
        logger.info("="*60)
        logger.info(f"Обработано: {pages_processed}/{total_pages}")
        logger.info(f"С таблицами: {pages_with_tables}")
        logger.info(f"Записей: {len(self.all_pumps)}")

        # Диагностика по сериям
        all_ids = set(p['id'] for p in self.all_pumps)
        target_series = {
            'CDM1-': 0, 'CDM3-': 0, 'CDM5-': 0,
            'CDM125': 0, 'CDM155': 0, 'CDM185': 0, 'CDM215': 0
        }
        for series in target_series:
            target_series[series] = len([id for id in all_ids if id.startswith(series)])

        logger.info("\nДиагностика по сериям:")
        for series, count in target_series.items():
            logger.info(f"  {series}: {count} моделей")

        logger.info("="*60)

        return self.all_pumps

    def remove_duplicates(self):
        seen = set()
        unique_pumps = []
        for pump in self.all_pumps:
            key = (pump['id'], round(pump['kw'], 2), round(pump['q'], 1), round(pump['head_m'], 1))
            if key not in seen:
                seen.add(key)
                unique_pumps.append(pump)
        self.all_pumps = unique_pumps


def extract_cdm_from_pdf(pdf_path: str) -> List[Dict]:
    parser = CDMParserPyMuPDF(pdf_path)
    return parser.parse()


def main():
    if len(sys.argv) < 2:
        print("Использование: python3 cdm_parser_v12.py <input.pdf> [output.json]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) >= 3 else f"{os.path.splitext(os.path.basename(pdf_path))[0]}_parsed.json"

    if not os.path.exists(pdf_path):
        logger.error(f"Файл не найден: {pdf_path}")
        sys.exit(1)

    parser = CDMParserPyMuPDF(pdf_path)
    pumps = parser.parse()

    if not pumps:
        logger.error("Данные не найдены!")
        sys.exit(1)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(pumps, f, ensure_ascii=False, indent=2)

    logger.info(f"\nСохранено: {output_path}")


if __name__ == '__main__':
    main()
