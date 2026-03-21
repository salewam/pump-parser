#!/usr/bin/env python3
"""
CDM/CDMF Pump Data Parser V13
=============================
ИСПРАВЛЕНО: Правило 4 заменено на точный blacklist

V10 проблема: second > first * 2 фильтровал CDM1-3, CDM3-7, CDM5-11...
V13 решение: Только blacklist известных артефактов
"""

import fitz
import json
import re
import sys
import os
import logging
import gc
from typing import List, Dict, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class CDMParserPyMuPDF:

    # Только ИЗВЕСТНЫЕ артефакты (не валидные насосы!)
    GARBAGE_IDS = frozenset({
        'CDM8-24', 'CDM10-29', 'CDM11-75', 'CDM11-110',
        'CDM45-120', 'CDM50-110', 'CDM60-160', 'CDM80- 180',
        'CDM125-9*'
    })

    # Валидные серии
    VALID_SERIES = frozenset({1, 3, 5, 10, 15, 20, 32, 42, 65, 85, 95, 120, 125, 150, 155, 185, 200, 215})

    # Дубликаты для удаления
    DUPLICATE_RECORDS = frozenset({
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

    INLET_DIAMETERS = frozenset({0.37, 0.75, 1.1, 1.5, 3.0, 4.0, 5.5, 11.0})
    POWER_KW_VALUES = frozenset({4.0, 4.5, 5.5, 7.5, 11.0, 15.0, 18.5, 22.0, 30.0, 37.0, 45.0, 55.0, 75.0})

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.all_pumps = []

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
            except:
                return None
        return None

    def is_duplicate_record(self, record: Dict) -> bool:
        key = (record.get('id', ''), record.get('q', 0), record.get('head_m', 0), record.get('kw', 0))
        return key in self.DUPLICATE_RECORDS

    def is_garbage_pump(self, record: Dict) -> bool:
        """
        V13: Только blacklist + валидация серий
        БЕЗ агрессивного правила 4!
        """
        pump_id = record.get('id', '')
        q = record.get('q', 0)
        head = record.get('head_m', 0)
        kw = record.get('kw', 0)

        # 1. ID с пробелами
        if ' ' in pump_id:
            return True

        # 2. Blacklist известных артефактов
        if pump_id in self.GARBAGE_IDS:
            return True

        # 3. Невалидные серии (CDM8, CDM11, CDM45, CDM50, CDM60, CDM80)
        series_match = re.match(r'CDM[F]?(\d+)', pump_id)
        if series_match:
            series_num = int(series_match.group(1))
            if series_num not in self.VALID_SERIES:
                return True

        # 4. Диапазоны параметров
        if q < 0 or q > 500:
            return True
        if head < 0 or head > 250:
            return True
        if kw < 0.1 or kw > 200:
            return True

        # V13: Правило 4 УДАЛЕНО! (second > first * 2)
        # Оно фильтровало валидные насосы CDM1-3, CDM3-7, CDM5-11

        return False

    def is_metadata_record(self, record: Dict) -> bool:
        q = record.get('q', 0)
        head = record.get('head_m', 0)
        kw = record.get('kw', 0)
        pump_id = record.get('id', '')

        if q in self.INLET_DIAMETERS and head in self.POWER_KW_VALUES:
            return True
        if q == head == kw:
            return True
        if 'CDM95-3' in pump_id and q == 0.0:
            return True
        return False

    def quick_check_table(self, text_sample: str) -> Tuple[bool, Optional[str]]:
        if not text_sample or len(text_sample) < 50:
            return False, None

        sample = text_sample[:1500]
        sample_lower = sample.lower()

        table_keywords = ['таблица', 'table', 'характеристик', 'specifications']
        pump_keywords = ['cdm', 'cdmf', 'cmh', 'насос', 'pump']

        has_table = any(kw in sample_lower for kw in table_keywords)
        has_pump = any(kw in sample_lower for kw in pump_keywords)

        pump_patterns = [r'\d{2,3}[-−]\d+[*]?', r'CDM\s*\d{2,3}[-−]\d+', r'CDMF\d+\+CMH\d+-\d+']
        has_ids = any(re.search(p, sample) for p in pump_patterns)

        param_patterns = [r'Q\s*\(м[3³]/ч\)', r'H\s*\(м\)', r'кВт|kW']
        has_params = any(re.search(p, sample, re.IGNORECASE) for p in param_patterns)

        numbers = re.findall(r'\d+[.,]?\d*', sample)
        has_numbers = len(numbers) > 10

        if not ((has_table and has_pump) or (has_ids and has_params) or (has_numbers and has_pump and has_params)):
            return False, None

        if re.search(r'CDMF\d+\+CMH\d+-\d+', sample):
            return True, "CDMF+CMH"
        return True, "CDM"

    def parse_page_text(self, text: str, series_type: str) -> List[Dict]:
        lines = [l.strip() for l in text.split('\n') if l.strip()]

        # Поиск Q значений
        q_values = []
        q_section = False
        for line in lines:
            if 'Q (м3/ч)' in line or 'Q (м³/ч)' in line:
                q_section = True
                continue
            if q_section:
                q = self.parse_number(line)
                if q is not None and 0 <= q <= 500:
                    q_values.append(q)
                elif line and not line.replace(',', '.').replace('.', '').isdigit():
                    break

        if len(q_values) < 2:
            q_values = []
            sequences = []
            current = []
            for line in lines[:30]:
                if any(kw in line for kw in ['Таблица', 'CDM', 'Модель', '©']):
                    if current:
                        sequences.append(current)
                        current = []
                    continue
                q = self.parse_number(line)
                if q is not None and 10 <= q <= 500:
                    current.append(q)
                else:
                    if len(current) >= 2:
                        sequences.append(current)
                    current = []
            if sequences:
                q_values = max(sequences, key=len)

        if len(q_values) < 2:
            return []

        page_pumps = []
        skip = 0

        for i, line in enumerate(lines):
            if any(kw in line for kw in ['Outlet', 'Power', 'DN']):
                skip = 5

            line_clean = line.strip()
            id_pattern = r'\d+[-−\d АВЗабвзA-Za-z]*'

            match_prefix = re.match(rf'^(CDM[F]?\s*)({id_pattern})\*?$', line_clean)
            match_no_prefix = re.match(rf'^({id_pattern})\*?$', line_clean)

            if match_prefix:
                model_id = match_prefix.group(1).replace(' ', '') + match_prefix.group(2).replace('−', '-')
            elif match_no_prefix:
                model_id = "CDM" + match_no_prefix.group(1).replace('−', '-')
            else:
                continue

            model_id = model_id.replace('З', '3')
            if line_clean.endswith('*'):
                model_id += '*'

            if i + 1 >= len(lines):
                continue

            kw = self.parse_number(lines[i + 1])
            if not kw or kw < 0.1 or kw > 200:
                continue

            h_values = []
            start = i + 2
            while start < len(lines) and lines[start] in ['Н', '(м)']:
                start += 1

            for j in range(start, min(start + len(q_values) + 10, len(lines))):
                if skip > 0:
                    skip -= 1
                    continue

                if any(s in lines[j] for s in ['Outlet', 'Power', 'кВт', 'DN', 'Rp']):
                    skip = 5
                    continue

                h = self.parse_number(lines[j])
                if h is None:
                    if h_values and lines[j]:
                        break
                    continue

                if h in {1.0, 1.25, 1.5, 2.0, 2.5, 3.0}:
                    continue
                if h < 3 or h > 500:
                    continue

                # Проверка уменьшения Head
                if h_values and h > h_values[-1] + 10:
                    break

                h_values.append(h)
                if len(h_values) >= len(q_values):
                    break

            if len(h_values) >= 3:
                try:
                    s = model_id.replace('CDM', '').replace('CDMF', '').replace('*', '').split('-')[0]
                    series_num = int(''.join(c for c in s if c.isdigit())) if s else None
                except:
                    series_num = None

                for idx, h in enumerate(h_values):
                    if idx >= len(q_values):
                        break
                    q_val = q_values[idx]

                    if series_num and q_val == h == series_num:
                        continue

                    record = {"id": model_id, "kw": round(kw, 2), "q": q_val, "head_m": round(h, 1)}

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

        logger.info("=" * 60)
        logger.info("CDM PARSER V13 - Исправлен фильтр мусора")
        logger.info(f"Файл: {os.path.basename(self.pdf_path)}")
        logger.info("=" * 60)

        if not os.path.exists(self.pdf_path):
            raise FileNotFoundError(f"Файл не найден: {self.pdf_path}")

        pages_processed = 0
        pages_with_tables = 0
        total_pages = 0

        with fitz.open(self.pdf_path) as doc:
            total_pages = len(doc)
            logger.info(f"Страниц: {total_pages}\n")

            for page_num in range(total_pages):
                pages_processed += 1
                try:
                    text = doc[page_num].get_text()
                    if not text:
                        continue

                    has_table, series_type = self.quick_check_table(text)
                    if not has_table:
                        continue

                    pages_with_tables += 1
                    logger.info(f"[{page_num + 1}] Таблица {series_type}")

                    pumps = self.parse_page_text(text, series_type)
                    if pumps:
                        self.all_pumps.extend(pumps)
                        models = set(p['id'] for p in pumps)
                        logger.info(f"  {len(models)} моделей, {len(pumps)} записей")

                except Exception as e:
                    logger.error(f"  Ошибка: {e}")

                if page_num % 10 == 0:
                    gc.collect()

        # Дедупликация
        seen = set()
        unique = []
        for p in self.all_pumps:
            key = (p['id'], p['kw'], p['q'], p['head_m'])
            if key not in seen:
                seen.add(key)
                unique.append(p)
        self.all_pumps = unique

        logger.info("\n" + "=" * 60)
        logger.info("ПАРСИНГ ЗАВЕРШЁН (V13)")
        logger.info("=" * 60)
        logger.info(f"Страниц: {pages_processed}/{total_pages}")
        logger.info(f"С таблицами: {pages_with_tables}")
        logger.info(f"Записей: {len(self.all_pumps)}")

        ids = set(p['id'] for p in self.all_pumps)
        logger.info(f"Моделей: {len(ids)}")
        for s in ['CDM1-', 'CDM3-', 'CDM5-', 'CDM125', 'CDM155', 'CDM185', 'CDM215']:
            c = len([i for i in ids if i.startswith(s)])
            logger.info(f"  {s}: {c}")

        return self.all_pumps

    def remove_duplicates(self):
        seen = set()
        unique = []
        for p in self.all_pumps:
            key = (p['id'], p['kw'], p['q'], p['head_m'])
            if key not in seen:
                seen.add(key)
                unique.append(p)
        self.all_pumps = unique


def extract_cdm_from_pdf(pdf_path: str) -> List[Dict]:
    parser = CDMParserPyMuPDF(pdf_path)
    return parser.parse()


def main():
    if len(sys.argv) < 2:
        print("Использование: python3 cdm_parser_v13.py <input.pdf> [output.json]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) >= 3 else f"{os.path.splitext(os.path.basename(pdf_path))[0]}_parsed.json"

    parser = CDMParserPyMuPDF(pdf_path)
    pumps = parser.parse()

    if pumps:
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(pumps, f, ensure_ascii=False, indent=2)
        logger.info(f"\nСохранено: {output}")


if __name__ == '__main__':
    main()
