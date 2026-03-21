#!/usr/bin/env python3
"""
CDM/CDMF Pump Data Parser - ULTRA LIGHT VERSION
================================================
КРИТИЧЕСКАЯ ОПТИМИЗАЦИЯ ПАМЯТИ:
- Обработка PDF постранично БЕЗ загрузки всего файла
- Использование lazyloading для страниц
- Минимальное использование памяти
"""

import pdfplumber
import json
import re
import sys
import os
import logging
import gc
from typing import List, Dict, Optional, Tuple

# Минимальное логирование
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

logging.getLogger('pdfplumber').setLevel(logging.ERROR)
logging.getLogger('pdfminer').setLevel(logging.ERROR)


class CDMParserUltraLight:
    """
    УЛЬТРА-ЛЁГКИЙ ПАРСЕР с минимальным потреблением памяти
    """

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.all_pumps = []

    def parse_number(self, s: str) -> Optional[float]:
        """Парсинг числа из строки"""
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

    def quick_check_table(self, text_sample: str) -> Tuple[bool, Optional[str]]:
        """
        БЫСТРАЯ проверка наличия таблицы
        Проверяет только первые 800 символов текста
        """
        if not text_sample or len(text_sample) < 50:
            return False, None

        sample = text_sample[:800]

        # Проверка критериев
        has_table = 'Таблица' in sample and ('характеристик' in sample or 'CDM' in sample)
        if not has_table:
            return False, None

        # Определение типа
        if re.search(r'CDM/CDMF\d+', sample) or re.search(r'CDM\s*\d+', sample):
            return True, "CDM"
        if re.search(r'CDMF\d+\+CMH\d+-\d+', sample):
            return True, "CDMF+CMH"

        return False, None

    def find_q_values(self, lines: List[str]) -> Optional[List[float]]:
        """Поиск Q значений в таблице"""
        for line in lines:
            if any(kw in line for kw in ['Таблица', 'Модель', 'кВт', 'Характеристик']):
                continue

            potential_q = []
            for part in line.split():
                q = self.parse_number(part)
                if q is not None and 0 <= q <= 500:
                    potential_q.append(q)

            if len(potential_q) >= 5:
                return potential_q

        return None

    def parse_cdm_line(self, line: str, q_values: List[float]) -> List[Dict]:
        """Парсинг строки CDM"""
        match = re.match(r'^(\d+[-−][^\s]+)\s+(\d+[.,]?\d*)\s+(.+)$', line.strip())
        if not match:
            return []

        pump_id = match.group(1).replace('−', '-').replace('З', '3')
        kw = self.parse_number(match.group(2))
        if not kw or kw <= 0 or kw > 500:
            return []

        h_values = []
        for part in match.group(3).split():
            h = self.parse_number(part)
            if h is not None and 0 < h < 600:
                h_values.append(h)

        pumps = []
        for i, h in enumerate(h_values):
            if i < len(q_values):
                pumps.append({
                    "id": pump_id,
                    "kw": round(kw, 2),
                    "q": q_values[i],
                    "head_m": round(h, 1)
                })

        return pumps

    def parse_cdmf_cmh_line(self, line: str, q_values: List[float]) -> List[Dict]:
        """Парсинг строки CDMF+CMH"""
        fixed = line.strip()
        fixed = re.sub(r'CM\s+H', 'CMH', fixed)
        fixed = re.sub(r'(\d)\s+\.(\d)', r'\1.\2', fixed)
        fixed = re.sub(r'\.\s+(\d)', r'.\1', fixed)

        match = re.match(r'^(CDMF\d+-\d+\+CMH\d+-\d+)\s+([\d.,]+\+[\d.,]+)\s+(.+)$', fixed)
        if not match:
            return []

        pump_id = match.group(1)
        parts = match.group(2).split('+')
        kw = sum(self.parse_number(p) or 0 for p in parts)

        if not kw or kw <= 0 or kw > 500:
            return []

        h_str = match.group(3)
        h_str = re.sub(r'(\d)\s+(\d)(?=\s|$)', r'\1\2', h_str)

        h_values = []
        for part in h_str.split():
            h = self.parse_number(part)
            if h is not None and 0 < h < 600:
                h_values.append(h)

        pumps = []
        for i, h in enumerate(h_values):
            if i < len(q_values):
                pumps.append({
                    "id": pump_id,
                    "kw": round(kw, 2),
                    "q": q_values[i],
                    "head_m": round(h, 1)
                })

        return pumps

    def parse_page_text(self, text: str, series_type: str) -> List[Dict]:
        """Парсинг текста страницы"""
        lines = text.split('\n')

        # Найти Q значения
        q_values = self.find_q_values(lines)
        if not q_values:
            return []

        # Парсить строки
        page_pumps = []
        for line in lines:
            if any(kw in line for kw in ['Таблица', 'Модель', 'кВт', 'Характеристик']):
                continue

            if series_type == "CDM":
                pumps = self.parse_cdm_line(line, q_values)
            elif series_type == "CDMF+CMH":
                pumps = self.parse_cdmf_cmh_line(line, q_values)
            else:
                continue

            page_pumps.extend(pumps)

        return page_pumps

    def parse(self) -> List[Dict]:
        """
        УЛЬТРА-ЛЁГКИЙ ПАРСИНГ
        Открывает PDF один раз, но обрабатывает страницы с немедленным освобождением памяти
        """
        logger.info("="*60)
        logger.info(f"CDM PARSER ULTRALIGHT")
        logger.info(f"Файл: {os.path.basename(self.pdf_path)}")
        logger.info("="*60)

        pages_processed = 0
        pages_with_tables = 0
        pages_skipped = 0

        try:
            # Открываем PDF
            with pdfplumber.open(self.pdf_path) as pdf:
                total_pages = len(pdf.pages)
                logger.info(f"Страниц в PDF: {total_pages}\n")

                # КРИТИЧЕСКАЯ ОПТИМИЗАЦИЯ: обрабатываем страницы с НЕМЕДЛЕННЫМ освобождением
                for page_num in range(total_pages):
                    pages_processed += 1

                    # Загружаем ТОЛЬКО текущую страницу
                    page = pdf.pages[page_num]

                    # БЫСТРАЯ проверка - извлекаем МИНИМУМ текста
                    try:
                        text_sample = page.extract_text()
                        if not text_sample:
                            pages_skipped += 1
                            del page
                            gc.collect()  # Принудительная сборка мусора
                            continue

                        # Проверяем первые 800 символов
                        has_table, series_type = self.quick_check_table(text_sample)

                        if not has_table:
                            pages_skipped += 1
                            # НЕМЕДЛЕННО освобождаем память
                            del text_sample
                            del page
                            gc.collect()
                            continue

                        # Есть таблица - парсим
                        pages_with_tables += 1
                        logger.info(f"[{page_num + 1}] 📊 Таблица {series_type}")

                        # Парсим текст страницы
                        page_pumps = self.parse_page_text(text_sample, series_type)

                        if page_pumps:
                            self.all_pumps.extend(page_pumps)
                            models = set(p['id'] for p in page_pumps)
                            logger.info(f"  ✓ {len(models)} моделей, {len(page_pumps)} записей")

                    except Exception as e:
                        logger.error(f"  ❌ Ошибка: {e}")

                    finally:
                        # КРИТИЧНО: освобождаем память после КАЖДОЙ страницы
                        if 'text_sample' in locals():
                            del text_sample
                        if 'page_pumps' in locals():
                            del page_pumps
                        if 'page' in locals():
                            del page

                        # Принудительная сборка мусора каждые 10 страниц
                        if page_num % 10 == 0:
                            gc.collect()

        except Exception as e:
            logger.error(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            raise

        # Удаление дубликатов
        self.remove_duplicates()

        # Статистика
        logger.info("\n" + "="*60)
        logger.info("✅ ПАРСИНГ ЗАВЕРШЁН")
        logger.info("="*60)
        logger.info(f"Обработано: {pages_processed}/{pages_processed}")
        logger.info(f"С таблицами: {pages_with_tables}")
        logger.info(f"Пропущено: {pages_skipped}")
        logger.info(f"Записей: {len(self.all_pumps)}")
        logger.info("="*60)

        return self.all_pumps

    def remove_duplicates(self):
        """Удаление дубликатов"""
        seen = set()
        unique_pumps = []

        for pump in self.all_pumps:
            key = (pump['id'], pump['kw'], pump['q'], pump['head_m'])
            if key not in seen:
                seen.add(key)
                unique_pumps.append(pump)

        self.all_pumps = unique_pumps


def extract_cdm_from_pdf(pdf_path):
    """
    УЛЬТРА-ЛЁГКИЙ парсинг CDM PDF

    Оптимизации:
    - Быстрая проверка (первые 800 символов)
    - Немедленное освобождение памяти после каждой страницы
    - Принудительная сборка мусора
    - Минимальное потребление памяти

    Args:
        pdf_path: путь к PDF

    Returns:
        [{"id": "1-2", "kw": 0.37, "q": 0.0, "head_m": 11.5}, ...]
    """
    parser = CDMParserUltraLight(pdf_path)
    return parser.parse()


def main():
    if len(sys.argv) < 2:
        print("Использование: python3 cdm_parser_ultralight.py <input.pdf> [output.json]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) >= 3 else f"{os.path.splitext(os.path.basename(pdf_path))[0]}_parsed.json"

    if not os.path.exists(pdf_path):
        logger.error(f"❌ Файл не найден: {pdf_path}")
        sys.exit(1)

    parser = CDMParserUltraLight(pdf_path)
    pumps = parser.parse()

    if not pumps:
        logger.error("⚠️  Данные не найдены!")
        sys.exit(1)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(pumps, f, ensure_ascii=False, indent=2)

    logger.info(f"\n💾 Сохранено: {output_path}")


if __name__ == '__main__':
    main()
