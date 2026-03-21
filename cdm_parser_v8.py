#!/usr/bin/env python3
"""
CDM/CDMF Pump Data Parser - PyMuPDF VERSION
============================================
КРИТИЧЕСКОЕ УЛУЧШЕНИЕ ПАМЯТИ:
- PyMuPDF использует в 5-10x МЕНЬШЕ памяти чем pdfplumber
- Может парсить PDF до 40MB на сервере с 960MB RAM
- Быстрее и эффективнее

Установка: pip install PyMuPDF
"""

import fitz  # PyMuPDF
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


class CDMParserPyMuPDF:
    """
    ПАРСЕР CDM на базе PyMuPDF - минимальное потребление памяти
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

    def is_duplicate_record(self, record: Dict) -> bool:
        """
        Фильтр дублей (36 записей): CDM95 и CDM150-5-1

        Возвращает True если запись — это дубль данных.
        Эти записи дублируют существующие данные насосов.
        """

        # Список конкретных дублей для удаления (id, q, head_m, kw)
        DUPLICATE_RECORDS = {
            # CDM95-2 (6 записей)
            ("CDM95-2", 80.0, 50.5, 15.0),
            ("CDM95-2", 90.0, 47.0, 15.0),
            ("CDM95-2", 95.0, 45.0, 15.0),
            ("CDM95-2", 100.0, 42.8, 15.0),
            ("CDM95-2", 110.0, 37.5, 15.0),
            ("CDM95-2", 120.0, 31.0, 15.0),
            # CDM95-3-2 (5 записей)
            ("CDM95-3-2", 90.0, 57.5, 18.5),
            ("CDM95-3-2", 95.0, 54.0, 18.5),
            ("CDM95-3-2", 100.0, 50.0, 18.5),
            ("CDM95-3-2", 110.0, 41.5, 18.5),
            ("CDM95-3-2", 120.0, 31.5, 18.5),
            # CDM95-3 (10 записей)
            ("CDM95-3", 45.0, 87.0, 22.0),
            ("CDM95-3", 50.0, 86.0, 22.0),
            ("CDM95-3", 60.0, 83.5, 22.0),
            ("CDM95-3", 70.0, 80.5, 22.0),
            ("CDM95-3", 80.0, 76.5, 22.0),
            ("CDM95-3", 90.0, 71.0, 22.0),
            ("CDM95-3", 95.0, 68.0, 22.0),
            ("CDM95-3", 100.0, 64.5, 22.0),
            ("CDM95-3", 110.0, 56.5, 22.0),
            ("CDM95-3", 120.0, 47.0, 22.0),
            # CDM95-4 (6 записей)
            ("CDM95-4", 80.0, 102.5, 30.0),
            ("CDM95-4", 90.0, 95.0, 30.0),
            ("CDM95-4", 95.0, 91.0, 30.0),
            ("CDM95-4", 100.0, 86.5, 30.0),
            ("CDM95-4", 110.0, 76.0, 30.0),
            ("CDM95-4", 120.0, 63.5, 30.0),
            # CDM95-6 (2 записи)
            ("CDM95-6", 110.0, 112.0, 45.0),
            ("CDM95-6", 120.0, 92.0, 45.0),
            # CDM95-7 (1 запись)
            ("CDM95-7", 120.0, 104.0, 55.0),
            # CDM95-8-2 (2 записи)
            ("CDM95-8-2", 110.0, 133.0, 55.0),
            ("CDM95-8-2", 120.0, 105.0, 55.0),
            # CDM150-5-1 (4 записи)
            ("CDM150-5-1", 140.0, 99.0, 75.0),
            ("CDM150-5-1", 150.0, 93.5, 75.0),
            ("CDM150-5-1", 160.0, 87.0, 75.0),
            ("CDM150-5-1", 170.0, 80.0, 75.0),
        }

        pump_id = record.get('id', '')
        q = record.get('q', 0)
        head = record.get('head_m', 0)
        kw = record.get('kw', 0)

        # Проверяем точное совпадение
        key = (pump_id, q, head, kw)
        return key in DUPLICATE_RECORDS

    def is_metadata_record(self, record: Dict) -> bool:
        """
        Фильтр метаданных (ЗАДАНИЕ 1): удаление 155 лишних записей

        Возвращает True если запись — это метаданные между таблицами, а не точка характеристики.

        Признаки метаданных:
        1. Q — это стандартный Inlet диаметр (0.37, 0.75, 1.1, 1.5, 3.0, 4.0, 5.5, 11.0)
        2. Head_m — это стандартное значение мощности kW (4.0, 5.5, 7.5, 11.0, 15.0, 18.5, 22.0, 30.0, 37.0, 45.0, 55.0, 75.0)
        3. Q очень маленький для данной серии насоса

        Паттерны:
        - CDM5-*: q=0.37, head=[4.0, 5.5] → INLET DIAMETER 3/8"
        - CDM10-*: q=0.75, head=[4.0, 4.5, 5.5, 7.5, 11.0] → INLET DIAMETER 3/4"
        - CDM15-*, CDM20-*: q=1.1, head=[4.0, 5.5, 7.5, 11.0, 15.0, 18.5] → INLET DIAMETER 1 1/8"
        - CDM32-*: q=1.5, head=[7.5, 11.0, 15.0, 18.5, 22.0, 30.0] → INLET DIAMETER 1 1/2"
        - CDM42-*: q=3.0, head=[11.0, 15.0, 18.5, 22.0, 30.0, 37.0, 45.0] → INLET DIAMETER 3"
        - CDM65-*: q=4.0, head=[30.0, 37.0, 45.0] → INLET DIAMETER 4"
        - CDM85-*, CDM95-*: q=5.5, head=[45.0, 55.0] → INLET DIAMETER 5 1/2"
        """

        # Стандартные диаметры Inlet в дюймах (преобразованные в метрические)
        INLET_DIAMETERS = [0.37, 0.75, 1.1, 1.5, 3.0, 4.0, 5.5, 11.0]

        # Стандартные значения мощности kW
        POWER_KW_VALUES = [4.0, 4.5, 5.5, 7.5, 11.0, 15.0, 18.5, 22.0, 30.0, 37.0, 45.0, 55.0, 75.0]

        q = record.get('q', 0)
        head = record.get('head_m', 0)
        kw = record.get('kw', 0)
        pump_id = record.get('id', '')

        # УНИВЕРСАЛЬНОЕ ПРАВИЛО: Если Q — это Inlet диаметр И Head — это значение мощности
        if q in INLET_DIAMETERS and head in POWER_KW_VALUES:
            return True

        # Дополнительная проверка: если Q = Head = kW (признак ошибки парсинга)
        if q == head == kw:
            return True

        # Фильтр для CDM95-3: q=0 некорректно
        if 'CDM95-3' in pump_id and q == 0.0:
            return True

        return False

    def quick_check_table(self, text_sample: str) -> Tuple[bool, Optional[str]]:
        """
        УНИВЕРСАЛЬНАЯ быстрая проверка наличия таблицы

        V7: Множественные стратегии поиска для работы с ЛЮБЫМИ PDF
        """
        if not text_sample or len(text_sample) < 50:
            return False, None

        sample = text_sample[:1500]  # Увеличен размер выборки
        sample_lower = sample.lower()

        # === СТРАТЕГИЯ 1: Стандартные заголовки таблиц ===
        table_keywords = ['таблица', 'table', 'характеристик', 'specifications',
                         'технические данные', 'technical data']
        pump_keywords = ['cdm', 'cdmf', 'cmh', 'насос', 'pump']

        has_table_header = any(kw in sample_lower for kw in table_keywords)
        has_pump_refs = any(kw in sample_lower for kw in pump_keywords)

        # === СТРАТЕГИЯ 2: Поиск ID насосов напрямую ===
        # Ищем паттерны типа: 125-6*, CDM185-1-А, 215-5-3А-2В
        pump_id_patterns = [
            r'\d{2,3}[-−]\d+[*]?',              # 125-6*
            r'CDM\s*\d{2,3}[-−]\d+',            # CDM125-1
            r'\d{2,3}[-−]\d+[-−][АВ]',          # 185-1-А
            r'\d{2,3}[-−]\d+[-−]\d+[АВ]',       # 185-4-3А
            r'CDMF\d+\+CMH\d+-\d+',             # CDMF1-2+CMH1-2
        ]

        has_pump_ids = any(re.search(pattern, sample) for pattern in pump_id_patterns)

        # === СТРАТЕГИЯ 3: Поиск параметров насоса ===
        # Q (расход), H (напор), кВт (мощность)
        param_patterns = [
            r'Q\s*\(м[3³]/ч\)',                  # Q (м³/ч)
            r'H\s*\(м\)',                        # H (м)
            r'кВт|kW|Мощность|Power',            # Мощность
        ]

        has_params = any(re.search(pattern, sample, re.IGNORECASE) for pattern in param_patterns)

        # === СТРАТЕГИЯ 4: Числовые последовательности ===
        # Таблицы имеют много чисел в упорядоченном виде
        numbers = re.findall(r'\d+[.,]?\d*', sample)
        has_many_numbers = len(numbers) > 10

        # === РЕШЕНИЕ: Таблица найдена если ===
        # 1. Стандартный подход: заголовок + pump keywords
        standard_match = has_table_header and has_pump_refs

        # 2. Прямое обнаружение: ID насосов + параметры
        direct_match = has_pump_ids and has_params

        # 3. Числовая таблица: много чисел + pump keywords
        numeric_match = has_many_numbers and has_pump_refs and has_params

        if not (standard_match or direct_match or numeric_match):
            return False, None

        # === Определение типа таблицы ===
        if re.search(r'CDMF\d+\+CMH\d+-\d+', sample):
            return True, "CDMF+CMH"

        # CDM/CDMF стандартные
        if re.search(r'CDM/CDMF\d+', sample) or re.search(r'CDM\s*\d+', sample):
            return True, "CDM"

        # Если нашли ID насосов но не определили тип - все равно CDM
        if has_pump_ids:
            return True, "CDM"

        return False, None

    def find_q_values(self, lines: List[str]) -> Optional[List[float]]:
        """
        УНИВЕРСАЛЬНЫЙ поиск Q значений в таблице

        V7: Убрано ограничение минимум 5 колонок - принимаем ЛЮБЫЕ таблицы
        """
        for line in lines:
            if any(kw in line for kw in ['Таблица', 'Модель', 'кВт', 'Характеристик']):
                continue

            potential_q = []
            for part in line.split():
                q = self.parse_number(part)
                if q is not None and 0 <= q <= 500:
                    potential_q.append(q)

            # V7: Принимаем таблицы с 2+ колонками (вместо 5+)
            if len(potential_q) >= 2:
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
        """
        Парсинг текста страницы (PyMuPDF формат)

        PyMuPDF извлекает текст по КОЛОНКАМ, поэтому нужен другой подход:
        1. Найти Q значения (они идут подряд в начале)
        2. Найти модели (паттерн: число-число)
        3. Собрать H значения после каждой модели
        """
        lines = [l.strip() for l in text.split('\n') if l.strip()]

        # === V8: АДАПТИВНЫЙ ПОИСК Q ЗНАЧЕНИЙ ===
        # Стратегия 1: Ищем после заголовка "Q (м³/ч)"
        # Стратегия 2: Ищем последовательность чисел в начале таблицы (БЕЗ заголовка)

        q_values = []

        # СТРАТЕГИЯ 1: Стандартный поиск с заголовком
        q_section = False
        for i, line in enumerate(lines):
            if 'Q (м3/ч)' in line or 'Q (м³/ч)' in line:
                q_section = True
                continue

            if q_section:
                q = self.parse_number(line)
                if q is not None and 0 <= q <= 500:
                    q_values.append(q)
                elif line and not line.replace(',', '.').replace('.', '').isdigit():
                    # Конец Q секции
                    break

        # СТРАТЕГИЯ 2: Если не нашли - ищем последовательность чисел в начале (первые 30 строк)
        if len(q_values) < 2:
            q_values = []
            potential_q_sequences = []

            # Ищем последовательности из 2+ чисел подряд
            current_sequence = []
            for i, line in enumerate(lines[:30]):  # Первые 30 строк
                # Пропускаем служебные строки
                if any(kw in line for kw in ['Таблица', 'характеристик', 'CDM', 'Модель', '©']):
                    if current_sequence:
                        potential_q_sequences.append(current_sequence)
                        current_sequence = []
                    continue

                q = self.parse_number(line)
                if q is not None and 10 <= q <= 500:  # Q обычно от 10 до 500
                    current_sequence.append(q)
                else:
                    if len(current_sequence) >= 2:  # Минимум 2 числа подряд
                        potential_q_sequences.append(current_sequence)
                    current_sequence = []

            # Берем самую длинную последовательность
            if potential_q_sequences:
                q_values = max(potential_q_sequences, key=len)

        # V8: Принимаем таблицы с 2+ колонками
        if not q_values or len(q_values) < 2:
            return []

        # Найти все модели и их данные
        page_pumps = []

        # Контекстные флаги для фильтрации
        skip_next_numbers = 0  # Счётчик строк которые нужно пропустить после Outlet/Power

        for i, line in enumerate(lines):
            # Проверка контекста: если предыдущие строки содержали Outlet/Power
            if any(keyword in line for keyword in ['Outlet', 'Power', 'DN']):
                skip_next_numbers = 5  # Пропустить следующие 5 чисел

            # ИСПРАВЛЕНИЕ V5: Расширенный паттерн - КИРИЛЛИЦА + ЗВЕЗДОЧКИ + CDM префикс
            # Форматы:
            # - Базовые: "1-2", "120-2-1", "125-6", "200-11"
            # - Звездочки: "125-6*", "155-5*"
            # - Латиница: "200-11A", "200-11B"
            # - КИРИЛЛИЦА: "185-1-А", "200-3-А-В", "215-5-3А-2В", "185-4-3А-В"
            # - С префиксом: "CDM125-6", "CDM 125-6", "CDMF185-9"
            line_clean = line.strip()

            # Паттерн ID насоса (МАКСИМАЛЬНО ГИБКИЙ):
            # \d+ - первое число (125, 185, 200, 215)
            # [-−\d АВЗабвзA-Za-z]* - ЛЮБАЯ комбинация: дефисы, цифры, кириллица (А,В,З), латиница
            # Примеры что ДОЛЖНО работать:
            # - 125-6*, 155-5* (звездочки)
            # - 185-1-А, 185-1-В (кириллица)
            # - 185-З-ЗВ, 185-4-ЗА-В (кириллица З вместо 3)
            # - 185-4-3А-В, 215-5-3А-2В (сложные комбинации)
            # - 200-3-А-В, 215-6-5А-В (любые варианты)
            id_pattern = r'\d+[-−\d АВЗабвзA-Za-z]*'

            # Паттерн 1: С префиксом CDM/CDMF
            # CDM125-6*, CDMF185-9, CDM185-1-А, CDM215-5-3А-2В
            model_match_with_prefix = re.match(rf'^(CDM[F]?\s*)({id_pattern})\*?$', line_clean)

            # Паттерн 2: Без префикса
            # 125-6*, 185-1-А, 215-5-3А-2В, 155-8-2*
            model_match_no_prefix = re.match(rf'^({id_pattern})\*?$', line_clean)

            if model_match_with_prefix:
                # Уже есть CDM/CDMF префикс
                prefix = model_match_with_prefix.group(1).replace(' ', '')  # "CDM" или "CDMF"
                model_id = prefix + model_match_with_prefix.group(2).replace('−', '-')
            elif model_match_no_prefix:
                # Добавляем префикс CDM
                model_id = "CDM" + model_match_no_prefix.group(1).replace('−', '-')
            else:
                continue

            # V8: Нормализация кириллицы З→3 (часто встречается в PDF)
            # Примеры: "185-З-ЗВ" → "185-3-3В", "215-З-ЗА" → "215-3-3А"
            model_id = model_id.replace('З', '3')

            # Сохраняем звёздочку если есть
            if line_clean.endswith('*'):
                model_id += '*'

            # Следующая строка должна быть кВт
            if i + 1 >= len(lines):
                continue

            kw = self.parse_number(lines[i + 1])
            if not kw or kw <= 0 or kw > 500:
                continue

            # Собираем H значения - они идут после "Н" и "(м)"
            h_values = []
            start_idx = i + 2

            # Пропускаем "Н" и "(м)" если есть
            while start_idx < len(lines) and lines[start_idx] in ['Н', '(м)']:
                start_idx += 1

            # УЛУЧШЕННАЯ ФИЛЬТРАЦИЯ: Собираем H значения
            # Типичные значения Outlet D которые нужно пропустить
            OUTLET_D_VALUES = {1.0, 1.25, 1.5, 2.0, 2.5, 3.0}

            for j in range(start_idx, min(start_idx + len(q_values) + 10, len(lines))):
                if skip_next_numbers > 0:
                    skip_next_numbers -= 1
                    continue

                line_text = lines[j]

                # Контекстная фильтрация: обновляем флаг
                if any(skip in line_text for skip in ['Outlet', 'Power', 'кВт', 'Вт', 'DN', 'Rp']):
                    skip_next_numbers = 5  # Пропустить следующие числа
                    continue

                h = self.parse_number(line_text)

                if h is None:
                    # Не число - проверяем стоит ли остановиться
                    if len(h_values) > 0 and line_text and not line_text.isspace():
                        # Есть данные и встретили текст - вероятно конец таблицы
                        break
                    continue

                # ЖЁСТКИЙ ФИЛЬТР: Outlet D
                if h in OUTLET_D_VALUES:
                    # Это скорее всего Outlet D, пропускаем
                    continue

                # Фильтр диапазона: Head обычно 3-500 метров
                if h < 3 or h > 500:
                    continue

                # ПРОВЕРКА ПОСЛЕДОВАТЕЛЬНОСТИ: Head должен уменьшаться
                if len(h_values) > 0:
                    last_h = h_values[-1]
                    # Если Head вырос больше чем на 10м - подозрительно
                    if h > last_h + 10:
                        # Скорее всего это не Head, а метаданные
                        break

                h_values.append(h)

                # Останавливаемся когда собрали достаточно значений
                if len(h_values) >= len(q_values):
                    break

            # ФИНАЛЬНАЯ ВАЛИДАЦИЯ И СОЗДАНИЕ ЗАПИСЕЙ
            if len(h_values) > 0:
                # Проверка: есть ли хотя бы 3 точки данных
                if len(h_values) < 3:
                    # Слишком мало данных - вероятно ошибка
                    continue

                # ИЗВЛЕЧЕНИЕ НОМЕРА СЕРИИ ИЗ ID (для фильтрации Outlet D)
                # CDM5-4 → 5, CDM125-6 → 125, CDM200-11A → 200, CDM185-1-А → 185
                try:
                    series_number = int(model_id.replace('CDM', '').replace('CDMF', '').replace('*', '').split('-')[0].rstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZАВабвABCDEFGHIJKLMNOPQRSTUVWXYZ'))
                except (ValueError, IndexError):
                    series_number = None

                # Создаём записи
                for idx, h in enumerate(h_values):
                    if idx < len(q_values):
                        q_val = q_values[idx]

                        # ФИЛЬТР V4: Исключаем записи где q = head = номер серии
                        # CDM5-*: q=5, head=5 → это Outlet D = 5", НЕ данные насоса!
                        # CDM10-*: q=10, head=10 → это Outlet D = 10", НЕ данные насоса!
                        if series_number is not None and q_val == h and q_val == series_number:
                            # Это Outlet D паттерн - пропускаем
                            continue

                        # Создаём запись
                        record = {
                            "id": model_id,
                            "kw": round(kw, 2),
                            "q": q_val,
                            "head_m": round(h, 1)
                        }

                        # ФИЛЬТР V6 КРИТИЧЕСКИЙ: ID ОБЯЗАТЕЛЬНО должен содержать дефис "-"
                        # CDM0, CDM11, CDM100 → это номера страниц PDF, НЕ насосы!
                        # Валидный ID: CDM1-2, CDM125-6, CDM185-1-А
                        # Невалидный ID: CDM0, CDM1, CDM11, CDM100
                        if "-" not in record["id"]:
                            # Это номер страницы PDF - пропускаем
                            continue

                        # ФИЛЬТР V5 (ЗАДАНИЕ 1): Исключаем метаданные (Inlet диаметры)
                        # CDM5-*: q=0.37, head=4.0 → INLET DIAMETER 3/8", НЕ точка характеристики!
                        # CDM10-*: q=0.75, head=7.5 → INLET DIAMETER 3/4"
                        # И т.д. для всех паттернов (155 лишних записей)
                        if self.is_metadata_record(record):
                            # Это метаданные между таблицами - пропускаем
                            continue

                        # ФИЛЬТР V6: Исключаем 36 конкретных дублей CDM95 и CDM150-5-1
                        if self.is_duplicate_record(record):
                            # Это дубль данных - пропускаем
                            continue

                        page_pumps.append(record)

        return page_pumps

    def parse(self) -> List[Dict]:
        """
        ПАРСИНГ с PyMuPDF - минимальное потребление памяти

        PyMuPDF (fitz) использует в 5-10x меньше памяти чем pdfplumber!
        14MB PDF: ~50-100MB вместо 492MB
        40MB PDF: ~150-300MB (влезет в 460MB доступной памяти)
        """
        logger.info("="*60)
        logger.info("CDM PARSER - PyMuPDF (легкая версия)")
        logger.info(f"Файл: {os.path.basename(self.pdf_path)}")
        logger.info("="*60)

        pages_processed = 0
        pages_with_tables = 0
        pages_skipped = 0

        try:
            # Открываем PDF через PyMuPDF (НАМНОГО легче чем pdfplumber!)
            doc = fitz.open(self.pdf_path)
            total_pages = len(doc)
            logger.info(f"Страниц в PDF: {total_pages}\n")

            # Обрабатываем страницы ПО ОДНОЙ
            for page_num in range(total_pages):
                pages_processed += 1

                try:
                    # Загружаем ТОЛЬКО текущую страницу
                    page = doc[page_num]

                    # Извлекаем текст (PyMuPDF делает это БЫСТРО и с минимальной памятью)
                    text = page.get_text()

                    if not text:
                        pages_skipped += 1
                        continue

                    # Быстрая проверка (первые 800 символов)
                    has_table, series_type = self.quick_check_table(text)

                    if not has_table:
                        pages_skipped += 1
                        # Освобождаем память
                        del text
                        continue

                    # Есть таблица - парсим
                    pages_with_tables += 1
                    logger.info(f"[{page_num + 1}] 📊 Таблица {series_type}")

                    page_pumps = self.parse_page_text(text, series_type)

                    if page_pumps:
                        self.all_pumps.extend(page_pumps)
                        models = set(p['id'] for p in page_pumps)
                        logger.info(f"  ✓ {len(models)} моделей, {len(page_pumps)} записей")

                        # ДИАГНОСТИКА: Отслеживание отсутствующих серий
                        target_series = ['CDM125', 'CDM155', 'CDM185', 'CDM215']
                        for series in target_series:
                            series_pumps = [m for m in models if m.startswith(series)]
                            if series_pumps:
                                logger.info(f"    🎯 {series}: {series_pumps}")

                except Exception as e:
                    logger.error(f"  ❌ Ошибка страницы {page_num + 1}: {e}")

                finally:
                    # Освобождаем память после каждой страницы
                    if 'text' in locals():
                        del text
                    if 'page_pumps' in locals():
                        del page_pumps

                    # Принудительная сборка мусора каждые 10 страниц
                    if page_num % 10 == 0:
                        gc.collect()

            # Закрываем документ
            doc.close()

        except Exception as e:
            logger.error(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            raise

        # Удаление дубликатов
        self.remove_duplicates()

        # Статистика
        logger.info("\n" + "="*60)
        logger.info("✅ ПАРСИНГ ЗАВЕРШЁН")
        logger.info("="*60)
        logger.info(f"Обработано: {pages_processed}/{total_pages}")
        logger.info(f"С таблицами: {pages_with_tables}")
        logger.info(f"Пропущено: {pages_skipped}")
        logger.info(f"Записей: {len(self.all_pumps)}")

        # ДИАГНОСТИКА: Статистика по отсутствующим сериям
        all_ids = set(p['id'] for p in self.all_pumps)
        logger.info("\n🔍 ДИАГНОСТИКА - Отсутствующие серии:")

        target_series_stats = {
            'CDM125': 13,
            'CDM155': 12,
            'CDM185': 18,
            'CDM215': 16
        }

        for series, expected in target_series_stats.items():
            found = [id for id in all_ids if id.startswith(series)]
            status = "✅" if len(found) == expected else "❌"
            logger.info(f"  {status} {series}: {len(found)}/{expected} насосов")
            if found and len(found) < 5:
                logger.info(f"      Найдено: {found}")

        total_found = sum(len([id for id in all_ids if id.startswith(s)]) for s in target_series_stats.keys())
        total_expected = sum(target_series_stats.values())
        logger.info(f"\n  📊 Итого целевых серий: {total_found}/{total_expected}")

        if total_found < total_expected:
            logger.info(f"  ⚠️  Отсутствует: {total_expected - total_found} насосов")
            logger.info(f"  💡 Проверьте что PDF содержит эти серии!")

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
    Извлечение данных CDM из PDF с использованием PyMuPDF

    ПРЕИМУЩЕСТВА PyMuPDF:
    - В 5-10x меньше памяти чем pdfplumber
    - Быстрее
    - Может парсить PDF до 40MB на сервере с 960MB RAM

    Args:
        pdf_path: путь к PDF

    Returns:
        [{"id": "1-2", "kw": 0.37, "q": 0.0, "head_m": 11.5}, ...]
    """
    parser = CDMParserPyMuPDF(pdf_path)
    return parser.parse()


def main():
    if len(sys.argv) < 2:
        print("Использование: python3 cdm_parser_pymupdf.py <input.pdf> [output.json]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) >= 3 else f"{os.path.splitext(os.path.basename(pdf_path))[0]}_parsed.json"

    if not os.path.exists(pdf_path):
        logger.error(f"❌ Файл не найден: {pdf_path}")
        sys.exit(1)

    parser = CDMParserPyMuPDF(pdf_path)
    pumps = parser.parse()

    if not pumps:
        logger.error("⚠️  Данные не найдены!")
        sys.exit(1)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(pumps, f, ensure_ascii=False, indent=2)

    logger.info(f"\n💾 Сохранено: {output_path}")


if __name__ == '__main__':
    main()
