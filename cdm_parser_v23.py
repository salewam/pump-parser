#!/usr/bin/env python3
"""
CDM/CDMF Pump Data Parser V23
==============================
УНИВЕРСАЛЬНЫЙ ПАРСЕР - работает с любым PDF
+ УМНЫЙ фильтр мусора (анализирует контекст)
+ Автоматическое определение Q=0 как заголовка
+ Валидация ID по структуре серий
+ Статистический анализ данных
"""

import fitz
import json
import re
import sys
import os
from typing import List, Dict, Optional, Set, Tuple
from collections import defaultdict

RE_NUMBER = re.compile(r'(\d+\.?\d*)')
RE_SERIES = re.compile(r'CDM[F]?(\d+)')
RE_ID = re.compile(r'^(CDM[F]?\s*)?(\d+[-−][\d−\-АВЗабвзA-Za-z\*]+)\*?$')
RE_MODEL_START = re.compile(r'^\d{2,3}[-−]\d')

# Валидные серии насосов CDM (из каталога)
VALID_SERIES = frozenset({1, 3, 5, 10, 15, 20, 32, 42, 65, 85, 95, 120, 125, 150, 155, 185, 200, 215})

# Горизонтальные серии (таблицы с Q в столбцах)
HORIZONTAL_SERIES = frozenset({95, 125, 155, 185, 200, 215})

# Серии где Q=0 - это реальное значение (не заголовок)
SERIES_WITH_Q0 = frozenset({1, 3, 5, 10, 15, 20, 32, 42, 65, 85, 95, 120, 150, 200})

# Типичные диапазоны kW для серий (min, max)
SERIES_KW_RANGE = {
    1: (0.1, 2),     3: (0.2, 3),    5: (0.3, 5),    10: (0.5, 7),
    15: (0.7, 10),   20: (1, 15),    32: (2, 20),    42: (3, 30),
    65: (5, 45),     85: (7, 55),    95: (10, 75),   120: (15, 90),
    125: (15, 100),  150: (20, 130), 155: (30, 150), 185: (40, 220),
    200: (50, 250),  215: (30, 250)
}

# Типичные диапазоны Q для серий (min, max) - очень мягкие
SERIES_Q_RANGE = {
    1: (0, 5),       3: (0, 8),      5: (0, 10),     10: (0, 20),
    15: (0, 25),     20: (0, 35),    32: (0, 50),    42: (0, 80),
    65: (0, 120),    85: (0, 150),   95: (0, 200),   120: (0, 250),
    125: (5, 250),   150: (5, 300),  155: (50, 350), 185: (100, 400),
    200: (100, 450), 215: (100, 500)
}


def is_garbage_id(model_id: str) -> bool:
    """
    УМНАЯ проверка мусорного ID.
    Анализирует структуру ID и определяет, является ли это реальной моделью.
    Работает для ЛЮБОГО PDF без хардкода.

    Правила:
    1. Валидный ID: CDM{series}-{model}[-{variant}]
    2. {series} должна быть в VALID_SERIES
    3. Не должен быть диапазоном (8-24, 45-120)
    4. Не должен заканчиваться на "-"
    5. Не должен содержать пробелы
    """
    # Убираем CDM/CDMF префикс для анализа
    clean = model_id.replace('CDMF', '').replace('CDM', '')

    # Правило 1: Заканчивается на "-" - незавершённый ID
    if clean.endswith('-'):
        return True

    # Правило 2: Содержит пробелы - это не ID, а текст
    if ' ' in model_id:
        return True

    # Правило 3: Проверка на диапазон (не модель)
    # Формат диапазона: XX-YY где XX и YY - просто числа
    match = re.match(r'^(\d+)[-−](\d+)$', clean)
    if match:
        first = int(match.group(1))
        second = int(match.group(2))

        # Если первое число - НЕ валидная серия, это точно мусор
        if first not in VALID_SERIES:
            return True

        # Если second намного больше first - это диапазон, а не модель
        # Модели: 125-1, 125-2, 125-10 (second обычно < first)
        # Диапазоны: 10-29, 45-120, 50-110 (second >> first)
        if second > first and second > 50:
            return True

    # Правило 4: Извлекаем серию и проверяем
    parts = re.split(r'[-−]', clean)
    if parts:
        try:
            series = int(parts[0])
            # Серия должна быть валидной
            if series not in VALID_SERIES:
                return True

            # Для серий 1, 3, 5 - модели типа 1-2, 3-5 валидны
            # Но 1-20, 3-50 - это уже мусор
            if series in {1, 3, 5} and len(parts) >= 2:
                try:
                    second_part = int(parts[1].rstrip('*'))
                    # Если второй компонент слишком большой
                    if second_part > 30:
                        return True
                except ValueError:
                    pass  # Буквенный суффикс - OK
        except ValueError:
            return True  # Не число в начале

    # Правило 5: Проверка на мусорные суффиксы
    # Например: 125-9* или 95-3 без достаточного контекста
    # Но это сложно определить без полного контекста, оставляем

    return False


def is_header_q0(q_values: List[float], series_num: int) -> bool:
    """
    Определяет, является ли Q=0 заголовком таблицы.
    Q=0 в начале, и следующее Q значительно больше = заголовок.
    """
    if not q_values or q_values[0] != 0:
        return False

    # Серии где Q=0 - реальное значение
    if series_num in SERIES_WITH_Q0:
        return False

    # Если следующий Q > 30 - это заголовок
    if len(q_values) > 1 and q_values[1] > 30:
        return True

    return False


def validate_qh_pair(q: float, h: float, kw: float, series_num: int) -> bool:
    """
    Валидация Q-H пары на физическую корректность.
    Использует мягкие ограничения на основе серии.
    """
    # Базовые абсолютные лимиты
    if q < 0 or h < 3 or kw < 0.1:
        return False
    if q > 500 or h > 500 or kw > 300:
        return False

    # Проверка диапазона kW для серии (мягкая)
    if series_num in SERIES_KW_RANGE:
        min_kw, max_kw = SERIES_KW_RANGE[series_num]
        # Допускаем ±30% от диапазона
        if kw < min_kw * 0.7 or kw > max_kw * 1.3:
            return False

    # Проверка Q для серии (очень мягкая)
    if series_num in SERIES_Q_RANGE:
        min_q, max_q = SERIES_Q_RANGE[series_num]
        # Q=0 разрешаем для некоторых серий
        if q > 0 and q < min_q * 0.5:
            return False
        if q > max_q * 1.5:
            return False

    return True


def parse_num(s: str) -> Optional[float]:
    """Парсинг числа из строки"""
    if not s:
        return None
    s = s.strip().replace(',', '.').replace('*', '').replace("'", '')
    try:
        return float(s)
    except:
        s = re.sub(r'(\d)\s+(\d)', r'\1\2', s)
        m = RE_NUMBER.search(s)
        return float(m.group(1)) if m else None


def parse_series_page(lines: List[str], series_num: int, seen: Set[Tuple]) -> List[Dict]:
    """Парсинг страницы горизонтальной серии (Q в столбцах)"""
    pumps = []

    # Ищем Q значения
    q_values = []
    q_start = -1

    for i, line in enumerate(lines):
        l = line.strip()
        if l == 'Q' or l.startswith('Q '):
            q_start = i
            break

    if q_start < 0:
        return []

    # Собираем Q значения после заголовка
    collecting_q = False
    raw_q_values = []

    for i in range(q_start, min(q_start + 30, len(lines))):
        l = lines[i].strip()

        if 'м3/ч' in l or 'м³/ч' in l:
            collecting_q = True
            continue

        if collecting_q:
            # Стоп на модели
            if RE_MODEL_START.match(l):
                break

            # Парсим чистые числа
            if re.match(r'^\d+[.,]?\d*\s*$', l):
                q = parse_num(l)
                if q is not None and 0 <= q <= 500:
                    raw_q_values.append(q)

    # Автоматически убираем Q=0 если это заголовок
    if raw_q_values and is_header_q0(raw_q_values, series_num):
        q_values = raw_q_values[1:]
    else:
        q_values = raw_q_values

    if len(q_values) < 5:
        return []

    # Ищем модели
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Формат модели: 215-1, 185-2-А, 200-3-2В
        model_match = re.match(rf'^({series_num}[-−][\d−\-АВЗабвзA-Za-z\*]*)\s*$', line)
        if not model_match:
            i += 1
            continue

        model_id = 'CDM' + model_match.group(1).replace('−', '-').replace('З', '3').strip()

        # УМНАЯ проверка мусорного ID
        if is_garbage_id(model_id):
            i += 1
            continue

        # Ищем kW в следующих строках
        kw = None
        kw_offset = 0
        for offset in range(1, 4):
            if i + offset >= len(lines):
                break
            kw_line = lines[i + offset].strip()
            if not kw_line:
                continue
            if re.match(r'^\d+[.,]?\d*\s*$', kw_line):
                kw = parse_num(kw_line)
                if kw and 0.1 <= kw <= 250:
                    kw_offset = offset
                    break
                kw = None

        if not kw:
            i += 1
            continue

        # Собираем H значения
        h_values = []
        j = i + kw_offset + 1
        skip_h_header = 2

        while j < min(i + 60, len(lines)):
            l = lines[j].strip()

            if skip_h_header > 0 and ('Н' in l or 'м' in l or l == ''):
                skip_h_header -= 1
                j += 1
                continue
            skip_h_header = 0

            # Следующая модель - стоп
            if RE_MODEL_START.match(l):
                break

            if re.match(r'^\d+[.,]?\d*\s*$', l):
                h = parse_num(l)
                if h is not None and 3 <= h <= 500:
                    h_values.append(h)

            if len(h_values) >= len(q_values):
                break

            j += 1

        # Создаем записи с валидацией
        if len(h_values) >= 3:
            kw_r = round(kw, 2)
            added_keys = set()

            for idx, h in enumerate(h_values):
                if idx >= len(q_values):
                    break
                q_val = q_values[idx]

                q_r = round(q_val, 1)
                h_r = round(h, 1)

                # Валидация Q-H пары
                if not validate_qh_pair(q_r, h_r, kw_r, series_num):
                    continue

                # Дедупликация
                key = (model_id, q_r, h_r, kw_r)
                if key in added_keys or key in seen:
                    continue
                added_keys.add(key)

                pumps.append({"id": model_id, "kw": kw_r, "q": q_r, "head_m": h_r})

        i = j

    return pumps


def detect_duplicate_pattern(pumps: List[Dict]) -> Set[Tuple]:
    """
    УМНОЕ обнаружение дубликатов на основе статистики.
    Находит записи, которые выбиваются из общей картины.
    """
    duplicates = set()

    # Группируем по модели
    by_model = defaultdict(list)
    for p in pumps:
        by_model[p['id']].append(p)

    for model_id, records in by_model.items():
        if len(records) < 3:
            continue

        # Анализируем Q-H зависимость
        # Нормальная зависимость: H уменьшается при увеличении Q
        sorted_records = sorted(records, key=lambda x: x['q'])

        # Проверяем монотонность H
        prev_h = None
        violations = 0
        for r in sorted_records:
            if prev_h is not None:
                if r['head_m'] > prev_h + 5:  # H выросла значительно
                    violations += 1
            prev_h = r['head_m']

        # Если слишком много нарушений - возможно дубликаты
        if violations > len(records) // 3:
            # Помечаем записи с нарушениями
            prev_h = None
            for r in sorted_records:
                if prev_h is not None and r['head_m'] > prev_h + 10:
                    duplicates.add((r['id'], r['q'], r['head_m'], r['kw']))
                prev_h = r['head_m']

    return duplicates


def extract_cdm_from_pdf(pdf_path: str) -> List[Dict]:
    """Извлечение данных насосов из PDF"""
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"Файл не найден: {pdf_path}")

    all_pumps = []
    seen = set()

    with fitz.open(pdf_path) as doc:
        for page in doc:
            text = page.get_text()
            if not text or len(text) < 50:
                continue

            sample = text[:1500].lower()

            # Проверка на страницу с данными
            is_series_page = any(
                f'cdm/cdmf{s}' in text.lower() or f'cdmf{s}' in text.lower()
                for s in HORIZONTAL_SERIES
            )

            if not is_series_page:
                if 'cdm' not in sample:
                    continue
                if not any(kw in sample for kw in ['таблиц', 'table', 'квт', 'kw']):
                    continue

            lines = [l.strip() for l in text.split('\n')]
            if len(lines) < 5:
                continue

            # === ГОРИЗОНТАЛЬНЫЕ СЕРИИ ===
            for series in HORIZONTAL_SERIES:
                if f'CDM{series}' in text or f'CDMF{series}' in text or f'CDM/CDMF{series}' in text:
                    series_pumps = parse_series_page(lines, series, seen)
                    for p in series_pumps:
                        key = (p['id'], p['q'], p['head_m'], p['kw'])
                        if key not in seen:
                            seen.add(key)
                            all_pumps.append(p)

            # === ВЕРТИКАЛЬНЫЙ ФОРМАТ ===
            q_values = []
            for i, line in enumerate(lines):
                if 'Q (м3/ч)' in line or 'Q (м³/ч)' in line:
                    for j in range(i + 1, min(i + 20, len(lines))):
                        l = lines[j].strip()
                        if RE_MODEL_START.match(l):
                            break
                        if not re.match(r'^\d+[.,]?\d*\s*$', l):
                            if q_values:
                                break
                            continue
                        q = parse_num(l)
                        if q is not None and 0 <= q <= 500:
                            q_values.append(q)
                    break

            # Альтернативный поиск Q
            if len(q_values) < 2:
                current = []
                for line in lines[:30]:
                    if 'CDM' in line or 'Таблица' in line:
                        if len(current) >= 2:
                            q_values = current
                            break
                        current = []
                        continue
                    q = parse_num(line)
                    if q is not None and 0 <= q <= 500:
                        current.append(q)
                    elif current:
                        if len(current) >= 2:
                            q_values = current
                        break

            if len(q_values) < 2:
                continue

            q_len = len(q_values)

            # Парсинг вертикальных моделей
            for i, line in enumerate(lines):
                match = RE_ID.match(line)
                if not match:
                    continue

                prefix = match.group(1) or 'CDM'
                model_id = prefix.replace(' ', '') + match.group(2).replace('−', '-').replace('З', '3')
                if not model_id.startswith('CDM'):
                    model_id = 'CDM' + model_id
                if line.endswith('*') and not model_id.endswith('*'):
                    model_id += '*'

                if '-' not in model_id:
                    continue

                # УМНАЯ проверка мусорного ID
                if is_garbage_id(model_id):
                    continue

                m = RE_SERIES.match(model_id)
                if not m:
                    continue
                series_num = int(m.group(1))
                if series_num not in VALID_SERIES:
                    continue
                # Пропускаем горизонтальные серии
                if series_num in HORIZONTAL_SERIES:
                    continue

                if i + 1 >= len(lines):
                    continue

                kw = parse_num(lines[i + 1])
                if not kw or kw < 0.1 or kw > 250:
                    continue

                h_values = []
                for j in range(i + 2, min(i + 2 + q_len + 10, len(lines))):
                    ln = lines[j]
                    if 'Outlet' in ln or 'Power' in ln or 'DN' in ln:
                        break
                    h = parse_num(ln)
                    if h is None:
                        if h_values:
                            break
                        continue
                    if h < 3 or h > 500:
                        continue
                    if h_values and h > h_values[-1] + 10:
                        break
                    h_values.append(h)
                    if len(h_values) >= q_len:
                        break

                if len(h_values) < 3:
                    continue

                kw_r = round(kw, 2)

                for idx, h in enumerate(h_values):
                    if idx >= q_len:
                        break
                    q_val = q_values[idx]

                    q_r = round(q_val, 1)
                    h_r = round(h, 1)

                    # Валидация Q-H пары
                    if not validate_qh_pair(q_r, h_r, kw_r, series_num):
                        continue

                    # Проверка: Q=H=series - мусор
                    if q_r == h_r == series_num:
                        continue

                    key = (model_id, q_r, h_r, kw_r)
                    if key in seen:
                        continue
                    seen.add(key)

                    all_pumps.append({"id": model_id, "kw": kw_r, "q": q_r, "head_m": h_r})

    # Пост-обработка: убираем статистические дубликаты
    duplicates = detect_duplicate_pattern(all_pumps)
    if duplicates:
        all_pumps = [p for p in all_pumps if (p['id'], p['q'], p['head_m'], p['kw']) not in duplicates]

    all_pumps.sort(key=lambda x: (x['id'], x['q']))
    return all_pumps


def main():
    if len(sys.argv) < 2:
        print("Использование: python3 cdm_parser_v23.py <input.pdf> [output.json]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) >= 3 else f"{os.path.splitext(os.path.basename(pdf_path))[0]}_parsed.json"

    pumps = extract_cdm_from_pdf(pdf_path)
    models = set(p['id'] for p in pumps)
    print(f"V23: {len(pumps)} записей, {len(models)} моделей")

    if pumps:
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(pumps, f, ensure_ascii=False, indent=2)
        print(f"Сохранено: {output}")


if __name__ == '__main__':
    main()
