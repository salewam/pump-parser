"""
Универсальный модуль извлечения таблиц из PDF.
Слой 1: Замена OCR на fitz.find_tables() + нормализация.

Работает детерминированно на любом сервере — никакого OCR.
Поддерживает: CDM, MV, LVR и любые другие PDF с таблицами.

КЛЮЧЕВОЕ ОТКРЫТИЕ:
- CDM и MV: find_tables() даёт ЧИСТЫЕ данные, одна модель на строку
- LVR: find_tables() даёт данные с двумя проблемами:
  1. Пробелы внутри чисел ("1 .1" вместо "1.1", "1 3" вместо "13")
  2. Merged cells — несколько моделей/значений через \\n в одной ячейке

Автор: Модуль для ONIS PDF Parser
Версия: 2.0
"""

import fitz
import re
from typing import List, Dict, Optional, Tuple


# =============================================================================
# НОРМАЛИЗАЦИЯ ЯЧЕЕК
# =============================================================================

def normalize_number(raw: str) -> Optional[float]:
    """
    Извлекает ОДНО число из строки (НЕ из raw ячейки — из уже разделённой части).

    Обрабатывает:
      - Запятая: "11,8" -> 11.8 (CDM)
      - Точка: "11.8" -> 11.8 (MV)
      - Пробел перед точкой: "1 .1" -> 1.1 (LVR)
      - Пробел между цифрами: "1 3" -> 13 (LVR)
      - "0 .37" -> 0.37, "51 0" -> 510
      - Комбинированное: "0.37+2.2" -> 0.37 (берём первое, CDMF+CMH формат)

    Возвращает None если не число.
    """
    if raw is None:
        return None

    text = raw.strip()
    if not text:
        return None

    # Комбинированные мощности CDMF: "0.37+2.2" -> берём первое число
    if '+' in text:
        text = text.split('+')[0].strip()

    # Убираем пробел перед точкой: "1 .1" -> "1.1"
    text = re.sub(r'(\d)\s+\.(\d)', r'\1.\2', text)

    # Убираем пробел после точки: "1. 1" -> "1.1"
    text = re.sub(r'(\d\.)\s+(\d)', r'\1\2', text)

    # Убираем пробелы между цифрами: "1 3" -> "13", "1 0 3" -> "103"
    for _ in range(4):
        text = re.sub(r'(\d)\s+(\d)', r'\1\2', text)

    # Запятая -> точка: "11,8" -> "11.8"
    text = text.replace(',', '.')

    text = text.strip()
    try:
        return float(text)
    except ValueError:
        return None


def normalize_model_name(raw: str) -> str:
    """
    Нормализует название модели насоса.

    LVR: "LVR(S)1 5-1" -> "LVR(S)15-1", "1 -2" -> "1-2"
    CDM/MV: без изменений.
    """
    if raw is None:
        return ""

    text = raw.strip()
    if not text:
        return ""

    # Пробел перед дефисом: "1 -2" -> "1-2"
    text = re.sub(r'(\d)\s+(-\d)', r'\1\2', text)

    # Пробел после дефиса: "15-1 2" -> "15-12"
    for _ in range(3):
        text = re.sub(r'(-\d+)\s+(\d)', r'\1\2', text)

    # После ) цифра пробел цифра: "LVR(S)1 5" -> "LVR(S)15"
    text = re.sub(r'(\))\s*(\d)\s+(\d)', r'\1\2\3', text)

    # Цифра пробел цифра перед дефисом: "1 5-1" -> "15-1"
    text = re.sub(r'(?<=\d)\s+(?=\d+[-])', r'', text)

    # Двойные пробелы
    text = re.sub(r'\s+', ' ', text).strip()

    return text


# =============================================================================
# ОПРЕДЕЛЕНИЕ ТИПА ТАБЛИЦЫ
# =============================================================================

def has_merged_cells(table_raw: List[List[Optional[str]]]) -> bool:
    """
    Проверяет есть ли в таблице merged DATA cells (не заголовки!).

    MV имеет многострочные ЗАГОЛОВКИ ('Артикул\\nнасоса\\nMVS') — это НЕ merged data.
    LVR имеет merged DATA cells ('20\\n30', 'LVR(S)10-2\\nLVR(S)10-3') — это merged data.

    Различие: проверяем только ячейки в DATA rows (после заголовка),
    и только числовые/модельные ячейки.
    """
    # Сначала находим строку заголовка (с Q)
    header_row_idx = 0
    for ri, row in enumerate(table_raw[:3]):
        for cell in row:
            if cell and ('Q' in cell.upper() or 'м3/ч' in cell.lower()):
                header_row_idx = ri
                break

    # Проверяем DATA rows (после заголовка)
    for ri in range(header_row_idx + 1, len(table_raw)):
        row = table_raw[ri]
        for cell in row:
            if cell and '\n' in cell:
                lines = cell.strip().split('\n')
                # Проверяем: есть ли среди строк НЕСКОЛЬКО числовых значений?
                nums = 0
                for line in lines:
                    if normalize_number(line) is not None:
                        nums += 1
                if nums >= 2:
                    return True

                # Проверяем: есть ли несколько модельных названий?
                model_patterns = 0
                for line in lines:
                    line = line.strip()
                    if re.search(r'LVR|LVS|MV|CDM|\d+-\d+', line):
                        model_patterns += 1
                if model_patterns >= 2:
                    return True

    return False


def find_q_values_in_header(table_raw: List[List[Optional[str]]]) -> Tuple[Optional[int], int, List[float]]:
    """
    Ищет Q-значения в заголовке таблицы.

    Возвращает: (row_idx, data_start_col, [Q values]) или (None, 0, [])
    """
    for row_idx in range(min(3, len(table_raw))):
        row = table_raw[row_idx]
        q_col = None

        for col_idx, cell in enumerate(row):
            if cell is None:
                continue
            # Берём только первую строку ячейки для проверки заголовка
            first_line = cell.split('\n')[0].strip()
            cell_text = cell.replace('\n', ' ')

            if re.search(r'\bQ\b', cell_text.upper()) or 'м3/ч' in cell_text.lower() or 'М3/Ч' in cell_text:
                q_col = col_idx
                break

        if q_col is not None:
            # Q-значения в ячейках ПОСЛЕ колонки Q
            q_values = []
            data_start_col = None
            for ci in range(q_col + 1, len(row)):
                cell = row[ci]
                if cell is None:
                    continue
                first_line = cell.split('\n')[0].strip()
                val = normalize_number(first_line)
                if val is not None:
                    if data_start_col is None:
                        data_start_col = ci
                    q_values.append(val)

            if data_start_col is not None and len(q_values) >= 3:
                return row_idx, data_start_col, q_values

    return None, 0, []


def is_performance_table(table_raw: List[List[Optional[str]]]) -> bool:
    """Определяет, является ли таблица таблицей производительности."""
    if not table_raw or len(table_raw) < 3:
        return False

    all_text = ""
    for row in table_raw:
        for cell in row:
            if cell:
                all_text += cell + " "

    has_q = bool(re.search(r'\bQ\b', all_text.upper())) or 'м3/ч' in all_text.lower()
    has_h = bool(re.search(r'[HН]\s*[\(\n]', all_text)) or '(м)' in all_text.lower()

    return has_q or has_h


# =============================================================================
# ПАТТЕРН A: СТАНДАРТНЫЕ ТАБЛИЦЫ (CDM, MV)
# Одна модель на строку, Q в заголовке, данные чистые
# =============================================================================

def extract_standard_table(table_raw: List[List[Optional[str]]]) -> List[Dict]:
    """
    Извлекает данные из стандартной таблицы (CDM, MV формат).
    Одна модель на строку, Q-значения в заголовке.
    """
    records = []

    row_idx, data_start_col, q_values = find_q_values_in_header(table_raw)
    if row_idx is None or not q_values:
        return records

    header_row = table_raw[row_idx]

    # Ищем колонку модели и кВт
    model_col = None
    kw_col = None

    for ci, cell in enumerate(header_row):
        if cell is None:
            continue
        cell_text = cell.replace('\n', ' ').lower()
        if 'модель' in cell_text or 'model' in cell_text:
            model_col = ci
        if 'двигатель' in cell_text or 'мощность' in cell_text or 'квт' in cell_text:
            kw_col = ci

    # Если нет колонки модели — ищем первую текстовую колонку
    if model_col is None:
        for ci in range(min(data_start_col, len(header_row))):
            cell = header_row[ci]
            if cell and not normalize_number(cell.replace('\n', ' ')):
                model_col = ci
                break

    current_model = ""
    current_kw = 0.0

    for ri in range(row_idx + 1, len(table_raw)):
        row = table_raw[ri]
        if not row or all(c is None or c.strip() == '' for c in row):
            continue

        # Модель
        if model_col is not None and model_col < len(row) and row[model_col]:
            raw = row[model_col].replace('\n', ' ').strip()
            nm = normalize_model_name(raw)
            if nm and len(nm) > 0:
                current_model = nm

        # кВт
        if kw_col is not None and kw_col < len(row) and row[kw_col]:
            kw = normalize_number(row[kw_col].replace('\n', ' '))
            if kw is not None and kw > 0:
                current_kw = kw

        # H-значения — берём только числовые ячейки
        has_data = False
        for q_idx, q_val in enumerate(q_values):
            col = data_start_col + q_idx
            if col >= len(row):
                break
            cell = row[col]
            if cell is None:
                continue
            # Берём первую строку (в MV бывает "DN\n25" или "H\n(м)")
            first_line = cell.split('\n')[0].strip()
            h = normalize_number(first_line)
            if h is not None and h > 0:
                has_data = True
                records.append({
                    "model": current_model,
                    "kw": current_kw,
                    "q": q_val,
                    "h": h,
                })

    return records


# =============================================================================
# ПАТТЕРН B: MERGED-CELL ТАБЛИЦЫ (LVR)
# Несколько моделей в одной ячейке через \n, значения тоже через \n
# кВт-колонка содержит ВСЕ мощности для всей таблицы
# =============================================================================

def extract_merged_table(table_raw: List[List[Optional[str]]]) -> List[Dict]:
    """
    Извлекает данные из таблицы с merged cells (LVR формат).

    Структура LVR (страницы 9-11):
    Row 0: [Модель (кВт) Q (м3/ч)] [5.0] [6.0] [8.0] [10] [12]
    Row 1: [LVR(S)10-2\\nLVR(S)10-3] [0.75\\n1.1\\n1.5\\n...] [20\\n30] [19\\n29] ...
    Row 2: [LVR(S)10-4] [None] [40] [40] ...

    Колонка 1 первого data row содержит ВСЕ кВт для всех моделей таблицы.
    """
    records = []

    row_idx, data_start_col, q_values = find_q_values_in_header(table_raw)
    if row_idx is None or not q_values:
        return records

    # Шаг 1: Извлекаем ВСЕ кВт значения из merged kw-колонки
    all_kw_values = []
    kw_col = None

    # Ищем колонку с максимальным количеством \n-разделённых числовых значений
    for ri in range(row_idx + 1, min(row_idx + 3, len(table_raw))):
        row = table_raw[ri]
        for ci in range(min(data_start_col, len(row))):
            cell = row[ci]
            if cell is None:
                continue
            lines = cell.strip().split('\n')
            kw_candidates = []
            for line in lines:
                val = normalize_number(line)
                if val is not None and val > 0:
                    kw_candidates.append(val)
            if len(kw_candidates) > len(all_kw_values):
                all_kw_values = kw_candidates
                kw_col = ci

    # Шаг 2: Обрабатываем каждую строку данных
    kw_index = 0  # Текущий индекс в списке all_kw_values

    for ri in range(row_idx + 1, len(table_raw)):
        row = table_raw[ri]
        if not row or all(c is None or c.strip() == '' for c in row):
            continue

        # Извлекаем модели из колонки 0
        models_in_row = []
        model_cell = row[0] if len(row) > 0 and row[0] else ""
        if model_cell:
            for line in model_cell.split('\n'):
                line = line.strip()
                if line and re.search(r'\d', line):
                    nm = normalize_model_name(line)
                    if nm:
                        models_in_row.append(nm)

        if not models_in_row:
            continue

        n_models = len(models_in_row)

        # Назначаем кВт каждой модели
        kw_for_models = []
        for i in range(n_models):
            if kw_index < len(all_kw_values):
                kw_for_models.append(all_kw_values[kw_index])
                kw_index += 1
            else:
                kw_for_models.append(0.0)

        # Извлекаем H-значения для каждого Q
        for q_idx, q_val in enumerate(q_values):
            col = data_start_col + q_idx
            if col >= len(row):
                break

            cell = row[col]
            if cell is None:
                continue

            # Разделяем merged values
            lines = cell.strip().split('\n')
            h_values = []
            for line in lines:
                h = normalize_number(line)
                if h is not None:
                    h_values.append(h)

            # Сопоставляем H-значения с моделями
            if len(h_values) == n_models:
                # Идеальный случай: количество H совпадает с количеством моделей
                for i in range(n_models):
                    if h_values[i] > 0:
                        records.append({
                            "model": models_in_row[i],
                            "kw": kw_for_models[i],
                            "q": q_val,
                            "h": h_values[i],
                        })
            elif len(h_values) == 1 and n_models == 1:
                # Одна модель, одно значение
                if h_values[0] > 0:
                    records.append({
                        "model": models_in_row[0],
                        "kw": kw_for_models[0],
                        "q": q_val,
                        "h": h_values[0],
                    })
            elif len(h_values) >= 1 and n_models >= 1:
                # Неравное количество — берём что можем
                for i in range(min(n_models, len(h_values))):
                    if h_values[i] > 0:
                        records.append({
                            "model": models_in_row[i],
                            "kw": kw_for_models[i],
                            "q": q_val,
                            "h": h_values[i],
                        })

    return records


# =============================================================================
# ПАТТЕРН C: TEXT-BLOCK FALLBACK
# Когда find_tables() даёт битую структуру (LVR страницы 4, 8)
# Извлекаем данные из позиционных текстовых блоков
# =============================================================================

def extract_from_text_blocks(page) -> List[Dict]:
    """
    Извлекает данные из текстовых блоков страницы (fallback).

    Работает для страниц где find_tables() даёт битую структуру,
    но визуально данные расположены столбцами:
    - Левый столбец (x<100): модели, кВт, H-значения (вертикально)
    - Средний столбец (x~200): Q-значения

    Проверено на LVR страницах 4 и 8.
    """
    records = []

    blocks_raw = page.get_text("blocks")
    text_blocks = []
    for b in blocks_raw:
        if b[6] == 0 and b[4].strip():
            text_blocks.append({
                'x': round(b[0]), 'y': round(b[1]),
                'lines': [l.strip() for l in b[4].strip().split('\n') if l.strip()]
            })
    text_blocks.sort(key=lambda b: (b['y'], b['x']))

    # Ищем блоки в левой колонке (x < 100) в нижней половине страницы (y > 350)
    left_blocks = [b for b in text_blocks if b['x'] < 100 and b['y'] > 350 and len(b['lines']) > 5]

    if len(left_blocks) < 3:
        return records

    # Block 0: Модели (содержит названия с дефисами или LVR)
    model_block = None
    kw_block = None
    h_blocks = []

    for bi, bl in enumerate(left_blocks):
        # Проверяем: это модели?
        model_count = sum(1 for l in bl['lines'] if re.search(r'\d+-\d+', l) or 'LVR' in l)
        if model_count > len(bl['lines']) * 0.5 and model_block is None:
            model_block = bl
            continue

        # Проверяем: это кВт? (стандартные значения кВт: 0.37, 0.55, 0.75, 1.1, 1.5...)
        STANDARD_KW = {0.37, 0.55, 0.75, 1.1, 1.5, 2.2, 3.0, 4.0, 5.5, 7.5, 11.0, 15.0, 18.5, 22.0, 30.0, 37.0, 45.0}
        kw_count = 0
        for l in bl['lines']:
            v = normalize_number(l)
            if v is not None and v in STANDARD_KW:
                kw_count += 1
        if kw_count > len(bl['lines']) * 0.3 and kw_block is None and model_block is not None:
            kw_block = bl
            continue

        # Остальные блоки — H-значения
        # ВАЖНО: сохраняем позиции! '-' (нет данных) → 0 как placeholder
        if model_block is not None and kw_block is not None:
            h_vals = []
            for l in bl['lines']:
                l_clean = l.strip()
                if l_clean in ('-', '—', '–', ''):
                    h_vals.append(0)  # placeholder для отсутствующего значения
                else:
                    v = normalize_number(l_clean)
                    h_vals.append(v if v is not None else 0)
            if len(h_vals) >= len(model_block['lines']) * 0.8:
                h_blocks.append(h_vals)

    # Ищем Q-значения: блоки правее (x > 130), числовые, маленькие значения
    # На некоторых страницах Q — один блок с 5+ значениями
    # На других — несколько блоков по 1-2 значения каждый
    # x-порог 130: стр.4/8 имеют Q при x~198, стр.15 при x~147
    q_values = []
    q_blocks_found = []

    # Сначала ищем единый Q-блок (как на стр.4)
    for bl in text_blocks:
        if 130 < bl['x'] < 300 and bl['y'] > 400:
            vals = []
            for l in bl['lines']:
                v = normalize_number(l)
                if v is not None and 0 <= v <= 500:
                    vals.append(v)
            if len(vals) >= 5:
                q_values = vals
                break

    # Если не нашли единый блок — собираем из маленьких блоков (как на стр.8, стр.15)
    if not q_values:
        for bl in text_blocks:
            if 130 < bl['x'] < 300 and bl['y'] > 500:
                vals = []
                for l in bl['lines']:
                    v = normalize_number(l)
                    if v is not None and 0 <= v <= 500:
                        vals.append(v)
                if vals and len(bl['lines']) <= 3:  # Маленький блок с Q
                    for v in vals:
                        q_blocks_found.append((bl['y'], v))

        if len(q_blocks_found) >= 3:
            q_blocks_found.sort()
            q_values = [v for _, v in q_blocks_found]

    if not model_block or not kw_block or not q_values or not h_blocks:
        return records

    # Извлекаем модели
    models = []
    for l in model_block['lines']:
        nm = normalize_model_name(l)
        if nm and re.search(r'\d', nm):
            models.append(nm)

    # Извлекаем кВт
    kw_values = []
    for l in kw_block['lines']:
        v = normalize_number(l)
        if v is not None:
            kw_values.append(v)

    if len(models) != len(kw_values):
        # Попробуем обрезать до минимума
        n = min(len(models), len(kw_values))
        models = models[:n]
        kw_values = kw_values[:n]

    n_models = len(models)

    # Сопоставляем H-блоки с Q-значениями
    n_q = min(len(q_values), len(h_blocks))

    for q_idx in range(n_q):
        q_val = q_values[q_idx]
        h_vals = h_blocks[q_idx]

        for mi in range(min(n_models, len(h_vals))):
            if h_vals[mi] > 0:
                records.append({
                    "model": models[mi],
                    "kw": kw_values[mi],
                    "q": q_val,
                    "h": h_vals[mi],
                })

    return records


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ: ПАРСИНГ PDF
# =============================================================================

def parse_pdf(pdf_path: str, progress_callback=None) -> List[Dict]:
    """
    Парсит PDF файл и извлекает данные о насосах.

    Три уровня извлечения:
    1. find_tables() стандартный (CDM, MV)
    2. find_tables() merged cells (LVR страницы 9-11, 15)
    3. Text-block fallback (LVR страницы 4, 8 — где find_tables() битый)

    Аргументы:
        pdf_path: путь к PDF файлу
        progress_callback: функция(page, total_pages) для отслеживания прогресса

    Возвращает: список записей [{"model": ..., "kw": ..., "q": ..., "h": ...}, ...]
    """
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    all_records = []

    for page_idx in range(total_pages):
        page = doc[page_idx]
        page_records = []

        # Уровень 1-2: find_tables()
        finder = page.find_tables()

        for table_obj in finder.tables:
            raw_data = table_obj.extract()

            if not is_performance_table(raw_data):
                continue

            if has_merged_cells(raw_data):
                records = extract_merged_table(raw_data)
            else:
                records = extract_standard_table(raw_data)

            page_records.extend(records)

        # Проверяем качество: невалидные модели, нулевые кВт, нереальные Q/H
        # Валидная модель содержит дефис (1-2, MV(S) 1-2, LVR(S)10-3, CDMF1-2+CMH1-40)
        # Q > 500 или H > 5000 — признак слияния колонок find_tables()
        if page_records:
            bad = sum(1 for r in page_records if not r["model"] or '-' not in r["model"] or r["kw"] == 0)
            max_q = max((r["q"] for r in page_records), default=0)
            max_h = max((r["h"] for r in page_records), default=0)
            has_insane_values = max_q > 500 or max_h > 5000

            if bad > len(page_records) * 0.5 or has_insane_values:
                # Уровень 3: text-block fallback
                fallback = extract_from_text_blocks(page)
                if fallback:
                    bad_fb = sum(1 for r in fallback if not r["model"] or r["kw"] == 0)
                    max_q_fb = max((r["q"] for r in fallback), default=0)
                    max_h_fb = max((r["h"] for r in fallback), default=0)
                    fb_sane = max_q_fb <= 500 and max_h_fb <= 5000
                    # Fallback лучше если: меньше ошибок ИЛИ find_tables дал нереальные значения
                    if (bad_fb < bad) or (has_insane_values and fb_sane):
                        page_records = fallback
        elif not page_records:
            # Если find_tables() ничего не нашёл — пробуем fallback
            fallback = extract_from_text_blocks(page)
            if fallback:
                page_records = fallback

        all_records.extend(page_records)

        if progress_callback:
            progress_callback(page_idx + 1, total_pages)

    doc.close()

    # Дедупликация
    all_records = deduplicate(all_records)

    return all_records


def deduplicate(records: List[Dict]) -> List[Dict]:
    """Убирает дубликаты записей."""
    seen = set()
    unique = []
    for rec in records:
        key = (rec["model"], rec["kw"], rec["q"], rec["h"])
        if key not in seen:
            seen.add(key)
            unique.append(rec)
    return unique


# =============================================================================
# ТЕСТИРОВАНИЕ
# =============================================================================

def test_normalization():
    """Тестирует нормализацию на реальных данных из LVR."""

    print("=" * 60)
    print("ТЕСТ НОРМАЛИЗАЦИИ ЧИСЕЛ")
    print("=" * 60)

    test_cases = [
        ("11,8", 11.8), ("11.8", 11.8), ("1 .1", 1.1), ("0 .37", 0.37),
        ("1 3", 13.0), ("1 03", 103.0), ("1 .5", 1.5), ("1 62", 162.0),
        ("206", 206.0), ("0,5", 0.5), ("17,5", 17.5), ("1 0", 10.0),
        ("1 00", 100.0), ("1 97", 197.0), ("51 0", 510.0), ("21 0", 210.0),
        ("1 29", 129.0), ("31 30", 3130.0),
    ]

    passed = failed = 0
    for raw, expected in test_cases:
        result = normalize_number(raw)
        ok = result is not None and abs(result - expected) < 0.001
        status = "OK" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"  {status}: '{raw}' -> {result} (ожидалось {expected})")
    print(f"\nРезультат: {passed}/{passed + failed}")

    print("\n" + "=" * 60)
    print("ТЕСТ НОРМАЛИЗАЦИИ МОДЕЛЕЙ")
    print("=" * 60)

    model_tests = [
        ("LVR(S)1 5-1", "LVR(S)15-1"),
        ("LVR(S)1 0-1 6", "LVR(S)10-16"),
        ("LVR(S)20-1 0", "LVR(S)20-10"),
        ("LVR(S)1 5-1 2", "LVR(S)15-12"),
        ("LVR(S)1 0-1 8", "LVR(S)10-18"),
        ("1 -2", "1-2"),
        ("1 -3", "1-3"),
        ("MV(S) 1-2", "MV(S) 1-2"),
        ("1-2", "1-2"),
        ("32-1 0-2", "32-10-2"),
        ("1 5-1 4", "15-14"),
    ]

    passed = failed = 0
    for raw, expected in model_tests:
        result = normalize_model_name(raw)
        ok = result == expected
        status = "OK" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"  {status}: '{raw}' -> '{result}' (ожидалось '{expected}')")
    print(f"\nРезультат: {passed}/{passed + failed}")


def test_on_pdf(pdf_path: str, name: str):
    """Тестирует модуль на конкретном PDF."""

    print(f"\n{'=' * 60}")
    print(f"ТЕСТ: {name}")
    print(f"{'=' * 60}")

    records = parse_pdf(pdf_path)

    models = set(r["model"] for r in records)
    kw_values = sorted(set(r["kw"] for r in records))

    print(f"Всего записей: {len(records)}")
    print(f"Уникальных моделей: {len(models)}")
    print(f"Мощности (кВт): {kw_values}")

    print(f"\nПервые 15 записей:")
    for i, rec in enumerate(records[:15]):
        print(f"  {i+1}. model={rec['model']}, kw={rec['kw']}, q={rec['q']}, h={rec['h']}")

    empty_models = [r for r in records if not r["model"]]
    zero_kw = [r for r in records if r["kw"] == 0]

    print(f"\nДиагностика:")
    print(f"  Записей без модели: {len(empty_models)}")
    print(f"  Записей с kw=0: {len(zero_kw)}")

    return records


if __name__ == "__main__":
    import sys, os

    test_normalization()

    if len(sys.argv) > 1:
        test_on_pdf(sys.argv[1], sys.argv[1].split('/')[-1])
    else:
        pdfs = [
            ("/sessions/sweet-dazzling-franklin/mnt/uploads/CDM_CDMF_241125.pdf", "CDM"),
            ("/sessions/sweet-dazzling-franklin/mnt/uploads/Каталог LVR.pdf", "LVR"),
            ("/sessions/sweet-dazzling-franklin/mnt/uploads/Каталог MV.pdf", "MV"),
        ]
        for path, name in pdfs:
            if os.path.exists(path):
                test_on_pdf(path, name)
            else:
                print(f"\n[SKIP] {name}: файл не найден")
