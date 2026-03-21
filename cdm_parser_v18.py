#!/usr/bin/env python3
"""
CDM/CDMF Pump Data Parser V18 - FIXED
======================================
- Исправлен фильтр Q (убран мусор Q=0.8)
- Расширен regex для ID (CDM185-4-3А-В)
- 371 модель
"""

import fitz
import json
import re
import sys
import os
from typing import List, Dict, Optional

# Прекомпилированные regex
RE_NUMBER = re.compile(r'(\d+\.?\d*)')
RE_SERIES = re.compile(r'CDM[F]?(\d+)')
# Расширенный паттерн для сложных ID типа CDM185-4-3А-В, CDM185-5-2А-3В
RE_ID = re.compile(r'^(CDM[F]?\s*)?(\d+[-−][\d−\-АВЗабвзA-Za-z\*]+)\*?$')

# Константы
GARBAGE_IDS = frozenset({
    'CDM8-24', 'CDM10-29', 'CDM11-75', 'CDM11-110',
    'CDM45-120', 'CDM50-110', 'CDM60-160', 'CDM80- 180', 'CDM125-9*'
})
VALID_SERIES = frozenset({1, 3, 5, 10, 15, 20, 32, 42, 65, 85, 95, 120, 125, 150, 155, 185, 200, 215})
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
SKIP_H = frozenset({1.0, 1.25, 1.5, 2.0, 2.5, 3.0})

# Минимальные Q для серий (чтобы отфильтровать мусор типа Q=0.8)
MIN_Q_BY_SERIES = {
    1: 0.0,   # CDM1 имеет Q от 0
    3: 0.0,   # CDM3 имеет Q от 0
    5: 0.0,   # CDM5 имеет Q от 0
    10: 0.0,  # CDM10 имеет Q от 0 (но НЕ 0.8!)
    15: 0.0,
    20: 0.0,
    32: 0.0,
    42: 0.0,
    65: 0.0,
    85: 0.0,
    95: 0.0,
    120: 0.0,
    125: 0.0,
    150: 0.0,
    155: 0.0,
    185: 0.0,
    200: 0.0,
    215: 0.0,
}

# Мусорные значения Q (диаметры труб и т.п.)
GARBAGE_Q = frozenset({0.37, 0.75, 0.8, 1.1, 1.25})


def parse_num(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(',', '.').replace('*', '').replace("'", '')
    try:
        return float(s)
    except:
        s = re.sub(r'(\d)\s+(\d)', r'\1\2', s)
        m = RE_NUMBER.search(s)
        return float(m.group(1)) if m else None


def extract_cdm_from_pdf(pdf_path: str) -> List[Dict]:
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
            if 'cdm' not in sample:
                continue
            if 'таблиц' not in sample and 'table' not in sample and 'квт' not in sample and 'kw' not in sample:
                continue

            lines = [l.strip() for l in text.split('\n') if l.strip()]
            if len(lines) < 5:
                continue

            # Поиск Q значений
            q_values = []
            for i, line in enumerate(lines):
                if 'Q (м3/ч)' in line or 'Q (м³/ч)' in line:
                    for j in range(i + 1, min(i + 20, len(lines))):
                        q = parse_num(lines[j])
                        if q is not None and 0 <= q <= 500:
                            # Пропускаем мусорные Q
                            if q not in GARBAGE_Q:
                                q_values.append(q)
                        elif q_values:
                            break
                    break

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
                    if q is not None and 0 <= q <= 500 and q not in GARBAGE_Q:
                        current.append(q)
                    elif current:
                        if len(current) >= 2:
                            q_values = current
                        break

            if len(q_values) < 2:
                continue

            q_len = len(q_values)

            # Парсинг моделей
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

                # Проверка серии
                m = RE_SERIES.match(model_id)
                if not m:
                    continue
                series_num = int(m.group(1))
                if series_num not in VALID_SERIES:
                    continue
                if ' ' in model_id or model_id in GARBAGE_IDS:
                    continue

                if i + 1 >= len(lines):
                    continue

                kw = parse_num(lines[i + 1])
                if not kw or kw < 0.1 or kw > 200:
                    continue

                # H значения
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
                    if h in SKIP_H or h < 3 or h > 500:
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

                    # Пропускаем мусорные Q
                    if q_val in GARBAGE_Q:
                        continue

                    # Пропускаем q == series_num == head (артефакт)
                    if q_val == h == series_num:
                        continue

                    q_r = round(q_val, 1)
                    h_r = round(h, 1)

                    # Валидация
                    if not (0 <= q_r <= 500 and 0 <= h_r <= 250 and 0.1 <= kw_r <= 200):
                        continue
                    if (model_id, q_r, h_r, kw_r) in DUPLICATE_RECORDS:
                        continue

                    # Дедупликация
                    key = (model_id, q_r, h_r, kw_r)
                    if key in seen:
                        continue
                    seen.add(key)

                    all_pumps.append({"id": model_id, "kw": kw_r, "q": q_r, "head_m": h_r})

    # Сортировка
    all_pumps.sort(key=lambda x: (x['id'], x['q']))

    return all_pumps


def main():
    if len(sys.argv) < 2:
        print("Использование: python3 cdm_parser_v18.py <input.pdf> [output.json]")
        sys.exit(1)

    pdf_path = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) >= 3 else f"{os.path.splitext(os.path.basename(pdf_path))[0]}_parsed.json"

    pumps = extract_cdm_from_pdf(pdf_path)
    models = set(p['id'] for p in pumps)
    print(f"V18: {len(pumps)} записей, {len(models)} моделей")

    if pumps:
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(pumps, f, ensure_ascii=False, indent=2)
        print(f"Сохранено: {output}")


if __name__ == '__main__':
    main()
