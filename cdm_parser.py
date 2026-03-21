#!/usr/bin/env python3
"""
CDM/CDMF Pump Data Parser
=========================
Извлекает характеристики насосов (модель, кВт, Q, H) из PDF-каталогов CDM/CDMF.

Использование:
    python3 cdm_parser.py input.pdf [output.json]

Если output не указан, создаёт CDM_BASE.json рядом с input.
"""

import pdfplumber
import json
import re
import sys
import os


def parse_number(s):
    """Парсим число из строки, обрабатывая разные форматы."""
    if not s:
        return None
    s = str(s).strip().replace(',', '.')
    # Убираем звёздочки и прочие пометки
    s = s.rstrip('*')
    try:
        return float(s)
    except:
        return None


def fix_broken_text(line):
    """
    Исправляем текст с разбитыми пробелами из PDF.
    Пример: '0 .37+2.2 246 24 5 242' -> '0.37+2.2 246 245 242'
    """
    # Исправляем "0 .37" -> "0.37"
    line = re.sub(r'(\d)\s+\.(\d)', r'\1.\2', line)
    # Исправляем "24 5" -> "245" (число + пробел + 1-2 цифры без точки дальше)
    # Но аккуратно, не склеивая разные числа
    # Стратегия: склеиваем только если 2-я часть — 1 цифра
    line = re.sub(r'(\d)\s+(\d)(?=\s|$)', r'\1\2', line)
    # Повторяем для каскадных случаев "2 4 5" 
    line = re.sub(r'(\d)\s+(\d)(?=\s|$)', r'\1\2', line)
    return line


def extract_cdm_from_pdf(pdf_path):
    """
    Главная функция: извлекает все насосы CDM/CDMF из PDF.
    
    Возвращает список словарей:
    [{"id": "CDM1-2", "kw": 0.37, "q": 0.0, "head_m": 11.8}, ...]
    """
    all_pumps = []
    
    with pdfplumber.open(pdf_path) as pdf:
        current_series = None
        current_prefix = None  # "CDM" или "CDMF1+CMH1-40" и т.д.
        q_values = None
        
        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue
            
            # Работаем только со страницами с таблицами характеристик
            if 'Таблица характеристик' not in text:
                continue
            
            lines = text.split('\n')
            
            # --- Определяем серию из заголовка ---
            for line in lines[:5]:
                # CDM/CDMF серии: "CDM/CDMF1 – Таблица характеристик"
                match = re.search(r'CDM/CDMF(\d+)\s*[–-]\s*Таблица', line)
                if match:
                    current_series = match.group(1)
                    current_prefix = "CDM"
                    q_values = None
                    break
                
                # CDMF+CMH бустерные: "CDMF1+CMH1-40 – Таблица характеристик"
                match = re.search(r'(CDMF\d+\+CMH\d+-\d+)\s*[–-]\s*Таблица', line)
                if match:
                    current_series = match.group(1)
                    current_prefix = "CDMF+CMH"
                    q_values = None
                    break
            
            if not current_series:
                continue
            
            # --- Ищем строку Q и данные ---
            for line in lines:
                # Пропускаем заголовки
                if 'Таблица' in line or 'Модель' in line or '(кВт)' in line:
                    if q_values is None:
                        # Ищем Q значения в этой или ближайших строках
                        parts = line.split()
                        potential_q = []
                        for p in parts:
                            q = parse_number(p)
                            if q is not None and 0 <= q <= 500:
                                potential_q.append(q)
                        if len(potential_q) >= 5:
                            q_values = potential_q
                    continue
                
                # Ищем строку с Q значениями (много чисел подряд)
                if q_values is None:
                    parts = line.split()
                    potential_q = []
                    for p in parts:
                        q = parse_number(p)
                        if q is not None and 0 <= q <= 500:
                            potential_q.append(q)
                    if len(potential_q) >= 5:
                        q_values = potential_q
                    continue
                
                # --- Парсим строки данных ---
                if current_prefix == "CDM":
                    # Формат: "1-2 0,37 11.8 11.5 ..."
                    match = re.match(
                        r'^(\d+[-−][^\s]+)\s+(\d+[.,]?\d*)\s+(.+)$',
                        line.strip()
                    )
                elif current_prefix == "CDMF+CMH":
                    # Формат: "CDMF1-2+CM H1-40 0 .37+2.2 246 24 5 ..."
                    # Текст сильно разбит пробелами — сначала убираем ВСЕ пробелы,
                    # потом восстанавливаем структуру
                    fixed = line.strip()
                    # Убираем пробел в "CM H" -> "CMH"
                    fixed = re.sub(r'CM\s+H', 'CMH', fixed)
                    # Убираем пробел в "0 .37" -> "0.37"  
                    fixed = re.sub(r'(\d)\s+\.(\d)', r'\1.\2', fixed)
                    # Убираем пробел в ". 2" -> ".2"
                    fixed = re.sub(r'\.\s+(\d)', r'.\1', fixed)
                    # Убираем пробел в "2 2" -> "22" только для kW части (до первого H)
                    # Лучше: сначала извлекаем модель и kW, потом числа
                    # Убираем "Н" и "(м)" 
                    fixed = re.sub(r'\s*Н\s*', ' ', fixed)
                    fixed = re.sub(r'\s*\(\s*м\s*\)\s*', ' ', fixed)
                    
                    match = re.match(
                        r'^(CDMF\d+-\d+\+CMH\d+-\d+)\s+([\d.,]+\+[\d.,]+)\s+(.+)$',
                        fixed
                    )
                else:
                    match = None
                
                if not match or not q_values:
                    continue
                
                model_raw = match.group(1).replace('−', '-')
                kw_raw = match.group(2)
                h_str = match.group(3)
                
                # Обрабатываем кВт
                if '+' in kw_raw:
                    # CDMF+CMH: "0.37+2.2" — суммируем
                    parts = kw_raw.split('+')
                    kw = sum(parse_number(p) or 0 for p in parts)
                else:
                    kw = parse_number(kw_raw)
                
                if not kw or kw <= 0 or kw > 500:
                    continue
                
                # Убираем метки Н и (м)
                h_str = re.sub(r'\s*Н\s*', ' ', h_str)
                h_str = re.sub(r'\s*\(м\)\s*', ' ', h_str)
                
                # Для CDMF+CMH текст разбит — исправляем
                if current_prefix == "CDMF+CMH":
                    # Склеиваем разбитые числа: "24 5" -> "245", "28 0" -> "280"
                    h_str = re.sub(r'(\d)\s+(\d)(?=\s|$)', r'\1\2', h_str)
                    h_str = re.sub(r'(\d)\s+(\d)(?=\s|$)', r'\1\2', h_str)
                
                # Извлекаем H значения
                h_values = []
                for p in h_str.split():
                    h = parse_number(p)
                    if h is not None and 0 < h < 600:
                        h_values.append(h)
                
                # Формируем ID
                if current_prefix == "CDM":
                    # Исправляем кириллическую "З" на "3"
                    model_raw = model_raw.replace('З', '3')
                    pump_id = f"CDM{model_raw}"
                else:
                    pump_id = model_raw  # Уже с CDMF+CMH
                
                # Записываем точки
                for i, h in enumerate(h_values):
                    if i < len(q_values):
                        all_pumps.append({
                            "id": pump_id,
                            "kw": round(kw, 2),
                            "q": q_values[i],
                            "head_m": round(h, 1)
                        })
    
    # --- Убираем дубликаты ---
    seen = set()
    unique_pumps = []
    for p in all_pumps:
        key = (p['id'], p['kw'], p['q'], p['head_m'])
        if key not in seen:
            seen.add(key)
            unique_pumps.append(p)
    
    return unique_pumps


def print_summary(pumps):
    """Выводит сводку по извлечённым данным."""
    series = {}
    for p in pumps:
        # Определяем серию
        match = re.match(r'CDM(\d+)', p['id'])
        if match:
            s = f"CDM{match.group(1)}"
        elif 'CDMF' in p['id']:
            match = re.match(r'(CDMF\d+\+CMH\d+-\d+)', p['id'])
            s = match.group(1) if match else p['id']
        else:
            s = 'OTHER'
        
        if s not in series:
            series[s] = {'models': set(), 'points': 0}
        series[s]['models'].add(p['id'])
        series[s]['points'] += 1
    
    print("\n" + "=" * 55)
    print(f"{'Серия':<25} {'Моделей':>8} {'Точек':>8}")
    print("-" * 55)
    
    total_m = 0
    total_p = 0
    for s in sorted(series.keys()):
        m = len(series[s]['models'])
        pt = series[s]['points']
        total_m += m
        total_p += pt
        print(f"{s:<25} {m:>8} {pt:>8}")
    
    print("-" * 55)
    print(f"{'ИТОГО':<25} {total_m:>8} {total_p:>8}")
    print("=" * 55)


def main():
    if len(sys.argv) < 2:
        print("Использование: python3 cdm_parser.py <input.pdf> [output.json]")
        print("Пример:        python3 cdm_parser.py CDM_CDMF_241125.pdf CDM_BASE.json")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    
    if len(sys.argv) >= 3:
        output_path = sys.argv[2]
    else:
        base = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = f"{base}_parsed.json"
    
    if not os.path.exists(pdf_path):
        print(f"❌ Файл не найден: {pdf_path}")
        sys.exit(1)
    
    print(f"📄 Парсим: {pdf_path}")
    
    pumps = extract_cdm_from_pdf(pdf_path)
    
    if not pumps:
        print("⚠️  Данные не найдены!")
        sys.exit(1)
    
    # Сохраняем
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(pumps, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Сохранено: {output_path}")
    print_summary(pumps)


if __name__ == '__main__':
    main()
