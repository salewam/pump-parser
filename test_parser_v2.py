#!/usr/bin/env python3
"""
Тест парсера CDM V2
"""

import sys
sys.path.insert(0, '.')

# Тест 1: Импорт
print("="*60)
print("ТЕСТ 1: Импорт парсера")
print("="*60)
try:
    import cdm_parser_v2
    print("✅ cdm_parser_v2 импортирован")
except Exception as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

# Тест 2: Создание парсера
print("\n" + "="*60)
print("ТЕСТ 2: Создание объекта парсера")
print("="*60)
try:
    parser = cdm_parser_v2.CDMParser("test.pdf")
    print(f"✅ Парсер создан: {parser}")
except Exception as e:
    print(f"❌ Ошибка создания: {e}")
    sys.exit(1)

# Тест 3: Парсинг чисел
print("\n" + "="*60)
print("ТЕСТ 3: Парсинг чисел")
print("="*60)
test_cases = [
    ("0,37", 0.37),
    ("2.2", 2.2),
    ("15*", 15.0),
    ("0 .37", 0.37),
    ("1 5", 15.0),
    ("24 5", 245.0),
]

all_passed = True
for test_input, expected in test_cases:
    result = parser.parse_number(test_input)
    if result == expected:
        print(f"✅ parse_number('{test_input}') = {result}")
    else:
        print(f"❌ parse_number('{test_input}') = {result}, ожидалось {expected}")
        all_passed = False

if all_passed:
    print("\n✅ ВСЕ ТЕСТЫ ПАРСИНГА ЧИСЕЛ ПРОЙДЕНЫ!")
else:
    print("\n❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ")

# Тест 4: Поиск Q значений
print("\n" + "="*60)
print("ТЕСТ 4: Поиск Q значений")
print("="*60)
test_lines = [
    "Модель кВт Q (м³/ч)",
    "0 0.5 0.7 1.0 1.2 1.4 1.6 1.8 2.0",
    "1-2 0.37 11.8 11.5 11.2 10.5 10.3 9.7 9.0"
]
q_values = parser.find_q_values(test_lines)
if q_values and len(q_values) >= 5:
    print(f"✅ Найдены Q значения: {q_values}")
else:
    print(f"❌ Q значения не найдены или недостаточно")

# Тест 5: Определение серии
print("\n" + "="*60)
print("ТЕСТ 5: Определение серии")
print("="*60)
test_series_lines = [
    "CDM/CDMF1 – Таблица характеристик",
    "Модель кВт H (м)"
]
series, prefix = parser.detect_series(test_series_lines)
if series == "1" and prefix == "CDM":
    print(f"✅ Серия определена: CDM{series}")
else:
    print(f"❌ Серия не определена корректно: {series}, {prefix}")

print("\n" + "="*60)
print("ИТОГ: Базовые тесты парсера завершены")
print("="*60)
print("\n⚠️  Для полного теста нужен реальный PDF файл")
