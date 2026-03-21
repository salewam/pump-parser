#!/usr/bin/env python3
import cdm_parser_v2

# Тест парсинга одной строки
parser = cdm_parser_v2.CDMParser("test.pdf")

# Тестируем парсинг строки CDM
q_values = [0.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
test_line = "1-2 0.37 11.8 11.5 10.5 9.7 9.0 8.0 7.0"

result = parser.parse_cdm_line(test_line, q_values)

print(f"\n=== ТЕСТ ID ФОРМАТА ===")
print(f"Входная строка: {test_line}")
print(f"Q значения: {q_values}")
print(f"\nРезультат:")
for i, pump in enumerate(result[:3]):
    print(f"{i+1}. {pump}")

print(f"\nПроверка: ID должен быть '1-2' (БЕЗ 'CDM')")
print(f"Фактический ID: '{result[0]['id']}'")
if result[0]['id'] == '1-2':
    print("✅ ПРАВИЛЬНО!")
else:
    print(f"❌ ОШИБКА! Ожидалось '1-2', получено '{result[0]['id']}'")
