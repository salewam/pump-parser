#!/usr/bin/env python3
"""
Тест импортов и базовой функциональности парсера
"""

import sys
sys.path.insert(0, '/root/pump_parser')

try:
    import cdm_parser
    print("✅ cdm_parser импортирован успешно")

    # Проверяем наличие основных функций
    assert hasattr(cdm_parser, 'extract_cdm_from_pdf'), "Функция extract_cdm_from_pdf не найдена"
    print("✅ Функция extract_cdm_from_pdf найдена")

    assert hasattr(cdm_parser, 'parse_number'), "Функция parse_number не найдена"
    print("✅ Функция parse_number найдена")

    # Тестируем parse_number
    assert cdm_parser.parse_number("0,37") == 0.37, "parse_number не работает с запятой"
    print("✅ parse_number('0,37') = 0.37")

    assert cdm_parser.parse_number("2.2") == 2.2, "parse_number не работает с точкой"
    print("✅ parse_number('2.2') = 2.2")

    assert cdm_parser.parse_number("15*") == 15.0, "parse_number не обрабатывает звездочку"
    print("✅ parse_number('15*') = 15.0")

    print("\n🎉 Все тесты пройдены! Парсер готов к работе.")

except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.exit(1)
except AssertionError as e:
    print(f"❌ Тест не пройден: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Неожиданная ошибка: {e}")
    sys.exit(1)
