#!/usr/bin/env python3
"""
Тест использования памяти парсером CDM
"""
import sys
sys.path.insert(0, '/root/pump_parser')

import cdm_parser_v2
import tracemalloc
import os

pdf_file = '/root/pump_parser/uploads/CDM_CDMF_241125.pdf'

print('='*60)
print('ТЕСТ ИСПОЛЬЗОВАНИЯ ПАМЯТИ')
print('='*60)
print(f'Файл: {os.path.basename(pdf_file)}')
print(f'Размер: {os.path.getsize(pdf_file) / 1024 / 1024:.1f} MB')
print('='*60)

# Запускаем мониторинг памяти
tracemalloc.start()

try:
    print('\n🔄 Запуск парсера...\n')
    pumps = cdm_parser_v2.extract_cdm_from_pdf(pdf_file)

    # Получаем статистику памяти
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print('\n' + '='*60)
    print('РЕЗУЛЬТАТЫ')
    print('='*60)
    print(f'Найдено записей: {len(pumps)}')
    print(f'Текущая память: {current / 1024 / 1024:.1f} MB')
    print(f'Пиковая память: {peak / 1024 / 1024:.1f} MB')
    print('='*60)

    if pumps:
        print(f'\nПример первой записи:')
        print(f'  {pumps[0]}')

        # Уникальные модели
        models = set(p['id'] for p in pumps)
        print(f'\nУникальных моделей: {len(models)}')
        print(f'Примеры моделей: {list(models)[:5]}')

except Exception as e:
    print(f'\n❌ ОШИБКА: {e}')
    import traceback
    traceback.print_exc()

print('\n✅ Тест завершён\n')
