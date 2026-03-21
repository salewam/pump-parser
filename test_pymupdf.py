#!/usr/bin/env python3
import sys
sys.path.insert(0, '/root/pump_parser')

import cdm_parser_pymupdf
import tracemalloc
import os

pdf_file = '/root/pump_parser/uploads/CDM_CDMF_241125.pdf'

print('='*60)
print('ТЕСТ PyMuPDF ПАРСЕРА (легкая библиотека)')
print('='*60)
print(f'Файл: {os.path.basename(pdf_file)}')
print(f'Размер: {os.path.getsize(pdf_file) / 1024 / 1024:.1f} MB')
print('='*60 + '\n')

tracemalloc.start()

try:
    pumps = cdm_parser_pymupdf.extract_cdm_from_pdf(pdf_file)

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print('\n' + '='*60)
    print('ИСПОЛЬЗОВАНИЕ ПАМЯТИ')
    print('='*60)
    print(f'Текущая: {current / 1024 / 1024:.1f} MB')
    print(f'Пиковая: {peak / 1024 / 1024:.1f} MB')

    # Сравнение
    old_peak = 492  # MB для pdfplumber
    improvement = ((old_peak - (peak / 1024 / 1024)) / old_peak) * 100
    print(f'\nСравнение с pdfplumber:')
    print(f'Было: {old_peak} MB')
    print(f'Стало: {peak / 1024 / 1024:.1f} MB')
    print(f'Экономия: {improvement:.1f}%')
    print('='*60)

    if pumps:
        models = set(p['id'] for p in pumps)
        print(f'\n✅ УСПЕХ!')
        print(f'Записей: {len(pumps)}')
        print(f'Моделей: {len(models)}')
        print(f'Пример: {pumps[0]}')
    else:
        print('\n⚠️ Данные не найдены')

except Exception as e:
    print(f'\n❌ ОШИБКА: {e}')
    import traceback
    traceback.print_exc()
