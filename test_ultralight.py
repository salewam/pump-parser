#!/usr/bin/env python3
import sys
sys.path.insert(0, '/root/pump_parser')

import cdm_parser_ultralight
import tracemalloc
import os

pdf_file = '/root/pump_parser/uploads/CDM_CDMF_241125.pdf'

print('='*60)
print('ТЕСТ ULTRALIGHT ПАРСЕРА')
print('='*60)
print(f'Файл: {os.path.basename(pdf_file)}')
print(f'Размер: {os.path.getsize(pdf_file) / 1024 / 1024:.1f} MB')
print('='*60 + '\n')

tracemalloc.start()

try:
    pumps = cdm_parser_ultralight.extract_cdm_from_pdf(pdf_file)

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print('\n' + '='*60)
    print('ПАМЯТЬ')
    print('='*60)
    print(f'Текущая: {current / 1024 / 1024:.1f} MB')
    print(f'Пиковая: {peak / 1024 / 1024:.1f} MB')
    print('='*60)

    if pumps:
        models = set(p['id'] for p in pumps)
        print(f'\n✅ Результат: {len(pumps)} записей, {len(models)} моделей')
        print(f'Пример: {pumps[0]}')

except Exception as e:
    print(f'\n❌ ОШИБКА: {e}')
    import traceback
    traceback.print_exc()
