#!/usr/bin/env python3
"""
Отладка извлечения текста PyMuPDF
"""
import fitz

pdf_file = '/root/pump_parser/uploads/CDM_CDMF_241125.pdf'

doc = fitz.open(pdf_file)

# Проверяем страницу с таблицей (страница 21 по логам)
page_num = 20  # 0-indexed, так что 21-1=20

page = doc[page_num]
text = page.get_text()

print('='*60)
print(f'ТЕКСТ СО СТРАНИЦЫ {page_num + 1}')
print('='*60)
print(text[:2000])  # Первые 2000 символов
print('='*60)

# Также проверим строки
lines = text.split('\n')
print(f'\nВсего строк: {len(lines)}')
print('\nПервые 30 строк:')
for i, line in enumerate(lines[:30]):
    if line.strip():
        print(f'{i+1:3d}: {line}')

doc.close()
