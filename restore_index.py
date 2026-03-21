#!/usr/bin/env python3
"""
Восстанавливает index.html без истории парсинга
Оставляет только кнопку "Скачать JSON"
"""

with open('/root/pump_parser/templates/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Убираем кнопку "История парсинга" из header
old_header = '''            <div class="subtitle">Парсинг PDF каталогов насосов CDM/CDMF для ONIS Bot</div>
            <div style="margin-top: 15px;">
                <button class="btn btn-secondary" onclick="showHistory()" style="padding: 8px 20px; font-size: 0.9em;">
                    📚 История парсинга
                </button>
            </div>
        </header>'''

new_header = '''            <div class="subtitle">Парсинг PDF каталогов насосов CDM/CDMF для ONIS Bot</div>
        </header>'''

if old_header in content:
    content = content.replace(old_header, new_header)
    print('✅ Убрана кнопка "История парсинга"')

# 2. Убираем вызов saveToLocalStorage из displayResults
old_display = '''            document.getElementById('results-section').classList.add('active');

            // Сохраняем в localStorage
            saveToLocalStorage(data);
        }'''

new_display = '''            document.getElementById('results-section').classList.add('active');
        }'''

if old_display in content:
    content = content.replace(old_display, new_display)
    print('✅ Убрано автосохранение в localStorage')

# 3. Убираем все функции для работы с localStorage (большой блок кода)
# Ищем начало функции saveToLocalStorage и конец функции loadHistoryItem
import re

# Паттерн для удаления всех функций localStorage
pattern = r'\n\s+function saveToLocalStorage\(data\).*?function loadHistoryItem\(index\).*?\}\s+\}'

content = re.sub(pattern, '', content, flags=re.DOTALL)
print('✅ Удалены функции localStorage')

# Сохраняем
with open('/root/pump_parser/templates/index.html', 'w', encoding='utf-8') as f:
    f.write(content)

print('\n✅ index.html восстановлен!')
print('📋 Осталось:')
print('   - Кнопка "Скачать JSON" ✅')
print('   - Без истории парсинга ✅')
print('   - Без автосохранения ✅')
