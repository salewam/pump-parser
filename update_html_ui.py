#!/usr/bin/env python3
# -*- coding: utf-8 -*-

with open('/root/pump_parser/templates/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Добавляем placeholder в tbody
old_tbody = '''<tbody id="results-tbody">
                    </tbody>'''

new_tbody = '''<tbody id="results-tbody">
                        <tr id="placeholder-row">
                            <td colspan="5" style="text-align: center; padding: 40px; color: #666;">
                                <div style="font-size: 3em; margin-bottom: 10px;">📄</div>
                                <div style="font-size: 1.2em;">Загрузите PDF файл для начала парсинга</div>
                                <div style="font-size: 0.9em; margin-top: 10px; color: #888;">Результаты появятся здесь автоматически</div>
                            </td>
                        </tr>
                    </tbody>'''

if old_tbody in content:
    content = content.replace(old_tbody, new_tbody)
    print("✅ Placeholder добавлен")
else:
    print("⚠️  Старый tbody не найден, пропускаем")

# 2. Обновляем displayResults чтобы удалять placeholder
old_js = "tbody.innerHTML = '';"
new_js = """// Удаляем placeholder если есть
            const placeholder = document.getElementById('placeholder-row');
            if (placeholder) placeholder.remove();
            tbody.innerHTML = '';"""

if old_js in content:
    content = content.replace(old_js, new_js)
    print("✅ JavaScript обновлён")
else:
    print("⚠️  JavaScript уже обновлён или не найден")

# Сохраняем
with open('/root/pump_parser/templates/index.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("\n✅ HTML ОБНОВЛЁН УСПЕШНО!")
