#!/usr/bin/env python3
"""
Добавляет кнопку "Скачать JSON" в index.html
"""

with open('/root/pump_parser/templates/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Добавляем функцию downloadJSON в JavaScript секцию (перед resetParser)
old_resetparser = '''        function resetParser() {
            parsedData = null;'''

new_code = '''        function downloadJSON() {
            if (!parsedData || parsedData.length === 0) {
                showMessage('Нет данных для скачивания', 'error');
                return;
            }

            // Создаём blob с JSON данными
            const jsonStr = JSON.stringify(parsedData, null, 2);
            const blob = new Blob([jsonStr], { type: 'application/json' });

            // Создаём ссылку для скачивания
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;

            // Генерируем имя файла с датой
            const date = new Date().toISOString().split('T')[0];
            const timestamp = new Date().toTimeString().split(' ')[0].replace(/:/g, '-');
            a.download = `cdm_parsed_${date}_${timestamp}.json`;

            // Триггерим скачивание
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);

            showMessage('✅ JSON файл скачан!', 'success');
        }

        function resetParser() {
            parsedData = null;'''

content = content.replace(old_resetparser, new_code)

# 2. Добавляем кнопку в HTML (перед кнопкой "Новый файл")
old_buttons = '''                <button class="btn btn-secondary" onclick="resetParser()">
                    🔄 Новый файл
                </button>'''

new_buttons = '''                <button class="btn btn-secondary" onclick="downloadJSON()">
                    💾 Скачать JSON
                </button>
                <button class="btn btn-secondary" onclick="resetParser()">
                    🔄 Новый файл
                </button>'''

content = content.replace(old_buttons, new_buttons)

# Сохраняем
with open('/root/pump_parser/templates/index.html', 'w', encoding='utf-8') as f:
    f.write(content)

print('✅ Кнопка "Скачать JSON" добавлена!')
print('📍 Расположение: рядом с кнопкой "Новый файл"')
print('💾 Имя файла: cdm_parsed_YYYY-MM-DD_HH-MM-SS.json')
