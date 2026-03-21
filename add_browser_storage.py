#!/usr/bin/env python3
"""
Добавляет сохранение распарсенных данных в localStorage браузера
"""

with open('/root/pump_parser/templates/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Добавляем функции для работы с localStorage после displayResults
old_display_end = '''            document.getElementById('results-section').classList.add('active');
        }'''

new_display_end = '''            document.getElementById('results-section').classList.add('active');

            // Сохраняем в localStorage
            saveToLocalStorage(data);
        }

        function saveToLocalStorage(data) {
            try {
                const timestamp = new Date().toISOString();
                const savedData = {
                    timestamp: timestamp,
                    date: new Date().toLocaleString('ru-RU'),
                    total: data.stats.total,
                    models: data.stats.models,
                    data: data.data
                };

                // Получаем историю
                let history = JSON.parse(localStorage.getItem('cdm_parse_history') || '[]');

                // Добавляем новую запись в начало
                history.unshift(savedData);

                // Храним последние 10 парсингов
                if (history.length > 10) {
                    history = history.slice(0, 10);
                }

                // Сохраняем
                localStorage.setItem('cdm_parse_history', JSON.stringify(history));
                localStorage.setItem('cdm_latest_parse', JSON.stringify(savedData));

                console.log('✅ Данные сохранены в браузере');
            } catch (e) {
                console.error('Ошибка сохранения:', e);
            }
        }

        function loadFromLocalStorage() {
            try {
                const latest = localStorage.getItem('cdm_latest_parse');
                if (latest) {
                    const data = JSON.parse(latest);
                    return data;
                }
            } catch (e) {
                console.error('Ошибка загрузки:', e);
            }
            return null;
        }

        function showHistory() {
            try {
                const history = JSON.parse(localStorage.getItem('cdm_parse_history') || '[]');
                if (history.length === 0) {
                    showMessage('История парсинга пуста', 'error');
                    return;
                }

                let historyHtml = '<div style="max-height: 400px; overflow-y: auto;">';
                historyHtml += '<h3 style="color: #ff9800; margin-bottom: 20px;">📚 История парсинга</h3>';

                history.forEach((item, index) => {
                    historyHtml += `
                        <div style="background: rgba(255,255,255,0.05); padding: 15px; margin-bottom: 10px; border-radius: 8px; border-left: 3px solid #ff9800;">
                            <div style="color: #ff9800; font-weight: bold;">#{index + 1} - ${item.date}</div>
                            <div style="margin-top: 5px;">Записей: ${item.total} | Моделей: ${item.models}</div>
                            <button class="btn" style="margin-top: 10px; padding: 5px 15px; font-size: 0.9em;"
                                    onclick="loadHistoryItem(${index})">Загрузить</button>
                        </div>
                    `;
                });

                historyHtml += '</div>';

                const container = document.getElementById('message-container');
                container.innerHTML = `<div class="message" style="text-align: left;">${historyHtml}</div>`;
            } catch (e) {
                showMessage('Ошибка загрузки истории: ' + e.message, 'error');
            }
        }

        function loadHistoryItem(index) {
            try {
                const history = JSON.parse(localStorage.getItem('cdm_parse_history') || '[]');
                if (history[index]) {
                    parsedData = history[index].data;
                    displayResults({
                        data: history[index].data,
                        stats: {
                            total: history[index].total,
                            models: history[index].models
                        }
                    });
                    showMessage(`✅ Загружено из истории: ${history[index].date}`, 'success');
                }
            } catch (e) {
                showMessage('Ошибка загрузки: ' + e.message, 'error');
            }
        }'''

content = content.replace(old_display_end, new_display_end)

# 2. Добавляем кнопку "История" в header
old_header_close = '''            <div class="subtitle">Парсинг PDF каталогов насосов CDM/CDMF для ONIS Bot</div>
        </header>'''

new_header_close = '''            <div class="subtitle">Парсинг PDF каталогов насосов CDM/CDMF для ONIS Bot</div>
            <div style="margin-top: 15px;">
                <button class="btn btn-secondary" onclick="showHistory()" style="padding: 8px 20px; font-size: 0.9em;">
                    📚 История парсинга
                </button>
            </div>
        </header>'''

content = content.replace(old_header_close, new_header_close)

# Сохраняем
with open('/root/pump_parser/templates/index.html', 'w', encoding='utf-8') as f:
    f.write(content)

print('✅ Автосохранение в браузере добавлено!')
print('📦 Хранилище: localStorage браузера')
print('🔢 Хранится: последние 10 парсингов')
print('📚 Кнопка "История парсинга" добавлена в шапку')
print('')
print('Функции:')
print('  - Автосохранение после каждого парсинга')
print('  - Просмотр истории (последние 10)')
print('  - Загрузка из истории')
print('  - Данные хранятся локально в браузере')
