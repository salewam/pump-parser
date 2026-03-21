#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Добавляет детальное логирование ошибок и endpoint /errors в app.py
"""

import re

with open('/root/pump_parser/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Добавляем импорты для логирования
if 'import traceback' not in content:
    imports_section = content.split('from flask import')[0]
    new_imports = imports_section + '''import traceback
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('/root/pump_parser/app_errors.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Глобальный список ошибок (последние 50)
ERROR_LOG = []
MAX_ERRORS = 50

def log_error(error_type, message, traceback_str=None):
    """Логирует ошибку в файл и в память"""
    error_entry = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'type': error_type,
        'message': str(message),
        'traceback': traceback_str
    }
    ERROR_LOG.insert(0, error_entry)
    if len(ERROR_LOG) > MAX_ERRORS:
        ERROR_LOG.pop()
    logger.error(f"{error_type}: {message}")
    if traceback_str:
        logger.error(traceback_str)

'''
    content = new_imports + 'from flask import' + content.split('from flask import')[1]
    print("✅ Импорты добавлены")

# 2. Добавляем endpoint /errors
if '@app.route(\'/errors\')' not in content:
    # Находим место после route /stats
    stats_route_end = content.find('@app.route(\'/stats\')')
    if stats_route_end > 0:
        # Находим конец функции stats
        next_route = content.find('@app.route', stats_route_end + 1)
        if next_route > 0:
            errors_endpoint = '''

@app.route('/errors')
def errors():
    """Возвращает последние ошибки"""
    return jsonify({
        'errors': ERROR_LOG,
        'count': len(ERROR_LOG)
    })

'''
            content = content[:next_route] + errors_endpoint + content[next_route:]
            print("✅ Endpoint /errors добавлен")

# 3. Оборачиваем /upload в try-except с детальным логированием
old_upload = '''@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не найден'}), 400'''

new_upload = '''@app.route('/upload', methods=['POST'])
def upload():
    try:
        logger.info("=== НАЧАЛО ЗАГРУЗКИ PDF ===")
        if 'file' not in request.files:
            log_error('ValidationError', 'Файл не найден в request.files')
            return jsonify({'error': 'Файл не найден'}), 400'''

if old_upload in content:
    content = content.replace(old_upload, new_upload)
    print("✅ Обработка ошибок в /upload добавлена")

# 4. Добавляем try-except вокруг парсинга
old_parse = 'pumps = cdm_parser.extract_cdm_from_pdf(filepath)'
new_parse = '''try:
            logger.info(f"Начало парсинга: {filepath}")
            pumps = cdm_parser.extract_cdm_from_pdf(filepath)
            logger.info(f"Парсинг завершён: {len(pumps)} записей")
        except MemoryError as e:
            log_error('MemoryError', 'Недостаточно памяти для парсинга', traceback.format_exc())
            return jsonify({'error': 'Недостаточно памяти. Попробуйте файл меньшего размера'}), 500
        except Exception as e:
            log_error('ParsingError', f'Ошибка парсинга: {str(e)}', traceback.format_exc())
            return jsonify({'error': f'Ошибка парсинга: {str(e)}'}), 500'''

if old_parse in content:
    content = content.replace(old_parse, new_parse)
    print("✅ Try-except вокруг парсинга добавлен")

# 5. Добавляем общий exception handler
if '@app.errorhandler(Exception)' not in content:
    error_handler = '''

@app.errorhandler(Exception)
def handle_exception(e):
    """Глобальный обработчик ошибок"""
    log_error('UnhandledException', str(e), traceback.format_exc())
    return jsonify({'error': f'Внутренняя ошибка сервера: {str(e)}'}), 500

'''
    # Добавляем в конец перед if __name__
    if 'if __name__' in content:
        parts = content.split('if __name__')
        content = parts[0] + error_handler + 'if __name__' + parts[1]
        print("✅ Глобальный exception handler добавлен")

# Сохраняем
with open('/root/pump_parser/app.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("\n✅ APP.PY ОБНОВЛЁН С ДЕТАЛЬНЫМ ЛОГИРОВАНИЕМ!")
