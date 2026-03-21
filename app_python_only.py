#!/usr/bin/env python3
"""
PDF Парсер ONIS — Flask-приложение
"""

from flask import Flask, request, redirect, url_for, flash, session, send_file, jsonify, render_template_string
import os
import json
import re
from werkzeug.utils import secure_filename
from datetime import datetime
import sys
import threading
import uuid
import time

# Импорт парсеров
sys.path.append('/root/projects777')
sys.path.append('/root/pump_parser')
from universal_table_extractor import parse_pdf as _universal_parse
from cdm_parser_v24_learned import extract_from_pdf as _cdm_lvr_parse

def extract_cdm_from_pdf(filepath):
    """CDM/LVR → точный парсер, всё остальное → universal extractor"""
    import fitz
    # Определяем бренд по первым страницам
    brand = None
    with fitz.open(filepath) as doc:
        for i in range(min(5, len(doc))):
            text = doc[i].get_text().upper()
            if 'CDM' in text or 'CDMF' in text:
                brand = 'CDM'
                break
            if 'LVR' in text or 'LVS' in text:
                brand = 'LVR'
                break

    if brand in ('CDM', 'LVR'):
        return _cdm_lvr_parse(filepath)
    else:
        raw = _universal_parse(filepath)
        return [{'id': r['model'], 'kw': r['kw'], 'q': r['q'], 'head_m': r['h']} for r in raw]

app = Flask(__name__)
app.secret_key = 'cdm-parser-super-secret-key-2026'
app.config['UPLOAD_FOLDER'] = '/root/pump_parser/uploads'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

# Серверное хранилище задач и результатов (вместо cookie-сессий)
parse_tasks = {}

HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PDF Парсер ONIS</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%);
            color: #e0e0e0;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        header {
            text-align: center;
            margin-bottom: 30px;
            padding: 25px;
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            border: 1px solid rgba(46,184,170,0.2);
        }
        h1 {
            font-size: 2.2em;
            background: linear-gradient(135deg, #2EB8AA, #1A9E8F);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 8px;
        }
        .subtitle { color: #999; font-size: 1.05em; }
        .upload-section {
            background: rgba(255,255,255,0.05);
            border: 2px dashed rgba(46,184,170,0.5);
            border-radius: 12px;
            padding: 40px;
            text-align: center;
            margin-bottom: 30px;
            transition: border-color 0.3s, background 0.3s;
        }
        .upload-section:hover { border-color: #2EB8AA; }
        .upload-section.dragover { border-color: #2EB8AA; background: rgba(46,184,170,0.08); }
        .upload-icon { font-size: 3.5em; margin-bottom: 15px; }
        .upload-text { font-size: 1.15em; color: #ccc; margin-bottom: 20px; }
        input[type="file"] { position: absolute; left: -9999px; }
        .file-label {
            background: linear-gradient(135deg, #2EB8AA, #1A9E8F);
            color: #fff; padding: 14px 36px;
            border-radius: 8px; font-size: 1.05em;
            cursor: pointer; transition: all 0.3s;
            display: inline-block;
        }
        .file-label:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(46,184,170,0.4);
        }
        .btn {
            background: linear-gradient(135deg, #2EB8AA, #1A9E8F);
            color: #fff; border: none;
            padding: 12px 28px; border-radius: 8px;
            font-size: 1em; cursor: pointer;
            transition: all 0.3s; margin: 8px 4px;
            text-decoration: none; display: inline-block;
        }
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(46,184,170,0.4);
        }
        .btn-secondary { background: rgba(255,255,255,0.1); }
        .results-section {
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 25px;
            margin-top: 20px;
            border: 1px solid rgba(46,184,170,0.15);
        }
        .results-header {
            display: flex; justify-content: space-between;
            align-items: center; margin-bottom: 20px;
            flex-wrap: wrap; gap: 15px;
        }
        .results-title { font-size: 1.4em; color: #2EB8AA; }
        .results-stats { display: flex; gap: 25px; }
        .results-stat { text-align: center; }
        .results-stat-value { font-size: 1.5em; font-weight: bold; color: #2EB8AA; }
        .results-stat-label { font-size: 0.8em; color: #999; }
        .table-container { max-height: 500px; overflow-y: auto; margin-top: 15px; }
        table { width: 100%; border-collapse: collapse; }
        thead { position: sticky; top: 0; background: #2d2d2d; z-index: 10; }
        th {
            padding: 12px; text-align: left;
            border-bottom: 2px solid #2EB8AA;
            color: #2EB8AA; font-weight: 600;
        }
        td { padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.08); }
        tr:hover { background: rgba(46,184,170,0.05); }
        .message {
            padding: 14px 20px; border-radius: 8px;
            margin: 15px 0; text-align: center; font-size: 1.05em;
        }
        .message.success {
            background: rgba(76,175,80,0.15);
            border: 1px solid #4caf50; color: #4caf50;
        }
        .message.error {
            background: rgba(244,67,54,0.15);
            border: 1px solid #f44336; color: #f44336;
        }
        .actions { text-align: center; margin-top: 20px; }
        footer {
            text-align: center; margin-top: 40px;
            padding: 15px; color: #555; font-size: 0.85em;
        }
        .progress-overlay {
            display: none;
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(0,0,0,0.8); z-index: 1000;
            justify-content: center; align-items: center; flex-direction: column;
        }
        .progress-overlay.active { display: flex; }
        .progress-box {
            background: #2d2d2d; border-radius: 12px; padding: 40px 50px;
            border: 1px solid rgba(46,184,170,0.3); text-align: center; min-width: 350px;
        }
        .progress-title { color: #2EB8AA; font-size: 1.2em; margin-bottom: 20px; }
        .progress-bar-bg {
            width: 100%; height: 8px; background: rgba(255,255,255,0.1);
            border-radius: 4px; overflow: hidden; margin-bottom: 12px;
        }
        .progress-bar-fill {
            height: 100%; width: 0%; border-radius: 4px;
            background: linear-gradient(90deg, #2EB8AA, #1A9E8F);
            transition: width 0.3s;
        }
        .progress-percent { color: #ccc; font-size: 1.1em; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>PDF Парсер ONIS</h1>
            <div class="subtitle">Загрузите PDF каталог — получите структурированные данные</div>
        </header>

        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="message {{ category }}">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}

        <div class="progress-overlay" id="progress-overlay">
            <div class="progress-box">
                <div class="progress-title" id="progress-title">Загрузка файла...</div>
                <div class="progress-bar-bg"><div class="progress-bar-fill" id="progress-bar"></div></div>
                <div class="progress-percent" id="progress-percent">0%</div>
            </div>
        </div>

        {% if not parsed_data %}
        <form method="POST" action="/upload" enctype="multipart/form-data" id="upload-form">
            <div class="upload-section" id="drop-zone">
                <div class="upload-icon">📄</div>
                <div>
                    <input type="file" name="file" id="file-input" accept=".pdf" required>
                    <label for="file-input" class="file-label">Выбрать PDF файл</label>
                </div>
            </div>
        </form>
        <script>
        var overlay = document.getElementById('progress-overlay');
        var bar = document.getElementById('progress-bar');
        var pct = document.getElementById('progress-percent');
        var title = document.getElementById('progress-title');

        function uploadFile(file) {
            var fd = new FormData();
            fd.append('file', file);
            overlay.classList.add('active');
            title.textContent = 'Загрузка файла...';
            bar.style.width = '0%';
            pct.textContent = '0%';

            var xhr = new XMLHttpRequest();
            xhr.upload.addEventListener('progress', function(e) {
                if (e.lengthComputable) {
                    var p = Math.round(e.loaded / e.total * 100);
                    bar.style.width = p + '%';
                    pct.textContent = p + '%';
                }
            });
            xhr.addEventListener('load', function() {
                try {
                    var resp = JSON.parse(xhr.responseText);
                    if (resp.task_id) {
                        title.textContent = 'Парсинг...';
                        bar.style.width = '0%';
                        pct.textContent = '0%';
                        pollProgress(resp.task_id, resp.total_pages);
                    } else {
                        window.location.href = '/';
                    }
                } catch(err) { window.location.href = '/'; }
            });
            xhr.addEventListener('error', function() { window.location.href = '/'; });
            xhr.open('POST', '/upload');
            xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
            xhr.send(fd);
        }

        function pollProgress(taskId, totalPages) {
            var interval = setInterval(function() {
                var r = new XMLHttpRequest();
                r.open('GET', '/progress/' + taskId);
                r.addEventListener('load', function() {
                    try {
                        var d = JSON.parse(r.responseText);
                        if (d.status === 'done') {
                            clearInterval(interval);
                            bar.style.width = '100%';
                            pct.textContent = '100%';
                            title.textContent = 'Готово!';
                            setTimeout(function() {
                                window.location.href = '/results/' + taskId;
                            }, 500);
                        } else if (d.status === 'error') {
                            clearInterval(interval);
                            title.textContent = 'Ошибка!';
                            setTimeout(function() { window.location.href = '/'; }, 1000);
                        } else {
                            bar.style.width = d.progress + '%';
                            pct.textContent = d.progress + '%';
                            title.textContent = 'Парсинг...';
                        }
                    } catch(err) {}
                });
                r.send();
            }, 500);
        }

        document.getElementById('file-input').addEventListener('change', function() {
            if (this.files.length) uploadFile(this.files[0]);
        });
        var dropZone = document.getElementById('drop-zone');
        dropZone.addEventListener('dragover', function(e) {
            e.preventDefault();
            dropZone.classList.add('dragover');
        });
        dropZone.addEventListener('dragleave', function(e) {
            e.preventDefault();
            dropZone.classList.remove('dragover');
        });
        dropZone.addEventListener('drop', function(e) {
            e.preventDefault();
            dropZone.classList.remove('dragover');
            var files = e.dataTransfer.files;
            if (files.length && files[0].name.toLowerCase().endsWith('.pdf')) {
                uploadFile(files[0]);
            }
        });
        </script>
        {% endif %}

        {% if parsed_data %}
        <div class="results-section">
            <div class="results-header">
                <div class="results-title">Результаты парсинга</div>
                <div class="results-stats">
                    <div class="results-stat">
                        <div class="results-stat-value">{{ parsed_stats.total }}</div>
                        <div class="results-stat-label">Записей</div>
                    </div>
                    <div class="results-stat">
                        <div class="results-stat-value">{{ parsed_stats.models }}</div>
                        <div class="results-stat-label">Моделей</div>
                    </div>
                </div>
            </div>

            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>№</th>
                            <th>Модель</th>
                            <th>Мощность (кВт)</th>
                            <th>Расход Q (м³/ч)</th>
                            <th>Напор H (м)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for pump in parsed_data %}
                        <tr>
                            <td>{{ loop.index }}</td>
                            <td><strong>{{ pump.id }}</strong></td>
                            <td>{{ pump.kw }}</td>
                            <td>{{ pump.q }}</td>
                            <td>{{ pump.head_m }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

            <div class="actions">
                {% if result_id %}
                <a href="/download/{{ result_id }}" class="btn">Скачать JSON</a>
                <button class="btn" id="btn-save-bot" onclick="saveToBotKB('{{ result_id }}')">Загрузить в базу знаний бота</button>
                {% endif %}
                <a href="/" class="btn btn-secondary">Загрузить другой файл</a>
            </div>
            <div id="save-msg" class="message" style="display:none;"></div>
            <script>
            function saveToBotKB(taskId) {
                var btn = document.getElementById('btn-save-bot');
                btn.textContent = 'Сохранение...';
                btn.style.opacity = '0.6';
                btn.disabled = true;
                var r = new XMLHttpRequest();
                r.open('POST', '/save_to_bot/' + taskId);
                r.addEventListener('load', function() {
                    var msg = document.getElementById('save-msg');
                    try {
                        var d = JSON.parse(r.responseText);
                        if (d.ok) {
                            msg.className = 'message success';
                            msg.textContent = d.message;
                            btn.textContent = 'Загружено';
                        } else {
                            msg.className = 'message error';
                            msg.textContent = d.error;
                            btn.textContent = 'Загрузить в базу знаний бота';
                            btn.style.opacity = '1';
                            btn.disabled = false;
                        }
                    } catch(e) {
                        msg.className = 'message error';
                        msg.textContent = 'Ошибка';
                        btn.textContent = 'Загрузить в базу знаний бота';
                        btn.style.opacity = '1';
                        btn.disabled = false;
                    }
                    msg.style.display = 'block';
                });
                r.send();
            }
            </script>
        </div>
        {% endif %}

        <footer></footer>
    </div>
</body>
</html>'''


@app.route('/')
def index():
    """Главная страница"""
    return render_template_string(HTML_TEMPLATE, parsed_data=None, parsed_stats=None, result_id=None)


def _run_parse(task_id, filepath):
    """Фоновый парсинг"""
    try:
        parse_tasks[task_id]['status'] = 'parsing'
        parse_tasks[task_id]['start_time'] = time.time()

        parsed_data = extract_cdm_from_pdf(filepath)

        if not parsed_data or len(parsed_data) == 0:
            parse_tasks[task_id]['status'] = 'error'
            parse_tasks[task_id]['error'] = 'В PDF не найдено данных о насосах'
        else:
            models = set(p['id'] for p in parsed_data)
            # Сохраняем JSON на диск
            result_path = os.path.join(app.config['UPLOAD_FOLDER'], f'result_{task_id}.json')
            os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
            with open(result_path, 'w', encoding='utf-8') as f:
                json.dump(parsed_data, f, ensure_ascii=False, indent=2)

            # Определяем тип каталога по моделям в данных
            all_ids = ' '.join(p['id'] for p in parsed_data).upper()
            if 'CDM' in all_ids:
                catalog_type = 'CDM'
            elif 'LVR' in all_ids or 'LVS' in all_ids:
                catalog_type = 'LVR'
            elif 'MV' in all_ids:
                catalog_type = 'MV'
            else:
                catalog_type = 'PUMP'

            parse_tasks[task_id]['catalog_type'] = catalog_type
            parse_tasks[task_id]['parsed_data'] = parsed_data
            parse_tasks[task_id]['parsed_stats'] = {
                'total': len(parsed_data),
                'models': len(models)
            }
            parse_tasks[task_id]['result_path'] = result_path
            parse_tasks[task_id]['status'] = 'done'

    except Exception as e:
        parse_tasks[task_id]['status'] = 'error'
        parse_tasks[task_id]['error'] = str(e)
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)


@app.route('/upload', methods=['POST'])
def upload():
    """Загрузка PDF"""
    is_xhr = request.headers.get('X-Requested-With') == 'XMLHttpRequest'

    try:
        if 'file' not in request.files:
            if is_xhr:
                return jsonify({'error': 'Файл не выбран'})
            flash('Файл не выбран', 'error')
            return redirect(url_for('index'))

        file = request.files['file']

        if file.filename == '':
            if is_xhr:
                return jsonify({'error': 'Файл не выбран'})
            flash('Файл не выбран', 'error')
            return redirect(url_for('index'))

        if not file.filename.lower().endswith('.pdf'):
            if is_xhr:
                return jsonify({'error': 'Выберите PDF файл'})
            flash('Пожалуйста, выберите PDF файл', 'error')
            return redirect(url_for('index'))

        # Надёжное сохранение файла
        filename = secure_filename(file.filename)
        if not filename or filename == '':
            filename = 'upload.pdf'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        file.stream.seek(0)
        raw = file.stream.read()
        with open(filepath, 'wb') as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())

        # Проверяем что PDF валидный и считаем страницы
        try:
            import fitz
            doc = fitz.open(filepath)
            total_pages = len(doc)
            doc.close()
        except Exception as e:
            if os.path.exists(filepath):
                os.remove(filepath)
            if is_xhr:
                return jsonify({'error': f'Файл повреждён: {str(e)}'})
            flash(f'Файл повреждён: {str(e)}', 'error')
            return redirect(url_for('index'))

        # Тип каталога определяется ПОСЛЕ парсинга по моделям (CDM, LVR, MV...)
        # Пока ставим заглушку, реальный тип будет определён в _run_parse
        catalog_type = 'PUMP'

        if is_xhr:
            task_id = uuid.uuid4().hex[:8]
            parse_tasks[task_id] = {
                'status': 'starting',
                'progress': 0,
                'total_pages': total_pages,
                'start_time': time.time(),
                'catalog_type': catalog_type
            }

            thread = threading.Thread(target=_run_parse, args=(task_id, filepath), daemon=True)
            thread.start()

            return jsonify({'task_id': task_id, 'total_pages': total_pages})
        else:
            # Обычная форма
            try:
                parsed_data = extract_cdm_from_pdf(filepath)
                if parsed_data:
                    models = set(p['id'] for p in parsed_data)
                    flash(f'Извлечено {len(parsed_data)} записей из {len(models)} моделей', 'success')
                    task_id = uuid.uuid4().hex[:8]
                    result_path = os.path.join(app.config['UPLOAD_FOLDER'], f'result_{task_id}.json')
                    with open(result_path, 'w', encoding='utf-8') as f:
                        json.dump(parsed_data, f, ensure_ascii=False, indent=2)
                    parse_tasks[task_id] = {
                        'parsed_data': parsed_data,
                        'parsed_stats': {'total': len(parsed_data), 'models': len(models)},
                        'result_path': result_path,
                        'status': 'done',
                        'catalog_type': catalog_type
                    }
                    return redirect(f'/results/{task_id}')
                else:
                    flash('В PDF не найдено данных', 'error')
            except Exception as e:
                flash(f'Ошибка: {str(e)}', 'error')
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)
            return redirect(url_for('index'))

    except Exception as e:
        if is_xhr:
            return jsonify({'error': str(e)})
        flash(f'Ошибка: {str(e)}', 'error')
        return redirect(url_for('index'))


@app.route('/progress/<task_id>')
def progress(task_id):
    """Прогресс парсинга — оценка по времени и страницам"""
    task = parse_tasks.get(task_id)
    if not task:
        return jsonify({'status': 'error', 'error': 'Задача не найдена'})

    status = task.get('status', 'unknown')
    total_pages = task.get('total_pages', 1)

    if status == 'done':
        pct = 100
    elif status in ('parsing', 'starting'):
        elapsed = time.time() - task.get('start_time', time.time())
        est_seconds = total_pages * 0.35
        if est_seconds > 0:
            pct = min(int(elapsed / est_seconds * 100), 95)
        else:
            pct = 50
    else:
        pct = 0

    resp = {'status': status, 'progress': pct, 'total_pages': total_pages}
    if status == 'error':
        resp['error'] = task.get('error', '')
    return jsonify(resp)


@app.route('/results/<task_id>')
def results(task_id):
    """Страница результатов"""
    task = parse_tasks.get(task_id)
    if not task or 'parsed_data' not in task:
        flash('Результаты не найдены', 'error')
        return redirect(url_for('index'))

    return render_template_string(
        HTML_TEMPLATE,
        parsed_data=task['parsed_data'],
        parsed_stats=task['parsed_stats'],
        result_id=task_id
    )


@app.route('/download/<task_id>')
def download(task_id):
    """Скачать JSON"""
    task = parse_tasks.get(task_id)
    if not task or 'result_path' not in task:
        flash('Нет данных для скачивания', 'error')
        return redirect(url_for('index'))

    result_path = task['result_path']
    if not os.path.exists(result_path):
        flash('Файл результатов не найден', 'error')
        return redirect(url_for('index'))

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    return send_file(
        result_path,
        as_attachment=True,
        download_name=f"parsed_{timestamp}.json",
        mimetype='application/json'
    )


@app.route('/save_to_bot/<task_id>', methods=['POST'])
def save_to_bot(task_id):
    """Сохранить результат в базу знаний бота (/root/) с именем каталога"""
    task = parse_tasks.get(task_id)
    if not task or 'parsed_data' not in task:
        return jsonify({'ok': False, 'error': 'Результаты не найдены'})

    try:
        # Сохраняем в /root/pump_base/{TYPE}_BASE.json
        catalog_type = task.get('catalog_type', 'PUMP')
        os.makedirs('/root/pump_base', exist_ok=True)
        dest_path = f'/root/pump_base/{catalog_type}_BASE.json'
        with open(dest_path, 'w', encoding='utf-8') as f:
            json.dump(task['parsed_data'], f, ensure_ascii=False, indent=2)
        count = len(task['parsed_data'])
        return jsonify({'ok': True, 'message': f'Сохранено {count} записей → pump_base/{catalog_type}_BASE.json'})
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Ошибка сохранения: {str(e)}'})


# Алиас для совместимости
application = app

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
