#!/usr/bin/env python3
"""
PDF Pump Parser v3 — Slim Flask app.
All parsing logic in pipeline/, storage in storage/, models in models/.
This file: routes only.
"""
import os
import sys
import json
import threading
import time
import uuid
import logging

sys.path.insert(0, "/root/pump_parser")

from flask import (Flask, request, redirect, url_for, flash, jsonify,
                   render_template_string, send_file, send_from_directory)
from werkzeug.utils import secure_filename

from config import (UPLOAD_DIR, BASE_DIR, ONIS_DB_DIR, PHOTOS_DIR, DRAWINGS_DIR,
                    MAX_UPLOAD_SIZE, SECRET_KEY, CATALOGS_DIR, BACKUP_API_KEY)
from pipeline.orchestrator import PipelineOrchestrator
from storage.task_manager import TaskManager
from storage.base_manager import BaseManager
from brand_qualifier import BrandQualifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# ── App setup ───────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config["UPLOAD_FOLDER"] = UPLOAD_DIR
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_SIZE

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(ONIS_DB_DIR, exist_ok=True)
os.makedirs(PHOTOS_DIR, exist_ok=True)

# ── Shared instances ────────────────────────────────────────────────

pipeline = PipelineOrchestrator()
task_mgr = TaskManager()
base_mgr = BaseManager()
brand_qual = BrandQualifier()

# Legacy: keep parse_tasks in memory for backward compat with UI polling
parse_tasks = task_mgr._tasks


# ── Helper: run pipeline in background thread ───────────────────────

def _run_pipeline(task_id, filepath):
    """Background thread: run 4-stage pipeline, update task."""
    try:
        def progress_cb(phase, pct):
            task_mgr.update_task(task_id, {"phase": phase, "progress": pct, "status": "parsing"})

        result = pipeline.parse(filepath, progress_cb=progress_cb)

        # Save result JSON
        result_path = os.path.join(UPLOAD_DIR, f"result_{task_id}.json")
        models_dicts = [m.to_dict() for m in result.models]
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(models_dicts, f, ensure_ascii=False, indent=2)

        task_mgr.update_task(task_id, {
            "status": "done",
            "parsed_data": models_dicts,
            "parsed_stats": {
                "total": result.total_models,
                "complete": result.complete_models,
                "completeness": result.completeness,
                "series": len(result.series_detected),
                "time": result.elapsed,
            },
            "result_path": result_path,
            "catalog_type": result.series_detected[0] if result.series_detected else "PUMP",
            "brand": result.brand,
            "brand_confidence": result.brand_confidence,
            "brand_source": result.brand_source,
            "series_detected": result.series_detected,
            "models_found": result.total_models,
            "elapsed": result.elapsed,
            "stages_completed": result.stages_completed,
            "errors": result.errors,
        })

    except Exception as e:
        logging.error("Pipeline error for task %s: %s", task_id, e)
        task_mgr.update_task(task_id, {"status": "error", "error": str(e)})
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)


# ══════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════

# ── Main pages (import HTML from old parser_app for now) ─────────────
# TODO: Story 8.1 — extract to ui/templates/*.html

# For now, import HTML_TEMPLATE, DOCS_HTML, CATALOGS_HTML from old file
try:
    from parser_app import HTML_TEMPLATE, DOCS_HTML, CATALOGS_HTML
    _HAS_TEMPLATES = True
except ImportError:
    _HAS_TEMPLATES = False
    HTML_TEMPLATE = "<h1>Parser v3</h1><p>Templates not loaded</p>"
    DOCS_HTML = "<h1>Docs</h1>"
    CATALOGS_HTML = "<h1>Catalogs</h1>"


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE, parsed_data=None, parsed_stats=None,
                                  result_id=None, catalog_type=None, existing_catalogs=None)


@app.route("/docs")
def docs():
    return render_template_string(DOCS_HTML)


@app.route("/catalogs")
def catalogs_page():
    if _HAS_TEMPLATES:
        from parser_app import load_onis_catalogs
        catalogs = load_onis_catalogs()
    else:
        catalogs = []
    return render_template_string(CATALOGS_HTML, catalogs=catalogs, parse_tasks=parse_tasks)


# ── Upload + Parse ──────────────────────────────────────────────────

@app.route("/upload", methods=["POST"])
def upload():
    is_xhr = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    if "file" not in request.files:
        return jsonify({"error": "Файл не выбран"}) if is_xhr else (flash("Файл не выбран", "error"), redirect("/"))[1]

    f = request.files["file"]
    if not f.filename or not f.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Нужен PDF"}) if is_xhr else (flash("Нужен PDF", "error"), redirect("/"))[1]

    filename = secure_filename(f.filename)
    filepath = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex[:8]}_{filename}")
    f.save(filepath)

    file_size = os.path.getsize(filepath)

    # Count pages
    total_pages = 0
    try:
        import fitz
        doc = fitz.open(filepath)
        total_pages = len(doc)
        doc.close()
    except Exception:
        pass

    task_id, is_new = task_mgr.create_task(filename, file_size)

    if not is_new:
        os.remove(filepath)
        task = task_mgr.get_task(task_id)
        return jsonify({"task_id": task_id, "status": task.get("status", "done"),
                        "models_found": task.get("models_found", 0), "cached": True})

    task_mgr.update_task(task_id, {
        "status": "uploading",
        "total_pages": total_pages,
        "phase": "Запуск pipeline...",
        "progress": 5,
        "onis_mode": False,
    })

    t = threading.Thread(target=_run_pipeline, args=(task_id, filepath), daemon=True)
    t.start()

    return jsonify({"task_id": task_id, "total_pages": total_pages})


# ── Progress ────────────────────────────────────────────────────────

@app.route("/progress/<task_id>")
def progress(task_id):
    task = task_mgr.get_task(task_id)
    if not task:
        return jsonify({"status": "error", "error": "Задача не найдена"})

    status = task.get("status", "unknown")
    if status == "done":
        return jsonify({
            "status": "done",
            "progress": 100,
            "models_found": task.get("models_found", 0),
            "elapsed": str(task.get("elapsed", 0)),
            "brand": task.get("brand", ""),
            "brand_confidence": task.get("brand_confidence", 0),
        })
    elif status == "error":
        return jsonify({"status": "error", "error": task.get("error", "")})
    else:
        return jsonify({
            "status": "parsing",
            "progress": task.get("progress", 0),
            "phase": task.get("phase", "Парсинг..."),
        })


# ── Results ─────────────────────────────────────────────────────────

@app.route("/results/<task_id>")
def results(task_id):
    task = task_mgr.get_task(task_id)
    if not task:
        return "Задача не найдена", 404
    data = task.get("parsed_data", [])
    stats = task.get("parsed_stats", {})
    catalog_type = task.get("catalog_type", "PUMP")
    return render_template_string(HTML_TEMPLATE, parsed_data=data, parsed_stats=stats,
                                  result_id=task_id, catalog_type=catalog_type, existing_catalogs=None)


@app.route("/download/<task_id>")
def download(task_id):
    task = task_mgr.get_task(task_id)
    if not task or "result_path" not in task:
        return "Нет данных", 404
    return send_file(task["result_path"], as_attachment=True,
                     download_name=f"pumps_{task.get('catalog_type', 'data')}.json")


# ── Save to bot KB ──────────────────────────────────────────────────

@app.route("/save_to_bot/<task_id>", methods=["POST"])
def save_to_bot(task_id):
    task = task_mgr.get_task(task_id)
    if not task or "parsed_data" not in task:
        return jsonify({"ok": False, "error": "Нет данных"})

    catalog_type = task.get("catalog_type", "PUMP")
    models = task["parsed_data"]
    brand = task.get("brand", "Unknown")

    # Add brand to each model
    for m in models:
        m["brand"] = brand

    # Detect series and save per-series BASE files
    from models.pump_model import detect_series, detect_catalog_type
    series_groups = {}
    for m in models:
        s = detect_series(m.get("model", ""))
        series_groups.setdefault(s, []).append(m)

    saved = 0
    for series, series_models in series_groups.items():
        if not series or len(series) < 2:
            continue
        base_models = []
        for m in series_models:
            base_models.append({
                "id": m.get("model", ""),
                "kw": m.get("power_kw", 0),
                "q": m.get("q_nom", 0),
                "head_m": m.get("h_nom", 0),
                "series": series,
                "brand": brand,
                "flagship": series.upper() in ("MV", "INL", "MBL"),
                "confidence": m.get("confidence", 0),
            })
        count = base_mgr.save(series, base_models)
        saved += count

    base_mgr.rebuild_index()

    return jsonify({"ok": True, "message": f"Сохранено {saved} моделей ({catalog_type})"})


# ── API: Bases ──────────────────────────────────────────────────────

@app.route("/api/bases")
def api_bases():
    try:
        with open(os.path.join(BASE_DIR, "brands_index.json")) as f:
            index = json.load(f)
    except Exception:
        index = {}

    brands = []
    total_models = 0
    for brand_name, brand_data in index.items():
        series_list = brand_data.get("series", [])
        brand_models = sum(s["count"] for s in series_list)
        total_models += brand_models
        brands.append({
            "brand": brand_name,
            "series": sorted(series_list, key=lambda s: (-int(s.get("flagship", False)), -s["count"])),
            "series_count": len(series_list),
            "models_count": brand_models,
        })
    brands.sort(key=lambda b: (0 if b["brand"] == "ONIS" else 1, -b["models_count"]))
    return jsonify({"brands": brands, "total_models": total_models, "total_brands": len(brands)})


@app.route("/api/rebrand", methods=["POST"])
def api_rebrand():
    updated = 0
    brand_summary = {}
    for b in base_mgr.list_bases():
        models = base_mgr.load(b["name"])
        if not models:
            continue
        br = brand_qual.qualify_from_models(models)
        flagship = b["name"].upper() in ("MV", "INL", "MBL")
        changed = False
        for m in models:
            if m.get("brand") != br.brand or m.get("flagship") != flagship:
                m["brand"] = br.brand
                m["flagship"] = flagship
                changed = True
        if changed:
            base_mgr.save(b["name"], models)
            updated += 1
        brand_summary[br.brand] = brand_summary.get(br.brand, 0) + 1

    base_mgr.rebuild_index()
    return jsonify({"ok": True, "updated": updated, "brands": brand_summary})


@app.route("/api/reparse-all", methods=["POST"])
def api_reparse_all():
    if not os.path.isdir(CATALOGS_DIR):
        return jsonify({"ok": False, "error": "Catalogs dir not found"})
    pdfs = sorted([f for f in os.listdir(CATALOGS_DIR) if f.lower().endswith(".pdf")])
    results = []
    for pdf in pdfs:
        br = brand_qual.qualify(os.path.join(CATALOGS_DIR, pdf))
        results.append({"file": pdf, "brand": br.brand, "confidence": br.confidence})
    return jsonify({"ok": True, "catalogs": results, "total": len(results)})


# ── ONIS Flagships (legacy compat) ──────────────────────────────────

@app.route("/onis/parse_auto", methods=["POST"])
def onis_parse_auto():
    """Upload PDF for ONIS flagships — uses same pipeline."""
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "Файл не выбран"})
    f = request.files["file"]
    if not f.filename:
        return jsonify({"ok": False, "error": "Пустое имя"})

    filename = secure_filename(f.filename)
    filepath = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex[:8]}_{filename}")
    f.save(filepath)

    file_size = os.path.getsize(filepath)
    task_id, is_new = task_mgr.create_task(filename, file_size)

    if not is_new:
        os.remove(filepath)
        return jsonify({"ok": True, "task_id": task_id, "cached": True})

    # Extract catalog name from filename
    name = os.path.splitext(filename)[0].upper()
    task_mgr.update_task(task_id, {
        "status": "uploading",
        "onis_mode": True,
        "onis_name": name,
        "phase": "Запуск pipeline...",
    })

    t = threading.Thread(target=_run_pipeline, args=(task_id, filepath), daemon=True)
    t.start()

    return jsonify({"ok": True, "task_id": task_id})


# ── Static files ────────────────────────────────────────────────────

@app.route("/drawings/<path:filename>")
def serve_drawing(filename):
    return send_from_directory(DRAWINGS_DIR, filename)

@app.route("/photos/<filename>")
def serve_photo(filename):
    return send_from_directory(PHOTOS_DIR, filename)


# ── Backup API ──────────────────────────────────────────────────────

@app.route("/api/backup", methods=["GET"])
def api_backup():
    key = request.args.get("key", "")
    if key != BACKUP_API_KEY:
        return jsonify({"error": "Invalid API key"}), 403
    import zipfile, io
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(BASE_DIR):
            if fname.endswith(".json"):
                zf.write(os.path.join(BASE_DIR, fname), fname)
    buf.seek(0)
    return send_file(buf, mimetype="application/zip", as_attachment=True, download_name="pump_base_backup.zip")


# ── Run ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
