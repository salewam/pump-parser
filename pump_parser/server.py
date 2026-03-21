"""FastAPI REST server for pump_parser.

Endpoints:
    POST   /parse              — upload PDF, start async parse job
    GET    /parse/{job_id}     — get job status and result
    POST   /batch              — upload multiple PDFs, start batch parse
    GET    /batch/{batch_id}   — get batch status and per-file results
    GET    /pumps              — search/filter pumps in database
    GET    /pumps/{pump_id}    — get single pump by ID
    GET    /catalogs           — list parsed catalogs
    GET    /catalogs/{id}      — get catalog with its pumps
    DELETE /catalogs/{id}      — delete catalog and pumps
    GET    /stats              — global stats (in-memory + DB)
    GET    /health             — health check
"""

import csv
import io
import json
import uuid
import time
import shutil
import logging
import threading
from pathlib import Path
from typing import Any

from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

from pump_parser.config import OUTPUT_DIR, DATA_DIR, BATCH_WORKERS, BATCH_TIMEOUT_PER_PDF
from pump_parser.core.orchestrator import parse_pdf
from pump_parser.output.writer import write_json, generate_report
from pump_parser.verification.quality_gate import check_quality
from pump_parser.db import PumpDB
from pump_parser.learning.recipe_store import RecipeStore
from pump_parser.learning.recipe import Recipe

log = logging.getLogger("pump_parser.server")

app = FastAPI(
    title="Pump Parser API",
    version="1.0.0",
    description="Universal PDF pump catalog parser",
)

# ─── In-memory job store ──────────────────────────────────────────────────────

UPLOAD_DIR = DATA_DIR / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

db = PumpDB()
recipe_store = RecipeStore()

jobs: dict[str, dict[str, Any]] = {}
batches: dict[str, dict[str, Any]] = {}
_lock = threading.Lock()


def _set_job(job_id: str, data: dict) -> None:
    with _lock:
        jobs[job_id] = data


def _get_job(job_id: str) -> dict | None:
    with _lock:
        return jobs.get(job_id)


# ─── Background parse worker ─────────────────────────────────────────────────

def _run_parse(job_id: str, pdf_path: str, min_confidence: float) -> None:
    """Run parsing in background thread."""
    try:
        _set_job(job_id, {
            "status": "processing",
            "progress": "parsing PDF...",
            "started_at": time.time(),
            "pdf_path": pdf_path,
        })

        result = parse_pdf(pdf_path, min_confidence=min_confidence)

        # Quality gate
        quality = check_quality(result)

        # Write JSON output
        stem = Path(pdf_path).stem
        json_path = str(OUTPUT_DIR / f"{stem}_{job_id[:8]}.json")
        write_json(result, json_path)

        # Build response
        pumps = [e.to_dict() for e in result.entries]
        report_text = generate_report(result)

        _set_job(job_id, {
            "status": "completed",
            "started_at": _get_job(job_id)["started_at"],
            "completed_at": time.time(),
            "pdf_path": pdf_path,
            "result": {
                "source": result.source,
                "total_models": len(result.entries),
                "pages_processed": result.pages_processed,
                "pages_skipped": result.pages_skipped,
                "extraction_time_s": round(result.extraction_time_s, 2),
                "avg_confidence": round(
                    sum(e.confidence for e in result.entries) / len(result.entries), 3
                ) if result.entries else 0.0,
                "recipe_used": result.recipe_used,
                "quality": quality.verdict.value,
                "quality_warnings": quality.warnings,
                "json_output": json_path,
                "pumps": pumps,
            },
            "report": report_text,
        })

        # Save to database
        try:
            catalog_id = db.save_result(result, quality_verdict=quality.verdict.value)
            db.save_job(job_id, Path(pdf_path).name, "completed", catalog_id=catalog_id)
        except Exception as db_err:
            log.warning("Failed to save to DB: %s", db_err)

        log.info("Job %s completed: %d models from %s",
                 job_id, len(result.entries), Path(pdf_path).name)

    except Exception as e:
        log.error("Job %s failed: %s", job_id, e, exc_info=True)
        _set_job(job_id, {
            "status": "failed",
            "error": str(e),
            "started_at": _get_job(job_id)["started_at"] if _get_job(job_id) else time.time(),
            "completed_at": time.time(),
            "pdf_path": pdf_path,
        })


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0"}


@app.post("/parse")
async def parse_upload(
    pdf: UploadFile = File(...),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
):
    """Upload a PDF and start parsing.

    Returns job_id to poll status via GET /parse/{job_id}.
    """
    if not pdf.filename or not pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "File must be a PDF")

    job_id = str(uuid.uuid4())
    pdf_path = str(UPLOAD_DIR / f"{job_id}_{pdf.filename}")

    with open(pdf_path, "wb") as f:
        shutil.copyfileobj(pdf.file, f)

    # Start background parse
    thread = threading.Thread(
        target=_run_parse,
        args=(job_id, pdf_path, min_confidence),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "status": "processing", "filename": pdf.filename}


@app.get("/parse/{job_id}")
def get_parse_status(job_id: str, include_pumps: bool = Query(True)):
    """Get parse job status and results."""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} not found")

    response = {
        "job_id": job_id,
        "status": job["status"],
    }

    if job["status"] == "processing":
        response["progress"] = job.get("progress", "")
        elapsed = time.time() - job.get("started_at", time.time())
        response["elapsed_s"] = round(elapsed, 1)

    elif job["status"] == "completed":
        result = job["result"]
        response["result"] = {
            "source": result["source"],
            "total_models": result["total_models"],
            "pages_processed": result["pages_processed"],
            "pages_skipped": result["pages_skipped"],
            "extraction_time_s": result["extraction_time_s"],
            "avg_confidence": result["avg_confidence"],
            "recipe_used": result["recipe_used"],
            "quality": result["quality"],
            "quality_warnings": result["quality_warnings"],
        }
        if include_pumps:
            response["result"]["pumps"] = result["pumps"]

    elif job["status"] == "failed":
        response["error"] = job.get("error", "Unknown error")

    return response


@app.get("/parse/{job_id}/report")
def get_parse_report(job_id: str):
    """Get detailed text report for a completed job."""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} not found")
    if job["status"] != "completed":
        raise HTTPException(400, f"Job {job_id} is {job['status']}, not completed")

    return JSONResponse(
        content={"job_id": job_id, "report": job.get("report", "")},
        media_type="application/json",
    )


# ─── Batch processing ─────────────────────────────────────────────────────────

def _parse_single_for_batch(pdf_path: str, min_confidence: float) -> dict:
    """Parse a single PDF and return summary dict. Used by batch worker."""
    filename = Path(pdf_path).name
    try:
        result = parse_pdf(pdf_path, min_confidence=min_confidence)
        quality = check_quality(result)

        # Save to DB
        try:
            catalog_id = db.save_result(result, quality_verdict=quality.verdict.value)
        except Exception:
            catalog_id = None

        # Write JSON
        stem = Path(pdf_path).stem
        json_path = str(OUTPUT_DIR / f"{stem}.json")
        write_json(result, json_path)

        return {
            "filename": filename,
            "status": "completed",
            "total_models": len(result.entries),
            "pages_processed": result.pages_processed,
            "extraction_time_s": round(result.extraction_time_s, 2),
            "avg_confidence": round(
                sum(e.confidence for e in result.entries) / len(result.entries), 3
            ) if result.entries else 0.0,
            "quality": quality.verdict.value,
            "catalog_id": catalog_id,
        }
    except Exception as e:
        return {
            "filename": filename,
            "status": "failed",
            "error": str(e),
        }


def _run_batch(batch_id: str, pdf_paths: list[str], min_confidence: float) -> None:
    """Run batch parsing in background thread with thread pool."""
    with _lock:
        batches[batch_id]["status"] = "processing"

    results = []
    total_models = 0
    workers = min(BATCH_WORKERS, len(pdf_paths))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_parse_single_for_batch, path, min_confidence): path
            for path in pdf_paths
        }
        for future in futures:
            res = future.result()
            results.append(res)
            if res["status"] == "completed":
                total_models += res.get("total_models", 0)

            # Update progress
            with _lock:
                batches[batch_id]["completed_count"] = len(results)
                batches[batch_id]["progress"] = f"{len(results)}/{len(pdf_paths)}"

    with _lock:
        batches[batch_id].update({
            "status": "completed",
            "completed_at": time.time(),
            "results": results,
            "total_models": total_models,
            "succeeded": sum(1 for r in results if r["status"] == "completed"),
            "failed_count": sum(1 for r in results if r["status"] == "failed"),
        })

    log.info("Batch %s completed: %d PDFs, %d models",
             batch_id, len(pdf_paths), total_models)


@app.post("/batch")
async def batch_upload(
    pdfs: list[UploadFile] = File(...),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
):
    """Upload multiple PDFs for batch parsing.

    Returns batch_id to poll status via GET /batch/{batch_id}.
    """
    if not pdfs:
        raise HTTPException(400, "No files provided")

    pdf_paths = []
    filenames = []
    batch_id = str(uuid.uuid4())

    for pdf in pdfs:
        if not pdf.filename or not pdf.filename.lower().endswith(".pdf"):
            continue
        path = str(UPLOAD_DIR / f"{batch_id}_{pdf.filename}")
        with open(path, "wb") as f:
            shutil.copyfileobj(pdf.file, f)
        pdf_paths.append(path)
        filenames.append(pdf.filename)

    if not pdf_paths:
        raise HTTPException(400, "No valid PDF files provided")

    with _lock:
        batches[batch_id] = {
            "status": "queued",
            "started_at": time.time(),
            "total": len(pdf_paths),
            "completed_count": 0,
            "filenames": filenames,
            "progress": f"0/{len(pdf_paths)}",
        }

    thread = threading.Thread(
        target=_run_batch,
        args=(batch_id, pdf_paths, min_confidence),
        daemon=True,
    )
    thread.start()

    return {
        "batch_id": batch_id,
        "status": "processing",
        "total": len(pdf_paths),
        "filenames": filenames,
    }


@app.get("/batch/{batch_id}")
def get_batch_status(batch_id: str):
    """Get batch processing status and per-file results."""
    with _lock:
        batch = batches.get(batch_id)
    if not batch:
        raise HTTPException(404, f"Batch {batch_id} not found")

    response = {
        "batch_id": batch_id,
        "status": batch["status"],
        "total": batch["total"],
        "completed_count": batch.get("completed_count", 0),
        "progress": batch.get("progress", ""),
    }

    if batch["status"] == "completed":
        elapsed = batch.get("completed_at", time.time()) - batch.get("started_at", time.time())
        response.update({
            "total_models": batch.get("total_models", 0),
            "succeeded": batch.get("succeeded", 0),
            "failed_count": batch.get("failed_count", 0),
            "elapsed_s": round(elapsed, 1),
            "results": batch.get("results", []),
        })

    return response


@app.get("/stats")
def get_stats():
    """Global parser statistics (in-memory jobs + persistent DB)."""
    with _lock:
        total_jobs = len(jobs)
        completed = [j for j in jobs.values() if j["status"] == "completed"]
        failed = [j for j in jobs.values() if j["status"] == "failed"]
        processing = [j for j in jobs.values() if j["status"] == "processing"]

    avg_time = (
        sum(j["result"]["extraction_time_s"] for j in completed if "result" in j) / len(completed)
        if completed else 0.0
    )

    from pump_parser.config import RECIPE_DIR
    recipe_count = len(list(RECIPE_DIR.glob("*.json")))

    db_stats = db.get_stats()

    return {
        "session_jobs": total_jobs,
        "session_completed": len(completed),
        "session_failed": len(failed),
        "session_processing": len(processing),
        "avg_extraction_time_s": round(avg_time, 2),
        "recipe_count": recipe_count,
        "db_total_catalogs": db_stats["total_catalogs"],
        "db_total_pumps": db_stats["total_pumps"],
        "db_avg_confidence": db_stats["avg_confidence"],
        "db_pumps_with_curve": db_stats["pumps_with_curve"],
    }


# ─── Pump search/filter ──────────────────────────────────────────────────────

@app.get("/pumps")
def search_pumps(
    q_min: float | None = Query(None, description="Min flow Q (m³/h)"),
    q_max: float | None = Query(None, description="Max flow Q (m³/h)"),
    h_min: float | None = Query(None, description="Min head H (m)"),
    h_max: float | None = Query(None, description="Max head H (m)"),
    p_min: float | None = Query(None, description="Min power P (kW)"),
    p_max: float | None = Query(None, description="Max power P (kW)"),
    model: str | None = Query(None, description="Model name substring"),
    series: str | None = Query(None, description="Series name substring"),
    manufacturer: str | None = Query(None, description="Manufacturer substring"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    has_curve: bool | None = Query(None, description="Filter by curve data"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Search pumps in the database with filters."""
    results, total = db.search_pumps(
        q_min=q_min, q_max=q_max,
        h_min=h_min, h_max=h_max,
        p_min=p_min, p_max=p_max,
        model=model, series=series, manufacturer=manufacturer,
        min_confidence=min_confidence,
        has_curve=has_curve,
        limit=limit, offset=offset,
    )
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "pumps": results,
    }


@app.get("/pumps/{pump_id}")
def get_pump(pump_id: int):
    """Get a single pump by database ID."""
    conn = db._get_conn()
    row = conn.execute("SELECT * FROM pumps WHERE id = ?", (pump_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Pump {pump_id} not found")
    return db._pump_row_to_dict(row)


# ─── Catalog endpoints ───────────────────────────────────────────────────────

@app.get("/catalogs")
def list_catalogs():
    """List all parsed catalogs in the database."""
    return {"catalogs": db.list_catalogs()}


@app.get("/catalogs/{catalog_id}")
def get_catalog(catalog_id: int, include_pumps: bool = Query(True)):
    """Get catalog metadata and optionally its pumps."""
    cat = db.get_catalog(catalog_id)
    if not cat:
        raise HTTPException(404, f"Catalog {catalog_id} not found")

    response = {"catalog": cat}
    if include_pumps:
        response["pumps"] = db.get_pumps(catalog_id)
    return response


@app.delete("/catalogs/{catalog_id}")
def delete_catalog(catalog_id: int):
    """Delete a catalog and all its pumps."""
    if not db.delete_catalog(catalog_id):
        raise HTTPException(404, f"Catalog {catalog_id} not found")
    return {"deleted": True, "catalog_id": catalog_id}


# ─── Export endpoints ─────────────────────────────────────────────────────────

EXPORT_FIELDS = [
    "model", "series", "manufacturer", "article",
    "q_nom", "h_nom", "power_kw", "rpm", "efficiency",
    "q_points", "h_points",
    "dn_suction", "dn_discharge", "weight_kg", "voltage", "phases", "stages",
    "source_file", "source_page", "data_source", "confidence",
]


def _export_query(
    q_min, q_max, h_min, h_max, p_min, p_max,
    model, series, manufacturer, min_confidence, has_curve,
) -> list[dict]:
    """Run search with high limit for export."""
    results, _ = db.search_pumps(
        q_min=q_min, q_max=q_max,
        h_min=h_min, h_max=h_max,
        p_min=p_min, p_max=p_max,
        model=model, series=series, manufacturer=manufacturer,
        min_confidence=min_confidence,
        has_curve=has_curve,
        limit=50000, offset=0,
    )
    return results


@app.get("/export/json")
def export_json(
    q_min: float | None = None, q_max: float | None = None,
    h_min: float | None = None, h_max: float | None = None,
    p_min: float | None = None, p_max: float | None = None,
    model: str | None = None, series: str | None = None,
    manufacturer: str | None = None,
    min_confidence: float = 0.0,
    has_curve: bool | None = None,
):
    """Export filtered pumps as downloadable JSON."""
    pumps = _export_query(
        q_min, q_max, h_min, h_max, p_min, p_max,
        model, series, manufacturer, min_confidence, has_curve,
    )
    content = json.dumps({"total": len(pumps), "pumps": pumps}, ensure_ascii=False, indent=2)
    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=pumps_export.json"},
    )


@app.get("/export/csv")
def export_csv(
    q_min: float | None = None, q_max: float | None = None,
    h_min: float | None = None, h_max: float | None = None,
    p_min: float | None = None, p_max: float | None = None,
    model: str | None = None, series: str | None = None,
    manufacturer: str | None = None,
    min_confidence: float = 0.0,
    has_curve: bool | None = None,
):
    """Export filtered pumps as downloadable CSV."""
    pumps = _export_query(
        q_min, q_max, h_min, h_max, p_min, p_max,
        model, series, manufacturer, min_confidence, has_curve,
    )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for p in pumps:
        row = {k: p.get(k, "") for k in EXPORT_FIELDS}
        row["q_points"] = ";".join(str(v) for v in p.get("q_points", []))
        row["h_points"] = ";".join(str(v) for v in p.get("h_points", []))
        writer.writerow(row)

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=pumps_export.csv"},
    )


@app.get("/export/excel")
def export_excel(
    q_min: float | None = None, q_max: float | None = None,
    h_min: float | None = None, h_max: float | None = None,
    p_min: float | None = None, p_max: float | None = None,
    model: str | None = None, series: str | None = None,
    manufacturer: str | None = None,
    min_confidence: float = 0.0,
    has_curve: bool | None = None,
):
    """Export filtered pumps as downloadable Excel (.xlsx).

    Requires openpyxl. Falls back to CSV if not installed.
    """
    pumps = _export_query(
        q_min, q_max, h_min, h_max, p_min, p_max,
        model, series, manufacturer, min_confidence, has_curve,
    )

    try:
        from openpyxl import Workbook
    except ImportError:
        raise HTTPException(500, "openpyxl not installed. Use /export/csv instead.")

    wb = Workbook()
    ws = wb.active
    ws.title = "Pumps"

    # Header row
    ws.append(EXPORT_FIELDS)

    # Data rows
    for p in pumps:
        row = []
        for f in EXPORT_FIELDS:
            val = p.get(f, "")
            if f in ("q_points", "h_points"):
                val = ";".join(str(v) for v in (val if isinstance(val, list) else []))
            row.append(val)
        ws.append(row)

    # Auto-width for first few columns
    for col_idx, field in enumerate(EXPORT_FIELDS[:8], 1):
        ws.column_dimensions[chr(64 + col_idx)].width = max(12, len(field) + 4)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=pumps_export.xlsx"},
    )


# ─── Recipe endpoints ────────────────────────────────────────────────────────

@app.get("/recipes")
def list_recipes():
    """List all recipes with summary info."""
    recipes = recipe_store.all()
    return {
        "total": len(recipes),
        "recipes": [
            {
                "recipe_id": r.recipe_id,
                "name": r.name,
                "manufacturer": r.manufacturer,
                "extractor_type": r.extraction.extractor_type,
                "confidence": r.confidence,
                "uses_count": r.uses_count,
                "success_rate": round(r.success_rate(), 2) if r.uses_count > 0 else None,
                "auto_generated": r.auto_generated,
            }
            for r in recipes
        ],
    }


@app.get("/recipes/{recipe_id}")
def get_recipe(recipe_id: str):
    """Get full recipe details."""
    recipe = recipe_store.load(recipe_id)
    if not recipe:
        raise HTTPException(404, f"Recipe '{recipe_id}' not found")
    return recipe.to_dict()


@app.delete("/recipes/{recipe_id}")
def delete_recipe(recipe_id: str):
    """Delete a recipe."""
    if not recipe_store.delete(recipe_id):
        raise HTTPException(404, f"Recipe '{recipe_id}' not found")
    return {"deleted": True, "recipe_id": recipe_id}


@app.post("/recipes/seed")
def seed_recipes():
    """Generate seed recipes for known catalog formats."""
    from pump_parser.learning.seed_recipes import generate_seed_recipes
    recipes = generate_seed_recipes()
    return {
        "created": len(recipes),
        "recipes": [
            {"recipe_id": r.recipe_id, "name": r.name, "manufacturer": r.manufacturer}
            for r in recipes
        ],
    }
