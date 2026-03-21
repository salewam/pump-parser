"""
Parser v3 Configuration.
All constants, paths, timeouts, known series in one place.
"""
import os

# ── GPU Server ──────────────────────────────────────────────────────
GPU_DOCLING_URL = "http://82.22.53.231:5001"
GPU_VISION_URL = "http://82.22.53.231:8000"
GPU_OLLAMA_URL = "http://82.22.53.231:11434"
GPU_API_KEY = os.environ.get("GPU_API_KEY", "")

# ── Timeouts & Retries ─────────────────────────────────────────────
DOCLING_TIMEOUT = 900       # 15 min for large PDFs
DOCLING_HEALTH_TIMEOUT = 5
DOCLING_RETRIES = 3
VLM_TIMEOUT = 60
VLM_RETRIES = 2
OCR_TIMEOUT = 30
OCR_RETRIES = 2
SELFCORRECT_MAX_ATTEMPTS = 3

# ── Confidence ──────────────────────────────────────────────────────
CONFIDENCE_THRESHOLD = 0.8  # below this → trigger OCR verification
OCR_AGREE_THRESHOLD = 0.05  # 5% tolerance for number comparison
RENDER_DPI = 200            # PDF page → PNG render resolution

# ── Paths ───────────────────────────────────────────────────────────
UPLOAD_DIR = "/root/pump_parser/uploads"
BASE_DIR = "/root/pump_base"
ONIS_DB_DIR = "/root/pump_base/onis"
PHOTOS_DIR = "/root/pump_base/photos"
DRAWINGS_DIR = "/root/pump_base/drawings"
TASKS_FILE = "/root/pump_parser/uploads/parse_tasks.json"
CATALOGS_DIR = "/root/ONIS/catalogs"
BRANDS_INDEX = "/root/pump_base/brands_index.json"

# ── Limits ──────────────────────────────────────────────────────────
MAX_UPLOAD_SIZE = 500 * 1024 * 1024  # 500MB
MAX_BATCH_FILES = 10
MAX_PARALLEL_GPU = 3  # GPU server has 3 workers

# ── API Keys ────────────────────────────────────────────────────────
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
BACKUP_API_KEY = os.environ.get("BACKUP_API_KEY", "")

# ── Known Series ────────────────────────────────────────────────────
KNOWN_SERIES = {
    "CDM", "CDMF", "CDL", "CDLF", "CV", "CVF", "CMI", "PV",
    "MV", "MVS", "EVR", "EVS", "TG", "TL", "TD", "INL", "MBL",
    "FV", "FVH", "FST", "FS", "FS4", "FSM", "NBS", "EST", "ESST",
    "LVR", "CHL", "CHLF", "BM", "BMN",
}

HORIZONTAL_SERIES = {"INL", "MBL", "FVH", "FV", "FST", "FS", "FS4", "FSM", "NBS"}
VERTICAL_SERIES = {"MV", "MVS", "CDM", "CDMF", "CDL", "CDLF", "CV", "CVF",
                   "EVR", "EVS", "CMI", "PV", "TG", "TL", "TD", "EST", "ESST"}
FLAGSHIP_SERIES = {"MV", "INL", "MBL"}

# ── Pump Model Regex ────────────────────────────────────────────────
PUMP_MODEL_RE = (
    r"(CDM|CDMF|CV|CVF|CMI|NBS|TG|TL|TD|FST|FS4|FSM|FV|FVH|EVR|EVS|"
    r"CDL|CDLF|INL|MBL|МВL|МBL|CHL|CHLF|BM|BMN)"
    r"\s*[/]?\s*\w*\s*\d+[\s-]*\d*"
)

# ── Flask ───────────────────────────────────────────────────────────
SECRET_KEY = "cdm-parser-super-secret-key-2026"
