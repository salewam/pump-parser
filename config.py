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
VLM_TIMEOUT = 300
VLM_RETRIES = 4
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

# ── Known Series (universal — all brands) ───────────────────────────
KNOWN_SERIES = {
    # Wellmix
    "CDM", "CDMF", "CDL", "CDLF", "CV", "CVF", "CMI", "PV",
    "TG", "TL", "TD", "NBS", "CHL", "CHLF",
    # ONIS
    "MV", "MVS", "INL", "MBL",
    # Fancy
    "FV", "FVH", "FST", "FS", "FS4", "FSM", "FZ",
    "CHL", "CHLF", "BM", "BMN",
    # Aquastrong
    "EVR", "EVS", "EST", "ESST", "EJ", "EWS",
    # Lowara
    "LVR", "CEA", "CA", "CAM", "e-SV", "e-HM", "e-NSC", "e-GS",
    "FH", "FC", "FCT", "ESH", "VM", "BG", "CO", "SVI",
    "SCUBA", "DOC", "DOMO", "DIWA",
    # CNP
    "ZS", "SJ",
    # Ebara
    "DW", "DWF", "CDX", "CDXM", "MATRIX", "EVMS", "GP", "GPE",
    "CDA", "BestOne", "3D",
    # Pedrollo
    "PK", "PKm", "CP", "CPm", "F", "HT", "RX", "4SR", "6SR",
    "TOP", "UP", "D", "BC",
    # Grundfos
    "CR", "CRI", "CRN", "CRE", "CM", "CME", "SP",
    # Wilo
    "MHI", "MHIL", "MVI", "MVIL", "Helix",
    "CronoLine", "CronoTwin", "CronoBloc",
    # KSB
    "Etanorm", "Movitec", "Multitec", "MegaCPK",
    # DAB
    "SET", "NKM", "NKP", "KDN", "KH", "NOVA", "FEKA", "DRENAG",
    "VEA", "VA", "ALM", "KLM", "DKLM",
}

HORIZONTAL_SERIES = {"INL", "MBL", "FVH", "FV", "FST", "FS", "FS4", "FSM", "NBS",
                     "CEA", "CA", "CAM", "FH", "FC", "CO", "e-NSC", "e-HM",
                     "F", "CP", "CPm", "PK", "PKm", "CR", "CRI", "CRN", "CM", "CME",
                     "Etanorm", "MHI", "MHIL", "ZS"}
VERTICAL_SERIES = {"MV", "MVS", "CDM", "CDMF", "CDL", "CDLF", "CV", "CVF",
                   "EVR", "EVS", "CMI", "PV", "TG", "TL", "TD", "EST", "ESST",
                   "e-SV", "e-GS", "SCUBA", "SVI", "4SR", "6SR",
                   "MVI", "MVIL", "EVMS", "Movitec", "Multitec", "SP"}
FLAGSHIP_SERIES = {"MV", "INL", "MBL"}

# ── Pump Model Regex (universal) ────────────────────────────────────
# Matches any pump model: series prefix + numbers with separators
# Examples: CDM32-1-1, CR 1-2, MHI 204, F 32/160C, PKm65, e-SV 5SV07,
#           Etanorm 040-025-160, ZS 65-40-200/7.5, DW 75, MATRIX 3-2T/0.45
PUMP_MODEL_RE = (
    r"(?:"
    # Original ONIS/Wellmix/Fancy/Aquastrong series
    r"(?:CDM|CDMF|CV|CVF|CMI|NBS|TG|TL|TD|FST|FS4|FSM|FV|FVH|EVR|EVS|"
    r"CDL|CDLF|INL|MBL|МВL|МBL|CHL|CHLF|BM|BMN|LVR|EST|ESST|LLT)"
    r"|"
    # Lowara series
    r"(?:CEA|CA[M]?|e-SV|e-HM|e-NSC|e-GS|e-LNE|e-IXP|FH[EFS]?|FC[T]?|"
    r"ESH|VM|BG|CO|SVI|DOC|DOMO|DIWA|SCUBA|\dSC)"
    r"|"
    # CNP
    r"(?:ZS|SJ)"
    r"|"
    # Ebara
    r"(?:DW[F]?|CDX[M]?|MATRIX|EVMS[NKL]?|GP[E]?|CDA|BestOne)"
    r"|"
    # Pedrollo
    r"(?:PK[m]?|CP[m]?|HT|RX|TOP|UP|BC|\d+SR|\d+HR)"
    r"|"
    # Grundfos
    r"(?:CR[IENM]?|CM[E]?|SP[A]?)"
    r"|"
    # Wilo
    r"(?:MHI[L]?|MVI[LSE]?|Helix|CronoLine|CronoTwin|CronoBloc)"
    r"|"
    # KSB
    r"(?:Etanorm|Movitec|Multitec|MegaCPK)"
    r"|"
    # DAB
    r"(?:SET|NKM|NKP|KDN|KH|NOVA|FEKA|DRENAG|VEA|ALM|KLM|DKLM)"
    r"|"
    # Generic fallback: 1-5 letters + digit + separator + digits
    r"(?:[A-ZА-Я]{1,5}\d)"
    r")"
    r"\s*[/]?\s*\w*\s*\d+[\s\-/]*\d*"
)

# ── Flask ───────────────────────────────────────────────────────────
SECRET_KEY = "cdm-parser-super-secret-key-2026"
