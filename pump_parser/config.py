"""Global settings and constants for pump_parser."""

from pathlib import Path
from dataclasses import dataclass, field

# ─── Paths ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RECIPE_DIR = DATA_DIR / "recipes"
OUTPUT_DIR = DATA_DIR / "output"
LOG_DIR = DATA_DIR / "logs"
DB_PATH = DATA_DIR / "pumps.db"

# Ensure dirs exist
for d in (RECIPE_DIR, OUTPUT_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ─── Extraction Defaults ──────────────────────────────────────────────────────

DEFAULT_TABLE_STRATEGY = "auto"  # "auto" | "fitz" | "pdfplumber" | "lines" | "text"
MIN_TABLE_ROWS = 2
MIN_TABLE_COLS = 2
PAGE_IMAGE_DPI = 150

# ─── Confidence Thresholds ────────────────────────────────────────────────────

CONFIDENCE_HIGH = 0.90
CONFIDENCE_MEDIUM = 0.70
CONFIDENCE_LOW = 0.50

RECIPE_GENERATE_THRESHOLD = 0.80    # min avg confidence to auto-generate recipe
RECIPE_DEMOTE_THRESHOLD = 0.70      # rolling success rate below → demote
RECIPE_DELETE_THRESHOLD = 0.50      # rolling success rate below → delete
RECIPE_DELETE_CONSECUTIVE = 3       # consecutive failures → delete

SELF_HEAL_TRIGGER = 0.20            # % of entries with low confidence to trigger self-heal

# ─── Discovery ────────────────────────────────────────────────────────────────

DISCOVERY_EARLY_STOP_SCORE = 90     # if extractor scores > this, skip remaining
DISCOVERY_AI_FALLBACK_SCORE = 50    # if best score < this, use Vision AI

# ─── Physics ──────────────────────────────────────────────────────────────────

RHO_WATER = 998.0       # kg/m³ at 20°C
G_GRAVITY = 9.81         # m/s²
ETA_MIN_VALID = 0.02     # below = garbage
ETA_MAX_VALID = 0.96     # above = impossible
ETA_RANGE_NORMAL = (0.15, 0.90)

# ─── Scoring Weights ─────────────────────────────────────────────────────────

SCORE_QUANTITY_WEIGHT = 0.5     # per model, capped at 50
SCORE_QUANTITY_CAP = 50
SCORE_COMPLETENESS_WEIGHT = 30  # % entries with Q>0, H>0, P>0
SCORE_PHYSICS_WEIGHT = 30       # % entries passing physics validation
SCORE_GROUNDING_WEIGHT = 20     # % model names found in page text
SCORE_CURVE_BONUS = 2           # per entry with curve, capped at 15
SCORE_CURVE_CAP = 15

# ─── Vision AI ────────────────────────────────────────────────────────────────


@dataclass
class VisionConfig:
    model: str = "gemini-2.5-flash-lite"
    fallback_model: str = "gemini-2.5-flash"
    max_pages_per_pdf: int = 30
    skip_if_text_confidence: float = 0.85
    batch_size: int = 2
    rate_limit_seconds: int = 3
    max_cost_per_pdf_usd: float = 0.50
    cost_per_page_primary: float = 0.002
    cost_per_page_fallback: float = 0.008


VISION = VisionConfig()

# ─── Batch ────────────────────────────────────────────────────────────────────

BATCH_WORKERS = 4
BATCH_TIMEOUT_PER_PDF = 300  # seconds

# ─── Logging ──────────────────────────────────────────────────────────────────

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5
