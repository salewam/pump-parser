"""Classify PDF pages by type: data table, graph, dimensions, cover, etc.

Supports two modes:
1. Text-based classification (fast, no API) — keyword scoring + structural signals
2. Vision AI classification (fallback) — sends page image to Gemini
"""

import re
import logging

from pump_parser.models import PageType, ClassifiedPage, PDFDocument

log = logging.getLogger("pump_parser.page_classifier")

# Map AI response strings to PageType
_AI_TYPE_MAP = {
    "data_table": PageType.DATA_TABLE,
    "data_graph": PageType.DATA_GRAPH,
    "cover": PageType.COVER,
    "toc": PageType.TOC,
    "dimensions": PageType.DIMENSIONS,
    "model_code": PageType.MODEL_CODE,
    "model_range": PageType.MODEL_RANGE,
    "materials": PageType.MATERIALS,
    "installation": PageType.INSTALLATION,
    "other": PageType.OTHER,
}

# ─── Keywords ─────────────────────────────────────────────────────────────────

# Pages to SKIP (not pump performance data)
SKIP_KEYWORDS = {
    # Dimensions / drawings
    "габаритные размеры", "dimensional drawing", "dimensions and", "maße",
    "dimensioni", "dimensions et poids", "dimensiones",
    "overall dimensions", "outline dimensions",
    # Installation
    "инструкция по", "installation", "montage", "installazione",
    "руководство по эксплуатации", "operating instructions",
    "instructions de", "istruzioni",
    # Materials
    "материалы конструкции", "material description", "werkstoff",
    "materiali", "matériaux", "взрыв-схема", "exploded view",
    "explosionszeichnung", "vista esplosa",
    # Warranty / legal
    "гарантийные", "warranty", "garantie", "garanzia",
    # Ordering / packaging
    "условия заказа", "ordering", "bestellung", "commande",
    "упаковка", "packaging",
}

# Pages with DATA tables
DATA_KEYWORDS = {
    # Performance headers (RU)
    "qном", "hном", "qном.", "hном.", "номинальный расход", "номинальный напор",
    "подача", "напор", "мощность p2", "мощность, квт", "артикул",
    # Performance headers (EN)
    "q=delivery", "technical data", "performance data", "pump data",
    "rated flow", "rated head", "nominal flow", "nominal head",
    # Performance headers (MULTI)
    "delivery/caudal", "débit/подача",
    # Unit markers in table context
    "м³/ч", "m³/h", "m3/h", "l/min", "gpm",
}

# Graph / curve indicators
GRAPH_KEYWORDS = {
    "q-h", "h-q", "performance curve", "характеристик",
    "кривая", "рабочая точка", "working point",
    "kennlinie", "courbe", "curva",
}

# Cover page indicators
COVER_KEYWORDS = {
    "каталог продукции", "product catalog", "produktkatalog",
    "catalogue", "catalogo", "серия насосов", "pump series",
    "модельный ряд",
}

# TOC indicators
TOC_KEYWORDS = {
    "содержание", "содержание:", "table of contents", "contents",
    "inhaltsverzeichnis", "sommaire", "indice",
}

# Model code explanation
MODEL_CODE_KEYWORDS = {
    "расшифровка обозначения", "расшифровка модели", "model designation",
    "type key", "typenschlüssel", "обозначение модели",
    "код модели", "model code",
}

# Model range overview
MODEL_RANGE_KEYWORDS = {
    "рабочее поле", "selection chart", "auswahldiagramm",
    "campo di lavoro", "диапазон применения", "application range",
    "рабочий диапазон", "operating range",
}

# ─── Patterns ─────────────────────────────────────────────────────────────────

# Model name patterns (indicate data pages)
RE_MODEL_NAMES = re.compile(
    r"[A-ZА-ЯЁ]{2,6}[\s\-]?\d{1,3}[\s\-/]\d{1,3}",
)

# Numeric table pattern: 3+ numbers in a row separated by whitespace
RE_NUMERIC_ROW = re.compile(
    r"(?:[\d]+[.,]?\d*\s+){3,}[\d]+[.,]?\d*"
)

# Q header pattern (curve tables)
RE_Q_HEADER = re.compile(
    r"Q\s*[\(\[]?\s*(?:м[³3]/ч|m[³3]/h|m3/h|l/min|gpm|л/мин)",
    re.IGNORECASE
)

# Unit row pattern
RE_UNIT_ROW = re.compile(
    r"(?:м³/ч|m³/h|m3/h|квт|kw|hp|об/мин|rpm|л/с|l/s|gpm)",
    re.IGNORECASE
)


def _keyword_score(text_lower: str, keywords: set) -> int:
    """Count how many keywords appear in text."""
    return sum(1 for kw in keywords if kw in text_lower)


def classify_page(text: str, page_num: int, has_tables: bool = False, num_tables: int = 0) -> ClassifiedPage:
    """Classify a single page by its text content.

    Args:
        text: raw text from page
        page_num: 0-indexed page number
        has_tables: whether table extractor found tables
        num_tables: number of tables found

    Returns:
        ClassifiedPage with type and confidence
    """
    text_lower = text.lower().strip()
    text_len = len(text_lower)

    # Very short pages = likely cover or image-only
    if text_len < 30:
        return ClassifiedPage(
            page_num=page_num,
            page_type=PageType.COVER if page_num < 3 else PageType.OTHER,
            text=text,
            confidence=0.5,
        )

    # Score each category
    skip_score = _keyword_score(text_lower, SKIP_KEYWORDS)
    data_score = _keyword_score(text_lower, DATA_KEYWORDS)
    graph_score = _keyword_score(text_lower, GRAPH_KEYWORDS)
    cover_score = _keyword_score(text_lower, COVER_KEYWORDS)
    toc_score = _keyword_score(text_lower, TOC_KEYWORDS)
    code_score = _keyword_score(text_lower, MODEL_CODE_KEYWORDS)
    range_score = _keyword_score(text_lower, MODEL_RANGE_KEYWORDS)

    # Boost data_score with structural signals
    model_matches = len(RE_MODEL_NAMES.findall(text))
    numeric_rows = len(RE_NUMERIC_ROW.findall(text))
    has_q_header = bool(RE_Q_HEADER.search(text))
    has_units = bool(RE_UNIT_ROW.search(text))

    if model_matches >= 3:
        data_score += 3
    elif model_matches >= 1:
        data_score += 1

    if numeric_rows >= 3:
        data_score += 2

    if has_tables and num_tables >= 1:
        data_score += 2

    if has_q_header:
        data_score += 2

    if has_units:
        data_score += 1

    # Decision
    scores = {
        PageType.DIMENSIONS: skip_score if "габарит" in text_lower or "dimension" in text_lower else 0,
        PageType.INSTALLATION: skip_score if "инструкция" in text_lower or "installation" in text_lower else 0,
        PageType.MATERIALS: skip_score if "материал" in text_lower or "material" in text_lower else 0,
        PageType.DATA_TABLE: data_score,
        PageType.DATA_GRAPH: graph_score + (2 if has_q_header and not has_tables else 0),
        PageType.COVER: cover_score + (2 if page_num == 0 else 0),
        PageType.TOC: toc_score,
        PageType.MODEL_CODE: code_score,
        PageType.MODEL_RANGE: range_score,
    }

    # Pick highest score
    best_type = max(scores, key=scores.get)
    best_score = scores[best_type]

    # If skip_score is high and best isn't data, mark as skip type
    if skip_score >= 2 and best_type not in (PageType.DATA_TABLE, PageType.DATA_GRAPH):
        # Determine which skip type
        if "габарит" in text_lower or "dimension" in text_lower:
            best_type = PageType.DIMENSIONS
        elif "инструкция" in text_lower or "installation" in text_lower:
            best_type = PageType.INSTALLATION
        elif "материал" in text_lower or "material" in text_lower:
            best_type = PageType.MATERIALS

    # Fallback: if no strong signal, check for numeric density
    if best_score <= 1:
        if numeric_rows >= 5 and model_matches >= 2:
            best_type = PageType.DATA_TABLE
            best_score = 3
        elif page_num == 0:
            best_type = PageType.COVER
        else:
            best_type = PageType.OTHER

    # Calculate confidence
    confidence = min(0.95, 0.5 + best_score * 0.1)

    return ClassifiedPage(
        page_num=page_num,
        page_type=best_type,
        text=text,
        confidence=confidence,
    )


def classify_page_vision(
    page_image: bytes,
    page_num: int,
    vision_api,
) -> ClassifiedPage | None:
    """Classify a page using Vision AI (Gemini).

    Args:
        page_image: PNG bytes of the page
        page_num: page number
        vision_api: VisionAPI instance

    Returns:
        ClassifiedPage or None if AI fails.
    """
    if not page_image or not vision_api:
        return None

    from pump_parser.vision.prompts import get_prompt

    prompt = get_prompt("page_classify")
    result = vision_api.classify_page(page_image, prompt)

    if not result or not isinstance(result, dict):
        return None

    ai_type_str = result.get("page_type", "other")
    page_type = _AI_TYPE_MAP.get(ai_type_str, PageType.OTHER)
    confidence = float(result.get("confidence", 0.5))

    log.debug(
        "Vision classify page %d: %s (conf=%.2f)",
        page_num, page_type.value, confidence,
    )

    return ClassifiedPage(
        page_num=page_num,
        page_type=page_type,
        text="",
        confidence=confidence,
    )


def classify_all_pages(doc: PDFDocument) -> list[ClassifiedPage]:
    """Classify all pages in a PDF document.

    Returns list of ClassifiedPage sorted by page number.
    """
    if doc._doc is None:
        return []

    from pump_parser.core.table_extractor import extract_tables

    pages: list[ClassifiedPage] = []
    for i in range(doc.num_pages):
        fitz_page = doc._doc[i]
        text = fitz_page.get_text("text")

        # Quick table check
        try:
            tabs = fitz_page.find_tables()
            num_tables = len(tabs.tables) if tabs and tabs.tables else 0
        except Exception:
            num_tables = 0

        cp = classify_page(
            text=text,
            page_num=i,
            has_tables=num_tables > 0,
            num_tables=num_tables,
        )
        pages.append(cp)

    # Log summary
    type_counts = {}
    for p in pages:
        t = p.page_type.value
        type_counts[t] = type_counts.get(t, 0) + 1
    log.info("Page classification: %s", type_counts)

    return pages
