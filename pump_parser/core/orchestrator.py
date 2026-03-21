"""Main parsing orchestrator — wires PDF → classify → extract → validate → output.

Two paths:
1. Fast path: recipe matched → skip classification, use recipe's extractor directly
2. Discovery path: classify pages → detect table type → try extractors → pick best

Vision AI fallback:
- Graph pages → GraphReaderExtractor with page image
- Low-score pages → Vision AI flat/matrix extraction
- Scanned PDFs → OCR + Vision AI
"""

import time
import logging
from pathlib import Path

from pump_parser.models import (
    PumpEntry, PDFDocument, ParseResult, ParseReport, PageReport,
    PageType, TableType, ExtractionResult,
)
from pump_parser.config import (
    DISCOVERY_EARLY_STOP_SCORE, DISCOVERY_AI_FALLBACK_SCORE,
)
from pump_parser.core.ingestion import load_pdf, get_page_text, get_page_image, close_pdf
from pump_parser.core.table_extractor import extract_tables
from pump_parser.classifiers.page_classifier import classify_page, classify_page_vision
from pump_parser.classifiers.table_type import detect_table_type_for_page
from pump_parser.extractors import (
    FlatTableExtractor,
    QHMatrixExtractor,
    CurveTableExtractor,
    TransposedExtractor,
    GraphReaderExtractor,
    ListParserExtractor,
)
from pump_parser.validation.physics import validate_pump_physics
from pump_parser.learning.recipe import Recipe
from pump_parser.learning.recipe_store import RecipeStore
from pump_parser.learning.recipe_matcher import RecipeMatcher
from pump_parser.learning.recipe_generator import generate_recipe
from pump_parser.learning.recipe_evaluator import RecipeEvaluator
from pump_parser.vision.api import VisionAPI
from pump_parser.vision.prompts import get_prompt
from pump_parser.vision.cross_validate import cross_validate_entries
from pump_parser.vision.ocr import ocr_page
from pump_parser.verification.self_heal import self_heal_entries
from pump_parser.verification.cross_page import cross_page_merge, enrich_from_series
from pump_parser.verification.confidence import calibrate_confidence
from pump_parser.verification.anomaly import detect_anomalies
from pump_parser.verification.auto_retry import retry_page, should_retry

log = logging.getLogger("pump_parser.orchestrator")

# Type → Extractor mapping (text-based extractors)
EXTRACTORS = {
    TableType.FLAT_TABLE: FlatTableExtractor(),
    TableType.QH_MATRIX: QHMatrixExtractor(),
    TableType.CURVE_TABLE: CurveTableExtractor(),
    TableType.TRANSPOSED: TransposedExtractor(),
    TableType.LIST_FORMAT: ListParserExtractor(),
}

# Extractor name → TableType
_NAME_TO_TYPE = {
    "flat_table": TableType.FLAT_TABLE,
    "qh_matrix": TableType.QH_MATRIX,
    "curve_table": TableType.CURVE_TABLE,
    "transposed": TableType.TRANSPOSED,
    "graph_reader": TableType.GRAPH,
    "list_parser": TableType.LIST_FORMAT,
}

# Page types that contain extractable data
DATA_PAGE_TYPES = {PageType.DATA_TABLE, PageType.MODEL_RANGE, PageType.DATA_GRAPH}


def parse_pdf(
    pdf_path: str,
    page_range: tuple[int, int] | None = None,
    min_confidence: float = 0.0,
    use_recipes: bool = True,
    recipe_store: RecipeStore | None = None,
    vision_api: VisionAPI | None = None,
) -> ParseResult:
    """Parse a single PDF catalog end-to-end.

    Pipeline:
    1. Load PDF, try recipe match (fast path)
    2. Fast path: recipe's extractor on all pages, skip classification
    3. Discovery path: classify → detect type → try extractors
    4. Vision AI fallback for graph pages and low-score pages
    5. Deduplicate across pages
    6. Validate physics
    7. Evaluate recipe / auto-generate new recipe
    8. Return ParseResult
    """
    t0 = time.time()
    source = Path(pdf_path).name

    # 1. Load
    doc = load_pdf(pdf_path)
    if doc._doc is None:
        return ParseResult(source=source, warnings=["Failed to open PDF"])

    start_page = page_range[0] if page_range else 0
    end_page = page_range[1] if page_range else doc.num_pages - 1
    end_page = min(end_page, doc.num_pages - 1)

    # Try recipe match
    recipe: Recipe | None = None
    store = recipe_store or RecipeStore()
    first_pages_text = ""

    if use_recipes:
        sample_pages = min(3, doc.num_pages)
        first_pages_text = " ".join(
            get_page_text(doc, p) for p in range(sample_pages)
        )
        matcher = RecipeMatcher()
        recipe = matcher.match(pdf_path, store.all(), first_pages_text)

    # Choose path
    if recipe:
        log.info("Fast path: recipe '%s' matched for %s", recipe.name, source)
        all_entries, page_reports, pages_skipped, extractor_counts = _fast_path(
            doc, recipe, start_page, end_page, source, pdf_path, vision_api,
        )
    else:
        log.info("Discovery path for %s", source)
        all_entries, page_reports, pages_skipped, extractor_counts = _discovery_path(
            doc, start_page, end_page, source, pdf_path, vision_api,
        )

    # Collect page texts for confidence grounding (before closing PDF)
    page_texts = {}
    for e in all_entries:
        if e.source_page not in page_texts:
            page_texts[e.source_page] = get_page_text(doc, e.source_page)

    close_pdf(doc)

    # Cross-page merge (combines data from multiple pages per model)
    all_entries = cross_page_merge(all_entries)

    # Filter garbage model names (text fragments, numbers, unit artifacts)
    all_entries = _filter_garbage_models(all_entries)

    # Enrich with series-level data (RPM, voltage)
    all_entries = enrich_from_series(all_entries)

    # Self-heal entries with physics violations
    all_entries, heal_count = self_heal_entries(all_entries)

    # Calibrate confidence from multiple signals
    all_entries = calibrate_confidence(all_entries, page_texts)

    # Detect anomalies (outliers, inconsistencies)
    all_entries = detect_anomalies(all_entries)

    # Validate
    validated, physics_pass = _validate(all_entries, min_confidence)

    # Build result
    avg_conf = sum(e.confidence for e in validated) / len(validated) if validated else 0.0
    physics_rate = physics_pass / len(all_entries) if all_entries else 0.0

    # Vision AI stats
    vision_cost = 0.0
    vision_pages = 0
    if vision_api:
        vision_cost = vision_api.budget.spent_usd
        vision_pages = vision_api.budget.calls

    report = ParseReport(
        per_page=page_reports,
        total_models=len(validated),
        avg_confidence=round(avg_conf, 3),
        physics_pass_rate=round(physics_rate, 3),
        extractor_breakdown=extractor_counts,
        self_heal_applied=heal_count > 0,
        vision_ai_pages=vision_pages,
        vision_ai_cost_usd=round(vision_cost, 4),
    )

    elapsed = time.time() - t0
    path_name = f"recipe:{recipe.name}" if recipe else "discovery"
    log.info(
        "%s: %d models, %.1fs, avg_conf=%.2f, physics=%.0f%% [%s]",
        source, len(validated), elapsed, avg_conf, physics_rate * 100, path_name,
    )

    result = ParseResult(
        entries=validated,
        source=source,
        recipe_used=recipe.recipe_id if recipe else None,
        pages_processed=end_page - start_page + 1 - pages_skipped,
        pages_skipped=pages_skipped,
        extraction_time_s=elapsed,
        avg_confidence=round(avg_conf, 3),
        warnings=[],
        report=report,
    )

    # Post-parse: evaluate recipe or auto-generate
    if use_recipes:
        _post_parse(recipe, result, store, pdf_path, doc.hash, first_pages_text)

    return result


def _fast_path(
    doc: PDFDocument,
    recipe: Recipe,
    start_page: int,
    end_page: int,
    source: str,
    pdf_path: str,
    vision_api: VisionAPI | None = None,
) -> tuple[list[PumpEntry], list[PageReport], int, dict[str, int]]:
    """Fast path: use recipe's extractor, skip classification/type detection."""
    all_entries: list[PumpEntry] = []
    page_reports: list[PageReport] = []
    pages_skipped = 0
    extractor_counts: dict[str, int] = {}

    # Get extractor from recipe
    ext_type = _NAME_TO_TYPE.get(recipe.extraction.extractor_type)
    primary_ext = EXTRACTORS.get(ext_type) if ext_type else None

    for page_num in range(start_page, end_page + 1):
        text = get_page_text(doc, page_num)

        # OCR fallback for scanned pages
        if len(text.strip()) < 30 and doc.is_scanned:
            image = get_page_image(doc, page_num)
            ocr_text = ocr_page(doc, page_num, page_image=image, vision_api=vision_api)
            if ocr_text:
                text = ocr_text

        # Quick skip: very short pages (covers, blank)
        if len(text.strip()) < 50:
            pages_skipped += 1
            continue

        tables = extract_tables(doc._doc[page_num], page_num, pdf_path=pdf_path)

        # Try primary extractor from recipe
        result = None
        if primary_ext:
            result = primary_ext.extract(text, tables, page_num, source)
            if not result.entries:
                result = None

        # Also try discovery — keep better result (more models)
        discovery_result = _run_discovery(text, tables, page_num, source)
        if discovery_result and discovery_result.entries:
            if result is None or len(discovery_result.entries) > len(result.entries):
                result = discovery_result

        # Vision AI fallback for pages with no result
        if (result is None or not result.entries) and vision_api:
            result = _vision_fallback(
                doc, page_num, text, tables, source, vision_api,
            )

        if result and result.entries:
            all_entries.extend(result.entries)
            ext_name = result.extractor_type
            extractor_counts[ext_name] = extractor_counts.get(ext_name, 0) + len(result.entries)
            page_reports.append(PageReport(
                page_num=page_num,
                page_type=PageType.DATA_TABLE,
                extractor_used=ext_name,
                models_found=len(result.entries),
                avg_confidence=sum(e.confidence for e in result.entries) / len(result.entries),
                warnings=result.warnings,
            ))
        else:
            pages_skipped += 1

    return all_entries, page_reports, pages_skipped, extractor_counts


def _discovery_path(
    doc: PDFDocument,
    start_page: int,
    end_page: int,
    source: str,
    pdf_path: str,
    vision_api: VisionAPI | None = None,
) -> tuple[list[PumpEntry], list[PageReport], int, dict[str, int]]:
    """Discovery path: classify → detect type → try extractors → Vision AI fallback."""
    all_entries: list[PumpEntry] = []
    page_reports: list[PageReport] = []
    pages_skipped = 0
    extractor_counts: dict[str, int] = {}

    for page_num in range(start_page, end_page + 1):
        text = get_page_text(doc, page_num)

        # OCR fallback for scanned pages
        if len(text.strip()) < 30 and doc.is_scanned:
            image = get_page_image(doc, page_num)
            ocr_text = ocr_page(doc, page_num, page_image=image, vision_api=vision_api)
            if ocr_text:
                text = ocr_text
                log.debug("OCR fallback on page %d: %d chars", page_num, len(text.strip()))

        tables = extract_tables(doc._doc[page_num], page_num, pdf_path=pdf_path)

        classified = classify_page(text, page_num, has_tables=bool(tables), num_tables=len(tables))
        page_type = classified.page_type

        # Vision AI classification fallback for uncertain pages
        if classified.confidence < 0.6 and vision_api and page_type not in DATA_PAGE_TYPES:
            ai_classified = classify_page_vision(
                get_page_image(doc, page_num), page_num, vision_api,
            )
            if ai_classified and ai_classified.page_type in DATA_PAGE_TYPES:
                page_type = ai_classified.page_type
                log.debug(
                    "AI reclassified page %d: %s → %s",
                    page_num, classified.page_type.value, page_type.value,
                )

        if page_type not in DATA_PAGE_TYPES:
            pages_skipped += 1
            continue

        result = None

        # Graph pages → Vision AI directly
        if page_type == PageType.DATA_GRAPH and vision_api:
            result = _extract_graph(doc, page_num, text, source, vision_api)

        # Table pages → detect type + extract
        if result is None or not result.entries:
            table_type, tt_conf, _ = detect_table_type_for_page(tables, text)
            result = _run_extraction(table_type, text, tables, page_num, source)

        # Auto-retry with alternative table strategies if score is low
        if should_retry(result):
            retry_result = retry_page(
                doc._doc[page_num], page_num, text, source,
                result, EXTRACTORS, pdf_path=pdf_path,
            )
            if retry_result and (result is None or not result.entries or
                                 (retry_result.entries and retry_result.score > (result.score if result else 0))):
                result = retry_result

        # Vision AI fallback for low-score or empty results
        if vision_api and (result is None or not result.entries or result.score < DISCOVERY_AI_FALLBACK_SCORE):
            ai_result = _vision_fallback(
                doc, page_num, text, tables, source, vision_api,
            )
            if ai_result and ai_result.entries:
                if result is None or len(ai_result.entries) > len(result.entries):
                    result = ai_result

        if result and result.entries:
            all_entries.extend(result.entries)
            ext_name = result.extractor_type
            extractor_counts[ext_name] = extractor_counts.get(ext_name, 0) + len(result.entries)
            page_reports.append(PageReport(
                page_num=page_num,
                page_type=page_type,
                extractor_used=ext_name,
                models_found=len(result.entries),
                avg_confidence=sum(e.confidence for e in result.entries) / len(result.entries),
                warnings=result.warnings,
            ))
        else:
            pages_skipped += 1

    return all_entries, page_reports, pages_skipped, extractor_counts


def _run_extraction(
    table_type: TableType,
    text: str,
    tables: list,
    page_num: int,
    source: str,
) -> ExtractionResult | None:
    """Run the matching extractor, or try all if type unknown."""
    if table_type != TableType.UNKNOWN and table_type in EXTRACTORS:
        ext = EXTRACTORS[table_type]
        result = ext.extract(text, tables, page_num, source)
        if result.entries:
            return result

    return _run_discovery(text, tables, page_num, source)


def _run_discovery(
    text: str,
    tables: list,
    page_num: int,
    source: str,
) -> ExtractionResult | None:
    """Try all text-based extractors, pick best result.

    Selection: highest score wins, but if another extractor finds >=50% more
    entries with a reasonable score (>=50), prefer it — more models is better.
    """
    best: ExtractionResult | None = None
    all_results: list[ExtractionResult] = []

    for ttype, ext in EXTRACTORS.items():
        try:
            result = ext.extract(text, tables, page_num, source)
        except Exception as e:
            log.debug("Extractor %s failed on page %d: %s", ext.type_name, page_num, e)
            continue

        if not result.entries:
            continue

        all_results.append(result)

        if best is None or result.score > best.score:
            best = result

    # Check if any extractor found significantly more entries
    if best and all_results:
        for result in all_results:
            if (result is not best
                    and result.score >= 50
                    and len(result.entries) >= len(best.entries) * 1.5):
                log.debug(
                    "Preferring %s (%d entries) over %s (%d entries) on page %d",
                    result.extractor_type, len(result.entries),
                    best.extractor_type, len(best.entries), page_num,
                )
                best = result

    return best


def _extract_graph(
    doc: PDFDocument,
    page_num: int,
    page_text: str,
    source: str,
    vision_api: VisionAPI,
) -> ExtractionResult | None:
    """Extract curves from a graph page via Vision AI."""
    image = get_page_image(doc, page_num)
    if not image:
        return None

    graph_ext = GraphReaderExtractor(vision_api=vision_api)
    return graph_ext.extract(
        page_text, [], page_num=page_num,
        source_file=source, page_image=image,
    )


def _vision_fallback(
    doc: PDFDocument,
    page_num: int,
    page_text: str,
    tables: list,
    source: str,
    vision_api: VisionAPI,
) -> ExtractionResult | None:
    """Vision AI fallback: send page image for flat/matrix extraction."""
    if not vision_api.budget.can_spend(vision_api.config.cost_per_page_primary):
        return None

    image = get_page_image(doc, page_num)
    if not image:
        return None

    # Detect table type visually
    prompt_tt = get_prompt("table_type")
    tt_result = vision_api.call(prompt_tt, image_bytes=image)

    if not tt_result or not isinstance(tt_result, dict):
        return None

    ai_table_type = tt_result.get("table_type", "flat_table")

    # Choose extraction prompt
    if ai_table_type == "qh_matrix":
        prompt = get_prompt("extract_matrix")
    elif ai_table_type in ("data_graph", "graph"):
        return _extract_graph(doc, page_num, page_text, source, vision_api)
    else:
        prompt = get_prompt("extract_flat")

    # Extract data
    data = vision_api.extract_data(image, prompt)
    if not data or not isinstance(data, dict):
        return None

    # Parse response into PumpEntry list
    entries = _parse_vision_response(data, ai_table_type, page_num, source, page_text)

    if not entries:
        return None

    return ExtractionResult(
        entries=entries,
        score=0.0,
        extractor_type=f"vision_{ai_table_type}",
        page_num=page_num,
    )


def _parse_vision_response(
    data: dict,
    table_type: str,
    page_num: int,
    source: str,
    page_text: str,
) -> list[PumpEntry]:
    """Convert Vision AI extraction response to PumpEntry list."""
    entries = []

    if table_type == "qh_matrix":
        q_values = data.get("q_values", [])
        for pump in data.get("pumps", []):
            model = str(pump.get("model", "")).strip()
            if not model:
                continue
            h_values = pump.get("h_values", [])
            q_pts = [float(v) for v in q_values if v is not None]
            h_pts = [float(v) for v in h_values if v is not None]
            min_len = min(len(q_pts), len(h_pts))

            entry = PumpEntry(
                model=model,
                q_nom=float(pump.get("q_nom", 0) or 0),
                h_nom=float(pump.get("h_nom", 0) or 0),
                power_kw=float(pump.get("power_kw", 0) or 0),
                q_points=q_pts[:min_len],
                h_points=h_pts[:min_len],
                source_file=source,
                source_page=page_num,
                data_source="vision_matrix",
                confidence=0.65,
            )
            entries.append(entry)
    else:
        for pump in data.get("pumps", []):
            model = str(pump.get("model", "")).strip()
            if not model:
                continue
            entry = PumpEntry(
                model=model,
                article=str(pump.get("article", "") or ""),
                q_nom=float(pump.get("q_nom", 0) or 0),
                h_nom=float(pump.get("h_nom", 0) or 0),
                power_kw=float(pump.get("power_kw", 0) or 0),
                rpm=int(pump.get("rpm", 0) or 0),
                stages=int(pump.get("stages", 0) or 0),
                dn_suction=int(pump.get("dn", 0) or 0),
                source_file=source,
                source_page=page_num,
                data_source="vision_flat",
                confidence=0.65,
            )
            entries.append(entry)

    # Cross-validate against page text
    if entries:
        entries = cross_validate_entries(entries, page_text)

    return entries


def _validate(
    entries: list[PumpEntry],
    min_confidence: float,
) -> tuple[list[PumpEntry], int]:
    """Validate physics, filter by confidence. Returns (validated, physics_pass_count)."""
    validated = []
    physics_pass = 0
    for entry in entries:
        ok, warnings, _ = validate_pump_physics(
            entry.q_nom, entry.h_nom, entry.power_kw,
            entry.q_points, entry.h_points,
            entry.model, entry.series,
        )
        if warnings:
            entry.warnings.extend(warnings)
        if ok:
            physics_pass += 1
        if entry.confidence >= min_confidence:
            validated.append(entry)
    return validated, physics_pass


def _post_parse(
    recipe: Recipe | None,
    result: ParseResult,
    store: RecipeStore,
    pdf_path: str,
    pdf_hash: str,
    first_pages_text: str,
) -> None:
    """Post-parse: evaluate existing recipe or generate new one."""
    if recipe:
        evaluator = RecipeEvaluator(store)
        action = evaluator.evaluate(recipe, result)
        log.debug("Recipe '%s' evaluation: %s", recipe.name, action)
    else:
        new_recipe = generate_recipe(pdf_path, result, pdf_hash, first_pages_text)
        if new_recipe:
            matcher = RecipeMatcher()
            existing = matcher.match(pdf_path, store.all(), first_pages_text)
            if not existing:
                store.save(new_recipe)
                log.info("Auto-generated recipe '%s' from %s", new_recipe.name, result.source)


import re as _re

# Garbage model name patterns
_RE_VALID_MODEL = _re.compile(
    r'^[A-ZА-ЯЁa-zа-яё]{2,}[\s\-_]?\d',  # must start with 2+ letters then digit
)
_GARBAGE_WORDS = {
    "конструкция", "aisi", "astm", "таблица", "содержание",
    "чугун", "бронза", "сталь", "фирменная", "табличка",
    "описание", "description", "applications", "применение",
    "материал", "material",
}
_GARBAGE_PREFIXES = {
    "вт ", "npsh", "dn", "in ", "ll ", "dh ", "ye", "сч", "вч",
}


def _is_garbage_model(name: str) -> bool:
    """Check if model name is garbage (text fragment, number, unit artifact)."""
    s = name.strip()
    if len(s) < 3:
        return True
    # Pure number
    try:
        float(s.replace(",", "."))
        return True
    except ValueError:
        pass
    # No digits at all
    if not any(c.isdigit() for c in s):
        return True
    # Must start with letters then digit (BM 1-2A, FST 32-125/7, etc.)
    if not _RE_VALID_MODEL.match(s):
        return True
    # Contains garbage words
    sl = s.lower()
    for w in _GARBAGE_WORDS:
        if w in sl:
            return True
    # Starts with garbage prefix
    for p in _GARBAGE_PREFIXES:
        if sl.startswith(p):
            return True
    # DN+digits pattern (flange sizes, not models)
    if _re.match(r'^DN\s?\d', s, _re.IGNORECASE):
        return True
    # Multiple model-like tokens = not a single model (e.g. "YE3-315L2-2 YE3-355M-2")
    tokens = s.split()
    if len(tokens) >= 3 and sum(1 for t in tokens if _re.match(r'[A-ZА-ЯЁ]{2,}\d', t)) >= 2:
        return True
    # Contains ". " followed by uppercase — text fragment ("4.3. BM")
    if _re.search(r'\d\.\s+[A-ZА-ЯЁ]', s):
        return True
    # Too long (>40 chars) — likely a text fragment
    if len(s) > 40:
        return True
    return False


def _filter_garbage_models(entries: list[PumpEntry]) -> list[PumpEntry]:
    """Remove entries with garbage model names."""
    filtered = []
    removed = 0
    for e in entries:
        if _is_garbage_model(e.model):
            removed += 1
            log.debug("Filtered garbage model: '%s' (page %d)", e.model, e.source_page)
        else:
            filtered.append(e)
    if removed:
        log.info("Filtered %d garbage model names", removed)
    return filtered
