"""Auto-retry pipeline — re-extract pages with low scores using alternative strategies.

Retry strategies:
1. Try all table extraction backends (fitz → pdfplumber → lines → text)
2. Try all extractors regardless of detected type
3. Score and compare, keep best result
"""

import logging

from pump_parser.models import PumpEntry, ExtractedTable, ExtractionResult
from pump_parser.extractors.base import BaseExtractor

log = logging.getLogger("pump_parser.verification.auto_retry")

# All table strategies to try
TABLE_STRATEGIES = ["fitz", "pdfplumber", "lines", "text"]


def retry_page(
    fitz_page,
    page_num: int,
    page_text: str,
    source: str,
    current_result: ExtractionResult | None,
    extractors: dict,
    pdf_path: str = "",
    min_score: float = 50.0,
) -> ExtractionResult | None:
    """Retry extraction on a page with alternative table strategies.

    Only retries if current result is None or score < min_score.

    Args:
        fitz_page: PyMuPDF page object
        page_num: page number
        page_text: text from page
        source: source filename
        current_result: result from first attempt
        extractors: dict of TableType → BaseExtractor
        pdf_path: path to PDF (for pdfplumber)
        min_score: minimum score threshold to accept without retry

    Returns:
        Best ExtractionResult, or current_result if no improvement.
    """
    # Check if retry needed
    if current_result and current_result.entries and current_result.score >= min_score:
        return current_result

    from pump_parser.core.table_extractor import extract_tables

    best = current_result
    best_score = current_result.score if current_result else 0.0

    for strategy in TABLE_STRATEGIES:
        # Skip if same as default (auto picks fitz first)
        try:
            tables = extract_tables(fitz_page, page_num, strategy=strategy, pdf_path=pdf_path)
        except Exception as e:
            log.debug("Retry strategy %s failed page %d: %s", strategy, page_num, e)
            continue

        if not tables:
            continue

        # Try all extractors with these tables
        for ttype, ext in extractors.items():
            try:
                result = ext.extract(page_text, tables, page_num, source)
            except Exception:
                continue

            if not result.entries:
                continue

            # Score this result
            score = ext.score(result, page_text)
            result.score = score

            if score > best_score:
                best = result
                best_score = score
                log.debug(
                    "Retry improved page %d: %s + %s → score %.1f (%d models)",
                    page_num, strategy, ext.type_name, score, len(result.entries),
                )

                if score >= 90:
                    return best  # good enough

    return best


def should_retry(result: ExtractionResult | None, min_score: float = 50.0) -> bool:
    """Check if a page result warrants a retry."""
    if result is None:
        return True
    if not result.entries:
        return True
    if result.score < min_score:
        return True
    return False
