"""OCR fallback for scanned PDFs.

Two strategies:
1. PyMuPDF built-in OCR (Tesseract via fitz) — fast, local
2. Vision AI OCR — send page image to Gemini for text extraction

Falls back gracefully: PyMuPDF OCR → Vision AI → empty string.
"""

import logging

from pump_parser.models import PDFDocument

log = logging.getLogger("pump_parser.vision.ocr")

# Prompt for Vision AI OCR
PROMPT_OCR = """Extract ALL text from this pump catalog page image.

Return the text exactly as it appears, preserving:
- Table structure (use tab characters between columns)
- Line breaks between rows
- Numbers exactly as printed (decimal separators, units)
- Model names, series names, specifications

Do NOT interpret or summarize — just transcribe all visible text.
Output raw text only, no JSON, no markdown."""


def ocr_page_pymupdf(doc: PDFDocument, page_num: int) -> str:
    """OCR a page using PyMuPDF's built-in Tesseract integration."""
    if doc._doc is None or page_num >= doc.num_pages:
        return ""

    page = doc._doc[page_num]

    try:
        # PyMuPDF OCR: renders page and runs Tesseract
        tp = page.get_textpage_ocr(language="rus+eng", dpi=300)
        text = page.get_text("text", textpage=tp)
        if text and len(text.strip()) > 20:
            log.debug("PyMuPDF OCR page %d: %d chars", page_num, len(text.strip()))
            return text
    except Exception as e:
        log.debug("PyMuPDF OCR failed on page %d: %s", page_num, e)

    return ""


def ocr_page_vision(page_image: bytes, vision_api) -> str:
    """OCR a page using Vision AI (Gemini)."""
    if not page_image or not vision_api:
        return ""

    try:
        result = vision_api.call(
            PROMPT_OCR,
            image_bytes=page_image,
            parse_json=False,
        )
        if result and isinstance(result, str) and len(result.strip()) > 20:
            log.debug("Vision OCR: %d chars", len(result.strip()))
            return result
    except Exception as e:
        log.debug("Vision OCR failed: %s", e)

    return ""


def ocr_page(
    doc: PDFDocument,
    page_num: int,
    page_image: bytes | None = None,
    vision_api=None,
) -> str:
    """OCR a page with fallback chain: PyMuPDF → Vision AI.

    Args:
        doc: loaded PDF document
        page_num: page number
        page_image: pre-rendered PNG bytes (optional, for Vision AI)
        vision_api: VisionAPI instance (optional)

    Returns:
        Extracted text or empty string.
    """
    # Strategy 1: PyMuPDF OCR (local Tesseract)
    text = ocr_page_pymupdf(doc, page_num)
    if text:
        return text

    # Strategy 2: Vision AI OCR
    if vision_api and page_image:
        text = ocr_page_vision(page_image, vision_api)
        if text:
            return text

    return ""
