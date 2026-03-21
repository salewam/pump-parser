"""PDF loading, metadata extraction, page access."""

import hashlib
import logging
from pathlib import Path

import fitz  # PyMuPDF

from pump_parser.models import PDFDocument

log = logging.getLogger("pump_parser.ingestion")


def compute_hash(path: str) -> str:
    """SHA256 hash of file for dedup."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_pdf(path: str) -> PDFDocument:
    """Load PDF and extract metadata. Returns PDFDocument with _doc reference."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"PDF not found: {path}")

    try:
        doc = fitz.open(str(p))
    except Exception as e:
        log.error("Failed to open PDF %s: %s", path, e)
        return PDFDocument(path=str(p), warnings=[f"Failed to open: {e}"])

    meta = doc.metadata or {}
    file_size_mb = p.stat().st_size / (1024 * 1024)

    # Detect scanned: if <10% of pages have extractable text (>50 chars)
    text_pages = 0
    for i in range(min(doc.page_count, 20)):  # sample first 20 pages
        text = doc[i].get_text("text")
        if len(text.strip()) > 50:
            text_pages += 1
    sampled = min(doc.page_count, 20)
    is_scanned = (text_pages / sampled) < 0.10 if sampled > 0 else False

    pdf_doc = PDFDocument(
        path=str(p),
        hash=compute_hash(str(p)),
        num_pages=doc.page_count,
        producer=meta.get("producer", ""),
        creator=meta.get("creator", ""),
        title=meta.get("title", ""),
        file_size_mb=round(file_size_mb, 2),
        is_scanned=is_scanned,
        _doc=doc,
    )
    log.info("Loaded %s: %d pages, %.1f MB, scanned=%s", p.name, doc.page_count, file_size_mb, is_scanned)
    return pdf_doc


def get_page_text(doc: PDFDocument, page_num: int) -> str:
    """Extract text from a single page."""
    if doc._doc is None or page_num >= doc.num_pages:
        return ""
    return doc._doc[page_num].get_text("text")


def get_page_image(doc: PDFDocument, page_num: int, dpi: int = 150) -> bytes:
    """Render page as PNG image bytes (for Vision AI)."""
    if doc._doc is None or page_num >= doc.num_pages:
        return b""
    page = doc._doc[page_num]
    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    return pix.tobytes("png")


def get_page_tables(doc: PDFDocument, page_num: int) -> list:
    """Extract tables from page using PyMuPDF's find_tables()."""
    if doc._doc is None or page_num >= doc.num_pages:
        return []
    page = doc._doc[page_num]
    try:
        tabs = page.find_tables()
        return tabs.tables if tabs else []
    except Exception as e:
        log.debug("find_tables failed on page %d: %s", page_num, e)
        return []


def close_pdf(doc: PDFDocument) -> None:
    """Close the PDF document."""
    if doc._doc is not None:
        doc._doc.close()
        doc._doc = None
