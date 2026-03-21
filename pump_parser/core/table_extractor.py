"""Dual-backend table extraction: PyMuPDF + pdfplumber."""

import logging
from typing import Optional

import fitz

from pump_parser.models import ExtractedTable

log = logging.getLogger("pump_parser.table_extractor")

# Optional pdfplumber
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False


def _extract_fitz(page: fitz.Page, page_num: int) -> list[ExtractedTable]:
    """Extract tables using PyMuPDF find_tables()."""
    try:
        tabs = page.find_tables()
        if not tabs or not tabs.tables:
            return []
    except Exception as e:
        log.debug("fitz find_tables failed page %d: %s", page_num, e)
        return []

    results = []
    for tab in tabs.tables:
        try:
            data = tab.extract()
        except Exception:
            continue
        if not data or len(data) < 2:
            continue

        # First row = headers, rest = data rows
        headers = [str(c).strip() if c else "" for c in data[0]]
        rows = []
        for row in data[1:]:
            rows.append([str(c).strip() if c else "" for c in row])

        results.append(ExtractedTable(
            headers=headers,
            rows=rows,
            bbox=tab.bbox if hasattr(tab, "bbox") else (),
            page_num=page_num,
            strategy_used="fitz",
        ))
    return results


def _extract_pdfplumber(pdf_path: str, page_num: int) -> list[ExtractedTable]:
    """Extract tables using pdfplumber."""
    if not HAS_PDFPLUMBER:
        return []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_num >= len(pdf.pages):
                return []
            page = pdf.pages[page_num]
            tables = page.extract_tables()
            if not tables:
                return []
    except Exception as e:
        log.debug("pdfplumber failed page %d: %s", page_num, e)
        return []

    results = []
    for table in tables:
        if not table or len(table) < 2:
            continue

        headers = [str(c).strip() if c else "" for c in table[0]]
        rows = []
        for row in table[1:]:
            rows.append([str(c).strip() if c else "" for c in row])

        results.append(ExtractedTable(
            headers=headers,
            rows=rows,
            bbox=(),
            page_num=page_num,
            strategy_used="pdfplumber",
        ))
    return results


def _extract_lines(page: fitz.Page, page_num: int) -> list[ExtractedTable]:
    """Extract table-like structures by analyzing text line alignment.

    Groups text spans by Y-coordinate into rows, then detects column boundaries
    by clustering X-coordinates across rows.
    """
    blocks = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]

    # Collect all spans with position
    spans = []
    for block in blocks:
        if block.get("type") != 0:  # text blocks only
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                text = span["text"].strip()
                if text:
                    spans.append({
                        "text": text,
                        "x0": round(span["bbox"][0], 1),
                        "y0": round(span["bbox"][1], 1),
                        "x1": round(span["bbox"][2], 1),
                        "y1": round(span["bbox"][3], 1),
                    })

    if len(spans) < 6:
        return []

    # Group spans into rows by Y proximity (tolerance 3pt)
    spans.sort(key=lambda s: (s["y0"], s["x0"]))
    rows_raw: list[list[dict]] = []
    current_row: list[dict] = [spans[0]]
    for s in spans[1:]:
        if abs(s["y0"] - current_row[0]["y0"]) <= 3:
            current_row.append(s)
        else:
            if current_row:
                rows_raw.append(sorted(current_row, key=lambda x: x["x0"]))
            current_row = [s]
    if current_row:
        rows_raw.append(sorted(current_row, key=lambda x: x["x0"]))

    # Need at least 3 rows with 3+ cells to be a table
    table_rows = [r for r in rows_raw if len(r) >= 3]
    if len(table_rows) < 3:
        return []

    # Build string rows
    headers = [s["text"] for s in table_rows[0]]
    rows = []
    for r in table_rows[1:]:
        rows.append([s["text"] for s in r])

    return [ExtractedTable(
        headers=headers,
        rows=rows,
        bbox=(),
        page_num=page_num,
        strategy_used="lines",
    )]


def _extract_text_columns(page: fitz.Page, page_num: int) -> list[ExtractedTable]:
    """Extract table by splitting raw text into columns by whitespace alignment."""
    text = page.get_text("text")
    lines = [l for l in text.split("\n") if l.strip()]
    if len(lines) < 3:
        return []

    # Split each line by 2+ spaces
    split_lines = []
    for line in lines:
        import re
        parts = re.split(r"\s{2,}", line.strip())
        if len(parts) >= 3:
            split_lines.append(parts)

    if len(split_lines) < 3:
        return []

    headers = split_lines[0]
    rows = split_lines[1:]

    return [ExtractedTable(
        headers=headers,
        rows=rows,
        bbox=(),
        page_num=page_num,
        strategy_used="text",
    )]


def extract_tables(
    page: fitz.Page,
    page_num: int,
    strategy: str = "auto",
    pdf_path: Optional[str] = None,
) -> list[ExtractedTable]:
    """Extract tables from a page using the specified strategy.

    Args:
        page: PyMuPDF page object
        page_num: page number (0-indexed)
        strategy: "auto" | "fitz" | "pdfplumber" | "lines" | "text"
        pdf_path: path to PDF file (needed for pdfplumber strategy)

    Returns:
        List of ExtractedTable objects
    """
    if strategy == "fitz":
        return _extract_fitz(page, page_num)

    if strategy == "pdfplumber":
        if pdf_path is None:
            log.warning("pdfplumber strategy requires pdf_path")
            return []
        return _extract_pdfplumber(pdf_path, page_num)

    if strategy == "lines":
        return _extract_lines(page, page_num)

    if strategy == "text":
        return _extract_text_columns(page, page_num)

    # "auto": try fitz → lines → pdfplumber
    tables = _extract_fitz(page, page_num)
    if tables and any(len(t.rows) >= 2 for t in tables):
        return tables

    tables = _extract_lines(page, page_num)
    if tables and any(len(t.rows) >= 2 for t in tables):
        return tables

    if pdf_path and HAS_PDFPLUMBER:
        tables = _extract_pdfplumber(pdf_path, page_num)
        if tables:
            return tables

    return []
