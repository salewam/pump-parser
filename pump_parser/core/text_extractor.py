"""Raw text extraction with position info, line parsing, block detection."""

import logging

import fitz

from pump_parser.models import TextLine, TextBlock

log = logging.getLogger("pump_parser.text_extractor")


def extract_text(page: fitz.Page) -> str:
    """Extract plain text from page."""
    return page.get_text("text")


def extract_lines(page: fitz.Page) -> list[TextLine]:
    """Extract text lines with position and font info."""
    blocks = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]
    result: list[TextLine] = []

    for block in blocks:
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            # Merge spans in same line
            texts = []
            max_size = 0.0
            has_bold = False
            x0 = 999999.0
            y = 0.0

            for span in line.get("spans", []):
                t = span["text"]
                if not t.strip():
                    continue
                texts.append(t)
                size = span.get("size", 0)
                if size > max_size:
                    max_size = size
                flags = span.get("flags", 0)
                if flags & 2 ** 4:  # bold flag
                    has_bold = True
                sx0 = span["bbox"][0]
                if sx0 < x0:
                    x0 = sx0
                y = span["bbox"][1]

            full_text = " ".join(texts).strip()
            if full_text:
                result.append(TextLine(
                    text=full_text,
                    y=round(y, 1),
                    x0=round(x0, 1) if x0 < 999999 else 0.0,
                    font_size=round(max_size, 1),
                    is_bold=has_bold,
                ))

    return result


def extract_blocks(page: fitz.Page) -> list[TextBlock]:
    """Extract text blocks (groups of lines forming logical units)."""
    lines = extract_lines(page)
    if not lines:
        return []

    # Group lines into blocks by vertical proximity
    # Lines within 2x font_size of each other belong to same block
    blocks: list[TextBlock] = []
    current_lines: list[TextLine] = [lines[0]]

    for line in lines[1:]:
        prev = current_lines[-1]
        gap = line.y - prev.y
        threshold = max(prev.font_size, line.font_size) * 2.0

        if gap <= threshold:
            current_lines.append(line)
        else:
            blocks.append(_make_block(current_lines))
            current_lines = [line]

    if current_lines:
        blocks.append(_make_block(current_lines))

    return blocks


def _make_block(lines: list[TextLine]) -> TextBlock:
    """Create a TextBlock from a group of lines."""
    if not lines:
        return TextBlock()

    x0 = min(l.x0 for l in lines)
    y0 = lines[0].y
    y1 = lines[-1].y + lines[-1].font_size
    # Estimate x1 from longest line text length
    x1 = max(l.x0 + len(l.text) * l.font_size * 0.5 for l in lines)

    # Detect table-like: multiple lines with similar column count
    # when split by 2+ spaces
    import re
    col_counts = []
    for l in lines:
        parts = re.split(r"\s{2,}", l.text.strip())
        col_counts.append(len(parts))

    is_table = (
        len(lines) >= 3
        and len(set(col_counts)) <= 2  # consistent column count
        and max(col_counts) >= 3       # at least 3 columns
    )

    return TextBlock(
        lines=lines,
        bbox=(round(x0, 1), round(y0, 1), round(x1, 1), round(y1, 1)),
        is_table_like=is_table,
    )
