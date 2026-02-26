#!/usr/bin/env python3
"""
Vision PDF Parser for pump catalogs.
Uses GPT-4o-mini vision to extract structured pump data from PDF pages.

Usage:
    python3 vision_pdf_parser.py <catalog.pdf> [--output result.json] [--pages 1-10]
"""

import argparse
import asyncio
import base64
import io
import json
import os
import sys
import time
from pathlib import Path

import fitz  # PyMuPDF

try:
    from openai import AsyncOpenAI
except ImportError:
    print("pip install openai PyMuPDF Pillow")
    sys.exit(1)

# --- Config ---
MODEL = "gpt-4o-mini"
MAX_CONCURRENT = 5  # parallel API calls
DPI = 200  # render quality (200 is good balance)
MAX_TOKENS = 4096

SYSTEM_PROMPT = """You are a pump catalog data extractor. You analyze images of pump catalog pages and extract structured pump data.

For each pump model found on the page, extract:
- model: full model name (e.g. "CDM1-10", "CMI 25-160", "NBS 40-200/210")
- series: series name (e.g. "CDM", "CMI", "NBS")
- q_nom: nominal flow rate in m³/h (convert from l/s, l/min, GPM if needed)
- h_nom: nominal head in meters (convert from bar, kPa, PSI if needed)
- power_kw: motor power in kW (convert from HP if needed)

Rules:
- Extract ALL pump models visible on the page
- If a page has a performance table with multiple models, extract each row
- If a page has Q-H curves, extract the nominal point (design point or BEP)
- If power is given as HP, convert: 1 HP = 0.746 kW
- If flow is in l/s, convert: multiply by 3.6 to get m³/h
- If flow is in l/min, convert: divide by 16.67 to get m³/h
- If head is in bar, convert: multiply by 10.2 to get meters
- Skip pages with no pump data (cover pages, text-only, drawings without specs)
- Return empty array [] if no pump data found on the page

Respond ONLY with valid JSON array. No markdown, no explanation."""

USER_PROMPT = """Extract all pump models with their specifications from this catalog page.
Return JSON array: [{"model": "...", "series": "...", "q_nom": 0.0, "h_nom": 0.0, "power_kw": 0.0}]
Return [] if no pump data on this page."""


def pdf_page_to_base64(doc: fitz.Document, page_num: int, dpi: int = DPI) -> str:
    """Render PDF page to base64 PNG."""
    page = doc[page_num]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat)
    img_bytes = pix.tobytes("png")
    return base64.b64encode(img_bytes).decode("utf-8")


async def parse_page(
    client: AsyncOpenAI,
    img_b64: str,
    page_num: int,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Send one page image to GPT-4o-mini vision and parse response."""
    async with semaphore:
        try:
            resp = await client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": USER_PROMPT},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{img_b64}",
                                    "detail": "high",
                                },
                            },
                        ],
                    },
                ],
                max_tokens=MAX_TOKENS,
                temperature=0.0,
            )
            raw = resp.choices[0].message.content.strip()
            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                if raw.endswith("```"):
                    raw = raw[:-3]
                raw = raw.strip()

            pumps = json.loads(raw)
            if not isinstance(pumps, list):
                pumps = [pumps]

            # Add page number to each entry
            for p in pumps:
                p["page"] = page_num + 1  # 1-indexed

            tokens_in = resp.usage.prompt_tokens if resp.usage else 0
            tokens_out = resp.usage.completion_tokens if resp.usage else 0

            return {
                "page": page_num + 1,
                "pumps": pumps,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "error": None,
            }
        except json.JSONDecodeError as e:
            return {
                "page": page_num + 1,
                "pumps": [],
                "tokens_in": 0,
                "tokens_out": 0,
                "error": f"JSON parse error: {e}. Raw: {raw[:200]}",
            }
        except Exception as e:
            return {
                "page": page_num + 1,
                "pumps": [],
                "tokens_in": 0,
                "tokens_out": 0,
                "error": str(e),
            }


def parse_page_range(page_range: str, total_pages: int) -> list[int]:
    """Parse page range string like '1-10,15,20-25' into list of 0-indexed page numbers."""
    if not page_range:
        return list(range(total_pages))

    pages = set()
    for part in page_range.split(","):
        part = part.strip()
        if "-" in part:
            start, end = part.split("-", 1)
            start = max(1, int(start))
            end = min(total_pages, int(end))
            pages.update(range(start - 1, end))
        else:
            p = int(part) - 1
            if 0 <= p < total_pages:
                pages.add(p)
    return sorted(pages)


async def run_parser(pdf_path: str, output_path: str | None, page_range: str | None):
    """Main parser pipeline."""
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        env_file = Path(__file__).parent / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("OPENAI_API_KEY="):
                    api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if not api_key:
        print("ERROR: Set OPENAI_API_KEY env var or create .env file")
        sys.exit(1)

    client = AsyncOpenAI(api_key=api_key)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    # Open PDF
    pdf_path = str(Path(pdf_path).resolve())
    doc = fitz.open(pdf_path)
    total_pages = doc.page_count
    pages = parse_page_range(page_range, total_pages)

    print(f"PDF: {Path(pdf_path).name}")
    print(f"Pages: {len(pages)} of {total_pages}")
    print(f"Model: {MODEL}")
    print(f"Concurrency: {MAX_CONCURRENT}")
    print()

    # Render all pages to images
    print("Rendering pages...", end=" ", flush=True)
    images = {}
    for i, page_num in enumerate(pages):
        images[page_num] = pdf_page_to_base64(doc, page_num)
    print(f"done ({len(images)} images)")

    # Send all pages to API in parallel
    t0 = time.time()
    print(f"Parsing with {MODEL}...")

    tasks = [
        parse_page(client, images[page_num], page_num, semaphore)
        for page_num in pages
    ]
    results = await asyncio.gather(*tasks)

    elapsed = time.time() - t0

    # Collect results
    all_pumps = []
    total_tokens_in = 0
    total_tokens_out = 0
    errors = []

    for r in sorted(results, key=lambda x: x["page"]):
        total_tokens_in += r["tokens_in"]
        total_tokens_out += r["tokens_out"]
        if r["error"]:
            errors.append(f"  Page {r['page']}: {r['error']}")
        if r["pumps"]:
            all_pumps.extend(r["pumps"])
            print(f"  Page {r['page']}: {len(r['pumps'])} pumps found")

    # Deduplicate by model name (keep first occurrence)
    seen = set()
    unique_pumps = []
    for p in all_pumps:
        key = p.get("model", "")
        if key and key not in seen:
            seen.add(key)
            unique_pumps.append(p)
        elif not key:
            unique_pumps.append(p)

    # Cost calculation
    cost_in = total_tokens_in / 1_000_000 * 0.15
    cost_out = total_tokens_out / 1_000_000 * 0.60
    total_cost = cost_in + cost_out

    # Build output
    output = {
        "meta": {
            "source_file": Path(pdf_path).name,
            "total_pages": total_pages,
            "pages_parsed": len(pages),
            "model": MODEL,
            "parse_time_seconds": round(elapsed, 1),
            "total_pumps_found": len(unique_pumps),
            "duplicates_removed": len(all_pumps) - len(unique_pumps),
            "tokens": {
                "input": total_tokens_in,
                "output": total_tokens_out,
                "cost_usd": round(total_cost, 4),
            },
            "errors": len(errors),
        },
        "pumps": unique_pumps,
    }

    # Print summary
    print()
    print(f"=== Results ===")
    print(f"Pumps found: {len(unique_pumps)} (deduped from {len(all_pumps)})")
    print(f"Time: {elapsed:.1f}s")
    print(f"Tokens: {total_tokens_in:,} in / {total_tokens_out:,} out")
    print(f"Cost: ${total_cost:.4f}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for e in errors:
            print(e)

    # Save output
    if not output_path:
        stem = Path(pdf_path).stem
        output_path = str(Path(pdf_path).parent / f"{stem}_parsed.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nSaved: {output_path}")

    return output


def main():
    parser = argparse.ArgumentParser(description="Vision PDF Parser for pump catalogs")
    parser.add_argument("pdf", help="Path to PDF catalog")
    parser.add_argument("-o", "--output", help="Output JSON path")
    parser.add_argument("-p", "--pages", help="Page range (e.g. '1-10,15,20-25')")
    parser.add_argument("-c", "--concurrency", type=int, default=5, help="Max parallel API calls")
    parser.add_argument("--dpi", type=int, default=200, help="Render DPI (default 200)")
    args = parser.parse_args()

    global MAX_CONCURRENT, DPI
    MAX_CONCURRENT = args.concurrency
    DPI = args.dpi

    asyncio.run(run_parser(args.pdf, args.output, args.pages))


if __name__ == "__main__":
    main()
