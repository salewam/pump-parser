"""
Stage: Vision API — extract pump data from performance curve charts.
Uses DeepSeek VL API to read Q-H curves from PDF page images.
Fallback for catalogs where Docling finds 0 performance tables (data in charts only).
"""
import os
import re
import json
import time
import base64
import logging
import requests
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
# Gemini Flash — cheap, fast, excellent vision
VISION_MODEL = "google/gemini-2.5-flash"
RENDER_DPI = 200
MAX_PAGES = 30
PAGE_TIMEOUT = 60

EXTRACT_PROMPT = """Analyze this pump catalog page image. Extract ALL pump models with their specifications.

For each pump model found, extract:
- model: full model name (e.g. "SET 150-65-200/7.5", "NOVA 600 M-A")
- q: nominal flow rate in m³/h (convert from l/min if needed: divide by 60)
- h: nominal head in meters
- kw: motor power in kW

Return ONLY a JSON array, no other text:
[{"model": "...", "q": 0.0, "h": 0.0, "kw": 0.0}, ...]

If no pump performance data on this page, return: []
Important: extract REAL values from the image. Do NOT interpolate or guess."""


class VisionAPIStage:
    """Extract pump specs from chart images via DeepSeek Vision API."""

    def __init__(self, api_key=None):
        self._api_key = api_key or OPENROUTER_API_KEY
        if not self._api_key:
            # Try systemd env
            self._api_key = os.environ.get("OPENROUTER_API_KEY", "")

    def extract_from_pdf(self, pdf_path, skip_pages=None, existing_models=None):
        """Extract pump models from PDF pages with charts.

        Args:
            pdf_path: path to PDF file
            skip_pages: set of page numbers to skip (already parsed by Docling)
            existing_models: list of already-found models (for dedup)

        Returns:
            list of dicts with model/q/h/kw
        """
        if not self._api_key:
            logger.error("No DeepSeek API key")
            return []

        doc = fitz.open(pdf_path)
        total_pages = doc.page_count
        skip_pages = skip_pages or set()
        existing_keys = set()
        if existing_models:
            for m in existing_models:
                key = re.sub(r'[\s()]+', '', str(m.get("model", m.model if hasattr(m, "model") else "")).upper())
                existing_keys.add(key)

        all_results = []
        pages_processed = 0

        for pg in range(total_pages):
            if pg in skip_pages:
                continue
            if pages_processed >= MAX_PAGES:
                break

            # Check if page likely has performance data (charts with curves)
            text = doc[pg].get_text()
            has_pump_hints = any(kw in text.lower() for kw in [
                "q", "h", "kw", "hp", "m³/h", "l/min", "head", "flow",
                "напор", "подача", "мощность", "performance", "curve",
            ])
            # Also check pages with very little text (scanned charts)
            is_mostly_image = len(text.strip()) < 100

            if not has_pump_hints and not is_mostly_image:
                continue

            # Render page to PNG
            try:
                pix = doc[pg].get_pixmap(dpi=RENDER_DPI)
                img_bytes = pix.tobytes("png")
                img_b64 = base64.b64encode(img_bytes).decode("utf-8")
            except Exception as e:
                logger.warning("Failed to render page %d: %s", pg, e)
                continue

            # Send to DeepSeek API
            models = self._call_vision_api(img_b64, pg)
            if models:
                # Dedup against existing
                for m in models:
                    key = re.sub(r'[\s()]+', '', m.get("model", "").upper())
                    if key and key not in existing_keys:
                        existing_keys.add(key)
                        m["page"] = pg + 1
                        m["source"] = "vision_api"
                        all_results.append(m)

                logger.info("Page %d: %d new models from vision API", pg + 1, len(models))
                pages_processed += 1

            # Rate limit: 1 request per second
            time.sleep(1)

        doc.close()
        logger.info("Vision API: %d total models from %d pages", len(all_results), pages_processed)
        return all_results

    def _call_vision_api(self, img_b64, page_num, retries=2):
        """Call DeepSeek API with image and extract pump data."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://500ideas.ru",
            "X-Title": "Pump Parser",
        }

        payload = {
            "model": VISION_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{img_b64}"
                            }
                        },
                        {
                            "type": "text",
                            "text": EXTRACT_PROMPT
                        }
                    ]
                }
            ],
            "max_tokens": 4096,
            "temperature": 0.1,
        }

        for attempt in range(retries):
            try:
                resp = requests.post(
                    OPENROUTER_URL,
                    headers=headers,
                    json=payload,
                    timeout=PAGE_TIMEOUT,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    content = data["choices"][0]["message"]["content"]
                    return self._parse_response(content, page_num)
                elif resp.status_code == 429:
                    logger.warning("Rate limited, waiting 5s...")
                    time.sleep(5)
                else:
                    logger.warning("DeepSeek API %d: %s", resp.status_code, resp.text[:200])
            except requests.exceptions.Timeout:
                logger.warning("Page %d timeout", page_num + 1)
            except Exception as e:
                logger.warning("Page %d error: %s", page_num + 1, e)

            if attempt < retries - 1:
                time.sleep(2)

        return []

    def _parse_response(self, content, page_num):
        """Parse JSON array from LLM response."""
        # Extract JSON from markdown code blocks if present
        json_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', content, re.DOTALL)
        if json_match:
            content = json_match.group(1)
        else:
            # Try finding raw JSON array
            arr_match = re.search(r'\[.*\]', content, re.DOTALL)
            if arr_match:
                content = arr_match.group(0)

        try:
            models = json.loads(content)
            if not isinstance(models, list):
                return []

            valid = []
            for m in models:
                if not isinstance(m, dict):
                    continue
                model_name = str(m.get("model", "")).strip()
                if not model_name or len(model_name) < 3:
                    continue

                q = float(m.get("q", 0) or 0)
                h = float(m.get("h", 0) or 0)
                kw = float(m.get("kw", 0) or 0)

                # Basic sanity
                if q > 2000 or h > 1000 or kw > 500:
                    continue

                valid.append({
                    "model": model_name,
                    "q": round(q, 1),
                    "h": round(h, 1),
                    "kw": round(kw, 2),
                })

            return valid

        except json.JSONDecodeError as e:
            logger.warning("Page %d: JSON parse error: %s", page_num + 1, e)
            return []


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    pdf = sys.argv[1] if len(sys.argv) > 1 else "/root/ONIS/catalogs/DAB_general_old.pdf"

    stage = VisionAPIStage()
    results = stage.extract_from_pdf(pdf)

    print(f"\nTotal: {len(results)} models")
    for m in results[:10]:
        print(f"  {m['model']:<30} Q={m['q']} H={m['h']} kW={m['kw']} pg={m.get('page')}")

    # Save
    out = pdf.replace(".pdf", "_vision.json")
    with open(out, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nSaved to {out}")
