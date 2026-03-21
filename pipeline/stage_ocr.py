"""
Stage 3: PaddleOCR verification — ground truth for numbers.
Verifies Docling and VLM extracted values against raw OCR text.
"""
import re
import time
import logging
import base64
import requests

import sys
sys.path.insert(0, "/root/pump_parser")
from config import GPU_VISION_URL, OCR_TIMEOUT, OCR_RETRIES, OCR_AGREE_THRESHOLD
from models.parse_result import PumpModelResult, StageResult

logger = logging.getLogger(__name__)

# Reasonable value ranges for pump parameters
REASONABLE_RANGES = {
    "q": (0.1, 1000),    # m³/h
    "h": (0.5, 500),     # m
    "kw": (0.1, 500),    # kW
}


class OCRStage:
    """Stage 3: PaddleOCR number verification."""

    def __init__(self, url=None, timeout=None, retries=None):
        self._url = url or GPU_VISION_URL
        self._timeout = timeout or OCR_TIMEOUT
        self._retries = retries or OCR_RETRIES

    # ── HTTP client ─────────────────────────────────────────────────

    def _call_ocr(self, image_b64: str) -> dict:
        """POST /ocr — send image, get OCR results back."""
        last_error = None
        for attempt in range(1, self._retries + 1):
            try:
                resp = requests.post(
                    f"{self._url}/ocr",
                    data={"image": image_b64, "lang": "ru"},
                    timeout=self._timeout,
                )
                if resp.status_code == 200:
                    return resp.json()
                last_error = f"HTTP {resp.status_code}"
            except requests.exceptions.Timeout:
                last_error = "timeout"
            except Exception as e:
                last_error = str(e)

            if attempt < self._retries:
                time.sleep(2 * attempt)

        logger.warning("OCR: all retries failed (%s)", last_error)
        return None

    # ── Number extraction ───────────────────────────────────────────

    def _extract_numbers(self, ocr_data: dict) -> list:
        """Extract all numeric values from OCR response.
        Returns list of {value, text, bbox} dicts.
        """
        numbers = []
        if not ocr_data:
            return numbers

        results = ocr_data.get("results", [])
        if not results and "text" in ocr_data:
            # Simple format: just text
            results = [{"text": ocr_data["text"], "bbox": []}]

        for item in results:
            text = str(item.get("text", ""))
            bbox = item.get("bbox", [])

            # Find all numbers in this text block
            for match in re.finditer(r"(\d+[.,]\d+|\d+)", text):
                try:
                    val_str = match.group(1).replace(",", ".")
                    val = float(val_str)
                    numbers.append({
                        "value": val,
                        "text": match.group(0),
                        "bbox": bbox,
                    })
                except (ValueError, TypeError):
                    pass

        return numbers

    def _extract_text(self, ocr_data: dict) -> str:
        """Extract full text from OCR response."""
        if not ocr_data:
            return ""
        results = ocr_data.get("results", [])
        if not results and "text" in ocr_data:
            return str(ocr_data["text"])
        return " ".join(str(r.get("text", "")) for r in results)

    # ── Validation helpers ──────────────────────────────────────────

    def _is_reasonable(self, attr: str, value: float) -> bool:
        """Check if value is within reasonable range for pump parameter."""
        r = REASONABLE_RANGES.get(attr)
        if not r:
            return True
        return r[0] <= value <= r[1]

    def _values_agree(self, v1: float, v2: float) -> bool:
        """Check if two values agree within OCR_AGREE_THRESHOLD (5%)."""
        if v1 == 0 or v2 == 0:
            return v1 == v2
        return abs(v1 - v2) / max(abs(v1), abs(v2)) <= OCR_AGREE_THRESHOLD

    def _find_closest(self, target: float, numbers: list, attr: str) -> float:
        """Find closest reasonable OCR number to target value."""
        candidates = [
            n["value"] for n in numbers
            if self._is_reasonable(attr, n["value"])
        ]
        if not candidates:
            return 0.0
        return min(candidates, key=lambda x: abs(x - target))

    # ── Model verification ──────────────────────────────────────────

    def _verify_model(self, model: PumpModelResult, ocr_text: str, ocr_numbers: list):
        """Verify a single model's values against OCR. Mutates model in-place."""
        # Check if model name appears in OCR text
        name_clean = re.sub(r"[\s\-]", "", model.model.upper())
        text_clean = re.sub(r"[\s\-]", "", ocr_text.upper())

        if name_clean not in text_clean:
            # Model not found on this page by OCR — skip
            return

        # Verify each parameter
        for attr, val, conf_attr, src_attr in [
            ("q", model.q, "confidence_q", "source_q"),
            ("h", model.h, "confidence_h", "source_h"),
            ("kw", model.kw, "confidence_kw", "source_kw"),
        ]:
            current_val = val
            current_conf = getattr(model, conf_attr)

            if current_val <= 0:
                continue

            # Check if OCR has a matching number
            found_match = any(
                self._values_agree(current_val, n["value"])
                for n in ocr_numbers
                if self._is_reasonable(attr, n["value"])
            )

            if found_match:
                # OCR confirms → boost confidence
                new_conf = min(1.0, current_conf + 0.3)
                setattr(model, conf_attr, new_conf)
            elif current_conf <= 0.5:
                # Low confidence + OCR disagrees → try OCR override
                closest = self._find_closest(current_val, ocr_numbers, attr)
                if closest > 0 and not self._values_agree(current_val, closest):
                    old_val = current_val
                    setattr(model, attr, closest)
                    setattr(model, conf_attr, 0.7)
                    setattr(model, src_attr, "ocr")
                    logger.info("%s: OCR override %s=%.1f→%.1f", model.model, attr, old_val, closest)

    # ── Main verify method ──────────────────────────────────────────

    def verify(self, pdf_path: str, merged_result: StageResult) -> StageResult:
        """Verify models against OCR. Only processes pages with low confidence.
        Args:
            pdf_path: path to PDF
            merged_result: StageResult with models to verify
        Returns: StageResult with verified models (same models, updated confidence).
        """
        result = StageResult(source="ocr")

        if not merged_result.models:
            return result

        # Group models by page, find pages needing verification
        pages_to_check = {}  # page_num -> [model]
        for m in merged_result.models:
            needs_check = (
                (m.q > 0 and m.confidence_q < 0.8) or
                (m.h > 0 and m.confidence_h < 0.8) or
                (m.kw > 0 and m.confidence_kw < 0.8)
            )
            if needs_check:
                pages_to_check.setdefault(m.page_number, []).append(m)

        if not pages_to_check:
            logger.info("OCR: no pages need verification (all confidence >= 0.8)")
            return result

        logger.info("OCR: verifying %d models on %d pages",
                     sum(len(v) for v in pages_to_check.values()), len(pages_to_check))

        try:
            import fitz
            doc = fitz.open(pdf_path)
        except Exception as e:
            result.errors.append(f"Не удалось открыть PDF: {e}")
            return result

        try:
            for page_num, models in sorted(pages_to_check.items()):
                try:
                    # Render page
                    page = doc[page_num]
                    zoom = 200 / 72.0
                    mat = fitz.Matrix(zoom, zoom)
                    pix = page.get_pixmap(matrix=mat)
                    png = pix.tobytes("png")
                    b64 = base64.b64encode(png).decode("ascii")

                    # OCR
                    ocr_data = self._call_ocr(b64)
                    if not ocr_data:
                        result.errors.append(f"OCR page {page_num}: no response")
                        continue

                    ocr_text = self._extract_text(ocr_data)
                    ocr_numbers = self._extract_numbers(ocr_data)

                    # Verify each model on this page
                    for m in models:
                        self._verify_model(m, ocr_text, ocr_numbers)

                    result.models.extend(models)
                    result.pages_processed += 1

                except Exception as e:
                    result.errors.append(f"OCR page {page_num}: {e}")
                    logger.warning("OCR page %d error: %s", page_num, e)
        finally:
            doc.close()

        logger.info("OCR done: verified %d models on %d pages", len(result.models), result.pages_processed)
        return result
