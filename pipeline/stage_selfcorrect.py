"""
Stage 4: Self-correction — targeted VLM queries for missing Q/H/kW values.
For each model with zeros: renders the page, asks VLM specifically for that number.
"""
import re
import logging
import base64

import sys
sys.path.insert(0, "/root/pump_parser")
from config import SELFCORRECT_MAX_ATTEMPTS
from models.parse_result import PumpModelResult, StageResult
from pipeline.stage_vlm import VLMStage

logger = logging.getLogger(__name__)

PROMPT_SELF_CORRECT = (
    "На этой странице PDF каталога насосов есть модель \"{model}\".\n"
    "Найди значение: {param_description}.\n"
    "Ответь ОДНИМ числом. Если не нашёл — ответь 0."
)

PARAM_DESC = {
    "q": "подача Q (м³/ч, flow rate)",
    "h": "напор H (м, head)",
    "kw": "мощность P2 (кВт, power)",
}


class SelfCorrectionStage:
    """Stage 4: Fill gaps in parsed models via targeted VLM queries."""

    def __init__(self, vlm_stage: VLMStage = None):
        self._vlm = vlm_stage or VLMStage()
        self._max_attempts = SELFCORRECT_MAX_ATTEMPTS

    def fill_gaps(self, pdf_path: str, models: list) -> list:
        """For each model with q=0/h=0/kw=0: ask VLM to find the missing value.
        Mutates models in-place AND returns the list.
        """
        # Find models with gaps
        gaps = [(i, m) for i, m in enumerate(models) if not m.is_complete and m.page_number >= 0]

        if not gaps:
            logger.info("Self-correct: no gaps to fill")
            return models

        logger.info("Self-correct: %d models with gaps", len(gaps))

        try:
            import fitz
            doc = fitz.open(pdf_path)
        except Exception as e:
            logger.error("Self-correct: can't open PDF: %s", e)
            return models

        # Cache rendered pages
        page_cache = {}

        try:
            for idx, model in gaps:
                pg = model.page_number
                if pg < 0 or pg >= len(doc):
                    continue

                # Render page (cached)
                if pg not in page_cache:
                    try:
                        page = doc[pg]
                        zoom = 200 / 72.0
                        mat = fitz.Matrix(zoom, zoom)
                        pix = page.get_pixmap(matrix=mat)
                        png = pix.tobytes("png")
                        page_cache[pg] = base64.b64encode(png).decode("ascii")
                    except Exception:
                        continue

                b64 = page_cache.get(pg)
                if not b64:
                    continue

                # Fill each missing parameter
                for attr in ("q", "h", "kw"):
                    if getattr(model, attr, 0) > 0:
                        continue  # already has value

                    value = self._targeted_extract(b64, model.model, attr)
                    if value and value > 0:
                        setattr(model, attr, value)
                        setattr(model, f"confidence_{attr}", 0.4)
                        setattr(model, f"source_{attr}", "selfcorrect")
                        logger.info("Self-correct: %s.%s = %.1f", model.model, attr, value)

        finally:
            doc.close()

        filled = sum(1 for _, m in gaps if m.is_complete)
        logger.info("Self-correct done: %d/%d gaps filled to complete", filled, len(gaps))
        return models

    def _targeted_extract(self, image_b64: str, model_name: str, attr: str) -> float:
        """Ask VLM for a specific missing value. Returns float or 0."""
        desc = PARAM_DESC.get(attr, attr)
        task = PROMPT_SELF_CORRECT.format(model=model_name, param_description=desc)

        for attempt in range(self._max_attempts):
            resp = self._vlm._call_analyze(image_b64, task)
            if not resp:
                continue

            value = self._parse_single_number(resp)
            if value and value > 0:
                return value

        return 0.0

    def _parse_single_number(self, resp: dict) -> float:
        """Extract a single float from VLM response."""
        if not resp:
            return 0.0

        analysis = resp.get("analysis", "")
        if isinstance(analysis, dict):
            # Try "value" key
            v = analysis.get("value") or analysis.get("result") or analysis.get("answer")
            if v is not None:
                try:
                    return float(str(v).replace(",", "."))
                except (ValueError, TypeError):
                    pass

        if isinstance(analysis, (int, float)):
            return float(analysis)

        text = str(analysis)
        # Find numbers in text
        numbers = re.findall(r"(\d+[.,]\d+|\d+)", text)
        if numbers:
            try:
                return float(numbers[0].replace(",", "."))
            except (ValueError, TypeError):
                pass

        return 0.0
