"""
Stage 2: Qwen2.5-VL via Vision Pipeline — validation, fallback, column classifier.
HTTP client + page rendering. Mode logic added in later stories.
"""
import os
import io
import base64
import time
import json
import logging
import requests

import sys
sys.path.insert(0, "/root/pump_parser")
from config import GPU_VISION_URL, VLM_TIMEOUT, VLM_RETRIES, RENDER_DPI
from models.parse_result import PumpModelResult, StageResult
from models.pump_model import detect_series, parse_number

logger = logging.getLogger(__name__)


class VLMStage:
    """Stage 2: Qwen2.5-VL vision language model for validation/fallback."""

    def __init__(self, url=None, timeout=None, retries=None):
        self._url = url or GPU_VISION_URL
        self._timeout = timeout or VLM_TIMEOUT
        self._retries = retries or VLM_RETRIES

    # ── Health check ────────────────────────────────────────────────

    def _health_check(self) -> bool:
        try:
            resp = requests.get(f"{self._url}/health", timeout=10)
            return resp.status_code == 200
        except Exception:
            return False

    # ── HTTP calls ──────────────────────────────────────────────────

    def _call_analyze(self, image_b64: str, task: str, context: str = "") -> dict:
        """POST /analyze — send image to VLM with task description.
        Returns response dict or None on failure."""
        payload = {
            "image": image_b64,
            "task": task,
            "context": context,
        }
        return self._post("/analyze", payload)

    def _call_extract(self, image_b64: str) -> dict:
        """POST /extract — OCR + VLM extraction with cross-validation.
        Returns response dict or None on failure."""
        payload = {
            "image": image_b64,
        }
        return self._post("/extract", payload)

    def _post(self, endpoint: str, payload: dict) -> dict:
        """HTTP POST with retry logic."""
        last_error = None
        for attempt in range(1, self._retries + 1):
            try:
                logger.info("VLM %s attempt %d/%d", endpoint, attempt, self._retries)
                resp = requests.post(
                    f"{self._url}{endpoint}",
                    data=payload,
                    timeout=self._timeout,
                )
                if resp.status_code == 200:
                    body = resp.json()
                    # Check for Ollama error wrapped in 200 response
                    if body.get("error") and not body.get("pumps") and not body.get("models") and not body.get("analysis"):
                        last_error = f"Ollama error: {str(body['error'])[:60]}"
                        logger.warning("VLM attempt %d: %s", attempt, last_error)
                    else:
                        return body
                else:
                    last_error = f"HTTP {resp.status_code}"
                    logger.warning("VLM attempt %d: %s", attempt, last_error)
            except requests.exceptions.Timeout:
                last_error = "timeout"
                logger.warning("VLM attempt %d: timeout (%ds)", attempt, self._timeout)
            except requests.exceptions.ConnectionError as e:
                last_error = f"connection: {e}"
                logger.warning("VLM attempt %d: connection error", attempt)
            except Exception as e:
                last_error = str(e)
                logger.warning("VLM attempt %d: %s", attempt, last_error)

            if attempt < self._retries:
                backoff = 10 * attempt  # 10s, 20s, 30s — Ollama needs time to load
                logger.info("VLM retry in %ds...", backoff)
                time.sleep(backoff)

        logger.error("VLM %s: all retries failed (%s)", endpoint, last_error)
        return None

    # ── Page rendering ──────────────────────────────────────────────

    def _render_page(self, doc, page_num: int, dpi: int = None) -> bytes:
        """Render PDF page to PNG bytes via PyMuPDF.
        Args:
            doc: fitz.Document object (already opened)
            page_num: 0-indexed page number
            dpi: render resolution (default from config)
        Returns: PNG bytes or empty bytes on error.
        """
        if dpi is None:
            dpi = RENDER_DPI
        try:
            page = doc[page_num]
            zoom = dpi / 72.0
            mat = __import__("fitz").Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat)
            return pix.tobytes("png")
        except Exception as e:
            logger.error("Render page %d failed: %s", page_num, e)
            return b""

    def _render_page_b64(self, doc, page_num: int, dpi: int = None) -> str:
        """Render page and return as base64 string."""
        png = self._render_page(doc, page_num, dpi)
        if not png:
            return ""
        return base64.b64encode(png).decode("ascii")

    # ── VLM response parsing ────────────────────────────────────────

    def _parse_vlm_models(self, resp: dict) -> list:
        """Parse VLM response into List[PumpModelResult].
        Handles multiple response formats:
        - {"models": [{...}]}
        - {"analysis": '{"models": [...]}'}  (JSON string)
        - {"analysis": {"models": [...]}}     (dict)
        """
        if not resp or not isinstance(resp, dict):
            return []

        models_raw = None

        # Format 1: direct "models" key
        if "models" in resp and isinstance(resp["models"], list):
            models_raw = resp["models"]

        # Format 2/3: inside "analysis"
        elif "analysis" in resp:
            analysis = resp["analysis"]
            if isinstance(analysis, str):
                # JSON string — parse it
                try:
                    parsed = json.loads(analysis)
                    if isinstance(parsed, dict) and "models" in parsed:
                        models_raw = parsed["models"]
                    elif isinstance(parsed, list):
                        models_raw = parsed
                except (json.JSONDecodeError, TypeError):
                    # Try to extract JSON from text
                    try:
                        start = analysis.index("[")
                        end = analysis.rindex("]") + 1
                        models_raw = json.loads(analysis[start:end])
                    except (ValueError, json.JSONDecodeError):
                        pass
            elif isinstance(analysis, dict) and "models" in analysis:
                models_raw = analysis["models"]
            elif isinstance(analysis, list):
                models_raw = analysis

        if not models_raw or not isinstance(models_raw, list):
            return []

        results = []
        for item in models_raw:
            if not isinstance(item, dict):
                continue

            # Model name: try multiple keys
            name = ""
            for key in ("model", "name", "pump", "тип", "модель"):
                if key in item and item[key]:
                    name = str(item[key]).strip()
                    break
            if not name or len(name) < 2:
                continue

            # Q value
            q = 0.0
            for key in ("q", "flow", "q_nom", "подача", "расход", "Q"):
                if key in item and item[key] is not None:
                    v = parse_number(item[key])
                    if v is not None:
                        q = v
                        break

            # H value
            h = 0.0
            for key in ("h", "head", "h_nom", "напор", "H"):
                if key in item and item[key] is not None:
                    v = parse_number(item[key])
                    if v is not None:
                        h = v
                        break

            # kW value
            kw = 0.0
            for key in ("kw", "power", "power_kw", "мощность", "kW", "p2"):
                if key in item and item[key] is not None:
                    v = parse_number(item[key])
                    if v is not None:
                        kw = v
                        break

            series = detect_series(name)

            results.append(PumpModelResult(
                model=name,
                series=series,
                q=q,
                h=h,
                kw=kw,
                confidence_q=0.5 if q else 0.0,
                confidence_h=0.5 if h else 0.0,
                confidence_kw=0.5 if kw else 0.0,
                source_q="vlm" if q else "",
                source_h="vlm" if h else "",
                source_kw="vlm" if kw else "",
            ))

        return results

    # ── Mode A: Validate Docling results ────────────────────────────

    def _mode_a_validate(self, image_b64: str, docling_models: list) -> list:
        """Send page image to VLM with Docling models as context.
        VLM independently extracts Q/H/kW and returns its version.
        """
        # Build context from Docling models
        context_models = []
        for m in docling_models:
            context_models.append({
                "model": m.model,
                "q": m.q,
                "h": m.h,
                "kw": m.kw,
            })

        task = (
            "На этой странице таблица с характеристиками насосов. "
            "Извлеки ВСЕ модели насосов с параметрами: Q (подача, м³/ч), H (напор, м), kW (мощность). "
            "Верни JSON: {\"models\": [{\"model\": \"...\", \"q\": N, \"h\": N, \"kw\": N}]}"
        )
        context = (
            "Для справки, ранее из этой таблицы были извлечены модели: "
            + json.dumps(context_models, ensure_ascii=False)
            + ". Проверь и дополни эти данные."
        )

        resp = self._call_analyze(image_b64, task, context)
        if not resp:
            return []

        return self._parse_vlm_models(resp)

    # ── Mode B: Fallback extraction (Docling failed) ────────────────

    def _mode_b_extract(self, image_b64: str) -> list:
        """VLM extracts pump models from scratch when Docling found nothing.
        Uses /extract endpoint (OCR + VLM cross-validation on GPU server).
        """
        resp = self._call_extract(image_b64)
        if not resp:
            return []

        return self._parse_vlm_models(resp)

    # ── Mode C: Column classifier (replaces DeepSeek) ───────────────

    PROMPT_CLASSIFY = (
        "На изображении таблица с данными о насосах.\n"
        "Колонки таблицы: {columns}\n"
        "Первые строки данных:\n{sample}\n\n"
        "Определи роль каждой колонки. Верни JSON:\n"
        '{{"model": "имя_колонки_с_моделями", '
        '"q": "имя_колонки_с_подачей_м3ч", '
        '"h": "имя_колонки_с_напором_м", '
        '"kw": "имя_колонки_с_мощностью_квт", '
        '"rpm": "имя_колонки_с_оборотами"}}\n'
        "Если колонки нет — пропусти ключ."
    )

    def classify_columns(self, pdf_path: str, page_num: int, columns: list, sample_rows: list) -> dict:
        """VLM determines column roles from page image + column names.
        Returns {"model": "col_name", "q": "col_name", ...} or empty dict.
        """
        try:
            import fitz
            doc = fitz.open(pdf_path)
            b64 = self._render_page_b64(doc, page_num)
            doc.close()
        except Exception as e:
            logger.error("classify_columns render failed: %s", e)
            return {}

        if not b64:
            return {}

        # Build sample text
        sample_lines = []
        for row in sample_rows[:3]:
            vals = [f"{k}: {v}" for k, v in row.items()]
            sample_lines.append(" | ".join(vals))

        task = self.PROMPT_CLASSIFY.format(
            columns=", ".join(str(c) for c in columns),
            sample="\n".join(sample_lines),
        )

        resp = self._call_analyze(b64, task)
        if not resp:
            return {}

        # Parse response — extract JSON dict
        analysis = resp.get("analysis", "")
        if isinstance(analysis, dict):
            return {k: v for k, v in analysis.items() if k in ("model", "q", "h", "kw", "rpm") and v}

        if isinstance(analysis, str):
            try:
                # Find JSON in response
                start = analysis.index("{")
                end = analysis.rindex("}") + 1
                parsed = json.loads(analysis[start:end])
                return {k: v for k, v in parsed.items() if k in ("model", "q", "h", "kw", "rpm") and v}
            except (ValueError, json.JSONDecodeError):
                pass

        return {}

    # ── Main process method ─────────────────────────────────────────

    def process(self, pdf_path: str, docling_result: StageResult) -> StageResult:
        """Orchestrate VLM modes based on Docling results.
        - Pages with gaps (q=0/h=0/kw=0) → Mode A (validate)
        - Pages with 0 models from Docling → Mode B (extract from scratch)
        - Docling failed entirely → Mode B for all pages
        """
        result = StageResult(source="vlm")

        if not self._health_check():
            result.errors.append("VLM сервер недоступен")
            return result

        try:
            import fitz
            doc = fitz.open(pdf_path)
        except Exception as e:
            result.errors.append(f"Не удалось открыть PDF: {e}")
            return result

        try:
            total_pages = len(doc)

            # Build page → models map from Docling
            page_models = {}  # page_num -> [PumpModelResult]
            pages_with_tables = set()
            for m in docling_result.models:
                pg = m.page_number
                page_models.setdefault(pg, []).append(m)
                pages_with_tables.add(pg)

            # Also track pages that had raw tables but 0 models
            for t in docling_result.raw_tables:
                pg = t.get("page", 0)
                pages_with_tables.add(pg)

            docling_total = len(docling_result.models)

            # Decide which pages need VLM
            pages_mode_a = []  # validate (has models but gaps)
            pages_mode_b = []  # extract from scratch

            if docling_total == 0:
                # Docling failed entirely → Mode B for pages with tables
                if pages_with_tables:
                    pages_mode_b = sorted(pages_with_tables)
                else:
                    # No tables found at all — try first 10 pages
                    pages_mode_b = list(range(min(10, total_pages)))
            else:
                for pg in pages_with_tables:
                    models = page_models.get(pg, [])
                    if not models:
                        # Table on page but no models extracted
                        pages_mode_b.append(pg)
                    elif any(not m.is_complete for m in models):
                        # Has models but some incomplete
                        pages_mode_a.append(pg)

            logger.info("VLM plan: %d pages Mode A, %d pages Mode B (total docling: %d models)",
                        len(pages_mode_a), len(pages_mode_b), docling_total)

            # Process Mode A pages
            for pg in pages_mode_a:
                try:
                    b64 = self._render_page_b64(doc, pg)
                    if not b64:
                        continue
                    models = self._mode_a_validate(b64, page_models.get(pg, []))
                    for m in models:
                        m.page_number = pg
                    result.models.extend(models)
                    result.pages_processed += 1
                except Exception as e:
                    result.errors.append(f"Mode A page {pg}: {e}")
                    logger.warning("Mode A page %d error: %s", pg, e)

            # Process Mode B pages
            for pg in pages_mode_b:
                try:
                    b64 = self._render_page_b64(doc, pg)
                    if not b64:
                        continue
                    models = self._mode_b_extract(b64)
                    for m in models:
                        m.page_number = pg
                    result.models.extend(models)
                    result.pages_processed += 1
                except Exception as e:
                    result.errors.append(f"Mode B page {pg}: {e}")
                    logger.warning("Mode B page %d error: %s", pg, e)

        finally:
            doc.close()

        logger.info("VLM done: %d models from %d pages", len(result.models), result.pages_processed)
        return result
