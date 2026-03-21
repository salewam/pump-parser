"""
Stage 1: Docling TableFormer — PDF table extraction via GPU server.
HTTP client with retry logic. Strategy 1 (direct columns) + Strategy 2 (CDM-style).
"""
import os
import re
import time
import logging
import requests

import sys
sys.path.insert(0, "/root/pump_parser")
from config import GPU_DOCLING_URL, DOCLING_TIMEOUT, DOCLING_HEALTH_TIMEOUT, DOCLING_RETRIES, PUMP_MODEL_RE
from models.parse_result import PumpModelResult, StageResult
from models.pump_model import detect_series, parse_number, enrich_from_model_name, validate_pump_physics

logger = logging.getLogger(__name__)


class DoclingStage:
    """Stage 1: Extract tables from PDF via Docling GPU server."""

    def __init__(self, url=None, timeout=None, retries=None):
        self._url = url or GPU_DOCLING_URL
        self._timeout = timeout or DOCLING_TIMEOUT
        self._retries = retries or DOCLING_RETRIES

    def _health_check(self) -> bool:
        try:
            resp = requests.get(f"{self._url}/health", timeout=DOCLING_HEALTH_TIMEOUT)
            return resp.status_code == 200
        except Exception:
            return False

    def extract(self, pdf_path: str) -> StageResult:
        """Send PDF to Docling, get tables, convert to models."""
        result = StageResult(source="docling")

        if not self._health_check():
            result.errors.append("Docling GPU недоступен")
            logger.error("Docling health check failed: %s", self._url)
            return result

        tables = None
        last_error = None

        for attempt in range(1, self._retries + 1):
            try:
                logger.info("Docling attempt %d/%d: %s", attempt, self._retries, os.path.basename(pdf_path))
                with open(pdf_path, "rb") as f:
                    resp = requests.post(
                        f"{self._url}/parse",
                        files={"file": (os.path.basename(pdf_path), f, "application/pdf")},
                        timeout=self._timeout,
                    )
                if resp.status_code == 200:
                    data = resp.json()
                    tables = data.get("tables", [])
                    result.pages_processed = data.get("total_pages", 0)
                    logger.info("Docling OK: %d tables from %d pages", len(tables), result.pages_processed)
                    break
                else:
                    last_error = f"HTTP {resp.status_code}"
                    logger.warning("Docling attempt %d: %s", attempt, last_error)
            except requests.exceptions.Timeout:
                last_error = f"Timeout ({self._timeout}s)"
                logger.warning("Docling attempt %d: timeout", attempt)
            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {e}"
                logger.warning("Docling attempt %d: connection error", attempt)
            except Exception as e:
                last_error = str(e)
                logger.warning("Docling attempt %d: %s", attempt, last_error)

            if attempt < self._retries:
                backoff = 2 ** attempt
                logger.info("Retrying in %ds...", backoff)
                time.sleep(backoff)

        if tables is None:
            result.errors.append(f"Docling: все попытки исчерпаны ({last_error})")
            return result

        result.raw_tables = tables
        result.models = self._tables_to_models(tables)
        return result

    # ── Column identification ───────────────────────────────────────

    def _identify_columns(self, cols):
        """Keyword-based column role detection."""
        model_col = q_col = h_col = kw_col = rpm_col = None

        MODEL_KW = ["модель", "model", "тип", "type", "наименование", "насос", "pump", "обозначение"]
        Q_KW = ["подача", "расход", "flow", "q,", "q ", "м³/ч", "m3/h", "производительность", "capacity", "q"]
        H_KW = ["напор", "head", "h,", "h ", "давление", "pressure", "h"]
        KW_KW = ["мощность", "power", "квт", "kw", "p2", "p₂", "мощн"]
        RPM_KW = ["об/мин", "rpm", "частота вращения", "скорость", "n,"]

        for col in cols:
            cl = str(col).lower().strip()
            if not model_col and any(k in cl for k in MODEL_KW):
                model_col = col
            elif not q_col and any(k in cl for k in Q_KW):
                q_col = col
            elif not h_col and any(k in cl for k in H_KW):
                h_col = col
            elif not kw_col and any(k in cl for k in KW_KW):
                kw_col = col
            elif not rpm_col and any(k in cl for k in RPM_KW):
                rpm_col = col

        return model_col, q_col, h_col, kw_col, rpm_col

    def _find_header_models(self, cols):
        """Find columns whose names match pump model patterns."""
        models = []
        for col in cols:
            if re.match(PUMP_MODEL_RE, str(col).strip(), re.I):
                models.append(col)
        return models

    # ── Table to models conversion ──────────────────────────────────

    def _tables_to_models(self, tables):
        """Convert Docling tables to PumpModelResult list.
        Strategy 1: direct column mapping (model + Q/H/kW columns).
        Strategy 2: CDM-style (models in headers, params in rows).
        Strategy 3 (positional) deliberately NOT implemented — replaced by VLM.
        """
        all_models = []
        seen_keys = set()

        for table in tables:
            cols = table.get("columns", [])
            rows = table.get("data", [])
            page = table.get("page", 0)

            if not cols or not rows:
                continue

            model_col, q_col, h_col, kw_col, rpm_col = self._identify_columns(cols)

            # ── Strategy 1: direct column mapping ──
            if model_col and any([q_col, h_col, kw_col]):
                self._strategy1(rows, model_col, q_col, h_col, kw_col, rpm_col, page, all_models, seen_keys)
                continue

            # ── Strategy 2: CDM-style (models in headers, params in rows) ──
            header_models = self._find_header_models(cols)
            if header_models and len(rows) >= 3:
                self._strategy2(rows, cols, header_models, page, all_models, seen_keys)
                continue

        return all_models

    def _strategy1(self, rows, model_col, q_col, h_col, kw_col, rpm_col, page, all_models, seen_keys):
        """Strategy 1: direct column mapping."""
        for row in rows:
            model_name = str(row.get(model_col, "")).strip()
            if not model_name or len(model_name) < 3:
                continue

            q = parse_number(row.get(q_col)) if q_col else None
            h = parse_number(row.get(h_col)) if h_col else None
            kw = parse_number(row.get(kw_col)) if kw_col else None
            rpm_val = parse_number(row.get(rpm_col)) if rpm_col else None

            pm = self._build_model(model_name, q, h, kw, rpm_val, page)
            if pm.key and pm.key not in seen_keys:
                seen_keys.add(pm.key)
                all_models.append(pm)

    def _strategy2(self, rows, cols, header_models, page, all_models, seen_keys):
        """Strategy 2: CDM-style — models in headers, params in rows."""
        LABEL_Q = ["подача", "расход", "flow", "q,", "q ", "м³/ч"]
        LABEL_H = ["напор", "head", "h,", "h ", "давление"]
        LABEL_KW = ["мощность", "power", "квт", "kw", "p2"]
        LABEL_RPM = ["об/мин", "rpm", "частота"]

        # Find label column (first non-model column)
        label_col = None
        for c in cols:
            if c not in header_models:
                label_col = c
                break
        if not label_col:
            return

        # Build spec map: model_col -> {q, h, kw, rpm}
        spec_map = {mc: {"q": 0, "h": 0, "kw": 0, "rpm": 2900} for mc in header_models}

        for row in rows:
            label = str(row.get(label_col, "")).lower().strip()
            for mc in header_models:
                raw = row.get(mc)
                val = parse_number(raw if raw is not None else "")
                if val is None:
                    continue
                if any(k in label for k in LABEL_Q):
                    spec_map[mc]["q"] = val
                elif any(k in label for k in LABEL_H):
                    spec_map[mc]["h"] = val
                elif any(k in label for k in LABEL_KW):
                    spec_map[mc]["kw"] = val
                elif any(k in label for k in LABEL_RPM):
                    spec_map[mc]["rpm"] = int(val)

        for mc, specs in spec_map.items():
            pm = self._build_model(mc.strip(), specs["q"], specs["h"], specs["kw"], specs["rpm"], page)
            if pm.key and pm.key not in seen_keys:
                seen_keys.add(pm.key)
                all_models.append(pm)

    # ── Helpers ─────────────────────────────────────────────────────

    def _build_model(self, model_name, q, h, kw, rpm_val, page):
        """Build PumpModelResult with enrichment and validation."""
        series = detect_series(model_name)
        pm = PumpModelResult(
            model=model_name,
            series=series,
            q=q or 0.0,
            h=h or 0.0,
            kw=kw or 0.0,
            rpm=int(rpm_val) if rpm_val else 2900,
            page_number=page,
            confidence_q=0.6 if q else 0.0,
            confidence_h=0.6 if h else 0.0,
            confidence_kw=0.6 if kw else 0.0,
            source_q="docling" if q else "",
            source_h="docling" if h else "",
            source_kw="docling" if kw else "",
        )

        # Enrich from model name
        d = {"model": pm.model, "q_nom": pm.q, "h_nom": pm.h, "power_kw": pm.kw, "rpm": pm.rpm}
        enrich_from_model_name(d)
        validate_pump_physics(d)

        if d["q_nom"] and not pm.q:
            pm.q = d["q_nom"]
            pm.source_q = "enrichment"
            pm.confidence_q = 0.5
        if d["h_nom"] and not pm.h:
            pm.h = d["h_nom"]
            pm.source_h = "enrichment"
            pm.confidence_h = 0.5
        if d["power_kw"] and not pm.kw:
            pm.kw = d["power_kw"]
            pm.source_kw = "enrichment"
            pm.confidence_kw = 0.5
        if d["rpm"]:
            pm.rpm = d["rpm"]

        return pm
