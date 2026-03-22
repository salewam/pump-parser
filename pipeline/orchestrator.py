"""
Pipeline Orchestrator v2: simplified sequential approach.
Docling first → stop Docling → VLM page-by-page → OCR verify → merge → restart Docling.
No warmup hacks. VLM loads on first request naturally.
"""
import os
import time
import base64
import logging
import subprocess
import requests

import sys
sys.path.insert(0, "/root/pump_parser")
from config import GPU_DOCLING_URL, GPU_VISION_URL
from models.parse_result import PumpModelResult, ParseResult, StageResult
from models.pump_model import detect_series, enrich_from_model_name, validate_pump_physics
from pipeline.stage_docling import DoclingStage
from pipeline.stage_ocr import OCRStage
from pipeline.confidence import ConfidenceScorer
from brand_qualifier import BrandQualifier

logger = logging.getLogger(__name__)

GPU_HOST = "82.22.53.231"
SSH_CMD = 'sshpass -p "Kx9#mVp4\\!wL7nQ2z" ssh -o StrictHostKeyChecking=no root@' + GPU_HOST


def _gpu_ssh(cmd):
    try:
        subprocess.run(f'{SSH_CMD} "{cmd}"', shell=True, timeout=30, capture_output=True)
    except Exception:
        pass


class PipelineOrchestrator:
    """Simplified 4-stage pipeline. Sequential Docling/VLM to avoid VRAM conflict."""

    def __init__(self):
        self.docling = DoclingStage()
        self.ocr = OCRStage()
        self.scorer = ConfidenceScorer()
        self.brand_qualifier = BrandQualifier()

    def parse(self, pdf_path, progress_cb=None):
        t0 = time.time()

        def _p(msg, pct):
            if progress_cb:
                try:
                    progress_cb(msg, pct)
                except Exception:
                    pass

        # ── Stage 1: Docling ────────────────────────────────────────
        _p("Docling: извлечение таблиц...", 10)
        docling_result = self.docling.extract(pdf_path)
        logger.info("Docling: %d models, %d tables", len(docling_result.models), len(docling_result.raw_tables))

        # ── Stage 2: VLM (stop Docling, VLM page-by-page) ──────────
        _p("VLM: остановка Docling...", 25)
        _gpu_ssh("systemctl stop docling-parser")
        time.sleep(8)

        _p("VLM: анализ страниц...", 30)
        vlm_result = self._vlm_extract(pdf_path, docling_result)
        logger.info("VLM: %d models from %d pages", len(vlm_result.models), vlm_result.pages_processed)

        # ── Merge Docling + VLM ─────────────────────────────────────
        _p("Объединение результатов...", 60)
        merged = self._merge(docling_result, vlm_result)

        # ── Stage 3: OCR verification ───────────────────────────────
        _p("OCR: верификация...", 65)
        ocr_result = StageResult(source="ocr")
        try:
            ocr_result = self.ocr.verify(pdf_path, merged)
        except Exception as e:
            logger.warning("OCR error: %s", e)

        # ── Confidence scoring ──────────────────────────────────────
        _p("Scoring...", 75)
        result = self.scorer.merge_all(docling_result, vlm_result, ocr_result)

        # ── Restart Docling ─────────────────────────────────────────
        _p("Перезапуск Docling...", 80)
        _gpu_ssh("systemctl restart ollama && sleep 3 && systemctl start docling-parser")

        # ── Brand ───────────────────────────────────────────────────
        _p("Определение бренда...", 90)
        try:
            model_dicts = [{"model": m.model, "id": m.model, "series": m.series} for m in result.models]
            br = self.brand_qualifier.qualify_full(pdf_path, model_dicts)
            result.brand = br.brand
            result.brand_confidence = br.confidence
            result.brand_source = br.source
            result.series_detected = br.series_detected
        except Exception:
            result.brand = "Unknown"

        result.elapsed = round(time.time() - t0, 1)
        for s in [docling_result, vlm_result, ocr_result]:
            result.errors.extend(s.errors)

        stages = []
        if docling_result.models:
            stages.append("docling")
        if vlm_result.models:
            stages.append("vlm")
        if ocr_result.models:
            stages.append("ocr")
        result.stages_completed = stages

        _p("Готово!", 100)
        logger.info("Pipeline: %d models, %d complete (%.0f%%), brand=%s, %.0fs",
                     result.total_models, result.complete_models, result.completeness,
                     result.brand, result.elapsed)
        return result

    def _vlm_extract(self, pdf_path, docling_result):
        """VLM page-by-page extraction. No warmup — model loads on first request."""
        result = StageResult(source="vlm")
        try:
            import fitz
            doc = fitz.open(pdf_path)
        except Exception as e:
            result.errors.append(str(e))
            return result

        try:
            # Determine which pages to process
            pages_with_tables = set()
            for t in docling_result.raw_tables:
                pg = t.get("page", -1)
                if pg >= 0:
                    pages_with_tables.add(pg)

            if pages_with_tables:
                pages_to_check = sorted(pages_with_tables)
            else:
                # Pre-filter: only pages with pump model names or numeric tables
                import re as _re
                pages_to_check = []
                for pg_i in range(len(doc)):
                    text = doc[pg_i].get_text()
                    # Skip if page has < 3 numbers (no table data)
                    nums = _re.findall(r'\d+[.,]\d+', text)
                    # Skip cover/contents pages
                    has_models = bool(_re.search(r'[A-ZА-Я]{2,5}\s*\d+[-]\d+', text))
                    has_table_data = len(nums) >= 5
                    if has_models or has_table_data:
                        pages_to_check.append(pg_i)
                logger.info("VLM: %d/%d pages have table data", len(pages_to_check), len(doc))

            ollama_ready = False

            logger.info("VLM: processing %d pages: %s", len(pages_to_check), pages_to_check[:10])
            for pg_idx, pg in enumerate(pages_to_check):
                logger.info("VLM page %d/%d (pg%d)", pg_idx+1, len(pages_to_check), pg)
                if pg >= len(doc):
                    continue
                try:
                    pix = doc[pg].get_pixmap(matrix=__import__("fitz").Matrix(1.2, 1.2))
                    b64 = base64.b64encode(pix.tobytes("png")).decode()

                    task = "extract_pumps"

                    # Retry loop: 6 attempts for first page (model loading), 3 for others
                    max_attempts = 6 if not ollama_ready else 3
                    d = None
                    for attempt in range(max_attempts):
                        try:
                            r = requests.post(
                                f"http://{GPU_HOST}:8000/analyze",
                                data={"image": b64, "task": task},
                                timeout=300,
                            )
                        except requests.exceptions.Timeout:
                            logger.warning("VLM pg%d attempt %d: request timeout", pg, attempt + 1)
                            continue
                        except Exception as e:
                            logger.warning("VLM pg%d attempt %d: %s", pg, attempt + 1, e)
                            time.sleep(5)
                            continue

                        if r.status_code != 200:
                            logger.warning("VLM pg%d attempt %d: HTTP %d", pg, attempt + 1, r.status_code)
                            time.sleep(10 if not ollama_ready else 5)
                            continue

                        d = r.json()
                        if d.get("error"):
                            logger.info("VLM pg%d attempt %d: %s", pg, attempt + 1, str(d["error"])[:60])
                            time.sleep(15 if not ollama_ready else 5)
                            d = None
                            continue
                        break

                    if not d:
                        logger.warning("VLM pg%d: all %d attempts failed", pg, max_attempts)
                        continue

                    ollama_ready = True
                    logger.info("VLM pg%d: response OK", pg)

                    pumps = d.get("pumps", [])
                    if pumps:
                        for p in pumps:
                            name = str(p.get("model", "")).strip()
                            # Filter: must be real pump model name
                            if not name or len(name) < 5 or name.isdigit():
                                continue
                            from config import KNOWN_SERIES
                            series = detect_series(name)
                            # Must have series in KNOWN_SERIES, or be long enough to be real model
                            if series.upper() not in KNOWN_SERIES and len(name) < 10:
                                continue
                            result.models.append(PumpModelResult(
                                model=name,
                                series=series,
                                q=float(p.get("q_nom", 0) or 0),
                                h=float(p.get("h_nom", 0) or 0),
                                kw=float(p.get("power_kw", 0) or 0),
                                page_number=pg,
                                confidence_q=0.5 if p.get("q_nom") else 0,
                                confidence_h=0.5 if p.get("h_nom") else 0,
                                confidence_kw=0.5 if p.get("power_kw") else 0,
                                source_q="vlm" if p.get("q_nom") else "",
                                source_h="vlm" if p.get("h_nom") else "",
                                source_kw="vlm" if p.get("power_kw") else "",
                            ))
                        result.pages_processed += 1
                        logger.info("VLM pg%d: %d pumps", pg, len(pumps))

                except requests.exceptions.Timeout:
                    logger.warning("VLM pg%d: timeout", pg)
                except Exception as e:
                    logger.warning("VLM pg%d: %s", pg, e)

            # Second pass: retry failed pages (those that returned no pumps)
            processed_pages = {m.page_number for m in result.models}
            failed_pages = [pg for pg in pages_to_check if pg not in processed_pages and pg < len(doc)]
            if failed_pages and ollama_ready:
                logger.info("VLM retry: %d failed pages", len(failed_pages))
                for pg in failed_pages:
                    try:
                        pix = doc[pg].get_pixmap(matrix=__import__("fitz").Matrix(1.2, 1.2))
                        b64 = base64.b64encode(pix.tobytes("png")).decode()
                        r = requests.post(
                            f"http://{GPU_HOST}:8000/analyze",
                            data={"image": b64, "task": "extract_pumps"},
                            timeout=300,
                        )
                        if r.status_code == 200:
                            d = r.json()
                            if not d.get("error"):
                                for p in d.get("pumps", []):
                                    name = str(p.get("model", "")).strip()
                                    if not name or len(name) < 5 or name.isdigit():
                                        continue
                                    from config import KNOWN_SERIES
                                    series = detect_series(name)
                                    if series.upper() not in KNOWN_SERIES and len(name) < 10:
                                        continue
                                    result.models.append(PumpModelResult(
                                        model=name, series=series,
                                        q=float(p.get("q_nom", 0) or 0),
                                        h=float(p.get("h_nom", 0) or 0),
                                        kw=float(p.get("power_kw", 0) or 0),
                                        page_number=pg,
                                        confidence_q=0.5 if p.get("q_nom") else 0,
                                        confidence_h=0.5 if p.get("h_nom") else 0,
                                        confidence_kw=0.5 if p.get("power_kw") else 0,
                                        source_q="vlm", source_h="vlm", source_kw="vlm",
                                    ))
                                result.pages_processed += 1
                                logger.info("VLM retry pg%d: %d pumps", pg, len(d.get("pumps", [])))
                    except Exception as e:
                        logger.warning("VLM retry pg%d: %s", pg, e)

        finally:
            doc.close()

        return result

    def _merge(self, docling, vlm):
        """Merge: VLM fills Docling zeros.
        Strategy: group by Q+kW, sort by stages (from model name), assign H in order.
        """
        import re
        merged = StageResult(source="docling+vlm")
        vlm_map = {m.key: m for m in vlm.models}

        # Group VLM models by Q+kW for fuzzy matching
        vlm_by_qkw = {}  # (q, kw) -> [models sorted by H]
        for m in vlm.models:
            if m.q > 0 and m.h > 0:
                key = (round(m.q, 1), round(m.kw, 2))
                vlm_by_qkw.setdefault(key, []).append(m)
        # Sort each group by H ascending
        for key in vlm_by_qkw:
            vlm_by_qkw[key].sort(key=lambda m: m.h)

        # Group Docling models by Q+kW, sort by stages from name
        def _extract_stages(model_name):
            """Extract stages number from model name: CMI 1-20 → 20, CMI 1-30 → 30."""
            match = re.search(r'[-]\s*(\d+)', model_name)
            return int(match.group(1)) if match else 0

        doc_by_qkw = {}  # (q, kw) -> [(stages, model)]
        for dm in docling.models:
            if dm.q > 0 and dm.kw > 0 and not dm.h:
                key = (round(dm.q, 1), round(dm.kw, 2))
                stages = _extract_stages(dm.model)
                doc_by_qkw.setdefault(key, []).append((stages, dm))
        # Sort by stages ascending
        for key in doc_by_qkw:
            doc_by_qkw[key].sort(key=lambda x: x[0])

        # Match: for each Q+kW group, pair Docling (by stages asc) with VLM (by H asc)
        filled = 0
        for key, doc_group in doc_by_qkw.items():
            vlm_group = vlm_by_qkw.get(key, [])
            for i, (stages, dm) in enumerate(doc_group):
                if i < len(vlm_group):
                    vm = vlm_group[i]
                    dm.h = vm.h
                    dm.confidence_h = 0.5
                    dm.source_h = "vlm"
                    filled += 1

        logger.info("Merge: filled %d H values via Q+kW+stages matching", filled)

        # Try exact key match for remaining
        seen = set()
        # Build H lookup from already-filled models: model_base → H
        h_lookup = {}  # "CMI1-20" → 18.0
        for dm in docling.models:
            if dm.h > 0:
                # Strip T/variant suffix: "CMI 1-20T-BQCE" → "CMI1-20"
                base = re.sub(r'T?-?BQCE$', '', dm.model.upper().replace(' ', ''))
                h_lookup[base] = dm.h

        for dm in docling.models:
            if not dm.h:
                # Try exact VLM match
                vm = vlm_map.get(dm.key)
                if vm and vm.h:
                    dm.h = vm.h
                    dm.confidence_h = 0.5
                    dm.source_h = "vlm"
                    filled += 1
                else:
                    # Try T-variant: copy H from non-T model with same base name
                    base = re.sub(r'T?-?BQCE$', '', dm.model.upper().replace(' ', ''))
                    if base in h_lookup:
                        dm.h = h_lookup[base]
                        dm.confidence_h = 0.4
                        dm.source_h = "vlm_variant"
                        filled += 1
            merged.models.append(dm)
            seen.add(dm.key)

        logger.info("Merge total: %d H values filled", filled)

        # Add VLM-only models (only if they have actual data)
        for vm in vlm.models:
            if vm.key not in seen and (vm.q > 0 or vm.h > 0 or vm.kw > 0):
                merged.models.append(vm)
                seen.add(vm.key)

        return merged
