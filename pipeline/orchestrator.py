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
                # Docling doesn't report page numbers — scan all pages
                pages_to_check = list(range(min(len(doc), 25)))

            ollama_ready = False

            for pg in pages_to_check:
                if pg >= len(doc):
                    continue
                try:
                    pix = doc[pg].get_pixmap(matrix=__import__("fitz").Matrix(1.5, 1.5))
                    b64 = base64.b64encode(pix.tobytes("png")).decode()

                    task = "extract_pumps"

                    # Retry loop: Ollama needs time to load model (~30-60s)
                    d = None
                    for attempt in range(6 if not ollama_ready else 1):
                        r = requests.post(
                            f"http://{GPU_HOST}:8000/analyze",
                            data={"image": b64, "task": task},
                            timeout=300,
                        )
                        if r.status_code != 200:
                            if not ollama_ready:
                                time.sleep(15)
                            continue

                        d = r.json()
                        if d.get("error") and not ollama_ready:
                            logger.info("VLM pg%d: model loading (attempt %d)...", pg, attempt + 1)
                            time.sleep(15)
                            continue
                        break

                    if not d or d.get("error"):
                        continue

                    ollama_ready = True

                    pumps = d.get("pumps", [])
                    if pumps:
                        for p in pumps:
                            name = str(p.get("model", "")).strip()
                            # Filter: must be real pump model name (not just a number or 2-letter code)
                            if not name or len(name) < 5 or name.isdigit():
                                continue
                            result.models.append(PumpModelResult(
                                model=name,
                                series=detect_series(name),
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

        # Also try exact key match for any remaining
        seen = set()
        for dm in docling.models:
            if not dm.h:
                vm = vlm_map.get(dm.key)
                if vm and vm.h:
                    dm.h = vm.h
                    dm.confidence_h = 0.5
                    dm.source_h = "vlm"
            merged.models.append(dm)
            seen.add(dm.key)

        # Add VLM-only models
        for vm in vlm.models:
            if vm.key not in seen:
                merged.models.append(vm)
                seen.add(vm.key)

        return merged
