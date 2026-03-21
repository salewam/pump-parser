"""
Pipeline Orchestrator: coordinates 4 stages sequentially.
Docling → VLM → OCR → Self-correction → Brand → Done.
"""
import time
import logging

import sys
sys.path.insert(0, "/root/pump_parser")
from models.parse_result import ParseResult, StageResult
from pipeline.stage_docling import DoclingStage
from pipeline.stage_vlm import VLMStage
from pipeline.stage_ocr import OCRStage
from pipeline.stage_selfcorrect import SelfCorrectionStage
from pipeline.confidence import ConfidenceScorer
from brand_qualifier import BrandQualifier
from gpu_manager import stop_docling, start_docling

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Coordinates 4-stage parse pipeline."""

    def __init__(self):
        self.docling = DoclingStage()
        self.vlm = VLMStage()
        self.ocr = OCRStage()
        self.selfcorrect = SelfCorrectionStage(vlm_stage=self.vlm)
        self.scorer = ConfidenceScorer()
        self.brand_qualifier = BrandQualifier()

    def parse(self, pdf_path: str, progress_cb=None) -> ParseResult:
        """Run full 4-stage pipeline.
        Args:
            pdf_path: path to PDF catalog
            progress_cb: optional callback(phase: str, pct: int)
        Returns: ParseResult with models, brand, confidence scores.
        """
        t0 = time.time()

        def _progress(phase, pct):
            if progress_cb:
                try:
                    progress_cb(phase, pct)
                except Exception:
                    pass

        # ── Stage 1: Docling ────────────────────────────────────────
        _progress("Docling: извлечение таблиц...", 10)
        logger.info("Stage 1: Docling starting for %s", pdf_path)

        docling_result = self.docling.extract(pdf_path)

        logger.info("Stage 1 done: %d models, %d tables, errors=%s",
                     len(docling_result.models), len(docling_result.raw_tables), docling_result.errors)

        # ── Stage 2: VLM ────────────────────────────────────────────
        _progress("VLM: освобождение GPU...", 30)
        logger.info("Stage 2: stopping Docling to free VRAM")
        stop_docling()

        _progress("VLM: валидация и дополнение...", 35)
        logger.info("Stage 2: VLM starting")

        vlm_result = StageResult(source="vlm")
        try:
            vlm_result = self.vlm.process(pdf_path, docling_result)
        except Exception as e:
            logger.error("Stage 2 VLM error: %s", e)
            vlm_result.errors.append(str(e))
        finally:
            _progress("Перезапуск Docling...", 50)
            start_docling()

        logger.info("Stage 2 done: %d models, errors=%s",
                     len(vlm_result.models), vlm_result.errors)

        # ── Merge Docling + VLM ─────────────────────────────────────
        _progress("Объединение результатов...", 55)
        merged = self._merge_stages(docling_result, vlm_result)

        # ── Stage 3: OCR Verification ───────────────────────────────
        _progress("OCR: верификация чисел...", 65)
        logger.info("Stage 3: OCR verification starting")

        ocr_result = StageResult(source="ocr")
        try:
            ocr_result = self.ocr.verify(pdf_path, merged)
        except Exception as e:
            logger.error("Stage 3 OCR error: %s", e)
            ocr_result.errors.append(str(e))

        logger.info("Stage 3 done: %d verified, errors=%s",
                     len(ocr_result.models), ocr_result.errors)

        # ── Confidence scoring ──────────────────────────────────────
        _progress("Расчёт confidence...", 75)
        result = self.scorer.merge_all(docling_result, vlm_result, ocr_result)

        # ── Stage 4: Self-correction ────────────────────────────────
        _progress("Self-correction: поиск пропущенных...", 80)
        logger.info("Stage 4: Self-correction starting")

        gaps = [m for m in result.models if not m.is_complete]
        if gaps:
            try:
                self.selfcorrect.fill_gaps(pdf_path, gaps)
                result.stages_completed.append("selfcorrect")
            except Exception as e:
                logger.error("Stage 4 self-correct error: %s", e)
                result.errors.append(f"Self-correct: {e}")

        logger.info("Stage 4 done: %d/%d complete", result.complete_models, result.total_models)

        # ── Brand qualification ─────────────────────────────────────
        _progress("Определение бренда...", 90)

        try:
            # Convert models to dicts for brand_qualifier
            model_dicts = [{"model": m.model, "id": m.model, "series": m.series} for m in result.models]
            br = self.brand_qualifier.qualify_full(pdf_path, model_dicts)
            result.brand = br.brand
            result.brand_confidence = br.confidence
            result.brand_source = br.source
            result.series_detected = br.series_detected
        except Exception as e:
            logger.warning("Brand qualification error: %s", e)
            result.brand = "Unknown"

        # ── Finalize ────────────────────────────────────────────────
        result.elapsed = round(time.time() - t0, 1)

        # Collect all errors
        for stage in [docling_result, vlm_result, ocr_result]:
            result.errors.extend(stage.errors)

        _progress("Готово!", 100)
        logger.info("Pipeline complete: %d models (%d complete, %.0f%%), brand=%s, %.1fs",
                     result.total_models, result.complete_models, result.completeness,
                     result.brand, result.elapsed)

        return result

    def _merge_stages(self, docling: StageResult, vlm: StageResult) -> StageResult:
        """Intermediate merge: VLM fills Docling zeros."""
        merged = StageResult(source="docling+vlm")

        # Build VLM map by key
        vlm_map = {m.key: m for m in vlm.models}

        # Start with Docling models
        seen = set()
        for dm in docling.models:
            vm = vlm_map.get(dm.key)
            if vm:
                # Fill zeros from VLM
                if not dm.q and vm.q:
                    dm.q = vm.q
                    dm.confidence_q = vm.confidence_q
                    dm.source_q = vm.source_q
                if not dm.h and vm.h:
                    dm.h = vm.h
                    dm.confidence_h = vm.confidence_h
                    dm.source_h = vm.source_h
                if not dm.kw and vm.kw:
                    dm.kw = vm.kw
                    dm.confidence_kw = vm.confidence_kw
                    dm.source_kw = vm.source_kw
            merged.models.append(dm)
            seen.add(dm.key)

        # Add VLM-only models (not in Docling)
        for vm in vlm.models:
            if vm.key not in seen:
                merged.models.append(vm)
                seen.add(vm.key)

        return merged
