"""
Confidence scoring: merge 3 sources (Docling, VLM, OCR) into final result.
"""
import logging

import sys
sys.path.insert(0, "/root/pump_parser")
from models.parse_result import PumpModelResult, StageResult, ParseResult

logger = logging.getLogger(__name__)

AGREE_THRESHOLD = 0.05  # 5% tolerance


class ConfidenceScorer:
    """Merge and score models from 3 pipeline stages."""

    def merge_all(self, docling: StageResult, vlm: StageResult, ocr: StageResult) -> ParseResult:
        """Merge models from 3 sources by key. Compute per-value confidence."""
        # Build key → model maps
        d_map = {m.key: m for m in docling.models} if docling else {}
        v_map = {m.key: m for m in vlm.models} if vlm else {}
        o_map = {m.key: m for m in ocr.models} if ocr else {}

        all_keys = set(d_map) | set(v_map) | set(o_map)
        merged = []

        for key in sorted(all_keys):
            dm = d_map.get(key)
            vm = v_map.get(key)
            om = o_map.get(key)

            # Base model: prefer Docling for metadata (series, rpm, page)
            base = dm or vm or om

            # Merge each value
            q, conf_q, src_q = self._merge_value(
                dm.q if dm else 0, vm.q if vm else 0, om.q if om else 0)
            h, conf_h, src_h = self._merge_value(
                dm.h if dm else 0, vm.h if vm else 0, om.h if om else 0)
            kw, conf_kw, src_kw = self._merge_value(
                dm.kw if dm else 0, vm.kw if vm else 0, om.kw if om else 0)

            merged.append(PumpModelResult(
                model=base.model,
                series=base.series or (dm.series if dm else "") or (vm.series if vm else ""),
                q=q, h=h, kw=kw,
                rpm=base.rpm,
                page_number=base.page_number,
                confidence_q=conf_q, confidence_h=conf_h, confidence_kw=conf_kw,
                source_q=src_q, source_h=src_h, source_kw=src_kw,
            ))

        stages = []
        if docling and docling.models:
            stages.append("docling")
        if vlm and vlm.models:
            stages.append("vlm")
        if ocr and ocr.models:
            stages.append("ocr")

        return ParseResult(models=merged, stages_completed=stages)

    def _merge_value(self, d_val, v_val, o_val):
        """Merge a single value from 3 sources.
        Returns (value, confidence, source_label).
        """
        d = d_val or 0
        v = v_val or 0
        o = o_val or 0

        has_d = d > 0
        has_v = v > 0
        has_o = o > 0

        # All 3 agree
        if has_d and has_v and has_o:
            if self._agree(d, v) and self._agree(d, o):
                return d, 1.0, "docling+vlm+ocr"
            if self._agree(d, o):
                return d, 0.8, "docling+ocr"
            if self._agree(v, o):
                return v, 0.7, "vlm+ocr"
            if self._agree(d, v):
                return d, 0.8, "docling+vlm"
            # All disagree — OCR wins
            return o, 0.4, "ocr(conflict)"

        # 2 sources
        if has_d and has_v:
            if self._agree(d, v):
                return d, 0.8, "docling+vlm"
            return d, 0.5, "docling(conflict)"
        if has_d and has_o:
            if self._agree(d, o):
                return d, 0.8, "docling+ocr"
            return o, 0.5, "ocr(conflict)"
        if has_v and has_o:
            if self._agree(v, o):
                return v, 0.7, "vlm+ocr"
            return o, 0.5, "ocr(conflict)"

        # 1 source
        if has_d:
            return d, 0.6, "docling"
        if has_v:
            return v, 0.5, "vlm"
        if has_o:
            return o, 0.5, "ocr"

        # Nothing
        return 0, 0.0, ""

    @staticmethod
    def _agree(a, b):
        """Check if two values agree within AGREE_THRESHOLD (5%)."""
        if a == 0 or b == 0:
            return a == b
        return abs(a - b) / max(abs(a), abs(b)) <= AGREE_THRESHOLD
