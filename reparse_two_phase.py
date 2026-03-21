#!/usr/bin/env python3
"""
Two-phase parsing: Docling first (all catalogs), then VLM fills gaps.
Avoids VRAM conflict by running sequentially.
"""
import os
import sys
import time
import json
import logging

sys.path.insert(0, "/root/pump_parser")

from pipeline.stage_docling import DoclingStage
from pipeline.stage_vlm import VLMStage
from pipeline.stage_ocr import OCRStage
from pipeline.confidence import ConfidenceScorer
from brand_qualifier import BrandQualifier
from storage.base_manager import BaseManager
from models.parse_result import PumpModelResult, StageResult, ParseResult
from models.pump_model import detect_series
from gpu_manager import stop_docling, start_docling

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CATALOGS_DIR = "/root/ONIS/catalogs"


def main():
    ds = DoclingStage()
    bq = BrandQualifier()
    bm = BaseManager()

    pdfs = sorted(
        [f for f in os.listdir(CATALOGS_DIR) if f.lower().endswith(".pdf")],
        key=lambda f: os.path.getsize(os.path.join(CATALOGS_DIR, f))
    )

    print(f"\n{'='*60}")
    print(f"PHASE 1: Docling (all {len(pdfs)} catalogs)")
    print(f"{'='*60}\n")

    # Phase 1: Docling extracts tables
    docling_results = {}  # pdf -> (StageResult, brand)

    for i, pdf in enumerate(pdfs, 1):
        path = os.path.join(CATALOGS_DIR, pdf)
        short = pdf[:45] + "..." if len(pdf) > 45 else pdf

        br = bq.qualify(path)
        t0 = time.time()
        result = ds.extract(path)
        elapsed = time.time() - t0

        n = len(result.models)
        c = sum(1 for m in result.models if m.is_complete)
        pct = round(c/n*100) if n else 0

        docling_results[pdf] = (result, br.brand, path)
        print(f"[{i:>2}] {short:<48} {br.brand:<10} {n:>3}m {c:>3}c ({pct:>3}%)  {elapsed:>3.0f}s", flush=True)

    total_d = sum(len(r[0].models) for r in docling_results.values())
    total_c = sum(sum(1 for m in r[0].models if m.is_complete) for r in docling_results.values())
    print(f"\nPhase 1 total: {total_d} models, {total_c} complete ({round(total_c/total_d*100) if total_d else 0}%)")

    # Phase 2: VLM fills gaps (Docling stopped)
    print(f"\n{'='*60}")
    print(f"PHASE 2: VLM validation (Docling stopped)")
    print(f"{'='*60}\n")

    print("Stopping Docling...", flush=True)
    stop_docling()
    time.sleep(15)
    print("Waiting for Ollama to load model...", flush=True)
    time.sleep(30)  # Give Ollama time to be ready

    vlm = VLMStage()

    # Only process catalogs with gaps
    for pdf, (docling_result, brand, path) in docling_results.items():
        gaps = [m for m in docling_result.models if not m.is_complete]
        if not gaps:
            continue

        short = pdf[:45] + "..." if len(pdf) > 45 else pdf
        print(f"VLM: {short} ({len(gaps)} gaps)...", flush=True)

        try:
            vlm_result = vlm.process(path, docling_result)

            # Merge VLM into Docling results
            vlm_map = {m.key: m for m in vlm_result.models}
            filled = 0
            for dm in docling_result.models:
                vm = vlm_map.get(dm.key)
                if vm:
                    if not dm.q and vm.q:
                        dm.q = vm.q; dm.source_q = "vlm"; dm.confidence_q = 0.5
                    if not dm.h and vm.h:
                        dm.h = vm.h; dm.source_h = "vlm"; dm.confidence_h = 0.5
                        filled += 1
                    if not dm.kw and vm.kw:
                        dm.kw = vm.kw; dm.source_kw = "vlm"; dm.confidence_kw = 0.5

            print(f"  VLM filled {filled} H values", flush=True)
        except Exception as e:
            print(f"  VLM error: {e}", flush=True)

    # Restart Docling
    print("\nRestarting Docling...", flush=True)
    start_docling()

    # Save results
    print(f"\n{'='*60}")
    print(f"SAVING RESULTS")
    print(f"{'='*60}\n")

    total_m = 0
    total_c2 = 0

    for pdf, (result, brand, path) in docling_results.items():
        if not result.models:
            continue

        # Group by series
        series_groups = {}
        for m in result.models:
            s = m.series or detect_series(m.model)
            if s and len(s) >= 2:
                series_groups.setdefault(s.upper(), []).append(m)

        for series, models in series_groups.items():
            base_models = [{
                "id": m.model, "kw": m.kw, "q": m.q, "head_m": m.h,
                "series": series, "brand": brand,
                "flagship": series in ("MV", "INL", "MBL"),
                "confidence": m.confidence,
                "orientation": "horizontal" if series in ("INL","MBL","FVH","FV","FST","FS","FS4","FSM","NBS") else "vertical",
            } for m in models]
            bm.save(series, base_models)

        n = len(result.models)
        c = sum(1 for m in result.models if m.is_complete)
        total_m += n
        total_c2 += c

    bm.rebuild_index()

    print(f"\nFINAL: {total_m} models, {total_c2} complete ({round(total_c2/total_m*100) if total_m else 0}%)")
    print(f"Was: {total_c}/{total_d} ({round(total_c/total_d*100) if total_d else 0}%) Docling only")
    print(f"Now: {total_c2}/{total_m} ({round(total_c2/total_m*100) if total_m else 0}%) with VLM")


if __name__ == "__main__":
    main()
