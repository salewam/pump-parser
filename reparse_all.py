#!/usr/bin/env python3
"""
Re-parse all PDF catalogs through the new 4-stage pipeline.
Saves results to BASE files + rebuilds brands_index.
"""
import os
import sys
import time
import json
import logging

sys.path.insert(0, "/root/pump_parser")

from pipeline.orchestrator import PipelineOrchestrator
from storage.base_manager import BaseManager
from models.pump_model import detect_series

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reparse")

CATALOGS_DIR = "/root/ONIS/catalogs"


def main():
    po = PipelineOrchestrator()
    bm = BaseManager()

    pdfs = sorted([f for f in os.listdir(CATALOGS_DIR) if f.lower().endswith(".pdf")])
    print(f"\n{'='*70}")
    print(f"RE-PARSE: {len(pdfs)} catalogs through 4-stage pipeline")
    print(f"{'='*70}\n")

    total_models = 0
    total_complete = 0
    total_errors = 0
    results = []

    for i, pdf in enumerate(pdfs, 1):
        pdf_path = os.path.join(CATALOGS_DIR, pdf)
        size_mb = os.path.getsize(pdf_path) / 1024 / 1024
        short = pdf[:50] + "..." if len(pdf) > 50 else pdf

        print(f"[{i}/{len(pdfs)}] {short} ({size_mb:.1f}MB)")

        try:
            t0 = time.time()
            result = po.parse(pdf_path)
            elapsed = time.time() - t0

            # Save to BASE per series
            series_groups = {}
            for m in result.models:
                s = m.series or detect_series(m.model)
                if s and len(s) >= 2:
                    series_groups.setdefault(s.upper(), []).append(m)

            saved_series = []
            for series, models in series_groups.items():
                base_models = []
                for m in models:
                    base_models.append({
                        "id": m.model,
                        "kw": m.kw,
                        "q": m.q,
                        "head_m": m.h,
                        "series": series,
                        "brand": result.brand,
                        "flagship": series in ("MV", "INL", "MBL"),
                        "confidence": m.confidence,
                        "orientation": "horizontal" if series in ("INL", "MBL", "FVH", "FV", "FST", "FS", "FS4", "FSM", "NBS") else "vertical",
                    })
                bm.save(series, base_models)
                saved_series.append(f"{series}({len(base_models)})")

            total_models += result.total_models
            total_complete += result.complete_models

            status = "OK" if not result.errors else f"WARN({len(result.errors)}err)"
            stages = "+".join(result.stages_completed)
            print(f"  {status} {result.total_models} models, {result.complete_models} complete ({result.completeness}%), "
                  f"brand={result.brand}, {elapsed:.0f}s, stages=[{stages}]")
            if saved_series:
                print(f"  Saved: {', '.join(saved_series)}")

            results.append({
                "file": pdf,
                "models": result.total_models,
                "complete": result.complete_models,
                "completeness": result.completeness,
                "brand": result.brand,
                "elapsed": round(elapsed, 1),
                "errors": len(result.errors),
            })

        except Exception as e:
            logger.error("FAILED %s: %s", pdf, e)
            print(f"  FAILED: {e}")
            total_errors += 1
            results.append({"file": pdf, "models": 0, "error": str(e)})

        print()

    # Rebuild index
    bm.rebuild_index()

    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"Catalogs: {len(pdfs)}")
    print(f"Total models: {total_models}")
    print(f"Complete (Q+H+kW): {total_complete}")
    print(f"Completeness: {round(total_complete/total_models*100, 1) if total_models else 0}%")
    print(f"Errors: {total_errors}")
    print()

    # Save report
    report_path = "/root/pump_parser/reparse_report.json"
    with open(report_path, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Report saved: {report_path}")


if __name__ == "__main__":
    main()
