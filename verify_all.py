#!/usr/bin/env python3
"""Verify all parsed catalogs: GT coverage, completeness, physics."""
import sys, fitz, re, json, random
sys.path.insert(0, "/root/pump_parser")
from pipeline.stage_docling import DoclingStage

catalogs = [
    ("/root/ONIS/catalogs/Katalog-CMI-21.07.2025.pdf", r"CMI\s+\d+-\d+T?-BQCE"),
    ("/root/ONIS/catalogs/Katalog-NBS-29.09.2025.pdf", r"NBS\s+\d+-\d+-\d+[/_]\d+[.,]?\d*T?_\d+_BQCE"),
    ("/root/ONIS/catalogs/Katalog-TG_-TL_-TD-12.12.2025.pdf", r"(?:TG|TL|TD)\s+\d+-\d+[/_]\S+_BQCE"),
]

ds = DoclingStage()
sep = "=" * 60

for pdf, gt_regex in catalogs:
    name = pdf.split("/")[-1][:35]

    # Ground truth from PDF text
    doc = fitz.open(pdf)
    gt = set()
    for pg in range(doc.page_count):
        for m in re.findall(gt_regex, doc[pg].get_text()):
            gt.add(re.sub(r"\s+", " ", m.strip().replace(",", "."))[:40])
    doc.close()

    # Parse
    r = ds.extract(pdf)
    parsed_keys = set(m.model.replace(",", ".") for m in r.models)

    complete = sum(1 for m in r.models if m.is_complete)
    incomplete = [m for m in r.models if not m.is_complete]

    # Coverage
    found = sum(1 for g in gt if g in parsed_keys)

    # Physics check
    bad_physics = 0
    for m in r.models:
        if m.q > 0 and m.h > 0 and m.kw > 0:
            p_hyd = m.q / 3600 * m.h * 9.81
            eff = p_hyd / m.kw * 100
            if eff > 95 or eff < 5:
                bad_physics += 1

    print(f"\n{sep}", flush=True)
    print(f"{name}", flush=True)
    print(f"  GT models: {len(gt)}", flush=True)
    print(f"  Parsed: {len(r.models)}", flush=True)
    print(f"  Complete: {complete}/{len(r.models)} ({round(complete/len(r.models)*100) if r.models else 0}%)", flush=True)
    print(f"  Coverage: {found}/{len(gt)}", flush=True)
    print(f"  Bad physics: {bad_physics}", flush=True)

    if incomplete:
        print(f"  Incomplete ({len(incomplete)}):", flush=True)
        for m in incomplete[:3]:
            print(f"    {m.model:<30} Q={m.q} H={m.h} kW={m.kw}", flush=True)

    # Spot check 5 random
    complete_models = [m for m in r.models if m.is_complete]
    sample = random.sample(complete_models, min(5, len(complete_models)))
    print(f"  Spot check:", flush=True)
    for m in sample:
        eff = m.q / 3600 * m.h * 9.81 / m.kw * 100 if m.kw else 0
        flag = "OK" if 10 < eff < 90 else "SUS"
        print(f"    {flag} {m.model:<30} Q={m.q:<7} H={m.h:<7} kW={m.kw:<6} eff={eff:.0f}%", flush=True)

print(f"\n{sep}", flush=True)
print("ALL CHECKS DONE", flush=True)
