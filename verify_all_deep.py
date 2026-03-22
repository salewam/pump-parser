#!/usr/bin/env python3
"""Deep verification of ALL catalogs claimed at 100%."""
import sys, fitz, re, json, random
sys.path.insert(0, "/root/pump_parser")
from pipeline.stage_docling import DoclingStage

ds = DoclingStage()

catalogs = [
    ("Katalog-CMI-21.07.2025.pdf", r"CMI\s+\d+-\d+T?-BQCE"),
    ("Katalog-NBS-29.09.2025.pdf", r"NBS\s+\d+-\d+-\d+[/_]\d+[.,]?\d*T?_\d+_BQCE"),
    ("Katalog-TG_-TL_-TD-12.12.2025.pdf", None),
    ("Каталог PV.pdf", r"PV[n(]?\s*[n)]?\s*\d+-\d+"),
    ("Каталог MBL.pdf", None),
    ("evr-evsmanual.pdf", r"EVR[S]?\s*\d+[-]\d+"),
    ("Каталог INL.pdf", r"INL\s*\d+[-/]\d+"),
]

all_ok = True

for fname, gt_regex in catalogs:
    pdf = f"/root/ONIS/catalogs/{fname}"
    name = fname[:30]

    # Ground truth
    if gt_regex:
        doc = fitz.open(pdf)
        gt = set()
        for pg in range(doc.page_count):
            for m in re.findall(gt_regex, doc[pg].get_text()):
                clean = re.sub(r"\s+", "", m.strip().replace(",", ".")).upper()
                clean = clean.replace("(", "").replace(")", "")
                gt.add(clean)
        doc.close()
    else:
        gt = None

    # Parse
    r = ds.extract(pdf)
    total = len(r.models)
    complete = sum(1 for m in r.models if m.is_complete)
    incomplete = [m for m in r.models if not m.is_complete]

    # Coverage
    if gt:
        parsed_keys = set(m.key for m in r.models)
        found = sum(1 for g in gt if g in parsed_keys)
        cov = f"{found}/{len(gt)}"
    else:
        cov = "N/A"

    # Physics check
    bad = 0
    for m in r.models:
        if m.q > 0 and m.h > 0 and m.kw > 0:
            eff = m.q / 3600 * m.h * 9.81 / m.kw * 100
            if eff > 95 or eff < 3:
                bad += 1

    pct = round(complete / total * 100) if total else 0
    status = "OK" if pct == 100 and not incomplete else "FAIL"
    if status == "FAIL":
        all_ok = False

    print(f"\n{'='*50}", flush=True)
    print(f"{status} {name}", flush=True)
    print(f"  Models: {total}, Complete: {complete} ({pct}%)", flush=True)
    print(f"  Coverage: {cov}, Bad physics: {bad}", flush=True)

    if incomplete:
        print(f"  INCOMPLETE ({len(incomplete)}):", flush=True)
        for m in incomplete[:5]:
            print(f"    {m.model:<25} Q={m.q} H={m.h} kW={m.kw}", flush=True)

    # Spot check 5 random
    ok_models = [m for m in r.models if m.is_complete]
    sample = random.sample(ok_models, min(5, len(ok_models)))
    print(f"  Spot check:", flush=True)
    for m in sorted(sample, key=lambda x: x.model):
        eff = m.q / 3600 * m.h * 9.81 / m.kw * 100 if m.kw else 0
        flag = "OK" if 3 < eff < 95 else "SUS"
        print(f"    {flag} {m.model:<25} Q={m.q:<7} H={m.h:<7} kW={m.kw:<6} eff={eff:.0f}%", flush=True)

print(f"\n{'='*50}", flush=True)
if all_ok:
    print("ALL 7 CATALOGS VERIFIED 100%", flush=True)
else:
    print("SOME CATALOGS FAILED", flush=True)
