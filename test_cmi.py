#!/usr/bin/env python3
"""Simple test: parse CMI catalog with Docling + VLM."""
import os, sys, time, json, base64, subprocess
sys.path.insert(0, "/root/pump_parser")

import requests
import fitz
from pipeline.stage_docling import DoclingStage

pdf = "/root/ONIS/catalogs/Katalog-CMI-21.07.2025.pdf"
GPU = "82.22.53.231"
SSH = 'sshpass -p "Kx9#mVp4\\!wL7nQ2z" ssh -o StrictHostKeyChecking=no root@' + GPU

# STEP 1: Docling
print("STEP 1: Docling...", flush=True)
ds = DoclingStage()
doc_r = ds.extract(pdf)
n = len(doc_r.models)
c = sum(1 for m in doc_r.models if m.is_complete)
print(f"  {n} models, {c} complete", flush=True)

# STEP 2: Stop Docling
print("STEP 2: Stop Docling...", flush=True)
subprocess.run(SSH + ' "systemctl stop docling-parser"', shell=True, timeout=15)
time.sleep(10)

# STEP 3: VLM page by page
print("STEP 3: VLM...", flush=True)
doc = fitz.open(pdf)
vlm_models = []

for pg in range(min(len(doc), 20)):
    pix = doc[pg].get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
    b64 = base64.b64encode(pix.tobytes("png")).decode()

    t0 = time.time()
    try:
        r = requests.post(f"http://{GPU}:8000/analyze",
                          data={"image": b64, "task": "extract_pumps"}, timeout=300)
        d = r.json()
        pumps = d.get("pumps", [])
        has_error = bool(d.get("error"))
        elapsed = time.time() - t0

        if pumps and not has_error:
            vlm_models.extend(pumps)
            print(f"  pg{pg}: {len(pumps)} pumps ({elapsed:.0f}s)", flush=True)
        elif has_error:
            print(f"  pg{pg}: loading... ({elapsed:.0f}s)", flush=True)
        else:
            print(f"  pg{pg}: 0 pumps ({elapsed:.0f}s)", flush=True)
    except Exception as e:
        print(f"  pg{pg}: {e}", flush=True)

doc.close()

# STEP 4: Merge
print(f"\nVLM total: {len(vlm_models)} pumps", flush=True)
vlm_map = {}
for p in vlm_models:
    key = p.get("model", "").upper().replace(" ", "")
    if key:
        vlm_map[key] = p

filled = 0
for m in doc_r.models:
    key = m.model.upper().replace(" ", "")
    vp = vlm_map.get(key)
    if vp:
        if not m.h and vp.get("h_nom"):
            m.h = float(vp["h_nom"])
            filled += 1
        if not m.q and vp.get("q_nom"):
            m.q = float(vp["q_nom"])
        if not m.kw and vp.get("power_kw"):
            m.kw = float(vp["power_kw"])

complete = sum(1 for m in doc_r.models if m.is_complete)
print(f"\nFINAL: {n} models, {complete} complete ({round(complete/n*100)}%)", flush=True)
print(f"H filled by VLM: {filled}", flush=True)
for m in doc_r.models[:15]:
    print(f"  {m.model:<25} Q={m.q:<8} H={m.h:<8} kW={m.kw:<6}", flush=True)

# STEP 5: Restart
print("\nRestarting Docling...", flush=True)
subprocess.run(SSH + ' "systemctl restart ollama && sleep 3 && systemctl start docling-parser"', shell=True, timeout=30)
print("Done.", flush=True)
