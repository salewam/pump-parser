#!/usr/bin/env python3
"""
PDF Парсер ONIS v2 — Docling GPU + Flask UI
GPU backend: 82.22.53.231:5001 (Docling + TableFormer)
Bot DB: /root/pump_base/{TYPE}_BASE.json
"""

from flask import Flask, request, redirect, url_for, flash, session, send_file, send_from_directory, jsonify, render_template_string
import os
import json
import re
from werkzeug.utils import secure_filename
from datetime import datetime
import sys
import threading
import uuid
import time
import requests
from brand_qualifier import BrandQualifier, brand_for_series

brand_qualifier = BrandQualifier()

app = Flask(__name__)
app.secret_key = 'cdm-parser-super-secret-key-2026'
app.config['UPLOAD_FOLDER'] = '/root/pump_parser/uploads'
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB for batch

# GPU сервер с Docling
GPU_SERVER = 'http://82.22.53.231:5001'

# Серверное хранилище задач
parse_tasks = {}
_TASKS_FILE = '/root/pump_parser/uploads/parse_tasks.json'

def _save_tasks():
    try:
        # Only save completed/error tasks (not in-progress)
        saveable = {}
        for tid, t in parse_tasks.items():
            if t.get('status') in ('done', 'error'):
                saveable[tid] = {k: v for k, v in t.items() if k != 'parsed_data'}
        with open(_TASKS_FILE, 'w') as f:
            json.dump(saveable, f, ensure_ascii=False)
    except Exception:
        pass

def _load_tasks():
    try:
        if os.path.exists(_TASKS_FILE):
            with open(_TASKS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

parse_tasks = _load_tasks()

# ======== DOCLING TABLE → PUMP MODEL CONVERTER ========

def detect_series(model_name):
    """CDM/CDMF 1-5 → CDM 1"""
    m = re.match(r'(CDM|CDMF|CV|CVF|CMI|NBS|TG|TL|TD|FST|FS4|FSM|FV|FVH|EVR|EVS|CDL|CDLF)\s*[/]?\s*\w*\s*(\d+)', model_name, re.I)
    if m:
        prefix = m.group(1).upper()
        num = m.group(2)
        return f"{prefix} {num}"
    return model_name.split('-')[0].split()[0] if model_name else ''


def parse_number(val):
    """Parse number from table cell: '1,5' → 1.5, '12.5 м³/ч' → 12.5"""
    if not val:
        return None
    val = str(val).strip()
    val = val.lstrip('~>< ≈≥≤')  # strip prefixes like ~12.5, >10
    # Handle thousand separators: 1,234.5 or 1 234.5
    if re.match(r'^\d{1,3}(,\d{3})+(\.\d+)?$', val):
        val = val.replace(',', '')
    if not val:
        return None
    # Skip if looks like model name or text
    if re.match(r'[A-ZА-Яa-zа-я]{2}', val):
        return None
    # Handle ranges: take first number from "12.5-15.0" or "12.5...15"
    val = val.replace(',', '.')
    if 'e' in val.lower() and any(c.isdigit() for c in val):
        try:
            n = float(val)
            if abs(n) > 1e6:
                return None
            return n
        except ValueError:
            return None
    val = re.split(r'[-–—~…]', val)[0].strip()
    # Handle fractions like "1.5/2" — take numerator only
    if '/' in val:
        val = val.split('/')[0].strip()
    # Strip units and non-numeric chars
    val = re.sub(r'[^\d.]', '', val)
    # Handle multiple dots: "12.515.0" → invalid
    if val.count('.') > 1:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def tables_to_pump_models(tables):
    """Convert Docling tables → [{model, series, q_nom, h_nom, power_kw, rpm}]"""
    models = []
    seen = set()

    for table in tables:
        if table.get('type') != 'pump_data':
            continue

        cols = table.get('columns', [])
        data = table.get('data', [])
        if not data or not cols:
            continue
        # Guard against empty/None values in columns
        cols = [str(c) if c else '' for c in cols]
        data = [row for row in data if row and isinstance(row, dict)]
        if not data:
            continue

        cols_lower = [str(c).lower().replace('\n', ' ').strip() for c in cols]
        cols_str = ' '.join(cols_lower)

        # Try to identify column mapping
        model_col = None
        q_col = None
        h_col = None
        kw_col = None
        rpm_col = None

        for c in cols:
            cl = str(c).lower().replace('\n', ' ').strip()
            if any(k in cl for k in ['модель', 'model', 'тип', 'type', 'наименование', 'насос']):
                model_col = c
            elif any(k in cl for k in ['подача', 'расход', 'производительн', 'qном', 'q ном', 'q,', 'q макс', 'q ', 'q(', 'm3/h', 'м3/ч', 'м³/ч', 'flow', 'capacity']):
                q_col = c
            elif any(k in cl for k in ['напор', 'нном', 'н ном', 'head', 'h,', 'h ', 'h(', 'давлен']):
                h_col = c
            elif any(k in cl for k in ['мощность', 'kw', 'квт', 'р2', 'p2', 'power', 'мощн']):
                kw_col = c
            elif any(k in cl for k in ['об/мин', 'rpm', 'частота вращения', 'n,']):
                rpm_col = c

        # Auto-detect columns via LLM if headers didn't match
        if not q_col and not h_col and not kw_col and len(data) >= 3:
            mapping = llm_classify_columns(cols, data[:3])
            if mapping:
                if 'model' in mapping and not model_col:
                    model_col = mapping['model']
                if 'q' in mapping:
                    q_col = mapping['q']
                if 'h' in mapping:
                    h_col = mapping['h']
                if 'kw' in mapping:
                    kw_col = mapping['kw']
                if 'rpm' in mapping:
                    rpm_col = mapping['rpm']

        # Strategy 1: Direct column mapping
        if model_col and (q_col or h_col or kw_col):
            for row in data:
                model_name = str(row.get(model_col, '')).strip()
                if not model_name or len(model_name) < 2:
                    continue

                q = parse_number(row.get(q_col)) if q_col else None
                h = parse_number(row.get(h_col)) if h_col else None
                kw = parse_number(row.get(kw_col)) if kw_col else None
                rpm_val = parse_number(row.get(rpm_col)) if rpm_col else 2900

                if q is not None or h is not None or kw is not None:
                    key = f"{model_name}|{q}|{h}|{kw}"
                    if key not in seen:
                        seen.add(key)
                        models.append({
                            'model': model_name,
                            'series': detect_series(model_name),
                            'q_nom': q or 0,
                            'h_nom': h or 0,
                            'power_kw': kw or 0,
                            'rpm': int(rpm_val) if rpm_val else 2900
                        })
            continue

        # Strategy 2: Multi-column spec tables (CDM style)
        # Rows like: "Номинальная подача (м³/ч)" | "1" | "3" | "5" | ...
        # Headers contain model names as columns
        header_models = []
        for c in cols:
            cs = str(c).strip()
            if re.search(r'(CDM|CDMF|CV|CVF|CMI|NBS|TG|TL|TD|FST|FS|CDL|CDLF|FV|FVH|EVR|EVS)', cs, re.I):
                header_models.append(c)

        if header_models and len(data) >= 3:
            spec_map = {}  # col → {q, h, kw, rpm}
            for mc in header_models:
                spec_map[mc] = {'q': None, 'h': None, 'kw': None, 'rpm': None}

            for row in data:
                # Find the label column (first non-model column)
                label = ''
                for c in cols:
                    if c not in header_models:
                        label = str(row.get(c, '')).lower()
                        break

                for mc in header_models:
                    val = parse_number(row.get(mc))
                    if val is None:
                        continue
                    if any(k in label for k in ['подача', 'расход', 'производительн', 'q', 'm3', 'м3', 'м³', 'flow']):
                        spec_map[mc]['q'] = val
                    elif any(k in label for k in ['напор', 'head', 'h ']):
                        spec_map[mc]['h'] = val
                    elif any(k in label for k in ['мощность', 'kw', 'квт', 'power']):
                        spec_map[mc]['kw'] = val
                    elif any(k in label for k in ['об/мин', 'rpm', 'частота']):
                        spec_map[mc]['rpm'] = val

            for mc in header_models:
                specs = spec_map[mc]
                model_name = str(mc).strip()
                if specs['q'] or specs['h'] or specs['kw']:
                    key = f"{model_name}|{specs['q']}|{specs['h']}|{specs['kw']}"
                    if key not in seen:
                        seen.add(key)
                        models.append({
                            'model': model_name,
                            'series': detect_series(model_name),
                            'q_nom': specs['q'] or 0,
                            'h_nom': specs['h'] or 0,
                            'power_kw': specs['kw'] or 0,
                            'rpm': int(specs['rpm']) if specs['rpm'] else 2900
                        })
            continue

        # Strategy 3: Scan all cells for pump model patterns + nearby numbers
        _PUMP_RE = r'(CDM|CDMF|CV|CVF|CMI|NBS|TG|TL|TD|FST|FS4|FSM|FV|FVH|EVR|EVS|CDL|CDLF|INL|MBL|МВL|МBL|CHL|CHLF|BM|BMN)\s*[/]?\s*\w*\s*\d+[\s-]*\d*'
        for row in data:
            vals = list(row.values())
            for i, v in enumerate(vals):
                vs = str(v).strip()
                m = re.match(_PUMP_RE, vs, re.I)
                if m:
                    model_name = m.group(0).strip()
                    nums = []
                    for j in range(i+1, min(i+5, len(vals))):
                        n = parse_number(vals[j])
                        if n is not None:
                            nums.append(n)

                    if len(nums) >= 2:
                        key = f"{model_name}|{nums}"
                        if key not in seen:
                            seen.add(key)
                            # Heuristic: first number = Q or kW, depends on magnitude
                            q = nums[0] if len(nums) > 0 else 0
                            h = nums[1] if len(nums) > 1 else 0
                            kw = nums[2] if len(nums) > 2 else 0
                            models.append({
                                'model': model_name,
                                'series': detect_series(model_name),
                                'q_nom': q,
                                'h_nom': h,
                                'power_kw': kw,
                                'rpm': 2900
                            })

        # Strategy 4: Model name embedded in column headers (no model column)
        # Some catalogs have pump name as a column header, specs in rows
        if not model_col:
            _HEADER_RE = r'(CDM|CDMF|CV|CVF|CMI|NBS|TG|TL|TD|FST|FS4|FSM|FV|FVH|EVR|EVS|CDL|CDLF|INL|MBL|МВL|МBL|CHL|CHLF|BM|BMN)\s*[/]?\s*\w*\s*\d+[\s-]*[\d./]*'
            for c in cols:
                cs = str(c).strip()
                hm = re.match(_HEADER_RE, cs, re.I)
                if hm:
                    model_name = hm.group(0).strip()
                    # Gather all numbers from data rows under this column
                    nums = []
                    for row in data[:10]:
                        n = parse_number(row.get(c))
                        if n is not None:
                            nums.append(n)
                    if nums and model_name:
                        key = f"hdr|{model_name}"
                        if key not in seen:
                            seen.add(key)
                            models.append({
                                'model': model_name,
                                'series': detect_series(model_name),
                                'q_nom': 0, 'h_nom': 0,
                                'power_kw': 0, 'rpm': 2900
                            })

    # Split merged cells: "МВL 32-200-4/2 МВL 32-200-5.5/2" → two models
    _SPLIT_RE = r'((?:CDM|CDMF|CV|CVF|CMI|NBS|TG|TL|TD|FST|FS4|FSM|FV|FVH|EVR|EVS|CDL|CDLF|INL|MBL|МВL|МBL|CHL|CHLF|BM|BMN)\s*[/]?\s*\w*\s*\d+[\s-]*[\d.]+(?:/\d+)?)'
    split_models = []
    for m in models:
        name = m['model']
        parts = re.findall(_SPLIT_RE, name, re.I)
        if len(parts) > 1:
            # Merged cell detected — split into individual models
            for part in parts:
                part = part.strip()
                if part and part not in [p['model'] for p in split_models]:
                    split_models.append({
                        'model': part,
                        'series': detect_series(part),
                        'q_nom': 0, 'h_nom': 0, 'power_kw': 0, 'rpm': 2900
                    })
        else:
            split_models.append(m)
    models = split_models

    # Post-process: extract Q/H/kW from model name — ALWAYS for known patterns
    # Model name is authoritative for INL/MBL (table values are often wrong)
    for m in models:
        _enrich_from_model_name(m)

    # Validate physics: catch garbage values from table parsing
    for m in models:
        _validate_pump_physics(m)

    return models


def _validate_pump_physics(m):
    """Flag/reset values that are physically impossible for centrifugal pumps.
    Based on real pump engineering limits:
    - Q > 500 m3/h only for DN150+ industrial pumps (kW > 30)
    - H > 200m only for multistage high-power pumps (kW > 15)
    - kW and H/Q must be proportional (hydraulic power = Q*H*rho*g / efficiency)
    """
    q = m.get('q_nom', 0)
    h = m.get('h_nom', 0)
    kw = m.get('power_kw', 0)

    # Absolute limits
    if q > 1000 or h > 500 or kw > 500:
        m['q_nom'] = 0
        m['h_nom'] = 0
        m['power_kw'] = 0
        return

    # Cross-check: hydraulic power P_hyd = Q(m3/s) * H(m) * 9810 / 1000
    # Pump efficiency 40-80%, so kW should be >= P_hyd / 0.8
    # If kW is known and Q*H implies 10x more power — table values are garbage
    if kw > 0 and q > 0 and h > 0:
        p_hyd = (q / 3600) * h * 9.81  # kW
        ratio = p_hyd / kw
        if ratio > 3.0:  # Q*H requires 3x more power than stated — values are wrong
            m['q_nom'] = 0
            m['h_nom'] = 0


def _enrich_from_model_name(m):
    """Extract Q, H, kW from ONIS model naming conventions.
    INL{DN}-{Q}-{H}-{kW}/{poles}  e.g. INL32-12.5-18-1.1/2
    MBL {DN}-{H}-{kW}/{poles}     e.g. MBL 32-160-1.5/2
    For known patterns: OVERRIDE table values (model name is truth).
    For unknown patterns: only fill zeros.
    """
    if not m.get('model'):
        return
    name = m['model'].replace(',', '.').strip()
    name = name.replace('\u200b', '').replace('\u200c', '').replace('\ufeff', '')
    name = name.replace('\u2013', '-').replace('\u2014', '-').replace('\u2012', '-')
    m['model'] = name  # update with cleaned name

    # INL pattern: INL{DN}-{Q}-{H}-{kW}/{poles}
    match = re.match(r'INL\s*(\d+)\s*[-]\s*([\d.]+)\s*[-]\s*([\d.]+)\s*[-]\s*([\d.]+)\s*/\s*(\d+)', name, re.I)
    if match:
        m['q_nom'] = float(match.group(2))
        m['h_nom'] = float(match.group(3))
        m['power_kw'] = float(match.group(4))
        poles = int(match.group(5))
        m['rpm'] = 3000 // (poles // 2) if poles >= 2 else 2900
        return

    # MBL/МВL pattern: MBL {DN}-{H}[A]-{kW}/{poles} (kW and poles optional)
    match = re.match(r'[МM][ВB]L\s*(\d+)\s*[-]\s*([\d.]+)[A-Za-zА-Яа-я]*(?:\s*[-]\s*([\d.]+)\s*(?:/\s*(\d+))?)?', name, re.I)
    if match:
        dn = int(match.group(1))
        imp_mm = float(match.group(2))
        # Second number is impeller diameter (mm), NOT head
        import math as _math
        _u2 = _math.pi * (imp_mm / 1000) * (m['rpm'] or 2900) / 60
        m['h_nom'] = round(_u2**2 / 9.81 * 0.55, 1)
        if match.group(3):
            m['power_kw'] = float(match.group(3))
        if match.group(4):
            poles = int(match.group(4))
            m['rpm'] = 3000 // (poles // 2) if poles >= 2 else 2900
        dn_q = {32: 6.3, 40: 12.5, 50: 25, 65: 50, 80: 50, 100: 100, 125: 160, 150: 200, 200: 400}
        m['q_nom'] = dn_q.get(dn, dn * 0.5)
        return

    # FV/FVH pattern: FV {Q}x{stages}/{kW} or FVH{Q}x{stages}/{kW}
    fv_match = re.match(r'(?:FV|FVH)\s*(\d+)[x×](\d+)[/]?([\d.]*)', name, re.I)
    if fv_match and not m['q_nom']:
        m['q_nom'] = float(fv_match.group(1))
        if fv_match.group(3):
            try:
                m['power_kw'] = float(fv_match.group(3))
            except:
                pass

    # TL/TG/TD pattern: TL {DN}-{stages} - use DN->Q map
    tl_match = re.match(r'(?:TL|TG|TD)\s*(\d+)\s*[-]\s*(\d+)', name, re.I)
    if tl_match and not m['q_nom']:
        dn = int(tl_match.group(1))
        TL_DN_Q = {25: 6, 32: 12.5, 40: 20, 50: 30, 65: 50, 80: 80, 100: 120, 125: 180, 150: 250, 200: 400}
        m['q_nom'] = TL_DN_Q.get(dn, dn * 0.5)

    # FST/FS/FS4/FSM pattern: FST {DN}-{impeller}/{kW}
    fst_match = re.match(r'(?:FST4|FST|FS4|FSM|FS)\s*(\d+)\s*[-]\s*(\d+)(?:\s*/\s*([\d.]+))?', name, re.I)
    if fst_match and not m['q_nom']:
        dn = int(fst_match.group(1))
        FST_DN_Q = {25: 3, 32: 6.3, 40: 12.5, 50: 25, 65: 50, 80: 50, 100: 100, 125: 160, 150: 200}
        m['q_nom'] = FST_DN_Q.get(dn, dn * 0.5)
        if fst_match.group(3) and not m['power_kw']:
            try:
                kw_val = float(fst_match.group(3))
                if kw_val <= 200:  # >200 is likely article number, not kW
                    m['power_kw'] = kw_val
            except:
                pass

    # PV pattern: PV(n) {Q}-{stages} or PV {Q}-{stages}
    pv_match = re.match(r'PV\s*\(?n?\)?\s*(\d+)\s*[-]\s*(\d+)', name, re.I)
    if pv_match and not m['q_nom']:
        m['q_nom'] = float(pv_match.group(1))

    # CDM/CDMF/CDL/CDLF pattern: CDM{Q}-{stages}
    cdm_match = re.match(r'(?:CDM|CDMF|CDL|CDLF)\s*F?\s*(\d+)\s*[-]\s*(\d+)', name, re.I)
    if cdm_match and not m['q_nom']:
        m['q_nom'] = float(cdm_match.group(1))

    # CV/CVF pattern: CV {Q}-{stages}-{variant}
    cv_match = re.match(r'(?:CV|CVF)\s*(\d+)\s*[-]\s*(\d+)', name, re.I)
    if cv_match and not m['q_nom']:
        m['q_nom'] = float(cv_match.group(1))

    # EVR/EVS pattern: {Q}--{stages} or EVR(S){Q}-{stages}
    if not m['q_nom']:
        evr_match = re.match(r'(?:EVR|EVS)\(?S?\)?\s*(\d+)\s*[-]+\s*(\d+)', name, re.I)
        if not evr_match:
            evr_match = re.match(r'(\d+)\s*[-]{1,}\s*(\d+)', name)
        if evr_match:
            q_val = float(evr_match.group(1))
            if q_val <= 200:  # reasonable Q range
                m['q_nom'] = q_val

    # Generic: only fill zeros
    if m['q_nom'] != 0 and m['power_kw'] != 0:
        return
    match = re.match(r'[A-ZА-Яa-zа-я]+\s*([\d.]+)\s*[-]\s*([\d.]+)\s*[-]\s*([\d.]+)\s*(?:/\s*(\d+))?', name)
    if match:
        nums = [float(match.group(i)) for i in (1, 2, 3) if match.group(i)]
        if len(nums) >= 3:
            if m['power_kw'] == 0 and min(nums) < 50:
                m['power_kw'] = min(nums)
        if match.group(4):
            poles = int(match.group(4))
            m['rpm'] = 3000 // (poles // 2) if poles >= 2 else 2900


def _normalize_model_key(name):
    """Normalize model name for dedup: strip spaces, trailing .0, lowercase"""
    if not name:
        return ''
    k = name.strip()
    k = re.sub(r'\.0(?=[/\s-]|$)', '', k)  # 3.0/2 → 3/2
    k = re.sub(r'\s+', ' ', k)  # multiple spaces → one
    k = re.sub(r'([A-Za-zА-я])\s+(\d)', r'\1\2', k)  # INL 32 → INL32
    k = k.replace('\u041c', 'M').replace('\u0412', 'B')  # Cyrillic → Latin
    k = k.replace('\u200b', '').replace('\u200c', '').replace('\ufeff', '')  # zero-width chars
    k = k.replace('\u2013', '-').replace('\u2014', '-').replace('\u2012', '-')  # en/em dash → hyphen
    return k
def detect_catalog_type(models):
    """Detect catalog type from model names"""
    if not models:
        return 'PUMP'
    all_names = ' '.join(m['model'] for m in models).upper()
    for prefix in ['CDMF', 'CDM', 'CDLF', 'CDL', 'CVF', 'CV', 'CMI', 'NBS', 'TD', 'TG', 'TL',
                    'FST', 'FS4', 'FSM', 'FV', 'FVH', 'EVR', 'EVS']:
        if prefix in all_names:
            # Group related types
            if prefix in ('CDM', 'CDMF'):
                return 'CDM_CDMF'
            elif prefix in ('CDL', 'CDLF'):
                return 'CDL_CDLF'
            elif prefix in ('CV', 'CVF'):
                return 'CV_CVF'
            elif prefix in ('TG', 'TL', 'TD'):
                return 'TG_TL_TD'
            elif prefix in ('FST', 'FS4', 'FSM'):
                return 'FST_FS'
            elif prefix in ('FV', 'FVH'):
                return 'FV_FVH'
            elif prefix in ('EVR', 'EVS'):
                return 'EVR_EVS'
            return prefix
    return 'PUMP'


# ======== HTML TEMPLATE ========

HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PDF Парсер ONIS v2</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #0a0e17 0%, #1a1f2e 100%);
            color: #e0e0e0;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1100px; margin: 0 auto; }
        header {
            text-align: center; margin-bottom: 25px; padding: 25px;
            background: rgba(255,255,255,0.04); border-radius: 12px;
            border: 1px solid rgba(46,184,170,0.2);
        }
        h1 { font-size: 2em; background: linear-gradient(135deg, #2EB8AA, #1A9E8F);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 8px; }
        .subtitle { color: #777; font-size: 0.95em; }
        .gpu-badge {
            display: inline-block; margin-top: 8px;
            background: rgba(46,184,170,0.15); color: #2EB8AA;
            padding: 4px 12px; border-radius: 20px; font-size: 0.8em;
            border: 1px solid rgba(46,184,170,0.3);
        }
        .upload-section {
            background: rgba(255,255,255,0.04);
            border: 2px dashed rgba(46,184,170,0.4);
            border-radius: 12px;
            padding: 40px;
            text-align: center;
            margin-bottom: 30px;
            transition: border-color 0.3s, background 0.3s;
        }
        .upload-section:hover { border-color: #2EB8AA; }
        .upload-section.dragover { border-color: #2EB8AA; background: rgba(46,184,170,0.08); }
        .upload-icon { font-size: 3.5em; margin-bottom: 15px; }
        .upload-text { font-size: 1.15em; color: #999; margin-bottom: 20px; }
        input[type="file"] { position: absolute; left: -9999px; }
        .file-label {
            background: linear-gradient(135deg, #2EB8AA, #1A9E8F);
            color: #fff; padding: 14px 36px;
            border-radius: 8px; font-size: 1.05em;
            cursor: pointer; transition: all 0.3s;
            display: inline-block;
        }
        .file-label:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(46,184,170,0.4);
        }
        .btn {
            background: linear-gradient(135deg, #2EB8AA, #1A9E8F);
            color: #fff; border: none;
            padding: 12px 28px; border-radius: 8px;
            font-size: 1em; cursor: pointer;
            transition: all 0.3s; margin: 8px 4px;
            text-decoration: none; display: inline-block;
        }
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(46,184,170,0.4);
        }
        .btn-secondary { background: rgba(255,255,255,0.1); }
        .btn-danger { background: linear-gradient(135deg, #e74c3c, #c0392b); }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; box-shadow: none; }

        /* Results */
        .results-section {
            background: rgba(255,255,255,0.04);
            border-radius: 12px;
            padding: 25px;
            margin-top: 20px;
            border: 1px solid rgba(46,184,170,0.15);
        }
        .results-header {
            display: flex; justify-content: space-between;
            align-items: center; margin-bottom: 20px;
            flex-wrap: wrap; gap: 15px;
        }
        .results-title { font-size: 1.4em; color: #2EB8AA; }
        .results-stats { display: flex; gap: 25px; }
        .results-stat { text-align: center; }
        .results-stat-value { font-size: 1.5em; font-weight: bold; color: #2EB8AA; }
        .results-stat-label { font-size: 0.8em; color: #777; }
        .catalog-type-badge {
            background: rgba(46,184,170,0.2); color: #2EB8AA;
            padding: 6px 16px; border-radius: 6px; font-weight: 600; font-size: 1.1em;
        }
        .table-container { max-height: 500px; overflow-y: auto; margin-top: 15px; }
        table { width: 100%; border-collapse: collapse; }
        thead { position: sticky; top: 0; background: #1a1f2e; z-index: 10; }
        th {
            padding: 12px; text-align: left;
            border-bottom: 2px solid #2EB8AA;
            color: #2EB8AA; font-weight: 600;
        }
        td { padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.06); font-family: 'SF Mono', monospace; font-size: 0.92em; }
        tr:hover { background: rgba(46,184,170,0.05); }
        .message {
            padding: 14px 20px; border-radius: 8px;
            margin: 15px 0; text-align: center; font-size: 1.05em;
        }
        .message.success { background: rgba(76,175,80,0.15); border: 1px solid #4caf50; color: #4caf50; }
        .message.error { background: rgba(244,67,54,0.15); border: 1px solid #f44336; color: #f44336; }
        .actions { text-align: center; margin-top: 20px; }

        /* Progress */
        .progress-overlay {
            display: none;
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(0,0,0,0.85); z-index: 1000;
            justify-content: center; align-items: center; flex-direction: column;
        }
        .progress-overlay.active { display: flex; }
        .progress-box {
            background: #1a1f2e; border-radius: 12px; padding: 40px 50px;
            border: 1px solid rgba(46,184,170,0.3); text-align: center; min-width: 400px;
        }
        .progress-title { color: #2EB8AA; font-size: 1.2em; margin-bottom: 20px; }
        .progress-bar-bg {
            width: 100%; height: 10px; background: rgba(255,255,255,0.08);
            border-radius: 5px; overflow: hidden; margin-bottom: 12px;
        }
        .progress-bar-fill {
            height: 100%; width: 0%; border-radius: 5px;
            background: linear-gradient(90deg, #2EB8AA, #1A9E8F);
            transition: width 0.3s;
        }
        .progress-percent { color: #ccc; font-size: 1.3em; font-weight: 600; }
        .progress-detail { color: #666; font-size: 0.85em; margin-top: 8px; }

        /* Batch queue */
        .batch-queue {
            background: rgba(255,255,255,0.04);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 25px;
            border: 1px solid rgba(46,184,170,0.15);
        }
        .batch-title { font-size: 1.2em; color: #2EB8AA; margin-bottom: 15px; }
        .batch-item {
            display: flex; align-items: center; gap: 15px;
            padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.06);
        }
        .batch-item:last-child { border-bottom: none; }
        .batch-item-name { flex: 1; color: #ccc; font-size: 0.95em; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .batch-item-size { color: #666; font-size: 0.85em; min-width: 60px; text-align: right; }
        .batch-item-bar { width: 120px; height: 6px; background: rgba(255,255,255,0.08); border-radius: 3px; overflow: hidden; }
        .batch-item-fill { height: 100%; width: 0%; background: linear-gradient(90deg, #2EB8AA, #1A9E8F); border-radius: 3px; transition: width 0.3s; }
        .batch-item-status { min-width: 90px; font-size: 0.85em; text-align: right; }
        .batch-item-status.pending { color: #555; }
        .batch-item-status.uploading { color: #f0ad4e; }
        .batch-item-status.parsing { color: #2EB8AA; }
        .batch-item-status.done { color: #4caf50; }
        .batch-item-status.error { color: #f44336; }
        .batch-summary {
            margin-top: 15px; padding-top: 15px;
            border-top: 1px solid rgba(255,255,255,0.1);
            display: flex; justify-content: space-between; align-items: center;
        }
        .batch-summary-text { color: #999; font-size: 0.95em; }
        .batch-summary-text strong { color: #2EB8AA; }

        /* Existing catalogs */
        .catalogs-section {
            background: rgba(255,255,255,0.04);
            border-radius: 12px;
            padding: 20px;
            margin-top: 25px;
            border: 1px solid rgba(255,255,255,0.08);
        }
        .catalogs-title { font-size: 1.1em; color: #999; margin-bottom: 15px; }
        .brand-tab {
            display: inline-block; padding: 8px 16px; border-radius: 8px; font-size: 0.9em;
            margin: 3px; cursor: pointer; background: rgba(255,255,255,0.06); color: #888;
            border: 1px solid rgba(255,255,255,0.1); transition: all 0.2s;
        }
        .brand-tab:hover { border-color: rgba(46,184,170,0.3); color: #ccc; }
        .brand-tab.active { background: linear-gradient(135deg, #2EB8AA, #1A9E8F); color: #fff; border-color: transparent; }
        .catalog-card {
            display: inline-block; background: rgba(255,255,255,0.06);
            border-radius: 8px; padding: 12px 18px; margin: 5px;
            border: 1px solid rgba(255,255,255,0.08);
        }
        .catalog-card-name { color: #2EB8AA; font-weight: 600; font-size: 1em; }
        .catalog-card-count { color: #777; font-size: 0.85em; }
        .docs-link {
            position: fixed; top: 12px; right: 40px; z-index: 900;
            background: rgba(255,255,255,0.06); color: #888; padding: 6px 14px;
            border-radius: 6px; font-size: 0.8em; text-decoration: none;
            border: 1px solid rgba(255,255,255,0.1); transition: all 0.2s;
        }
        .docs-link:hover { color: #2EB8AA; border-color: rgba(46,184,170,0.3); background: rgba(46,184,170,0.08); }
    </style>
</head>
<body>
    <a href="/docs" class="docs-link">Документация</a>
    <div class="container">
        <header>
            <h1>PDF Парсер ONIS</h1>
            <div class="subtitle">Загрузите PDF каталог — получите структурированные данные</div>
        </header>

        <div style="text-align:center; margin-bottom:20px;">
            <a href="/" style="color:#2EB8AA; text-decoration:none; margin:0 15px; padding:8px 16px; border-radius:6px; border:1px solid rgba(46,184,170,0.3); background:rgba(46,184,170,0.15);">Общий парсер</a>
            <a href="/catalogs" style="color:#2EB8AA; text-decoration:none; margin:0 15px; padding:8px 16px; border-radius:6px; border:1px solid rgba(46,184,170,0.3);">ONIS Флагманы</a>
        </div>

        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="message {{ category }}">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}

        <div class="progress-overlay" id="progress-overlay">
            <div class="progress-box">
                <div class="progress-title" id="progress-title">Загрузка файла...</div>
                <div class="progress-bar-bg"><div class="progress-bar-fill" id="progress-bar"></div></div>
                <div class="progress-percent" id="progress-percent">0%</div>
                <div class="progress-detail" id="progress-detail"></div>
            </div>
        </div>

        {% if not parsed_data %}
        <div class="upload-section" id="drop-zone">
            <div class="upload-icon">📄</div>
            <div class="upload-text">Перетащите до 10 PDF каталогов или нажмите кнопку</div>
            <div>
                <input type="file" name="files" id="file-input" accept=".pdf" multiple>
                <label for="file-input" class="file-label">Выбрать PDF файлы</label>
            </div>
        </div>

        <div class="batch-queue" id="batch-queue" style="display:none;">
            <div class="batch-title" id="batch-title">Очередь парсинга</div>
            <div id="batch-items"></div>
            <div class="batch-summary" id="batch-summary" style="display:none;">
                <div class="batch-summary-text">
                    Готово: <strong id="batch-done-count">0</strong> / <span id="batch-total-count">0</span> |
                    Моделей: <strong id="batch-models-count">0</strong>
                </div>
                <div>
                    <button class="btn" id="btn-save-all" onclick="saveAllToBotKB()" style="display:none;">
                        Загрузить всё в базу бота
                    </button>
                    <a href="/" class="btn btn-secondary" id="btn-new-batch" style="display:none;">Новый батч</a>
                </div>
            </div>
        </div>

        <div class="catalogs-section" id="live-bases">
            <div class="catalogs-title">В базе бота: <span id="bases-total" style="color:#2EB8AA;"></span></div>
            <div id="bases-cards" style="display:flex;flex-wrap:wrap;gap:15px;margin-top:15px;"></div>
        </div>

        <script>
        var batchQueue = [];  // [{file, taskId, status, models, elapsed}]
        var batchActive = 0;
        var MAX_PARALLEL = 5;  // GPU has 3 parallel workers, 5 = keep pipeline full

        function startBatch(files) {
            var pdfFiles = [];
            for (var i = 0; i < Math.min(files.length, 10); i++) {
                if (files[i].name.toLowerCase().endsWith('.pdf')) pdfFiles.push(files[i]);
            }
            if (!pdfFiles.length) return;

            batchQueue = [];
            var container = document.getElementById('batch-items');
            container.innerHTML = '';
            document.getElementById('batch-queue').style.display = '';
            document.getElementById('batch-summary').style.display = '';
            document.getElementById('btn-save-all').style.display = 'none';
            document.getElementById('btn-new-batch').style.display = 'none';
            document.getElementById('batch-total-count').textContent = pdfFiles.length;
            document.getElementById('batch-done-count').textContent = '0';
            document.getElementById('batch-models-count').textContent = '0';
            document.getElementById('drop-zone').style.display = 'none';

            for (var i = 0; i < pdfFiles.length; i++) {
                var f = pdfFiles[i];
                var item = {file: f, taskId: null, status: 'pending', models: 0, elapsed: 0, idx: i};
                batchQueue.push(item);

                var html = '<div class="batch-item" id="bi-' + i + '">' +
                    '<div class="batch-item-name">' + f.name + '</div>' +
                    '<div class="batch-item-size">' + (f.size/1024/1024).toFixed(1) + ' MB</div>' +
                    '<div class="batch-item-bar"><div class="batch-item-fill" id="bif-' + i + '"></div></div>' +
                    '<div class="batch-item-status pending" id="bis-' + i + '">Ожидание</div>' +
                '</div>';
                container.innerHTML += html;
            }

            batchActive = 0;
            for (var p = 0; p < MAX_PARALLEL; p++) processNext();
        }

        function processNext() {
            var next = null;
            for (var i = 0; i < batchQueue.length; i++) {
                if (batchQueue[i].status === 'pending') { next = batchQueue[i]; break; }
            }
            if (!next) return;
            batchActive++;
            uploadOne(next);
        }

        function uploadOne(item) {
            item.status = 'uploading';
            setItemStatus(item.idx, 'uploading', 'Загрузка...');
            setItemBar(item.idx, 10);

            var fd = new FormData();
            fd.append('file', item.file);

            var xhr = new XMLHttpRequest();
            xhr.upload.addEventListener('progress', function(e) {
                if (e.lengthComputable) {
                    setItemBar(item.idx, Math.round(e.loaded / e.total * 30));
                }
            });
            xhr.addEventListener('load', function() {
                try {
                    var resp = JSON.parse(xhr.responseText);
                    if (resp.task_id) {
                        item.taskId = resp.task_id;
                        item.status = 'parsing';
                        setItemStatus(item.idx, 'parsing', 'Парсинг...');
                        setItemBar(item.idx, 35);
                        pollOne(item, resp.total_pages);
                    } else {
                        item.status = 'error';
                        setItemStatus(item.idx, 'error', resp.error || 'Ошибка');
                        itemFinished(item);
                    }
                } catch(e) {
                    item.status = 'error';
                    setItemStatus(item.idx, 'error', 'Ошибка');
                    itemFinished(item);
                }
            });
            xhr.addEventListener('error', function() {
                item.status = 'error';
                setItemStatus(item.idx, 'error', 'Сеть');
                itemFinished(item);
            });
            xhr.open('POST', '/upload');
            xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
            xhr.send(fd);
        }

        function pollOne(item, totalPages) {
            var interval = setInterval(function() {
                var r = new XMLHttpRequest();
                r.open('GET', '/progress/' + item.taskId);
                r.addEventListener('load', function() {
                    try {
                        var d = JSON.parse(r.responseText);
                        if (d.status === 'done') {
                            clearInterval(interval);
                            item.status = 'done';
                            item.models = d.models_found || 0;
                            item.elapsed = d.elapsed || 0;
                            setItemBar(item.idx, 100);
                            var brandTag = d.brand ? (' [' + d.brand + ']') : '';
                            setItemStatus(item.idx, 'done', item.models + ' мод. ' + item.elapsed + 's' + brandTag);
                            itemFinished(item);
                        } else if (d.status === 'error') {
                            clearInterval(interval);
                            item.status = 'error';
                            setItemStatus(item.idx, 'error', (d.error || '').substring(0, 40));
                            setItemBar(item.idx, 0);
                            itemFinished(item);
                        } else {
                            var p = 35 + Math.round((d.progress || 0) * 0.6);
                            setItemBar(item.idx, Math.min(p, 95));
                            setItemStatus(item.idx, 'parsing', (d.pages_done || '?') + '/' + totalPages + ' стр.');
                        }
                    } catch(e) {}
                });
                r.send();
            }, 1500);
        }

        function itemFinished(item) {
            batchActive--;
            updateBatchSummary();
            processNext();

            // Check if all done
            var allDone = batchQueue.every(function(q) { return q.status === 'done' || q.status === 'error'; });
            if (allDone) {
                var hasOk = batchQueue.some(function(q) { return q.status === 'done'; });
                document.getElementById('batch-title').textContent = 'Батч завершён';
                if (hasOk) document.getElementById('btn-save-all').style.display = '';
                document.getElementById('btn-new-batch').style.display = '';
            }
        }

        function updateBatchSummary() {
            var done = 0, models = 0;
            batchQueue.forEach(function(q) {
                if (q.status === 'done') { done++; models += q.models; }
            });
            document.getElementById('batch-done-count').textContent = done;
            document.getElementById('batch-models-count').textContent = models;
        }

        function setItemStatus(idx, cls, text) {
            var el = document.getElementById('bis-' + idx);
            if (el) { el.className = 'batch-item-status ' + cls; el.textContent = text; }
        }
        function setItemBar(idx, pct) {
            var el = document.getElementById('bif-' + idx);
            if (el) el.style.width = pct + '%';
        }

        function saveAllToBotKB() {
            var btn = document.getElementById('btn-save-all');
            btn.textContent = 'Сохранение...';
            btn.disabled = true;
            var taskIds = batchQueue.filter(function(q) { return q.status === 'done' && q.taskId; }).map(function(q) { return q.taskId; });
            var saved = 0, errors = 0, totalMsg = [];

            taskIds.forEach(function(tid) {
                var r = new XMLHttpRequest();
                r.open('POST', '/save_to_bot/' + tid, true);
                r.addEventListener('load', function() {
                    saved++;
                    try {
                        var d = JSON.parse(r.responseText);
                        if (d.ok) totalMsg.push(d.message);
                        else errors++;
                    } catch(e) { errors++; }
                    if (saved >= taskIds.length) {
                        btn.textContent = 'Сохранено ' + (saved - errors) + '/' + taskIds.length;
                        if (totalMsg.length) {
                            var msgEl = document.createElement('div');
                            msgEl.className = 'message success';
                            msgEl.textContent = totalMsg.join(' | ');
                            document.getElementById('batch-queue').appendChild(msgEl);
                        }
                    }
                });
                r.send();
            });
        }

        // Live bases refresh
        var _basesData = null;
        function renderBases() {
            if (!_basesData) return;
            var brands = _basesData.brands || [];
            var total = _basesData.total_models || 0;
            var nbrands = _basesData.total_brands || 0;
            var bc = document.getElementById('bases-cards');
            var bt = document.getElementById('bases-total');
            if (!bc) return;
            if (bt) bt.textContent = total + ' \u043c\u043e\u0434\u0435\u043b\u0435\u0439 \u2022 ' + nbrands + ' \u0431\u0440\u0435\u043d\u0434\u043e\u0432';

            var html = '';
            brands.forEach(function(b) {
                var badges = '';
                (b.series || []).forEach(function(s) {
                    var star = s.flagship ? '\u2605' : '';
                    badges += '<span style="display:inline-block;background:rgba(255,255,255,0.08);padding:3px 8px;border-radius:4px;margin:2px;font-size:0.85em;">' + star + s.name + ' <span style="color:#666;">' + s.count + '</span></span>';
                });
                html += '<div style="flex:1;min-width:260px;background:rgba(255,255,255,0.04);border-radius:10px;padding:18px;border:1px solid rgba(46,184,170,0.15);">';
                html += '<div style="font-size:1.2em;font-weight:600;color:#2EB8AA;margin-bottom:4px;">' + b.brand + '</div>';
                html += '<div style="color:#777;font-size:0.9em;margin-bottom:10px;">' + b.series_count + ' \u0441\u0435\u0440\u0438\u0439 \u2022 ' + b.models_count + ' \u043c\u043e\u0434\u0435\u043b\u0435\u0439</div>';
                html += '<div style="display:flex;flex-wrap:wrap;gap:2px;">' + badges + '</div>';
                html += '</div>';
            });
            bc.innerHTML = html;
        }
        function refreshBases() {
            var r = new XMLHttpRequest();
            r.open('GET', '/api/bases');
            r.addEventListener('load', function() {
                try {
                    _basesData = JSON.parse(r.responseText);
                    renderBases();
                } catch(e) { console.error(e); }
            });
            r.send();
        }
        refreshBases();
        setInterval(refreshBases, 10000);
        document.getElementById('file-input').addEventListener('change', function() {
            if (this.files.length) startBatch(this.files);
        });
        var dropZone = document.getElementById('drop-zone');
        dropZone.addEventListener('dragover', function(e) { e.preventDefault(); dropZone.classList.add('dragover'); });
        dropZone.addEventListener('dragleave', function(e) { e.preventDefault(); dropZone.classList.remove('dragover'); });
        dropZone.addEventListener('drop', function(e) {
            e.preventDefault(); dropZone.classList.remove('dragover');
            var files = e.dataTransfer.files;
            if (files.length) startBatch(files);
        });
        </script>
        {% endif %}

        {% if parsed_data %}
        <div class="results-section">
            <div class="results-header">
                <div>
                    <div class="results-title">Результаты парсинга</div>
                    {% if catalog_type %}
                    <span class="catalog-type-badge">{{ catalog_type }}</span>
                    {% endif %}
                </div>
                <div class="results-stats">
                    <div class="results-stat">
                        <div class="results-stat-value">{{ parsed_stats.total }}</div>
                        <div class="results-stat-label">Записей</div>
                    </div>
                    <div class="results-stat">
                        <div class="results-stat-value">{{ parsed_stats.models }}</div>
                        <div class="results-stat-label">Моделей</div>
                    </div>
                    <div class="results-stat">
                        <div class="results-stat-value">{{ parsed_stats.series }}</div>
                        <div class="results-stat-label">Серий</div>
                    </div>
                    {% if parsed_stats.time %}
                    <div class="results-stat">
                        <div class="results-stat-value">{{ parsed_stats.time }}s</div>
                        <div class="results-stat-label">GPU время</div>
                    </div>
                    {% endif %}
                </div>
            </div>

            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Модель</th>
                            <th>Серия</th>
                            <th>Q (м³/ч)</th>
                            <th>H (м)</th>
                            <th>P (кВт)</th>
                            <th>RPM</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for pump in parsed_data %}
                        <tr>
                            <td>{{ loop.index }}</td>
                            <td><strong>{{ pump.model }}</strong></td>
                            <td>{{ pump.series }}</td>
                            <td>{{ pump.q_nom }}</td>
                            <td>{{ pump.h_nom }}</td>
                            <td>{{ pump.power_kw }}</td>
                            <td>{{ pump.rpm }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

            <div class="actions">
                {% if result_id %}
                <a href="/download/{{ result_id }}" class="btn">Скачать JSON</a>
                <button class="btn" id="btn-save-bot" onclick="saveToBotKB('{{ result_id }}')">
                    Загрузить в базу бота
                </button>
                {% endif %}
                <a href="/" class="btn btn-secondary">Загрузить другой файл</a>
            </div>
            <div id="save-msg" class="message" style="display:none;"></div>
            <script>
            function saveToBotKB(taskId) {
                var btn = document.getElementById('btn-save-bot');
                btn.textContent = 'Сохранение...';
                btn.disabled = true;
                var r = new XMLHttpRequest();
                r.open('POST', '/save_to_bot/' + taskId);
                r.addEventListener('load', function() {
                    var msg = document.getElementById('save-msg');
                    try {
                        var d = JSON.parse(r.responseText);
                        if (d.ok) {
                            msg.className = 'message success';
                            msg.textContent = d.message;
                            btn.textContent = 'Загружено в базу бота';
                        } else {
                            msg.className = 'message error';
                            msg.textContent = d.error;
                            btn.textContent = 'Загрузить в базу бота';
                            btn.disabled = false;
                        }
                    } catch(e) {
                        msg.className = 'message error';
                        msg.textContent = 'Ошибка сохранения';
                        btn.textContent = 'Загрузить в базу бота';
                        btn.disabled = false;
                    }
                    msg.style.display = 'block';
                });
                r.send();
            }
            </script>
        </div>
        {% endif %}

        <footer></footer>
    </div>
</body>
</html>'''


def get_existing_catalogs():
    """Read existing catalogs from bot's pump_base"""
    catalogs = []
    pump_base = '/root/pump_base'
    if not os.path.isdir(pump_base):
        return catalogs
    for fname in sorted(os.listdir(pump_base)):
        if fname.endswith('.json'):
            try:
                fpath = os.path.join(pump_base, fname)
                with open(fpath) as f:
                    data = json.load(f)
                name = fname.replace('_BASE.json', '').replace('.json', '').replace('pump_models_', '')
                catalogs.append({'name': name, 'count': len(data), 'file': fname})
            except Exception:
                pass
    return catalogs


@app.route('/api/bases')
def api_bases():
    # Return pump bases grouped by brand
    try:
        with open('/root/pump_base/brands_index.json') as f:
            index = json.load(f)
    except Exception:
        index = {}

    brands = []
    total_models = 0
    for brand_name, brand_data in index.items():
        series_list = brand_data.get("series", [])
        brand_models = sum(s["count"] for s in series_list)
        total_models += brand_models
        brands.append({
            "brand": brand_name,
            "series": sorted(series_list, key=lambda s: (-int(s.get("flagship", False)), -s["count"])),
            "series_count": len(series_list),
            "models_count": brand_models,
        })

    # ONIS first, then by model count
    brands.sort(key=lambda b: (0 if b["brand"] == "ONIS" else 1, -b["models_count"]))

    return jsonify({
        "brands": brands,
        "total_models": total_models,
        "total_brands": len(brands),
    })


@app.route('/api/rebrand', methods=['POST'])
def api_rebrand():
    """Re-classify all BASE files by brand without GPU re-parse."""
    import json as _j
    base_dir = '/root/pump_base'
    updated = 0
    brand_summary = {}

    for fname in sorted(os.listdir(base_dir)):
        if not fname.endswith('_BASE.json'):
            continue
        fpath = os.path.join(base_dir, fname)
        with open(fpath) as f:
            models = _j.load(f)
        if not models:
            continue

        series = fname.replace('_BASE.json', '')
        br = brand_qualifier.qualify_from_models(models)
        flagship = series in ('MV', 'INL', 'MBL')

        changed = False
        for m in models:
            if m.get('brand') != br.brand or m.get('flagship') != flagship:
                m['brand'] = br.brand
                m['flagship'] = flagship
                changed = True

        if changed:
            with open(fpath, 'w') as f:
                _j.dump(models, f, ensure_ascii=False, indent=2)
            updated += 1

        b = br.brand
        if b not in brand_summary:
            brand_summary[b] = 0
        brand_summary[b] += 1

    _rebuild_brands_index()
    return jsonify({'ok': True, 'updated': updated, 'brands': brand_summary})


@app.route('/api/reparse-all', methods=['POST'])
def api_reparse_all():
    """Trigger re-parse of all PDFs in catalogs dir with brand detection."""
    catalog_dir = '/root/ONIS/catalogs'
    if not os.path.isdir(catalog_dir):
        return jsonify({'ok': False, 'error': 'Catalogs dir not found'})

    pdfs = sorted([f for f in os.listdir(catalog_dir) if f.lower().endswith('.pdf')])
    results = []
    for pdf in pdfs:
        pdf_path = os.path.join(catalog_dir, pdf)
        br = brand_qualifier.qualify(pdf_path)
        results.append({
            'file': pdf,
            'brand': br.brand,
            'confidence': br.confidence,
            'source': br.source,
            'markers': br.markers_found,
        })

    return jsonify({'ok': True, 'catalogs': results, 'total': len(results)})


@app.route('/')
def index():
    cats = get_existing_catalogs()
    return render_template_string(HTML_TEMPLATE, parsed_data=None, parsed_stats=None,
                                  result_id=None, catalog_type=None, existing_catalogs=cats)


def _run_parse_docling(task_id, filepath):
    """Background: send PDF to GPU server, get tables, convert to pump models"""
    try:
        parse_tasks[task_id]['status'] = 'uploading'
        parse_tasks[task_id]['phase'] = 'Проверка GPU сервера...'
        t0 = time.time()

        # Quick health check — fail fast if GPU is down
        try:
            hc = requests.get(f'{GPU_SERVER}/health', timeout=5)
            if hc.status_code != 200:
                raise ConnectionError("GPU not healthy")
        except Exception:
            parse_tasks[task_id]['status'] = 'error'
            parse_tasks[task_id]['error'] = 'GPU сервер недоступен. Попробуйте позже.'
            return

        parse_tasks[task_id]['phase'] = 'Отправка на GPU сервер...'

        # Send PDF to GPU server
        with open(filepath, 'rb') as f:
            resp = requests.post(
                f'{GPU_SERVER}/parse',
                files={'file': (os.path.basename(filepath), f, 'application/pdf')},
                timeout=900  # 15 min max for large PDFs
            )

        if resp.status_code != 200:
            parse_tasks[task_id]['status'] = 'error'
            parse_tasks[task_id]['error'] = f'GPU сервер: HTTP {resp.status_code}'
            return

        result = resp.json()
        tables = result.get('tables', [])
        elapsed = time.time() - t0

        parse_tasks[task_id]['phase'] = 'Извлечение моделей из таблиц...'
        parse_tasks[task_id]['progress'] = 80

        # Convert tables to pump models
        models = tables_to_pump_models(tables)

        if not models:
            parse_tasks[task_id]['status'] = 'error'
            parse_tasks[task_id]['error'] = f'Найдено {len(tables)} таблиц, но моделей насосов не обнаружено'
            return

        # Detect catalog type
        catalog_type = detect_catalog_type(models)

        # Deduplicate by model name
        unique_models = list({m['model']: m for m in models}.values())
        unique_series = set(m['series'] for m in unique_models)

        # Save results
        result_path = os.path.join(app.config['UPLOAD_FOLDER'], f'result_{task_id}.json')
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(unique_models, f, ensure_ascii=False, indent=2)

        parse_tasks[task_id].update({
            'status': 'done',
            'parsed_data': unique_models,
            'parsed_stats': {
                'total': len(unique_models),
                'models': len(set(m['model'] for m in unique_models)),
                'series': len(unique_series),
                'time': round(elapsed, 1)
            },
            'result_path': result_path,
            'catalog_type': catalog_type,
            'brand': 'Unknown',
            'models_found': len(unique_models),
            'elapsed': round(elapsed, 1),
            'tables_raw': len(tables)
        })
        # ── Brand qualification ──
        try:
            if os.path.exists(filepath):
                _br = brand_qualifier.qualify_full(filepath, unique_models)
            else:
                _br = brand_qualifier.qualify_from_models(unique_models)
            parse_tasks[task_id]['brand'] = _br.brand
            parse_tasks[task_id]['brand_confidence'] = _br.confidence
            parse_tasks[task_id]['brand_source'] = _br.source
            parse_tasks[task_id]['series_detected'] = _br.series_detected
            for _m in unique_models:
                _m['brand'] = _br.brand
            with open(result_path, 'w', encoding='utf-8') as f:
                json.dump(unique_models, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
        _save_tasks()

    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as gpu_err:
        parse_tasks[task_id]['status'] = 'error'
        parse_tasks[task_id]['error'] = 'GPU сервер недоступен (82.22.53.231:5001)'
    except Exception as e:
        parse_tasks[task_id]['status'] = 'error'
        parse_tasks[task_id]['error'] = str(e)
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)


@app.route('/upload', methods=['POST'])
def upload():
    is_xhr = request.headers.get('X-Requested-With') == 'XMLHttpRequest'

    if 'file' not in request.files:
        return jsonify({'error': 'Файл не выбран'}) if is_xhr else (flash('Файл не выбран', 'error'), redirect(url_for('index')))[1]

    file = request.files['file']
    if file.filename == '' or not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Выберите PDF файл'}) if is_xhr else (flash('Выберите PDF', 'error'), redirect(url_for('index')))[1]

    filename = secure_filename(file.filename) or 'upload.pdf'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{timestamp}_{filename}"
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    # Count pages
    total_pages = 0
    try:
        import fitz
        doc = fitz.open(filepath)
        total_pages = len(doc)
        doc.close()
    except Exception:
        total_pages = 50  # estimate

    task_id = uuid.uuid4().hex[:8]
    parse_tasks[task_id] = {
        'status': 'starting',
        'progress': 0,
        'total_pages': total_pages,
        'start_time': time.time(),
        'phase': 'Инициализация...'
    }

    thread = threading.Thread(target=_run_parse_docling, args=(task_id, filepath), daemon=True)
    thread.start()

    if is_xhr:
        return jsonify({'task_id': task_id, 'total_pages': total_pages})
    else:
        # Wait synchronously (fallback for no-JS)
        for _ in range(600):
            time.sleep(1)
            if parse_tasks[task_id]['status'] in ('done', 'error'):
                break
        return redirect(f'/results/{task_id}')


@app.route('/progress/<task_id>')
def progress(task_id):
    task = parse_tasks.get(task_id)
    if not task:
        return jsonify({'status': 'error', 'error': 'Задача не найдена'})

    status = task.get('status', 'unknown')
    total_pages = task.get('total_pages', 1)

    if status == 'done':
        return jsonify({
            'status': 'done',
            'progress': 100,
            'models_found': task.get('models_found', 0),
            'elapsed': f"{task.get('elapsed', 0)}"
        })
    elif status == 'error':
        return jsonify({'status': 'error', 'error': task.get('error', '')})
    elif status in ('uploading', 'parsing', 'starting'):
        elapsed = time.time() - task.get('start_time', time.time())
        # Estimate: ~3s per page on GPU
        est_seconds = total_pages * 3.0
        pct = min(int(elapsed / est_seconds * 100), 95) if est_seconds > 0 else 50
        pct = max(pct, task.get('progress', 0))
        pages_done = min(int(elapsed / 3.0), total_pages)
        return jsonify({
            'status': 'parsing',
            'progress': pct,
            'phase': task.get('phase', 'Парсинг...'),
            'pages_done': pages_done,
            'total_pages': total_pages
        })

    return jsonify({'status': status, 'progress': 0})


@app.route('/results/<task_id>')
def results(task_id):
    task = parse_tasks.get(task_id)
    if not task or 'parsed_data' not in task:
        flash('Результаты не найдены', 'error')
        return redirect(url_for('index'))

    return render_template_string(
        HTML_TEMPLATE,
        parsed_data=task['parsed_data'],
        parsed_stats=task['parsed_stats'],
        result_id=task_id,
        catalog_type=task.get('catalog_type', ''),
        existing_catalogs=None
    )


@app.route('/download/<task_id>')
def download(task_id):
    task = parse_tasks.get(task_id)
    if not task or 'result_path' not in task:
        flash('Нет данных', 'error')
        return redirect(url_for('index'))

    result_path = task['result_path']
    if not os.path.exists(result_path):
        flash('Файл не найден', 'error')
        return redirect(url_for('index'))

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    cat_type = task.get('catalog_type', 'PUMP')
    return send_file(
        result_path,
        as_attachment=True,
        download_name=f"pump_models_{cat_type}_{timestamp}.json",
        mimetype='application/json'
    )


@app.route('/save_to_bot/<task_id>', methods=['POST'])
def save_to_bot(task_id):
    """Save parsed models to bot's knowledge base"""
    task = parse_tasks.get(task_id)
    if not task or 'parsed_data' not in task:
        return jsonify({'ok': False, 'error': 'Результаты не найдены'})

    try:
        catalog_type = task.get('catalog_type', 'PUMP')
        brand = task.get('brand', 'OTHER')
        os.makedirs('/root/pump_base', exist_ok=True)

        # Save as pump_models_{TYPE}.json (matches bot format)
        dest_path = f'/root/pump_base/pump_models_{catalog_type}.json'

        # Merge with existing if present
        existing = []
        if os.path.exists(dest_path):
            with open(dest_path) as f:
                existing = json.load(f)

        # Merge: new data replaces matching models, keeps unique old ones (normalized keys)
        new_models = {_normalize_model_key(m['model']): m for m in task['parsed_data']}
        for old in existing:
            k = _normalize_model_key(old.get('model', ''))
            if k not in new_models:
                new_models[k] = old

        merged = sorted(new_models.values(), key=lambda x: (x['series'], x.get('q_nom', 0)))

        with open(dest_path, 'w', encoding='utf-8') as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)

        return jsonify({
            'ok': True,
            'message': f'{len(task["parsed_data"])} моделей → pump_models_{catalog_type}.json (всего {len(merged)} в базе)'
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Ошибка: {str(e)}'})


PHOTOS_DIR = '/root/pump_base/photos'
DRAWINGS_DIR = '/root/pump_base/drawings'

# === LLM Column Classifier ===
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")

def llm_classify_columns(columns, sample_rows):
    """Ask DeepSeek to classify table columns: which is model, Q, H, kW, RPM.
    Only classifies structure — never touches actual numbers.
    Returns: {role: column_name} e.g. {"model": "Насос", "q": "Подача", "h": "Напор", "kw": "P2"}
    """
    import requests as _req
    try:
        # Build compact table preview
        preview = "Columns: " + " | ".join(str(c) for c in columns) + "\n"
        for row in sample_rows[:3]:
            preview += " | ".join(str(row.get(c, "")) for c in columns) + "\n"

        resp = _req.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "deepseek-chat",
                "temperature": 0,
                "max_tokens": 200,
                "messages": [
                    {"role": "system", "content": "You classify pump catalog table columns. Return JSON only."},
                    {"role": "user", "content": f"""This is a pump catalog table. Identify which column contains:
- model: pump model name/designation
- q: flow rate (m³/h, л/с, подача, расход, Q)
- h: head/pressure (м, напор, H)
- kw: power (кВт, kW, мощность, P2)
- rpm: rotation speed (об/мин, RPM)

Return ONLY a JSON object mapping role to exact column name. Use null for missing columns.
Example: {{"model": "Тип", "q": "Q, м³/ч", "h": "H, м", "kw": "P2, кВт", "rpm": null}}

Table:
{preview}"""}
                ]
            },
            timeout=15
        )
        data = resp.json()
        text = data["choices"][0]["message"]["content"].strip()
        # Extract JSON from response
        import re as _re
        m = _re.search(r'\{[^}]+\}', text)
        if m:
            import json as _json
            mapping = _json.loads(m.group(0))
            # Validate: values must be actual column names
            valid = {}
            col_strs = [str(c) for c in columns]
            for role, col_name in mapping.items():
                if col_name and col_name in col_strs:
                    valid[role] = col_name
                elif col_name:
                    # Fuzzy match
                    for real_col in col_strs:
                        if col_name.lower() in real_col.lower() or real_col.lower() in col_name.lower():
                            valid[role] = real_col
                            break
            return valid if valid else None
        return None
    except Exception as e:
        print(f"LLM classify error: {e}")
        return None


DOCS_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Документация — PDF Парсер ONIS</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #0a0e17 0%, #1a1f2e 100%);
            color: #d0d0d0; min-height: 100vh; padding: 20px;
        }
        .container { max-width: 800px; margin: 0 auto; }
        .back { display: inline-block; color: #2EB8AA; text-decoration: none; margin-bottom: 20px; font-size: 0.95em; }
        .back:hover { text-decoration: underline; }
        h1 { font-size: 1.8em; color: #2EB8AA; margin-bottom: 25px; }
        h2 { font-size: 1.3em; color: #2EB8AA; margin: 30px 0 12px 0; padding-bottom: 6px; border-bottom: 1px solid rgba(46,184,170,0.2); }
        h3 { font-size: 1.05em; color: #e0e0e0; margin: 18px 0 8px 0; }
        p { line-height: 1.7; margin-bottom: 10px; color: #bbb; }
        ul { margin: 8px 0 12px 24px; }
        li { margin-bottom: 6px; line-height: 1.6; color: #bbb; }
        .step {
            background: rgba(255,255,255,0.04); border-radius: 8px; padding: 14px 18px;
            margin: 10px 0; border-left: 3px solid #2EB8AA;
        }
        .step-num { color: #2EB8AA; font-weight: 700; font-size: 1.1em; }
        code {
            background: rgba(46,184,170,0.1); color: #2EB8AA; padding: 2px 6px;
            border-radius: 4px; font-size: 0.9em;
        }
        .note {
            background: rgba(255,152,0,0.08); border: 1px solid rgba(255,152,0,0.2);
            border-radius: 8px; padding: 12px 16px; margin: 12px 0; color: #ffb74d;
        }
        .section { margin-bottom: 35px; }
    </style>
</head>
<body>
<div class="container">
    <a href="/" class="back">&larr; Назад к парсеру</a>
    <h1>Документация</h1>

    <div class="section">
        <h2>Общий парсер</h2>
        <p>Загрузка любых PDF каталогов насосов в базу бота.</p>
        <div class="step"><span class="step-num">1.</span> Выберите до 10 PDF файлов или перетащите их в зону загрузки</div>
        <div class="step"><span class="step-num">2.</span> Дождитесь парсинга — прогресс виден по каждому файлу</div>
        <div class="step"><span class="step-num">3.</span> Нажмите <code>Загрузить все в базу бота</code></div>
        <p>Извлекается: модель, серия, подача Q, напор H, мощность P, обороты.</p>
    </div>

    <div class="section">
        <h2>ONIS Флагманы</h2>
        <p>Каталоги флагманских серий ONIS с фотографиями для PDF паспорта.</p>
        <div class="step"><span class="step-num">1.</span> <code>Выбрать PDF</code> — загрузите каталог серии. Название берется из имени файла (<code>INL.pdf</code> &rarr; <b>INL</b>)</div>
        <div class="step"><span class="step-num">2.</span> <code>Фото серии</code> — добавьте фото насоса для PDF паспорта (можно позже)</div>
        <div class="step"><span class="step-num">3.</span> <code>Загрузить</code> — данные и фото сохраняются автоматически</div>
        <p>Фото можно заменить на карточке каталога. Крестик удаляет каталог целиком.</p>
    </div>
</div>
</body>
</html>'''


@app.route('/docs')
def docs():
    return render_template_string(DOCS_HTML)

CATALOGS_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ONIS Флагманы — PDF Паспорт</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #0a0e17 0%, #1a1f2e 100%);
            color: #e0e0e0; min-height: 100vh; padding: 20px;
        }
        .container { max-width: 1100px; margin: 0 auto; }
        header {
            text-align: center; margin-bottom: 25px; padding: 25px;
            background: rgba(255,255,255,0.04); border-radius: 12px;
            border: 1px solid rgba(46,184,170,0.2);
        }
        h1 { font-size: 2em; background: linear-gradient(135deg, #2EB8AA, #1A9E8F);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 8px; }
        .sub { color: #777; font-size: 0.95em; }
        .nav { text-align: center; margin-bottom: 25px; }
        .nav a {
            color: #2EB8AA; text-decoration: none; margin: 0 12px; font-size: 1em;
            padding: 8px 16px; border-radius: 6px; border: 1px solid rgba(46,184,170,0.3);
        }
        .nav a:hover, .nav a.active { background: rgba(46,184,170,0.15); }

        /* Add new catalog */
        .add-section {
            background: rgba(255,255,255,0.04); border-radius: 12px; padding: 20px;
            border: 1px solid rgba(46,184,170,0.15); margin-bottom: 25px;
            display: flex; gap: 12px; align-items: center; flex-wrap: wrap;
        }
        .add-section input[type="text"] {
            background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.15);
            color: #e0e0e0; padding: 10px 16px; border-radius: 6px; font-size: 1em; width: 220px;
        }
        .add-section input::placeholder { color: #555; }
        .btn {
            background: linear-gradient(135deg, #2EB8AA, #1A9E8F);
            color: #fff; border: none; padding: 10px 22px; border-radius: 6px;
            font-size: 0.95em; cursor: pointer; transition: all 0.3s;
        }
        .btn:hover { transform: translateY(-1px); box-shadow: 0 4px 15px rgba(46,184,170,0.3); }
        .btn-sm { padding: 6px 14px; font-size: 0.85em; }
        .btn-danger { background: linear-gradient(135deg, #e74c3c, #c0392b); }

        /* Catalog cards */
        .catalog-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr)); gap: 20px; }
        .cat-card {
            background: rgba(255,255,255,0.04); border-radius: 12px; padding: 20px;
            border: 1px solid rgba(255,255,255,0.08);
        }
        .cat-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
        .cat-name { font-size: 1.4em; font-weight: 700; color: #2EB8AA; }
        .cat-meta { color: #666; font-size: 0.85em; }
        .cat-badges { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 12px; }
        .badge {
            padding: 3px 10px; border-radius: 12px; font-size: 0.8em;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .badge-models { background: rgba(46,184,170,0.1); color: #2EB8AA; }
        .badge-photo { background: rgba(76,175,80,0.1); color: #4caf50; }
        .badge-no-photo { background: rgba(244,67,54,0.1); color: #f44336; }
        .badge-no-data { background: rgba(255,152,0,0.1); color: #ff9800; }

        /* Photo area */
        .cat-photo {
            width: 100%; height: 160px; background: rgba(255,255,255,0.02);
            border-radius: 8px; display: flex; align-items: center; justify-content: center;
            margin-bottom: 12px; overflow: hidden; border: 1px dashed rgba(255,255,255,0.1);
            position: relative;
        }
        .cat-photo img { max-width: 100%; max-height: 100%; object-fit: contain; }
        .cat-photo .no-photo { color: #444; font-size: 0.85em; }

        /* Upload row */
        .cat-uploads { display: flex; gap: 16px; flex-wrap: wrap; align-items: center; }
        .cat-uploads input[type="file"] { display: none; }
        .cat-uploads label {
            background: rgba(255,255,255,0.06); color: #aaa; padding: 7px 14px;
            border-radius: 6px; cursor: pointer; font-size: 0.85em;
            border: 1px solid rgba(255,255,255,0.1); transition: all 0.2s;
        }
        .cat-uploads label:hover { background: rgba(46,184,170,0.1); color: #2EB8AA; border-color: rgba(46,184,170,0.3); }
        .upload-status { font-size: 0.8em; color: #666; }
        .msg { padding: 8px 12px; border-radius: 6px; margin-top: 8px; font-size: 0.85em; display: none; }
        .msg.ok { display: block; background: rgba(76,175,80,0.1); color: #4caf50; }
        .msg.err { display: block; background: rgba(244,67,54,0.1); color: #f44336; }
        .docs-link {
            position: fixed; top: 12px; right: 40px; z-index: 900;
            background: rgba(255,255,255,0.06); color: #888; padding: 6px 14px;
            border-radius: 6px; font-size: 0.8em; text-decoration: none;
            border: 1px solid rgba(255,255,255,0.1); transition: all 0.2s;
        }
        .docs-link:hover { color: #2EB8AA; border-color: rgba(46,184,170,0.3); background: rgba(46,184,170,0.08); }
    </style>
</head>
<body>
<a href="/docs" class="docs-link">Документация</a>
<div class="container">
    <header>
        <h1>ONIS Флагманы</h1>
        <div class="sub">Парсинг каталогов + фото серий для PDF паспорта</div>
    </header>

    <div class="nav">
        <a href="/">Общий парсер</a>
        <a href="/catalogs" class="active">ONIS Флагманы</a>
    </div>

    <div class="add-section" style="flex-direction:column; gap:14px;">
        <div style="color:#999; font-size:0.9em;">Загрузите PDF + фото + чертежи — название определится из файла</div>
        <div style="display:flex; gap:16px; align-items:center; flex-wrap:wrap;">
            <input type="file" id="onis-pdf-input" accept=".pdf" style="display:none" onchange="onPdfPicked(this)">
            <label for="onis-pdf-input" class="btn" style="cursor:pointer;" id="lbl-pdf">PDF каталог</label>

            <input type="file" id="onis-img-input" accept="image/*" style="display:none" onchange="onImgPicked(this)">
            <label for="onis-img-input" class="btn" style="cursor:pointer; background:rgba(255,255,255,0.1);" id="lbl-img">Фото серии</label>

            <input type="file" id="onis-drw-input" accept="image/*" multiple style="display:none" onchange="onDrwPicked(this)">
            <label for="onis-drw-input" class="btn" style="cursor:pointer; background:rgba(255,255,255,0.1);" id="lbl-drw">Чертежи</label>

            <button class="btn" id="btn-go" style="display:none;" onclick="goUpload()">Загрузить</button>
            <span class="upload-status" id="onis-pdf-status"></span>
        </div>
    </div>

    <div class="catalog-grid" id="catalog-grid">
        {% for cat in catalogs %}
        <div class="cat-card" id="card-{{ cat.name }}">
            <div class="cat-header">
                <div class="cat-name">{{ cat.name }}</div>
                <button class="btn btn-sm btn-danger" onclick="deleteCatalog('{{ cat.name }}')">&#x2715;</button>
            </div>

            <div class="cat-badges">
                {% if cat.count > 0 %}
                <span class="badge badge-models">{{ cat.count }} моделей</span>
                {% else %}
                <span class="badge badge-no-data">Нет данных</span>
                {% endif %}
                {% if cat.has_photo %}
                <span class="badge badge-photo">Фото есть</span>
                {% else %}
                <span class="badge badge-no-photo">Нет фото</span>
                {% endif %}
                {% if cat.drawings %}
                <span class="badge badge-photo">{{ cat.drawings|length }} чертежей</span>
                {% else %}
                <span class="badge badge-no-photo">Нет чертежей</span>
                {% endif %}

            </div>

            <div class="cat-photo" id="photo-{{ cat.name }}">
                {% if cat.has_photo %}
                <img src="/photos/{{ cat.name }}.png?t={{ cat.photo_mtime }}">
                {% else %}
                <div class="no-photo">Загрузите фото серии для PDF паспорта</div>
                {% endif %}
            </div>

            <div class="cat-uploads" style="display:flex; gap:16px; flex-wrap:wrap; margin-top:10px;">
                <div>
                    <input type="file" id="img-{{ cat.name }}" accept="image/*"
                        onchange="uploadPhoto('{{ cat.name }}', this.files[0])">
                    <label for="img-{{ cat.name }}">
                        {% if cat.has_photo %}Заменить фото{% else %}Добавить фото{% endif %}
                    </label>
                </div>
                <div>
                    <input type="file" id="drw-{{ cat.name }}" accept="image/*" multiple
                        onchange="uploadDrawings('{{ cat.name }}', this.files)">
                    <label for="drw-{{ cat.name }}">
                        Загрузить чертежи (папка)
                    </label>
                </div>
                <span class="upload-status" id="st-{{ cat.name }}"></span>
            </div>

            <div id="drawings-{{ cat.name }}" style="margin-top:8px;">
                {% if cat.drawings %}
                <div style="display:flex; flex-wrap:wrap; gap:6px;">
                    {% for d in cat.drawings %}
                    <div style="background:rgba(255,255,255,0.03); border:1px solid #222; border-radius:6px; padding:4px; text-align:center; width:80px;">
                        <a href="/drawings/{{ cat.name }}/{{ d.file }}" target="_blank"><img src="/drawings/{{ cat.name }}/{{ d.file }}?t={{ d.mtime }}" style="max-width:72px; max-height:50px; object-fit:contain; cursor:pointer;"></a>
                        <div style="font-size:0.7em; color:#888; margin-top:2px;">{{ d.label }}</div>
                    </div>
                    {% endfor %}
                </div>
                {% else %}
                <div style="color:#444; font-size:0.85em;">Загрузите чертежи — имена файлов: INL32.png, INL40.png или all.png</div>
                {% endif %}
            </div>
            <div class="msg" id="msg-{{ cat.name }}"></div>
        </div>
        {% endfor %}
    </div>
</div>

<script>
function setStatus(name, text, color) {
    var el = document.getElementById('st-' + name);
    if (el) { el.textContent = text; el.style.color = color || '#666'; }
}
function setMsg(name, text, ok) {
    var el = document.getElementById('msg-' + name);
    if (el) { el.textContent = text; el.className = 'msg ' + (ok ? 'ok' : 'err'); }
}

var _pdfFile = null;
var _imgFile = null;
var _drwFiles = null;

function uploadDrawings(name, files) {
    if (!files || files.length === 0) return;
    setStatus(name, 'Загрузка ' + files.length + ' чертежей...', '#2196f3');
    var fd = new FormData();
    for (var i = 0; i < files.length; i++) {
        fd.append('drawings', files[i]);
    }
    var r = new XMLHttpRequest();
    r.onload = function() {
        if (r.status === 200) {
            var resp = JSON.parse(r.responseText);
            setStatus(name, resp.count + ' чертежей загружено', '#4caf50');
            location.reload();
        } else {
            setStatus(name, 'Ошибка', '#f44336');
        }
    };
    r.open('POST', '/upload_drawings/' + name);
    r.send(fd);
}

function onPdfPicked(input) {
    _pdfFile = input.files[0] || null;
    if (_pdfFile) {
        document.getElementById('lbl-pdf').textContent = _pdfFile.name.substring(0, 25);
        document.getElementById('lbl-pdf').style.background = 'rgba(46,184,170,0.3)';
        document.getElementById('btn-go').style.display = '';
    }
}
function onDrwPicked(input) {
    _drwFiles = input.files;
    if (_drwFiles && _drwFiles.length > 0) {
        document.getElementById('lbl-drw').textContent = _drwFiles.length + ' чертежей';
        document.getElementById('lbl-drw').style.background = 'rgba(76,175,80,0.2)';
        document.getElementById('lbl-drw').style.color = '#4caf50';
        document.getElementById('lbl-drw').style.borderColor = '#4caf50';
    }
}

function onImgPicked(input) {
    _imgFile = input.files[0] || null;
    if (_imgFile) {
        document.getElementById('lbl-img').textContent = _imgFile.name.substring(0, 20);
        document.getElementById('lbl-img').style.background = 'rgba(76,175,80,0.2)';
        document.getElementById('lbl-img').style.color = '#4caf50';
        document.getElementById('lbl-img').style.borderColor = '#4caf50';
    }
}
function goUpload() {
    if (!_pdfFile) return;
    var st = document.getElementById('onis-pdf-status');
    st.textContent = 'Загрузка PDF...'; st.style.color = '#f0ad4e';
    document.getElementById('btn-go').disabled = true;

    var fd = new FormData();
    fd.append('file', _pdfFile);
    if (_imgFile) fd.append('photo', _imgFile);
    if (_drwFiles) {
        for (var i = 0; i < _drwFiles.length; i++) {
            fd.append('drawings', _drwFiles[i]);
        }
    }

    var r = new XMLHttpRequest();
    r.upload.addEventListener('progress', function(e) {
        if (e.lengthComputable) st.textContent = 'Загрузка ' + Math.round(e.loaded/e.total*100) + '%';
    });
    r.open('POST', '/onis/parse_auto');
    r.addEventListener('load', function() {
        try {
            var d = JSON.parse(r.responseText);
            if (d.task_id) {
                st.textContent = 'Парсинг на GPU...'; st.style.color = '#2EB8AA';
                pollOnisAuto(d.task_id, st);
            } else {
                st.textContent = d.error || 'Ошибка'; st.style.color = '#f44336';
                document.getElementById('btn-go').disabled = false;
            }
        } catch(e) { st.textContent = 'Ошибка'; st.style.color = '#f44336'; }
    });
    r.send(fd);
}

function pollOnisAuto(taskId, stEl) {
    var interval = setInterval(function() {
        var r = new XMLHttpRequest();
        r.open('GET', '/progress/' + taskId);
        r.addEventListener('load', function() {
            try {
                var d = JSON.parse(r.responseText);
                if (d.status === 'done') {
                    clearInterval(interval);
                    stEl.textContent = d.models_found + ' моделей, ' + d.elapsed + 's';
                    stEl.style.color = '#4caf50';
                    var s = new XMLHttpRequest();
                    s.open('POST', '/onis/save_auto/' + taskId);
                    s.addEventListener('load', function() {
                        setTimeout(function() { window.location.reload(); }, 500);
                    });
                    s.send();
                } else if (d.status === 'error') {
                    clearInterval(interval);
                    stEl.textContent = d.error || 'Ошибка'; stEl.style.color = '#f44336';
                    document.getElementById('btn-go').disabled = false;
                } else {
                    stEl.textContent = (d.progress||0) + '% парсинг...';
                }
            } catch(e) {}
        });
        r.send();
    }, 1500);
}

function deleteCatalog(name) {
    if (!confirm('Удалить каталог ' + name + '?')) return;
    var r = new XMLHttpRequest();
    r.open('POST', '/onis/delete/' + name);
    r.addEventListener('load', function() {
        try {
            var d = JSON.parse(r.responseText);
            if (d.ok) {
                var card = document.getElementById('card-' + name);
                if (card) card.remove();
            }
        } catch(e) { window.location.reload(); }
    });
    r.addEventListener('error', function() { window.location.reload(); });
    r.send();
}

function uploadPhoto(name, file) {
    if (!file) return;
    setStatus(name, 'Загрузка фото...', '#f0ad4e');
    var fd = new FormData();
    fd.append('photo', file);
    var r = new XMLHttpRequest();
    r.open('POST', '/upload_photo/' + name);
    r.addEventListener('load', function() {
        var d = JSON.parse(r.responseText);
        if (d.ok) {
            setStatus(name, 'Фото загружено', '#4caf50');
            var ph = document.getElementById('photo-' + name);
            ph.innerHTML = '<img src="/photos/' + name + '.png?t=' + Date.now() + '">';
        } else { setStatus(name, d.error, '#f44336'); }
    });
    r.send(fd);
}

function uploadCatalogPDF(name, file) {
    if (!file) return;
    setStatus(name, 'Загрузка PDF...', '#f0ad4e');
    var fd = new FormData();
    fd.append('file', file);
    var r = new XMLHttpRequest();
    r.open('POST', '/onis/parse/' + name);
    r.upload.addEventListener('progress', function(e) {
        if (e.lengthComputable) {
            var p = Math.round(e.loaded / e.total * 100);
            setStatus(name, 'Загрузка ' + p + '%', '#f0ad4e');
        }
    });
    r.addEventListener('load', function() {
        try {
            var d = JSON.parse(r.responseText);
            if (d.task_id) {
                setStatus(name, 'Парсинг на GPU...', '#2EB8AA');
                pollOnisParse(name, d.task_id);
            } else if (d.error) {
                setStatus(name, '', ''); setMsg(name, d.error, false);
            }
        } catch(e) { setStatus(name, 'Ошибка', '#f44336'); }
    });
    r.send(fd);
}

function pollOnisParse(name, taskId) {
    var interval = setInterval(function() {
        var r = new XMLHttpRequest();
        r.open('GET', '/progress/' + taskId);
        r.addEventListener('load', function() {
            try {
                var d = JSON.parse(r.responseText);
                if (d.status === 'done') {
                    clearInterval(interval);
                    setStatus(name, d.models_found + ' моделей, ' + d.elapsed + 's', '#4caf50');
                    setMsg(name, 'Каталог спарсен и сохранён в базу ONIS', true);
                    // Auto-save to ONIS passport DB
                    var s = new XMLHttpRequest();
                    s.open('POST', '/onis/save/' + taskId + '/' + name);
                    s.send();
                } else if (d.status === 'error') {
                    clearInterval(interval);
                    setStatus(name, '', ''); setMsg(name, d.error, false);
                } else {
                    setStatus(name, (d.progress || 0) + '% парсинг...', '#2EB8AA');
                }
            } catch(e) {}
        });
        r.send();
    }, 1500);
}
</script>
</body>
</html>'''


ONIS_DB_DIR = '/root/pump_base/onis'  # Separate from general bot KB


def load_onis_catalogs():
    """Load ONIS passport catalogs"""
    os.makedirs(ONIS_DB_DIR, exist_ok=True)
    os.makedirs(PHOTOS_DIR, exist_ok=True)
    catalogs = []
    for fname in sorted(os.listdir(ONIS_DB_DIR)):
        if not fname.endswith('.json'):
            continue
        name = fname.replace('.json', '')
        try:
            with open(os.path.join(ONIS_DB_DIR, fname)) as f:
                data = json.load(f)
            count = len(data)
        except Exception:
            data = []
            count = 0

        photo_path = os.path.join(PHOTOS_DIR, f'{name}.png')
        has_photo = os.path.exists(photo_path)
        photo_mtime = int(os.path.getmtime(photo_path)) if has_photo else 0
        drawing_dir = os.path.join(DRAWINGS_DIR, name)
        drawings_list = []
        has_drawing = False
        if os.path.isdir(drawing_dir):
            for df in sorted(os.listdir(drawing_dir)):
                if df.endswith(('.png', '.jpg', '.jpeg')):
                    has_drawing = True
                    label = df.replace('.png','').replace('.jpg','')
                    if label == 'all':
                        label = 'Все'
                    else:
                        label = f'DN{label}'
                    drawings_list.append({
                        'file': df,
                        'label': label,
                        'mtime': int(os.path.getmtime(os.path.join(drawing_dir, df)))
                    })
        # Fallback: old single file
        old_drawing = os.path.join(DRAWINGS_DIR, f'{name}.png')
        if not has_drawing and os.path.exists(old_drawing):
            has_drawing = True
            drawings_list.append({'file': f'../{name}.png', 'label': 'Все', 'mtime': int(os.path.getmtime(old_drawing))})
        drawing_mtime = 0

        # Calculate data quality stats
        import re as _re_stats
        q_filled = sum(1 for m in data if m.get('q_nom', 0) > 0)
        h_filled = sum(1 for m in data if m.get('h_nom', 0) > 0)
        kw_filled = sum(1 for m in data if m.get('power_kw', 0) > 0)
        w_filled = sum(1 for m in data if m.get('weight_net', 0) > 0)
        series_set = set()
        for m in data:
            s = _re_stats.match(r'[A-ZА-Я]+', (m.get('model','') or '').upper().replace('М','M').replace('В','B'))
            if s and len(s.group(0)) <= 5:
                series_set.add(s.group(0))
        q_pct = int(q_filled / count * 100) if count else 0
        h_pct = int(h_filled / count * 100) if count else 0

        catalogs.append({
            'name': name, 'count': count,
            'has_photo': has_photo, 'photo_mtime': photo_mtime,
            'has_drawing': has_drawing, 'drawing_mtime': drawing_mtime,
            'drawings': drawings_list,
            'q_filled': q_filled, 'h_filled': h_filled, 'kw_filled': kw_filled,
            'w_filled': w_filled, 'q_pct': q_pct, 'h_pct': h_pct,
            'series': ', '.join(sorted(series_set)),
        })
    return catalogs


@app.route('/catalogs')
def catalogs_page():
    return render_template_string(CATALOGS_HTML, catalogs=load_onis_catalogs())


@app.route('/onis/create/<name>', methods=['POST'])
def onis_create(name):
    """Create a new ONIS catalog"""
    name = re.sub(r'[^A-Za-z0-9_-]', '', name).upper()
    if not name:
        return jsonify({'ok': False, 'error': 'Некорректное название'})
    os.makedirs(ONIS_DB_DIR, exist_ok=True)
    fpath = os.path.join(ONIS_DB_DIR, f'{name}.json')
    if os.path.exists(fpath):
        return jsonify({'ok': False, 'error': f'Каталог {name} уже существует'})
    with open(fpath, 'w') as f:
        json.dump([], f)
    return jsonify({'ok': True})



@app.route('/upload_drawings/<name>', methods=['POST'])
def upload_drawings(name):
    files = request.files.getlist('drawings')
    if not files:
        return jsonify({'error': 'no files'}), 400
    series_dir = os.path.join(DRAWINGS_DIR, name)
    os.makedirs(series_dir, exist_ok=True)
    count = 0
    for f in files:
        if not f.filename:
            continue
        # Extract DN from filename: INL32.png -> 32, all.png -> all
        fname = f.filename
        import re as _re
        dn_match = _re.search(r'(\d+)', fname)
        if 'all' in fname.lower():
            save_name = 'all.png'
        elif dn_match:
            save_name = f'{dn_match.group(1)}.png'
        else:
            save_name = fname
        f.save(os.path.join(series_dir, save_name))
        count += 1
    return jsonify({'ok': True, 'count': count})


@app.route('/drawings/<path:filename>')
def serve_drawing(filename):
    return send_from_directory(DRAWINGS_DIR, filename)

@app.route('/onis/delete/<name>', methods=['POST', 'DELETE'])
def onis_delete(name):
    """Delete an ONIS catalog"""
    name = re.sub(r'[^A-Za-z0-9_-]', '', name).upper()
    fpath = os.path.join(ONIS_DB_DIR, f'{name}.json')
    if os.path.exists(fpath):
        os.remove(fpath)
    photo = os.path.join(PHOTOS_DIR, f'{name}.png')
    if os.path.exists(photo):
        os.remove(photo)
    import shutil
    drawing_dir = os.path.join(DRAWINGS_DIR, name)
    if os.path.isdir(drawing_dir):
        shutil.rmtree(drawing_dir)
    drawing_single = os.path.join(DRAWINGS_DIR, f'{name}.png')
    if os.path.exists(drawing_single):
        os.remove(drawing_single)
    # Also remove from bot's base
    # Extract clean series name for BASE file
    import re as _re_del
    clean_series = _re_del.sub(r'^(КАТАЛОГ|CATALOG|KATALOG)[_\s-]*', '', name, flags=_re_del.IGNORECASE).strip('_- ')
    for pattern in [f'{name}_BASE.json', f'{clean_series}_BASE.json', f'pump_models_{name}.json', f'pump_models_{clean_series}.json']:
        base_path = os.path.join('/root/pump_base', pattern)
        if os.path.exists(base_path):
            os.remove(base_path)
    return jsonify({'ok': True})


@app.route('/onis/parse_auto', methods=['POST'])
def onis_parse_auto():
    """Upload PDF + optional photo, parse via GPU, name from filename"""
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не выбран'})

    file = request.files['file']
    if not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Только PDF'})

    # Validate PDF magic bytes
    file.seek(0)
    magic = file.read(4)
    file.seek(0)
    if magic != b'%PDF':
        return jsonify({'error': 'Файл повреждён или не является PDF'})

    # Original extension check (kept for clarity)
    if False:
        return jsonify({'error': 'Только PDF'})

    # Extract catalog name from filename
    raw_name = os.path.splitext(file.filename)[0]
    # Clean up common prefixes from catalog names
    raw_name = re.sub(r'^(каталог|katalog|catalog)[_\s-]*', '', raw_name, flags=re.IGNORECASE).strip('_- ')
    # Clean: remove dates, underscores, etc — keep meaningful part
    catalog_name = re.sub(r'[\d_\-\.]+$', '', raw_name).strip('_ -')
    catalog_name = re.sub(r'^\d+[_\-\s]*', '', catalog_name).strip('_ -')
    if not catalog_name:
        catalog_name = raw_name
    # Keep it uppercase, replace spaces
    catalog_name = catalog_name.replace(' ', '_').upper()
    catalog_name = re.sub(r'[^A-ZА-Я0-9_\-]', '', catalog_name)
    if not catalog_name:
        catalog_name = 'CATALOG'
    catalog_name = catalog_name[:50]  # prevent overflow

    # Save PDF
    filename = secure_filename(file.filename) or 'upload.pdf'
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{ts}_{filename}"
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    # Save photo if provided (will be attached after parsing when we know catalog name)
    pending_photo = None
    if 'photo' in request.files:
        photo = request.files['photo']
        if photo.filename:
            photo_tmp = os.path.join(app.config['UPLOAD_FOLDER'], f'photo_{ts}.tmp')
            photo.save(photo_tmp)
            pending_photo = photo_tmp

    # Save drawings if provided
    pending_drawings = []
    drawings_files = request.files.getlist('drawings')
    for df in drawings_files:
        if df.filename:
            drw_tmp = os.path.join(app.config['UPLOAD_FOLDER'], f'drw_{ts}_{df.filename}')
            df.save(drw_tmp)
            pending_drawings.append({'tmp': drw_tmp, 'name': df.filename})

    total_pages = 50
    try:
        import fitz
        doc = fitz.open(filepath)
        total_pages = len(doc)
        doc.close()
    except Exception:
        pass

    task_id = uuid.uuid4().hex[:8]
    parse_tasks[task_id] = {
        'status': 'starting', 'progress': 0,
        'total_pages': total_pages, 'start_time': time.time(),
        'phase': 'Инициализация...', 'onis_mode': True,
        'pending_photo': pending_photo,
        'pending_drawings': pending_drawings,
        'onis_name': catalog_name
    }

    thread = threading.Thread(target=_run_parse_docling, args=(task_id, filepath), daemon=True)
    thread.start()
    return jsonify({'task_id': task_id, 'total_pages': total_pages})


def _sync_onis_to_base(name, models):
    """Create/update {SERIES}_BASE.json for bot from onis data.
    Extracts clean series name from catalog name (КАТАЛОГ_MBL -> MBL).
    """
    import re as _re_sync
    # Detect series from MODELS (not filename) for correct BASE naming
    _KNOWN = {'CDM','CDMF','CDL','CDLF','CV','CVF','CMI','PV','MV','MVS','EVR','EVS','TG','TL','TD','INL','MBL','FV','FVH','FST','FS','FS4','FSM','NBS','EST','ESST','LVR'}
    series_counts = {}
    for _m in models:
        _name = (_m.get('model','') or '').upper().replace('\u041c','M').replace('\u0412','B')
        for _s in sorted(_KNOWN, key=len, reverse=True):
            if _name.startswith(_s):
                series_counts[_s] = series_counts.get(_s, 0) + 1
                break
    if series_counts:
        # Use most common series
        series = max(series_counts, key=series_counts.get)
    else:
        series = _re_sync.sub(r'^(КАТАЛОГ|CATALOG|KATALOG)[_\s-]*', '', name, flags=_re_sync.IGNORECASE).strip('_- ')
        if not series:
            series = name

    HORIZONTAL = {'INL', 'MBL', 'FVH', 'FV', 'FST', 'FS', 'FS4', 'FSM', 'NBS'}
    VERTICAL = {'MV', 'MVS', 'CDM', 'CDMF', 'CDL', 'CDLF', 'CV', 'CVF', 'EVR', 'EVS', 'CMI', 'PV', 'TG', 'TL', 'TD', 'EST', 'ESST'}
    base = []
    for m in models:
        if series.upper() in HORIZONTAL:
            orientation = 'horizontal'
        elif series.upper() in VERTICAL:
            orientation = 'vertical'
        else:
            orientation = 'vertical'
        base.append({
            'id': m['model'],
            'kw': m.get('power_kw', 0),
            'q': m.get('q_nom', 0),
            'head_m': m.get('h_nom', 0),
            'series': series,
            'orientation': orientation,
            'flagship': series in ('MV', 'INL', 'MBL'),
            'brand': brand_for_series(series),
        })

    # Series must look like a real pump series: 2-5 latin letters, optionally followed by a digit
    import re as _re_guard
    series = series.upper().strip()
    if not _re_guard.match(r'^[A-Z]{2,5}\d?$', series):
        print(f"Skip sync: '{series}' doesn't look like a pump series")
        return
    base_path = os.path.join('/root/pump_base', f'{series}_BASE.json')
    with open(base_path, 'w', encoding='utf-8') as f:
        json.dump(base, f, ensure_ascii=False, indent=2)
    print(f"Synced {series}_BASE.json: {len(base)} models")
    # Rebuild brands_index.json
    _rebuild_brands_index()


def _rebuild_brands_index():
    idx = {}
    for fn in os.listdir('/root/pump_base'):
        if not fn.endswith('_BASE.json'):
            continue
        fp = os.path.join('/root/pump_base', fn)
        try:
            with open(fp) as fh:
                data = json.load(fh)
            if not data:
                continue
            s = fn.replace('_BASE.json', '')
            b = data[0].get('brand', 'Unknown')
            fl = data[0].get('flagship', False)
            if b not in idx:
                idx[b] = {'series': []}
            idx[b]['series'].append({'name': s, 'count': len(data), 'flagship': fl})
        except Exception:
            pass
    for b in idx:
        idx[b]['series'].sort(key=lambda x: (-int(x['flagship']), -x['count']))
    with open('/root/pump_base/brands_index.json', 'w') as fh:
        json.dump(idx, fh, ensure_ascii=False, indent=2)


@app.route('/onis/save_auto/<task_id>', methods=['POST'])
def onis_save_auto(task_id):
    """Auto-detect catalog type and save to ONIS passport DB"""
    task = parse_tasks.get(task_id)
    if not task or 'parsed_data' not in task:
        return jsonify({'ok': False, 'error': 'Нет данных'})

    # Use filename-based name if available, fallback to auto-detect
    name = task.get('onis_name') or task.get('catalog_type', 'PUMP')
    name = name.upper()

    os.makedirs(ONIS_DB_DIR, exist_ok=True)
    fpath = os.path.join(ONIS_DB_DIR, f'{name}.json')

    existing = []
    if os.path.exists(fpath):
        try:
            with open(fpath) as f:
                existing = json.load(f)
        except Exception:
            pass

    new_models = {_normalize_model_key(m['model']): m for m in task['parsed_data']}
    for old in existing:
        k = _normalize_model_key(old.get('model', ''))
        if k not in new_models:
            new_models[k] = old

    merged = sorted(new_models.values(), key=lambda x: (x.get('series', ''), x.get('q_nom', 0)))
    with open(fpath, 'w', encoding='utf-8') as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # Enrich models before syncing to BASE
    for m in merged:
        _enrich_from_model_name(m)
        _validate_pump_physics(m)

    # Re-save onis with enriched data
    with open(fpath, 'w', encoding='utf-8') as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # Sync to bot BASE file
    _sync_onis_to_base(name, merged)

    # Attach pending photo if provided
    # Save drawings to series dir
    pending_drawings = task.get('pending_drawings', [])
    if pending_drawings and name:
        import re as _re3
        drw_dir = os.path.join(DRAWINGS_DIR, name)
        os.makedirs(drw_dir, exist_ok=True)
        for d in pending_drawings:
            tmp_path = d['tmp']
            fname = d['name']
            # Extract DN from filename
            dn_match = _re3.search(r'(\d+)', fname)
            if 'all' in fname.lower():
                save_name = 'all.png'
            elif dn_match:
                save_name = f'{dn_match.group(1)}.png'
            else:
                save_name = fname
            if os.path.exists(tmp_path):
                shutil.copy2(tmp_path, os.path.join(drw_dir, save_name))
                os.remove(tmp_path)

    pending_photo = task.get('pending_photo')
    if pending_photo and os.path.exists(pending_photo):
        os.makedirs(PHOTOS_DIR, exist_ok=True)
        photo_dest = os.path.join(PHOTOS_DIR, f'{name}.png')
        try:
            from PIL import Image
            img = Image.open(pending_photo)
            if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                img = img.convert('RGBA')
            else:
                img = img.convert('RGB')
            if img.width > 1200:
                ratio = 1200 / img.width
                img = img.resize((1200, int(img.height * ratio)), Image.LANCZOS)
            img.save(photo_dest, 'PNG', quality=90)
        except ImportError:
            import shutil
            shutil.copy2(pending_photo, photo_dest)
        os.remove(pending_photo)

    return jsonify({'ok': True, 'name': name, 'count': len(merged)})


@app.route('/upload_photo/<catalog_name>', methods=['POST'])
def upload_photo(catalog_name):
    """Upload photo for a catalog/series"""
    if 'photo' not in request.files:
        return jsonify({'ok': False, 'error': 'Файл не выбран'})

    file = request.files['photo']
    if file.filename == '':
        return jsonify({'ok': False, 'error': 'Файл не выбран'})

    # Accept any image
    allowed = ('.png', '.jpg', '.jpeg', '.webp', '.gif')
    if not any(file.filename.lower().endswith(ext) for ext in allowed):
        return jsonify({'ok': False, 'error': 'Только изображения (PNG, JPG, WEBP)'})

    try:
        os.makedirs(PHOTOS_DIR, exist_ok=True)
        # Always save as PNG for consistency
        from PIL import Image
        img = Image.open(file.stream)
        if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
            img = img.convert('RGBA')
        else:
            img = img.convert('RGB')

        # Resize if too large (max 1200px wide)
        if img.width > 1200:
            ratio = 1200 / img.width
            img = img.resize((1200, int(img.height * ratio)), Image.LANCZOS)

        photo_path = os.path.join(PHOTOS_DIR, f'{catalog_name}.png')
        img.save(photo_path, 'PNG', quality=90)

        size_kb = os.path.getsize(photo_path) // 1024
        return jsonify({'ok': True, 'message': f'Фото сохранено ({size_kb} КБ)'})
    except ImportError:
        # Fallback without PIL — save raw
        os.makedirs(PHOTOS_DIR, exist_ok=True)
        photo_path = os.path.join(PHOTOS_DIR, f'{catalog_name}.png')
        file.save(photo_path)
        return jsonify({'ok': True, 'message': 'Фото сохранено'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/photos/<filename>')
def serve_photo(filename):
    """Serve catalog photos"""
    photo_path = os.path.join(PHOTOS_DIR, filename)
    if os.path.exists(photo_path):
        return send_file(photo_path)
    return '', 404


application = app

# === BACKUP API ===
BACKUP_API_KEY = os.environ.get("BACKUP_API_KEY", "")
BACKUP_DIRS = {
    'pump_base': '/root/pump_base',
    'onis_db': '/root/pump_base/onis',
    'photos': '/root/pump_base/photos',
    'drawings': '/root/pump_base/drawings',
}

@app.route('/api/backup', methods=['GET'])
def api_backup():
    """Download full backup as ZIP. Requires API key."""
    key = request.args.get('key') or request.headers.get('X-API-Key')
    if key != BACKUP_API_KEY:
        return jsonify({'error': 'unauthorized'}), 403

    import zipfile, io, time
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for label, dirpath in BACKUP_DIRS.items():
            if not os.path.isdir(dirpath):
                continue
            for root, dirs, files in os.walk(dirpath):
                for fname in files:
                    fpath = os.path.join(root, fname)
                    arcname = os.path.join(label, os.path.relpath(fpath, dirpath))
                    zf.write(fpath, arcname)
    buf.seek(0)
    ts = time.strftime('%Y%m%d_%H%M%S')
    return send_file(buf, mimetype='application/zip',
                     as_attachment=True, download_name=f'onis_backup_{ts}.zip')


@app.route('/api/restore', methods=['POST'])
def api_restore():
    """Restore from ZIP backup. Requires API key."""
    key = request.form.get('key') or request.headers.get('X-API-Key')
    if key != BACKUP_API_KEY:
        return jsonify({'error': 'unauthorized'}), 403

    if 'file' not in request.files:
        return jsonify({'error': 'no file'}), 400

    import zipfile, io
    f = request.files['file']
    zf = zipfile.ZipFile(io.BytesIO(f.read()))
    restored = 0
    for name in zf.namelist():
        parts = name.split('/', 1)
        if len(parts) < 2 or not parts[1]:
            continue
        label, relpath = parts
        if label not in BACKUP_DIRS:
            continue
        dest = os.path.join(BACKUP_DIRS[label], relpath)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, 'wb') as out:
            out.write(zf.read(name))
        restored += 1
    return jsonify({'ok': True, 'restored': restored})


@app.route('/api/backup/status', methods=['GET'])
def api_backup_status():
    """Show what would be backed up."""
    key = request.args.get('key') or request.headers.get('X-API-Key')
    if key != BACKUP_API_KEY:
        return jsonify({'error': 'unauthorized'}), 403

    result = {}
    total_size = 0
    for label, dirpath in BACKUP_DIRS.items():
        files = []
        if os.path.isdir(dirpath):
            for root, dirs, fnames in os.walk(dirpath):
                for fn in fnames:
                    fp = os.path.join(root, fn)
                    sz = os.path.getsize(fp)
                    total_size += sz
                    files.append({'name': os.path.relpath(fp, dirpath), 'size_kb': sz // 1024})
        result[label] = {'files': len(files), 'items': files}
    result['total_size_mb'] = round(total_size / 1024 / 1024, 1)
    return jsonify(result)



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
