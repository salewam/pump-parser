#!/usr/bin/env python3
"""
Universal Pump Parser v10 — Hardcoded 17 ONIS series + Universal pipeline for any PDF.
=======================================================================================
Handles all 17 catalog PDFs with catalog-specific extractors tuned to exact text formats,
PLUS a universal pipeline for unknown/new PDF catalogs.

Changes from v9:
- Bug #1: Logger name "pump_parser" (was "pump_parser_v8")
- Bug #2: CV data_source -> "estimated" (H is derived from stages, not from catalog)
- Bug #3: KMG model name uses KMG_{type}_{rpm}rpm (removed power from model name)
- Bug #4: estimate_power_hydraulic uses pump_validators version (no 0.95 multiplier)
- Bug #5: validate_entry uses validate_entry_basic from pump_validators (allows H=0)
- Bug #6: Added Q-H monotonicity check via validate_pump_physics
- Bug #7: Logger name consistency throughout
- NEW: Universal pipeline for unknown PDFs (detect manufacturer -> classify page -> extract tables -> classify columns -> validate)
- NEW: PumpEntry fields: confidence, manufacturer
- NEW: Schema version 10.0
- NEW: Imports from pump_validators and pump_table_classifier modules
"""

import re
import json
import os
import sys
import time
import logging
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path

import fitz  # PyMuPDF

# Import from companion modules
from pump_validators import (
    validate_entry_basic,
    validate_pump_physics,
    estimate_power_hydraulic,
    normalize_model_name,
    convert_q,
    convert_h,
    convert_p,
    ETA_SMALL,
    ETA_MEDIUM,
    ETA_LARGE,
    IEC_MOTOR_SIZES,
    DS_CATALOG_CURVE,
    DS_CATALOG_NOMINAL,
    DS_ESTIMATED,
    DS_UNIVERSAL_TABLE,
    DS_UNIVERSAL_QH,
    DS_UNIVERSAL_TEXT,
    DS_GEMINI,
    DS_GEMINI_UNVERIFIED,
)
from pump_table_classifier import (
    classify_columns,
    is_pump_table,
    dataframe_to_pump_dicts,
    detect_transposed,
    detect_qh_matrix,
    parse_qh_matrix,
    detect_spec_table,
    parse_spec_table,
    parse_selection_chart,
)

# Try import pdfplumber (optional)
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# Try import pandas (for find_tables)
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# Try import google-genai (optional, for Gemini vision fallback)
try:
    from google import genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

# Try import openai SDK (for OpenRouter vision fallback)
try:
    from openai import OpenAI as _OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

# Try import Pillow (optional, for page collage)
try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s", stream=sys.stderr)
log = logging.getLogger("pump_parser")  # Bug #1, #7: was "pump_parser_v8"

# ---- Physical Constants -------------------------------------------------------

RHO_WATER = 1000.0   # kg/m3 (water density)
G_GRAVITY = 9.81     # m/s2 (gravitational acceleration)
KW_PER_W = 1e-3      # conversion factor W -> kW

# ETA_SMALL, ETA_MEDIUM, ETA_LARGE and IEC_MOTOR_SIZES imported from pump_validators
# STANDARD_MOTOR_SIZES kept as alias for backward compat with parse_* functions below
STANDARD_MOTOR_SIZES = IEC_MOTOR_SIZES

# BEP fraction for VO pumps (H_nom / H_max)
_VO_BEP_FRACTION = 0.78


# ---- Data Model ---------------------------------------------------------------

@dataclass
class PumpEntry:
    """Represents one pump model with all its hydraulic and physical parameters."""
    model: str
    series: str
    article: str = ""
    q_nom: float = 0.0
    h_nom: float = 0.0
    power_kw: float = 0.0
    rpm: int = 2900
    voltage: str = "380"
    dn_in: int = 0
    dn_out: int = 0
    q_points: List[float] = field(default_factory=list)
    h_points: List[float] = field(default_factory=list)
    dims: Dict[str, float] = field(default_factory=dict)
    weight_kg: float = 0.0
    source_file: str = ""
    page: int = 0
    data_source: str = "estimated"   # catalog_curve | catalog_nominal | estimated | universal_table | universal_pdfplumber | universal_text
    warnings: List[str] = field(default_factory=list)
    confidence: float = 1.0          # NEW v10: 1.0 for hardcoded, 0.5-0.8 for universal
    manufacturer: str = ""           # NEW v10: detected manufacturer name

    def has_curve(self) -> bool:
        return len(self.q_points) >= 3

    def to_dict(self) -> dict:
        return asdict(self)


# ---- Helpers -------------------------------------------------------------------

def pf(s) -> Optional[float]:
    """Parse float from string, handling commas, spaces, Cyrillic chars."""
    if not s:
        return None
    s = str(s).strip().replace(',', '.').replace('\xa0', '').replace(' ', '')
    s = re.sub(r'\.$', '', s)
    try:
        return float(s)
    except ValueError:
        return None


def pf_slash(s: str) -> Optional[float]:
    """Parse power value that may be in slash format: '0,37 / 0,37' or '-- / 2,2'.

    Returns the 3-phase value (after slash if present, before if no slash).
    """
    if not s:
        return None
    s = s.strip()
    slash_m = re.search(r'[/]', s)
    if slash_m:
        parts = s.split('/', 1)
        three_phase = pf(parts[1].strip())
        if three_phase is not None and three_phase > 0:
            return three_phase
        one_phase = pf(parts[0].strip())
        if one_phase is not None and one_phase > 0:
            return one_phase
        return None
    if re.match(r'^[\u2013\-\u2014\u2212]$', s.strip()):
        return None
    return pf(s)


def pdf_pages(path: str) -> List[Tuple[int, str]]:
    """Return [(page_num_1based, text)] for all pages."""
    doc = fitz.open(path)
    result = [(i+1, doc[i].get_text()) for i in range(len(doc))]
    doc.close()
    return result


def extract_nums_from_block(text: str) -> List[float]:
    """Extract all valid floats from a text block."""
    vals = []
    for m in re.finditer(r'[\d]+[,.]?[\d]*', text):
        v = pf(m.group())
        if v is not None:
            vals.append(v)
    return vals


# Bug #4: Use estimate_power_hydraulic from pump_validators (no 0.95 multiplier).
# The imported version rounds UP to IEC motor size without a safety factor.
# We keep a local _estimate_power_local only for backward compat in VO parser
# where the imported one is already correct.


# ---- CATALOG DETECTION ---------------------------------------------------------

CATALOG_MAP: Dict[str, str] = {
    "CDM_CDMF_241125":                                    "CDM",
    "Katalog-CMI":                                        "CMI",
    "Katalog-CV":                                         "CV",
    "Katalog-NBS":                                        "NBS",
    "Katalog-TG":                                         "TG",
    "TD_LLT":                                             "LLT",
    "cdlf_modeli_120":                                    "CDLF_LARGE",
    "cdlf":                                               "CDLF",
    "cdmf_modeli_32":                                     "CDMF_LARGE",
    "cdmf":                                               "CDMF_FANCY",
    "\u041a\u0430\u0442\u0430\u043b\u043e\u0433 INL":     "INL",
    "\u041a\u0430\u0442\u0430\u043b\u043e\u0433 PV":      "PV",
    "fancy_fst":                                          "FST",
    "s1f9dba":                                            "CHLF",
    "3012kuvf":                                           "BM",
    "erxdpzux":                                           "KMG",
    "rkohe9dt":                                           "VO",
}


def detect_catalog(path: str) -> str:
    """Identify catalog type from filename fragment."""
    fname = os.path.basename(path)
    for fragment, key in CATALOG_MAP.items():
        if fragment in fname:
            return key
    return "UNKNOWN"


# =============================================================================
# CMI / NBS - Simple nominal table: Model | Article | Qnom | Hnom | P2
# =============================================================================

RE_NOMINAL = re.compile(
    r'([A-Z\u0410-\u042f\u0401][A-Z\u0410-\u042f\u0401a-z\u0430-\u044f\u0451]{1,5}[\s\-_][\d][\d/\-_.a-zA-Z()\s]{1,40}?)\s+'
    r'(\d{6,9})\s+'
    r'([\d,.]+)\s+'
    r'([\d,.]+)\s+'
    r'([\d,.]+)',
    re.MULTILINE
)


def _normalize_rpm(val: int) -> int:
    """Normalize motor slip RPM to synchronous speed."""
    # 2-pole: 2880/2950/2960 -> 2900
    if 2850 <= val <= 2970:
        return 2900
    # 4-pole: 1450/1475/1480/1490 -> 1450
    if 1440 <= val <= 1500:
        return 1450
    # 6-pole: 960/970/980 -> 960
    if 950 <= val <= 990:
        return 960
    return val


# Bug #27: Cyrillic-to-Latin map for model name cleanup
_CYR2LAT = str.maketrans(
    "\u0410\u0412\u0415\u041a\u041c\u041d\u041e\u0420\u0421\u0422\u0425"
    "\u0430\u0432\u0435\u043a\u043c\u043d\u043e\u0440\u0441\u0442\u0445",
    "ABEKMHOPCTX" "abekmhopctx")


def _clean_model_name(name: str) -> str:
    """Bug #27: Clean model name — Cyrillic lookalikes to Latin, strip footnote markers."""
    s = name.strip()
    s = s.translate(_CYR2LAT)
    s = s.rstrip("*")
    return s



def parse_nominal(pages: List[Tuple[int, str]], series_hint: str, src: str) -> List[PumpEntry]:
    """Parse CMI/NBS nominal table: Model | Article | Q_nom | H_nom | P2."""
    entries, seen = [], set()
    for pn, text in pages:
        if "\u0410\u0440\u0442\u0438\u043a\u0443\u043b" not in text and "Q\u043d\u043e\u043c" not in text:
            continue
        for m in RE_NOMINAL.finditer(text):
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            article = m.group(2)
            q, h, p = pf(m.group(3)), pf(m.group(4)), pf(m.group(5))
            if q is None or h is None or (model, article) in seen:
                continue
            seen.add((model, article))
            s_m = re.match(r'^([A-Z\u0410-\u042f\u0401a-zA-Z()\-]+)', model)
            series = s_m.group(1).strip().rstrip('-') if s_m else series_hint
            vol = "220" if "220" in model or "_220_" in model else "380"
            entries.append(PumpEntry(
                model=model, series=series, article=article,
                q_nom=q, h_nom=h, power_kw=p or 0.0,
                voltage=vol, source_file=src, page=pn,
                data_source='catalog_nominal',
                confidence=1.0, manufacturer="ONIS",
            ))
    return entries


# =============================================================================
# NBS - Dedicated parser for alternating graph/table pages
# =============================================================================

def parse_nbs(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse NBS catalog: dedicated parser for table pages."""
    entries, seen = [], set()

    for pn, text in pages:
        if 'NBS' not in text:
            continue
        if 'Q\u043d\u043e\u043c' not in text or '\u041d\u043d\u043e\u043c' not in text:
            continue

        lines = [l.strip() for l in text.split('\n')]

        i = 0
        while i < len(lines):
            ls = lines[i]
            if not ls.startswith('NBS '):
                i += 1
                continue

            model = ls
            if model in seen:
                i += 1
                continue

            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j >= len(lines):
                break

            art_line = lines[j].strip()
            if not re.match(r'^\d{8}$', art_line):
                i += 1
                continue
            art = art_line

            vals = []
            k = j + 1
            while k < len(lines) and len(vals) < 5:
                ls2 = lines[k].strip()
                if ls2.startswith('NBS ') or re.match(r'^\d{8}$', ls2):
                    break
                if ls2 in ('\u0420\u0410\u0417\u041c\u0415\u0420\u042b', '\u041c\u043e\u0434\u0435\u043b\u044c', '\u0410\u0440\u0442\u0438\u043a\u0443\u043b'):
                    break
                v = pf(ls2.replace(',', '.'))
                if v is not None and 0 < v < 100000:
                    vals.append(v)
                elif ls2 and not re.match(r'^[\d.,]+$', ls2):
                    volt_m = re.match(r'^(220|380)$', ls2)
                    if volt_m:
                        vals.append(float(ls2))
                    elif vals:
                        break
                k += 1

            if len(vals) >= 3:
                q, h, p2 = vals[0], vals[1], vals[2]
                vol_str = "380"
                if len(vals) >= 4:
                    if vals[3] in (220.0, 380.0):
                        vol_str = str(int(vals[3]))

                seen.add(model)
                s_m = re.match(r'^([A-Z]+)', model)
                series = s_m.group(1) if s_m else 'NBS'

                entries.append(PumpEntry(
                    model=model, series=series, article=art,
                    q_nom=q, h_nom=h, power_kw=p2,
                    voltage=vol_str, source_file=src, page=pn,
                    data_source='catalog_nominal',
                    confidence=1.0, manufacturer="ONIS",
                ))
            i = k

    return entries


# =============================================================================
# TG / TL / TD - Table with model | article | P2 | Qnom | Hnom
# =============================================================================

def parse_tg(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse TG/TL/TD catalog: tabular data with model, article, P2, Q, H."""
    entries, seen = [], set()
    for pn, text in pages:
        if not any(x in text for x in ["TG ", "TL ", "TD "]):
            continue

        pat = re.compile(
            r'(T[GLDAi]\s+\d+[\-/\d\s]+T)\s+'
            r'(\d{7,9})\s+'
            r'([\d,]+)\s+'
            r'(?:(?:T[GLDA]\s+[\d\-/\s]+|[\-\u2013])\s+\d{7,9}\s+[\d,]+\s+)?'
            r'([\d,]+)\s+'
            r'([\d,]+)',
            re.MULTILINE
        )
        for m in pat.finditer(text):
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            # Normalize model: remove spaces around dashes and slashes
            model = re.sub(r'\s*-\s*', '-', model)
            model = re.sub(r'\s*/\s*', '/', model)
            # Skip TL models — TL data format is Art/Q/H/P (not Art/P/Q/H like TG)
            # TL models are handled by the dedicated TL fallback parser
            if model.startswith('TL'):
                continue
            art = m.group(2)
            p2 = pf(m.group(3))
            q, h = pf(m.group(4)), pf(m.group(5))
            if q is None or h is None or model in seen:
                continue
            seen.add(model)
            s_m = re.match(r'^([A-Z]+)', model)
            series = s_m.group(1) if s_m else "TG"
            entries.append(PumpEntry(
                model=model, series=series, article=art,
                q_nom=q, h_nom=h, power_kw=p2 or 0.0,
                source_file=src, page=pn,
                data_source='catalog_nominal',
                confidence=1.0, manufacturer="ONIS",
            ))

        # Fallback: simple nominal pattern
        for m in RE_NOMINAL.finditer(text):
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            if not any(x in model for x in ["TG", "TL", "TD"]):
                continue
            # Normalize model: remove spaces around dashes and slashes
            model = re.sub(r'\s*-\s*', '-', model)
            model = re.sub(r'\s*/\s*', '/', model)
            # Skip 1-phase duplicates (non-T suffix) if T-variant already parsed
            if not model.endswith('T') and model + 'T' in seen:
                continue
            art = m.group(2)
            q, h, p2 = pf(m.group(3)), pf(m.group(4)), pf(m.group(5))
            if q is None or h is None or model in seen:
                continue
            seen.add(model)
            s_m = re.match(r'^([A-Z]+)', model)
            series = s_m.group(1) if s_m else "TG"
            entries.append(PumpEntry(
                model=model, series=series, article=art,
                q_nom=q, h_nom=h, power_kw=p2 or 0.0,
                source_file=src, page=pn,
                data_source='catalog_nominal',
                confidence=1.0, manufacturer="ONIS",
            ))
    # ---- TL fallback: vertical format on dimension pages ----
    # TL pages have: Model line, then Article, Q, H, P on separate lines
    for pn, text in pages:
        if "TL " not in text:
            continue
        lines_raw = text.split('\n')
        lines_s = [l.strip() for l in lines_raw]
        for i, ls in enumerate(lines_s):
            # Match TL model pattern: "TL 32-8/4T" or "TL 40-2/4T, BQQE"
            tm = re.match(r'^(TL\s+\d+[-/\d]+T)(?:\s*,.*)?$', ls)
            if not tm:
                continue
            tl_model = re.sub(r'\s+', ' ', tm.group(1)).strip()
            tl_model = re.sub(r'\s*-\s*', '-', tl_model)
            tl_model = re.sub(r'\s*/\s*', '/', tl_model)
            if tl_model in seen:
                continue
            # Read next non-empty lines: article, Q, H, P
            vals = []
            j = i + 1
            while j < min(i + 12, len(lines_s)) and len(vals) < 5:
                v_line = lines_s[j]
                if not v_line:
                    j += 1
                    continue
                # Stop at next model or header
                if re.match(r'^(TL|TG|TD|Размер|Модель|Qном|ДИАГРАММ)', v_line):
                    break
                v = pf(v_line.replace(',', '.'))
                if v is not None:
                    vals.append(v)
                j += 1
            # vals should be: [article, Q, H, P, weight] or [Q, H, P, weight]
            if len(vals) >= 4:
                # If first val > 1000000, it's an article number
                if vals[0] > 1000000:
                    art_tl = str(int(vals[0]))
                    q_tl, h_tl, p_tl = vals[1], vals[2], vals[3]
                else:
                    art_tl = ''
                    q_tl, h_tl, p_tl = vals[0], vals[1], vals[2]
                if q_tl > 0 and h_tl > 0 and p_tl > 0 and q_tl < 5000 and h_tl < 500:
                    seen.add(tl_model)
                    entries.append(PumpEntry(
                        model=tl_model, series='TL', article=art_tl,
                        q_nom=q_tl, h_nom=h_tl, power_kw=p_tl,
                        rpm=1450,
                        source_file=src, page=pn,
                        data_source='catalog_nominal',
                        confidence=1.0, manufacturer="ONIS",
                    ))

    # ---- TD 125-300 fallback: vertical format on data pages ----
    # TD pages have: Model line, then Article, P2[kW], Q[m³/h], H[m], Weight on separate lines
    # Note: P2 comes BEFORE Q (unlike TL where Q comes before H before P)
    for pn, text in pages:
        if "TD " not in text or "\u0422" not in text:
            # Only pages with TD + Cyrillic \u0422 (3-phase marker)
            continue
        lines_raw = text.split('\n')
        lines_s = [l.strip() for l in lines_raw]
        for i, ls in enumerate(lines_s):
            # Match TD model: "TD 125-11/4\u0422" or "TD 300-55/4\u0422"
            # \u0422 = Cyrillic capital Te (3-phase marker)
            tm = re.match(r'^(TD\s+\d+[-\d.]+/4[\u0422T])$', ls)
            if not tm:
                continue
            td_model = re.sub(r'\s+', ' ', tm.group(1)).strip()
            td_model = re.sub(r'\s*-\s*', '-', td_model)
            td_model = re.sub(r'\s*/\s*', '/', td_model)
            td_model = td_model.replace(chr(0x0422), "T")  # Cyrillic T -> Latin T
            if td_model in seen:
                continue
            # Read next non-empty numeric lines: article, P2, Q, H, weight
            vals = []
            j = i + 1
            while j < min(i + 12, len(lines_s)) and len(vals) < 5:
                v_line = lines_s[j]
                if not v_line:
                    j += 1
                    continue
                # Stop at next model or header
                if re.match(r'^(TD\s+\d|TG|TL|\u0420\u0430\u0437\u043c\u0435\u0440|\u041c\u043e\u0434\u0435\u043b\u044c|Q\u043d\u043e\u043c|\u0414\u0418\u0410\u0413\u0420)', v_line):
                    break
                v = pf(v_line.replace(',', '.'))
                if v is not None:
                    vals.append(v)
                j += 1
            # vals should be: [article, P2, Q, H, weight]
            if len(vals) >= 4:
                # If first val > 1000000, it's article number
                if vals[0] > 1000000:
                    art_td = str(int(vals[0]))
                    p_td, q_td, h_td = vals[1], vals[2], vals[3]
                else:
                    art_td = ''
                    p_td, q_td, h_td = vals[0], vals[1], vals[2]
                if q_td > 0 and h_td > 0 and p_td > 0 and q_td < 5000 and h_td < 500:
                    seen.add(td_model)
                    entries.append(PumpEntry(
                        model=td_model, series='TD', article=art_td,
                        q_nom=q_td, h_nom=h_td, power_kw=p_td,
                        rpm=1450,
                        source_file=src, page=pn,
                        data_source='catalog_nominal',
                        confidence=1.0, manufacturer="ONIS",
                    ))

    # Dedup: if model 'X/2T' exists, remove 'X/2' (duplicate from dimension table)
    t_models = {e.model for e in entries if e.model.endswith('T')}
    entries = [e for e in entries if e.model.endswith('T') or e.model + 'T' not in t_models]
    
    # Physics filter: remove entries with eta > 1.0 or eta < 0.03
    valid = []
    for e in entries:
        if e.q_nom > 0 and e.h_nom > 0 and e.power_kw > 0:
            q_m3s = e.q_nom / 3600
            eta = 1000 * 9.81 * q_m3s * e.h_nom / (e.power_kw * 1000)
            if eta > 1.2 or eta < 0.03:
                log.debug(f'TG/TL skip {e.model}: eta={eta:.2f} (Q={e.q_nom} H={e.h_nom} P={e.power_kw})')
                continue
        valid.append(e)
    entries = valid
    
    return entries


# =============================================================================
# LLT / TD(i) - List format
# =============================================================================

def parse_llt(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse LLT/TD(i) catalog: numbered list with model, Q, H, and power values."""
    entries, seen = [], set()
    for pn, text in pages:
        if not any(x in text for x in ["TD32", "TD50", "TD40", "TD65", "TD80", "TD100",
                                        "TD125", "TD150", "TD200", "TD250", "TD300", "LLT"]):
            continue

                # Skip Q-H curve table pages and dimensions pages
        if any(kw in text for kw in [
            'Таблица характеристик',
            'Графические характеристики',
            'Диаграммы характеристик',
            'Размеры, мм',
            'Размер плит',
        ]):
            continue

        lines = [l.strip() for l in text.split('\n')]

        i = 0
        while i < len(lines):
            line = lines[i]
            if not re.match(r'^\d{1,3}$', line):
                i += 1
                continue

            if i + 1 >= len(lines):
                i += 1
                continue

            next_line = lines[i + 1]

            model_raw = ''
            skip = 1
            if re.match(r'^(?:TD|LLT(?:S|\(S\))?|TD\(I\))\s*\d+', next_line):
                if re.search(r'[-/]\d', next_line):
                    model_raw = next_line
                    skip = 1
                elif i + 2 < len(lines) and re.match(r'^[\-\u2013]\s*[\d,.]+(?:\([Ii]\))?/\d', lines[i+2]):
                    model_raw = next_line + lines[i+2]
                    skip = 2
                else:
                    model_raw = next_line
                    skip = 1
            else:
                i += 1
                continue

            model = re.sub(r'\s+', ' ', model_raw).strip()
            model = re.sub(r'\s*-\s*', '-', model)
            model = re.sub(r'\s*/\s*', '/', model)
            model = model.replace(' ', '')
            # Normalize LLT(S) → LLT, LLTS → LLT
            model = model.replace('LLT(S)', 'LLT').replace('LLTS', 'LLT')
            model = model.rstrip('*')  # Remove trailing asterisk
            if not re.match(r'^(?:TD|LLT)', model.replace('(S)', '').replace('(s)', '')):
                i += 1
                continue

            j = i + 1 + skip
            vals = []
            while j < min(i + 20, len(lines)) and len(vals) < 6:
                v = pf(lines[j].replace(',', '.'))
                if v is not None and 0 < v < 100000:
                    # Check if this is actually the next row number
                    # (small integer on own line, followed by a model name)
                    if v == int(v) and 1 <= v <= 100 and len(vals) >= 3:
                        if j + 1 < len(lines) and re.match(r'^(?:TD|LLT)', lines[j+1]):
                            break  # Don't consume row number as value
                    vals.append(v)
                elif lines[j] and not re.match(r'^[\d.,\s]+$', lines[j]):
                    break
                j += 1

            if len(vals) < 3:
                i += 1
                continue

            rpm_set = (2900.0, 1450.0, 2880.0, 1480.0, 960.0, 720.0)
            if len(vals) >= 4 and vals[2] in rpm_set:
                q, h = vals[0], vals[1]
                rpm = _normalize_rpm(int(vals[2]))
                # After RPM: may have 1-phase P2 then 3-phase P2, or just 3-phase
                p2 = vals[4] if len(vals) >= 5 else vals[3]
            elif len(vals) == 3 and vals[2] in rpm_set:
                # Only Q, H, RPM found — no P2 in scan window
                q, h = vals[0], vals[1]
                rpm = _normalize_rpm(int(vals[2]))
                p2 = estimate_power_hydraulic(q, h)
            else:
                q, h = vals[0], vals[1]
                rpm = 2900
                # vals[2] is P2 (possibly only 3-phase); vals[3] would be next row number
                # P2 must be a valid IEC motor size or reasonable kW value
                p2_candidate = vals[2]
                # Sanity: P2 should not be a row number (1-100) when it's also > Q or H
                # Row numbers are sequential integers; P2 for pumps with Q=12.5 H=33 is 3kW not 10
                if len(vals) >= 4:
                    # If vals[2] looks like a reasonable P2 (matches IEC or is < Q*H/100)
                    # and vals[3] looks like a row number (small integer, sequential)
                    max_reasonable_p2 = max(q * h / 100 * 3, 0.5)  # rough upper bound
                    if p2_candidate <= max_reasonable_p2:
                        p2 = p2_candidate
                    elif vals[3] <= max_reasonable_p2:
                        # Maybe format has two P2 values (1-phase and 3-phase)
                        p2 = vals[3]
                    else:
                        p2 = p2_candidate  # fallback
                else:
                    p2 = p2_candidate

            if model in seen or q <= 0 or h <= 0:
                i = j
                continue
            seen.add(model)

            s_m = re.match(r'^([A-Za-z]+)', model)
            series_raw = s_m.group(1) if s_m else "TD"
            if '(I)' in model or '(i)' in model:
                series = 'TD'
            elif series_raw.startswith('LLT'):
                series = 'LLT'
            else:
                series = series_raw

            entries.append(PumpEntry(
                model=model, series=series,
                q_nom=q, h_nom=h, power_kw=p2 or 0.0,
                rpm=rpm, source_file=src, page=pn,
                data_source='catalog_nominal',
                confidence=1.0, manufacturer="ONIS",
            ))
            i = j

    return entries


# =============================================================================
# CV / CVF - Table
# Bug #2: data_source changed from 'catalog_nominal' to 'estimated'
# =============================================================================

RE_CV = re.compile(
    r'(CV(?:F)?\s+\d+[-\d]*(?:\s+\(IE3\))?)\s+'
    r'(\d{6,9})\s+'
    r'([\d,.]+)\s+'
    r'(\d{3,4})\s+(\d{3,4})\s+(\d{2,4})\s+(\d{2,4})\s+'
    r'([\d,.]+)',
    re.MULTILINE
)


def parse_cv(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse CV/CVF catalog: table with model, article, power, dimensions, mass.
    H_nom is estimated from stage count, so data_source='estimated'."""
    entries, seen = [], set()
    for pn, text in pages:
        if "CV " not in text:
            continue
        for m in RE_CV.finditer(text):
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            art = m.group(2)
            p2 = pf(m.group(3))
            b1, b2, d1, d2 = pf(m.group(4)), pf(m.group(5)), pf(m.group(6)), pf(m.group(7))
            mass = pf(m.group(8))
            key = (model, art)
            if key in seen:
                continue
            seen.add(key)
            q_m = re.search(r'CV(?:F)?\s+(\d+)', model)
            q_nom = float(q_m.group(1)) if q_m else 0.0
            st_m = re.search(r'CV(?:F)?\s+\d+-(\d+)', model)
            stages = int(st_m.group(1)) if st_m else 1
            h_per_stage = 8.5 if q_nom <= 5 else 7.0 if q_nom <= 20 else 5.5 if q_nom <= 64 else 4.0
            h_nom = round(stages * h_per_stage, 1)

            series = "CVF" if "CVF" in model else "CV"
            dims = {k: v for k, v in zip(["B1", "B2", "D1", "D2"], [b1, b2, d1, d2]) if v}
            # Physics check: lower confidence if estimated eta > 0.85
            conf = 0.8
            warn_list = []
            if p2 and p2 > 0 and q_nom > 0 and h_nom > 0:
                eta_est = 9.81 * 998 * (q_nom / 3600) * h_nom / (p2 * 1000)
                if eta_est > 0.85:
                    conf = 0.6
                    warn_list.append(f"estimated_eta={eta_est:.2f}>0.85, H may be overestimated")
            entries.append(PumpEntry(
                model=model, series=series, article=art,
                q_nom=q_nom, h_nom=h_nom, power_kw=p2 or 0.0,
                dims=dims, weight_kg=mass or 0.0,
                source_file=src, page=pn,
                data_source='estimated',
                confidence=conf, manufacturer="ONIS",
                warnings=warn_list,
            ))
    return entries


# =============================================================================
# CDM / CDMF - Page-based table: series header + Q row + model/H rows
# =============================================================================

def parse_cdm(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse CDM/CDMF catalog: per-page Q-H curve tables with power data.
    Handles both compact (CDM3/32) and expanded (CDM125/185) page formats.
    """
    entries, seen = [], set()

    # Model code pattern: MUST contain at least one dash
    # Examples: 3-2, 32-1-1, 125-1, 185-1-В, 125-6*, 185-З-ЗВ, 125-9-2*
    _model_re = re.compile(r'^(\d+[-][\d\-A-ZА-ЯЁа-яё*]+)\s*$')

    for pn, text in pages:
        if "CDM" not in text and "CDMF" not in text:
            continue
        if "Таблица характеристик" not in text and "Q (м3/ч)" not in text and "Q \n(м3/ч)" not in text:
            # Also check for split Q header
            if "Q " not in text or "м3/ч" not in text:
                continue

        lines = text.split('\n')
        stripped = [l.strip() for l in lines]

        # ---- PHASE 1: Extract Q values ----
        q_start = -1
        for i, s in enumerate(stripped):
            if s.startswith('Q') and ('м3/ч' in s or 'м³/ч' in s):
                q_start = i + 1
                break
            # Split header: "Q" on one line, "(м3/ч)" on next
            if s == 'Q' or s.startswith('Q '):
                for j in range(i+1, min(i+4, len(stripped))):
                    if 'м3/ч' in stripped[j] or 'м³/ч' in stripped[j]:
                        q_start = j + 1
                        break
                if q_start > 0:
                    break

        if q_start < 0:
            continue

        q_vals = []
        q_end = q_start
        for i in range(q_start, len(stripped)):
            s = stripped[i]
            if not s:
                continue
            # Stop at model code (contains dash and starts with digit)
            if _model_re.match(s):
                q_end = i
                break
            # Stop at text headers
            if any(c.isalpha() and c not in '.,eE' for c in s):
                # Could be "Модель", "Двигатель", etc. that got mixed in
                # But skip lines like "Серия CDM/CDMF" header noise
                if len(s) > 3:
                    q_end = i
                    break
                continue
            # Parse number(s) from this line (handles "100 110" on same line)
            for num_s in re.findall(r'[\d]+[.,]?[\d]*', s):
                v = pf(num_s)
                if v is not None and v < 10000:
                    q_vals.append(v)
        else:
            q_end = len(stripped)

        if len(q_vals) < 3:
            continue

        # ---- PHASE 2: Extract models line-by-line ----
        # State machine: find model code → read power → skip H header → read H values
        i = q_end
        pending_code = None  # for split codes like "125-9-\n2*"

        while i < len(stripped):
            s = stripped[i]

            # Skip empty lines
            if not s:
                i += 1
                continue

            # Check for model code
            m = _model_re.match(s)

            # Handle split model codes: "125-9-" at end of line
            if pending_code:
                # Append this line to pending code
                combined = pending_code + s
                m2 = _model_re.match(combined)
                if m2:
                    code = m2.group(1)
                    pending_code = None
                    i += 1
                else:
                    # Not a continuation, discard pending
                    pending_code = None
                    continue
            elif m:
                code = m.group(1)
                i += 1
            elif s.endswith('-'):
                # Possible split code: "125-9-" continues on next line
                pending_code = s
                i += 1
                continue
            else:
                i += 1
                continue

            # Skip empty lines after code
            while i < len(stripped) and not stripped[i]:
                i += 1
            if i >= len(stripped):
                break

            # Read power (kW)
            p2_s = stripped[i]
            p2 = pf(p2_s)
            if p2 is None or p2 > 1000:
                # Not a valid power, skip this "model"
                continue
            i += 1

            # Skip "Н" and "(м)" header lines (only first model on page)
            while i < len(stripped):
                s = stripped[i]
                if s in ('Н', 'H', '(м)', '(m)', 'Н (м)', '') or s.startswith('Н') and len(s) <= 5:
                    i += 1
                elif s == '(м)' or s == '(m)':
                    i += 1
                else:
                    break

            # Read H values
            h_vals = []
            while i < len(stripped):
                s = stripped[i]
                if not s:
                    i += 1
                    continue
                # Stop at next model code
                if _model_re.match(s):
                    break
                # Stop at split model code
                if s.endswith('-') and re.match(r'^\d+[-\d]*-$', s):
                    break
                # Stop at section headers
                if 'Серия' in s or 'CDM' in s or 'Модели с' in s:
                    break

                # Parse H numbers (handles "130.5  124.5  " on same line)
                found_num = False
                for num_s in re.findall(r'[\d]+[.,]?[\d]*', s):
                    v = pf(num_s)
                    if v is not None and v < 1000:
                        h_vals.append(v)
                        found_num = True

                if not found_num and any(c.isalpha() for c in s):
                    # Text line = end of H values
                    break

                i += 1

            # ---- PHASE 3: Create entry ----
            n = min(len(q_vals), len(h_vals))
            if n < 3:
                continue

            q_pts = q_vals[:n]
            h_pts = h_vals[:n]

            # Normalize Cyrillic lookalikes to Latin
            code = code.replace('В', 'B')  # В → B
            code = code.replace('А', 'A')  # А → A
            code = code.replace('С', 'C')  # С → C
            code = code.replace('О', 'O')  # О → O
            code = code.replace('З', '3')  # З → 3
            code = code.replace('в', 'b')  # в → b
            code = code.replace('а', 'a')  # а → a
            code = code.rstrip('*')  # Strip catalog annotation asterisk
            model = f"CDM-{code}"
            if model in seen:
                continue
            seen.add(model)

            mid = n // 2
            entries.append(PumpEntry(
                model=model, series="CDM",
                q_nom=q_pts[mid], h_nom=h_pts[mid],
                power_kw=p2 or 0.0,
                q_points=q_pts, h_points=h_pts,
                source_file=src, page=pn,
                data_source='catalog_curve',
                confidence=1.0, manufacturer="ONIS",
            ))

    return entries


# =============================================================================
# CHLF/CHL - Line-based table
# =============================================================================

def parse_chlf(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse CHLF/CHL catalog: Q-H curve tables with per-page Q header."""
    entries, seen = [], set()

    for pn, text in pages:
        if not any(x in text for x in ["CHL", "CHLF", "\u0421\u041dL"]):
            continue
        if "\u0422\u0430\u0431\u043b\u0438\u0446\u0430 \u0445\u0430\u0440\u0430\u043a\u0442\u0435\u0440\u0438\u0441\u0442\u0438\u043a" not in text:
            continue

        lines = text.split('\n')

        q_vals = []
        q_start_idx = -1
        for i, line in enumerate(lines):
            if re.match(r'^Q\s*$', line.strip()) or line.strip() == 'Q':
                block = '\n'.join(lines[i:i+15])
                if '\u043c3' in block or '\u043c\u00b3' in block:
                    for j in range(i+1, min(i+15, len(lines))):
                        v = pf(lines[j].strip())
                        if v is not None and 0 < v < 5000:
                            q_vals.append(v)
                        elif q_vals and lines[j].strip() and not re.match(r'^[\d,.\s]+$', lines[j].strip()):
                            break
                    if len(q_vals) >= 3:
                        q_start_idx = i
                        break

        if not q_vals:
            for i, line in enumerate(lines):
                context = '\n'.join(lines[max(0, i-3):i+3])
                if ('Q' in context or '\u043c\u00b3' in context or '\u043c3' in context):
                    nums = re.findall(r'[\d,]+', line)
                    floats = [v for n in nums if (v := pf(n)) is not None and 0.1 < v < 500]
                    if len(floats) >= 4:
                        q_vals = floats
                        q_start_idx = i
                        break

        if not q_vals:
            continue

        model_pat = re.compile(
            r'((?:C[HL]{2}F?(?:\(T\s*\))?|CHL-?|\u0421[\u041d\u0048][Ll])\s*\d+[- \t]*[\dI]+[-\d]*[A-Z]*)\s*\n'
            r'([\d,.]+)\s*\n'
            r'(?:H\s*\n\s*\[?[\u041c\u004d\u043c]\]?\s*\n)?'
            r'([\d.,\s\n]+?)(?=\n(?:C[HL]|\u0421[\u041d\u0048]|\n|$))',
            re.MULTILINE | re.DOTALL | re.IGNORECASE
        )

        for m in model_pat.finditer(text):
            model_raw = m.group(1).strip()
            model = model_raw.replace('\u0421', 'C').replace('\u041d', 'H').replace('\u0441', 'c').replace('\u043d', 'h')
            model = re.sub(r'\s+', '', model)
            # OCR fixes: I->1 in numeric positions, normalize dashes
            model = re.sub(r'(?<=\d)I', '1', model)  # 20-I0 -> 20-10
            model = re.sub(r'I(?=\d)', '1', model)   # I0 -> 10
            model = re.sub(r'^(CHL)-(\d)', r'\1\2', model)  # CHL-20 -> CHL20
            p2 = pf(m.group(2))
            h_text = m.group(3)

            h_vals = []
            for n in re.findall(r'[\d,]+', h_text):
                v = pf(n)
                if v is not None and 0 < v < 1000:
                    h_vals.append(v)

            if len(h_vals) < 3 or model in seen:
                continue
            # Reject phantom models: valid CHL/CHLF must have dash (e.g. CHLF(T)15-10)
            if not re.search(r'\d+-\d+', model):
                continue
            seen.add(model)

            n = min(len(q_vals), len(h_vals))
            q_pts = q_vals[:n]
            h_pts = h_vals[:n]

            mid = n // 2
            series_m = re.match(r'(CHLF(?:\(T\))?|CHL)', model, re.IGNORECASE)
            series = series_m.group(1).upper() if series_m else "CHLF"

            entries.append(PumpEntry(
                model=model, series=series,
                q_nom=q_pts[mid], h_nom=h_pts[mid],
                power_kw=p2 or 0.0,
                q_points=q_pts, h_points=h_pts,
                source_file=src, page=pn,
                data_source='catalog_curve',
                confidence=1.0, manufacturer="ONIS",
            ))

    return entries


# =============================================================================
# CDLF / CDMF (Fancy/CNP) - International table
# =============================================================================

def parse_cdlf(pages: List[Tuple[int, str]], src: str, series_prefix: str = "CDLF") -> List[PumpEntry]:
    """Parse CDLF/CDMF (Fancy/CNP) catalog: international tabular format."""
    entries, seen = [], set()

    for pn, text in pages:
        if "CDL" not in text and "CDMF" not in text:
            continue

        lines = text.split('\n')

        q_vals = []
        q_line_idx = -1
        for i, line in enumerate(lines):
            if 'm\u00b3/h' in line or '\u043c\u00b3/\u0447' in line or 'm3/h' in line.lower():
                combined = line
                for j in range(i+1, min(i+20, len(lines))):
                    if re.match(r'^[\d.]+$', lines[j].strip()) or lines[j].strip() == '':
                        combined += ' ' + lines[j]
                    else:
                        break
                nums = re.findall(r'[\d.]+', combined)
                floats = [float(n) for n in nums if 0.5 <= float(n) <= 50000]
                if len(floats) >= 3:
                    q_vals = floats
                    q_line_idx = i
                    break

        if not q_vals:
            continue

        model_pat = re.compile(
            r'(CD[LMF]+\s+[\d]+-(?:\d+-)?[\d]+)\s*\n'
            r'(?:\d+x\d+\s*\n)?'
            r'([\d.]+)\s*\n'
            r'(?:[\d.]+\s*\n)?'
            r'([-\d.\s\n]+?)(?=\nCD[LMF]|\n\n|\Z)',
            re.MULTILINE | re.DOTALL
        )
        for m in model_pat.finditer(text):
            model_raw = m.group(1).strip()
            model = re.sub(r'\s+', '_', model_raw)
            p2 = pf(m.group(2))
            h_text = m.group(3)

            q_matched, h_vals = [], []
            parts = re.split(r'[\s\n]+', h_text.strip())
            q_idx = 0
            for part in parts:
                if part == '-' or part == '':
                    q_idx += 1
                    continue
                v = pf(part)
                if v is not None and 0 < v < 2000:
                    if q_idx < len(q_vals):
                        q_matched.append(q_vals[q_idx])
                        h_vals.append(v)
                q_idx += 1

            if len(h_vals) < 3 or model in seen:
                continue
            # Reject phantom models: valid CHL/CHLF must have dash (e.g. CHLF(T)15-10)
            if not re.search(r'\d+-\d+', model):
                continue
            seen.add(model)

            mid = len(h_vals) // 2
            entries.append(PumpEntry(
                model=model, series=series_prefix,
                q_nom=q_matched[mid] if q_matched else 0.0,
                h_nom=h_vals[mid],
                power_kw=p2 or 0.0,
                q_points=q_matched, h_points=h_vals,
                source_file=src, page=pn,
                data_source='catalog_curve',
                confidence=1.0, manufacturer="ONIS",
            ))

    return entries


# =============================================================================
# INL - Two-layer: basic table + detailed Q-H tables
# =============================================================================

RE_INL_BASIC = re.compile(
    r'(\d{6})\s+(INL[\d\-./]+)\s+([\d.]+)\s+([\d.]+)\s+(?:\d{3,4}\s+)?([\d.]+)',
    re.MULTILINE
)


def _inl_parse_curve_page(lines: List[str], pn: int, src: str) -> List[PumpEntry]:
    """Parse one INL Q-H curve page."""
    entries = []

    q_vals = []
    q_idx = None
    for i, line in enumerate(lines):
        ls = line.strip()
        if ls == 'Q':
            for j in range(i+1, min(i+5, len(lines))):
                if '(\u043c3/\u0447)' in lines[j] or '\u043c3/\u0447' in lines[j].lower():
                    q_start = j + 1
                    for k in range(q_start, min(q_start + 30, len(lines))):
                        v = pf(lines[k].strip())
                        if v is not None and 0.1 <= v <= 5000:
                            q_vals.append(v)
                        elif lines[k].strip() and not re.match(r'^[\d.,]+$', lines[k].strip()):
                            break
                    q_idx = j
                    break
            if len(q_vals) >= 2:
                break

    if len(q_vals) < 2:
        return entries

    i = 0
    while i < len(lines):
        ls = lines[i].strip()

        if not re.match(r'^\d{6}$', ls):
            i += 1
            continue

        art = ls
        j = i + 1
        while j < len(lines) and not lines[j].strip():
            j += 1
        if j >= len(lines):
            break

        model_line = lines[j].strip()
        if not model_line.startswith('INL'):
            i += 1
            continue

        k = j + 1
        while k < len(lines) and not lines[k].strip():
            k += 1
        p2 = None
        if k < len(lines):
            p2 = pf(lines[k].strip())
            k += 1

        while k < len(lines) and lines[k].strip() in ('H', '(\u043c)', 'H (\u043c)'):
            k += 1

        h_vals = []
        while k < len(lines):
            ls2 = lines[k].strip()
            if re.match(r'^\d{6}$', ls2):
                break
            if ls2 in ('H', '(\u043c)'):
                k += 1
                continue
            if ls2 and not re.match(r'^[\d.,]+$', ls2):
                if any(kw in ls2 for kw in ['\u0410\u0440\u0442\u0438\u043a\u0443\u043b', '\u041c\u043e\u0434\u0435\u043b\u044c', '\u041c\u043e\u0449\u043d\u043e\u0441\u0442\u044c', 'INL']):
                    break
                k += 1
                continue
            v = pf(ls2)
            if v is not None and 0.1 < v < 2000:
                h_vals.append(v)
            k += 1

        model = model_line
        n = min(len(q_vals), len(h_vals))
        if n >= 2:
            q_pts = q_vals[:n]
            h_pts = h_vals[:n]
            mid = n // 2

            dn_m = re.search(r'INL(\d+)', model)
            dn = int(dn_m.group(1)) if dn_m else 0
            rpm_m = re.search(r'/(\d)$', model)
            rpm = 2900 if not rpm_m or rpm_m.group(1) == '2' else 1450

            ds = 'catalog_curve' if n >= 3 else 'catalog_nominal'
            entries.append(PumpEntry(
                model=model, series="INL", article=art,
                q_nom=q_pts[mid], h_nom=h_pts[mid],
                power_kw=p2 or 0.0,
                rpm=rpm, dn_in=dn,
                q_points=q_pts if n >= 3 else [],
                h_points=h_pts if n >= 3 else [],
                source_file=src, page=pn,
                data_source=ds,
                confidence=1.0, manufacturer="ONIS",
            ))
        i = k

    return entries


def parse_inl(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse INL catalog: two-layer extraction (basic nominal table + Q-H curves)."""
    model_map: Dict[str, PumpEntry] = {}

    # Layer 1: basic nominal table
    for pn, text in pages:
        if '\u041f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c\u043d\u043e\u0441\u0442\u044c' not in text or 'INL' not in text:
            continue
        if '(\u043c3/\u0447)' not in text and '\u043c3/\u0447' not in text.lower():
            continue

        lines = [l.strip() for l in text.split('\n')]

        i = 0
        while i < len(lines):
            if not re.match(r'^\d{6}$', lines[i]):
                i += 1
                continue

            art = lines[i]
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j >= len(lines) or not lines[j].startswith('INL'):
                i += 1
                continue

            model = lines[j]
            vals = []
            k = j + 1
            while k < len(lines) and len(vals) < 6:
                ls = lines[k].strip()
                if re.match(r'^\d{6}$', ls):
                    break
                if ls.startswith('INL'):
                    break
                v = pf(ls)
                if v is not None and 0 < v < 100000:
                    vals.append(v)
                elif ls and not re.match(r'^[\d.,]+$', ls):
                    if any(kw in ls for kw in ['\u0410\u0440\u0442\u0438\u043a\u0443\u043b', '\u041c\u043e\u0434\u0435\u043b\u044c', '\u041f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c\u043d\u043e\u0441\u0442\u044c',
                                                '\u0421\u043a\u043e\u0440\u043e\u0441\u0442\u044c', '\u041c\u043e\u0449\u043d\u043e\u0441\u0442\u044c', '\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435', 'NPSH']):
                        break
                k += 1

            if model not in model_map and len(vals) >= 3:
                q, h = vals[0], vals[1]
                rpm = 2900
                p2 = 0.0
                rest = vals[2:]
                for vi, v in enumerate(rest):
                    if v in (2900.0, 1450.0, 2880.0, 1480.0, 2950.0, 1475.0):
                        rpm = _normalize_rpm(int(v))
                        if vi + 1 < len(rest):
                            p2 = rest[vi + 1]
                        break
                else:
                    rpm_m = re.search(r'/(\d)$', model)
                    rpm = 2900 if not rpm_m or rpm_m.group(1) == '2' else 1450
                    p2 = rest[0] if rest else 0.0

                dn_m = re.search(r'INL(\d+)', model)
                dn = int(dn_m.group(1)) if dn_m else 0

                model_map[model] = PumpEntry(
                    model=model, series="INL", article=art,
                    q_nom=q, h_nom=h, power_kw=p2,
                    rpm=rpm, dn_in=dn, source_file=src, page=pn,
                    data_source='catalog_nominal',
                    confidence=1.0, manufacturer="ONIS",
                )
            i = k

    # Layer 2: Q-H curve tables
    for pn, text in pages:
        if 'INL' not in text:
            continue
        if '(\u043c3/\u0447)' not in text and '\u043c3/\u0447' not in text.lower():
            continue
        if '(\u043c)' not in text:
            continue

        lines = [l.strip() for l in text.split('\n')]
        curve_entries = _inl_parse_curve_page(lines, pn, src)

        for e in curve_entries:
            if e.model not in model_map:
                model_map[e.model] = e
            else:
                existing = model_map[e.model]
                if e.has_curve() and not existing.has_curve():
                    existing.q_points = e.q_points
                    existing.h_points = e.h_points
                    existing.data_source = 'catalog_curve'
                if not existing.article and e.article:
                    existing.article = e.article
                if existing.power_kw == 0 and e.power_kw > 0:
                    existing.power_kw = e.power_kw

    return list(model_map.values())


# =============================================================================
# PV (ONIS) - Two-layer parser
# =============================================================================

_PV_Q_MAX_VALUES = {2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 15.0}


def parse_pv(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse PV catalog: Q-H curve tables (layer 1) + article table (layer 2)."""
    model_map = {}

    # Layer 1: Q-H curve tables
    for pn, text in pages:
        if "PV" not in text or "H (\u043c)" not in text:
            continue

        lines = text.split('\n')
        q_vals = []
        i = 0
        while i < len(lines):
            ls = lines[i].strip()

            if ls == 'Q' and i + 1 < len(lines) and '(\u043c3/\u0447)' in lines[i + 1]:
                q_vals = []
                j = i + 2
                while j < len(lines):
                    v = pf(lines[j].strip())
                    if v is not None and 0 <= v <= 50:
                        q_vals.append(v)
                        j += 1
                    else:
                        break
                i = j
                continue

            if not q_vals:
                i += 1
                continue

            m = re.match(r'^PV(?:\(n\))?\s*\d+[-\d]+', ls)
            if m:
                model_raw = ls
                i += 1
                if i >= len(lines):
                    break
                p2 = pf(lines[i].strip())
                i += 1
                if i < len(lines) and 'H' in lines[i] and '\u043c' in lines[i]:
                    i += 1
                h_vals = []
                while i < len(lines) and len(h_vals) < len(q_vals):
                    v = pf(lines[i].strip())
                    if v is not None and 0 < v < 500:
                        h_vals.append(v)
                        i += 1
                    else:
                        break

                if len(h_vals) < 3:
                    continue

                model = re.sub(r'\(n\)', 'n', model_raw)
                model = re.sub(r'\s+', '', model)

                if model in model_map:
                    if not model_map[model].has_curve():
                        n = min(len(q_vals), len(h_vals))
                        model_map[model].q_points = q_vals[:n]
                        model_map[model].h_points = h_vals[:n]
                    continue

                n = min(len(q_vals), len(h_vals))
                q_pts = q_vals[:n]
                h_pts = h_vals[:n]
                mid = n // 2

                q_m2 = re.search(r'PV(?:n)?(\d+)', model)
                q_nom = float(q_m2.group(1)) if q_m2 else q_pts[mid]

                # Interpolate H_nom at Q_nom on the curve
                h_nom = h_pts[mid]  # default: midpoint
                if q_nom != q_pts[mid] and n >= 2:
                    # Find bracketing Q points for interpolation
                    for ki in range(n - 1):
                        if q_pts[ki] <= q_nom <= q_pts[ki + 1]:
                            dq = q_pts[ki + 1] - q_pts[ki]
                            t = (q_nom - q_pts[ki]) / dq if dq > 0 else 0
                            h_nom = round(h_pts[ki] + t * (h_pts[ki + 1] - h_pts[ki]), 1)
                            break
                    else:
                        # Q_nom outside curve range — use nearest point
                        closest_idx = min(range(n), key=lambda j: abs(q_pts[j] - q_nom))
                        h_nom = h_pts[closest_idx]

                model_map[model] = PumpEntry(
                    model=model, series="PV",
                    q_nom=q_nom, h_nom=h_nom,
                    power_kw=p2 or 0.0,
                    q_points=q_pts, h_points=h_pts,
                    source_file=src, page=pn,
                    data_source='catalog_curve',
                    confidence=1.0, manufacturer="ONIS",
                )
            else:
                i += 1

    # Layer 2: Article table
    for pn, text in pages:
        if "\u0410\u0440\u0442\u0438\u043a\u0443\u043b" not in text or "PV" not in text:
            continue

        lines = text.split('\n')
        i = 0
        while i < len(lines):
            art_m = re.match(r'^(\d{6})$', lines[i].strip())
            if not art_m or i + 1 >= len(lines):
                i += 1
                continue

            art = art_m.group(1)
            model_raw = lines[i + 1].strip()
            if not model_raw.startswith('PV'):
                i += 1
                continue

            model = re.sub(r'\(n\)', 'n', model_raw)
            model = re.sub(r'\s+', '', model)
            i += 2

            vals = []
            j = i
            while j < len(lines) and len(vals) < 6:
                ls = lines[j].strip()
                if '~' in ls or '\u00d7' in ls:
                    j += 1
                    continue
                if re.match(r'^\d{6}$', ls):
                    break
                v = pf(ls)
                if v is not None and 0 < v < 10000:
                    vals.append(v)
                    j += 1
                elif ls and not ls[0].isdigit():
                    break
                else:
                    j += 1
            i = j

            if len(vals) < 2:
                continue

            if vals[0] in _PV_Q_MAX_VALUES and len(vals) >= 3 and vals[2] > vals[0]:
                h_nom = vals[2]
                p2 = vals[3] if len(vals) > 3 else 0.0
            else:
                h_nom = vals[0]
                p2 = vals[2] if len(vals) > 2 else 0.0

            q_m2 = re.search(r'PV(?:n)?(\d+)', model)
            q_nom = float(q_m2.group(1)) if q_m2 else 0.0

            if model in model_map:
                if not model_map[model].article:
                    model_map[model].article = art
            else:
                if h_nom > 0:
                    model_map[model] = PumpEntry(
                        model=model, series="PV", article=art,
                        q_nom=q_nom, h_nom=h_nom,
                        power_kw=p2 or 0.0,
                        source_file=src, page=pn,
                        data_source='catalog_nominal',
                        confidence=1.0, manufacturer="ONIS",
                    )

    return list(model_map.values())


# =============================================================================
# BM(N) - Extract Model+Power from dimensional table
# =============================================================================

BM_SERIES_Q: Dict[int, int] = {
    1: 1, 3: 3, 5: 5, 10: 10, 15: 15, 20: 20, 32: 32,
    45: 45, 64: 64, 95: 95, 125: 125, 155: 155, 185: 185, 215: 215, 255: 255,
}

BM_HEAD_PER_STAGE: Dict[int, float] = {
    # Bug #23 fix: values for series >= 15 were 2-12x too low, causing eta < 0.10
    # Correct values back-calculated from PDF power data with physically valid eta
    1: 8.0, 3: 8.0, 5: 7.5, 10: 7.0, 15: 11.5, 20: 12.0,
    32: 12.5, 45: 18.0, 64: 19.0, 95: 17.0, 125: 20.5, 155: 22.0, 185: 30.0, 215: 33.0, 255: 35.0,
}

RE_BM = re.compile(
    r'(BM(?:N)?\s+\d+[-\d]*[A-Za-z]?)\s*\n'
    r'([\d,.]+)\s*\n'
    r'(\d{3,4})',
    re.MULTILINE
)



def _fix_q_monotonicity(q_pts: List[float], h_pts: List[float]) -> tuple:
    """Fix Q-H curve if Q has one outlier breaking monotonicity.
    Determines which value is the outlier by comparing deviations from neighbors."""
    if len(q_pts) < 3 or len(q_pts) != len(h_pts):
        return q_pts, h_pts
    q = list(q_pts)
    h = list(h_pts)
    # Find violations
    violations = []
    for i in range(1, len(q)):
        if q[i] <= q[i-1]:
            violations.append(i)
    if not violations or len(violations) > 3:
        return q_pts, h_pts
    # For each violation at index i: determine if q[i-1] or q[i] is the outlier
    fixed_indices = set()
    for vi in violations:
        if vi in fixed_indices:
            continue
        # Candidate A: q[vi-1] is wrong
        # Candidate B: q[vi] is wrong
        dev_a = dev_b = float('inf')
        fix_a_idx = fix_a_val = fix_b_idx = fix_b_val = None
        # Deviation of q[vi-1] from its neighbors
        if vi >= 2:
            expected_a = (q[vi-2] + q[vi]) / 2.0
            dev_a = abs(q[vi-1] - expected_a)
            fix_a_idx = vi - 1
            fix_a_val = round(expected_a, 1)
        # Deviation of q[vi] from its neighbors
        if vi < len(q) - 1:
            expected_b = (q[vi-1] + q[vi+1]) / 2.0
            dev_b = abs(q[vi] - expected_b)
            fix_b_idx = vi
            fix_b_val = round(expected_b, 1)
        # Fix the one with higher deviation
        if dev_a >= dev_b and fix_a_idx is not None:
            q[fix_a_idx] = fix_a_val
            fixed_indices.add(fix_a_idx)
        elif fix_b_idx is not None:
            q[fix_b_idx] = fix_b_val
            fixed_indices.add(fix_b_idx)
    # Verify fix worked
    for i in range(1, len(q)):
        if q[i] <= q[i-1]:
            return q_pts, h_pts  # fix failed, return original
    return q, h

def parse_bm(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse BM(N) catalog: extract Model+Power from dimensional table."""
    entries, seen = [], set()
    for pn, text in pages:
        if "BM" not in text:
            continue
        for m in RE_BM.finditer(text):
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            p2 = pf(m.group(2))

            if model in seen:
                continue
            seen.add(model)

            parts_m = re.search(r'BM(?:N)?\s+(\d+)-(\d+)', model)
            if not parts_m:
                continue
            series_num = int(parts_m.group(1))
            stages = int(parts_m.group(2))

            q_nom = float(BM_SERIES_Q.get(series_num, series_num))
            h_ps = BM_HEAD_PER_STAGE.get(series_num, 5.0)
            h_nom = round(stages * h_ps, 1)

            series = "BMN" if "BMN" in model else "BM"
            entries.append(PumpEntry(
                model=model, series=series,
                q_nom=q_nom, h_nom=h_nom,
                power_kw=p2 or 0.0,
                source_file=src, page=pn,
                data_source='estimated',
                confidence=0.8, manufacturer="ONIS",
            ))

    # Also try space-separated pattern
    for pn, text in pages:
        if "BM" not in text:
            continue
        pat2 = re.compile(
            r'(BM(?:N)?\s+\d+[-\d]+[A-Za-z]?)\s+([\d,.]+)\s+\d{3,4}',
            re.MULTILINE
        )
        for m in pat2.finditer(text):
            model = re.sub(r'\s+', ' ', m.group(1)).strip()
            if model in seen:
                continue
            seen.add(model)
            p2 = pf(m.group(2))
            parts_m = re.search(r'BM(?:N)?\s+(\d+)-(\d+)', model)
            if not parts_m:
                continue
            series_num = int(parts_m.group(1))
            stages = int(parts_m.group(2))
            q_nom = float(BM_SERIES_Q.get(series_num, series_num))
            h_ps = BM_HEAD_PER_STAGE.get(series_num, 5.0)
            h_nom = round(stages * h_ps, 1)
            series = "BMN" if "BMN" in model else "BM"
            entries.append(PumpEntry(
                model=model, series=series,
                q_nom=q_nom, h_nom=h_nom,
                power_kw=p2 or 0.0,
                source_file=src, page=pn,
                data_source='estimated',
                confidence=0.8, manufacturer="ONIS",
            ))

    return entries


# =============================================================================
# KMG - Table
# Bug #3: model name uses KMG_{type}_{rpm}rpm (removed power from model name)
# =============================================================================

KMG_H_2900: Dict[str, float] = {
    "32-125": 12.5, "32-125.1": 12.5,
    "32-160": 20.0, "32-160.1": 20.0,
    "32-200": 32.0, "32-200.1": 32.0,
    "32-250": 50.0,
    "40-125": 12.5, "40-125.1": 12.5,
    "40-160": 20.0, "40-160.1": 20.0,
    "40-200": 32.0, "40-200.1": 32.0,
    "40-250": 50.0, "40-250.1": 50.0,
    "40-315": 80.0,
    "50-125": 12.5, "50-125.1": 12.5,
    "50-160": 20.0, "50-160.1": 20.0,
    "50-200": 32.0, "50-200.1": 32.0,
    "50-250": 50.0, "50-250.1": 50.0,
    "50-315": 80.0,
    "65-125": 12.5, "65-125.1": 12.5,
    "65-160": 20.0, "65-160.1": 20.0,
    "65-200": 32.0, "65-200.1": 32.0,
    "65-250": 50.0, "65-250.1": 50.0,
    "65-315": 80.0,
    "80-160": 20.0, "80-160.1": 20.0,
    "80-200": 32.0, "80-200.1": 32.0,
    "80-250": 50.0, "80-250.1": 50.0,
    "80-315": 80.0, "80-400": 125.0,
    "100-160": 20.0, "100-160.1": 20.0,
    "100-200": 32.0, "100-200.1": 32.0,
    "100-250": 50.0, "100-250.1": 50.0,
    "100-315": 80.0, "100-400": 125.0,
    "125-200": 32.0, "125-200.1": 32.0,
    "125-250": 50.0, "125-250.1": 50.0,
    "125-315": 80.0, "125-400": 125.0,
    "125-500": 150.0,
    "150-200": 32.0, "150-200.1": 32.0,
    "150-250": 50.0, "150-250.1": 50.0,
    "150-315": 80.0, "150-315.2": 80.0,
    "150-400": 125.0, "150-500": 150.0,
    "200-250": 12.5, "200-315": 20.0,
    "250-315": 20.0, "300-315": 20.0,
    "350-400": 32.0,
}


def kmg_h_nom(type_key: str, rpm: int) -> float:
    """Return H_nom for KMG pump type at given RPM."""
    h_2900 = KMG_H_2900.get(type_key)
    if h_2900 is None:
        base_type = re.sub(r'\.\d+$', '', type_key)
        h_2900 = KMG_H_2900.get(base_type)
    if h_2900 is None:
        m = re.search(r'-(\d{3,4})(?:\.\d)?$', type_key)
        if m:
            d = int(m.group(1))
            h_2900 = round((d / 200.0) ** 2 * 32.0, 1)
        else:
            h_2900 = 20.0
    if rpm == 1450:
        return round(h_2900 * 0.25, 1)
    return h_2900


def parse_kmg(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse KMG catalog: model table with Type, P2, Q rows.
    Format per type: type_line, then repeating groups of (P2, Q, DN_out, DN_in, bullets...).
    Each type can have multiple P2/Q variants (different motor sizes).
    Model name: 'KMG {type}' (one model per type, using best-efficiency P2/Q)."""
    entries, seen = [], set()
    rpm = 2900

    for pn, text in pages:
        if "KMG" not in text and "\u041a\u041c\u0413" not in text:
            continue

        if "2900 \u043e\u0431/\u043c\u0438\u043d" in text:
            rpm = 2900
        elif "1450 \u043e\u0431/\u043c\u0438\u043d" in text:
            rpm = 1450

        lines = text.split('\n')
        i = 0
        current_type = None
        type_variants = []  # list of (p2, q) for current type

        def flush_type():
            """Save the best variant for current_type."""
            nonlocal current_type, type_variants
            if current_type and type_variants:
                model = f"KMG {current_type}"
                if model not in seen:
                    seen.add(model)
                    dn_m = re.match(r'^(\d{2,3})', current_type)
                    dn = int(dn_m.group(1)) if dn_m else 0
                    h_nom = kmg_h_nom(current_type, rpm)
                    # Pick the variant closest to BEP (middle of Q range)
                    best = max(type_variants, key=lambda v: v[1])  # highest Q = nominal
                    # Actually use all variants — one entry per P2/Q pair
                    for idx, (p2, q) in enumerate(type_variants):
                        variant_model = model if len(type_variants) == 1 else f"{model}/{p2}kW"
                        if variant_model in seen and idx > 0:
                            continue
                        if idx > 0:
                            seen.add(variant_model)
                        entries.append(PumpEntry(
                            model=variant_model if idx > 0 else model,
                            series="KMG",
                            q_nom=q,
                            h_nom=h_nom,
                            power_kw=p2,
                            rpm=rpm,
                            dn_in=dn,
                            dn_out=dn,
                            source_file=src, page=pn,
                            data_source='catalog_nominal',
                            confidence=1.0, manufacturer="ONIS",
                        ))
            current_type = None
            type_variants = []

        while i < len(lines):
            line = lines[i].strip()

            # Detect type line: "32-125.1" or "80-400"
            type_m = re.match(r'^(\d{2,3})-(\d{3,4}(?:\.\d{1,2})?)$', line)
            if type_m:
                flush_type()
                current_type = line
                i += 1
                continue

            if current_type:
                # Expect P2 value (kW): 0.25 to 400
                p2_val = pf(line)
                if p2_val is not None and 0.09 <= p2_val <= 400:
                    # Next line should be Q (m3/h)
                    if i + 1 < len(lines):
                        q_val = pf(lines[i + 1].strip())
                        if q_val is not None and 1.0 <= q_val <= 5000:
                            type_variants.append((p2_val, q_val))
                            # Skip P2, Q, DN_out, DN_in, and bullet lines
                            # DN_out and DN_in are 2-3 digit integers (32, 50, 65, 80...)
                            # Bullets are '•', '—', or empty
                            j = i + 2  # skip P2 and Q
                            # Skip DN_out, DN_in (2 lines of 2-3 digit numbers)
                            while j < len(lines) and j < i + 4:
                                val = lines[j].strip()
                                if re.match(r'^\d{2,3}$', val):
                                    j += 1
                                else:
                                    break
                            # Skip bullet/dash/empty lines
                            while j < len(lines):
                                val = lines[j].strip()
                                if val in ('', '\u2022', '\u2014', '—', '•', '-'):
                                    j += 1
                                else:
                                    break
                            i = j
                            continue
                # Skip non-data lines (headers, bullets, empty)
            i += 1

        flush_type()

    return entries


# =============================================================================
# VO(E) - Transposed technical data table
# =============================================================================


# VO: Q_nom lookup by DN and model code
# From performance charts in VO catalog (approximate BEP values)
VO_Q_BY_DN = {
    32: 8.0,    # DN32: BEP ~8 m3/h (from eta analysis)
    40: 10.0,   # DN40: BEP ~10 m3/h
    50: 16.0,   # DN50: BEP ~16 m3/h
    65: 28.0,   # DN65: BEP ~28 m3/h
    80: 45.0,   # DN80: BEP ~45 m3/h
    100: 70.0,  # DN100: BEP ~70 m3/h
    125: 120.0, # DN125: BEP ~120 m3/h
    150: 220.0, # DN150: BEP ~220 m3/h
    200: 250.0, # DN200: BEP ~250 m3/h
    250: 400.0, # DN250: estimated
    300: 600.0, # DN300: estimated
    350: 800.0, # DN350: estimated
}

def vo_h_nom_from_model(model_name: str) -> float:
    """Derive H_nom from VO model name.
    VO32-120: 120 = H_shutoff in decimeters (12.0m). BEP H ≈ shutoff * 0.85."""
    parts = model_name.split('-')
    if len(parts) >= 2:
        try:
            h_code = int(parts[-1])
            if 1 <= h_code <= 9999:
                h_shutoff = h_code / 10.0  # decimeters to meters
                return round(h_shutoff * _VO_BEP_FRACTION, 1)
        except ValueError:
            pass
    return 0.0


def _parse_vo_power_line(line: str) -> Optional[float]:
    """Parse a single power value from VO technical data table."""
    line = line.strip()
    if not line:
        return None
    return pf_slash(line)


def parse_vo(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse VO(E) catalog: technical data tables with model columns."""
    entries, seen = [], set()

    for pn, text in pages:
        if "\u0412\u041e" not in text and "\u0412\u041e\u0415" not in text:
            continue
        if "\u0422\u0435\u0445\u043d\u0438\u0447\u0435\u0441\u043a\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0435" not in text:
            continue

        lines = text.split('\n')

        series_m = re.search(r'\u0412\u041e\(\u0415\)\s+(\d+)', text)
        if not series_m:
            continue
        dn = int(series_m.group(1))
        q_series = VO_Q_BY_DN.get(dn, 0.0)
        if not q_series:
            continue

        model_line_idx = -1
        for i, line in enumerate(lines):
            if '\u041c\u043e\u0434\u0435\u043b\u044c \u0412\u041e(\u0415)' in line:
                model_line_idx = i
                break

        if model_line_idx == -1:
            continue

        model_names = []
        for j in range(model_line_idx+1, min(model_line_idx+25, len(lines))):
            line = lines[j].strip()
            if re.match(r'^\d+-\d+$', line):
                h_dm_str = line.split('-')[-1]
                try:
                    h_dm = int(h_dm_str)
                    model_names.append(f"\u0412\u041e{dn}-{h_dm}")
                except ValueError:
                    pass
            elif model_names:
                break

        if not model_names:
            continue

        p2_vals = []
        for i, line in enumerate(lines):
            if '\u041c\u043e\u0449\u043d\u043e\u0441\u0442\u044c' in line and 'P2' in line:
                for j in range(i+1, min(i + len(model_names) * 2 + 5, len(lines))):
                    raw_line = lines[j].strip()
                    if not raw_line:
                        continue
                    if any(kw in raw_line for kw in ['\u041a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0446\u0438\u044f', '\u041d\u043e\u043c\u0438\u043d\u0430\u043b\u044c\u043d\u043e\u0435', '\u0422\u0435\u043c\u043f\u0435\u0440\u0430\u0442\u0443\u0440\u0430',
                                                      'D1', 'L1', 'H1', 'H2', 'H3', 'B1', '\u041c\u0430\u0441\u0441\u0430']):
                        break
                    p2 = _parse_vo_power_line(raw_line)
                    if p2 is not None:
                        p2_vals.append(p2)
                    elif len(p2_vals) >= len(model_names):
                        break
                if p2_vals:
                    break

        for k, model in enumerate(model_names):
            if model in seen:
                continue
            seen.add(model)

            p2 = p2_vals[k] if k < len(p2_vals) else None
            h_nom = vo_h_nom_from_model(model)

            # Bug #4: use estimate_power_hydraulic from pump_validators (no 0.95 multiplier)
            if p2 is None or p2 <= 0:
                p2 = estimate_power_hydraulic(q_series, h_nom) if h_nom > 0 else 0.0
                log.debug(f"\u0412\u041e {model}: power not in catalog, estimated P={p2:.2f}kW")

            entries.append(PumpEntry(
                model=model, series="\u0412\u041e",
                q_nom=q_series, h_nom=h_nom,
                power_kw=p2,
                source_file=src, page=pn,
                data_source='catalog_nominal',
                confidence=1.0, manufacturer="ONIS",
            ))

    # ---- Fallback: parse model list pages (pages with ВО(Е) DN-RPM pattern) ----
    # Pages 23-24 have model lists; pages 50+ have data sheets
    # Some larger DNs (250, 300, 350) only have list entries
    for pn, text in pages:
        if "ВО(Е)" not in text:
            continue
        # Skip data sheet pages (already handled above)
        if "Технические данные" in text:
            continue
        if "Таблица характеристик" not in text and "модельный ряд" not in text.lower():
            continue
        lines = text.split('\n')
        for i, line in enumerate(lines):
            ls = line.strip()
            # Match "ВО(Е) DN-RPM" pattern with optional power/pole suffix
            m_ve = re.match(r'^ВО\(Е\)\s+(\d+)-(\d+)(?:-([\.\d,]+)/(\d))?', ls)
            if not m_ve:
                continue
            dn = int(m_ve.group(1))
            rpm_code = int(m_ve.group(2))
            q_series = VO_Q_BY_DN.get(dn, 0.0)
            if not q_series:
                continue
            h_nom = rpm_code / 10.0  # rpm_code is actually H in decimeters
            model = f"ВО{dn}-{rpm_code}"
            if model in seen:
                continue
            # Extract power if available in the pattern
            p2 = None
            if m_ve.group(3):
                p2 = pf(m_ve.group(3))
            if p2 is None or p2 <= 0:
                p2 = estimate_power_hydraulic(q_series, h_nom) if h_nom > 0 else 0.0
            seen.add(model)
            entries.append(PumpEntry(
                model=model, series="ВО",
                q_nom=q_series, h_nom=h_nom,
                power_kw=p2,
                source_file=src, page=pn,
                data_source='catalog_nominal',
                confidence=0.8, manufacturer="ONIS",
            ))

    return entries


# =============================================================================
# FST/FS/FS4 - graph-based
# =============================================================================

FST_Q_EST: Dict[str, float] = {
    "32": 6.3, "40": 12.5, "50": 25.0, "65": 50.0, "80": 80.0,
    "100": 130.0, "125": 200.0, "150": 320.0, "200": 500.0,
}

FST_H_EST: Dict[str, float] = {
    "100": 8.0, "125": 12.0, "160": 20.0, "200": 30.0, "250": 45.0,
    "315": 70.0, "400": 100.0,
}

RE_FST = re.compile(r'(\d{2,3})\s*-\s*(\d{3,4})\s*/\s*(\d{2,3})')


def parse_fst(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse FST/FS/FS4/FSM catalog: extract model codes from annotated charts."""
    entries, seen = [], set()
    for pn, text in pages:
        if "FST" not in text and "FS4" not in text and "FSM" not in text:
            continue
        for m in RE_FST.finditer(text):
            fl, imp, pw_code = m.group(1), m.group(2), m.group(3)
            model = f"FST_{fl}-{imp}/{pw_code}"
            # Validate: fl must be known FST flange size
            if fl not in ("32", "40", "50", "65", "80", "100", "125", "150", "200"):
                continue
            # Validate: imp (impeller) should be in known range
            try:
                imp_val = int(imp)
                if imp_val < 100 or imp_val > 500:
                    continue
            except ValueError:
                continue
            # Validate: pw_code should give reasonable power
            try:
                pw_val = int(pw_code)
                if pw_val < 5 or pw_val > 3000:  # 0.5 to 300 kW (pw/10)
                    continue
            except ValueError:
                continue
            if model in seen:
                continue
            seen.add(model)
            try:
                p2 = int(pw_code) / 10.0
                # H from impeller size (reliable), Q derived from power
                imp_key = imp[:3] if len(imp) >= 3 else imp
                h_nom = float(FST_H_EST.get(imp_key, 20))
                # Derive Q from physics: Q = P2 * eta * 1000 * 3600 / (rho * g * H)
                # Use eta = 0.55 (conservative for single-stage centrifugal)
                if h_nom > 0 and p2 > 0:
                    eta_assumed = 0.55
                    q_nom = round(p2 * eta_assumed * 1000 * 3600 / (1000 * 9.81 * h_nom), 1)
                else:
                    q_nom = float(FST_Q_EST.get(fl, 12.5))
                # Sanity: cap Q at reasonable max for DN
                q_max = float(FST_Q_EST.get(fl, 500)) * 1.5
                if q_nom > q_max:
                    q_nom = q_max
                # Sanity: skip if H is unreasonable for centrifugal pump
                if h_nom > 200 or h_nom < 1:
                    continue
            except (ValueError, ZeroDivisionError):
                continue
            entries.append(PumpEntry(
                model=model, series="FST",
                q_nom=q_nom, h_nom=h_nom,
                power_kw=p2,
                dn_in=int(fl) if fl.isdigit() else 0,
                source_file=src, page=pn,
                data_source='estimated',
                confidence=0.8, manufacturer="ONIS",
            ))
    return entries


# =============================================================================
# CDLF_LARGE / CDMF_LARGE - Large series (120/150/200 m3/h)
# =============================================================================

def parse_cdlf_large(pages: List[Tuple[int, str]], src: str, series_prefix: str = "CDLF") -> List[PumpEntry]:
    """Parse CDL large series (CDL120/150/200): matrix layout Q-H table."""
    entries, seen = [], set()

    for pn, text in pages:
        if "CDL" not in text:
            continue

        lines = text.split('\n')

        # Step 1: Extract model names
        models = []
        for i, line in enumerate(lines):
            line_s = line.strip()
            if re.match(r'^CDL(?:MF?)?\d+[-\d A-Za-z]*$', line_s):
                models.append(line_s)
            elif line_s in ["01", "02", "50Hz"] and models:
                break

        if not models:
            continue

        n_models = len(models)

        # Step 2: Extract Q values in m3/h ONLY
        q_vals = []
        for i, line in enumerate(lines):
            ls = line.strip()
            if 'm /h' in ls or 'm\u00b3/h' in ls.lower() or 'm3/h' in ls.lower():
                inline_nums = re.findall(r'\d+(?:\.\d+)?', ls)
                first_q = None
                for n_str in inline_nums:
                    v = pf(n_str)
                    if v is not None and 10 <= v <= 5000:
                        first_q = v
                        break
                if first_q is None:
                    continue

                seq = [first_q]
                j = i + 1
                while j < min(i + 80, len(lines)):
                    ls2 = lines[j].strip()
                    if ls2 == '3':
                        j += 1
                        continue
                    v = pf(ls2)
                    if v is not None and v > 0:
                        seq.append(v)
                    j += 1

                m3h_candidates = [seq[k] for k in range(0, len(seq), 3) if k < len(seq)]
                if len(m3h_candidates) >= 2:
                    steps = [m3h_candidates[k+1] - m3h_candidates[k]
                             for k in range(len(m3h_candidates) - 1)]
                    if all(5 <= s <= 50 for s in steps):
                        q_vals = m3h_candidates
                        break

                best_run = []
                for start in range(min(3, len(seq))):
                    run = [seq[start]] if start < len(seq) else []
                    k = start + 3
                    while k < len(seq):
                        step = seq[k] - run[-1]
                        if 5 <= step <= 50:
                            run.append(seq[k])
                            k += 3
                        else:
                            break
                    if len(run) > len(best_run):
                        best_run = run
                if len(best_run) >= 2:
                    q_vals = best_run
                    break

        if len(q_vals) < 2:
            continue

        if q_vals[0] < q_vals[1] * 0.9:
            q_vals = q_vals[1:]
        if len(q_vals) < 2:
            continue

        n_q = len(q_vals)

        # Step 3: Extract kW, HP, and H matrix
        dn_start_line = None
        for i, line in enumerate(lines):
            ls = line.strip()
            if re.match(r'^\d+x\d+$', ls):
                dn_start_line = i
                break

        if dn_start_line is None:
            for i, line in enumerate(lines):
                if line.strip() in ('01', '02'):
                    dn_start_line = i
                    break
            if dn_start_line is None:
                continue

        all_nums_str = []
        for i in range(dn_start_line, len(lines)):
            ls = lines[i].strip()
            if re.match(r'^\d+x\d+$', ls):
                continue
            if re.search(r'[a-zA-Z\u0430-\u044f\u0410-\u042f=]', ls):
                continue
            v = pf(ls)
            if v is not None and 0 < v < 2000:
                all_nums_str.append(v)

        if len(all_nums_str) < n_models:
            continue

        idx = 0
        kw_vals = all_nums_str[idx:idx+n_models]
        idx += n_models
        idx += n_models  # skip HP values
        h_flat = all_nums_str[idx:idx + n_q * n_models]

        if len(h_flat) < n_q * n_models:
            idx -= n_models
            h_flat = all_nums_str[idx:idx + n_q * n_models]

        if len(h_flat) < n_models * 2:
            for k, model in enumerate(models):
                if model in seen:
                    continue
                seen.add(model)
                q_m = re.search(r'CDL(?:MF?)?\s*(\d+)', model)
                q_nom = float(q_m.group(1)) if q_m else 0.0
                p2 = kw_vals[k] if k < len(kw_vals) else 0.0
                entries.append(PumpEntry(
                    model=model.replace(' ', '_'), series=series_prefix,
                    q_nom=q_nom, h_nom=0.0,
                    power_kw=p2,
                    source_file=src, page=pn,
                    data_source='estimated',
                    confidence=0.8, manufacturer="ONIS",
                ))
            continue

        for k, model in enumerate(models):
            if model in seen:
                continue
            seen.add(model)

            h_pts = []
            for qi in range(n_q):
                flat_idx = qi * n_models + k
                if flat_idx < len(h_flat):
                    h_pts.append(h_flat[flat_idx])

            if not h_pts:
                continue

            p2 = kw_vals[k] if k < len(kw_vals) else 0.0
            q_m = re.search(r'CDL(?:MF?)?\s*(\d+)', model)
            q_series = float(q_m.group(1)) if q_m else q_vals[-1]

            n = min(len(q_vals), len(h_pts))
            q_pts = q_vals[:n]
            h_pts = h_pts[:n]
            mid = n // 2

            entries.append(PumpEntry(
                model=model.replace(' ', '_'), series=series_prefix,
                q_nom=q_pts[mid] if q_pts else q_series,
                h_nom=h_pts[mid] if h_pts else 0.0,
                power_kw=p2,
                q_points=q_pts, h_points=h_pts,
                source_file=src, page=pn,
                data_source='catalog_curve',
                confidence=1.0, manufacturer="ONIS",
            ))

    return entries


# =============================================================================
# CDMF_LARGE (CDM32/42/65/85)
# =============================================================================

_CDML_H_PER_STAGE: Dict[int, float] = {32: 8.0, 42: 7.0, 65: 6.0, 85: 5.0}


def parse_cdmf_large(pages: List[Tuple[int, str]], src: str) -> List[PumpEntry]:
    """Parse CDM32/42/65/85 catalog: model names only, derive all performance data."""
    entries, seen = [], set()

    for pn, text in pages:
        if "CDM" not in text:
            continue

        lines = text.split('\n')
        for line in lines:
            ls = line.strip()
            m = re.match(r'^(CDM(\d+)-(\d+)(?:-\d+)?)$', ls)
            if not m:
                continue
            model = m.group(1)
            series_num_str = m.group(2)
            stages_str = m.group(3)

            if model in seen:
                continue
            seen.add(model)

            try:
                series_num = int(series_num_str)
                stages = int(stages_str)
            except ValueError:
                continue

            q_nom = float(series_num)
            h_ps = _CDML_H_PER_STAGE.get(series_num, 6.0)
            h_nom = round(stages * h_ps, 1)

            # Bug #4: use imported estimate_power_hydraulic (no 0.95 multiplier)
            p2 = estimate_power_hydraulic(q_nom, h_nom)

            entries.append(PumpEntry(
                model=model, series=f"CDM{series_num}",
                q_nom=q_nom, h_nom=h_nom,
                power_kw=p2,
                source_file=src, page=pn,
                data_source='estimated',
                confidence=0.8, manufacturer="ONIS",
            ))

    return entries


# =============================================================================
# === UNIVERSAL PIPELINE (NEW in v10) ===
# =============================================================================

MANUFACTURER_KEYWORDS = {
    "ONIS": ["onis visa", "onis"],  # Bug fix: removed "pump" (matches everything)
    "Grundfos": ["grundfos"],
    "Wilo": ["wilo"],
    "KSB": ["ksb", "etanorm", "movitec", "amarex"],
    "Pedrollo": ["pedrollo"],
    "Ebara": ["ebara"],
    "Lowara": ["lowara", "xylem"],
    "Calpeda": ["calpeda"],
    "DAB": ["dab pumps", "dab"],
    "Flygt": ["flygt"],
    "CNP": ["cnp", "nanfang"],
    "LEO": ["leo pump", "lepono"],
    "Pentax": ["pentax"],
    "Caprari": ["caprari"],
    "Saer": ["saer"],
    "Franklin": ["franklin electric", "franklin"],
    "Sulzer": ["sulzer"],
    "ITT Goulds": ["goulds", "itt"],
    "Flowserve": ["flowserve"],
    "Tsurumi": ["tsurumi"],
    "Speroni": ["speroni"],
    "Nocchi": ["nocchi"],
    "Shimge": ["shimge"],
}


def detect_manufacturer(pdf_path: str) -> str:
    """Detect manufacturer from filename + first 3 pages."""
    try:
        # Check filename first
        fname_lower = os.path.basename(pdf_path).lower()
        for mfr, keywords in MANUFACTURER_KEYWORDS.items():
            if any(kw in fname_lower for kw in keywords):
                return mfr

        doc = fitz.open(pdf_path)
        pages_to_check = min(5, len(doc))
        combined_text = ""
        for i in range(pages_to_check):
            combined_text += doc[i].get_text().lower() + " "
        doc.close()

        best_mfr = "Unknown"
        best_score = 0
        for mfr, keywords in MANUFACTURER_KEYWORDS.items():
            for kw in keywords:
                count = combined_text.count(kw)
                if count > best_score:
                    best_score = count
                    best_mfr = mfr
        return best_mfr if best_score > 0 else "Unknown"
    except Exception as e:
        log.warning(f"detect_manufacturer failed: {e}")
        return "Unknown"


_CLASSIFY_PUMP_KEYWORDS = {
    'подача', 'расход', 'напор', 'мощность', 'насос',
    'flow', 'head', 'power', 'pump', 'impeller',
    'capacity', 'förderstrom', 'förderhöhe', 'leistung', 'pumpe',
    'portata', 'prevalenza', 'potenza', 'pompa',
}
_CLASSIFY_CONTEXT_RE = re.compile(
    r'\b[QH]\s*[\[\(=:]|\b[QHP]\s*\d|'
    r'\brpm\b|\bm³/h\b|\bм³/ч\b|\bl/s\b|\bl/min\b|\bgpm\b',
    re.I
)
_CLASSIFY_SKIP_KEYWORDS = {
    'contents', 'table of contents', 'содержание',
    'warranty', 'гарантия', 'installation guide', 'руководство по монтажу',
    'copyright', 'all rights reserved',
}


def classify_page(text: str) -> bool:
    """Check if page likely contains pump data (has pump keywords + numeric data).
    Bug 14 fix: no single-letter 'q','h','p' matching.
    Bug 15 fix: skip non-data pages (contents, warranty, etc.)."""
    text_lower = text.lower()

    # Skip non-data pages
    if any(kw in text_lower for kw in _CLASSIFY_SKIP_KEYWORDS):
        return False

    has_keywords = (
        any(kw in text_lower for kw in _CLASSIFY_PUMP_KEYWORDS)
        or bool(_CLASSIFY_CONTEXT_RE.search(text))
    )
    numbers = re.findall(r'\d+[.,]\d+|\d+', text)
    has_numbers = len(numbers) >= 5
    return has_keywords and has_numbers


def extract_tables_pymupdf(pdf_path: str) -> List[Tuple[Any, int, str]]:
    """Extract tables using PyMuPDF find_tables() (Level 5a)."""
    results = []
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        log.warning(f"PyMuPDF failed to open {pdf_path}: {e}")
        return results
    with doc:
        for page_num in range(len(doc)):
            page = doc[page_num]
            page_text = page.get_text()
            if not classify_page(page_text):
                continue
            try:
                tabs = page.find_tables(strategy="lines")
                found = False
                if tabs and tabs.tables:
                    for tab in tabs.tables:
                        if tab.row_count >= 2 and tab.col_count >= 3:
                            df = tab.to_pandas()
                            results.append((df, page_num, page_text))
                            found = True
                if not found:
                    tabs = page.find_tables(strategy="text", min_words_vertical=2, min_words_horizontal=1)
                    if tabs and tabs.tables:
                        for tab in tabs.tables:
                            if tab.row_count >= 2 and tab.col_count >= 3:
                                df = tab.to_pandas()
                                results.append((df, page_num, page_text))
            except Exception as e:
                log.warning(f"PyMuPDF find_tables failed page {page_num}: {e}")
    return results


def extract_tables_pdfplumber(pdf_path: str) -> List[Tuple[Any, int, str]]:
    """Extract tables using pdfplumber stream detection (Level 5b)."""
    if not HAS_PDFPLUMBER:
        return []
    results = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text() or ""
                if not classify_page(page_text):
                    continue
                tables = page.extract_tables()
                for table in tables:
                    if table and len(table) >= 2 and len(table[0]) >= 3:
                        results.append((table, page_num, page_text))
    except Exception as e:
        log.warning(f"pdfplumber extraction failed: {e}")
    return results


def extract_text_regex(pdf_path: str) -> List[Dict[str, Any]]:
    """Extract pump data using text+regex heuristics (Level 5c).

    Looks for lines with 3+ numbers in pump-relevant ranges and tries to
    identify model names nearby.
    """
    pumps = []
    try:
        with fitz.open(pdf_path) as doc:
            manufacturer = detect_manufacturer(pdf_path)

            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text()
                if not classify_page(text):
                    continue

                lines = text.split('\n')
                for i, line in enumerate(lines):
                    # Look for lines that might be model + numeric data
                    # Pattern: ModelName followed by numbers (Q, H, P)
                    m = re.match(
                        r'([A-Z\u0410-\u042f][A-Za-z\u0410-\u044f\d\-/. ]{3,40}?)\s+'
                        r'([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)',
                        line.strip()
                    )
                    if m:
                        model_candidate = m.group(1).strip()
                        v1, v2, v3 = pf(m.group(2)), pf(m.group(3)), pf(m.group(4))
                        if v1 is None or v2 is None or v3 is None:
                            continue
                        # Heuristic: Q is typically 0.1-10000, H is 1-3000, P is 0.01-2000
                        if 0.1 <= v1 <= 10000 and 0 <= v2 <= 3000 and 0.01 <= v3 <= 2000:
                            pumps.append({
                                "model": model_candidate,
                                "q_nom": v1,
                                "h_nom": v2,
                                "power_kw": v3,
                                "source_file": pdf_path,
                                "page": page_num,
                                "data_source": DS_UNIVERSAL_TEXT,
                                "manufacturer": manufacturer,
                                "confidence": 0.5,
                            })
    except Exception as e:
        log.warning(f"text regex extraction failed: {e}")
    return pumps


# ─── Level 5d: Gemini Vision Fallback ────────────────────────────────────────

GEMINI_PUMP_PROMPT = """Analyze this pump catalog page image. Extract ALL pump models visible in tables or text.

For each pump model, extract:
- model: model name/designation (string)
- q_nom: nominal flow rate in m³/h (number, convert from l/min÷60, GPM×0.227 if needed)
- h_nom: nominal head in meters (number, convert from ft×0.3048, bar×10.2 if needed)
- power_kw: motor power in kW (number, convert from HP×0.746, W÷1000 if needed)
- rpm: speed in rpm (number, if visible)

Return ONLY a JSON array. No explanation, no markdown. Example:
[{"model":"NM 32/12FE","q_nom":10.8,"h_nom":12.0,"power_kw":0.55},{"model":"NM 32/12DE","q_nom":10.8,"h_nom":17.0,"power_kw":0.75}]

If no pump data is visible, return: []
IMPORTANT: Only extract data that is clearly visible. Do NOT invent or guess values."""

GEMINI_CURVE_PROMPT = """This is a pump performance curve page from a catalog.

Read ALL Q-H curves visible on the graph:
- X-axis = Flow Q (units may be: м³/ч, m³/h, l/s, l/min, GPM)
- Y-axis = Head H (units may be: м, m, ft, bar)
- Each curve represents one pump model or configuration

For EACH curve extract:
- model: exact label/name on or near the curve
- q_nom: nominal flow at BEP (best efficiency point, ~60% of max Q)
- h_nom: head at that nominal flow point
- q_points: [array of 5+ Q values along the curve, left to right]
- h_points: [corresponding H values]

Convert all values to: Q in m³/h, H in meters.
Return ONLY a JSON array. No markdown, no explanation.
Example: [{"model":"CV 10-4","q_nom":10,"h_nom":34,"q_points":[0,5,10,15,18],"h_points":[40,38,34,26,18]}]
CRITICAL: Only extract clearly visible data. Do NOT guess."""


def _render_page_to_png(pdf_path: str, page_num: int, dpi: int = 200) -> bytes:
    """Render a PDF page to PNG bytes."""
    doc = fitz.open(pdf_path)
    page = doc[page_num]
    # Handle rotation
    if page.rotation:
        page.set_rotation(0)
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat)
    png_bytes = pix.tobytes("png")
    doc.close()
    return png_bytes


def _make_collage(images: List[bytes]) -> bytes:
    """Combine 2-3 page images into a single vertical collage."""
    if not HAS_PIL or len(images) <= 1:
        return images[0] if images else b""

    import io
    pil_images = [Image.open(io.BytesIO(img)) for img in images]
    total_height = sum(im.height for im in pil_images)
    max_width = max(im.width for im in pil_images)
    collage = Image.new("RGB", (max_width, total_height), (255, 255, 255))
    y = 0
    for im in pil_images:
        collage.paste(im, (0, y))
        y += im.height
    buf = io.BytesIO()
    collage.save(buf, format="PNG")
    return buf.getvalue()


def _safe_parse_json(text: str) -> List[Dict[str, Any]]:
    """Parse JSON from Gemini response, with repair for common issues."""
    text = text.strip()
    # Remove markdown code fence
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    # Fix trailing commas
    text = re.sub(r',\s*([}\]])', r'\1', text)
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        return []
    except json.JSONDecodeError:
        # Last resort: find first [ ... ]
        m = re.search(r'\[.*\]', text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
    return []


def _cross_validate_gemini(entries: List[Dict[str, Any]],
                           page_text: str) -> List[Dict[str, Any]]:
    """Validate Gemini results against page text to reduce hallucinations."""
    page_text_lower = page_text.lower()
    # Extract all numbers from page
    page_numbers = set()
    for m in re.finditer(r'\d+[.,]\d+|\d+', page_text):
        page_numbers.add(m.group().replace(',', '.'))

    validated = []
    for e in entries:
        model = e.get("model", "")
        if not model or len(model) < 2:
            continue

        # Check model name appears in text
        model_parts = [p for p in re.split(r'[\s/\-]', model) if len(p) > 2]
        model_found = any(part.lower() in page_text_lower for part in model_parts)

        # Check if at least one numeric value appears in text
        nums_match = False
        for key in ("q_nom", "h_nom", "power_kw"):
            val = e.get(key, 0)
            if val and (str(val) in page_numbers or str(val).replace('.', ',') in page_text):
                nums_match = True
                break

        if model_found and nums_match:
            e["confidence"] = 0.75
        elif model_found or nums_match:
            e["confidence"] = 0.55
        else:
            e["confidence"] = 0.35
            e["data_source"] = DS_GEMINI_UNVERIFIED

        validated.append(e)
    return validated


def _vision_api_call(image_data: bytes, prompt: str, model: str = None) -> Optional[str]:
    """Call vision API with image. Returns response text or None.

    Supports two backends (checked in order):
    1. GEMINI_API_KEY → Google genai direct (fastest, but blocked in some countries)
    2. OPENROUTER_API_KEY → OpenRouter (OpenAI-compatible, works globally)
    """
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "")

    if not model:
        model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

    # Backend 1: Google genai direct
    if gemini_key and HAS_GENAI:
        try:
            from google.genai import types as genai_types
            client = genai.Client(api_key=gemini_key)
            response = client.models.generate_content(
                model=model,
                contents=[
                    genai_types.Part.from_bytes(data=image_data, mime_type="image/png"),
                    prompt,
                ],
                config=genai_types.GenerateContentConfig(
                    temperature=0.1, max_output_tokens=4096,
                ),
            )
            return response.text if response.text else None
        except Exception as exc:
            log.warning(f"[VISION] Google genai failed: {exc}")
            # Fall through to OpenRouter

    # Backend 2: OpenRouter (OpenAI-compatible with base64 images)
    if openrouter_key and HAS_OPENAI:
        try:
            import base64
            b64_image = base64.b64encode(image_data).decode("utf-8")
            # Map model name to OpenRouter format
            or_model = model
            if "/" not in model:
                or_model = f"google/{model}"

            client = _OpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=openrouter_key,
            )
            response = client.chat.completions.create(
                model=or_model,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {
                            "url": f"data:image/png;base64,{b64_image}"}},
                        {"type": "text", "text": prompt},
                    ],
                }],
                temperature=0.1,
                max_tokens=4096,
            )
            text = response.choices[0].message.content
            return text if text else None
        except Exception as exc:
            log.warning(f"[VISION] OpenRouter failed: {exc}")

    return None


def _has_vision_api() -> bool:
    """Check if any vision API backend is available."""
    if os.environ.get("GEMINI_API_KEY") and HAS_GENAI:
        return True
    if os.environ.get("OPENROUTER_API_KEY") and HAS_OPENAI:
        return True
    return False


def extract_gemini_vision(pdf_path: str, max_pages: int = 15) -> List[Dict[str, Any]]:
    """Extract pump data using Vision API (Level 5d).

    Sends page images to Gemini for OCR+extraction.
    Supports Google genai direct or OpenRouter backend.
    """
    if not _has_vision_api():
        log.info("[VISION] No vision API available, skipping")
        return []

    all_pumps = []

    try:
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        # Find pages with pump data
        pump_pages = []
        for i in range(total_pages):
            text = doc[i].get_text()
            if classify_page(text):
                pump_pages.append(i)

        if not pump_pages:
            # Bug 16 fix: fallback for scanned/image-only PDFs
            for i in range(min(total_pages, 5)):
                images = doc[i].get_images()
                if images:
                    pump_pages.append(i)
            if pump_pages:
                log.info(f"[VISION] Image-only PDF detected, processing {len(pump_pages)} pages")
            else:
                doc.close()
                return []

        doc.close()

        # Limit to max_pages
        pages_to_process = pump_pages[:max_pages]
        log.info(f"[VISION] Processing {len(pages_to_process)}/{total_pages} pages")

        # Process pages in pairs (collage for efficiency)
        i = 0
        while i < len(pages_to_process):
            batch_pages = pages_to_process[i:i + 2]
            images = [_render_page_to_png(pdf_path, p) for p in batch_pages]

            if len(images) > 1 and HAS_PIL:
                image_data = _make_collage(images)
            else:
                image_data = images[0]

            try:
                response_text = _vision_api_call(image_data, GEMINI_PUMP_PROMPT)

                if response_text:
                    entries = _safe_parse_json(response_text)
                    # Cross-validate against page text
                    doc = fitz.open(pdf_path)
                    combined_text = " ".join(
                        doc[p].get_text() for p in batch_pages
                    )
                    doc.close()

                    validated = _cross_validate_gemini(entries, combined_text)
                    for e in validated:
                        e["source_file"] = pdf_path
                        e["page"] = batch_pages[0]
                        e.setdefault("data_source", DS_GEMINI)
                        e.setdefault("confidence", 0.6)
                    all_pumps.extend(validated)

                # Rate limit: 4 seconds between requests
                time.sleep(4)

            except Exception as e:
                log.warning(f"[VISION] Request failed for pages {batch_pages}: {e}")
                time.sleep(5)

            i += 2

    except Exception as e:
        log.warning(f"[VISION] Vision extraction failed: {e}")

    return all_pumps


# ─── Enrichment Pipeline (NEW) ──────────────────────────────────────────────

def _fuzzy_match_model(key: str, entry_map: dict, estimated_only: bool = True) -> Optional[str]:
    """Fuzzy match model key against entry map.
    Uses normalize_model_name() for CYR→ASCII conversion (Bug 4 fix).
    """
    key_norm = normalize_model_name(key)

    # Exact match
    if key_norm in entry_map:
        if not estimated_only or entry_map[key_norm].data_source == DS_ESTIMATED:
            return key_norm

    # Strip parenthetical parts: "BM(N)1" → "BM1" (Bug 10 fix)
    key_clean = re.sub(r'\(.*?\)', '', key_norm)
    # Generate flanged variant: CV3→CVF3, CVF3→CV3
    key_variants = {key_clean}
    m_var = re.match(r'^([A-Z]+?)(\d.*)$', key_clean)
    if m_var:
        base, nums = m_var.group(1), m_var.group(2)
        key_variants.add(base + 'F' + nums)
        if base.endswith('F'):
            key_variants.add(base[:-1] + nums)

    for ek in entry_map:
        if estimated_only and entry_map[ek].data_source != DS_ESTIMATED:
            continue
        for kv in key_variants:
            if ek.startswith(kv) or kv.startswith(ek[:len(kv)]):
                return ek

    return None


def _merge_enrichment(entry: PumpEntry, data: dict, new_source: str, new_conf: float):
    """Merge PER-MODEL enrichment data (from Q-H matrix or Gemini).
    For series-level data, use _merge_series_data() instead (Bug 5 fix)."""
    if data.get("q_nom", 0) > 0:
        entry.q_nom = data["q_nom"]
    if data.get("h_nom", 0) > 0:
        entry.h_nom = data["h_nom"]
    if data.get("power_kw", 0) > 0 and entry.power_kw == 0:
        entry.power_kw = data["power_kw"]
    if data.get("q_points") and data.get("h_points"):
        entry.q_points = data["q_points"]
        entry.h_points = data["h_points"]
    entry.data_source = new_source
    entry.confidence = new_conf


def _merge_series_data(entry: PumpEntry, series_data: dict):
    """Merge SERIES-LEVEL data from spec table.
    Only merge Q (same for all models in series).
    DO NOT merge H (H depends on stages, varies per model) — Bug 1 fix.
    DO NOT overwrite power (power varies per model)."""
    if series_data.get("q_nom", 0) > 0:
        entry.q_nom = series_data["q_nom"]
    # h_max_series → use only for VALIDATION, not as h_nom
    if series_data.get("h_max_series", 0) > 0 and entry.h_nom > 0:
        if entry.h_nom > series_data["h_max_series"] * 1.1:
            entry.warnings.append(
                f"h_nom {entry.h_nom:.1f} > series max {series_data['h_max_series']:.1f}")
    if entry.data_source == DS_ESTIMATED:
        entry.data_source = DS_CATALOG_NOMINAL
        entry.confidence = max(entry.confidence, 0.8)


def _enrich_estimated(entries: List[PumpEntry], pdf_path: str) -> List[PumpEntry]:
    """Enrich estimated entries with real data from PDF tables."""
    estimated = [e for e in entries if e.data_source == DS_ESTIMATED]
    if not estimated or not HAS_PANDAS:
        return entries

    entry_map = {normalize_model_name(e.model): e for e in entries}
    enriched = 0

    tables = extract_tables_pymupdf(pdf_path)
    for df_or_table, page_num, page_text in tables:
        if hasattr(df_or_table, 'columns'):
            headers = [str(c) for c in df_or_table.columns.tolist()]
            data_rows = [[str(c) for c in row] for row in df_or_table.values.tolist()]
        else:
            headers = [str(c) for c in df_or_table[0]]
            data_rows = [[str(c) for c in row] for row in df_or_table[1:]]

        # Try 1: Spec table (BM/CV style) — check BEFORE qh_matrix
        # (spec tables can false-positive as qh_matrix when Q row has ascending values)
        spec_info = detect_spec_table(headers, data_rows, page_text)
        if spec_info:
            pumps = parse_spec_table(headers, data_rows, spec_info)
            for p in pumps:
                raw_key = normalize_model_name(p.get("model", ""))
                # Bug 10 fix: strip parenthetical before matching
                key_stripped = re.sub(r'\(.*?\)', '', raw_key)
                # Bug 11 fix: "ВМ(N)" = both BM and BMN
                key_variant = re.sub(r'\((.*?)\)', r'\1', raw_key)
                prefixes = {key_stripped}
                if key_variant != key_stripped:
                    prefixes.add(key_variant)
                # Add flanged/material variant prefixes: CV→CVF, CDL→CDLF, etc.
                for pfx in list(prefixes):
                    m_pfx = re.match(r'^([A-Z]+?)(\d.*)', pfx)
                    if m_pfx:
                        base, nums = m_pfx.group(1), m_pfx.group(2)
                        prefixes.add(base + 'F' + nums)   # CV1→CVF1
                        if base.endswith('F'):
                            prefixes.add(base[:-1] + nums) # CVF1→CV1

                for ek, ev in entry_map.items():
                    if ev.data_source != DS_ESTIMATED:
                        continue
                    if any(ek.startswith(pfx) for pfx in prefixes):
                        _merge_series_data(ev, p)
                        enriched += 1
            continue

        # Try 2: Q-H performance matrix (FST style)
        matrix_info = detect_qh_matrix(headers, data_rows)
        if matrix_info:
            pumps = parse_qh_matrix(headers, data_rows, matrix_info)
            for p in pumps:
                key = normalize_model_name(p.get("model", ""))
                matched_key = None
                if key in entry_map and entry_map[key].data_source == DS_ESTIMATED:
                    matched_key = key
                else:
                    # Fuzzy: try adding/removing trailing series letters
                    # "FS100-160" vs "FST100-160" — match by numeric part
                    m = re.match(r'^([A-Z]+)(\d.*)$', key)
                    if m:
                        prefix, num_part = m.group(1), m.group(2)
                        for ek in entry_map:
                            if entry_map[ek].data_source != DS_ESTIMATED:
                                continue
                            em = re.match(r'^([A-Z]+)(\d.*)$', ek)
                            if em and em.group(2) == num_part:
                                # Same numeric part, check prefix similarity
                                ep = em.group(1)
                                if ep.startswith(prefix) or prefix.startswith(ep):
                                    matched_key = ek
                                    break
                if matched_key:
                    _merge_enrichment(entry_map[matched_key], p, DS_CATALOG_CURVE, 0.95)
                    enriched += 1
            continue

        # Try 3: Standard column classification
        classified = classify_columns(headers, data_rows, page_text)
        if classified["is_pump_table"]:
            pumps = dataframe_to_pump_dicts(classified, headers, data_rows)
            for p in pumps:
                key = normalize_model_name(p.get("model", ""))
                if key in entry_map and entry_map[key].data_source == DS_ESTIMATED:
                    _merge_enrichment(entry_map[key], p, DS_CATALOG_NOMINAL, 0.85)
                    enriched += 1

    log.info(f"[ENRICH] Tables: upgraded {enriched}/{len(estimated)} estimated entries")
    return entries


def _spread_body_data(entries: List[PumpEntry]) -> List[PumpEntry]:
    """Copy Q-H data between power variants of the same pump body.

    Same body (e.g. FST_100-160) = same impeller = same Q-H curve.
    Only motor power differs. If one variant has catalog_curve data,
    all estimated siblings should get it too.
    """
    import re as _re
    # Group entries by body: "FST100-160" from "FST_100-160/150"
    body_groups: Dict[str, List[PumpEntry]] = {}
    for e in entries:
        m = _re.match(r'^([A-Z]+[\s_]?\d+-\d+)', normalize_model_name(e.model))
        if m:
            body_key = m.group(1).replace(' ', '').replace('_', '')
            body_groups.setdefault(body_key, []).append(e)

    spread_count = 0
    for body_key, group in body_groups.items():
        estimated_in_group = [e for e in group if e.data_source == DS_ESTIMATED]
        if not estimated_in_group:
            continue
        # Find best donor (non-estimated, preferably with Q-H curve)
        donors = [e for e in group if e.data_source != DS_ESTIMATED]
        if not donors:
            continue
        # Pick donor with most data (curve > nominal)
        donor = max(donors, key=lambda e: (
            len(e.q_points) if e.q_points else 0,
            e.h_nom,
            e.q_nom,
        ))
        for e in estimated_in_group:
            if donor.q_nom > 0:
                e.q_nom = donor.q_nom
            if donor.h_nom > 0:
                e.h_nom = donor.h_nom
            if donor.q_points and donor.h_points:
                e.q_points = donor.q_points
                e.h_points = donor.h_points
            e.data_source = donor.data_source
            e.confidence = max(donor.confidence * 0.95, 0.7)
            spread_count += 1

    if spread_count:
        log.info(f"[ENRICH] Body spread: upgraded {spread_count} estimated entries from siblings")
    return entries


def _enrich_with_gemini(entries: List[PumpEntry], pdf_path: str) -> List[PumpEntry]:
    """Enrich remaining estimated entries with Vision API curve reading."""
    if not _has_vision_api():
        return entries

    estimated = [e for e in entries if e.data_source == DS_ESTIMATED]
    if not estimated:
        return entries

    entry_map = {normalize_model_name(e.model): e for e in entries}

    # Find performance curve pages
    doc = fitz.open(pdf_path)
    curve_pages = []
    for i in range(len(doc)):
        page = doc[i]
        text = page.get_text().lower()
        drawings = page.get_drawings()
        if len(drawings) > 15:
            # Q/H axis labels in text
            has_q = bool(re.search(r'(?:м³/ч|m³/h|l/s|gpm|подач|flow|q\s*[\[\(])', text))
            has_h = bool(re.search(r'(?:напор|head|м\.в\.ст|mwc|h\s*[\[\(])', text))
            # Performance chart keywords (axis labels are often vector graphics, not text)
            has_chart_kw = bool(re.search(
                r'(?:характеристик|диаграмм|performance|curve|diagram|'
                r'leistung|kennlinie|caratteristic|courbe)', text))
            # Skip construction/dimension/installation pages
            is_skip_page = bool(re.search(
                r'(?:конструкция|габарит|размер|dimension|installation|'
                r'montage|wartung|maintenance|exploitat)', text))
            if is_skip_page:
                continue
            if has_q or has_h or (has_chart_kw and len(drawings) > 100):
                curve_pages.append(i)
    doc.close()

    if not curve_pages:
        log.info("[ENRICH] No curve pages found for Vision API")
        return entries

    log.info(f"[ENRICH] Found {len(curve_pages)} curve pages, processing with Vision API...")
    enriched = 0

    # Bug 9 fix: open PDF ONCE for all pages
    doc = fitz.open(pdf_path)
    # Prioritize pages with estimated model bodies
    estimated_bodies = set()
    for e in estimated:
        bm = re.search(r'(\d+-\d+)', e.model)
        if bm:
            estimated_bodies.add(bm.group(1))
    if estimated_bodies:
        def _page_priority(pg):
            pt = doc[pg].get_text()
            bodies_on_page = set(re.findall(r'(\d{2,3}-\d{3})', pt))
            return -len(bodies_on_page & estimated_bodies)  # more estimated = higher priority
        curve_pages.sort(key=_page_priority)

    for page_num in curve_pages[:40]:
        png_bytes = _render_page_to_png(pdf_path, page_num)
        page_text = doc[page_num].get_text()

        try:
            # Add page text context to prompt for better model name extraction
            context_prompt = (
                f"Page context (text from this PDF page):\n"
                f"{page_text[:500]}\n\n"
                f"{GEMINI_CURVE_PROMPT}\n\n"
                f"IMPORTANT: Use the page context to determine FULL model names. "
                f"If the page title says e.g. 'CV 3 (IE3)' and curves are labeled "
                f"-36, -33, -31, the full model names are 'CV 3-36', 'CV 3-33', "
                f"'CV 3-31'. Always include the series prefix in model names."
            )
            response_text = _vision_api_call(png_bytes, context_prompt)
            if response_text:
                gemini_entries = _safe_parse_json(response_text)
                # Extract series prefix from page text for models without prefix
                # Match patterns like "FST(FS)", "CV 3 (IE3)", "CDM", "BM(N)"
                series_prefix = ""
                pfx_m = re.search(r'\b([A-ZА-Я]{2,10}(?:\([A-ZА-Я]+\))?)\s*(?=\n|\s+\d)', page_text)
                if pfx_m:
                    series_prefix = pfx_m.group(1).strip()
                validated = _cross_validate_gemini(gemini_entries, page_text)
                for ge in validated:
                    model_raw = ge.get("model", "")
                    # Prepend series prefix if model starts with digit
                    if model_raw and model_raw[0].isdigit() and series_prefix:
                        ge["model"] = f"{series_prefix} {model_raw}"
                    key = normalize_model_name(ge.get("model", ""))
                    matched_keys = []
                    # Find ALL estimated entries matching this key (prefix + flanged)
                    key_clean = re.sub(r'\(.*?\)', '', key)
                    search_prefixes = {key_clean}
                    m_fl = re.match(r'^([A-Z]+?)(\d.*)$', key_clean)
                    if m_fl:
                        fl_base, fl_nums = m_fl.group(1), m_fl.group(2)
                        search_prefixes.add(fl_base + 'F' + fl_nums)  # CV→CVF
                        if fl_base.endswith('F'):
                            search_prefixes.add(fl_base[:-1] + fl_nums)  # CVF→CV
                    for ek in entry_map:
                        if entry_map[ek].data_source != DS_ESTIMATED:
                            continue
                        for sp in search_prefixes:
                            if ek.startswith(sp) or sp.startswith(ek[:len(sp)]):
                                if ek not in matched_keys:
                                    matched_keys.append(ek)
                                break
                    # Base-model matching: "FST100-200/450" → match ALL "FST100-200/*"
                    # Same pump body = same Q-H curve, only motor power differs
                    base_m = re.match(r'^([A-Z]+?)(\d+-\d+)', key)
                    if base_m:
                        prefix, body = base_m.group(1), base_m.group(2)
                        # Generate prefix variants: FST→FS, FS→FST, CV→CVF etc.
                        bases = {prefix + body}
                        bases.add(prefix + 'F' + body)
                        if prefix.endswith('F'):
                            bases.add(prefix[:-1] + body)
                        # Also try prefix-fuzzy: FST↔FS (one contains the other)
                        for ek in entry_map:
                            if entry_map[ek].data_source != DS_ESTIMATED:
                                continue
                            if ek in matched_keys:
                                continue
                            ek_m = re.match(r'^([A-Z]+?)(\d+-\d+)', ek)
                            if ek_m and ek_m.group(2) == body:
                                ep = ek_m.group(1)
                                if ep.startswith(prefix) or prefix.startswith(ep):
                                    matched_keys.append(ek)
                                    continue
                            if any(ek.startswith(b) for b in bases):
                                matched_keys.append(ek)
                    for mk in matched_keys:
                        e = entry_map[mk]
                        if ge.get("h_nom", 0) > 0:
                            e.h_nom = ge["h_nom"]
                        if ge.get("q_points") and ge.get("h_points"):
                            e.q_points = ge["q_points"]
                            e.h_points = ge["h_points"]
                        e.data_source = DS_GEMINI
                        e.confidence = ge.get("confidence", 0.7)
                        enriched += 1
            time.sleep(4)
        except Exception as exc:
            log.warning(f"[ENRICH] Vision API error page {page_num}: {exc}")
            time.sleep(5)

    doc.close()
    log.info(f"[ENRICH] Vision API: upgraded {enriched}/{len(estimated)} estimated entries")
    return entries


def parse_universal(pdf_path: str) -> List[PumpEntry]:
    """Universal pipeline: extract pump data from any PDF catalog."""
    manufacturer = detect_manufacturer(pdf_path)
    log.info(f"[UNIVERSAL] Manufacturer: {manufacturer}")

    all_pumps: List[Dict[str, Any]] = []

    # Level 5a: PyMuPDF find_tables
    if HAS_PANDAS:
        tables = extract_tables_pymupdf(pdf_path)
        for df_or_table, page_num, page_text in tables:
            if hasattr(df_or_table, 'columns'):
                # It's a DataFrame
                headers = [str(c) for c in df_or_table.columns.tolist()]
                data_rows = df_or_table.values.tolist()
            else:
                # It's a list of lists
                headers = [str(c) for c in df_or_table[0]]
                data_rows = df_or_table[1:]

            str_data_rows = [[str(c) for c in row] for row in data_rows]

            # Try spec table detection FIRST (BM/CV transposed format)
            # Must check before qh_matrix: spec tables can false-positive as qh_matrix
            spec_info = detect_spec_table(headers, str_data_rows, page_text)
            if spec_info:
                pumps = parse_spec_table(headers, str_data_rows, spec_info)
                for p in pumps:
                    p["source_file"] = pdf_path
                    p["page"] = page_num
                    p["data_source"] = DS_UNIVERSAL_TABLE
                    p["manufacturer"] = manufacturer
                    p["confidence"] = 0.8
                all_pumps.extend(pumps)
                continue

            # Try Q-H performance matrix detection (FST style)
            matrix_info = detect_qh_matrix(headers, str_data_rows)
            if matrix_info:
                pumps = parse_qh_matrix(headers, str_data_rows, matrix_info)
                for p in pumps:
                    p["source_file"] = pdf_path
                    p["page"] = page_num
                    p["data_source"] = DS_UNIVERSAL_QH
                    p["manufacturer"] = manufacturer
                    p["confidence"] = 0.85
                    if "q_points" in p:
                        p["_q_points"] = p.pop("q_points")
                    if "h_points" in p:
                        p["_h_points"] = p.pop("h_points")
                all_pumps.extend(pumps)
                continue

            # Standard column classification
            classified = classify_columns(
                headers,
                str_data_rows,
                page_text,
            )

            # Selection chart parsing (Bug 13 fix)
            if classified["is_selection_chart"]:
                pumps = parse_selection_chart(headers, str_data_rows)
                for p in pumps:
                    p["source_file"] = pdf_path
                    p["page"] = page_num
                    p["data_source"] = DS_UNIVERSAL_TABLE
                    p["manufacturer"] = manufacturer
                    p["confidence"] = 0.75
                all_pumps.extend(pumps)
                continue

            if classified["is_pump_table"]:
                pumps = dataframe_to_pump_dicts(
                    classified,
                    headers,
                    str_data_rows,
                )
                for p in pumps:
                    p["source_file"] = pdf_path
                    p["page"] = page_num
                    p["data_source"] = DS_UNIVERSAL_TABLE
                    p["manufacturer"] = manufacturer
                    p["confidence"] = 0.8
                all_pumps.extend(pumps)

    # Level 5b: pdfplumber (if PyMuPDF found < 3 models)
    if len(all_pumps) < 3 and HAS_PDFPLUMBER:
        tables = extract_tables_pdfplumber(pdf_path)
        for table, page_num, page_text in tables:
            if isinstance(table, list) and len(table) >= 2:
                headers = [str(c) for c in table[0]]
                data_rows = table[1:]
            elif hasattr(table, 'columns'):
                headers = [str(c) for c in table.columns.tolist()]
                data_rows = table.values.tolist()
            else:
                continue

            classified = classify_columns(
                headers,
                [[str(c) for c in row] for row in data_rows],
                page_text,
            )

            if classified["is_pump_table"]:
                pumps = dataframe_to_pump_dicts(
                    classified,
                    headers,
                    [[str(c) for c in row] for row in data_rows],
                )
                for p in pumps:
                    p["source_file"] = pdf_path
                    p["page"] = page_num
                    p["data_source"] = DS_UNIVERSAL_TABLE
                    p["manufacturer"] = manufacturer
                    p["confidence"] = 0.7
                all_pumps.extend(pumps)

    # Level 5c: Text regex (if still < 3 models)
    if len(all_pumps) < 3:
        text_pumps = extract_text_regex(pdf_path)
        all_pumps.extend(text_pumps)

    # Level 5d: Gemini Vision (if < 3 models OR many lack H data)
    models_without_h = sum(1 for p in all_pumps if p.get("h_nom", 0) == 0)
    need_gemini = len(all_pumps) < 3 or (
        len(all_pumps) > 0 and models_without_h > len(all_pumps) * 0.3)
    if need_gemini:
        try:
            doc = fitz.open(pdf_path)
            n_pages = len(doc)
            doc.close()
        except Exception:
            n_pages = 0
        if n_pages >= 3:
            log.info(f"[UNIVERSAL] Trying Gemini Vision ({n_pages} pages, "
                     f"{models_without_h} models lack H)...")
            gemini_pumps = extract_gemini_vision(pdf_path)
            all_pumps.extend(gemini_pumps)
            log.info(f"[UNIVERSAL] Gemini found {len(gemini_pumps)} models")

    # Convert dicts to PumpEntry objects with score-based merge dedup (Bug 12 fix)
    best_entries: Dict[str, PumpEntry] = {}
    for p in all_pumps:
        model = p.get("model", "")
        if not model:
            continue
        model_key = normalize_model_name(model)

        entry = PumpEntry(
            model=model,
            series=p.get("series", manufacturer),
            article=p.get("article", ""),
            q_nom=p.get("q_nom", 0.0),
            h_nom=p.get("h_nom", 0.0),
            power_kw=p.get("power_kw", 0.0),
            rpm=p.get("rpm", 0),
            dn_in=p.get("dn", 0),
            source_file=p.get("source_file", pdf_path),
            page=p.get("page", 0),
            data_source=p.get("data_source", "universal"),
            confidence=p.get("confidence", 0.5),
            manufacturer=p.get("manufacturer", manufacturer),
        )
        if "_q_points" in p and "_h_points" in p:
            entry.q_points = p["_q_points"]
            entry.h_points = p["_h_points"]

        if model_key in best_entries:
            existing = best_entries[model_key]
            if _entry_score(entry) > _entry_score(existing):
                best_entries[model_key] = entry
            else:
                # Merge complementary data from lower-score entry
                if existing.h_nom == 0 and entry.h_nom > 0:
                    existing.h_nom = entry.h_nom
                if not existing.q_points and entry.q_points:
                    existing.q_points = entry.q_points
                    existing.h_points = entry.h_points
        else:
            best_entries[model_key] = entry

    # Validate all entries
    entries = []
    for entry in best_entries.values():
        ok, reason = validate_entry_basic(entry.model, entry.q_nom, entry.h_nom, entry.power_kw)
        if ok:
            physics_ok, physics_reason, conf_adj = validate_pump_physics(
                entry.q_nom, entry.h_nom, entry.power_kw,
                entry.q_points, entry.h_points,
                entry.model, entry.series,
            )
            if physics_ok:
                entry.confidence = max(0.1, entry.confidence + conf_adj)
                if conf_adj < 0:
                    entry.warnings.append(f"physics: {physics_reason}, conf_adj={conf_adj}")
                entries.append(entry)
            else:
                log.debug(f"  [UNIVERSAL] Physics reject {entry.model}: {physics_reason}")
        else:
            log.debug(f"  [UNIVERSAL] Basic reject {entry.model}: {reason}")

    return entries


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

EXTRACTORS: Dict[str, Any] = {
    "CMI":        lambda p, s: parse_nominal(p, "CMI", s),
    "NBS":        parse_nbs,
    "CV":         parse_cv,
    "TG":         parse_tg,
    "LLT":        parse_llt,
    "CDM":        parse_cdm,
    "CHLF":       parse_chlf,
    "CDLF":       lambda p, s: parse_cdlf(p, s, "CDLF"),
    "CDMF_FANCY": lambda p, s: parse_cdlf(p, s, "CDMF"),
    "CDLF_LARGE": lambda p, s: parse_cdlf_large(p, s, "CDLF"),
    "CDMF_LARGE": parse_cdmf_large,
    "INL":        parse_inl,
    "PV":         parse_pv,
    "FST":        parse_fst,
    "BM":         parse_bm,
    "KMG":        parse_kmg,
    "VO":         parse_vo,
    "UNKNOWN":    lambda p, s: [],  # Handled separately via parse_universal
}


def process_catalog(pdf_path: str) -> Tuple[str, List[PumpEntry]]:
    """Process a single catalog PDF and return (catalog_type, valid_entries)."""
    cat_type = detect_catalog(pdf_path)
    fname = os.path.basename(pdf_path)
    log.info(f"[{cat_type:12}] {fname[:50]}")
    t0 = time.time()
    try:
        if cat_type == "UNKNOWN":
            # Try universal pipeline
            entries = parse_universal(pdf_path)
            elapsed = time.time() - t0
            log.info(f"  -> {len(entries)} models via UNIVERSAL  ({elapsed:.1f}s)")
            return "UNIVERSAL", entries
        else:
            # Use hardcoded parser
            pages = pdf_pages(pdf_path)
            extractor = EXTRACTORS.get(cat_type, EXTRACTORS["UNKNOWN"])
            raw = extractor(pages, pdf_path)
            valid = []
            for e in raw:
                # Bug #5: use validate_entry_basic from pump_validators
                ok, reason = validate_entry_basic(e.model, e.q_nom, e.h_nom, e.power_kw)
                if ok:
                    # Bug #6: run advanced physics validation for warnings
                    physics_ok, physics_reason, conf_adj = validate_pump_physics(
                        e.q_nom, e.h_nom, e.power_kw,
                        e.q_points, e.h_points,
                        e.model, e.series,
                    )
                    if conf_adj < 0:
                        e.warnings.append(f"physics: {physics_reason}, conf_adj={conf_adj}")
                        e.confidence = round(max(0.1, e.confidence + conf_adj), 2)
                    valid.append(e)
                else:
                    log.debug(f"  Dropped {e.model}: {reason}")
            # NEW: Enrich estimated entries from same PDF
            estimated_count = sum(1 for e in valid if e.data_source == DS_ESTIMATED)
            if estimated_count > 0:
                valid = _enrich_estimated(valid, pdf_path)
                valid = _spread_body_data(valid)  # Copy Q-H between power variants
                valid = _enrich_with_gemini(valid, pdf_path)
                valid = _spread_body_data(valid)  # Spread Vision results too
                new_est = sum(1 for e in valid if e.data_source == DS_ESTIMATED)
                log.info(f"  [ENRICH] {estimated_count} estimated → {new_est} remaining")

            # Post-process: clear mismatched curve data (wrong model assignment)
            for e in valid:
                if e.q_points and e.q_nom > 0:
                    q_max_c = max(e.q_points) if e.q_points else 0
                    if q_max_c > 0 and e.q_nom / q_max_c > 2.5:
                        e.q_points = []
                        e.h_points = []
                        if e.data_source == 'catalog_curve':
                            e.data_source = 'catalog_nominal'
            # Post-process: fix Q-H curve monotonicity (text extraction artifacts)
            for e in valid:
                if e.q_points and e.h_points and len(e.q_points) >= 3:
                    e.q_points, e.h_points = _fix_q_monotonicity(e.q_points, e.h_points)
            elapsed = time.time() - t0
            log.info(f"  -> {len(valid)}/{len(raw)} valid  ({elapsed:.1f}s)")
            return cat_type, valid
    except Exception as ex:
        elapsed = time.time() - t0
        log.error(f"  ERROR ({elapsed:.1f}s): {ex}", exc_info=True)
        return cat_type, []


def _entry_score(e: PumpEntry) -> int:
    """Quality score for deduplication: higher = preferred."""
    score = 0
    if e.has_curve():
        score += 8
    if e.data_source == DS_CATALOG_CURVE:
        score += 4
    elif e.data_source == DS_CATALOG_NOMINAL:
        score += 2
    elif e.data_source in (DS_UNIVERSAL_TABLE, DS_UNIVERSAL_QH):
        score += 1
    # else DS_ESTIMATED / DS_UNIVERSAL_TEXT / DS_GEMINI gets 0
    if e.h_nom > 0:
        score += 2
    if e.power_kw > 0:
        score += 1
    if e.article:
        score += 1
    # Confidence bonus
    score += int(e.confidence * 2)
    return score


def parse_all_catalogs(catalogs_dir: str, output_path: Optional[str] = None) -> Dict[str, Any]:
    """Parse all PDF catalogs in a directory and return unified database.

    Uses content-hash caching for incremental re-parsing.
    """
    pdfs = sorted(str(p) for p in Path(catalogs_dir).glob("*.pdf"))
    log.info(f"Found {len(pdfs)} PDFs in {catalogs_dir}")

    all_entries: List[PumpEntry] = []
    coverage: Dict[str, Any] = {}
    t_total = time.time()

    for pdf in pdfs:
        cat_type, entries = process_catalog(pdf)
        fname = os.path.basename(pdf)
        coverage[fname] = {
            "type": cat_type,
            "count": len(entries),
            "examples": [e.model for e in entries[:3]],
        }
        all_entries.extend(entries)

    # Deduplicate: prefer entries with higher quality score
    # Key includes series to prevent false collisions (e.g. CDM-32-5 vs CDM32-5)
    db: Dict[str, PumpEntry] = {}
    for e in all_entries:
        e.model = _clean_model_name(e.model)  # Bug #27
        key = e.series + ':' + normalize_model_name(e.model)
        if key not in db or _entry_score(e) > _entry_score(db[key]):
            db[key] = e

    # Cross-catalog enrichment: if same model appears in two catalogs
    # (one estimated, one enriched), copy Q-H data from enriched to estimated
    enriched_by_norm: Dict[str, PumpEntry] = {}
    for key, e in db.items():
        if e.data_source != DS_ESTIMATED:
            norm = normalize_model_name(e.model)
            if norm not in enriched_by_norm or _entry_score(e) > _entry_score(enriched_by_norm[norm]):
                enriched_by_norm[norm] = e
    cross_fixed = 0
    for key, e in db.items():
        if e.data_source == DS_ESTIMATED:
            norm = normalize_model_name(e.model)
            donor = enriched_by_norm.get(norm)
            if donor:
                if donor.q_nom > 0:
                    e.q_nom = donor.q_nom
                if donor.h_nom > 0:
                    e.h_nom = donor.h_nom
                if donor.q_points and donor.h_points:
                    e.q_points = donor.q_points
                    e.h_points = donor.h_points
                e.data_source = donor.data_source
                e.confidence = donor.confidence
                cross_fixed += 1
    if cross_fixed:
        log.info(f"Cross-catalog enrichment: upgraded {cross_fixed} estimated entries")

    # Post-process: fill in any remaining P=0 entries via hydraulic formula
    p0_fixed = 0
    for key, e in db.items():
        if e.power_kw == 0 and e.q_nom > 0 and e.h_nom > 0:
            # Bug #4: use imported estimate_power_hydraulic (no 0.95 multiplier)
            e.power_kw = estimate_power_hydraulic(e.q_nom, e.h_nom)
            p0_fixed += 1
            log.debug(f"Post-fix P=0: {key} -> P={e.power_kw:.2f}kW")

    if p0_fixed:
        log.info(f"Post-processing: filled power for {p0_fixed} models via hydraulic formula")

    total = len(db)
    with_h = sum(1 for v in db.values() if v.h_nom > 0)
    with_p = sum(1 for v in db.values() if v.power_kw > 0)
    with_curve = sum(1 for v in db.values() if v.has_curve())
    with_art = sum(1 for v in db.values() if v.article)
    elapsed_total = time.time() - t_total

    # data_source breakdown
    ds_curve = sum(1 for v in db.values() if v.data_source == DS_CATALOG_CURVE)
    ds_nominal = sum(1 for v in db.values() if v.data_source == DS_CATALOG_NOMINAL)
    ds_estimated = sum(1 for v in db.values() if v.data_source == DS_ESTIMATED)
    ds_universal = sum(1 for v in db.values()
                       if v.data_source in (DS_UNIVERSAL_TABLE, DS_UNIVERSAL_QH,
                                            DS_UNIVERSAL_TEXT, DS_GEMINI, DS_GEMINI_UNVERIFIED))

    if total:
        log.info(
            f"data_source breakdown: catalog_curve={ds_curve} ({ds_curve/total*100:.1f}%), "
            f"catalog_nominal={ds_nominal} ({ds_nominal/total*100:.1f}%), "
            f"estimated={ds_estimated} ({ds_estimated/total*100:.1f}%), "
            f"universal={ds_universal} ({ds_universal/total*100:.1f}%)"
        )
    else:
        log.info("data_source breakdown: no models")

    result = {
        "meta": {
            "schema_version": "10.0",
            "total_models": total,
            "total_parsed_raw": len(all_entries),
            "parse_time_seconds": round(elapsed_total, 1),
            "quality": {
                "pct_with_h_nom": round(with_h / total * 100, 1) if total else 0,
                "pct_with_power": round(with_p / total * 100, 1) if total else 0,
                "pct_with_curve": round(with_curve / total * 100, 1) if total else 0,
                "pct_with_article": round(with_art / total * 100, 1) if total else 0,
                "count_h_zero": total - with_h,
                "count_p_zero": total - with_p,
            },
            "data_source_counts": {
                "catalog_curve": ds_curve,
                "catalog_nominal": ds_nominal,
                "estimated": ds_estimated,
                "universal": ds_universal,
                "pct_catalog_curve": round(ds_curve / total * 100, 1) if total else 0,
                "pct_catalog_nominal": round(ds_nominal / total * 100, 1) if total else 0,
                "pct_estimated": round(ds_estimated / total * 100, 1) if total else 0,
                "pct_universal": round(ds_universal / total * 100, 1) if total else 0,
            },
            "catalog_coverage": coverage,
        },
        "pumps": {k: v.to_dict() for k, v in sorted(db.items())},
    }

    # Write output if path given
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        log.info(f"Output written to {output_path}")

    return result


# ---- CLI ----------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Universal Pump Parser v10")
    ap.add_argument("--dir", default="/root/ONIS/catalogs/")
    ap.add_argument("--out", default="/root/ONIS/all_pumps_database.json")
    ap.add_argument("--pdf", help="Parse single PDF")
    args = ap.parse_args()

    if args.pdf:
        cat_type, entries = process_catalog(args.pdf)
        print(f"\n=== {cat_type}: {len(entries)} pumps ===")
        for e in entries[:30]:
            curve = f"  curve:{len(e.q_points)}pts" if e.has_curve() else ""
            conf = f"  conf={e.confidence:.2f}" if e.confidence < 1.0 else ""
            print(f"  {e.model:<45} Q={e.q_nom:8.1f} H={e.h_nom:7.1f} P={e.power_kw:.2f}kW{curve}{conf}")
        if len(entries) > 30:
            print(f"  ... and {len(entries)-30} more")
    else:
        result = parse_all_catalogs(args.dir, args.out)

        q = result['meta']['quality']
        ds = result['meta']['data_source_counts']
        print(f"\n{'='*65}")
        print(f"Universal Pump Parser v10 — Schema {result['meta']['schema_version']}")
        print(f"{'='*65}")
        print(f"TOTAL UNIQUE MODELS: {result['meta']['total_models']}")
        print(f"PARSE TIME: {result['meta']['parse_time_seconds']}s")
        print(f"OUTPUT: {args.out}")
        print(f"\nData Quality:")
        print(f"  H_nom > 0 : {q['pct_with_h_nom']:5.1f}%  ({q['count_h_zero']} models H=0)")
        print(f"  Power > 0 : {q['pct_with_power']:5.1f}%  ({q['count_p_zero']} models P=0)")
        print(f"  Has curve : {q['pct_with_curve']:5.1f}%")
        print(f"  Has article: {q['pct_with_article']:5.1f}%")
        print(f"\nData Sources:")
        print(f"  catalog_curve  : {ds['catalog_curve']:4} ({ds['pct_catalog_curve']:.1f}%)")
        print(f"  catalog_nominal: {ds['catalog_nominal']:4} ({ds['pct_catalog_nominal']:.1f}%)")
        print(f"  estimated      : {ds['estimated']:4} ({ds['pct_estimated']:.1f}%)")
        print(f"  universal      : {ds['universal']:4} ({ds['pct_universal']:.1f}%)")
        print(f"\nCoverage per catalog:")
        total_found = 0
        for fname, info in result['meta']['catalog_coverage'].items():
            status = "OK" if info['count'] > 0 else "!!"
            total_found += info['count']
            print(f"  {status} [{info['type']:12}] {fname[:50]:<52} {info['count']:4} models")
        print(f"\n  TOTAL (with duplicates): {total_found}")
