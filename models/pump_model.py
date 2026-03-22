"""
Pump model helper functions.
Extracted from parser_app.py — detect_series, parse_number, enrich, validate, catalog type.
"""
import re
import math

from config import KNOWN_SERIES


def detect_series(model_name):
    """Extract series prefix from model name. CDM 10-5 → CDM"""
    if not model_name:
        return ""
    pattern = "|".join(sorted(KNOWN_SERIES, key=len, reverse=True))
    m = re.match(rf"({pattern})\s*[/]?\s*\w*\s*(\d+)", model_name, re.I)
    if m:
        return m.group(1).upper()
    parts = model_name.split("-")[0].split(); return parts[0].upper() if parts else "" if model_name else ""


def parse_number(val):
    """Parse number from table cell: '1,5' → 1.5, '12.5 м³/ч' → 12.5"""
    if val is None:
        return None
    val = str(val).strip()
    if not val:
        return None
    val = val.lstrip("~>< ≈≥≤")
    # Thousand separators: 1,234.5
    if re.match(r"^\d{1,3}(,\d{3})+(\.\d+)?$", val):
        val = val.replace(",", "")
    if not val:
        return None
    # Skip model names / text
    if re.match(r"[A-ZА-Яa-zа-я]{2}", val):
        return None
    val = val.replace(",", ".")
    # Scientific notation
    if "e" in val.lower() and any(c.isdigit() for c in val):
        try:
            n = float(val)
            return None if abs(n) > 1e6 else n
        except ValueError:
            return None
    # Ranges: take first
    val = re.split(r"[-–—~…]", val)[0].strip()
    # Fractions: take numerator
    if "/" in val:
        val = val.split("/")[0].strip()
    # Strip units
    val = re.sub(r"[^\d.]", "", val)
    if val.count(".") > 1:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def validate_pump_physics(m):
    """Reset physically impossible values. Works with dict (q_nom/h_nom/power_kw keys)."""
    q = m.get("q_nom", 0) or 0
    h = m.get("h_nom", 0) or 0
    kw = m.get("power_kw", 0) or 0

    if q > 1000 or h > 500 or kw > 500:
        m["q_nom"] = 0
        m["h_nom"] = 0
        m["power_kw"] = 0
        return

    if kw > 0 and q > 0 and h > 0:
        p_hyd = (q / 3600) * h * 9.81
        if p_hyd / kw > 3.0:
            m["q_nom"] = 0
            m["h_nom"] = 0


def enrich_from_model_name(m):
    """Extract Q, H, kW from pump model naming conventions. Works with dict."""
    if not m.get("model"):
        return
    name = m["model"].replace(",", ".").strip()
    name = name.replace("\u200b", "").replace("\u200c", "").replace("\ufeff", "")
    name = name.replace("\u2013", "-").replace("\u2014", "-").replace("\u2012", "-")
    m["model"] = name

    # INL: INL{DN}-{Q}-{H}-{kW}/{poles}
    match = re.match(r"INL\s*(\d+)\s*[-]\s*([\d.]+)\s*[-]\s*([\d.]+)\s*[-]\s*([\d.]+)\s*/\s*(\d+)", name, re.I)
    if match:
        m["q_nom"] = float(match.group(2))
        m["h_nom"] = float(match.group(3))
        m["power_kw"] = float(match.group(4))
        poles = int(match.group(5))
        m["rpm"] = 3000 // (poles // 2) if poles >= 2 else 2900
        return

    # MBL: MBL {DN}-{imp_mm}-{kW}/{poles}
    match = re.match(r"[МM][ВB]L\s*(\d+)\s*[-]\s*([\d.]+)[A-Za-zА-Яа-я]*(?:\s*[-]\s*([\d.]+)\s*(?:/\s*(\d+))?)?", name, re.I)
    if match:
        dn = int(match.group(1))
        imp_mm = float(match.group(2))
        rpm = m.get("rpm") or 2900
        u2 = math.pi * (imp_mm / 1000) * rpm / 60
        m["h_nom"] = round(u2**2 / 9.81 * 0.55, 1)
        if match.group(3):
            m["power_kw"] = float(match.group(3))
        if match.group(4):
            poles = int(match.group(4))
            m["rpm"] = 3000 // (poles // 2) if poles >= 2 else 2900
        dn_q = {32: 6.3, 40: 12.5, 50: 25, 65: 50, 80: 50, 100: 100, 125: 160, 150: 200, 200: 400}
        m["q_nom"] = dn_q.get(dn, dn * 0.5)
        return

    # FV/FVH: FV {Q}x{stages}/{kW}
    fv = re.match(r"(?:FV|FVH)\s*(\d+)[x×](\d+)[/]?([\d.]*)", name, re.I)
    if fv and not m.get("q_nom"):
        m["q_nom"] = float(fv.group(1))
        if fv.group(3):
            try: m["power_kw"] = float(fv.group(3))
            except: pass

    # TL/TG/TD: {series} {DN}-{stages}
    tl = re.match(r"(?:TL|TG|TD)\s*(\d+)\s*[-]\s*(\d+)", name, re.I)
    if tl and not m.get("q_nom"):
        dn = int(tl.group(1))
        dn_q = {25: 6, 32: 12.5, 40: 20, 50: 30, 65: 50, 80: 80, 100: 120, 125: 180, 150: 250, 200: 400}
        m["q_nom"] = dn_q.get(dn, dn * 0.5)

    # FST/FS/FS4/FSM: {series} {DN}-{impeller}/{kW}
    fst = re.match(r"(?:FST4|FST|FS4|FSM|FS)\s*(\d+)\s*[-]\s*(\d+)(?:\s*/\s*([\d.]+))?", name, re.I)
    if fst and not m.get("q_nom"):
        dn = int(fst.group(1))
        dn_q = {25: 3, 32: 6.3, 40: 12.5, 50: 25, 65: 50, 80: 50, 100: 100, 125: 160, 150: 200}
        m["q_nom"] = dn_q.get(dn, dn * 0.5)
        if fst.group(3) and not m.get("power_kw"):
            try:
                kw_val = float(fst.group(3))
                if kw_val <= 200:
                    m["power_kw"] = kw_val
            except: pass

    # PV: PV(n) {Q}-{stages}
    pv = re.match(r"PV\s*\(?n?\)?\s*(\d+)\s*[-]\s*(\d+)", name, re.I)
    if pv and not m.get("q_nom"):
        m["q_nom"] = float(pv.group(1))

    # CDM/CDMF/CDL/CDLF: {series} {Q}-{stages}
    cdm = re.match(r"(?:CDM|CDMF|CDL|CDLF)\s*F?\s*(\d+)\s*[-]\s*(\d+)", name, re.I)
    if cdm and not m.get("q_nom"):
        m["q_nom"] = float(cdm.group(1))

    # CV/CVF: {series} {Q}-{stages}
    cv = re.match(r"(?:CV|CVF)\s*(\d+)\s*[-]\s*(\d+)", name, re.I)
    if cv and not m.get("q_nom"):
        m["q_nom"] = float(cv.group(1))

    # EVR/EVS
    if not m.get("q_nom"):
        evr = re.match(r"(?:EVR|EVS)\(?S?\)?\s*(\d+)\s*[-]+\s*(\d+)", name, re.I)
        if not evr:
            evr = re.match(r"(\d+)\s*[-]{1,}\s*(\d+)", name)
        if evr:
            q_val = float(evr.group(1))
            if q_val <= 200:
                m["q_nom"] = q_val

    # Generic fallback
    if m.get("q_nom") and m.get("power_kw"):
        return
    match = re.match(r"[A-ZА-Яa-zа-я]+\s*([\d.]+)\s*[-]\s*([\d.]+)\s*[-]\s*([\d.]+)\s*(?:/\s*(\d+))?", name)
    if match:
        nums = [float(match.group(i)) for i in (1, 2, 3) if match.group(i)]
        if len(nums) >= 3 and not m.get("power_kw") and min(nums) < 50:
            m["power_kw"] = min(nums)
        if match.group(4):
            poles = int(match.group(4))
            m["rpm"] = 3000 // (poles // 2) if poles >= 2 else 2900


def normalize_model_key(name):
    """Normalize model name for dedup."""
    if not name:
        return ""
    k = name.strip()
    k = re.sub(r"\.0(?=[/\s-]|$)", "", k)
    k = re.sub(r"\s+", " ", k)
    k = re.sub(r"([A-Za-zА-я])\s+(\d)", r"\1\2", k)
    for cyr, lat in [("\u041c", "M"), ("\u0412", "B"), ("\u0421", "C"),
                     ("\u0415", "E"), ("\u041d", "H"), ("\u041e", "O"),
                     ("\u0420", "P"), ("\u0422", "T"), ("\u0410", "A")]:
        k = k.replace(cyr, lat)
    k = k.replace("\u200b", "").replace("\u200c", "").replace("\ufeff", "")
    k = k.replace(",", ".")  # normalize comma decimal
    k = k.replace("\u2013", "-").replace("\u2014", "-").replace("\u2012", "-")
    return k


def detect_catalog_type(models):
    """Detect catalog type from model names. Accepts list of dicts or PumpModelResults."""
    if not models:
        return "PUMP"
    names = []
    for m in models:
        if hasattr(m, "model"):
            names.append(m.model)
        elif isinstance(m, dict):
            names.append(m.get("model", ""))
    all_names = " ".join(names).upper()
    groups = [
        (("CDM", "CDMF"), "CDM_CDMF"),
        (("CDL", "CDLF"), "CDL_CDLF"),
        (("CV", "CVF"), "CV_CVF"),
        (("TG", "TL", "TD"), "TG_TL_TD"),
        (("FST", "FS4", "FSM"), "FST_FS"),
        (("FV", "FVH"), "FV_FVH"),
        (("EVR", "EVS"), "EVR_EVS"),
    ]
    for prefixes, group_name in groups:
        for p in prefixes:
            if p in all_names:
                return group_name
    # Single series
    for s in sorted(KNOWN_SERIES, key=len, reverse=True):
        if s in all_names:
            return s
    return "PUMP"
