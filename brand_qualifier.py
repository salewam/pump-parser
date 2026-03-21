"""
Universal Brand Qualification Module for PDF Pump Catalogs.
Determines pump catalog brand from PDF content and/or parsed models.
Standalone — no Flask, no GPU, no parser_app dependency.

ARCHITECTURE:
- PDF text is the ONLY source of truth for brand detection.
- qualify(pdf_path) is the PRIMARY method — reads PDF, finds brand markers.
- qualify_from_models() is FALLBACK ONLY — used when PDF is unavailable.
- qualify_full() calls both, but PDF ALWAYS wins if it found anything.
- Series in BRAND_REGISTRY are ONLY for series uniquely manufactured by that brand.
  Shared/resold series should NOT be in registry — they create false positives.
"""

import os
from dataclasses import dataclass, field

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None


@dataclass
class BrandResult:
    brand: str = "Unknown"
    confidence: float = 0.0
    source: str = "none"  # pdf_text | filename | models | combined
    series_detected: list = field(default_factory=list)
    markers_found: list = field(default_factory=list)

    def to_dict(self):
        return {
            "brand": self.brand,
            "confidence": round(self.confidence, 3),
            "source": self.source,
            "series_detected": self.series_detected,
            "markers_found": self.markers_found,
        }


# ── Brand Registry ──────────────────────────────────────────────────
# ONLY series uniquely manufactured by the brand.
# Do NOT add series that are resold/distributed by multiple brands.
# priority: lower = checked first.

BRAND_REGISTRY = {
    "Fancy": {
        "text_markers": ["fancy", "фэнси", "fancy pump", "fancypump", "fancy pumps"],
        "filename_markers": ["fancy"],
        "series_prefixes": ["FST", "FS4", "FSM", "FVH"],
        "series_exact": ["FS", "FV"],
        "priority": 5,
    },
    "Wellmix": {
        "text_markers": ["wellmix-pump.ru", "wellmix", "танк", "томская арматурно"],
        "domain_markers": ["wellmix-pump.ru"],
        "series_prefixes": ["CMI", "NBS", "CV", "CVF", "TG", "TL", "TD", "CDM", "CDMF", "CDL", "CDLF"],
        "priority": 7,
    },
    "Aquastrong": {
        "text_markers": ["aquastrong", "aquastrong.it"],
        "domain_markers": ["aquastrong.it"],
        "series_prefixes": ["EVR", "EVS"],
        "priority": 7,
    },
    "CNP": {
        "text_markers": ["nanfang", "сиэнпи", "cnp pumps"],
        "text_markers_min3": ["cnp"],
        "series_prefixes": ["ZS", "SJ"],
        "priority": 8,
    },
    "NPK Istra": {
        "text_markers": ["научно-производственный комплекс", "г. истра", "npk istra"],
        "series_prefixes": ["BM", "BMN"],
        "series_exact": ["BO"],
        "priority": 9,
    },
    "ONIS": {
        "text_markers": [
            "onis.ru", "онис", "onis innovation", "onis pumps",
            "8 (800) 500-63-17", "8(800)500-63-17", "8(800) 500-63-17",
        ],
        "domain_markers": ["onis.ru"],
        "series_prefixes": ["MV", "MVS", "INL", "MBL", "PV"],
        "priority": 10,
    },
    "Lowara": {
        "text_markers": ["lowara", "xylem"],
        "series_prefixes": ["LVR", "e-SV", "FCE"],
        "priority": 10,
    },
    "Grundfos": {
        "text_markers": ["grundfos"],
        "series_prefixes": ["CRN", "CRE", "CRI", "SPK", "NKE", "NBE", "TPE", "CME"],
        "series_exact": ["CR", "SP", "NK", "NB", "TP", "CM"],
        "priority": 10,
    },
    "Wilo": {
        "text_markers": ["wilo"],
        "series_prefixes": ["MVI", "HELIX", "COR-", "STRATOS", "TOP-S"],
        "priority": 10,
    },
    "KSB": {
        "text_markers": ["ksb"],
        "series_prefixes": ["Etanorm", "Movitec", "Etabloc", "MTC"],
        "priority": 10,
    },
    "Pedrollo": {
        "text_markers": ["pedrollo"],
        "series_prefixes": ["CP ", "HF ", "NGA"],
        "priority": 10,
    },
    "Ebara": {
        "text_markers": ["ebara"],
        "series_prefixes": ["DWO", "EVM", "EVMS", "3LM"],
        "priority": 10,
    },
    "DAB": {
        "text_markers": ["dab pumps", "dab water technology"],
        "series_prefixes": ["KVC", "ESYBOX", "E.SYBOX"],
        "priority": 10,
    },
}

# Series-to-brand mapping for quick lookup (built from registry)
SERIES_TO_BRAND = {}
for _brand, _cfg in BRAND_REGISTRY.items():
    for _s in _cfg.get("series_prefixes", []):
        SERIES_TO_BRAND[_s.strip().upper()] = _brand
    for _s in _cfg.get("series_exact", []):
        SERIES_TO_BRAND[_s.strip().upper()] = _brand


class BrandQualifier:
    """Universal brand qualifier for pump PDF catalogs."""

    def __init__(self, registry=None):
        self.registry = registry or BRAND_REGISTRY
        self._sorted = sorted(
            self.registry.items(),
            key=lambda x: x[1].get("priority", 99),
        )

    def qualify(self, pdf_path: str) -> BrandResult:
        """PRIMARY: determine brand from PDF text + filename.
        This is the source of truth. Always use this when PDF is available."""
        result = BrandResult()

        pdf_text = self._extract_text(pdf_path, max_pages=10)
        text_lower = pdf_text.lower()
        filename = os.path.basename(pdf_path).lower()

        best_brand = None
        best_conf = 0.0
        best_markers = []

        for brand_name, cfg in self._sorted:
            markers = []
            conf = 0.0

            for m in cfg.get("domain_markers", []):
                if m.lower() in text_lower:
                    markers.append(f"domain:{m}")
                    conf = max(conf, 0.95)

            for m in cfg.get("text_markers", []):
                if m.lower() in text_lower:
                    markers.append(f"text:{m}")
                    conf = max(conf, 0.9)

            for m in cfg.get("text_markers_min3", []):
                count = text_lower.count(m.lower())
                if count >= 3:
                    markers.append(f"text:{m}(x{count})")
                    conf = max(conf, 0.85)

            for m in cfg.get("filename_markers", []):
                if m.lower() in filename:
                    markers.append(f"filename:{m}")
                    conf = max(conf, 0.8)

            if conf > best_conf:
                best_conf = conf
                best_brand = brand_name
                best_markers = markers

        if best_brand and best_conf > 0:
            result.brand = best_brand
            result.confidence = best_conf
            result.source = (
                "pdf_text" if any("text:" in m or "domain:" in m for m in best_markers)
                else "filename"
            )
            result.markers_found = best_markers

        return result

    def qualify_from_models(self, models: list) -> BrandResult:
        """FALLBACK: determine brand from model names. Use ONLY when PDF unavailable."""
        result = BrandResult()
        if not models:
            return result

        names = []
        for m in models:
            n = m.get("model") or m.get("id") or m.get("name") or ""
            n = n.strip()
            if n:
                n = (n.replace("\u041c", "M").replace("\u0412", "B")
                      .replace("\u0421", "C").replace("\u0415", "E")
                      .replace("\u041d", "H").replace("\u041e", "O")
                      .replace("\u0420", "P").replace("\u0422", "T")
                      .replace("\u0410", "A").replace("\u041a", "K")
                      .replace("\u0425", "X"))
                names.append(n.upper())

        if not names:
            return result

        brand_scores = {}
        series_found = {}

        for brand_name, cfg in self._sorted:
            score = 0
            found = set()

            for pfx in cfg.get("series_prefixes", []):
                pfx_u = pfx.strip().upper()
                hits = sum(1 for n in names if n.startswith(pfx_u))
                if hits:
                    score += hits
                    found.add(pfx.strip())

            for ex in cfg.get("series_exact", []):
                ex_u = ex.strip().upper()
                hits = sum(
                    1 for n in names
                    if n.startswith(ex_u) and len(n) > len(ex_u)
                    and n[len(ex_u)] in " -0123456789"
                )
                if hits:
                    score += hits
                    found.add(ex.strip())

            if score > 0:
                brand_scores[brand_name] = score
                series_found[brand_name] = sorted(found)

        if brand_scores:
            best = max(brand_scores, key=brand_scores.get)
            ratio = brand_scores[best] / len(names) if names else 0
            result.brand = best
            result.confidence = min(0.95, 0.5 + ratio * 0.45)
            result.source = "models"
            result.series_detected = series_found.get(best, [])

        return result

    def qualify_full(self, pdf_path: str, models: list) -> BrandResult:
        """Combined: PDF text + models. PDF ALWAYS wins if it found anything."""
        pre = self.qualify(pdf_path)
        post = self.qualify_from_models(models)

        # PDF found a brand — it ALWAYS wins
        if pre.brand != "Unknown" and pre.confidence > 0:
            pre.series_detected = post.series_detected or pre.series_detected
            # Boost confidence if models agree
            if post.brand == pre.brand:
                pre.confidence = min(1.0, pre.confidence + 0.05)
                pre.source = "combined"
            return pre

        # PDF found nothing — fallback to models
        if post.brand != "Unknown":
            return post

        return BrandResult()

    @staticmethod
    def brand_for_series(series_name: str) -> str:
        """Quick lookup: series name -> brand."""
        return SERIES_TO_BRAND.get(series_name.strip().upper(), "Unknown")

    @staticmethod
    def _extract_text(pdf_path: str, max_pages: int = 10) -> str:
        if not fitz:
            return ""
        try:
            doc = fitz.open(pdf_path)
            parts = []
            for i, page in enumerate(doc):
                if i >= max_pages:
                    break
                parts.append(page.get_text())
            doc.close()
            return "\n".join(parts)
        except Exception:
            return ""


_default_qualifier = BrandQualifier()

def qualify_pdf(pdf_path: str) -> BrandResult:
    return _default_qualifier.qualify(pdf_path)

def qualify_models(models: list) -> BrandResult:
    return _default_qualifier.qualify_from_models(models)

def qualify_full(pdf_path: str, models: list) -> BrandResult:
    return _default_qualifier.qualify_full(pdf_path, models)

def brand_for_series(series_name: str) -> str:
    return BrandQualifier.brand_for_series(series_name)
