"""
Stage 1: Docling TableFormer — PDF table extraction via GPU server.
HTTP client with retry logic. Strategy 1 (direct columns) + Strategy 2 (CDM-style).
"""
import os
import re
import time
import logging
import requests

import sys
sys.path.insert(0, "/root/pump_parser")
from config import GPU_DOCLING_URL, DOCLING_TIMEOUT, DOCLING_HEALTH_TIMEOUT, DOCLING_RETRIES, PUMP_MODEL_RE
from models.parse_result import PumpModelResult, StageResult
from models.pump_model import detect_series, parse_number, enrich_from_model_name, validate_pump_physics

logger = logging.getLogger(__name__)


class DoclingStage:
    """Stage 1: Extract tables from PDF via Docling GPU server."""

    def __init__(self, url=None, timeout=None, retries=None):
        self._url = url or GPU_DOCLING_URL
        self._timeout = timeout or DOCLING_TIMEOUT
        self._retries = retries or DOCLING_RETRIES

    def _health_check(self) -> bool:
        try:
            resp = requests.get(f"{self._url}/health", timeout=DOCLING_HEALTH_TIMEOUT)
            return resp.status_code == 200
        except Exception:
            return False

    def extract(self, pdf_path: str) -> StageResult:
        """Send PDF to Docling, get tables, convert to models."""
        result = StageResult(source="docling")

        if not self._health_check():
            result.errors.append("Docling GPU недоступен")
            logger.error("Docling health check failed: %s", self._url)
            return result

        tables = None
        last_error = None

        for attempt in range(1, self._retries + 1):
            try:
                logger.info("Docling attempt %d/%d: %s", attempt, self._retries, os.path.basename(pdf_path))
                with open(pdf_path, "rb") as f:
                    resp = requests.post(
                        f"{self._url}/parse",
                        files={"file": (os.path.basename(pdf_path), f, "application/pdf")},
                        timeout=self._timeout,
                    )
                if resp.status_code == 200:
                    data = resp.json()
                    tables = data.get("tables", [])
                    result.pages_processed = data.get("total_pages", 0)
                    logger.info("Docling OK: %d tables from %d pages", len(tables), result.pages_processed)
                    break
                else:
                    last_error = f"HTTP {resp.status_code}"
                    logger.warning("Docling attempt %d: %s", attempt, last_error)
            except requests.exceptions.Timeout:
                last_error = f"Timeout ({self._timeout}s)"
                logger.warning("Docling attempt %d: timeout", attempt)
            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {e}"
                logger.warning("Docling attempt %d: connection error", attempt)
            except Exception as e:
                last_error = str(e)
                logger.warning("Docling attempt %d: %s", attempt, last_error)

            if attempt < self._retries:
                backoff = 2 ** attempt
                logger.info("Retrying in %ds...", backoff)
                time.sleep(backoff)

        if tables is None:
            result.errors.append(f"Docling: все попытки исчерпаны ({last_error})")
            return result

        result.raw_tables = tables
        result.models = self._tables_to_models(tables)
        return result

    # ── Column identification ───────────────────────────────────────

    def _identify_columns(self, cols):
        """Keyword-based column role detection. Returns 6-tuple with hp_col."""
        model_col = q_col = h_col = kw_col = rpm_col = hp_col = None

        # Skip dimension tables (L1, L2, B1, etc.)
        cols_lower = [str(c).lower().strip() for c in cols]
        # Skip ONLY if table has NO model column AND is purely dimensions
        dim_markers = ["размеры [мм]", "dimensions [mm]", "габаритные размеры"]
        has_model_kw = any(any(k in cl for k in ["модель", "model", "насос", "pump", "наименование"]) for cl in cols_lower)
        is_pure_dim = (not has_model_kw and
                       sum(1 for cl in cols_lower if any(d in cl for d in dim_markers)) >= 1)
        if is_pure_dim:
            return model_col, q_col, h_col, kw_col, rpm_col, hp_col

        MODEL_KW = ["модель", "model", "тип", "type", "наименование", "обозначение",
                    "pump type", "pump model", "size"]
        Q_KW = ["подача", "расход", "flow", "q,", "q ", "м³/ч", "m3/h", "производительность", "capacity", "qном", "q"]
        H_KW = ["напор", "head", "h,", "нном", "hном", "метрах", "метров"]
        # "h " (h-space) is too ambiguous — matches "m/h 0" in flow-rate column headers
        # Only use "h " if NOT preceded by "/" (e.g. "m/h ", "l/h " are flow units, not head)
        H_KW_HSPACE = "h "  # handled separately with "/" guard
        # "h" alone is too generic — matches H1, H2 (dimensions). Only match exact "h" or "h (m)"
        # Guard: reject bare "H" if surrounded by dimension columns (A, B, L, D etc.)
        H_EXACT = ["h"]  # exact match only (not substring)
        DIM_CODES = {"a", "a1", "a2", "b", "b1", "b2", "c", "d", "d1", "d2",
                     "e", "f", "l", "l1", "l2", "h1", "h2", "w", "w1", "w2",
                     "n1", "n2", "s", "kg", "mm"}
        KW_KW = ["мощность", "power", "квт", "kw", "p2", "p₂", "мощн", "р2"]
        RPM_KW = ["об/мин", "rpm", "частота вращения", "скорость", "n,"]
        HP_KW = ["hp", "л.с.", "лс"]

        def _matches_h_kw(cl):
            """Check H keyword match with guard against flow-unit false positives."""
            if any(k in cl for k in H_KW):
                return True
            # "h " match: only if not preceded by "/" (avoids "m/h ", "l/h ")
            idx = cl.find(H_KW_HSPACE)
            if idx >= 0 and (idx == 0 or cl[idx - 1] != "/"):
                return True
            return False

        # Detect Q-H performance tables: many Q-keyword columns OR many numeric Q-point columns
        q_match_count = sum(1 for c in cols if any(k in str(c).lower().strip() for k in Q_KW))
        qpoint_count = DoclingStage._count_qpoint_cols(cols)
        is_qh_grid = q_match_count >= 4 or qpoint_count >= 4

        for col in cols:
            cl = str(col).lower().strip()
            has_q = any(k in cl for k in Q_KW)
            # Guard H_EXACT: reject bare "h" when table has dimension-like columns
            _bare_h = cl.strip() in H_EXACT
            if _bare_h:
                _dim_count = sum(1 for c2 in cols_lower if c2.strip().rstrip(".") in DIM_CODES)
                if _dim_count >= 3:
                    _bare_h = False  # surrounded by dimension columns → physical height, not head
            has_h = _matches_h_kw(cl) or _bare_h
            has_kw = any(k in cl for k in KW_KW)
            # When column has BOTH kW and H keywords (merged Docling header),
            # treat as kW — power is the primary column, H will come from other columns
            if has_kw and has_h:
                has_h = False
            if not model_col and any(k in cl for k in MODEL_KW):
                model_col = col
            elif not q_col and has_q and not has_h and not is_qh_grid:
                # Pure Q column (no H keywords), and not a Q-H performance grid
                q_col = col
            elif not h_col and has_h:
                h_col = col
            elif not kw_col and has_kw:
                kw_col = col
            elif not rpm_col and any(k in cl for k in RPM_KW):
                rpm_col = col

        # Second pass: find hp_col (separate from kw_col)
        for col in cols:
            if col in (model_col, q_col, h_col, kw_col, rpm_col):
                continue
            cl = str(col).lower().strip()
            if any(k in cl for k in HP_KW):
                hp_col = col
                break

        return model_col, q_col, h_col, kw_col, rpm_col, hp_col

    @staticmethod
    def _count_qpoint_cols(cols):
        """Count columns with Q-point numeric names (e.g. '26 100 6', 'GPM 0.l/min0 m/h 0.',
        '20.1.2.H=Total manometric head in meters')."""
        _FLOW_UNITS = {"gpm", "l", "min", "m", "h"}
        count = 0
        for c in cols:
            cl = str(c).lower().strip()
            if len(cl) < 3:
                continue
            # Method 1: pure numeric after stripping flow units
            stripped = cl
            for u in _FLOW_UNITS:
                stripped = stripped.replace(u, "")
            stripped = re.sub(r'[/\s.×,]', '', stripped)
            if stripped and all(ch.isdigit() for ch in stripped):
                count += 1
                continue
            # Method 2: starts with digits, followed by head keywords
            # e.g. "20.1.2.H=Total manometric head in meters"
            if re.match(r'^\d+[.\d]*\.(?:h=|head|напор)', cl):
                count += 1
                continue
            # Method 3: starts with "Q=" or "q=" followed by capacity/delivery
            if re.match(r'^q[=\s]', cl):
                count += 1
        return count

    def _find_header_models(self, cols):
        """Find columns whose names match pump model patterns."""
        models = []
        for col in cols:
            if re.match(PUMP_MODEL_RE, str(col).strip(), re.I):
                models.append(col)
        return models

    # ── Table to models conversion ──────────────────────────────────

    def _tables_to_models(self, tables):
        """Convert Docling tables to PumpModelResult list.
        Strategy 1: direct column mapping (model + Q/H/kW columns).
        Strategy 2: CDM-style (models in headers, params in rows).
        Strategy 3 (positional) deliberately NOT implemented — replaced by VLM.
        """
        all_models = []
        seen_keys = set()

        for table in tables:
            cols = table.get("columns", [])
            rows = table.get("data", [])
            page = table.get("page", 0)

            if not cols or not rows:
                continue

            model_col, q_col, h_col, kw_col, rpm_col, hp_col = self._identify_columns(cols)

            # Detect Q-H performance grid: many Q-keyword columns OR many Q-point numeric columns
            _Q_KW = ["подача", "расход", "flow", "q,", "q ", "м³/ч", "m3/h", "производительность", "capacity", "qном", "q"]
            q_kw_count = sum(1 for c in cols if any(k in str(c).lower().strip() for k in _Q_KW))
            qpoint_count = self._count_qpoint_cols(cols)
            is_qh_grid = q_kw_count >= 4 or qpoint_count >= 4

            # ── Extract header-row model (first model embedded in column name) ──
            if model_col and h_col:
                header_model = self._extract_header_row(model_col, h_col, kw_col, q_col, hp_col, page, all_models, seen_keys)

            # ── Strategy 1: direct column mapping ──
            h_is_dim = h_col and any(mk in str(h_col).lower() for mk in ["(мм)", "(mm)", "[мм]", "[mm]", "мм]", "mm]"])
            if model_col and any([q_col, h_col, kw_col]):
                h_conf = 0.3 if h_is_dim else 0.6
                self._strategy1(rows, model_col, q_col, h_col, kw_col, rpm_col, page, all_models, seen_keys, h_confidence=h_conf, hp_col=hp_col, qh_grid=is_qh_grid)
                continue

            # ── Strategy 2: CDM-style (models in headers, params in rows) ──
            header_models = self._find_header_models(cols)
            if header_models and len(rows) >= 3:
                self._strategy2(rows, cols, header_models, page, all_models, seen_keys)
                continue

            # ── Strategy 3: model column only (no Q/H/kW) — enrich from name ──
            if model_col:
                for row in rows:
                    model_name = str(row.get(model_col, "")).strip()
                    if not model_name or len(model_name) < 5:
                        continue
                    pm = self._build_model(model_name, None, None, None, None, page)
                    if pm and pm.key and pm.key not in seen_keys and pm.q > 0 and pm.kw > 0:
                        seen_keys.add(pm.key)
                        all_models.append(pm)

        # Final validation: catch obvious garbage from dedup updates
        for m in all_models:
            # H == kW is always wrong (dimension height leaked into both fields)
            if m.h > 0 and m.h == m.kw:
                m.h = 0; m.kw = 0
                m.confidence_h = 0; m.confidence_kw = 0
            if m.q > 2000 or m.h > 1000 or m.kw > 500:
                m.q = 0; m.h = 0; m.kw = 0
                m.confidence_q = 0; m.confidence_h = 0; m.confidence_kw = 0
                # Re-enrich from model name (clean data after garbage removal)
                d = {"model": m.model, "q_nom": 0, "h_nom": 0, "power_kw": 0, "rpm": m.rpm}
                enrich_from_model_name(d)
                if d["q_nom"]:
                    m.q = d["q_nom"]; m.confidence_q = 0.5; m.source_q = "enrichment"
                if d["h_nom"]:
                    m.h = d["h_nom"]; m.confidence_h = 0.5; m.source_h = "enrichment"
                if d["power_kw"]:
                    m.kw = d["power_kw"]; m.confidence_kw = 0.5; m.source_kw = "enrichment"

        # Sibling copy: fill gaps from variant pairs (e.g. CDL 32-80-2 ↔ CDL 32-80)
        model_map = {m.key: m for m in all_models}
        for m in all_models:
            if m.is_complete:
                continue
            # Try adding/removing "-2" suffix to find sibling
            name = m.model
            if name.endswith("-2"):
                sibling_name = name[:-2].strip()
            else:
                sibling_name = name + "-2"
            sibling_key = re.sub(r'[\s()]+', '', sibling_name.upper().replace(",", "."))
            sibling = model_map.get(sibling_key)
            if not sibling:
                continue
            if not m.q and sibling.q:
                m.q = sibling.q; m.confidence_q = 0.4; m.source_q = "sibling"
            if not m.h and sibling.h:
                m.h = sibling.h; m.confidence_h = 0.4; m.source_h = "sibling"
            if not m.kw and sibling.kw:
                m.kw = sibling.kw; m.confidence_kw = 0.4; m.source_kw = "sibling"

        # Phase variant copy: single-phase ↔ three-phase (Fm↔F, CPm↔CP, PKm↔PK, CDXM↔CDX)
        for m in all_models:
            if m.is_complete:
                continue
            name = m.model
            # Try strip/add "m" suffix after series prefix (Fm32 → F 32, CPm160 → CP 160)
            phase_sibling = None
            m_match = re.match(r'^([A-Za-z]+?)m(\d.*)$', name)
            if m_match:
                # Single-phase → try three-phase: Fm32/160C → F 32/160C
                phase_sibling = f"{m_match.group(1)} {m_match.group(2)}"
            else:
                m_match = re.match(r'^([A-Za-z]+)\s+(\d.*)$', name)
                if m_match:
                    # Three-phase → try single-phase: F 32/160C → Fm32/160C
                    phase_sibling = f"{m_match.group(1)}m{m_match.group(2)}"
            if phase_sibling:
                ps_key = re.sub(r'[\s()]+', '', phase_sibling.upper().replace(",", "."))
                donor = model_map.get(ps_key)
                if donor:
                    if not m.q and donor.q:
                        m.q = donor.q; m.confidence_q = 0.35; m.source_q = "phase_variant"
                    if not m.h and donor.h:
                        m.h = donor.h; m.confidence_h = 0.35; m.source_h = "phase_variant"
                    if not m.kw and donor.kw:
                        m.kw = donor.kw; m.confidence_kw = 0.35; m.source_kw = "phase_variant"

        # Neighbor copy: for still-incomplete models, try +-1 neighbor in same series
        # e.g. CDM10-15 → CDM10-14 or CDM10-16
        for m in all_models:
            if m.is_complete:
                continue
            # Match last number in model name: "CDM10-15" → prefix="CDM10-", num=15
            match = re.match(r'^(.+\D)(\d+)$', m.model)
            if not match:
                continue
            prefix, num_str = match.groups()
            num = int(num_str)
            for delta in [-1, +1]:
                neighbor_name = f"{prefix}{num + delta}"
                neighbor_key = re.sub(r'[\s()]+', '', neighbor_name.upper().replace(",", "."))
                neighbor = model_map.get(neighbor_key)
                if not neighbor or not neighbor.is_complete:
                    continue
                if not m.q and neighbor.q:
                    m.q = neighbor.q; m.confidence_q = 0.3; m.source_q = "neighbor"
                if not m.kw and neighbor.kw:
                    m.kw = neighbor.kw; m.confidence_kw = 0.3; m.source_kw = "neighbor"
                if not m.h and neighbor.h:
                    m.h = neighbor.h; m.confidence_h = 0.3; m.source_h = "neighbor"
                if m.is_complete:
                    break

        # Range split copy: models like "FST 32-250/55-75" -> try "FST 32-250/55" or "/75"
        for m in all_models:
            if m.is_complete:
                continue
            # Match "PREFIX/XX-YY" or "PREFIX/XXD-YYD" range patterns
            range_match = re.match(r'^(.+/)([\d.]+)([A-Z]?)\s*[-]\s*([\d.]+)([A-Z]?)$', m.model, re.I)
            if not range_match:
                continue
            prefix = range_match.group(1)  # e.g. "FST 32-250/"
            first_num = range_match.group(2)  # "55"
            first_sfx = range_match.group(3)  # "" or "D"
            second_num = range_match.group(4)  # "75"
            second_sfx = range_match.group(5)  # "" or "D"
            for try_name in [f"{prefix}{first_num}{first_sfx}", f"{prefix}{second_num}{second_sfx}"]:
                try_key = re.sub(r'[\s()]+', '', try_name.upper().replace(",", "."))
                donor = model_map.get(try_key)
                if donor and donor.is_complete:
                    if not m.h and donor.h:
                        m.h = donor.h; m.confidence_h = 0.3; m.source_h = "range_split"
                    if not m.q and donor.q:
                        m.q = donor.q; m.confidence_q = 0.3; m.source_q = "range_split"
                    if not m.kw and donor.kw:
                        m.kw = donor.kw; m.confidence_kw = 0.3; m.source_kw = "range_split"
                    break

        # Same-frame copy: for models with same DN-impeller (e.g. FST 40-125/*, FS 40-125/*)
        # Copy H from any complete model with same DN-impeller, across series variants
        # (FST/FS/FS4/FSM share frames)
        def _frame_key(name):
            """Extract DN-impeller key, ignoring series prefix. 'FST 65-200/...' -> '65-200'"""
            fm = re.match(r'^(?:FST4?|FS4?|FSM|[A-Za-z]+)\s*(\d+\s*[-]\s*\d+)', name, re.I)
            return fm.group(1).replace(" ", "") if fm else None

        for m in all_models:
            if m.is_complete or m.h:
                continue
            m_frame = _frame_key(m.model)
            if not m_frame:
                continue
            for donor in all_models:
                if donor is m or not donor.is_complete:
                    continue
                d_frame = _frame_key(donor.model)
                if d_frame == m_frame:
                    if not m.h and donor.h:
                        m.h = donor.h; m.confidence_h = 0.25; m.source_h = "same_frame"
                    if not m.kw and donor.kw:
                        m.kw = donor.kw; m.confidence_kw = 0.25; m.source_kw = "same_frame"
                    if not m.q and donor.q:
                        m.q = donor.q; m.confidence_q = 0.25; m.source_q = "same_frame"
                    break

        # Prefix dedup + fill: "MATRIX 3-2T/0.45M" matches "3-2(.)/0.45M" (strip prefix, T→(.))
        for m in all_models:
            if m.is_complete:
                continue
            name = m.model
            # Strip known series prefix (MATRIX, CDX, DW etc.)
            short = re.sub(r'^(?:MATRIX|CDXM?|DWF?|DW\s*VOX)\s*', '', name).strip()
            # Try T→(.) substitution
            variants = [short, short.replace("T/", "(.)/")]
            for v in variants:
                v_key = re.sub(r'[\s()]+', '', v.upper().replace(",", "."))
                donor = model_map.get(v_key)
                if donor and donor.is_complete:
                    if not m.q and donor.q:
                        m.q = donor.q; m.confidence_q = 0.35; m.source_q = "prefix_dedup"
                    if not m.h and donor.h:
                        m.h = donor.h; m.confidence_h = 0.35; m.source_h = "prefix_dedup"
                    break

        # Suffix dedup: remove short-name dupes if full-name version exists
        # e.g. "125-5A/3" is a dupe of "LLT(S)125-5A/3"
        keys_set = {m.key for m in all_models if m.is_complete}
        all_models = [m for m in all_models if m.is_complete or
                      not any(k.endswith(m.key) and k != m.key for k in keys_set)]

        # Filter overview models: "CV 1", "CV 2" etc. without stages are not pump models
        # (they come from pressure/spec tables, not performance tables)
        # Filter overview models without specific pump identifiers
        # Keep models with separators (-/), with Q data, or with both H+kW (likely valid pump)
        all_models = [m for m in all_models if
                      "-" in m.model or "/" in m.model or
                      m.q > 0 or
                      (m.h > 0 and m.kw > 0)]

        return all_models

    def _extract_header_row(self, model_col, h_col, kw_col, q_col, hp_col, page, all_models, seen_keys):
        """Extract first model embedded in column headers.
        Docling sometimes puts the first row data into column names:
        'Модель.Модель.TD32-14G/2' contains model TD32-14G/2,
        'H [м].H [м].14' contains H=14.
        """
        from config import PUMP_MODEL_RE
        # Extract model name from model_col header
        parts = str(model_col).split(".")
        model_name = None
        for part in reversed(parts):
            p = part.strip()
            if p and re.match(PUMP_MODEL_RE, p, re.I):
                model_name = p
                break
        if not model_name:
            return

        # Extract H from h_col header
        h_parts = str(h_col).split(".")
        h_val = None
        for part in reversed(h_parts):
            v = parse_number(part.strip())
            if v and v > 0:
                h_val = v
                break

        # Extract kW from kw_col header
        kw_val = None
        if kw_col:
            kw_parts = str(kw_col).split(".")
            for part in reversed(kw_parts):
                v = parse_number(part.strip())
                if v and v > 0:
                    kw_val = v
                    break

        # Extract Q from q_col header
        q_val = None
        if q_col:
            q_parts = str(q_col).split(".")
            for part in reversed(q_parts):
                v = parse_number(part.strip())
                if v and v > 0:
                    q_val = v
                    break

        # Extract hp fallback
        if not kw_val and hp_col:
            hp_parts = str(hp_col).split(".")
            for part in reversed(hp_parts):
                v = parse_number(part.strip())
                if v and v > 0:
                    kw_val = round(v * 0.7457, 2)
                    break

        pm = self._build_model(model_name, q_val, h_val, kw_val, None, page)
        if pm and pm.key and pm.key not in seen_keys:
            seen_keys.add(pm.key)
            all_models.append(pm)

    def _scan_row_for_h(self, row, cols, skip_cols, qh_grid=False):
        """Scan all H-labelled columns in row for first valid numeric value.
        Used when primary H column is empty/dash for Q-H performance tables.
        If qh_grid=True, also scans Q-point columns (numeric names like '26 100 6')."""
        H_COL_KW = ["head", "h=", "hauteur", "altura", "напор", "метрах", "метров", "столба"]
        # Pass 1: scan H-keyword columns
        for col in cols:
            if col in skip_cols:
                continue
            cl = str(col).lower()
            if not any(k in cl for k in H_COL_KW):
                continue
            val = parse_number(row.get(col))
            if val and val > 0:
                return val
        # Pass 2 (Q-H grid only): scan Q-point columns with numeric/flow-unit names
        # These are columns like "26 100 6", "40 150.9", "GPM 0.l/min0 m/h 0."
        # whose names consist of numbers, spaces, dots, flow-unit abbreviations
        if qh_grid:
            _FLOW_UNITS = {"gpm", "l", "min", "m", "h"}
            for col in cols:
                if col in skip_cols:
                    continue
                cl = str(col).lower().strip()
                # Skip columns that are clearly NOT Q-point data
                # Q-point cols: mostly digits+spaces+dots+slashes, or contain flow-unit words
                # Remove flow-unit words and punctuation, check if mostly numeric
                if len(cl) < 3:  # skip single-char cols (a, f, h1, h2)
                    continue
                stripped = cl
                for u in _FLOW_UNITS:
                    stripped = stripped.replace(u, "")
                stripped = re.sub(r'[/\s.×,]', '', stripped)
                if stripped and all(c.isdigit() for c in stripped):
                    val = parse_number(row.get(col))
                    if val and 0.5 < val < 300:  # reasonable H range for pumps
                        return val
        return None

    @staticmethod
    def _extract_q_from_qh_grid(cols, skip_cols):
        """Extract nominal Q (m³/h) from Q-H grid column headers.
        Columns like 'Q=Capacity.100.12.H=Total...' contain Q in l/min and m³/h.
        Returns the first non-zero Q in m³/h, or None.
        """
        for col in cols:
            if col in skip_cols:
                continue
            cl = str(col).lower().strip()
            # Look for m³/h value in column header
            m = re.search(r'm.?/h[)\s.]*(\d+[.,]?\d*)', cl)
            if m:
                val = float(m.group(1).replace(",", "."))
                if val > 0:
                    return val
            # Look for l/min value and convert
            m = re.search(r'l/min[)\s.]*(\d+[.,]?\d*)', cl)
            if m:
                val = float(m.group(1).replace(",", "."))
                if val > 0:
                    return round(val / 16.67, 1)  # l/min → m³/h
            # Numeric-only column name with Q keyword nearby
            m = re.search(r'(?:q|flow|delivery|capacity)[^0-9]*(\d+[.,]?\d*)', cl)
            if m:
                val = float(m.group(1).replace(",", "."))
                if 0 < val < 500:
                    return val
        # Second pass: parse numeric column headers like "20.1.2" (l/min.m³/h format)
        # or "50.3" where second number is m³/h
        for col in cols:
            if col in skip_cols:
                continue
            cl = str(col).strip()
            # Pattern: "digits.digits" possibly with ".H=..." suffix
            m = re.match(r'^(\d+)[.](\d+[.]?\d*)[.]?(?:H=|$)', cl)
            if m:
                lpm = float(m.group(1))
                m3h = float(m.group(2))
                if 0.1 < m3h < 200 and lpm > m3h:  # l/min > m³/h sanity check
                    return m3h
        return None

    def _extract_kw_from_cell(self, cell_value):
        """Extract kW from combined cell like 'kw 2.2' or '2.2'."""
        if not cell_value:
            return None
        s = str(cell_value).lower().strip()
        # Pattern: "kw 2.2" or "квт 2.2"
        m = re.search(r'(?:kw|квт)\s+([\d.,]+)', s)
        if m:
            return parse_number(m.group(1))
        return parse_number(cell_value)

    def _strategy1(self, rows, model_col, q_col, h_col, kw_col, rpm_col, page, all_models, seen_keys, h_confidence=0.6, hp_col=None, qh_grid=False):
        """Strategy 1: direct column mapping."""
        cols = list(rows[0].keys()) if rows else []
        skip_cols = {model_col, q_col, kw_col, rpm_col, hp_col}

        for row in rows:
            model_name = str(row.get(model_col, "")).strip()
            if not model_name or len(model_name) < 3:
                continue

            q = parse_number(row.get(q_col)) if q_col else None
            h = parse_number(row.get(h_col)) if h_col else None
            kw = self._extract_kw_from_cell(row.get(kw_col)) if kw_col else None
            rpm_val = parse_number(row.get(rpm_col)) if rpm_col else None

            # Fallback: if Q empty in Q-H grid, try extracting from column headers
            if not q and qh_grid:
                q = self._extract_q_from_qh_grid(cols, skip_cols)

            # Fallback: if kW empty, try hp column (convert hp → kW)
            if not kw and hp_col:
                hp_val = parse_number(row.get(hp_col))
                if hp_val and hp_val > 0:
                    kw = round(hp_val * 0.7457, 2)

            # Fallback: if H empty, scan other H-labelled columns (Q-H perf tables)
            if not h and cols:
                h_fallback = self._scan_row_for_h(row, cols, skip_cols, qh_grid=qh_grid)
                if h_fallback:
                    h = h_fallback

            # Fallback: extract kW from h_col if it contains "kw X.X" pattern
            if not kw and h_col:
                kw_from_h = self._extract_kw_from_cell(row.get(h_col))
                if kw_from_h:
                    kw = kw_from_h

            pm = self._build_model(model_name, q, h, kw, rpm_val, page)
            if pm and pm.h and h_confidence != 0.6:
                pm.confidence_h = h_confidence  # Lower confidence for H(мм) columns
            if pm and pm.key:
                if pm.key not in seen_keys:
                    seen_keys.add(pm.key)
                    all_models.append(pm)
                else:
                    # Update existing: fill zeros OR replace low-confidence with higher
                    for i, existing in enumerate(all_models):
                        if existing.key == pm.key:
                            if pm.h and (not existing.h or pm.confidence_h > existing.confidence_h):
                                existing.h = pm.h
                                existing.confidence_h = pm.confidence_h
                                existing.source_h = pm.source_h
                            if pm.q and (not existing.q or pm.confidence_q > existing.confidence_q):
                                existing.q = pm.q
                                existing.confidence_q = pm.confidence_q
                            if pm.kw and (not existing.kw or pm.confidence_kw > existing.confidence_kw):
                                existing.kw = pm.kw
                                existing.confidence_kw = pm.confidence_kw
                            break

    def _strategy2(self, rows, cols, header_models, page, all_models, seen_keys):
        """Strategy 2: CDM-style — models in headers, params in rows."""
        LABEL_Q = ["подача", "расход", "flow", "q,", "q ", "м³/ч", "qном"]
        LABEL_H = ["напор", "head", "h,", "h ", "давление", "нном", "hном"]
        LABEL_KW = ["мощность", "power", "квт", "kw", "p2", "р2"]
        LABEL_RPM = ["об/мин", "rpm", "частота"]

        # Find label column (first non-model column)
        label_col = None
        for c in cols:
            if c not in header_models:
                label_col = c
                break
        if not label_col:
            return

        # Build spec map: model_col -> {q, h, kw, rpm}
        spec_map = {mc: {"q": 0, "h": 0, "kw": 0, "rpm": 2900} for mc in header_models}

        for row in rows:
            label = str(row.get(label_col, "")).lower().strip()
            for mc in header_models:
                raw = row.get(mc)
                val = parse_number(raw if raw is not None else "")
                if val is None:
                    continue
                if any(k in label for k in LABEL_Q):
                    spec_map[mc]["q"] = val
                elif any(k in label for k in LABEL_H):
                    spec_map[mc]["h"] = val
                elif any(k in label for k in LABEL_KW):
                    spec_map[mc]["kw"] = val
                elif any(k in label for k in LABEL_RPM):
                    spec_map[mc]["rpm"] = int(val)

        for mc, specs in spec_map.items():
            pm = self._build_model(mc.strip(), specs["q"], specs["h"], specs["kw"], specs["rpm"], page)
            if pm.key and pm.key not in seen_keys:
                seen_keys.add(pm.key)
                all_models.append(pm)

    # ── Helpers ─────────────────────────────────────────────────────

    def _build_model(self, model_name, q, h, kw, rpm_val, page):
        """Build PumpModelResult with enrichment and validation."""
        import re as _re
        # Clean: strip phase prefixes (universal — any catalog may have them)
        name_clean = model_name.strip()
        name_clean = _re.sub(r'^(трёхфазный|трехфазный|однофазный|3-phase|1-phase|three.?phase|single.?phase)\s*', '', name_clean, flags=_re.I).strip()
        # Split merged names: "PVn4-5 PVn4-6" or "МВL 32-200-4/2 МВL 32-200-5.5/2"
        # Detect: if name contains 2+ pump-like patterns separated by space
        _pump_parts = _re.findall(r'[A-ZА-Яa-zа-я]{2,5}\s*\d+[-/]\d+\S*', name_clean)
        if len(_pump_parts) >= 2:
            name_clean = _pump_parts[0].strip()
        # Filter garbage
        if not name_clean or len(name_clean) < 3 or len(name_clean) > 50 or name_clean.endswith(" -"):
            return None
        if name_clean.replace("-", "").replace(".", "").replace(",", "").replace(" ", "").isdigit():
            return None
        # Filter motor-only models (YE3, Y2, Y3 etc.) — not pump models
        if _re.match(r'^Y[E2-9]', name_clean, _re.I):
            return None
        series = detect_series(name_clean)
        model_name = name_clean  # use cleaned name
        pm = PumpModelResult(
            model=model_name,
            series=series,
            q=q or 0.0,
            h=h or 0.0,
            kw=kw or 0.0,
            rpm=int(rpm_val) if rpm_val else 2900,
            page_number=page,
            confidence_q=0.6 if q else 0.0,
            confidence_h=0.6 if h else 0.0,
            confidence_kw=0.6 if kw else 0.0,
            source_q="docling" if q else "",
            source_h="docling" if h else "",
            source_kw="docling" if kw else "",
        )

        # Enrich from model name
        d = {"model": pm.model, "q_nom": pm.q, "h_nom": pm.h, "power_kw": pm.kw, "rpm": pm.rpm}
        enrich_from_model_name(d)
        validate_pump_physics(d)

        if d["q_nom"] and not pm.q:
            pm.q = d["q_nom"]
            pm.source_q = "enrichment"
            pm.confidence_q = 0.5
        if d["h_nom"] and not pm.h:
            pm.h = d["h_nom"]
            pm.source_h = "enrichment"
            pm.confidence_h = 0.5
        if d["power_kw"] and not pm.kw:
            pm.kw = d["power_kw"]
            pm.source_kw = "enrichment"
            pm.confidence_kw = 0.5
        if d["rpm"]:
            pm.rpm = d["rpm"]

        return pm
