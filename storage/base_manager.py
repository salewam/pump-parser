"""
BASE file CRUD + brands_index rebuild.
Manages /root/pump_base/{SERIES}_BASE.json files.
"""
import json
import os
import shutil


class BaseManager:
    """Manages pump BASE files and brands_index.json."""

    def __init__(self, base_dir=None):
        if base_dir is None:
            from config import BASE_DIR
            base_dir = BASE_DIR
        self._dir = base_dir
        os.makedirs(self._dir, exist_ok=True)

    def save(self, series, models_list):
        """Save models to {SERIES}_BASE.json. Merges with existing (new override by ID)."""
        series = series.upper().strip()
        fpath = os.path.join(self._dir, f"{series}_BASE.json")

        # Backup before overwrite
        if os.path.exists(fpath):
            shutil.copy2(fpath, fpath + ".bak")

        # Load existing
        existing = {}
        if os.path.exists(fpath):
            try:
                with open(fpath) as f:
                    for m in json.load(f):
                        key = self._model_key(m.get("id", ""))
                        if key:
                            existing[key] = m
            except Exception:
                pass

        # Merge: new models override existing by key
        for m in models_list:
            key = self._model_key(m.get("id", ""))
            if key:
                existing[key] = m

        merged = sorted(existing.values(), key=lambda x: (x.get("series", ""), x.get("q", 0)))

        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)

        return len(merged)

    def load(self, series):
        """Load models from {SERIES}_BASE.json. Returns list of dicts."""
        series = series.upper().strip()
        fpath = os.path.join(self._dir, f"{series}_BASE.json")
        if not os.path.exists(fpath):
            return []
        try:
            with open(fpath) as f:
                return json.load(f)
        except Exception:
            return []

    def delete(self, series):
        """Delete a BASE file."""
        fpath = os.path.join(self._dir, f"{series.upper()}_BASE.json")
        if os.path.exists(fpath):
            os.remove(fpath)
            return True
        return False

    def list_bases(self):
        """List all BASE files with stats."""
        result = []
        for fname in sorted(os.listdir(self._dir)):
            if not fname.endswith("_BASE.json"):
                continue
            fpath = os.path.join(self._dir, fname)
            series = fname.replace("_BASE.json", "")
            try:
                with open(fpath) as f:
                    data = json.load(f)
                brand = data[0].get("brand", "Unknown") if data else "Unknown"
                flagship = data[0].get("flagship", False) if data else False
                result.append({
                    "name": series,
                    "count": len(data),
                    "brand": brand,
                    "flagship": flagship,
                    "file_size": os.path.getsize(fpath),
                })
            except Exception:
                pass
        return result

    def get_stats(self):
        """Overall stats: total models, brands, completeness."""
        bases = self.list_bases()
        total_models = sum(b["count"] for b in bases)
        brands = {}
        for b in bases:
            br = b["brand"]
            if br not in brands:
                brands[br] = {"series": 0, "models": 0}
            brands[br]["series"] += 1
            brands[br]["models"] += b["count"]

        # Completeness: count models with q>0 and head_m>0 and kw>0
        complete = 0
        for b in bases:
            models = self.load(b["name"])
            complete += sum(
                1 for m in models
                if m.get("q", 0) > 0 and m.get("head_m", 0) > 0 and m.get("kw", 0) > 0
            )

        return {
            "total_models": total_models,
            "total_bases": len(bases),
            "brands": brands,
            "complete_models": complete,
            "complete_pct": round(complete / total_models * 100, 1) if total_models else 0,
        }

    def rebuild_index(self):
        """Rebuild brands_index.json from all BASE files."""
        index = {}
        for fname in sorted(os.listdir(self._dir)):
            if not fname.endswith("_BASE.json"):
                continue
            fpath = os.path.join(self._dir, fname)
            try:
                with open(fpath) as f:
                    data = json.load(f)
                if not data:
                    continue
                series = fname.replace("_BASE.json", "")
                brand = data[0].get("brand", "Unknown")
                flagship = data[0].get("flagship", False)
                if brand not in index:
                    index[brand] = {"series": []}
                index[brand]["series"].append({
                    "name": series,
                    "count": len(data),
                    "flagship": flagship,
                })
            except Exception:
                pass

        # Sort: flagships first, then by count
        for brand in index:
            index[brand]["series"].sort(
                key=lambda s: (-int(s.get("flagship", False)), -s["count"])
            )

        index_path = os.path.join(self._dir, "brands_index.json")
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)

        return index

    @staticmethod
    def _model_key(model_id):
        """Normalize model ID for dedup."""
        if not model_id:
            return ""
        k = model_id.strip().upper()
        k = k.replace("\u041c", "M").replace("\u0412", "B")
        k = k.replace("\u200b", "").replace(" ", "")
        return k
