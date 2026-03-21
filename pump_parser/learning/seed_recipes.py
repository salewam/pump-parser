"""Seed recipes — convert knowledge of 17+ ONIS/CNP catalog formats to recipes.

Each recipe captures the parsing strategy that works for a specific catalog series.
Run generate_seed_recipes() to create all seed recipe JSON files.
"""

import logging
from datetime import datetime

from pump_parser.learning.recipe import (
    Recipe, MatchingConfig, ExtractionConfig, ValidationConfig,
)
from pump_parser.learning.recipe_store import RecipeStore

log = logging.getLogger("pump_parser.learning.seed_recipes")


def _make(
    rid: str,
    name: str,
    desc: str,
    fn_patterns: list[str],
    mfr_kw: list[str],
    series_kw: list[str],
    signatures: list[str],
    extractor: str,
    q_range: tuple,
    h_range: tuple,
    p_range: tuple = (0.0, 1000.0),
    rpm: list[int] | None = None,
    rpm_fixed: int = 0,
    column_map: dict | None = None,
) -> Recipe:
    return Recipe(
        recipe_id=rid,
        name=name,
        manufacturer="CNP",
        description=desc,
        matching=MatchingConfig(
            filename_patterns=fn_patterns,
            manufacturer_keywords=mfr_kw,
            series_keywords=series_kw,
            page_signatures=signatures,
            language="ru",
        ),
        extraction=ExtractionConfig(
            extractor_type=extractor,
            rpm_fixed=rpm_fixed,
            column_map=column_map or {},
        ),
        validation=ValidationConfig(
            q_range=q_range,
            h_range=h_range,
            p_range=p_range,
            rpm_expected=rpm or [],
        ),
        confidence=0.90,
        uses_count=0,
        success_count=0,
        created=datetime.utcnow().isoformat(),
        auto_generated=False,
    )


SEED_RECIPES = [
    # 1. CMI — flat table (Model, Article, Q, H, P)
    _make(
        "onis_cmi", "CMI", "CMI vertical multistage (flat table)",
        ["*CMI*", "*cmi*"],
        ["CNP", "Fancy"], ["CMI"],
        ["Qном", "Hном", "P2"],
        "flat_table",
        q_range=(0.5, 50.0), h_range=(5.0, 300.0), p_range=(0.1, 15.0),
        rpm=[2900],
    ),

    # 2. NBS — flat table
    _make(
        "onis_nbs", "NBS", "NBS booster (flat table)",
        ["*NBS*", "*nbs*"],
        ["CNP", "Fancy"], ["NBS"],
        ["Qном", "Hном"],
        "flat_table",
        q_range=(0.5, 100.0), h_range=(5.0, 400.0), p_range=(0.1, 30.0),
        rpm=[2900],
    ),

    # 3. FVH — Q-H matrix
    _make(
        "onis_fvh", "FVH", "FVH horizontal (Q-H matrix)",
        ["*fvh*", "*FVH*"],
        ["CNP", "Fancy"], ["FVH"],
        ["Q (м³/ч)"],
        "qh_matrix",
        q_range=(0.5, 200.0), h_range=(2.0, 200.0), p_range=(0.1, 45.0),
        rpm=[2900],
    ),

    # 4. FV — Q-H matrix
    _make(
        "onis_fv", "FV", "FV vertical inline (Q-H matrix)",
        ["*serii_fv*", "*FV*"],
        ["CNP", "Fancy"], ["FV"],
        ["Q (м³/ч)"],
        "qh_matrix",
        q_range=(0.5, 200.0), h_range=(2.0, 200.0), p_range=(0.1, 45.0),
        rpm=[2900],
    ),

    # 5. FST/FS/FS4/FSM — Q-H matrix
    _make(
        "onis_fst", "FST", "FST/FS cast iron console (Q-H matrix)",
        ["*fst*", "*FST*", "*fs4*"],
        ["CNP", "Fancy"], ["FST", "FS", "FS4", "FSM"],
        ["Q (м³/ч)", "l/min"],
        "qh_matrix",
        q_range=(1.0, 500.0), h_range=(2.0, 100.0), p_range=(0.3, 55.0),
        rpm=[1450, 2900],
    ),

    # 6. CDLF — curve table (shared Q row + model/H blocks)
    _make(
        "onis_cdlf", "CDLF", "CDLF vertical multistage (curve table)",
        ["*cdlf*", "*CDLF*"],
        ["CNP", "Fancy"], ["CDLF"],
        ["Q (м³/ч)"],
        "curve_table",
        q_range=(0.5, 200.0), h_range=(5.0, 500.0), p_range=(0.1, 75.0),
        rpm=[2900],
    ),

    # 7. CDLF large (120-150-200)
    _make(
        "onis_cdlf_large", "CDLF Large", "CDLF large models 120-200 (curve table)",
        ["*cdlf*120*", "*cdlf*150*", "*cdlf*200*"],
        ["CNP", "Fancy"], ["CDLF"],
        ["Q (м³/ч)"],
        "curve_table",
        q_range=(50.0, 500.0), h_range=(10.0, 500.0), p_range=(5.0, 200.0),
        rpm=[1450],
    ),

    # 8. CDMF — curve table
    _make(
        "onis_cdmf", "CDMF", "CDMF vertical multistage (curve table)",
        ["*cdmf*", "*CDMF*"],
        ["CNP", "Fancy"], ["CDMF"],
        ["Q (м³/ч)"],
        "curve_table",
        q_range=(0.5, 200.0), h_range=(5.0, 500.0), p_range=(0.1, 75.0),
        rpm=[2900],
    ),

    # 9. CDMF large (32-42-65-85)
    _make(
        "onis_cdmf_large", "CDMF Large", "CDMF large models 32-85 (curve table)",
        ["*cdmf*32*", "*cdmf*42*", "*cdmf*65*", "*cdmf*85*"],
        ["CNP", "Fancy"], ["CDMF"],
        ["Q (м³/ч)"],
        "curve_table",
        q_range=(10.0, 500.0), h_range=(10.0, 500.0), p_range=(3.0, 200.0),
        rpm=[1450, 2900],
    ),

    # 10. CDM — flat table + curve table mix
    _make(
        "onis_cdm", "CDM", "CDM/CDMF combined catalog (flat + curve)",
        ["*CDM*CDMF*", "*cdm_cdmf*"],
        ["CNP", "Fancy"], ["CDM", "CDMF"],
        ["Qном", "Q (м³/ч)"],
        "flat_table",
        q_range=(0.5, 500.0), h_range=(5.0, 500.0), p_range=(0.1, 200.0),
        rpm=[1450, 2900],
    ),

    # 11. PV — curve table (shared Q row)
    _make(
        "onis_pv", "PV", "PV vertical multistage (curve table)",
        ["*PV*", "*pv*"],
        ["CNP", "Fancy"], ["PV"],
        ["Q (м³/ч)"],
        "curve_table",
        q_range=(0.5, 50.0), h_range=(5.0, 300.0), p_range=(0.1, 15.0),
        rpm=[2900],
    ),

    # 12. INL — flat table
    _make(
        "onis_inl", "INL", "INL inline pumps (flat table)",
        ["*INL*", "*inl*"],
        ["CNP"], ["INL"],
        ["Qном", "Hном"],
        "flat_table",
        q_range=(1.0, 1000.0), h_range=(2.0, 100.0), p_range=(0.3, 90.0),
        rpm=[1450, 2900],
    ),

    # 13. CV/CVF — flat table
    _make(
        "onis_cv", "CV", "CV/CVF vertical inline (flat table)",
        ["*CV*", "*cv*", "*CVF*"],
        ["CNP"], ["CV", "CVF"],
        ["Qном", "Hном"],
        "flat_table",
        q_range=(1.0, 500.0), h_range=(2.0, 100.0), p_range=(0.3, 90.0),
        rpm=[1450, 2900],
    ),

    # 14. TG/TL/TD — transposed (model range) + flat table (spec pages)
    _make(
        "onis_tg", "TG/TL/TD", "TG/TL/TD inline pumps (transposed + flat)",
        ["*TG*TL*TD*", "*Katalog-TG*"],
        ["CNP"], ["TG", "TL", "TD"],
        ["Номинальный расход", "Номинальный напор"],
        "transposed",
        q_range=(3.0, 1000.0), h_range=(2.0, 100.0), p_range=(0.3, 200.0),
        rpm=[1450, 2900],
    ),

    # 15. TD/LLT/TD(i) — list parser (model list pages) + curve table
    _make(
        "onis_td_llt", "TD/LLT", "TD/LLT/TD(i) inline (list + curve table)",
        ["*TD_LLT*", "*td_llt*", "*TD(i)*"],
        ["CNP"], ["TD", "LLT", "TD(I)"],
        ["Q [м³/ч]", "H [м]", "Р2 [кВт]"],
        "list_parser",
        q_range=(3.0, 1000.0), h_range=(2.0, 100.0), p_range=(0.2, 200.0),
        rpm=[1450, 2900],
    ),

    # 16. BM — transposed spec table
    _make(
        "onis_bm", "BM", "BM booster (transposed spec table)",
        ["*bm*", "*BM*"],
        ["CNP"], ["BM", "BMN"],
        ["Номинальная подача", "Максимальный напор"],
        "transposed",
        q_range=(0.5, 50.0), h_range=(5.0, 200.0), p_range=(0.1, 15.0),
        rpm=[2900],
    ),

    # 17. EVR/EVS — flat table (manual format)
    _make(
        "onis_evr", "EVR/EVS", "EVR/EVS submersible (flat table)",
        ["*evr*", "*evs*", "*EVR*", "*EVS*"],
        ["CNP"], ["EVR", "EVS"],
        [],
        "flat_table",
        q_range=(0.5, 200.0), h_range=(5.0, 500.0), p_range=(0.1, 75.0),
        rpm=[2900],
    ),
]


def generate_seed_recipes(store: RecipeStore | None = None) -> list[Recipe]:
    """Save all seed recipes to store. Returns list of recipes."""
    if store is None:
        store = RecipeStore()

    saved = []
    for recipe in SEED_RECIPES:
        store.save(recipe)
        saved.append(recipe)
        log.info("Seed recipe: %s (%s)", recipe.name, recipe.recipe_id)

    log.info("Generated %d seed recipes", len(saved))
    return saved


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    recipes = generate_seed_recipes()
    print(f"Created {len(recipes)} seed recipes in data/recipes/")
    for r in recipes:
        print(f"  {r.recipe_id}: {r.name} ({r.extraction.extractor_type})")
