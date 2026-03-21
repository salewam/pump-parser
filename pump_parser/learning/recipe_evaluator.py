"""Recipe evaluator — auto-promote/demote recipes based on parse quality.

After each recipe use:
- Success: avg_confidence >= 0.8 AND physics_pass >= 0.8
- Promote: success_rate >= 0.9 → confidence +0.05 (cap 0.99)
- Demote: success_rate < 0.7 → confidence -0.10 (floor 0.10)
- Delete: success_rate < 0.5 OR consecutive_failures >= 3
"""

import logging

from pump_parser.models import ParseResult
from pump_parser.learning.recipe import Recipe
from pump_parser.learning.recipe_store import RecipeStore
from pump_parser.config import (
    RECIPE_DEMOTE_THRESHOLD,
    RECIPE_DELETE_THRESHOLD,
    RECIPE_DELETE_CONSECUTIVE,
)

log = logging.getLogger("pump_parser.learning.recipe_evaluator")

SUCCESS_CONFIDENCE = 0.8
SUCCESS_PHYSICS = 0.8
PROMOTE_RATE = 0.9
PROMOTE_STEP = 0.05
PROMOTE_CAP = 0.99
DEMOTE_STEP = 0.10
DEMOTE_FLOOR = 0.10


class RecipeEvaluator:
    """Evaluate and update recipe quality after each use."""

    def __init__(self, store: RecipeStore):
        self.store = store

    def evaluate(self, recipe: Recipe, result: ParseResult) -> str:
        """Evaluate parse result and update recipe.

        Returns action taken: "promote", "demote", "delete", "ok"
        """
        success = self._is_success(result)
        recipe.record_use(success)

        action = "ok"

        if recipe.consecutive_failures >= RECIPE_DELETE_CONSECUTIVE:
            log.warning(
                "Recipe '%s' — %d consecutive failures, deleting",
                recipe.name, recipe.consecutive_failures,
            )
            self.store.delete(recipe.recipe_id)
            return "delete"

        if recipe.uses_count >= 3 and recipe.success_rate() < RECIPE_DELETE_THRESHOLD:
            log.warning(
                "Recipe '%s' — success rate %.0f%% below delete threshold, deleting",
                recipe.name, recipe.success_rate() * 100,
            )
            self.store.delete(recipe.recipe_id)
            return "delete"

        if recipe.uses_count >= 3 and recipe.success_rate() >= PROMOTE_RATE:
            old_conf = recipe.confidence
            recipe.confidence = min(recipe.confidence + PROMOTE_STEP, PROMOTE_CAP)
            if recipe.confidence > old_conf:
                log.info(
                    "Recipe '%s' promoted: %.2f → %.2f (rate=%.0f%%)",
                    recipe.name, old_conf, recipe.confidence, recipe.success_rate() * 100,
                )
            action = "promote"

        elif recipe.uses_count >= 3 and recipe.success_rate() < RECIPE_DEMOTE_THRESHOLD:
            old_conf = recipe.confidence
            recipe.confidence = max(recipe.confidence - DEMOTE_STEP, DEMOTE_FLOOR)
            log.info(
                "Recipe '%s' demoted: %.2f → %.2f (rate=%.0f%%)",
                recipe.name, old_conf, recipe.confidence, recipe.success_rate() * 100,
            )
            action = "demote"

        self.store.update(recipe)
        return action

    def _is_success(self, result: ParseResult) -> bool:
        """Check if parse result meets success criteria."""
        if len(result.entries) < 1:
            return False
        if result.avg_confidence < SUCCESS_CONFIDENCE:
            return False
        if result.report and result.report.physics_pass_rate < SUCCESS_PHYSICS:
            return False
        return True
