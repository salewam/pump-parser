"""Recipe matcher — match incoming PDFs to stored recipes.

3-tier scoring:
1. Filename patterns (fnmatch) → +100 per match
2. Manufacturer/series keywords in text → +50 per keyword
3. Page signatures (exact text snippets) → +30 per signature

Returns best match if score >= threshold.
"""

import fnmatch
import logging
from pathlib import Path

from pump_parser.learning.recipe import Recipe

log = logging.getLogger("pump_parser.learning.recipe_matcher")

MATCH_THRESHOLD = 50  # minimum score to accept a match


class RecipeMatcher:
    """Match a PDF to the best fitting recipe."""

    def __init__(self, threshold: int = MATCH_THRESHOLD):
        self.threshold = threshold

    def match(
        self,
        pdf_path: str,
        recipes: list[Recipe],
        first_pages_text: str = "",
    ) -> Recipe | None:
        """Find best matching recipe for a PDF.

        Args:
            pdf_path: path to PDF file
            recipes: list of available recipes
            first_pages_text: concatenated text from first 3-5 pages

        Returns:
            Best matching Recipe, or None if no match above threshold.
        """
        if not recipes:
            return None

        filename = Path(pdf_path).name
        text_lower = first_pages_text.lower()

        best_recipe: Recipe | None = None
        best_score = 0

        for recipe in recipes:
            score = self._score_recipe(recipe, filename, text_lower)

            # Boost by recipe confidence (proven recipes rank higher)
            score *= (0.5 + recipe.confidence * 0.5)

            if score > best_score:
                best_score = score
                best_recipe = recipe

        if best_score >= self.threshold and best_recipe is not None:
            log.info(
                "Matched recipe '%s' (id=%s) for %s, score=%.0f",
                best_recipe.name, best_recipe.recipe_id, filename, best_score,
            )
            return best_recipe

        log.debug("No recipe matched for %s (best score=%.0f)", filename, best_score)
        return None

    def score_all(
        self,
        pdf_path: str,
        recipes: list[Recipe],
        first_pages_text: str = "",
    ) -> list[tuple[Recipe, float]]:
        """Score all recipes, return sorted list of (recipe, score)."""
        filename = Path(pdf_path).name
        text_lower = first_pages_text.lower()

        scored = []
        for recipe in recipes:
            score = self._score_recipe(recipe, filename, text_lower)
            score *= (0.5 + recipe.confidence * 0.5)
            scored.append((recipe, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    def _score_recipe(self, recipe: Recipe, filename: str, text_lower: str) -> float:
        """Calculate match score for a recipe against a PDF."""
        score = 0.0
        m = recipe.matching

        # Tier 1: Filename patterns (+100 each)
        for pattern in m.filename_patterns:
            if fnmatch.fnmatch(filename.lower(), pattern.lower()):
                score += 100

        # Tier 2: Manufacturer keywords (+50 each)
        for kw in m.manufacturer_keywords:
            if kw.lower() in text_lower:
                score += 50

        # Tier 2b: Series keywords (+50 each)
        for kw in m.series_keywords:
            if kw.lower() in text_lower:
                score += 50

        # Tier 3: Page signatures (+30 each)
        for sig in m.page_signatures:
            if sig.lower() in text_lower:
                score += 30

        # Bonus: producer match (+20)
        if m.producer_pattern and m.producer_pattern.lower() in text_lower:
            score += 20

        # Penalty: consecutive failures reduce score
        if recipe.consecutive_failures >= 3:
            score *= 0.3
        elif recipe.consecutive_failures >= 2:
            score *= 0.6

        return score
