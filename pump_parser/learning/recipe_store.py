"""Recipe store — JSON file-based CRUD for recipes."""

import json
import logging
from pathlib import Path

from pump_parser.learning.recipe import Recipe
from pump_parser.config import RECIPE_DIR

log = logging.getLogger("pump_parser.learning.recipe_store")


class RecipeStore:
    """File-based recipe storage. One JSON per recipe in recipe_dir."""

    def __init__(self, recipe_dir: str | Path | None = None):
        self.recipe_dir = Path(recipe_dir) if recipe_dir else RECIPE_DIR
        self.recipe_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, recipe_id: str) -> Path:
        return self.recipe_dir / f"{recipe_id}.json"

    def save(self, recipe: Recipe) -> str:
        """Save recipe to disk. Returns path."""
        if not recipe.recipe_id:
            recipe.recipe_id = Recipe.generate_id(recipe.manufacturer, recipe.name)
        path = self._path(recipe.recipe_id)
        recipe.save(str(path))
        log.info("Saved recipe %s (%s) → %s", recipe.recipe_id, recipe.name, path)
        return str(path)

    def load(self, recipe_id: str) -> Recipe | None:
        """Load recipe by ID. Returns None if not found."""
        path = self._path(recipe_id)
        if not path.exists():
            return None
        try:
            return Recipe.from_file(str(path))
        except Exception as e:
            log.error("Failed to load recipe %s: %s", recipe_id, e)
            return None

    def all(self) -> list[Recipe]:
        """Load all recipes from disk."""
        recipes = []
        for path in sorted(self.recipe_dir.glob("*.json")):
            try:
                recipes.append(Recipe.from_file(str(path)))
            except Exception as e:
                log.warning("Skipping bad recipe %s: %s", path.name, e)
        return recipes

    def delete(self, recipe_id: str) -> bool:
        """Delete recipe. Returns True if deleted."""
        path = self._path(recipe_id)
        if path.exists():
            path.unlink()
            log.info("Deleted recipe %s", recipe_id)
            return True
        return False

    def update(self, recipe: Recipe) -> str:
        """Update existing recipe (save overwrites)."""
        return self.save(recipe)

    def exists(self, recipe_id: str) -> bool:
        return self._path(recipe_id).exists()

    def count(self) -> int:
        return len(list(self.recipe_dir.glob("*.json")))
