"""Vision AI API wrapper — Gemini-based, rate-limited, budget-capped.

Provides unified interface for:
- Page classification
- Data extraction from images
- Graph/curve reading

Uses google-generativeai SDK with Gemini 2.5 Flash Lite (primary)
and Gemini 2.5 Flash (fallback).
"""

import base64
import json
import time
import logging
from dataclasses import dataclass, field
from typing import Any

from pump_parser.config import VisionConfig, VISION

log = logging.getLogger("pump_parser.vision.api")

# Lazy import — google-generativeai may not be installed
_genai = None


def _ensure_genai():
    global _genai
    if _genai is None:
        try:
            import google.generativeai as genai
            _genai = genai
        except ImportError:
            raise ImportError(
                "google-generativeai not installed. "
                "Install with: pip install google-generativeai"
            )
    return _genai


@dataclass
class BudgetTracker:
    """Track Vision AI spending per PDF."""
    max_cost_usd: float = 0.50
    spent_usd: float = 0.0
    calls: int = 0

    def can_spend(self, cost: float) -> bool:
        return self.spent_usd + cost <= self.max_cost_usd

    def record(self, cost: float) -> None:
        self.spent_usd += cost
        self.calls += 1

    @property
    def remaining(self) -> float:
        return max(0.0, self.max_cost_usd - self.spent_usd)


class VisionAPI:
    """Unified Vision AI interface with rate limiting and budget control."""

    def __init__(self, config: VisionConfig | None = None, api_key: str | None = None):
        self.config = config or VISION
        self.api_key = api_key
        self.budget = BudgetTracker(max_cost_usd=self.config.max_cost_per_pdf_usd)
        self._last_call_time = 0.0
        self._model = None
        self._fallback_model = None

    def _get_model(self, fallback: bool = False):
        """Lazy-init Gemini model."""
        genai = _ensure_genai()
        if self.api_key:
            genai.configure(api_key=self.api_key)

        model_name = self.config.fallback_model if fallback else self.config.model
        return genai.GenerativeModel(model_name)

    def _rate_limit(self) -> None:
        """Enforce rate limiting between calls."""
        now = time.time()
        elapsed = now - self._last_call_time
        if elapsed < self.config.rate_limit_seconds:
            time.sleep(self.config.rate_limit_seconds - elapsed)
        self._last_call_time = time.time()

    def call(
        self,
        prompt: str,
        image_bytes: bytes | None = None,
        images: list[bytes] | None = None,
        parse_json: bool = True,
        max_retries: int = 3,
    ) -> dict | str | None:
        """Send prompt (+optional image) to Vision AI.

        Args:
            prompt: text prompt
            image_bytes: single image (PNG bytes)
            images: list of images for batch
            parse_json: try to parse response as JSON
            max_retries: retry count with exponential backoff

        Returns:
            Parsed JSON dict, or raw text string, or None on failure.
        """
        # Budget check
        cost = self.config.cost_per_page_primary
        if not self.budget.can_spend(cost):
            log.warning("Budget exhausted (spent $%.3f / $%.2f)",
                        self.budget.spent_usd, self.budget.max_cost_usd)
            return None

        genai = _ensure_genai()

        # Build content parts
        parts = []
        if image_bytes:
            parts.append({
                "mime_type": "image/png",
                "data": base64.b64encode(image_bytes).decode(),
            })
        if images:
            for img in images:
                parts.append({
                    "mime_type": "image/png",
                    "data": base64.b64encode(img).decode(),
                })
        parts.append(prompt)

        # Retry loop
        for attempt in range(max_retries):
            fallback = attempt > 0  # use fallback model on retry
            model = self._get_model(fallback=fallback)

            self._rate_limit()

            try:
                response = model.generate_content(parts)
                text = response.text.strip()
                actual_cost = (self.config.cost_per_page_fallback
                               if fallback else self.config.cost_per_page_primary)
                self.budget.record(actual_cost)

                log.debug("Vision API call #%d: %d chars, $%.4f",
                          self.budget.calls, len(text), actual_cost)

                if parse_json:
                    return _parse_json_response(text)
                return text

            except Exception as e:
                log.warning("Vision API attempt %d/%d failed: %s",
                            attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # exponential backoff

        log.error("Vision API failed after %d attempts", max_retries)
        return None

    def classify_page(self, image_bytes: bytes, prompt: str) -> dict | None:
        """Classify a page image. Returns dict with page_type, confidence."""
        return self.call(prompt, image_bytes=image_bytes, parse_json=True)

    def extract_data(self, image_bytes: bytes, prompt: str) -> dict | None:
        """Extract structured data from a page image."""
        return self.call(prompt, image_bytes=image_bytes, parse_json=True)

    def extract_batch(self, images: list[bytes], prompt: str) -> dict | None:
        """Extract data from multiple images in one call (collage)."""
        if not self.budget.can_spend(self.config.cost_per_page_primary * len(images)):
            return None
        return self.call(prompt, images=images, parse_json=True)

    def reset_budget(self, max_cost: float | None = None) -> None:
        """Reset budget for new PDF."""
        self.budget = BudgetTracker(
            max_cost_usd=max_cost or self.config.max_cost_per_pdf_usd
        )

    @property
    def stats(self) -> dict:
        return {
            "calls": self.budget.calls,
            "spent_usd": round(self.budget.spent_usd, 4),
            "remaining_usd": round(self.budget.remaining, 4),
        }


def _parse_json_response(text: str) -> dict | str:
    """Try to extract JSON from AI response."""
    # Strip markdown code fences
    if "```json" in text:
        start = text.index("```json") + 7
        end = text.index("```", start) if "```" in text[start:] else len(text)
        text = text[start:end].strip()
    elif "```" in text:
        start = text.index("```") + 3
        end = text.index("```", start) if "```" in text[start:] else len(text)
        text = text[start:end].strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object/array in text
        for start_char, end_char in [("{", "}"), ("[", "]")]:
            si = text.find(start_char)
            ei = text.rfind(end_char)
            if si >= 0 and ei > si:
                try:
                    return json.loads(text[si:ei + 1])
                except json.JSONDecodeError:
                    continue

    return text  # return raw text if no JSON found
