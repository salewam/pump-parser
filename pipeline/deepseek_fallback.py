"""
DeepSeek API fallback for column classification and H extraction.
Used when Docling can't determine column roles or fill H values.
No vision needed — works with text data from Docling tables.
"""
import os
import json
import logging
import requests

logger = logging.getLogger(__name__)

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"


def classify_columns(columns, sample_rows, timeout=30):
    """Ask DeepSeek to identify column roles from table headers + sample data.
    Returns {"model": "col_name", "q": "col_name", "h": "col_name", "kw": "col_name"} or {}.
    """
    if not DEEPSEEK_API_KEY:
        return {}

    cols_str = ", ".join(str(c) for c in columns)
    rows_str = "\n".join(
        " | ".join(f"{k}: {v}" for k, v in row.items())
        for row in sample_rows[:3]
    )

    prompt = (
        f"Таблица насосов. Колонки: {cols_str}\n"
        f"Данные:\n{rows_str}\n\n"
        f"Определи роль каждой колонки. Верни JSON:\n"
        f'{{"model": "имя_колонки_с_моделями", '
        f'"q": "имя_колонки_с_подачей_м3ч", '
        f'"h": "имя_колонки_с_напором_м", '
        f'"kw": "имя_колонки_с_мощностью_квт"}}\n'
        f"Если колонки нет — пропусти ключ. Только JSON."
    )

    try:
        resp = requests.post(
            DEEPSEEK_URL,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
                "max_tokens": 200,
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            logger.warning("DeepSeek HTTP %d", resp.status_code)
            return {}

        text = resp.json()["choices"][0]["message"]["content"].strip()
        # Extract JSON
        if "{" in text:
            text = text[text.index("{"):text.rindex("}") + 1]
        result = json.loads(text)
        # Validate keys
        return {k: v for k, v in result.items() if k in ("model", "q", "h", "kw", "rpm") and v}

    except Exception as e:
        logger.warning("DeepSeek classify error: %s", e)
        return {}


def extract_h_from_table(model_name, table_text, timeout=30):
    """Ask DeepSeek to find H (head) value for a specific pump model from table text.
    Returns float or 0.
    """
    if not DEEPSEEK_API_KEY:
        return 0

    prompt = (
        f"В этой таблице найди напор H (в метрах) для модели {model_name}.\n"
        f"Таблица:\n{table_text[:2000]}\n\n"
        f"Ответь ОДНИМ числом. Если не нашёл — ответь 0."
    )

    try:
        resp = requests.post(
            DEEPSEEK_URL,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
                "max_tokens": 20,
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            return 0

        text = resp.json()["choices"][0]["message"]["content"].strip()
        import re
        nums = re.findall(r"(\d+[.,]?\d*)", text)
        if nums:
            return float(nums[0].replace(",", "."))
        return 0

    except Exception:
        return 0
