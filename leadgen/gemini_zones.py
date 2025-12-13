from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests


DEFAULT_GEMINI_MODEL = "models/gemini-flash-lite-latest"

# User-provided prompt (kept verbatim as much as possible). We append the user's input at runtime.
ZONES_PROMPT = """You are a Geographic Data API. I will provide a geographic input (e.g., a City, State, or Country). Your task is to return a JSON object containing exactly 10 distinct, relevant sub-zones.

Zoning Logic:

Analyze the Scale:

If input is a Country → Return top 10 States/Provinces or Major Cities.

If input is a State/Region → Return top 10 Counties or Major Cities.

If input is a City → Return top 10 Neighborhoods or Postal Codes.

Selection: Prioritize zones by population, economic relevance, or size.

Output Rules (Critical):

Return only raw JSON. No markdown formatting (no ```json).

NO placeholders: Do not use 'N/A', 'null', or empty fields.

Adaptive String Format: Format each zone string based on what is relevant for that specific location.

Good Example (City Input): "SoHo, New York, NY, USA"

Good Example (Country Input): "California, USA" (Note: No 'City' field needed)

Good Example (State Input): "Miami-Dade County, Florida, USA"

JSON Structure: {"zones": ["String 1", "String 2", ... ]}
"""


@dataclass(frozen=True)
class GeminiZonesResult:
    zones: list[str]
    raw_text: str


def _cache_path() -> Path:
    return Path(".cache") / "gemini_zones_cache.json"


def _load_cache() -> dict[str, Any]:
    path = _cache_path()
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache: dict[str, Any]) -> None:
    path = _cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # Cache is best-effort.
        return


def _normalize_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _extract_first_json_object(text: str) -> str:
    """
    Gemini sometimes returns extra whitespace or text. We try to extract the first JSON object.
    We also strip accidental markdown fences if present.
    """
    if not text:
        raise ValueError("Empty model output")

    cleaned = text.strip()
    # Defensive: if the model accidentally includes fences, remove them.
    cleaned = cleaned.replace("```json", "").replace("```", "").strip()

    if cleaned.startswith("{") and cleaned.endswith("}"):
        return cleaned

    # Greedy match between first '{' and last '}' (works for single JSON object).
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in model output")
    return cleaned[start : end + 1]


def _parse_zones_json(text: str) -> GeminiZonesResult:
    raw_text = text or ""
    json_str = _extract_first_json_object(raw_text)
    try:
        obj = json.loads(json_str)
    except Exception as e:
        raise ValueError(f"Invalid JSON from model: {e}") from e

    if not isinstance(obj, dict):
        raise ValueError("Model JSON root is not an object")

    zones = obj.get("zones")
    if not isinstance(zones, list):
        raise ValueError('Model JSON missing key "zones" as a list')

    normed: list[str] = []
    seen = set()
    for z in zones:
        if not isinstance(z, str):
            raise ValueError("Zone is not a string")
        zz = z.strip()
        if not zz:
            raise ValueError("Zone is empty")
        key = _normalize_key(zz)
        if key in seen:
            continue
        seen.add(key)
        normed.append(zz)

    # Requirement: EXACTLY 10 distinct zones.
    if len(normed) != 10:
        raise ValueError(f"Expected 10 distinct zones, got {len(normed)}")

    return GeminiZonesResult(zones=normed, raw_text=raw_text)


def get_gemini_api_key(
    provided_key: Optional[str] = None,
) -> Optional[str]:
    """
    Get the Gemini key from an explicit argument or environment variable.
    Streamlit secrets are handled by leadgen.config.get_secrets().
    """
    key = (provided_key or "").strip() or os.getenv("GOOGLE_GENERATIVE_AI_API_KEY", "").strip()
    return key or None


def generate_zones_with_gemini(
    location_input: str,
    *,
    api_key: Optional[str],
    model: str = DEFAULT_GEMINI_MODEL,
    timeout_s: int = 25,
    debug: bool = False,
    cache_ttl_s: int = 60 * 60 * 24 * 14,  # 14 days
) -> Optional[GeminiZonesResult]:
    """
    Returns GeminiZonesResult if successful; otherwise None (caller should fallback to no-split).
    """
    loc = (location_input or "").strip()
    if not loc:
        return None

    api_key = get_gemini_api_key(api_key)
    if not api_key:
        return None

    cache_key = f"{model}::{_normalize_key(loc)}"
    cache = _load_cache()
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        ts = cached.get("ts")
        zones = cached.get("zones")
        if isinstance(ts, (int, float)) and isinstance(zones, list) and (time.time() - ts) < cache_ttl_s:
            try:
                res = _parse_zones_json(json.dumps({"zones": zones}))
                # Keep raw_text empty for cached entries.
                return GeminiZonesResult(zones=res.zones, raw_text="")
            except Exception:
                # Ignore bad cache.
                pass

    url = f"https://generativelanguage.googleapis.com/v1beta/{model}:generateContent"
    params = {"key": api_key}
    prompt = f"{ZONES_PROMPT}\n\nINPUT: {loc}\n"

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            # Keep it stable-ish; we care about consistency more than creativity.
            "temperature": 0.3,
            "maxOutputTokens": 512,
        },
    }

    try:
        resp = requests.post(url, params=params, json=payload, timeout=timeout_s)
        if resp.status_code >= 400:
            if debug:
                return None
            return None
        data = resp.json()
        # Typical shape: candidates[0].content.parts[].text
        candidates = data.get("candidates") or []
        if not candidates:
            return None
        content = (candidates[0] or {}).get("content") or {}
        parts = content.get("parts") or []
        texts = []
        for p in parts:
            t = (p or {}).get("text")
            if isinstance(t, str):
                texts.append(t)
        out_text = "\n".join(texts).strip()
        if not out_text:
            return None

        parsed = _parse_zones_json(out_text)

        cache[cache_key] = {"ts": time.time(), "zones": parsed.zones}
        _save_cache(cache)

        return parsed
    except Exception:
        return None


