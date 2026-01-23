from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Iterable


def _is_nan(v: Any) -> bool:
    # Handles float('nan') and numpy.nan (which is a float subclass).
    return isinstance(v, float) and math.isnan(v)


def _is_inf(v: Any) -> bool:
    return isinstance(v, float) and math.isinf(v)


def sanitize_for_json(value: Any) -> Any:
    """
    Recursively sanitize a Python object so it is safe to pass to `requests(..., json=...)`
    which uses JSON encoding with `allow_nan=False` (raises on NaN/Infinity).

    Rules:
    - NaN/Infinity -> None
    - datetime/date -> ISO string
    - dict/list/tuple -> deep-sanitized
    - everything else unchanged
    """
    if value is None:
        return None

    if _is_nan(value) or _is_inf(value):
        return None

    # Pandas timestamps / NaT support (optional dependency).
    # We avoid importing pandas globally to keep this util lightweight.
    try:
        import pandas as pd  # type: ignore

        if pd.isna(value):  # covers NaT and pandas NA scalars
            return None
        if isinstance(value, pd.Timestamp):
            # Prefer timezone-preserving isoformat.
            return value.isoformat()
    except Exception:
        pass

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, dict):
        return {k: sanitize_for_json(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [sanitize_for_json(v) for v in value]

    return value


def find_non_json_numbers(value: Any, max_hits: int = 50) -> list[tuple[str, str]]:
    """
    Return a list of (path, repr(value)) for any NaN/Infinity values found.
    Useful for debugging payload issues before API calls.
    """
    hits: list[tuple[str, str]] = []

    def walk(v: Any, path: str) -> None:
        nonlocal hits
        if len(hits) >= max_hits:
            return

        if _is_nan(v) or _is_inf(v):
            hits.append((path, repr(v)))
            return

        try:
            import pandas as pd  # type: ignore

            if pd.isna(v):
                # Only record if it is a scalar Na-like (avoid flagging empty strings etc.)
                # pd.isna("") is False, so we're good.
                hits.append((path, repr(v)))
                return
        except Exception:
            pass

        if isinstance(v, dict):
            for k, vv in v.items():
                walk(vv, f"{path}.{k}" if path else str(k))
            return

        if isinstance(v, (list, tuple)):
            for i, vv in enumerate(v):
                walk(vv, f"{path}[{i}]")
            return

    walk(value, "")
    return hits

