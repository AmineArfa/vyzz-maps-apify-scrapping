from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any


def _is_bad_float(v: Any) -> bool:
    """
    Check if v is a float-like value that is NaN or Infinity.
    Works for Python float, numpy.float64, numpy.float32, etc.
    """
    # First: quick check for standard Python float
    if isinstance(v, float):
        return math.isnan(v) or math.isinf(v)
    
    # numpy scalar types (float64, float32, etc.) are not isinstance(float)
    # but they have .item() method and support math.isnan/isinf
    try:
        # Check if it's a numpy-like scalar with a numeric value
        if hasattr(v, 'dtype') and hasattr(v, 'item'):
            # It's a numpy scalar
            f = float(v)
            return math.isnan(f) or math.isinf(f)
    except (TypeError, ValueError):
        pass
    
    return False


def _is_pandas_na(v: Any) -> bool:
    """
    Check if v is a pandas NA-like value (NaT, NA, etc.) without raising.
    """
    try:
        import pandas as pd  # type: ignore
        # pd.isna returns a scalar bool for scalar inputs
        # For arrays it returns an array - we only want scalar check here
        result = pd.isna(v)
        # If result is a numpy array (happens for array inputs), skip
        if hasattr(result, '__len__') and not isinstance(result, (str, bytes)):
            return False
        return bool(result)
    except Exception:
        return False


def sanitize_for_json(value: Any) -> Any:
    """
    Recursively sanitize a Python object so it is safe to pass to `requests(..., json=...)`
    which uses JSON encoding with `allow_nan=False` (raises on NaN/Infinity).

    Rules:
    - NaN/Infinity (float or numpy scalar) -> None
    - pandas NaT/NA -> None
    - datetime/date/Timestamp -> ISO string
    - numpy arrays -> list (recursively sanitized)
    - dict/list/tuple -> deep-sanitized
    - everything else unchanged
    """
    if value is None:
        return None

    # Check for bad floats first (NaN, Inf) - covers float and numpy scalars
    if _is_bad_float(value):
        return None

    # Check for pandas NA-like values (NaT, pd.NA)
    if _is_pandas_na(value):
        return None

    # pandas Timestamp -> ISO string
    try:
        import pandas as pd  # type: ignore
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
    except Exception:
        pass

    # datetime/date -> ISO string
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # numpy arrays -> list (then recurse)
    try:
        import numpy as np  # type: ignore
        if isinstance(value, np.ndarray):
            return [sanitize_for_json(v) for v in value.tolist()]
    except Exception:
        pass

    # dict -> recurse on values
    if isinstance(value, dict):
        return {k: sanitize_for_json(v) for k, v in value.items()}

    # list/tuple -> recurse
    if isinstance(value, (list, tuple)):
        return [sanitize_for_json(v) for v in value]

    # numpy scalar types that aren't NaN/Inf -> convert to Python native
    try:
        if hasattr(value, 'dtype') and hasattr(value, 'item'):
            return value.item()
    except Exception:
        pass

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

        # Check for bad floats (NaN/Inf)
        if _is_bad_float(v):
            hits.append((path, repr(v)))
            return

        # Check for pandas NA-like
        if _is_pandas_na(v):
            hits.append((path, repr(v)))
            return

        # numpy arrays
        try:
            import numpy as np  # type: ignore
            if isinstance(v, np.ndarray):
                for i, vv in enumerate(v.tolist()):
                    walk(vv, f"{path}[{i}]")
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

