"""MillionVerifier email verification with multithreaded batch support."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

import requests

# Canonical statuses from MillionVerifier CSV/API output.
VALID_STATUSES = {"ok", "invalid", "catch_all", "unknown", "disposable"}

# Statuses considered safe to sync to Instantly.
GOOD_STATUSES = {"ok"}

# Everything else is blocked from Instantly.
BAD_STATUSES = VALID_STATUSES - GOOD_STATUSES

MILLIONVERIFIER_API_URL = "https://api.millionverifier.com/api/v3/"


def _normalize_status(raw: str | None) -> str:
    """Lowercase, strip, and validate a verification status string.

    Unknown or unexpected values are mapped to ``"unknown"`` so they are
    treated as *bad* (fail-safe).
    """
    if not raw or not isinstance(raw, str):
        return "unknown"
    cleaned = raw.strip().lower()
    if cleaned in VALID_STATUSES:
        return cleaned
    return "unknown"


def verify_single_email(api_key: str, email: str, timeout: int = 10) -> str:
    """Call the MillionVerifier single-email API.

    Parameters
    ----------
    api_key : str
        MillionVerifier API key.
    email : str
        Email address to verify.
    timeout : int
        HTTP request timeout in seconds (default 10).

    Returns
    -------
    str
        One of ``ok``, ``invalid``, ``catch_all``, ``unknown``, ``disposable``.
        Returns ``"unknown"`` on any network/API error (fail-safe).
    """
    if not api_key or not email:
        return "unknown"

    try:
        resp = requests.get(
            MILLIONVERIFIER_API_URL,
            params={"api": api_key, "email": email.strip().lower(), "timeout": timeout},
            timeout=timeout + 5,  # HTTP timeout slightly above the API timeout
        )
        if resp.status_code == 200:
            data = resp.json()
            return _normalize_status(data.get("result"))
        # Non-200 → treat as unknown (fail-safe)
        return "unknown"
    except Exception:
        return "unknown"


def verify_pending_leads(
    records: list[dict],
    api_key: str,
    *,
    max_workers: int = 15,
    on_progress: Callable[[int, int], None] | None = None,
) -> list[tuple[dict, str, bool]]:
    """Verify a batch of pending lead records.

    For each record:
    - If ``verification_status`` already has a value → trust it, skip API.
    - Otherwise → call :func:`verify_single_email` via a thread pool.

    Parameters
    ----------
    records : list[dict]
        Lead records (dicts from the Airtable DataFrame).
    api_key : str
        MillionVerifier API key.
    max_workers : int
        Maximum concurrent API calls (default 15).
    on_progress : callable, optional
        ``on_progress(completed, total)`` called after each record is resolved.

    Returns
    -------
    list[tuple[dict, str, bool]]
        A list of ``(record, normalized_status, was_api_call)`` tuples.
        ``was_api_call`` is ``True`` when the status came from the API (not
        pre-existing), which means we need to persist it to Airtable.
    """
    results: list[tuple[dict, str, bool]] = []
    to_verify: list[tuple[int, dict]] = []  # (index, record) for API calls

    # First pass: separate pre-verified from needs-API
    for idx, rec in enumerate(records):
        existing_status = rec.get("verification_status")
        if existing_status and isinstance(existing_status, str) and existing_status.strip():
            # Already verified (e.g. manual CSV upload) – trust it
            results.append((rec, _normalize_status(existing_status), False))
        else:
            # Placeholder – will be filled by the thread pool
            results.append((rec, "", False))
            to_verify.append((idx, rec))

    if not to_verify:
        # Everything was pre-verified
        if on_progress:
            on_progress(len(records), len(records))
        return results

    completed = len(records) - len(to_verify)  # pre-verified count

    # Second pass: multithreaded API verification
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {}
        for idx, rec in to_verify:
            email = rec.get("key_contact_email")
            if not email or not isinstance(email, str) or not email.strip():
                # No email → can't verify → unknown
                results[idx] = (rec, "unknown", True)
                completed += 1
                if on_progress:
                    on_progress(completed, len(records))
                continue
            future = executor.submit(verify_single_email, api_key, email.strip())
            future_to_idx[future] = idx

        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            rec = records[idx]
            try:
                status = future.result()
            except Exception:
                status = "unknown"
            results[idx] = (rec, _normalize_status(status), True)
            completed += 1
            if on_progress:
                on_progress(completed, len(records))

    return results
