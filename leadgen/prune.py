"""Prune leads that were contacted but never replied.

Two-step flow:

1. PREVIEW — paginate Instantly's `/leads/list` with FILTER_VAL_CONTACTED,
   keep only those with `email_reply_count == 0`, group the count by the
   `industry` custom variable so the operator sees a per-industry
   breakdown before committing.

2. EXECUTE — for each candidate:
     a. DELETE /api/v2/leads/{id} on Instantly (no bulk delete in v2).
     b. Soft-delete the matching raw.scraped_leads row by setting
        `excluded_at = now(), excluded_reason = 'contacted_no_reply'`.
        The matching key is instantly_lead_id when present, else email.

Soft-delete in raw is reversible (UPDATE excluded_at = NULL). The
Instantly delete is permanent — it frees plan capacity.
"""
from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from .campaign_push import _classify_error
from .instantly import (
    delete_lead_from_instantly,
    list_contacted_unreplied_leads,
)


_PRUNE_REASON = "contacted_no_reply"


def _industry_of(lead: dict) -> str:
    """Extract `industry` from the lead's custom_variables, with fallbacks."""
    cv = lead.get("payload") or lead.get("custom_variables") or {}
    if isinstance(cv, dict):
        ind = cv.get("industry")
        if isinstance(ind, str) and ind.strip():
            return ind.strip()
    # Some Instantly responses surface the value at the top level.
    top = lead.get("industry")
    if isinstance(top, str) and top.strip():
        return top.strip()
    return "(unknown)"


def preview_contacted_unreplied(
    api_key: str, *, log=None, on_progress=None,
) -> dict:
    """Build a per-industry breakdown of contacted-unreplied candidates.

    Returns:
        {
            "total":   int,
            "by_industry": [(industry, count), ...]  # sorted desc by count
            "candidates": [<lead dict>, ...],         # raw Instantly objects
        }
    """
    candidates = list_contacted_unreplied_leads(
        api_key, log=log, on_progress=on_progress,
    )
    counter: Counter[str] = Counter()
    for c in candidates:
        counter[_industry_of(c)] += 1
    return {
        "total": len(candidates),
        "by_industry": counter.most_common(),
        "candidates": candidates,
    }


def _normalize_email(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def execute_prune(
    backend,
    *,
    api_key: str,
    candidates: list[dict],
    debug: bool = False,
    max_workers: int = 5,
    on_progress=None,
    log=None,
) -> dict:
    """Delete each candidate from Instantly + soft-delete from raw.

    `candidates` is the list returned by preview_contacted_unreplied.
    Returns counts: {deleted_instantly, soft_deleted_raw, failed}.
    """
    total = len(candidates)
    if total == 0:
        return {"deleted_instantly": 0, "soft_deleted_raw": 0, "failed": 0, "details": []}

    def _emit(msg: str) -> None:
        if log is not None:
            try: log(msg)
            except Exception: pass

    now_iso = datetime.now(timezone.utc).isoformat()
    deleted = 0
    soft_deleted = 0
    failed = 0
    details: list[dict] = []
    processed = 0

    def _one(lead: dict) -> dict:
        instantly_id = lead.get("id")
        email = _normalize_email(lead.get("email"))
        result = {
            "instantly_id": instantly_id,
            "email": email,
            "industry": _industry_of(lead),
            "deleted": False,
            "soft_deleted": False,
            "error": None,
        }
        if instantly_id:
            ok, err = delete_lead_from_instantly(api_key, instantly_id, debug=debug)
            if ok or (err and "not found" in str(err).lower()):
                result["deleted"] = True
            else:
                result["error"] = f"Instantly delete failed: {err}"
                return result
        # Soft-delete raw row (match by instantly_lead_id, fallback to email).
        # We use a single batch_update so the matching happens in SQL via
        # the helper rather than fetching the row id first.
        upd_fields = {
            "excluded_at": now_iso,
            "excluded_reason": _PRUNE_REASON,
            "instantly_status": "Pruned",
        }
        # backend.batch_update requires a raw row id. The Instantly object
        # gives us instantly_lead_id and email — neither is the raw row id.
        # Use a backend-level helper that matches on those keys.
        ok = backend.soft_delete_by_instantly_id_or_email(
            instantly_lead_id=instantly_id, email=email, fields=upd_fields,
        )
        result["soft_deleted"] = bool(ok)
        if not ok:
            result["error"] = (result["error"] or "") + " soft-delete miss in raw"
        return result

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_one, c) for c in candidates]
        for fut in as_completed(futures):
            r = fut.result()
            details.append(r)
            if r["deleted"]:
                deleted += 1
            if r["soft_deleted"]:
                soft_deleted += 1
            if r["error"] and not r["deleted"]:
                failed += 1
            processed += 1
            if on_progress is not None:
                try: on_progress(processed, total)
                except Exception: pass

    _emit(
        f"📊 Prune summary → deleted_instantly={deleted} "
        f"soft_deleted_raw={soft_deleted} failed={failed}"
    )
    if failed or any(d.get("error") and d.get("deleted") for d in details):
        delete_buckets: dict[str, int] = {}
        raw_miss = 0
        for d in details:
            err = d.get("error")
            if not err:
                continue
            if not d.get("deleted"):
                delete_buckets[_classify_error(err)] = (
                    delete_buckets.get(_classify_error(err), 0) + 1
                )
            else:
                raw_miss += 1
        for bucket, n in sorted(delete_buckets.items(), key=lambda kv: kv[1], reverse=True):
            _emit(f"   ❌ {n}× {bucket}")
        if raw_miss:
            _emit(f"   ⚠️ {raw_miss}× soft-delete miss in raw (Instantly deleted OK)")

    return {
        "deleted_instantly": deleted,
        "soft_deleted_raw": soft_deleted,
        "failed": failed,
        "details": details,
    }
