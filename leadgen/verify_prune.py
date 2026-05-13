"""Verify every Instantly lead via MillionVerifier and prune the bad ones.

Two-step flow that mirrors :mod:`leadgen.prune` but uses email-quality as
the deletion criterion instead of "contacted but never replied":

1. PREVIEW — list every lead in the Instantly account (no filter),
   group counts by `industry` so the operator can sanity-check scope.

2. EXECUTE — for each candidate:
     a. Call MillionVerifier (threaded).
     b. If the status is BAD (``invalid`` / ``disposable`` — and optionally
        ``unknown`` if the operator opted in):
          • DELETE /api/v2/leads/{id} on Instantly (frees plan capacity).
          • Soft-delete the matching raw.scraped_leads row by setting
            ``excluded_at = now()`` and
            ``excluded_reason = 'bad_email:<status>'``.
            Match key: ``instantly_lead_id`` if present, else email.
     c. If the status is GOOD or SKIP: leave the lead alone.

The fail-safe rules from :mod:`leadgen.millionverifier` are preserved —
``unknown`` is treated as inconclusive (and only deleted when the operator
explicitly opts in), so a transient MillionVerifier outage cannot wipe
the account.
"""
from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from .campaign_push import _classify_error
from .instantly import delete_lead_from_instantly, list_all_leads
from .millionverifier import (
    BAD_STATUSES,
    GOOD_STATUSES,
    SKIP_STATUSES,
    verify_single_email,
)


_PRUNE_REASON_PREFIX = "bad_email"


def _industry_of(lead: dict) -> str:
    """Extract `industry` from the lead's custom_variables, with fallbacks."""
    cv = lead.get("payload") or lead.get("custom_variables") or {}
    if isinstance(cv, dict):
        ind = cv.get("industry")
        if isinstance(ind, str) and ind.strip():
            return ind.strip()
    top = lead.get("industry")
    if isinstance(top, str) and top.strip():
        return top.strip()
    return "(unknown)"


def _normalize_email(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def preview_all_leads(
    api_key: str, *, log=None, on_progress=None,
) -> dict:
    """Build a per-industry breakdown of every Instantly lead in the account.

    Returns:
        {
            "total":      int,
            "by_industry": [(industry, count), ...]  # sorted desc by count
            "candidates": [<lead dict>, ...],         # raw Instantly objects
        }
    """
    candidates = list_all_leads(
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


def execute_verify_prune(
    backend,
    *,
    api_key: str,
    mv_api_key: str,
    candidates: list[dict],
    debug: bool = False,
    max_workers: int = 10,
    include_unknown_as_bad: bool = False,
    on_progress=None,
    log=None,
) -> dict:
    """Verify each candidate via MillionVerifier; delete the bad ones.

    `candidates` is the list returned by :func:`preview_all_leads`.
    `mv_api_key` is the MillionVerifier API key.
    `include_unknown_as_bad`: when True, ``unknown`` is treated as bad and
        deleted. Default False (fail-safe: only ``invalid`` / ``disposable``
        get pruned).

    Returns counts:
        {
            "verified":            int,  # total checked
            "good":                int,  # ok / catch_all → kept
            "skipped":             int,  # unknown (not deleted unless opt-in)
            "bad":                 int,  # invalid / disposable (+ unknown if opt-in)
            "no_email":            int,  # candidate had no email to verify
            "deleted_instantly":   int,
            "soft_deleted_raw":    int,
            "failed":              int,
            "by_status":           dict[str, int],
            "details":             list[dict],  # per-lead outcome
        }
    """
    total = len(candidates)
    base_result = {
        "verified": 0, "good": 0, "skipped": 0, "bad": 0, "no_email": 0,
        "deleted_instantly": 0, "soft_deleted_raw": 0, "failed": 0,
        "by_status": {}, "details": [],
    }
    if total == 0:
        return base_result
    if not mv_api_key:
        raise ValueError("MillionVerifier API key is required to verify emails")

    def _emit(msg: str) -> None:
        if log is not None:
            try: log(msg)
            except Exception: pass

    delete_set = set(BAD_STATUSES)
    if include_unknown_as_bad:
        delete_set = delete_set | set(SKIP_STATUSES)

    now_iso = datetime.now(timezone.utc).isoformat()
    by_status: Counter[str] = Counter()
    details: list[dict] = []
    deleted = 0
    soft_deleted = 0
    failed = 0
    verified = 0
    good = 0
    skipped = 0
    bad = 0
    no_email = 0
    processed = 0

    def _one(lead: dict) -> dict:
        instantly_id = lead.get("id")
        email = _normalize_email(lead.get("email"))
        industry = _industry_of(lead)
        outcome: dict[str, Any] = {
            "instantly_id": instantly_id,
            "email": email,
            "industry": industry,
            "status": None,
            "verified": False,
            "deleted": False,
            "soft_deleted": False,
            "error": None,
        }

        if not email:
            outcome["status"] = "no_email"
            return outcome

        try:
            status = verify_single_email(mv_api_key, email)
        except Exception as e:
            outcome["error"] = f"MillionVerifier exception: {e}"
            outcome["status"] = "unknown"
            return outcome

        outcome["status"] = status
        outcome["verified"] = True

        # Decide action based on status bucket
        if status in GOOD_STATUSES:
            return outcome  # keep
        if status not in delete_set:
            # SKIP bucket (or unrecognized) → leave alone
            return outcome

        # Bad → delete from Instantly first
        if instantly_id:
            ok, err = delete_lead_from_instantly(api_key, instantly_id, debug=debug)
            if ok or (err and "not found" in str(err).lower()):
                outcome["deleted"] = True
            else:
                outcome["error"] = f"Instantly delete failed: {err}"
                return outcome
        else:
            # No instantly id to delete on; still try to soft-delete the raw row.
            outcome["error"] = "No Instantly lead id"

        # Soft-delete the raw row
        upd_fields = {
            "excluded_at": now_iso,
            "excluded_reason": f"{_PRUNE_REASON_PREFIX}:{status}",
            "instantly_status": "Pruned (bad email)",
        }
        try:
            ok = backend.soft_delete_by_instantly_id_or_email(
                instantly_lead_id=instantly_id, email=email, fields=upd_fields,
            )
        except Exception as e:
            outcome["error"] = (outcome["error"] or "") + f" soft-delete exception: {e}"
            return outcome
        outcome["soft_deleted"] = bool(ok)
        if not ok:
            outcome["error"] = (outcome["error"] or "") + " soft-delete miss in raw"
        return outcome

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_one, c) for c in candidates]
        for fut in as_completed(futures):
            r = fut.result()
            details.append(r)
            status = r.get("status") or "unknown"
            if status == "no_email":
                no_email += 1
            else:
                verified += 1 if r.get("verified") else 0
                by_status[status] += 1
                if status in GOOD_STATUSES:
                    good += 1
                elif status in delete_set:
                    bad += 1
                else:
                    skipped += 1
            if r.get("deleted"):
                deleted += 1
            if r.get("soft_deleted"):
                soft_deleted += 1
            # Count as failure only when we *tried* to delete and it didn't go.
            if r.get("error") and not r.get("deleted") and status in delete_set:
                failed += 1
            processed += 1
            if on_progress is not None:
                try: on_progress(processed, total)
                except Exception: pass

    _emit(
        f"📊 Verify summary → verified={verified} good={good} skipped={skipped} "
        f"bad={bad} no_email={no_email} deleted_instantly={deleted} "
        f"soft_deleted_raw={soft_deleted} failed={failed}"
    )
    if by_status:
        breakdown = ", ".join(
            f"{s}={n}" for s, n in sorted(by_status.items(), key=lambda kv: -kv[1])
        )
        _emit(f"   • by_status: {breakdown}")

    if failed or any(d.get("error") and d.get("deleted") for d in details):
        delete_buckets: dict[str, int] = {}
        delete_samples: dict[str, list[str]] = {}
        raw_miss = 0
        for d in details:
            err = d.get("error")
            if not err:
                continue
            if not d.get("deleted") and d.get("status") in delete_set:
                bucket = _classify_error(err)
                delete_buckets[bucket] = delete_buckets.get(bucket, 0) + 1
                samples = delete_samples.setdefault(bucket, [])
                if len(samples) < 2:
                    snippet = str(err)[:200]
                    if snippet not in samples:
                        samples.append(snippet)
            elif d.get("deleted") and not d.get("soft_deleted"):
                raw_miss += 1
        for bucket, n in sorted(delete_buckets.items(), key=lambda kv: kv[1], reverse=True):
            _emit(f"   ❌ {n}× {bucket}")
            for ex_snip in delete_samples.get(bucket, []):
                _emit(f"      e.g. {ex_snip}")
        if raw_miss:
            _emit(f"   ⚠️ {raw_miss}× soft-delete miss in raw (Instantly deleted OK)")

    return {
        "verified": verified,
        "good": good,
        "skipped": skipped,
        "bad": bad,
        "no_email": no_email,
        "deleted_instantly": deleted,
        "soft_deleted_raw": soft_deleted,
        "failed": failed,
        "by_status": dict(by_status),
        "details": details,
    }
