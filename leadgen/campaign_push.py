"""Push a filtered batch of raw.scraped_leads rows into an Instantly campaign.

This is the heart of the campaign composer flow. The contract is:

- For each lead with `instantly_lead_id` already set: MOVE it into the target
  campaign via Instantly's move endpoint. The id stays the same on both sides
  — no orphan in the old campaign, no duplicate in the new one.
- For each lead without `instantly_lead_id`: CREATE it in Instantly, then
  immediately write the returned id back to `raw.scraped_leads` so the row
  is always linked. The write-back happens inside the same loop iteration,
  not as a separate batch — even if the loop is interrupted halfway through
  we never end up with rows in Instantly that have no link in raw.

The push is per-lead. We could batch the create path with `/leads/add`, but
matching returned ids back to raw rows by email is fragile (case, trims,
duplicates) and the savings aren't worth losing the strict create-then-
writeback ordering. Instantly's per-call rate is fine for the scale we
operate at; if it ever isn't, parallelize via a ThreadPoolExecutor.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from .instantly import (
    bulk_move_leads_to_campaign,
    export_leads_to_instantly,
    inject_lid_to_lead,
    is_valid_uuid,
    move_lead_to_campaign,
    search_lead_by_email,
)
from .ticket_tier import TIERS


import time

# Conservative chunk size + inter-chunk delay. Empirically 100/call hit
# Instantly's rate limiter under heavy throughput, dropping whole chunks
# silently. 50 + 250ms throttle keeps us comfortably under the limit and
# the SQL filter on re-runs absorbs any chunks that still fail.
_BULK_MOVE_CHUNK = 50
_BULK_MOVE_THROTTLE_SEC = 0.25


def _normalize_email(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _classify_error(err: str | None) -> str:
    """Bucket a raw error string into a short, human-readable category.

    The streaming log used to print one line per failure — at thousands
    of failures that overwhelms the UI and is impossible to scan.
    Instead we group by category and emit a single count per bucket.
    """
    if not err:
        return "unknown"
    e = str(err).lower()
    if "lead limit reached" in e or "remaining uploads" in e:
        return "instantly quota / lead limit (403)"
    if "no resolvable source campaign" in e or "no source campaign" in e:
        return "lead in list (no source campaign for move)"
    if "must be object" in e or "fst_err_validation" in e:
        return "instantly schema validation (400)"
    if " 401 " in f" {e} " or "unauthorized" in e:
        return "auth (401)"
    if " 404 " in f" {e} " or "not found" in e:
        return "not found (404)"
    if " 429 " in f" {e} " or "rate limit" in e or "too many requests" in e:
        return "rate limit (429)"
    if " 402 " in f" {e} ":
        return "payment required (402)"
    if " 403 " in f" {e} ":
        return "forbidden (403)"
    if " 400 " in f" {e} ":
        return "bad request (400)"
    if "exception" in e or "timeout" in e or "connection" in e:
        return "network/exception"
    if "create returned 0" in e or "search found nothing" in e:
        return "create returned 0 + search miss"
    if "backend.batch_update" in e:
        return "backend writeback failed"
    if "no email" in e:
        return "no email on raw row"
    return "other"


def _summarize_failures(
    results: list[dict],
) -> list[tuple[str, int, list[str]]]:
    """Return [(bucket, count, sample_errors), ...] sorted desc by count.

    `sample_errors` is up to 2 distinct verbatim error strings per
    bucket so the operator can see the exact wording without opening
    the failed-leads table. Truncated to 200 chars each.
    """
    counter: dict[str, int] = {}
    samples: dict[str, list[str]] = {}
    for r in results:
        if r.get("op") != "failed":
            continue
        bucket = _classify_error(r.get("error"))
        counter[bucket] = counter.get(bucket, 0) + 1
        bucket_samples = samples.setdefault(bucket, [])
        if len(bucket_samples) < 2:
            err = (r.get("error") or "")[:200]
            if err and err not in bucket_samples:
                bucket_samples.append(err)
    return sorted(
        ((b, n, samples.get(b, [])) for b, n in counter.items()),
        key=lambda t: t[1],
        reverse=True,
    )


def _process_one(
    lead: dict,
    *,
    api_key: str,
    campaign_id: str,
    debug: bool,
) -> dict:
    """Process a single lead. Returns a result dict with op + status fields.

    Operations: 'moved' | 'created' | 'skipped' | 'failed' | 'already_in_place'.

    Side effect: when a lead is created, the returned `instantly_lead_id`
    field is set so the caller can write it back to raw.scraped_leads.
    """
    raw_id = lead.get("id")
    email = _normalize_email(lead.get("key_contact_email"))
    instantly_lead_id = lead.get("instantly_lead_id")
    current_campaign_id = lead.get("instantly_campaign_id")

    base = {
        "id": raw_id,
        "email": email,
        "company_name": lead.get("company_name"),
        "industry": lead.get("industry"),
        "ticket_tier": lead.get("ticket_tier"),
        "instantly_lead_id": instantly_lead_id,
    }

    # ── Already in target ────────────────────────────────────────────────
    # Defensive: callers should already filter these out at SQL level (see
    # build_where's `exclude_already_in_campaign_id`), but skip them here
    # too so the recategorize loop is safe to retry without burning API
    # calls on leads that are already where they should be.
    if (
        instantly_lead_id and is_valid_uuid(instantly_lead_id)
        and current_campaign_id == campaign_id
    ):
        return {
            **base, "op": "already_in_place",
            "instantly_lead_id": instantly_lead_id, "error": None,
        }

    # ── Move path ────────────────────────────────────────────────────────
    if instantly_lead_id and is_valid_uuid(instantly_lead_id):
        ok, err = move_lead_to_campaign(api_key, instantly_lead_id, campaign_id, debug=debug)
        if ok:
            return {**base, "op": "moved", "instantly_lead_id": instantly_lead_id, "error": None}
        return {**base, "op": "failed", "error": f"move failed: {err}"}

    # ── Create path ──────────────────────────────────────────────────────
    if not email:
        return {**base, "op": "skipped", "error": "no email"}

    cnt, created, _, err = export_leads_to_instantly(
        api_key, campaign_id, [lead], debug=debug,
    )
    if cnt > 0 and created:
        new_id = created[0].get("id")
        if new_id and is_valid_uuid(new_id):
            inject_lid_to_lead(api_key, new_id, debug=debug)
            return {**base, "op": "created", "instantly_lead_id": new_id, "error": None}
        return {**base, "op": "failed", "error": "create returned no id"}

    # Create returned 0 — likely already exists in Instantly under this
    # email but in a different campaign (so our raw row didn't have an id).
    # Look it up and move it in instead. This keeps us at one Instantly
    # lead per email, never duplicates.
    found, _ = search_lead_by_email(api_key, email, debug=debug)
    if found and found.get("id") and is_valid_uuid(found["id"]):
        found_id = found["id"]
        # The search response carries the lead's current campaign — pass it
        # so move_lead_to_campaign doesn't need a second GET to resolve.
        ok, move_err = move_lead_to_campaign(
            api_key, found_id, campaign_id,
            from_campaign_id=found.get("campaign"), debug=debug,
        )
        if ok:
            return {**base, "op": "moved", "instantly_lead_id": found_id, "error": None}
        return {**base, "op": "failed", "error": f"create=0, move-after-search failed: {move_err}"}

    if err:
        return {**base, "op": "failed", "error": f"create failed: {err}"}
    return {**base, "op": "failed", "error": "create returned 0 leads, search found nothing"}


def _base_for(lead: dict) -> dict:
    return {
        "id": lead.get("id"),
        "email": _normalize_email(lead.get("key_contact_email")),
        "company_name": lead.get("company_name"),
        "industry": lead.get("industry"),
        "ticket_tier": lead.get("ticket_tier"),
        "instantly_lead_id": lead.get("instantly_lead_id"),
    }


def _classify(leads: list[dict], campaign_id: str) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Split leads into buckets: already_in_place, to_move, to_create, skipped.

    Pure function; the SQL filter normally drops `already_in_place` upstream
    but the defensive split keeps the push correct if a caller passes stale
    rows.
    """
    already, to_move, to_create, skipped = [], [], [], []
    for lead in leads:
        instantly_lead_id = lead.get("instantly_lead_id")
        current_campaign_id = lead.get("instantly_campaign_id")
        email = _normalize_email(lead.get("key_contact_email"))

        if (
            instantly_lead_id and is_valid_uuid(instantly_lead_id)
            and current_campaign_id == campaign_id
        ):
            already.append(lead)
        elif instantly_lead_id and is_valid_uuid(instantly_lead_id):
            to_move.append(lead)
        elif email:
            to_create.append(lead)
        else:
            skipped.append(lead)
    return already, to_move, to_create, skipped


def push_leads_to_campaign(
    backend,
    *,
    api_key: str,
    leads: list[dict],
    campaign_id: str,
    debug: bool = False,
    max_workers: int = 5,
    on_progress=None,
    log=None,
) -> dict:
    """Push `leads` into Instantly `campaign_id` and write back the ids.

    Two-phase strategy:
      1. Pre-classify into already_in_place / to_move / to_create / skipped.
      2. Bulk-move existing leads in chunks of ~100 — one /leads/move call
         per chunk preserves lid + every other custom variable while cutting
         API call count by ~100x compared to per-lead PATCH.
      3. Per-lead create + writeback for new leads (the writeback is per-lead
         because the new id needs to land back in raw.scraped_leads
         regardless of batch outcome).

    `on_progress(done, total, phase)` is called after each meaningful step
    so the UI can refresh a status line / progress bar. Phases are
    'classify', 'move', 'create'.

    Returns the same shape as before: {moved, created, skipped,
    already_in_place, failed, details: [ ... ]}.
    """
    if not leads:
        return {
            "moved": 0, "created": 0, "skipped": 0, "failed": 0,
            "already_in_place": 0, "details": [],
        }

    total = len(leads)
    results: list[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    def _progress(done: int, phase: str) -> None:
        if on_progress is not None:
            try:
                on_progress(done, total, phase)
            except Exception:
                pass

    def _emit(msg: str) -> None:
        """Forward a log line to the caller's logger if provided."""
        if log is not None:
            try: log(msg)
            except Exception: pass

    def _log_failure(r: dict, ctx: str) -> None:
        """No-op: per-failure lines used to flood the log at thousands
        of failures. The end-of-run summary now emits a bucketed count
        instead. Per-failure rows still land in `results` and are shown
        in the UI's expandable failed-leads table.
        """
        return

    already, to_move, to_create, skipped = _classify(leads, campaign_id)
    _progress(0, "classify")

    # ── Already in place ─────────────────────────────────────────────────
    for lead in already:
        results.append({
            **_base_for(lead),
            "op": "already_in_place",
            "error": None,
        })

    def _writeback_one(r: dict) -> None:
        """Flush a single lead writeback immediately to raw.scraped_leads.

        Critical invariant: a successful Instantly create or move MUST be
        persisted before we move on to the next operation. Buffering
        writebacks in memory and flushing only at the end (the previous
        design) loses everything if the run is interrupted — the lead
        exists server-side (consuming plan capacity) but raw never knows,
        and re-runs can't deduplicate it.
        """
        if r["op"] in ("moved", "created") and r.get("id") and r.get("instantly_lead_id"):
            backend.batch_update([{
                "id": r["id"],
                "fields": {
                    "instantly_lead_id": r["instantly_lead_id"],
                    "instantly_campaign_id": campaign_id,
                    "instantly_statuts": "Success",
                    "last_synced_at": now_iso,
                },
            }])

    # ── Bulk move: group by SOURCE campaign first, then chunk ────────────
    # Instantly's /leads/move treats `ids` as a filter inside `campaign`
    # (the source). Leads from different source campaigns can't be moved
    # in a single call — group first, then chunk within each group.
    by_source: dict[str, list[dict]] = {}
    no_source: list[dict] = []
    for lead in to_move:
        src = lead.get("instantly_campaign_id")
        if src and is_valid_uuid(src):
            by_source.setdefault(src, []).append(lead)
        else:
            no_source.append(lead)

    moved_done = 0
    if by_source:
        _emit(
            f"📦 Move buckets: {len(by_source)} source campaign(s) "
            f"→ {sum(len(v) for v in by_source.values())} leads to move."
        )
    for src_id, src_leads in by_source.items():
        _emit(f"   • Source {src_id[:8]}…: {len(src_leads)} leads "
              f"in {-(-len(src_leads) // _BULK_MOVE_CHUNK)} chunks of {_BULK_MOVE_CHUNK}.")
        for i in range(0, len(src_leads), _BULK_MOVE_CHUNK):
            chunk = src_leads[i : i + _BULK_MOVE_CHUNK]
            ids = [l["instantly_lead_id"] for l in chunk]
            if (i > 0 or moved_done > 0) and _BULK_MOVE_THROTTLE_SEC > 0:
                time.sleep(_BULK_MOVE_THROTTLE_SEC)
            ok, err = bulk_move_leads_to_campaign(
                api_key, ids, campaign_id,
                from_campaign_id=src_id, debug=debug,
            )
            if ok:
                # Per-chunk writeback BEFORE moving to the next chunk so a
                # mid-loop crash leaves earlier chunks durably persisted.
                chunk_writeback = []
                for lead in chunk:
                    results.append({
                        **_base_for(lead), "op": "moved", "error": None,
                    })
                    chunk_writeback.append({
                        "id": lead["id"],
                        "fields": {
                            "instantly_lead_id": lead["instantly_lead_id"],
                            "instantly_campaign_id": campaign_id,
                            "instantly_statuts": "Success",
                            "last_synced_at": now_iso,
                        },
                    })
                if chunk_writeback:
                    backend.batch_update(chunk_writeback)
                moved_done += len(chunk)
            else:
                # Bulk failed — fall back to per-lead so we record granular
                # success/failure instead of failing the whole chunk.
                _emit(
                    f"⚠️ Bulk move chunk failed (size={len(chunk)} "
                    f"src={src_id[:8]}…): {err}. Falling back to per-lead."
                )
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futures = [
                        ex.submit(_process_one, lead, api_key=api_key,
                                  campaign_id=campaign_id, debug=debug)
                        for lead in chunk
                    ]
                    for fut in as_completed(futures):
                        r = fut.result()
                        results.append(r)
                        _writeback_one(r)
                        if r["op"] == "failed":
                            _log_failure(r, "move-fallback")
                moved_done += len(chunk)
            _progress(len(already) + moved_done, "move")

    # Leads with instantly_lead_id but no current campaign — rare. The
    # per-lead path resolves the source via GET /leads/{id} before moving.
    if no_source:
        _emit(f"📦 Per-lead move (no source campaign): {len(no_source)} leads.")
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [
                ex.submit(_process_one, lead, api_key=api_key,
                          campaign_id=campaign_id, debug=debug)
                for lead in no_source
            ]
            for fut in as_completed(futures):
                r = fut.result()
                results.append(r)
                _writeback_one(r)
                if r["op"] == "failed":
                    _log_failure(r, "no-source-move")
                moved_done += 1
                _progress(len(already) + moved_done, "move")

    # ── Per-lead create with immediate writeback ─────────────────────────
    create_done = 0
    if to_create:
        _emit(f"📦 Create bucket: {len(to_create)} new leads (per-lead, parallel x{max_workers}).")
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(
                    _process_one, lead, api_key=api_key,
                    campaign_id=campaign_id, debug=debug,
                ): lead
                for lead in to_create
            }
            for fut in as_completed(futures):
                r = fut.result()
                results.append(r)
                # IMMEDIATELY persist the new instantly_lead_id back to raw
                # so a subsequent crash never leaves an orphaned Instantly
                # lead that raw doesn't know about.
                _writeback_one(r)
                if r["op"] == "failed":
                    _log_failure(r, "create")
                create_done += 1
                _progress(len(already) + len(to_move) + create_done, "create")

    # ── No-email skips ───────────────────────────────────────────────────
    for lead in skipped:
        results.append({
            **_base_for(lead), "op": "skipped", "error": "no email",
        })

    counts = {"moved": 0, "created": 0, "skipped": 0, "failed": 0, "already_in_place": 0}
    for r in results:
        counts[r["op"]] = counts.get(r["op"], 0) + 1
    counts["details"] = results

    # Compact end-of-run summary. Replaces the per-failure ❌ FAIL lines
    # so the operator can copy a one-screen summary instead of scrolling
    # through thousands of identical errors.
    _emit(
        f"📊 Push summary → moved={counts['moved']} created={counts['created']} "
        f"already_in_place={counts['already_in_place']} skipped={counts['skipped']} "
        f"failed={counts['failed']}"
    )
    if counts["failed"]:
        for bucket, n, examples in _summarize_failures(results):
            _emit(f"   ❌ {n}× {bucket}")
            for ex in examples:
                _emit(f"      e.g. {ex}")

    return counts


def reconcile_unlinked_leads(
    backend,
    *,
    api_key: str,
    debug: bool = False,
    max_workers: int = 5,
    limit: int | None = None,
    on_progress=None,
    log=None,
) -> dict:
    """Recover leads whose Instantly create succeeded but writeback was lost.

    For each raw row with `instantly_lead_id IS NULL` and a real email,
    search Instantly by email. If found, write the existing lead id back
    so subsequent runs treat the row as MOVE-eligible (not CREATE-eligible),
    avoiding duplicates and reclaiming visibility into Instantly state.

    Returns:
        {
            "scanned": int,
            "linked": int,
            "not_found": int,
            "errored": int,
            "details": [ { id, email, found_id, found_campaign, error } ],
        }
    """
    candidates = backend.fetch_unlinked_leads_with_email(limit=limit)
    total = len(candidates)
    if not total:
        return {"scanned": 0, "linked": 0, "not_found": 0, "errored": 0, "details": []}

    def _emit(msg: str) -> None:
        if log is not None:
            try: log(msg)
            except Exception: pass

    now_iso = datetime.now(timezone.utc).isoformat()
    linked = 0
    not_found = 0
    errored = 0
    details: list[dict] = []

    def _one(lead: dict) -> dict:
        raw_id = lead.get("id")
        email = _normalize_email(lead.get("key_contact_email"))
        if not email:
            return {"id": raw_id, "email": None, "found_id": None, "error": "no email"}
        found, err = search_lead_by_email(api_key, email, debug=debug)
        if found and found.get("id") and is_valid_uuid(found["id"]):
            return {
                "id": raw_id, "email": email,
                "found_id": found["id"],
                "found_campaign": found.get("campaign"),
                "error": None,
            }
        return {
            "id": raw_id, "email": email,
            "found_id": None, "found_campaign": None,
            "error": err,
        }

    processed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_one, l): l for l in candidates}
        for fut in as_completed(futures):
            r = fut.result()
            processed += 1
            details.append(r)
            if r.get("found_id"):
                # Immediate writeback — same crash-safety principle as the
                # main push: never let an Instantly identity sit unlinked
                # in raw if we already know about it.
                fields = {
                    "instantly_lead_id": r["found_id"],
                    "last_synced_at": now_iso,
                }
                if r.get("found_campaign"):
                    fields["instantly_campaign_id"] = r["found_campaign"]
                ok = backend.batch_update([{"id": r["id"], "fields": fields}])
                if ok:
                    linked += 1
                else:
                    errored += 1
                    # Per-failure lines drop — see _summarize at end of run.
                    r["_bucket"] = "backend writeback failed"
            elif r.get("error"):
                errored += 1
                r["_bucket"] = _classify_error(r.get("error"))
            else:
                not_found += 1
            if on_progress is not None:
                try:
                    on_progress(processed, total)
                except Exception:
                    pass

    _emit(
        f"📊 Reconcile summary → scanned={total} linked={linked} "
        f"not_found={not_found} errored={errored}"
    )
    if errored:
        bucket_counts: dict[str, int] = {}
        bucket_samples: dict[str, list[str]] = {}
        for d in details:
            b = d.get("_bucket")
            if not b:
                continue
            bucket_counts[b] = bucket_counts.get(b, 0) + 1
            samples = bucket_samples.setdefault(b, [])
            if len(samples) < 2:
                err = (d.get("error") or "")[:200]
                if err and err not in samples:
                    samples.append(err)
        for bucket, n in sorted(bucket_counts.items(), key=lambda kv: kv[1], reverse=True):
            _emit(f"   ❌ {n}× {bucket}")
            for ex in bucket_samples.get(bucket, []):
                _emit(f"      e.g. {ex}")

    return {
        "scanned": total,
        "linked": linked,
        "not_found": not_found,
        "errored": errored,
        "details": details,
    }


def recategorize_all_by_tier(
    backend,
    *,
    api_key: str,
    resolve_campaign_id,
    debug: bool = False,
    max_workers: int = 5,
    on_tier_start=None,
    on_progress=None,
    log=None,
) -> dict:
    """One-click: re-route every lead in raw.scraped_leads into its tier campaign.

    Iterates LOW → MID → HIGH. For each tier:
      1. Resolve the target Instantly campaign id via `resolve_campaign_id(tier)`
         (caller decides find-or-create policy and naming).
      2. Pull every matching lead — including leads already in another active
         campaign — because the whole point is to reassign.
      3. Run the standard push machinery: existing leads MOVE (preserving
         instantly_lead_id and lid), new leads CREATE + writeback.

    Leads with `ticket_tier IS NULL` are not included in any tier filter, so
    they stay in their current campaign untouched. The operator should fix
    `industry` on those rows first if they want them recategorized.
    """
    by_tier: dict[str, dict] = {}
    for tier in TIERS:
        c_id = resolve_campaign_id(tier)
        if not c_id or not is_valid_uuid(c_id):
            by_tier[tier] = {
                "tier": tier,
                "campaign_id": None,
                "moved": 0, "created": 0, "skipped": 0, "failed": 0,
                "details": [],
                "error": f"could not resolve campaign for tier '{tier}'",
            }
            continue

        spec = {"type": "ticket_tier", "value": tier}
        # SQL-level skip for leads already in the target campaign — keeps
        # each iteration cheap so the operator can re-run the loop safely
        # if a previous run was interrupted (network, rate limit, etc.).
        total_in_tier = backend.count_leads_by_filter(
            spec, exclude_in_active_campaign=False,
        )
        leads = backend.fetch_leads_by_filter(
            spec,
            exclude_in_active_campaign=False,
            exclude_already_in_campaign_id=c_id,
        )
        already_in_place = max(total_in_tier - len(leads), 0)

        if on_tier_start is not None:
            try:
                on_tier_start(tier, len(leads), already_in_place, c_id)
            except Exception:
                pass

        def _tier_progress(done: int, total: int, phase: str) -> None:
            if on_progress is not None:
                try:
                    on_progress(tier, done, total, phase)
                except Exception:
                    pass

        push_result = push_leads_to_campaign(
            backend, api_key=api_key, leads=leads,
            campaign_id=c_id, debug=debug, max_workers=max_workers,
            on_progress=_tier_progress, log=log,
        )
        push_result["tier"] = tier
        push_result["campaign_id"] = c_id
        push_result["error"] = None
        # SQL excluded these so they don't show up in details, but the
        # operator should still see how many we left untouched.
        push_result["already_in_place"] = (
            push_result.get("already_in_place", 0) + already_in_place
        )
        by_tier[tier] = push_result

    aggregated = {
        "moved": 0, "created": 0, "skipped": 0, "failed": 0,
        "already_in_place": 0,
    }
    for r in by_tier.values():
        for k in aggregated:
            aggregated[k] += r.get(k, 0)
    aggregated["by_tier"] = by_tier
    return aggregated
