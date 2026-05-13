#!/usr/bin/env python3
"""One-shot backfill: write `industry2` into every Instantly lead's
custom_variables, preserving every other key Instantly already has.

Why this exists
---------------
We added the `industry2` field on 2026-05-10. Going forward, the scraper
writes it on INSERT and the campaign-push pipeline includes it in the
custom_variables for new leads. But ~23.5k leads were pushed to Instantly
*before* this change and have no industry2 server-side, so the
{{industry2}} merge variable would render empty in templates.

This script enumerates Instantly directly — NOT raw.scraped_leads — to
catch leads whose raw row lost its `instantly_lead_id` link (orphan-
satellite drift, see BRIEF.md). For every lead with a non-empty
`payload.industry`, we look up the canonical industry2 via the same map
as leadgen/industry2.py and PATCH it in.

Behavior
--------
- For each lead: read its current `payload` (Instantly's name for
  custom_variables). If industry2 already equals the mapped target,
  skip — no API call.
- Otherwise PATCH /leads/{id} with merged custom_variables. Every
  pre-existing key (lid, industry, ticket_tier, postalCode, …) is
  preserved; only industry2 is added/overwritten.
- Industries not in our map fall through to "skip" with reason "unmapped"
  — we never fabricate a label.
- 429s are retried with backoff. Failures don't abort the run.

Usage
-----
    python3 backfill_industry2_to_instantly.py \\
      [--dry-run] [--max N] [--workers 8] [--quiet]

The default workers (8) keeps API throughput high without tripping
Instantly's rate limit. Bump up if you have headroom.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

# Reuse the canonical map from the production module so this script and
# the live insert-path can never diverge.
sys.path.insert(0, str(Path(__file__).parent))
from leadgen.industry2 import INDUSTRY2_BY_INDUSTRY, compute_industry2  # noqa: E402

BASE_URL = "https://api.instantly.ai"


def _resolve_api_key() -> str:
    """Pick up the Instantly key from env, or fall back to enrich_lid.py
    which already hard-codes it (same key, same repo, same operator)."""
    key = os.environ.get("INSTANTLY_API_KEY")
    if key:
        return key
    here = Path(__file__).parent
    enrich = (here / "enrich_lid.py").read_text(encoding="utf-8")
    for line in enrich.splitlines():
        line = line.strip()
        if line.startswith("API_KEY = "):
            quoted = line.split("=", 1)[1].strip()
            return quoted.strip('"').strip("'")
    raise RuntimeError("INSTANTLY_API_KEY not in env and enrich_lid.py has no API_KEY")


API_KEY = _resolve_api_key()
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}


def request_with_retry(
    method: str,
    url: str,
    *,
    json_payload: dict | None = None,
    retries: int = 5,
    backoff: float = 1.0,
    timeout: int = 30,
) -> requests.Response:
    """HTTP with retry on 429 and 5xx."""
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = requests.request(
                method, url, headers=HEADERS, json=json_payload, timeout=timeout,
            )
            if resp.status_code == 429:
                wait = resp.headers.get("Retry-After")
                try:
                    wait_s = float(wait) if wait else backoff * (2**attempt)
                except ValueError:
                    wait_s = backoff * (2**attempt)
                time.sleep(min(wait_s, 60))
                continue
            if 500 <= resp.status_code < 600 and attempt < retries:
                time.sleep(min(backoff * (2**attempt), 30))
                continue
            return resp
        except Exception as e:
            last_exc = e
            if attempt >= retries:
                raise
            time.sleep(min(backoff * (2**attempt), 30))
    if last_exc:
        raise last_exc
    raise RuntimeError("retry loop ended unexpectedly")


# ── List all Instantly leads via cursor pagination ──────────────────────


def list_all_leads(*, page_size: int = 100, max_total: int | None = None,
                   on_progress=None) -> list[dict]:
    """Walk every lead in the Instantly account.

    POST /api/v2/leads/list with cursor `starting_after`. Returns a list
    of lead dicts (each with id, payload, etc.). `on_progress(loaded)` is
    called after each page so the caller can print a counter.
    """
    leads: list[dict] = []
    starting_after: str | None = None
    page = 0

    while True:
        page += 1
        body: dict[str, Any] = {"limit": page_size}
        if starting_after:
            body["starting_after"] = starting_after

        resp = request_with_retry("POST", f"{BASE_URL}/api/v2/leads/list",
                                  json_payload=body)
        if resp.status_code != 200:
            print(f"❌ list page {page} failed: {resp.status_code} {resp.text[:200]}",
                  file=sys.stderr)
            break

        data = resp.json()
        items = data.get("items", [])
        if not items:
            break
        leads.extend(items)
        if on_progress is not None:
            on_progress(len(leads))

        if max_total is not None and len(leads) >= max_total:
            return leads[:max_total]

        next_cursor = data.get("next_starting_after")
        if next_cursor:
            starting_after = next_cursor
        elif len(items) < page_size:
            break
        else:
            starting_after = items[-1].get("id")
            if not starting_after:
                break

    return leads


# ── Per-lead worker ─────────────────────────────────────────────────────


def _process_one(lead: dict, *, dry_run: bool) -> dict:
    """Return {lead_id, op: skip|patch|fail, reason}.

    `lead` is the lead dict from /leads/list — already has payload populated,
    so we don't need a second GET.
    """
    lead_id = lead.get("id")
    cv = lead.get("payload") or {}
    if not isinstance(cv, dict):
        cv = {}

    industry = cv.get("industry")
    if not industry:
        return {"lead_id": lead_id, "op": "skip", "reason": "no industry"}

    target = compute_industry2(industry)
    if not target:
        return {"lead_id": lead_id, "op": "skip",
                "reason": f"unmapped industry: {industry!r}"}

    if cv.get("industry2") == target:
        return {"lead_id": lead_id, "op": "skip", "reason": "already set"}

    if dry_run:
        return {"lead_id": lead_id, "op": "patch", "reason": "dry-run"}

    merged = {**cv, "industry2": target}
    patch_resp = request_with_retry(
        "PATCH",
        f"{BASE_URL}/api/v2/leads/{lead_id}",
        json_payload={"custom_variables": merged},
    )
    if patch_resp.status_code == 200:
        return {"lead_id": lead_id, "op": "patch", "reason": None}
    return {
        "lead_id": lead_id, "op": "fail",
        "reason": f"PATCH {patch_resp.status_code}: {patch_resp.text[:200]}",
    }


# ── Driver ──────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="GET + log only, no PATCH")
    ap.add_argument("--max", type=int, default=None,
                    help="Stop after listing N leads")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--quiet", action="store_true",
                    help="Suppress per-batch progress lines")
    args = ap.parse_args()

    print(f"🔎 Loaded {len(INDUSTRY2_BY_INDUSTRY)} industry → industry2 mappings")
    print("📥 Listing all leads from Instantly (paged)...")

    def _on_load(n: int) -> None:
        if not args.quiet:
            print(f"  loaded {n} leads...", end="\r", file=sys.stderr)

    listed = list_all_leads(page_size=100, max_total=args.max,
                            on_progress=_on_load)
    print(f"📥 Total leads listed: {len(listed)}")

    if not listed:
        return 0

    print(f"🚀 Patching missing industry2 (dry-run={args.dry_run}, "
          f"workers={args.workers})")

    counts = {"patch": 0, "skip": 0, "fail": 0}
    skip_reasons: dict[str, int] = {}
    failures: list[dict] = []
    start = time.monotonic()
    total = len(listed)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(_process_one, l, dry_run=args.dry_run): l for l in listed}
        done = 0
        for fut in as_completed(futures):
            r = fut.result()
            counts[r["op"]] = counts.get(r["op"], 0) + 1
            if r["op"] == "skip":
                reason = r.get("reason") or "unknown"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            if r["op"] == "fail":
                failures.append(r)
            done += 1
            if not args.quiet and (done % 250 == 0 or done == total):
                elapsed = time.monotonic() - start
                rate = done / max(elapsed, 1e-6)
                eta = (total - done) / max(rate, 1e-6)
                print(
                    f"  {done}/{total} "
                    f"(patch={counts.get('patch', 0)} "
                    f"skip={counts.get('skip', 0)} "
                    f"fail={counts.get('fail', 0)}) "
                    f"{rate:.1f}/s eta={eta:.0f}s"
                )

    print("📊 Final counts:", counts)
    if skip_reasons:
        print("📊 Skip breakdown:")
        for reason, n in sorted(skip_reasons.items(), key=lambda kv: -kv[1]):
            print(f"  - {n}: {reason}")
    if failures:
        print(f"⚠️ {len(failures)} failures (showing up to 10):")
        for f in failures[:10]:
            print(f"  - {f['lead_id'][:8]}…: {f['reason']}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
