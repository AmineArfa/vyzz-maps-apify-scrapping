#!/usr/bin/env python3
"""
Enrich existing Instantly leads with their own system ID as a 'lid' custom variable.

This makes {{lid}} available as a merge variable in email templates for
closed-loop click tracking: email link -> ?lid={{lid}} -> vyzz.io/audit -> Airtable.

Usage:
    python enrich_lid.py [--dry-run] [--max N] [--workers N]

The script:
1. Lists ALL leads from Instantly in batches of 100
2. For each lead without 'lid' in payload, updates custom_variables to include lid
3. Handles rate limiting (429) with exponential backoff
4. Reports progress and results
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

# ── Config ──────────────────────────────────────────────────────────────
API_KEY = "MGFjM2JlNjUtMmFmNy00ZWEwLTgxZGItMTg3OTNlYjlkYTQ3OlhBSWxaRmVkUFlySA=="
BASE_URL = "https://api.instantly.ai"
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}


def request_with_retry(method: str, url: str, json_payload: dict | None = None,
                       retries: int = 5, backoff: float = 1.0) -> requests.Response:
    """HTTP request with retry on 429 and 5xx."""
    for attempt in range(retries + 1):
        try:
            resp = requests.request(method, url, headers=HEADERS, json=json_payload, timeout=30)
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", backoff * (2 ** attempt)))
                print(f"  ⏳ Rate limited, waiting {wait:.1f}s...")
                time.sleep(min(wait, 60))
                continue
            if 500 <= resp.status_code < 600 and attempt < retries:
                time.sleep(backoff * (2 ** attempt))
                continue
            return resp
        except Exception as e:
            if attempt >= retries:
                raise
            time.sleep(backoff * (2 ** attempt))
    raise RuntimeError("Retry loop ended unexpectedly")


def list_all_leads() -> list[dict]:
    """Fetch ALL leads from Instantly using cursor-based pagination."""
    all_leads = []
    cursor = None
    page = 0

    while True:
        page += 1
        payload: dict[str, Any] = {"limit": 100}
        if cursor:
            payload["starting_after"] = cursor

        resp = request_with_retry("POST", f"{BASE_URL}/api/v2/leads/list", json_payload=payload)
        if resp.status_code != 200:
            print(f"❌ List leads failed: {resp.status_code} - {resp.text[:200]}")
            break

        data = resp.json()
        items = data.get("items", [])
        if not items:
            break

        all_leads.extend(items)
        print(f"  📥 Page {page}: fetched {len(items)} leads (total: {len(all_leads)})")

        # Cursor-based pagination
        pagination = data.get("pagination", {})
        next_cursor = pagination.get("next_starting_after") if isinstance(pagination, dict) else None
        if not next_cursor:
            # Fallback: check top-level next_starting_after
            next_cursor = data.get("next_starting_after")
        if not next_cursor:
            break
        cursor = next_cursor

    return all_leads


def enrich_lead(lead: dict, dry_run: bool = False) -> dict:
    """Add lid to a single lead's custom_variables. Returns result dict."""
    lead_id = lead.get("id")
    payload = lead.get("payload") or {}

    # Already has lid — skip
    if payload.get("lid") == lead_id:
        return {"id": lead_id, "status": "skipped", "reason": "already_has_lid"}

    if dry_run:
        return {"id": lead_id, "status": "dry_run", "reason": "would_add_lid"}

    # Merge lid into existing payload
    merged = {**payload, "lid": lead_id}

    resp = request_with_retry("PATCH", f"{BASE_URL}/api/v2/leads/{lead_id}",
                              json_payload={"custom_variables": merged})
    if resp.status_code == 200:
        return {"id": lead_id, "status": "updated"}
    else:
        return {"id": lead_id, "status": "error", "reason": f"{resp.status_code}: {resp.text[:100]}"}


def main():
    parser = argparse.ArgumentParser(description="Enrich Instantly leads with lid custom variable")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually update, just report")
    parser.add_argument("--max", type=int, default=0, help="Max leads to process (0 = all)")
    parser.add_argument("--workers", type=int, default=3, help="Concurrent workers (default: 3)")
    args = parser.parse_args()

    print("=" * 60)
    print("Instantly Lead Enrichment — Add 'lid' to custom_variables")
    print("=" * 60)
    if args.dry_run:
        print("🔍 DRY RUN — no changes will be made")
    print()

    # Step 1: List all leads
    print("📋 Fetching all leads from Instantly...")
    leads = list_all_leads()
    print(f"\n✅ Total leads fetched: {len(leads)}")

    # Step 2: Filter to leads needing lid
    needs_lid = [l for l in leads if (l.get("payload") or {}).get("lid") != l.get("id")]
    already_done = len(leads) - len(needs_lid)
    print(f"   Already have lid: {already_done}")
    print(f"   Need lid injection: {len(needs_lid)}")

    if args.max and args.max < len(needs_lid):
        needs_lid = needs_lid[:args.max]
        print(f"   Capped to --max={args.max}")

    if not needs_lid:
        print("\n✅ All leads already have lid! Nothing to do.")
        return

    # Step 3: Enrich
    print(f"\n🚀 Enriching {len(needs_lid)} leads with {args.workers} workers...")
    updated = 0
    skipped = 0
    errors = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(enrich_lead, lead, args.dry_run): lead
            for lead in needs_lid
        }
        for future in as_completed(futures):
            result = future.result()
            completed += 1

            if result["status"] == "updated" or result["status"] == "dry_run":
                updated += 1
            elif result["status"] == "skipped":
                skipped += 1
            else:
                errors += 1
                print(f"  ❌ {result['id']}: {result.get('reason', 'unknown error')}")

            if completed % 100 == 0 or completed == len(needs_lid):
                pct = (completed / len(needs_lid)) * 100
                print(f"  Progress: {completed}/{len(needs_lid)} ({pct:.1f}%) — "
                      f"updated={updated} skipped={skipped} errors={errors}")

    # Summary
    print()
    print("=" * 60)
    print("ENRICHMENT COMPLETE")
    print("=" * 60)
    print(f"  Total processed: {completed}")
    print(f"  Updated:         {updated}")
    print(f"  Skipped:         {skipped}")
    print(f"  Errors:          {errors}")
    if args.dry_run:
        print("\n  (DRY RUN — no actual changes were made)")


if __name__ == "__main__":
    main()
