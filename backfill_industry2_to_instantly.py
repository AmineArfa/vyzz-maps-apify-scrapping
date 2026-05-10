#!/usr/bin/env python3
"""One-shot backfill: write `industry2` into every existing Instantly lead's
custom_variables, preserving every other key Instantly already has.

Why this exists
---------------
We added the `industry2` field on 2026-05-10. Going forward, the scraper
writes it on INSERT and the campaign-push pipeline includes it in the
custom_variables for new leads. But ~23.5k leads were pushed to Instantly
*before* this change. They have no industry2 server-side, so the
{{industry2}} merge variable would render empty in templates for them.
This script walks every linked lead and PATCHes industry2 in.

Input
-----
A JSONL file (one record per line) with at minimum:

    {"instantly_lead_id": "uuid", "industry2": "Restaurant"}

Rows whose `industry2` is empty/null are skipped (NULL stays NULL — the
mapping never fabricates labels).

Source of truth
---------------
The data file is dumped directly from raw.scraped_leads via the Supabase
MCP — see the dump query in the runbook below. We don't connect to the
database from here so the script needs no DB credentials.

Behavior
--------
- For each row we GET the lead from Instantly to read its current
  custom_variables (Instantly returns them under "payload"). If
  industry2 already equals the target value, we skip — no API call.
- Otherwise we PATCH /leads/{id} with the merged custom_variables. Every
  pre-existing key (lid, industry, ticket_tier, postalCode, …) is
  preserved; only industry2 is added/overwritten.
- 429s are retried with backoff. Failures are logged but don't abort
  the run.

Usage
-----
    python3 backfill_industry2_to_instantly.py \\
      --data-file /tmp/industry2_backfill_data.jsonl \\
      [--dry-run] [--max N] [--workers 5] [--quiet]

The default workers (5) matches the rest of the codebase (see
campaign_push). Bump up if Instantly rate-limit headroom allows.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

# ── Config ──────────────────────────────────────────────────────────────
BASE_URL = "https://api.instantly.ai"


def _resolve_api_key() -> str:
    """Pick up the Instantly key from env, or fall back to enrich_lid.py
    which already hard-codes it (same key, same repo, same operator)."""
    key = os.environ.get("INSTANTLY_API_KEY")
    if key:
        return key
    # Fallback: parse out the API_KEY constant from enrich_lid.py without
    # importing it (the file has top-level imports we don't need here).
    here = Path(__file__).parent
    enrich = (here / "enrich_lid.py").read_text(encoding="utf-8")
    for line in enrich.splitlines():
        line = line.strip()
        if line.startswith("API_KEY = "):
            quoted = line.split("=", 1)[1].strip()
            return quoted.strip('"').strip("'")
    raise RuntimeError(
        "INSTANTLY_API_KEY not in env and enrich_lid.py has no API_KEY constant."
    )


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
    """HTTP with retry on 429 and 5xx. Same pattern as enrich_lid.py."""
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


# ── Per-lead worker ─────────────────────────────────────────────────────


def _process_one(row: dict, *, dry_run: bool) -> dict:
    """Return {lead_id, op: skip|patch|fail, reason}."""
    lead_id = row.get("instantly_lead_id")
    target = row.get("industry2")
    if not lead_id or not target:
        return {"lead_id": lead_id, "op": "skip", "reason": "no lead_id or industry2"}

    # 1. Fetch current lead.
    get_resp = request_with_retry("GET", f"{BASE_URL}/api/v2/leads/{lead_id}")
    if get_resp.status_code == 404:
        return {"lead_id": lead_id, "op": "skip", "reason": "404 lead gone"}
    if get_resp.status_code != 200:
        return {
            "lead_id": lead_id, "op": "fail",
            "reason": f"GET {get_resp.status_code}: {get_resp.text[:200]}",
        }

    lead = get_resp.json() or {}
    existing_vars = lead.get("payload") or {}
    if not isinstance(existing_vars, dict):
        existing_vars = {}

    # 2. Idempotent — skip if already correct.
    if existing_vars.get("industry2") == target:
        return {"lead_id": lead_id, "op": "skip", "reason": "already set"}

    # 3. PATCH with merged custom_variables.
    merged = {**existing_vars, "industry2": target}
    if dry_run:
        return {"lead_id": lead_id, "op": "patch", "reason": "dry-run"}

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


def _load_rows(path: Path, max_rows: int | None) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            lead_id = rec.get("instantly_lead_id")
            industry2 = rec.get("industry2")
            if not lead_id or not industry2:
                continue
            rows.append({"instantly_lead_id": lead_id, "industry2": industry2})
            if max_rows is not None and len(rows) >= max_rows:
                break
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-file", required=True, type=Path,
                    help="JSONL with {instantly_lead_id, industry2} per line")
    ap.add_argument("--dry-run", action="store_true",
                    help="GET + log only, no PATCH")
    ap.add_argument("--max", type=int, default=None,
                    help="Stop after N candidate rows (post-load filter)")
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--quiet", action="store_true",
                    help="Suppress per-batch progress lines")
    args = ap.parse_args()

    if not args.data_file.exists():
        print(f"❌ data file not found: {args.data_file}", file=sys.stderr)
        return 2

    rows = _load_rows(args.data_file, args.max)
    total = len(rows)
    if not total:
        print("ℹ️ No rows to process.")
        return 0

    print(f"🚀 Backfilling industry2 on {total} Instantly leads "
          f"(dry-run={args.dry_run}, workers={args.workers})")

    counts = {"patch": 0, "skip": 0, "fail": 0}
    failures: list[dict] = []
    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(_process_one, r, dry_run=args.dry_run): r for r in rows}
        done = 0
        for fut in as_completed(futures):
            r = fut.result()
            counts[r["op"]] = counts.get(r["op"], 0) + 1
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
    if failures:
        print(f"⚠️ {len(failures)} failures (showing up to 10):")
        for f in failures[:10]:
            print(f"  - {f['lead_id'][:8]}…: {f['reason']}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
