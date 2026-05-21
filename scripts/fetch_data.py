#!/usr/bin/env python3
"""
Lane 2 Activity Dashboard — Data Fetcher

Pulls Close CRM data and writes `data.json` for the Lane 2 Activity Dashboard.
Runs every 15 min Mon–Fri via GitHub Actions + cron-job.org (workflow_dispatch).

Two payloads:
  1. CALENDAR — for each day in the current month, count UNIQUE leads
     newly assigned to a Lane 2 rep that day. Dedupe per (lead, day) so a
     bounce A→B→A counts as 1. Excludes LTF - Quiz Funnel leads.
  2. REP DETAILS — per Lane 2 rep, a snapshot of: currently owned leads,
     Handraiser breakdown, MTD activities, leads with 0 comms ever,
     outbound calls/emails MTD, calls booked MTD, deals closed/lost MTD.

Output sorted by owned_leads desc.
"""

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# ============================================================================
# CONFIG
# ============================================================================

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
if not CLOSE_API_KEY:
    print("ERROR: CLOSE_API_KEY env var not set", file=sys.stderr)
    sys.exit(1)

CLOSE_BASE_URL = "https://api.close.com/api/v1"
TIMEZONE = ZoneInfo("America/Los_Angeles")
THROTTLE_SECONDS = 0.5  # gentle pacing between calls; matches sibling dashboards

# Lane 2 reps. Display order doesn't matter — output is sorted by owned_leads desc.
LANE_2_REPS = [
    {"name": "Bryan Barcus",    "user_id": "user_ulI4pdlkBQGJpFBjSfdf3U2deAXQATVPSAurnbL80T9"},
    {"name": "Cameron Caswell", "user_id": "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW"},
    {"name": "Elvis Ellis",     "user_id": "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr"},
    {"name": "Kelly Schrader",  "user_id": "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7"},
    {"name": "John Kirk",       "user_id": "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL"},
    {"name": "Lyle Hubbard",    "user_id": "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG"},
    {"name": "Jason Aaron",     "user_id": "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj"},
]
LANE_2_USER_IDS = {r["user_id"] for r in LANE_2_REPS}

# Close custom field IDs (verified from sibling dashboards)
FIELD_LEAD_OWNER         = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
FIELD_HANDRAISER         = "cf_Q1hRv8It46xsAEmpv4PRKdI1y0sPJnrnQrgRbIlF8uL"
FIELD_FUNNEL_NAME        = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
FIELD_FIRST_CALL_BOOKED  = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"

EXCLUDED_FUNNELS = {"LTF - Quiz Funnel"}

# ============================================================================
# HTTP SESSION
# ============================================================================

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")
session.headers.update({"Content-Type": "application/json"})


def close_get(path, params=None):
    """GET to Close API with throttle + simple retry on 429/5xx.

    On 4xx errors (other than 429), prints the response body before raising
    so we can see what Close actually rejected.
    """
    url = f"{CLOSE_BASE_URL}{path}"
    for attempt in range(3):
        r = session.get(url, params=params, timeout=30)
        time.sleep(THROTTLE_SECONDS)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            print(f"  rate-limited, sleeping {wait}s")
            time.sleep(wait)
            continue
        if 500 <= r.status_code < 600 and attempt < 2:
            print(f"  {r.status_code} from Close, retrying ({attempt + 1}/3)")
            time.sleep(2 ** attempt)
            continue
        if not r.ok:
            # Surface what Close actually said before raising — generic
            # raise_for_status() hides the body.
            print(f"  !! {r.status_code} {r.reason}  url: {r.url}")
            try:
                print(f"  body: {json.dumps(r.json(), indent=2)[:2000]}")
            except Exception:
                print(f"  body: {r.text[:2000]}")
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Close API failed after 3 attempts: {path}")


def close_paginate_skip(path, params=None):
    """Yield items across pages for endpoints using _skip pagination."""
    params = dict(params or {})
    params.setdefault("_limit", 100)
    skip = 0
    while True:
        params["_skip"] = skip
        data = close_get(path, params)
        items = data.get("data", [])
        for item in items:
            yield item
        if not data.get("has_more") or not items:
            return
        skip += len(items)


def close_paginate_cursor(path, params=None):
    """Yield items across pages for endpoints using _cursor pagination (e.g. /event/).

    The /event/ endpoint caps `_limit` at 50 per its docs, so default to 50 here.
    """
    params = dict(params or {})
    params.setdefault("_limit", 50)
    cursor = None
    while True:
        if cursor:
            params["_cursor"] = cursor
        data = close_get(path, params)
        for item in data.get("data", []):
            yield item
        cursor = data.get("cursor_next")
        if not cursor:
            return


# ============================================================================
# DATA HELPERS
# ============================================================================

def get_custom(payload, field_id):
    """
    Read a Close custom field from a payload that may use either
    flat keys ('custom.cf_xxx') or a nested dict ({'custom': {'cf_xxx': ...}}).

    Close's REST responses store custom fields as flat top-level keys (e.g.
    `"custom.cf_gOfS9pFw…": "user_xxx"`). The nested-dict form is included as
    a defensive fallback in case any endpoint serializes differently.
    """
    if not payload:
        return None
    v = payload.get(f"custom.{field_id}")
    if v is not None:
        return v
    nested = payload.get("custom")
    if isinstance(nested, dict):
        return nested.get(field_id)
    return None


# ============================================================================
# DATE HELPERS
# ============================================================================

def now_pt():
    return datetime.now(TIMEZONE)


def month_bounds(now=None):
    """Return (month_start_pt, month_end_pt_exclusive, 'YYYY-MM')."""
    now = now or now_pt()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end, start.strftime("%Y-%m")


def days_in_month(month_start, month_end):
    days, d, end_d = [], month_start.date(), month_end.date()
    while d < end_d:
        days.append(d)
        d += timedelta(days=1)
    return days


# ============================================================================
# CALENDAR — leads assigned to Lane 2 reps per day
# ============================================================================

def fetch_calendar(month_start, month_end):
    """
    Walks /event/ for object_type=lead, action=updated, in the month window.
    Counts a (lead, day) only when the lead-owner custom field changed to a
    Lane 2 rep. Then filters LTF - Quiz Funnel via per-lead funnel lookup.
    Dedupes per lead per calendar day (PT).

    Returns dict[YYYY-MM-DD] -> int.
    """
    print(f"[calendar] scanning lead.updated events "
          f"{month_start.date()} → {month_end.date()}")

    candidates = []         # list of (pt_date_iso, lead_id)
    lead_ids = set()        # for funnel lookup

    # Close /event/ filters by `date_updated`. Upper bound is unnecessary
    # because we always query the current month and the script runs in real
    # time — events from past months won't satisfy `date_updated__gte=<month_start>`.
    # Use Z-suffixed UTC (matches Close's documented format).
    params = {
        "object_type": "lead",
        "action": "updated",
        "date_updated__gte": month_start.astimezone(timezone.utc)
                                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    page_count = 0
    for ev in close_paginate_cursor("/event/", params):
        page_count += 1
        new_owner  = get_custom(ev.get("data"),          FIELD_LEAD_OWNER)
        prev_owner = get_custom(ev.get("previous_data"), FIELD_LEAD_OWNER)

        # Must be an actual owner change AND new owner must be Lane 2
        if new_owner == prev_owner:
            continue
        if new_owner not in LANE_2_USER_IDS:
            continue

        lead_id = ev.get("lead_id") or (ev.get("data") or {}).get("id")
        if not lead_id:
            continue

        # Events carry both date_created (original) and date_updated (latest
        # action — Close consolidates close-in-time updates). Use date_updated
        # to bucket the lead into the day the latest assignment landed on.
        ts = ev.get("date_updated") or ev.get("date_created")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        pt_date = dt.astimezone(TIMEZONE).date().isoformat()

        candidates.append((pt_date, lead_id))
        lead_ids.add(lead_id)

    print(f"[calendar] scanned {page_count} events → "
          f"{len(candidates)} candidates / {len(lead_ids)} unique leads")

    # Funnel lookup — one call per unique lead to filter LTF - Quiz Funnel
    excluded = set()
    if lead_ids:
        print(f"[calendar] funnel lookup for {len(lead_ids)} leads")
        for lid in lead_ids:
            try:
                ld = close_get(f"/lead/{lid}/",
                               {"_fields": f"id,custom.{FIELD_FUNNEL_NAME}"})
            except requests.HTTPError:
                # lead might be deleted; skip it (= include it, no funnel info)
                continue
            funnel = get_custom(ld, FIELD_FUNNEL_NAME)
            if funnel in EXCLUDED_FUNNELS:
                excluded.add(lid)
        print(f"[calendar] excluded {len(excluded)} LTF Quiz Funnel leads")

    # Dedupe per (lead, day), drop excluded leads
    per_day = defaultdict(set)
    for pt_date, lid in candidates:
        if lid in excluded:
            continue
        per_day[pt_date].add(lid)

    counts = {d: len(s) for d, s in per_day.items()}
    print(f"[calendar] days with activity: {len(counts)}")
    return counts


# ============================================================================
# REP DETAILS
# ============================================================================

def fetch_owned_leads(user_id):
    """Currently owned leads with handraiser + times_communicated."""
    leads = []
    q = f'custom.{FIELD_LEAD_OWNER}:"{user_id}"'
    for ld in close_paginate_skip("/lead/", {
        "query": q,
        # NOTE: times_communicated is the standard Close aggregate-comm counter.
        # If your Close env returns null here, swap to an /activity/ fallback.
        "_fields": f"id,times_communicated,custom.{FIELD_HANDRAISER}",
    }):
        leads.append({
            "id": ld.get("id"),
            "handraiser": get_custom(ld, FIELD_HANDRAISER),
            "times_communicated": ld.get("times_communicated") or 0,
        })
    return leads


def fetch_activity_mtd(user_id, month_start):
    """Total activities, outbound calls, outbound emails for this rep MTD."""
    since_iso = month_start.astimezone(timezone.utc).isoformat()
    total, ob_calls, ob_emails = 0, 0, 0
    for act in close_paginate_skip("/activity/", {
        "user_id": user_id,
        "date_created__gte": since_iso,
    }):
        total += 1
        atype = act.get("_type")
        direction = act.get("direction")
        if direction == "outbound":
            if atype == "Call":
                ob_calls += 1
            elif atype == "Email":
                ob_emails += 1
    return total, ob_calls, ob_emails


def fetch_calls_booked_mtd(user_id, month_start, month_end):
    """Leads where First Sales Call Booked Date in MTD AND owner = rep."""
    start_d = month_start.date().isoformat()
    end_d   = (month_end - timedelta(seconds=1)).date().isoformat()
    q = (
        f'custom.{FIELD_LEAD_OWNER}:"{user_id}" '
        f'custom.{FIELD_FIRST_CALL_BOOKED}>="{start_d}" '
        f'custom.{FIELD_FIRST_CALL_BOOKED}<="{end_d}"'
    )
    count = 0
    for _ in close_paginate_skip("/lead/", {"query": q, "_fields": "id"}):
        count += 1
    return count


def fetch_deals_mtd(user_id, month_start, month_end):
    """(won_count, lost_count) for opportunities owned by this rep MTD."""
    start_iso = month_start.astimezone(timezone.utc).isoformat()
    end_iso   = month_end.astimezone(timezone.utc).isoformat()

    won = sum(1 for _ in close_paginate_skip("/opportunity/", {
        "user_id": user_id,
        "status_type": "won",
        "date_won__gte": start_iso,
        "date_won__lt":  end_iso,
    }))
    lost = sum(1 for _ in close_paginate_skip("/opportunity/", {
        "user_id": user_id,
        "status_type": "lost",
        "date_lost__gte": start_iso,
        "date_lost__lt":  end_iso,
    }))
    return won, lost


def build_rep_row(rep, month_start, month_end):
    print(f"[rep] {rep['name']}")
    uid = rep["user_id"]

    leads = fetch_owned_leads(uid)
    handraiser_counts = defaultdict(int)
    zero_comm = 0
    for ld in leads:
        handraiser_counts[ld["handraiser"] or "(unset)"] += 1
        if ld["times_communicated"] == 0:
            zero_comm += 1

    total_acts, ob_calls, ob_emails = fetch_activity_mtd(uid, month_start)
    calls_booked = fetch_calls_booked_mtd(uid, month_start, month_end)
    won, lost = fetch_deals_mtd(uid, month_start, month_end)

    return {
        "user_id": uid,
        "name": rep["name"],
        "owned_leads": len(leads),
        "handraiser_breakdown": dict(sorted(
            handraiser_counts.items(), key=lambda kv: kv[1], reverse=True
        )),
        "activities_mtd": total_acts,
        "leads_zero_comms": zero_comm,
        "outbound_calls_mtd": ob_calls,
        "outbound_emails_mtd": ob_emails,
        "calls_booked_mtd": calls_booked,
        "deals_closed_mtd": won,
        "deals_lost_mtd": lost,
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    started = time.time()
    now = now_pt()
    month_start, month_end, month_label = month_bounds(now)
    print(f"Run: {now.isoformat()}")
    print(f"Month: {month_label} ({month_start.date()} → "
          f"{(month_end - timedelta(days=1)).date()})\n")

    calendar_counts = fetch_calendar(month_start, month_end)

    # Backfill every day in the month with 0 so the calendar grid is complete
    full_calendar = {
        d.isoformat(): calendar_counts.get(d.isoformat(), 0)
        for d in days_in_month(month_start, month_end)
    }

    print()
    reps = [build_rep_row(r, month_start, month_end) for r in LANE_2_REPS]
    reps.sort(key=lambda x: x["owned_leads"], reverse=True)

    output = {
        "generated_at": now.isoformat(),
        "month": month_label,
        "month_start": month_start.date().isoformat(),
        "month_end": (month_end - timedelta(days=1)).date().isoformat(),
        "calendar": full_calendar,
        "reps": reps,
    }

    # Write live data + monthly archive
    repo_root = Path(__file__).resolve().parent.parent
    (repo_root / "data.json").write_text(json.dumps(output, indent=2))
    print(f"\nwrote {repo_root / 'data.json'}")

    archives = repo_root / "archives"
    archives.mkdir(exist_ok=True)
    archive_path = archives / f"data_{month_label}.json"
    archive_path.write_text(json.dumps(output, indent=2))
    print(f"wrote {archive_path}")

    print(f"\nDone in {time.time() - started:.1f}s")


if __name__ == "__main__":
    main()
