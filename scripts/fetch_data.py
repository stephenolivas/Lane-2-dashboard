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
    print("ERROR: CLOSE_API_KEY env var not set", file=sys.stderr, flush=True)
    sys.exit(1)

CLOSE_BASE_URL = "https://api.close.com/api/v1"
TIMEZONE = ZoneInfo("America/Los_Angeles")
THROTTLE_SECONDS = 0.1  # Close allows ~60 req/sec; 0.1s = 10 req/sec is safe

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
            print(f"  rate-limited, sleeping {wait}s", flush=True)
            time.sleep(wait)
            continue
        if 500 <= r.status_code < 600 and attempt < 2:
            print(f"  {r.status_code} from Close, retrying ({attempt + 1}/3)", flush=True)
            time.sleep(2 ** attempt)
            continue
        if not r.ok:
            # Surface what Close actually said before raising — generic
            # raise_for_status() hides the body.
            print(f"  !! {r.status_code} {r.reason}  url: {r.url}", flush=True)
            try:
                print(f"  body: {json.dumps(r.json(), indent=2)[:2000]}", flush=True)
            except Exception:
                print(f"  body: {r.text[:2000]}", flush=True)
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


def close_count(path, params=None):
    """Cheap count of how many items match a list query.

    Fast path: call with _limit=1 and read `total_results` from the response.
    Works for /lead/ and /opportunity/.

    Slow path: type-specific activity endpoints (/activity/call/, /activity/email/, etc.)
    do NOT return `total_results`. When it's missing we re-paginate the endpoint
    fully and count items. Returning len(data) from the _limit=1 response would
    incorrectly give 1 for any non-empty result.
    """
    p_fast = dict(params or {})
    p_fast["_limit"] = 1
    data = close_get(path, p_fast)
    total = data.get("total_results")
    if total is not None:
        return total
    # Slow path: paginate the original params (without our _limit=1 override).
    return sum(1 for _ in close_paginate_skip(path, dict(params or {})))


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
    Walks /event/ for object_type=lead, action=updated, since month_start.
    For each event whose lead-owner custom field changed to a Lane 2 rep AND
    whose lead is not on the LTF Quiz Funnel, records (lead_id, PT calendar day).
    Funnel + handraiser are read from the event's `data` payload — no separate lookups.
    Dedupes per (lead, day) so a same-day bounce counts once.

    Returns (counts, breakdowns):
      counts:     dict[YYYY-MM-DD] -> int    (total leads that day)
      breakdowns: dict[YYYY-MM-DD] -> dict[handraiser_value] -> int
                  (per-day handraiser breakdown; "(unset)" used for missing values)
    """
    print(f"[calendar] scanning lead.updated events "
          f"{month_start.date()} → {month_end.date()}", flush=True)

    per_day = defaultdict(set)              # day_str -> set of lead_ids
    per_day_handraiser = defaultdict(dict)  # day_str -> {lead_id: handraiser}
    excluded_funnel_count = 0
    not_owner_change = 0
    not_lane2 = 0

    params = {
        "object_type": "lead",
        "action": "updated",
        "date_updated__gte": month_start.astimezone(timezone.utc)
                                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    event_count = 0
    for ev in close_paginate_cursor("/event/", params):
        event_count += 1
        if event_count % 1000 == 0:
            page_count = event_count // 50
            print(f"  [calendar] scanned {event_count} events "
                  f"(~{page_count} pages), {sum(len(s) for s in per_day.values())} qualifying so far",
                  flush=True)

        data = ev.get("data") or {}
        new_owner  = get_custom(data,                   FIELD_LEAD_OWNER)
        prev_owner = get_custom(ev.get("previous_data"), FIELD_LEAD_OWNER)

        if new_owner == prev_owner:
            not_owner_change += 1
            continue
        if new_owner not in LANE_2_USER_IDS:
            not_lane2 += 1
            continue

        funnel = get_custom(data, FIELD_FUNNEL_NAME)
        if funnel in EXCLUDED_FUNNELS:
            excluded_funnel_count += 1
            continue

        lead_id = ev.get("lead_id") or data.get("id")
        if not lead_id:
            continue

        ts = ev.get("date_updated") or ev.get("date_created")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        pt_date = dt.astimezone(TIMEZONE).date().isoformat()

        if lead_id in per_day[pt_date]:
            continue   # dedupe: lead already counted for this day
        per_day[pt_date].add(lead_id)

        handraiser = get_custom(data, FIELD_HANDRAISER) or "(unset)"
        per_day_handraiser[pt_date][lead_id] = handraiser

    print(f"[calendar] scanned {event_count} events total · "
          f"{not_owner_change} non-owner-change · "
          f"{not_lane2} non-Lane2 owner · "
          f"{excluded_funnel_count} LTF excluded",
          flush=True)

    counts = {d: len(s) for d, s in per_day.items()}
    breakdowns = {}
    for day, lead_handraisers in per_day_handraiser.items():
        bd = defaultdict(int)
        for h in lead_handraisers.values():
            bd[h] += 1
        # Sort by count desc with (unset) last for stable rendering
        ordered = sorted(
            bd.items(),
            key=lambda kv: (kv[0] == "(unset)", -kv[1], kv[0])
        )
        breakdowns[day] = dict(ordered)

    print(f"[calendar] days with activity: {len(counts)} · "
          f"total qualifying leads: {sum(counts.values())}",
          flush=True)
    return counts, breakdowns


# ============================================================================
# REP DETAILS
# ============================================================================

def fetch_owned_leads(user_id):
    """Currently owned leads with handraiser + comm counter, excluding LTF Quiz Funnel."""
    leads = []
    q = f'custom.{FIELD_LEAD_OWNER}:"{user_id}"'
    fields = (
        f"id,times_communicated,"
        f"custom.{FIELD_HANDRAISER},"
        f"custom.{FIELD_FUNNEL_NAME}"
    )
    for ld in close_paginate_skip("/lead/", {"query": q, "_fields": fields}):
        funnel = get_custom(ld, FIELD_FUNNEL_NAME)
        if funnel in EXCLUDED_FUNNELS:
            continue
        leads.append({
            "id": ld.get("id"),
            "handraiser": get_custom(ld, FIELD_HANDRAISER),
            "times_communicated": ld.get("times_communicated") or 0,
        })
    return leads


def fetch_activity_mtd(user_id, month_start):
    """Total activities, outbound calls, outbound emails for this rep MTD.

    The generic /activity/ endpoint rejects bare user_id filters
    ("You must provide a single 'lead_id' filter to use 'user_id'…").
    Type-specific activity endpoints (/activity/call/, /activity/email/, etc.)
    accept user_id directly, so we count each type and sum.

    "Total activities" sums the rep-driven communication types: calls, emails,
    SMS, notes, meetings. System-generated activities (status changes, task
    completions) are excluded since they don't reflect rep effort.
    """
    since_iso = month_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    base = {"user_id": user_id, "date_created__gte": since_iso}

    calls    = close_count("/activity/call/",    base)
    emails   = close_count("/activity/email/",   base)
    sms      = close_count("/activity/sms/",     base)
    notes    = close_count("/activity/note/",    base)
    meetings = close_count("/activity/meeting/", base)
    total = calls + emails + sms + notes + meetings

    # Outbound subsets. Close uses "outbound" for calls and "outgoing" for emails.
    ob_calls  = close_count("/activity/call/",  {**base, "direction": "outbound"})
    ob_emails = close_count("/activity/email/", {**base, "direction": "outgoing"})

    return total, ob_calls, ob_emails


def fetch_calls_booked_mtd(user_id, month_start, month_end):
    """Leads where First Sales Call Booked Date in MTD AND owner = rep, excluding LTF."""
    start_d = month_start.date().isoformat()
    end_d   = (month_end - timedelta(seconds=1)).date().isoformat()
    q = (
        f'custom.{FIELD_LEAD_OWNER}:"{user_id}" '
        f'custom.{FIELD_FIRST_CALL_BOOKED}>="{start_d}" '
        f'custom.{FIELD_FIRST_CALL_BOOKED}<="{end_d}"'
    )
    count = 0
    for ld in close_paginate_skip("/lead/", {
        "query": q,
        "_fields": f"id,custom.{FIELD_FUNNEL_NAME}",
    }):
        if get_custom(ld, FIELD_FUNNEL_NAME) in EXCLUDED_FUNNELS:
            continue
        count += 1
    return count


def fetch_deals_mtd(user_id, month_start, month_end):
    """(won_count, lost_count) for opportunities by this rep in MTD, excluding LTF.

    Close's /opportunity/ endpoint accepts `date_won__gte/lt` (server-side filter,
    works fine for won deals), but it does NOT accept `date_lost__gte/lt` as URL
    params — those filters are silently ignored, returning the rep's ENTIRE
    lifetime of lost opps. So for lost we paginate all and filter client-side
    on each opp's `date_lost` field.

    LTF exclusion is done by looking up each qualifying opp's lead funnel.
    """
    start_iso = month_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = month_end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ---- Won: date filter works server-side --------------------------------
    won_lead_ids = []
    for opp in close_paginate_skip("/opportunity/", {
        "user_id": user_id,
        "status_type": "won",
        "date_won__gte": start_iso,
        "date_won__lt":  end_iso,
        "_fields": "id,lead_id,date_won",
    }):
        if opp.get("lead_id"):
            won_lead_ids.append(opp["lead_id"])

    # ---- Lost: date filter doesn't work, must filter client-side ------------
    lost_lead_ids = []
    for opp in close_paginate_skip("/opportunity/", {
        "user_id": user_id,
        "status_type": "lost",
        "_fields": "id,lead_id,date_lost",
    }):
        date_lost = opp.get("date_lost")
        if not date_lost:
            continue
        try:
            dt = datetime.fromisoformat(date_lost.replace("Z", "+00:00"))
        except ValueError:
            continue
        # Compare in UTC. month_start is PT-tz-aware; .astimezone(utc) for apples-to-apples.
        dt_utc = dt.astimezone(timezone.utc)
        if not (month_start.astimezone(timezone.utc) <= dt_utc
                < month_end.astimezone(timezone.utc)):
            continue
        if opp.get("lead_id"):
            lost_lead_ids.append(opp["lead_id"])

    # ---- LTF exclusion: look up each unique lead's funnel once -------------
    unique_leads = set(won_lead_ids + lost_lead_ids)
    funnel_cache = {}
    for lid in unique_leads:
        try:
            ld = close_get(f"/lead/{lid}/",
                           {"_fields": f"id,custom.{FIELD_FUNNEL_NAME}"})
            funnel_cache[lid] = get_custom(ld, FIELD_FUNNEL_NAME)
        except requests.HTTPError:
            # lead may be deleted/merged; treat as unknown funnel (don't exclude)
            funnel_cache[lid] = None

    def _count_non_ltf(lead_id_list):
        return sum(1 for lid in lead_id_list
                   if funnel_cache.get(lid) not in EXCLUDED_FUNNELS)

    return _count_non_ltf(won_lead_ids), _count_non_ltf(lost_lead_ids)


def build_rep_row(rep, month_start, month_end):
    print(f"[rep] {rep['name']}", flush=True)
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
    print(f"Run: {now.isoformat()}", flush=True)
    print(f"Month: {month_label} ({month_start.date()} → "
          f"{(month_end - timedelta(days=1)).date()})\n", flush=True)

    calendar_counts, calendar_breakdowns = fetch_calendar(month_start, month_end)

    # Backfill every day in the month with 0 so the calendar grid is complete
    full_calendar = {
        d.isoformat(): calendar_counts.get(d.isoformat(), 0)
        for d in days_in_month(month_start, month_end)
    }

    print("", flush=True)
    reps = [build_rep_row(r, month_start, month_end) for r in LANE_2_REPS]
    reps.sort(key=lambda x: x["owned_leads"], reverse=True)

    output = {
        "generated_at": now.isoformat(),
        "month": month_label,
        "month_start": month_start.date().isoformat(),
        "month_end": (month_end - timedelta(days=1)).date().isoformat(),
        "calendar": full_calendar,
        "calendar_breakdowns": calendar_breakdowns,
        "reps": reps,
    }

    # Write live data + monthly archive
    repo_root = Path(__file__).resolve().parent.parent
    (repo_root / "data.json").write_text(json.dumps(output, indent=2))
    print(f"\nwrote {repo_root / 'data.json'}", flush=True)

    archives = repo_root / "archives"
    archives.mkdir(exist_ok=True)
    archive_path = archives / f"data_{month_label}.json"
    archive_path.write_text(json.dumps(output, indent=2))
    print(f"wrote {archive_path}", flush=True)

    print(f"\nDone in {time.time() - started:.1f}s", flush=True)


if __name__ == "__main__":
    main()
