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
    {"name": "Cameron Caswell", "user_id": "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW"},
    {"name": "Elvis Ellis",     "user_id": "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr"},
    {"name": "Kelly Schrader",  "user_id": "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7"},
    {"name": "Lyle Hubbard",    "user_id": "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG"},
    {"name": "Jason Aaron",     "user_id": "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj"},
]
LANE_2_USER_IDS = {r["user_id"] for r in LANE_2_REPS}

# Scrapers (a.k.a. setters). They book meetings for the closers.
# Each entry has the Close user_id (for activity attribution) and the exact
# string value that the `Reactivation - Setter Name` custom field is set to
# by the update_field.py automation — see reactivation-setter-name-field.md.
SCRAPERS = [
    {"name": "Vince Bartolini",
     "user_id": "user_dQi0iL0igjCKtEXPSsv8ALDZMAz9orJxL60O7Q921jy",
     "setter_field_value": "Vince Bartolini"},
    {"name": "William Nowak",
     "user_id": "user_ZNKG1S9eI71qxhSozBK4jskTVtJqXzfNCPWqmADRR9F",
     "setter_field_value": "William Nowak"},
]

# Close custom field IDs (verified from sibling dashboards)
FIELD_LEAD_OWNER         = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
FIELD_HANDRAISER         = "cf_Q1hRv8It46xsAEmpv4PRKdI1y0sPJnrnQrgRbIlF8uL"
FIELD_FUNNEL_NAME        = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
FIELD_FIRST_CALL_BOOKED  = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
FIELD_FIRST_CALL_SHOWUP  = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"
FIELD_SETTER_NAME        = "cf_vz6kNiu4ItFxRA8Y9HKlWIoQMq3TsdaQqKekQ2YuxVk"

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


def business_days_elapsed(now):
    """Count business days (Mon-Fri) from the 1st of the month through today inclusive."""
    d = now.replace(day=1).date()
    end_d = now.date()
    count = 0
    while d <= end_d:
        if d.weekday() < 5:   # 0=Mon ... 4=Fri
            count += 1
        d += timedelta(days=1)
    return max(count, 1)   # avoid divide-by-zero on day-1 weekend edge case


def week_bounds_pt(now=None):
    """Monday–Friday week containing `now` (or just-ended week if on Sat/Sun).

    Returns (week_start_mon_pt, week_end_sat_pt_exclusive, 'YYYY-MM-DD' label = Monday).
    The exclusive end is Saturday 00:00 PT (so the inclusive last day is Friday).
    """
    now = now or now_pt()
    days_since_monday = now.weekday()   # Mon=0 ... Sun=6
    week_start = (now - timedelta(days=days_since_monday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_end_exclusive = week_start + timedelta(days=5)   # Sat 00:00 PT
    return week_start, week_end_exclusive, week_start.strftime("%Y-%m-%d")


def business_days_elapsed_wtd(now, week_start, week_end_exclusive):
    """Count Mon-Fri days from week_start through min(today, Friday) inclusive."""
    last_day = (week_end_exclusive - timedelta(days=1)).date()   # Friday of the week
    end_d = min(now.date(), last_day)
    d = week_start.date()
    count = 0
    while d <= end_d:
        if d.weekday() < 5:
            count += 1
        d += timedelta(days=1)
    return max(count, 1)


def prev_n_weeks(week_start, n):
    """Return list of {start, end_exclusive, label} for the N completed weeks before `week_start`."""
    out = []
    for i in range(1, n + 1):
        ws = week_start - timedelta(weeks=i)
        we = ws + timedelta(days=5)
        out.append({"start": ws, "end_exclusive": we, "label": ws.strftime("%Y-%m-%d")})
    return out


def format_week_label(week_start, week_end_inclusive):
    """e.g. 'Jun 8 – 12, 2026' (or cross-month: 'May 27 – Jun 2, 2026')."""
    same_month = (week_start.month == week_end_inclusive.month
                   and week_start.year == week_end_inclusive.year)
    if same_month:
        return (f"{week_start.strftime('%b')} {week_start.day} – "
                f"{week_end_inclusive.day}, {week_end_inclusive.year}")
    return (f"{week_start.strftime('%b')} {week_start.day} – "
            f"{week_end_inclusive.strftime('%b')} {week_end_inclusive.day}, "
            f"{week_end_inclusive.year}")


# ============================================================================
# CALENDAR — leads assigned to Lane 2 reps per day
# ============================================================================

def fetch_calendar(month_start, month_end):
    """
    Walks /event/ for object_type=lead, action=updated, since month_start.
    For each event whose lead-owner custom field changed to a Lane 2 rep AND
    whose lead is not on the LTF Quiz Funnel, records (lead_id, PT calendar day,
    new_owner, handraiser). Dedupes per (lead, day) — events come latest-first
    from Close so the first event we see for a (lead, day) is the most recent
    assignment of that day; that rep gets credit.

    Returns (counts, breakdowns, per_rep_per_day):
      counts:          dict[YYYY-MM-DD] -> int       (total leads that day)
      breakdowns:      dict[YYYY-MM-DD] -> {handraiser: int}
      per_rep_per_day: dict[YYYY-MM-DD] -> {user_id: int}
    """
    print(f"[calendar] scanning lead.updated events "
          f"{month_start.date()} → {month_end.date()}", flush=True)

    # day_str -> {lead_id: (owner_user_id, handraiser_value)}
    # We use a dict so we keep only the FIRST (= latest) event per (lead, day).
    per_day_lead = defaultdict(dict)
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
                  f"(~{page_count} pages), "
                  f"{sum(len(d) for d in per_day_lead.values())} qualifying so far",
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

        # First event we see for a (lead, day) wins (= latest, since events
        # are ordered date_updated desc by Close).
        if lead_id in per_day_lead[pt_date]:
            continue

        handraiser = get_custom(data, FIELD_HANDRAISER) or "(unset)"
        per_day_lead[pt_date][lead_id] = (new_owner, handraiser)

    print(f"[calendar] scanned {event_count} events total · "
          f"{not_owner_change} non-owner-change · "
          f"{not_lane2} non-Lane2 owner · "
          f"{excluded_funnel_count} LTF excluded",
          flush=True)

    counts = {}
    breakdowns = {}
    per_rep_per_day = {}
    for day, leads in per_day_lead.items():
        counts[day] = len(leads)
        bd = defaultdict(int)
        rd = defaultdict(int)
        for owner, hr in leads.values():
            bd[hr] += 1
            rd[owner] += 1
        breakdowns[day] = dict(sorted(
            bd.items(),
            key=lambda kv: (kv[0] == "(unset)", -kv[1], kv[0])
        ))
        per_rep_per_day[day] = dict(rd)

    print(f"[calendar] days with activity: {len(counts)} · "
          f"total qualifying leads: {sum(counts.values())}",
          flush=True)
    return counts, breakdowns, per_rep_per_day


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


def fetch_rep_activities_per_day(user_id, fetch_start):
    """Per-day buckets for each activity type, plus outbound subsets for calls/emails.

    Paginates ALL 5 activity types from `fetch_start` onward — so the same data
    serves both MTD and WTD aggregations. SMS/notes/meetings used to use the
    cheaper close_count, but it paginates internally anyway when total_results
    is missing, so this is the same cost while giving us per-day granularity
    for the # Activities total in any time window.

    Returns:
      {
        "calls": {day: n},  "calls_outbound": {day: n},
        "emails": {day: n}, "emails_outbound": {day: n},
        "sms": {day: n}, "notes": {day: n}, "meetings": {day: n},
      }
    """
    since_iso = fetch_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    base = {"user_id": user_id, "date_created__gte": since_iso}

    def _bucket(path, outbound_values=None):
        all_pd = defaultdict(int)
        ob_pd = defaultdict(int)
        for act in close_paginate_skip(path, base):
            ts = act.get("date_created")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                day = dt.astimezone(TIMEZONE).date().isoformat()
            except ValueError:
                continue
            all_pd[day] += 1
            if outbound_values and (act.get("direction") or "").lower() in outbound_values:
                ob_pd[day] += 1
        return dict(all_pd), dict(ob_pd)

    calls_pd,    calls_ob_pd   = _bucket("/activity/call/",    {"outbound"})
    emails_pd,   emails_ob_pd  = _bucket("/activity/email/",   {"outgoing", "outbound"})
    sms_pd,      _             = _bucket("/activity/sms/")
    notes_pd,    _             = _bucket("/activity/note/")
    meetings_pd, _             = _bucket("/activity/meeting/")

    return {
        "calls":          calls_pd,
        "calls_outbound": calls_ob_pd,
        "emails":         emails_pd,
        "emails_outbound": emails_ob_pd,
        "sms":            sms_pd,
        "notes":          notes_pd,
        "meetings":       meetings_pd,
    }


def fetch_calls_booked_per_day(user_id, fetch_start, fetch_end_exclusive):
    """Per-day buckets of `First Sales Call Booked Date` for leads owned by `user_id`,
    over [fetch_start, fetch_end_exclusive]. LTF excluded. Returns {day: count}.
    """
    start_d = fetch_start.date().isoformat()
    end_d   = (fetch_end_exclusive - timedelta(seconds=1)).date().isoformat()
    q = (
        f'custom.{FIELD_LEAD_OWNER}:"{user_id}" '
        f'custom.{FIELD_FIRST_CALL_BOOKED}>="{start_d}" '
        f'custom.{FIELD_FIRST_CALL_BOOKED}<="{end_d}"'
    )
    per_day = defaultdict(int)
    for ld in close_paginate_skip("/lead/", {
        "query": q,
        "_fields": (
            f"id,custom.{FIELD_FUNNEL_NAME},"
            f"custom.{FIELD_FIRST_CALL_BOOKED}"
        ),
    }):
        if get_custom(ld, FIELD_FUNNEL_NAME) in EXCLUDED_FUNNELS:
            continue
        booked_date = get_custom(ld, FIELD_FIRST_CALL_BOOKED)
        if not booked_date:
            continue
        per_day[booked_date[:10]] += 1
    return dict(per_day)


def fetch_deals_per_day(user_id, fetch_start, fetch_end_exclusive):
    """Per-day buckets for won and lost deals for this rep, in PT.

    Won:  server-side date_won filter (works).
    Lost: paginate all, filter client-side on date_lost (server-side filter is silently ignored).
    LTF excluded via per-lead funnel lookup (cached).
    Returns (won_per_day, lost_per_day) as plain dicts.
    """
    start_iso = fetch_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = fetch_end_exclusive.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    won_records = []   # (lead_id, date_won_str)
    for opp in close_paginate_skip("/opportunity/", {
        "user_id": user_id,
        "status_type": "won",
        "date_won__gte": start_iso,
        "date_won__lt":  end_iso,
        "_fields": "id,lead_id,date_won",
    }):
        if opp.get("lead_id") and opp.get("date_won"):
            won_records.append((opp["lead_id"], opp["date_won"]))

    lost_records = []  # (lead_id, date_lost_str)
    f_start_utc = fetch_start.astimezone(timezone.utc)
    f_end_utc   = fetch_end_exclusive.astimezone(timezone.utc)
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
        if not (f_start_utc <= dt.astimezone(timezone.utc) < f_end_utc):
            continue
        if opp.get("lead_id"):
            lost_records.append((opp["lead_id"], date_lost))

    # LTF exclusion — look up each unique lead's funnel once
    unique_leads = set(r[0] for r in won_records) | set(r[0] for r in lost_records)
    funnel_cache = {}
    for lid in unique_leads:
        try:
            ld = close_get(f"/lead/{lid}/",
                           {"_fields": f"id,custom.{FIELD_FUNNEL_NAME}"})
            funnel_cache[lid] = get_custom(ld, FIELD_FUNNEL_NAME)
        except requests.HTTPError:
            funnel_cache[lid] = None

    def _bucket(records):
        per_day = defaultdict(int)
        for lid, date_str in records:
            if funnel_cache.get(lid) in EXCLUDED_FUNNELS:
                continue
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            per_day[dt.astimezone(TIMEZONE).date().isoformat()] += 1
        return dict(per_day)

    return _bucket(won_records), _bucket(lost_records)


def build_rep_data(rep, fetch_start, fetch_end_exclusive):
    """Fetch ALL of this rep's data covering [fetch_start, fetch_end_exclusive].

    Returns (snapshot, per_day):
      snapshot — current-state values (owned_leads, handraiser_breakdown, leads_zero_comms);
                 same regardless of which period is being viewed.
      per_day  — flat dict of per-day buckets keyed by metric name:
                 calls, calls_outbound, emails, emails_outbound, sms, notes, meetings,
                 calls_booked, deals_won, deals_lost.
                 Aggregations for MTD / WTD / backfill weeks slice this same dict.
    """
    print(f"[rep] {rep['name']}", flush=True)
    uid = rep["user_id"]

    leads = fetch_owned_leads(uid)
    handraiser_counts = defaultdict(int)
    zero_comm = 0
    for ld in leads:
        handraiser_counts[ld["handraiser"] or "(unset)"] += 1
        if ld["times_communicated"] == 0:
            zero_comm += 1
    snapshot = {
        "owned_leads": len(leads),
        "handraiser_breakdown": dict(sorted(
            handraiser_counts.items(), key=lambda kv: kv[1], reverse=True
        )),
        "leads_zero_comms": zero_comm,
    }

    act_pd = fetch_rep_activities_per_day(uid, fetch_start)
    calls_booked_pd = fetch_calls_booked_per_day(uid, fetch_start, fetch_end_exclusive)
    won_pd, lost_pd = fetch_deals_per_day(uid, fetch_start, fetch_end_exclusive)

    per_day = {
        **act_pd,                       # calls, calls_outbound, emails, emails_outbound, sms, notes, meetings
        "calls_booked": calls_booked_pd,
        "deals_won":    won_pd,
        "deals_lost":   lost_pd,
    }
    return snapshot, per_day


def _sum_in_range(per_day_dict, start_date, end_date_exclusive):
    """Sum values in a per-day dict whose keys fall in [start_date, end_date_exclusive)."""
    total = 0
    for day_str, count in (per_day_dict or {}).items():
        try:
            d = datetime.strptime(day_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if start_date <= d < end_date_exclusive:
            total += count
    return total


def aggregate_rep_for_period(per_day, start_pt, end_exclusive_pt, biz_days):
    """Aggregate a rep's per-day data into period totals."""
    s, e = start_pt.date(), end_exclusive_pt.date()
    calls    = _sum_in_range(per_day.get("calls"),    s, e)
    emails   = _sum_in_range(per_day.get("emails"),   s, e)
    sms      = _sum_in_range(per_day.get("sms"),      s, e)
    notes    = _sum_in_range(per_day.get("notes"),    s, e)
    meetings = _sum_in_range(per_day.get("meetings"), s, e)
    ob_calls  = _sum_in_range(per_day.get("calls_outbound"),  s, e)
    ob_emails = _sum_in_range(per_day.get("emails_outbound"), s, e)
    return {
        "activities":      calls + emails + sms + notes + meetings,
        "outbound_calls":  ob_calls,
        "outbound_emails": ob_emails,
        "outbound_calls_per_day_avg":  round(ob_calls  / max(biz_days, 1), 1),
        "outbound_emails_per_day_avg": round(ob_emails / max(biz_days, 1), 1),
        "calls_booked":  _sum_in_range(per_day.get("calls_booked"), s, e),
        "deals_closed":  _sum_in_range(per_day.get("deals_won"),    s, e),
        "deals_lost":    _sum_in_range(per_day.get("deals_lost"),   s, e),
    }


# ============================================================================
# SCRAPERS (setters)
# ============================================================================

def fetch_scraper_activities_per_day(user_id, fetch_start):
    """Outbound-only activities for this scraper from fetch_start onward.

    Per spec, voicemails are NOT counted — the underlying call activity already
    accounts for the contact attempt.

    Returns:
      {
        "outbound_calls":  {day: n},
        "outbound_emails": {day: n},
        "outbound_sms":    {day: n},
      }
    """
    since_iso = fetch_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    base = {"user_id": user_id, "date_created__gte": since_iso}

    def _bucket(path, outbound_values):
        per_day = defaultdict(int)
        for act in close_paginate_skip(path, base):
            if (act.get("direction") or "").lower() not in outbound_values:
                continue
            ts = act.get("date_created")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                per_day[dt.astimezone(TIMEZONE).date().isoformat()] += 1
            except ValueError:
                pass
        return dict(per_day)

    return {
        "outbound_calls":  _bucket("/activity/call/",  {"outbound"}),
        "outbound_emails": _bucket("/activity/email/", {"outgoing", "outbound"}),
        "outbound_sms":    _bucket("/activity/sms/",   {"outbound", "outgoing"}),
    }


def fetch_scraper_meetings_per_day(setter_value, fetch_start, fetch_end_exclusive):
    """Per-day buckets for booked / shown / closed meetings credited to a scraper,
    across [fetch_start, fetch_end_exclusive].

    A "meeting booked by X" = lead where:
      - custom.{Setter Name} == setter_value
      - custom.{First Sales Call Booked Date} in [fetch_start, fetch_end]
      - funnel is not LTF

    For each qualifying lead, day is its First Sales Call Booked Date.
    Shown:  custom.{First Call Show Up} = "Yes"
    Closed: ANY opportunity on the lead has status_type = won (any date, "ever")
    """
    start_d = fetch_start.date().isoformat()
    end_d   = (fetch_end_exclusive - timedelta(seconds=1)).date().isoformat()
    q = (
        f'custom.{FIELD_SETTER_NAME}:"{setter_value}" '
        f'custom.{FIELD_FIRST_CALL_BOOKED}>="{start_d}" '
        f'custom.{FIELD_FIRST_CALL_BOOKED}<="{end_d}"'
    )
    fields = (
        "id,opportunities,"
        f"custom.{FIELD_FIRST_CALL_BOOKED},"
        f"custom.{FIELD_FIRST_CALL_SHOWUP},"
        f"custom.{FIELD_FUNNEL_NAME}"
    )

    booked_pd = defaultdict(int)
    shown_pd  = defaultdict(int)
    closed_pd = defaultdict(int)

    for ld in close_paginate_skip("/lead/", {"query": q, "_fields": fields}):
        if get_custom(ld, FIELD_FUNNEL_NAME) in EXCLUDED_FUNNELS:
            continue
        booked_date = get_custom(ld, FIELD_FIRST_CALL_BOOKED)
        if not booked_date:
            continue
        day = booked_date[:10]
        booked_pd[day] += 1

        show_up = (get_custom(ld, FIELD_FIRST_CALL_SHOWUP) or "").strip().lower()
        if show_up == "yes":
            shown_pd[day] += 1

        opps = ld.get("opportunities") or []
        if any((opp or {}).get("status_type") == "won" for opp in opps):
            closed_pd[day] += 1

    return {
        "meetings_booked": dict(booked_pd),
        "meetings_shown":  dict(shown_pd),
        "meetings_closed": dict(closed_pd),
    }


def build_scraper_data(scraper, fetch_start, fetch_end_exclusive):
    """Fetch ALL scraper data covering [fetch_start, fetch_end_exclusive]. Returns per_day dict.
    """
    print(f"[scraper] {scraper['name']}", flush=True)
    uid = scraper["user_id"]
    act_pd = fetch_scraper_activities_per_day(uid, fetch_start)
    mtg_pd = fetch_scraper_meetings_per_day(
        scraper["setter_field_value"], fetch_start, fetch_end_exclusive
    )
    return {**act_pd, **mtg_pd}


def aggregate_scraper_for_period(per_day, start_pt, end_exclusive_pt):
    """Aggregate a scraper's per-day data into period totals."""
    s, e = start_pt.date(), end_exclusive_pt.date()
    oc = _sum_in_range(per_day.get("outbound_calls"),  s, e)
    oe = _sum_in_range(per_day.get("outbound_emails"), s, e)
    os = _sum_in_range(per_day.get("outbound_sms"),    s, e)
    return {
        "activities_total": oc + oe + os,
        "activities_breakdown": {
            "outbound_calls":  oc,
            "outbound_emails": oe,
            "outbound_sms":    os,
        },
        "meetings_booked": _sum_in_range(per_day.get("meetings_booked"), s, e),
        "meetings_shown":  _sum_in_range(per_day.get("meetings_shown"),  s, e),
        "meetings_closed": _sum_in_range(per_day.get("meetings_closed"), s, e),
    }


# ============================================================================
# MAIN
# ============================================================================

def _slice_dict_by_date(d, start_date, end_exclusive_date):
    """Return a copy of `d` keyed by YYYY-MM-DD whose keys fall in [start, end_exclusive)."""
    out = {}
    for k, v in (d or {}).items():
        try:
            dd = datetime.strptime(k, "%Y-%m-%d").date()
        except ValueError:
            continue
        if start_date <= dd < end_exclusive_date:
            out[k] = v
    return out


def _build_daily_breakdowns(rep_meta, rep_per_day_by_uid, calendar_per_rep_per_day,
                             start_pt, end_exclusive_pt):
    """Build the {day: {user_id: {name, new_leads, outbound_calls, outbound_emails,
                                   calls_booked, deals_closed}}} structure that powers
       the rep-view click drill-down, for the given date range."""
    s, e = start_pt.date(), end_exclusive_pt.date()
    out = {}
    for meta in rep_meta:
        uid = meta["user_id"]
        pd = rep_per_day_by_uid.get(uid, {})
        # Days where this rep had any activity in the period
        candidate_days = (
            set(pd.get("calls_outbound", {})) |
            set(pd.get("emails_outbound", {})) |
            set(pd.get("calls_booked", {})) |
            set(pd.get("deals_won", {}))
        )
        for day, by_rep in calendar_per_rep_per_day.items():
            if by_rep.get(uid):
                candidate_days.add(day)
        for day in candidate_days:
            try:
                dd = datetime.strptime(day, "%Y-%m-%d").date()
            except ValueError:
                continue
            if not (s <= dd < e):
                continue
            row = {
                "new_leads":       calendar_per_rep_per_day.get(day, {}).get(uid, 0),
                "outbound_calls":  pd.get("calls_outbound",  {}).get(day, 0),
                "outbound_emails": pd.get("emails_outbound", {}).get(day, 0),
                "calls_booked":    pd.get("calls_booked",    {}).get(day, 0),
                "deals_closed":    pd.get("deals_won",       {}).get(day, 0),
            }
            if any(row.values()):
                out.setdefault(day, {})[uid] = {"name": meta["name"], **row}
    return out


def _build_scraper_daily_breakdowns(scraper_meta, scraper_per_day_by_uid,
                                     start_pt, end_exclusive_pt):
    s, e = start_pt.date(), end_exclusive_pt.date()
    out = {}
    for meta in scraper_meta:
        uid = meta["user_id"]
        pd = scraper_per_day_by_uid.get(uid, {})
        days = (
            set(pd.get("meetings_booked", {})) |
            set(pd.get("meetings_shown",  {})) |
            set(pd.get("meetings_closed", {})) |
            set(pd.get("outbound_calls",  {})) |
            set(pd.get("outbound_emails", {})) |
            set(pd.get("outbound_sms",    {}))
        )
        for day in days:
            try:
                dd = datetime.strptime(day, "%Y-%m-%d").date()
            except ValueError:
                continue
            if not (s <= dd < e):
                continue
            row = {
                "meetings_booked": pd.get("meetings_booked", {}).get(day, 0),
                "meetings_shown":  pd.get("meetings_shown",  {}).get(day, 0),
                "meetings_closed": pd.get("meetings_closed", {}).get(day, 0),
                "outbound_calls":  pd.get("outbound_calls",  {}).get(day, 0),
                "outbound_emails": pd.get("outbound_emails", {}).get(day, 0),
                "outbound_sms":    pd.get("outbound_sms",    {}).get(day, 0),
            }
            if any(row.values()):
                out.setdefault(day, {})[uid] = {"name": meta["name"], **row}
    return out


def _build_scraper_calendar(scraper_meta, scraper_per_day_by_uid,
                             start_pt, end_exclusive_pt, fill_days):
    """Returns (calendar_counts {day: total_meetings_booked},
                breakdowns {day: {scraper_name: count}}),
       backfilled so every day in `fill_days` is present (zero if no bookings)."""
    s, e = start_pt.date(), end_exclusive_pt.date()
    counts = {d: 0 for d in fill_days}
    breakdowns = defaultdict(dict)
    for meta in scraper_meta:
        uid = meta["user_id"]
        booked_pd = scraper_per_day_by_uid.get(uid, {}).get("meetings_booked", {})
        for day, n in booked_pd.items():
            if not n:
                continue
            try:
                dd = datetime.strptime(day, "%Y-%m-%d").date()
            except ValueError:
                continue
            if not (s <= dd < e):
                continue
            counts[day] = counts.get(day, 0) + n
            breakdowns[day][meta["name"]] = n
    return counts, dict(breakdowns)


def _build_rep_view(rep_meta, rep_per_day_by_uid, start_pt, end_exclusive_pt, biz_days,
                     legacy_suffix="_mtd"):
    """Build the `reps` array for an output file. legacy_suffix is "_mtd" for both
    the live data.json and archives — the HTML already reads those names, and using
    them for weekly archives too means the rep-details renderer works unchanged.
    """
    out = []
    for meta in rep_meta:
        uid = meta["user_id"]
        pd = rep_per_day_by_uid.get(uid, {})
        agg = aggregate_rep_for_period(pd, start_pt, end_exclusive_pt, biz_days)
        out.append({
            **meta,
            f"activities{legacy_suffix}":      agg["activities"],
            f"outbound_calls{legacy_suffix}":  agg["outbound_calls"],
            f"outbound_emails{legacy_suffix}": agg["outbound_emails"],
            "outbound_calls_per_day_avg":      agg["outbound_calls_per_day_avg"],
            "outbound_emails_per_day_avg":     agg["outbound_emails_per_day_avg"],
            f"calls_booked{legacy_suffix}":    agg["calls_booked"],
            f"deals_closed{legacy_suffix}":    agg["deals_closed"],
            f"deals_lost{legacy_suffix}":      agg["deals_lost"],
        })
    out.sort(key=lambda x: x["owned_leads"], reverse=True)
    return out


def _build_scraper_view(scraper_meta, scraper_per_day_by_uid, start_pt, end_exclusive_pt):
    out = []
    for meta in scraper_meta:
        uid = meta["user_id"]
        pd = scraper_per_day_by_uid.get(uid, {})
        agg = aggregate_scraper_for_period(pd, start_pt, end_exclusive_pt)
        out.append({
            **meta,
            "activities_mtd_total":  agg["activities_total"],
            "activities_breakdown":  agg["activities_breakdown"],
            "meetings_booked_mtd":   agg["meetings_booked"],
            "meetings_shown_mtd":    agg["meetings_shown"],
            "meetings_closed_ever":  agg["meetings_closed"],
        })
    out.sort(key=lambda x: x["meetings_booked_mtd"], reverse=True)
    return out


def _mon_to_fri(week_start):
    """Returns the 5 Mon-Fri ISO date strings for a week starting `week_start`."""
    return [(week_start + timedelta(days=i)).date().isoformat() for i in range(5)]


def _update_archives_index(archives_dir):
    """Rewrite archives/index.json listing all months + weeks present."""
    months = sorted(
        [f.stem.removeprefix("data_") for f in archives_dir.glob("data_*.json")],
        reverse=True,
    )
    weeks = sorted(
        [f.stem.removeprefix("week_") for f in archives_dir.glob("week_*.json")],
        reverse=True,
    )
    # Attach human-readable labels for the picker
    def _week_label(ws_iso):
        try:
            ws = datetime.strptime(ws_iso, "%Y-%m-%d")
            we = ws + timedelta(days=4)
            return format_week_label(ws, we)
        except ValueError:
            return ws_iso

    def _month_label(m_iso):
        try:
            return datetime.strptime(m_iso, "%Y-%m").strftime("%B %Y")
        except ValueError:
            return m_iso

    index = {
        "months": [{"key": m, "label": _month_label(m)} for m in months],
        "weeks":  [{"key": w, "label": _week_label(w)}  for w in weeks],
    }
    (archives_dir / "index.json").write_text(json.dumps(index, indent=2))
    print(f"wrote {archives_dir / 'index.json'} "
          f"(months={len(months)}, weeks={len(weeks)})", flush=True)


def main():
    started = time.time()
    now = now_pt()

    month_start, month_end, month_label = month_bounds(now)
    week_start, week_end_excl, week_iso = week_bounds_pt(now)
    biz_days_mtd = business_days_elapsed(now)
    biz_days_wtd = business_days_elapsed_wtd(now, week_start, week_end_excl)

    # Backfill the previous 2 completed weeks (per spec). Extend fetch range
    # back to the earliest needed Monday so all per-day data is captured in
    # one pass.
    backfill_weeks = prev_n_weeks(week_start, 2)
    earliest_week = min([week_start] + [bw["start"] for bw in backfill_weeks])
    fetch_start = min(month_start, earliest_week)

    print(f"Run: {now.isoformat()}", flush=True)
    print(f"Month: {month_label} ({month_start.date()} → "
          f"{(month_end - timedelta(days=1)).date()}) biz_days={biz_days_mtd}", flush=True)
    print(f"Week:  {week_iso} ({week_start.date()} → "
          f"{(week_end_excl - timedelta(days=1)).date()}) biz_days={biz_days_wtd}", flush=True)
    print(f"Backfill weeks: {[bw['label'] for bw in backfill_weeks]}", flush=True)
    print(f"Fetch from: {fetch_start.date()} (covers all of the above)\n", flush=True)

    # ─ Calendar (lead.updated events) ─────────────────────────────────────
    cal_counts_all, cal_breakdowns_all, cal_per_rep_per_day_all = fetch_calendar(
        fetch_start, month_end
    )

    # ─ Per-rep data ───────────────────────────────────────────────────────
    print("", flush=True)
    rep_meta = []        # {user_id, name, owned_leads, handraiser_breakdown, leads_zero_comms}
    rep_per_day_by_uid = {}
    for rep_cfg in LANE_2_REPS:
        snap, per_day = build_rep_data(rep_cfg, fetch_start, month_end)
        rep_meta.append({"user_id": rep_cfg["user_id"],
                          "name": rep_cfg["name"], **snap})
        rep_per_day_by_uid[rep_cfg["user_id"]] = per_day

    # ─ Per-scraper data ───────────────────────────────────────────────────
    print("", flush=True)
    scraper_meta = []
    scraper_per_day_by_uid = {}
    for scr_cfg in SCRAPERS:
        per_day = build_scraper_data(scr_cfg, fetch_start, month_end)
        scraper_meta.append({"user_id": scr_cfg["user_id"],
                              "name": scr_cfg["name"]})
        scraper_per_day_by_uid[scr_cfg["user_id"]] = per_day

    # ─ Assemble views for MTD + current WTD ───────────────────────────────
    reps_mtd     = _build_rep_view(rep_meta, rep_per_day_by_uid,
                                    month_start, month_end, biz_days_mtd)
    reps_wtd     = _build_rep_view(rep_meta, rep_per_day_by_uid,
                                    week_start, week_end_excl, biz_days_wtd)
    scrapers_mtd = _build_scraper_view(scraper_meta, scraper_per_day_by_uid,
                                         month_start, month_end)
    scrapers_wtd = _build_scraper_view(scraper_meta, scraper_per_day_by_uid,
                                         week_start, week_end_excl)

    # Merge: each rep/scraper has top-level MTD fields + `wtd` sub-block. The
    # rep snapshots (owned/handraiser/0-comms) live at the top level — they're
    # "snapshot now" regardless of view, per the locked spec.
    by_uid_wtd_reps = {r["user_id"]: r for r in reps_wtd}
    reps_combined = []
    for r in reps_mtd:
        w = by_uid_wtd_reps.get(r["user_id"], {})
        wtd_block = {
            "activities":                  w.get("activities_mtd", 0),
            "outbound_calls":              w.get("outbound_calls_mtd", 0),
            "outbound_emails":             w.get("outbound_emails_mtd", 0),
            "outbound_calls_per_day_avg":  w.get("outbound_calls_per_day_avg", 0),
            "outbound_emails_per_day_avg": w.get("outbound_emails_per_day_avg", 0),
            "calls_booked":                w.get("calls_booked_mtd", 0),
            "deals_closed":                w.get("deals_closed_mtd", 0),
            "deals_lost":                  w.get("deals_lost_mtd", 0),
        }
        reps_combined.append({**r, "wtd": wtd_block})

    by_uid_wtd_scr = {s["user_id"]: s for s in scrapers_wtd}
    scrapers_combined = []
    for s in scrapers_mtd:
        w = by_uid_wtd_scr.get(s["user_id"], {})
        wtd_block = {
            "activities_total":     w.get("activities_mtd_total", 0),
            "activities_breakdown": w.get("activities_breakdown", {}),
            "meetings_booked":      w.get("meetings_booked_mtd", 0),
            "meetings_shown":       w.get("meetings_shown_mtd", 0),
            "meetings_closed":      w.get("meetings_closed_ever", 0),
        }
        scrapers_combined.append({**s, "wtd": wtd_block})

    # ─ Calendar slicing ───────────────────────────────────────────────────
    full_month_calendar = {
        d.isoformat(): cal_counts_all.get(d.isoformat(), 0)
        for d in days_in_month(month_start, month_end)
    }
    month_cal_breakdowns = _slice_dict_by_date(
        cal_breakdowns_all, month_start.date(), month_end.date()
    )
    daily_breakdowns_mtd = _build_daily_breakdowns(
        rep_meta, rep_per_day_by_uid, cal_per_rep_per_day_all,
        month_start, month_end
    )

    week_days = _mon_to_fri(week_start)
    week_calendar = {d: cal_counts_all.get(d, 0) for d in week_days}
    week_cal_breakdowns = _slice_dict_by_date(
        cal_breakdowns_all, week_start.date(), week_end_excl.date()
    )
    daily_breakdowns_wtd = _build_daily_breakdowns(
        rep_meta, rep_per_day_by_uid, cal_per_rep_per_day_all,
        week_start, week_end_excl
    )

    month_scraper_calendar, month_scraper_cal_breakdowns = _build_scraper_calendar(
        scraper_meta, scraper_per_day_by_uid,
        month_start, month_end,
        [d.isoformat() for d in days_in_month(month_start, month_end)],
    )
    scraper_daily_breakdowns_mtd = _build_scraper_daily_breakdowns(
        scraper_meta, scraper_per_day_by_uid, month_start, month_end
    )

    week_scraper_calendar, week_scraper_cal_breakdowns = _build_scraper_calendar(
        scraper_meta, scraper_per_day_by_uid,
        week_start, week_end_excl,
        week_days,
    )
    scraper_daily_breakdowns_wtd = _build_scraper_daily_breakdowns(
        scraper_meta, scraper_per_day_by_uid, week_start, week_end_excl
    )

    # ─ OUTPUT: data.json (live, has both MTD and WTD) ─────────────────────
    output = {
        "generated_at": now.isoformat(),
        "month": month_label,
        "month_start": month_start.date().isoformat(),
        "month_end": (month_end - timedelta(days=1)).date().isoformat(),
        "today": now.date().isoformat(),
        "business_days_elapsed": biz_days_mtd,
        # NEW: week-level metadata + data
        "week_start": week_start.date().isoformat(),
        "week_end": (week_end_excl - timedelta(days=1)).date().isoformat(),
        "week_label": format_week_label(week_start, week_end_excl - timedelta(days=1)),
        "business_days_elapsed_wtd": biz_days_wtd,
        # Existing month-level calendar data
        "calendar": full_month_calendar,
        "calendar_breakdowns": month_cal_breakdowns,
        "daily_breakdowns": daily_breakdowns_mtd,
        "scraper_calendar": month_scraper_calendar,
        "scraper_calendar_breakdowns": month_scraper_cal_breakdowns,
        "scraper_daily_breakdowns": scraper_daily_breakdowns_mtd,
        # NEW: week-level calendar data
        "week_calendar": week_calendar,
        "week_calendar_breakdowns": week_cal_breakdowns,
        "week_daily_breakdowns": daily_breakdowns_wtd,
        "scraper_week_calendar": week_scraper_calendar,
        "scraper_week_calendar_breakdowns": week_scraper_cal_breakdowns,
        "scraper_week_daily_breakdowns": scraper_daily_breakdowns_wtd,
        # Reps + scrapers (top-level MTD, wtd sub-block)
        "reps": reps_combined,
        "scrapers": scrapers_combined,
    }

    repo_root = Path(__file__).resolve().parent.parent
    (repo_root / "data.json").write_text(json.dumps(output, indent=2))
    print(f"\nwrote {repo_root / 'data.json'}", flush=True)

    archives = repo_root / "archives"
    archives.mkdir(exist_ok=True)

    # Monthly archive — same shape as data.json (so the live HTML can also
    # render a historical month archive without code changes).
    monthly_archive = archives / f"data_{month_label}.json"
    monthly_archive.write_text(json.dumps(output, indent=2))
    print(f"wrote {monthly_archive}", flush=True)

    # ─ Weekly archives: current week + backfill ───────────────────────────
    def _write_week_archive(ws, we_excl, label, biz_days):
        """A week-shaped file: top-level fields hold THAT week's data (no `wtd`
        nesting). Field names keep their `_mtd`/`_ever` suffixes so the existing
        HTML renderer reads them unchanged."""
        wdays = _mon_to_fri(ws)
        cal = {d: cal_counts_all.get(d, 0) for d in wdays}
        cal_bd = _slice_dict_by_date(cal_breakdowns_all, ws.date(), we_excl.date())
        daily_bd = _build_daily_breakdowns(
            rep_meta, rep_per_day_by_uid, cal_per_rep_per_day_all, ws, we_excl
        )
        scr_cal, scr_cal_bd = _build_scraper_calendar(
            scraper_meta, scraper_per_day_by_uid, ws, we_excl, wdays
        )
        scr_daily_bd = _build_scraper_daily_breakdowns(
            scraper_meta, scraper_per_day_by_uid, ws, we_excl
        )

        # Reps & scrapers — period totals at top level (this whole file IS the period)
        rep_view     = _build_rep_view(rep_meta, rep_per_day_by_uid,
                                        ws, we_excl, biz_days)
        scraper_view = _build_scraper_view(scraper_meta, scraper_per_day_by_uid,
                                            ws, we_excl)

        week_archive = {
            "kind": "week",
            "generated_at": now.isoformat(),
            "week_start": ws.date().isoformat(),
            "week_end":   (we_excl - timedelta(days=1)).date().isoformat(),
            "week_label": format_week_label(ws, we_excl - timedelta(days=1)),
            # Compat fields the HTML expects (it doesn't care that they hold week values)
            "month": month_label,
            "month_start": ws.date().isoformat(),
            "month_end":   (we_excl - timedelta(days=1)).date().isoformat(),
            "today":       (we_excl - timedelta(days=1)).date().isoformat(),
            "business_days_elapsed": biz_days,
            "business_days_elapsed_wtd": biz_days,
            "calendar": cal,
            "calendar_breakdowns": cal_bd,
            "daily_breakdowns": daily_bd,
            "scraper_calendar": scr_cal,
            "scraper_calendar_breakdowns": scr_cal_bd,
            "scraper_daily_breakdowns": scr_daily_bd,
            "reps": rep_view,
            "scrapers": scraper_view,
        }
        path = archives / f"week_{label}.json"
        path.write_text(json.dumps(week_archive, indent=2))
        print(f"wrote {path}", flush=True)
        return path

    _write_week_archive(week_start, week_end_excl, week_iso, biz_days_wtd)
    for bw in backfill_weeks:
        if bw["start"] < fetch_start:
            print(f"  skip backfill {bw['label']} (outside fetch range)", flush=True)
            continue
        _write_week_archive(bw["start"], bw["end_exclusive"], bw["label"], 5)

    # ─ Update the navigation index ────────────────────────────────────────
    _update_archives_index(archives)

    print(f"\nDone in {time.time() - started:.1f}s", flush=True)


if __name__ == "__main__":
    main()
