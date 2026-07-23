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
    {"name": "Kelly Schrader",  "user_id": "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7"},
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
    {"name": "Jacob Hepner",
     "user_id": "user_IeWR2TlhpjqoXy3K6jX7u9C8c83iBnHXSIvFZpotF3z",
     "setter_field_value": "Jacob Hepner"},
    {"name": "Juan Cajina",
     "user_id": "user_E2WNDcnSES6SFuqyEulrIakLepLqJzHIimWaovDFkhK",
     "setter_field_value": "Juan Cajina"},
    {"name": "Jennifer Padilla",
     "user_id": "user_QgFeDsKkV4fsOtkTYeOJMURXPqqhZA8d4kHbE8rzat7",
     "setter_field_value": "Jennifer Padilla"},
]

# Setters hold their own discovery calls (in addition to the calls they book for
# closers). Each entry pairs a Close user_id with the title prefix used by the
# Calendly→Close integration; we match meeting activities whose title startswith
# the prefix and whose user_id matches.
#
# For "Set" (meetings_set), we reuse the scraper section's meetings_booked count
# for this user — the same person is appearing in both tables.
SETTERS = [
    {"name": "William Nowak",
     "user_id": "user_ZNKG1S9eI71qxhSozBK4jskTVtJqXzfNCPWqmADRR9F",
     "discovery_title_prefix": "Vendingpreneurs Quick Discovery",
     # If this setter also books calls (Setter Name field), we bucket their
     # scraper-credited closes as Outbound (unless the lead also has a VQD by
     # this setter — those are Inbound). Value must match the SCRAPERS entry.
     "scraper_setter_field_value": "William Nowak"},
]

# Close custom field IDs (verified from sibling dashboards)
FIELD_LEAD_OWNER         = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
FIELD_HANDRAISER         = "cf_Q1hRv8It46xsAEmpv4PRKdI1y0sPJnrnQrgRbIlF8uL"
FIELD_FUNNEL_NAME        = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
FIELD_FIRST_CALL_BOOKED  = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
FIELD_FIRST_CALL_SHOWUP  = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"
FIELD_SETTER_NAME        = "cf_vz6kNiu4ItFxRA8Y9HKlWIoQMq3TsdaQqKekQ2YuxVk"

EXCLUDED_FUNNELS = {"LTF - Quiz Funnel"}

# Title prefixes that identify a scraper-booked "Next Steps" meeting. Different
# closers use slightly different Calendly links. All start with one of these:
NEXT_STEPS_TITLE_PREFIXES = (
    "Vendingpreneurs Call - Next Steps",
    "Vendingpreneurs Next Steps Call",
    "Vendingpreneurs - Next Steps",
    "Vendingpreneur Next Steps",
)

# ============================================================================
# HTTP SESSION
# ============================================================================

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")
session.headers.update({"Content-Type": "application/json"})


def close_get(path, params=None):
    """GET to Close API with throttle + retries on 429 / 5xx / network timeouts.

    The /event/ endpoint can occasionally take longer than 30s to respond when
    paginating through tens of thousands of events. The timeout is set to 60s
    and any ReadTimeout/ConnectionError is retried with exponential backoff.

    On 4xx errors (other than 429), prints the response body before raising
    so we can see what Close actually rejected.
    """
    url = f"{CLOSE_BASE_URL}{path}"
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            r = session.get(url, params=params, timeout=60)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # Transient network problem — retry with backoff
            if attempt < max_attempts - 1:
                wait = 2 ** attempt   # 1, 2, 4, 8s
                print(f"  ⚠ {type(e).__name__} on {path}, "
                      f"retry {attempt + 1}/{max_attempts - 1} in {wait}s",
                      flush=True)
                time.sleep(wait)
                continue
            raise

        time.sleep(THROTTLE_SECONDS)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            print(f"  rate-limited, sleeping {wait}s", flush=True)
            time.sleep(wait)
            continue
        if 500 <= r.status_code < 600 and attempt < max_attempts - 1:
            wait = 2 ** attempt
            print(f"  {r.status_code} from Close, retrying ({attempt + 1}/{max_attempts - 1}) in {wait}s", flush=True)
            time.sleep(wait)
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
    raise RuntimeError(f"Close API failed after {max_attempts} attempts: {path}")


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


def _lead_has_vqd_by(lead_id, setter_uid, title_prefix):
    """Does this specific lead have any meeting hosted by `setter_uid` whose title
    starts with `title_prefix`? Used to classify a closed-won lead as Inbound
    (setter held the discovery) — checks the lead's full history, not just the
    fetch window, because a VQD may have happened months before the deal closed.

    Robustness: we query by lead_id ONLY (not combined with user_id) and filter
    the setter and title client-side. Close's `/activity/meeting/` REST endpoint
    doesn't reliably intersect the two server-side filters — it can silently
    drop matches when both are supplied together. Client-side filtering also
    means we don't need `_fields`, so the full meeting object is available if
    we ever need more attributes.
    """
    for mtg in close_paginate_skip("/activity/meeting/", {
        "lead_id": lead_id,
    }):
        if mtg.get("user_id") != setter_uid:
            continue
        if (mtg.get("title") or "").startswith(title_prefix):
            return True
    return False


def fetch_closes_per_day(scrapers, setters, fetch_start, fetch_end_exclusive):
    """Close-date attribution for BOTH scraper closes AND setter revenue.

    Single org-wide `/opportunity/` query for the period, then one lead lookup
    each. For every configured setter we also make one per-lead meeting lookup
    to check for a Vendingpreneurs Quick Discovery hosted by that setter.

    Classification per lead per setter:
      - Setter held VQD on this lead (any time) → Inbound
      - Otherwise, lead's Setter Name field matches this setter's scraper handle → Outbound
      - Otherwise, no setter credit

    Returns (scraper_closes_by_uid, setter_closes_by_uid) where:

      scraper_closes_by_uid[uid] = {
        "meetings_closed":         {day: count},
        "meetings_closed_revenue": {day: dollars},
        "meetings_closed_leads":   {day: [{lead_id, lead_name, value, won_count}, ...]},
      }

      setter_closes_by_uid[uid] = {
        "inbound_revenue":  {day: dollars},
        "outbound_revenue": {day: dollars},
        "inbound_leads":    {day: [{lead_id, lead_name, value, won_count}, ...]},
        "outbound_leads":   {day: [...]},
      }
    """
    scraper_setter_to_uid = {s["setter_field_value"]: s["user_id"] for s in scrapers}
    setters = setters or []

    start_iso = fetch_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = fetch_end_exclusive.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1) Pull every won opp in the period (single org-wide query) and group by lead.
    leads_to_opps = defaultdict(list)
    opp_count = 0
    for opp in close_paginate_skip("/opportunity/", {
        "status_type":     "won",
        "date_won__gte":   start_iso,
        "date_won__lt":    end_iso,
        "_fields":         "id,lead_id,value,date_won",
    }):
        opp_count += 1
        if opp.get("lead_id") and opp.get("date_won"):
            leads_to_opps[opp["lead_id"]].append(opp)
    print(f"  closes-in-period: {opp_count} won opps across {len(leads_to_opps)} unique leads",
          flush=True)

    scraper_out = {
        s["user_id"]: {
            "meetings_closed":         defaultdict(int),
            "meetings_closed_revenue": defaultdict(float),
            "meetings_closed_leads":   defaultdict(list),
        } for s in scrapers
    }
    setter_out = {
        s["user_id"]: {
            "inbound_revenue":  defaultdict(float),
            "outbound_revenue": defaultdict(float),
            "inbound_leads":    defaultdict(list),
            "outbound_leads":   defaultdict(list),
        } for s in setters
    }

    scraper_matched = 0
    setter_inbound  = 0
    setter_outbound = 0
    vqd_hits_by_uid = defaultdict(int)   # per-setter counter of leads where has_vqd=True
    vqd_miss_by_uid = defaultdict(int)   # per-setter counter of leads where has_vqd=False

    for lead_id, opps in leads_to_opps.items():
        try:
            ld = close_get(f"/lead/{lead_id}/", {
                "_fields": (
                    "id,display_name,"
                    f"custom.{FIELD_SETTER_NAME},"
                    f"custom.{FIELD_FUNNEL_NAME}"
                )
            })
        except requests.HTTPError:
            continue

        if get_custom(ld, FIELD_FUNNEL_NAME) in EXCLUDED_FUNNELS:
            continue

        setter_name_val = (get_custom(ld, FIELD_SETTER_NAME) or "").strip()
        lead_name = ld.get("display_name") or "(unnamed lead)"

        # Group opps under their date_won (PT) once — reused for both attributions.
        opps_by_day = defaultdict(list)
        for opp in opps:
            try:
                dt = datetime.fromisoformat(opp["date_won"].replace("Z", "+00:00"))
                day = dt.astimezone(TIMEZONE).date().isoformat()
                opps_by_day[day].append(opp)
            except ValueError:
                continue
        if not opps_by_day:
            continue

        # ─ SCRAPER attribution: lead's Setter Name matches a scraper handle ─
        scraper_uid = scraper_setter_to_uid.get(setter_name_val)
        if scraper_uid:
            scraper_matched += 1
            for day, day_opps in opps_by_day.items():
                scraper_out[scraper_uid]["meetings_closed"][day] += 1
                day_value = sum(int(o.get("value") or 0) for o in day_opps) / 100.0
                scraper_out[scraper_uid]["meetings_closed_revenue"][day] += day_value
                scraper_out[scraper_uid]["meetings_closed_leads"][day].append({
                    "lead_id":   lead_id,
                    "lead_name": lead_name,
                    "value":     round(day_value, 2),
                    "won_count": len(day_opps),
                })

        # ─ SETTER attribution: for each configured setter, classify inbound/outbound ─
        for setter in setters:
            setter_uid = setter["user_id"]
            prefix     = setter["discovery_title_prefix"]
            scraper_handle = setter.get("scraper_setter_field_value")

            has_vqd = _lead_has_vqd_by(lead_id, setter_uid, prefix)
            if has_vqd:
                vqd_hits_by_uid[setter_uid] += 1
            else:
                vqd_miss_by_uid[setter_uid] += 1

            if has_vqd:
                bucket = "inbound"
            elif scraper_handle and setter_name_val == scraper_handle:
                bucket = "outbound"
            else:
                continue

            for day, day_opps in opps_by_day.items():
                day_value = sum(int(o.get("value") or 0) for o in day_opps) / 100.0
                entry = {
                    "lead_id":   lead_id,
                    "lead_name": lead_name,
                    "value":     round(day_value, 2),
                    "won_count": len(day_opps),
                }
                if bucket == "inbound":
                    setter_out[setter_uid]["inbound_revenue"][day] += day_value
                    setter_out[setter_uid]["inbound_leads"][day].append(entry)
                    setter_inbound += 1
                else:
                    setter_out[setter_uid]["outbound_revenue"][day] += day_value
                    setter_out[setter_uid]["outbound_leads"][day].append(entry)
                    setter_outbound += 1

    print(f"  scraper credit: {scraper_matched} leads matched a scraper Setter Name",
          flush=True)
    if setters:
        print(f"  setter credit:  {setter_inbound} inbound + {setter_outbound} outbound (rows)",
              flush=True)
        for s in setters:
            uid = s["user_id"]
            in_rev  = sum(setter_out[uid]["inbound_revenue"].values())
            out_rev = sum(setter_out[uid]["outbound_revenue"].values())
            print(f"    {s['name']}: VQD hits={vqd_hits_by_uid[uid]} misses={vqd_miss_by_uid[uid]}"
                  f" · inbound=${in_rev:,.0f} · outbound=${out_rev:,.0f}",
                  flush=True)

    scraper_result = {
        uid: {k: dict(v) for k, v in data.items()}
        for uid, data in scraper_out.items()
    }
    setter_result = {
        uid: {k: dict(v) for k, v in data.items()}
        for uid, data in setter_out.items()
    }
    return scraper_result, setter_result


# Backwards-compat shim so any external caller (or future me) that expects the
# old signature keeps working. Just calls fetch_closes_per_day with no setters
# and returns the scraper dict.
def fetch_scraper_closes_per_day(scrapers, fetch_start, fetch_end_exclusive):
    scraper_result, _ = fetch_closes_per_day(scrapers, [], fetch_start, fetch_end_exclusive)
    return scraper_result


def build_scraper_data(scraper, fetch_start, fetch_end_exclusive):
    """Fetch scraper ACTIVITY data (outbound calls / emails / sms) covering
    [fetch_start, fetch_end_exclusive]. Meetings Set / Booked / Shown are
    populated separately in main() via `fetch_scraper_meetings_bulk`.
    """
    print(f"[scraper] {scraper['name']}", flush=True)
    return fetch_scraper_activities_per_day(scraper["user_id"], fetch_start)


def _is_next_steps(title):
    """Does this meeting title identify a scraper-booked Next Steps meeting?"""
    if not title:
        return False
    return any(title.startswith(p) for p in NEXT_STEPS_TITLE_PREFIXES)


def _parse_pt_day(iso_str):
    """Parse a Close ISO timestamp and return its PT calendar day (or None)."""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.astimezone(TIMEZONE).date()


def fetch_scraper_meetings_bulk(scrapers, meetings_set_start,
                                  fetch_start, fetch_end_exclusive):
    """Combined org-wide meeting query producing all three scraper meeting metrics
    in a single pass:

      - **Meetings Set** — bucketed by `date_created`. ANY meeting on a lead
        with Setter Name = scraper and funnel = "Reactivation Scrapers" counts
        (matches the call-capacity EOD email's convention).
      - **Meetings Booked** — bucketed by `starts_at`. Only meetings whose title
        matches one of NEXT_STEPS_TITLE_PREFIXES count. This replaces the older
        `First Sales Call Booked Date` lead-side approach, which missed any
        second/third Next Steps meeting on the same lead.
      - **Meetings Shown** — subset of Booked where the lead's
        `First Call Show Up (Opp)` field is "Yes". This field is per-lead (not
        per-meeting), so all Next Steps meetings on a lead where the first call
        showed are counted as shown. Coarser than ideal but matches how the
        field is populated today; the user is planning a follow-up on this.

    Pre-screens each meeting before hitting the lead endpoint: if a meeting's
    date_created isn't in Set range AND its title/starts_at don't qualify for
    Booked, we skip the lead lookup entirely. Cuts the expensive per-lead
    lookups by ~10-30x on a typical run.

    Returns:
      {scraper_uid: {
        "meetings_set":    {day: count},
        "meetings_booked": {day: count},
        "meetings_shown":  {day: count},
      }}
    """
    if not scrapers:
        return {}
    setter_to_uid = {s["setter_field_value"]: s["user_id"] for s in scrapers}

    # We query by date_created, but Booked/Shown bucket by starts_at. Meetings
    # scheduled for `fetch_start` were typically created within ~3 weeks before.
    # Extend the date_created range back to catch those. A meeting created before
    # this cutoff whose starts_at is in the fetch range would be missed — rare
    # in practice for scraper Next Steps calls (typical booking window is 1-14 days).
    query_start_pt = min(
        meetings_set_start,
        fetch_start - timedelta(days=21),
    )
    since_iso = query_start_pt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = fetch_end_exclusive.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    set_lo_d = meetings_set_start.date()
    set_hi_d = fetch_end_exclusive.date()
    bs_lo_d  = fetch_start.date()
    bs_hi_d  = fetch_end_exclusive.date()

    # lead_id -> (setter_field_value or None, showed_bool)
    lead_cache = {}

    result = {
        uid: {
            "meetings_set":    defaultdict(int),
            "meetings_booked": defaultdict(int),
            "meetings_shown":  defaultdict(int),
        } for uid in setter_to_uid.values()
    }

    total_seen = 0
    prescreen_pass = 0
    n_set = 0
    n_booked = 0
    n_shown = 0

    for mtg in close_paginate_skip("/activity/meeting/", {
        "date_created__gte": since_iso,
        "date_created__lt":  end_iso,
    }):
        total_seen += 1

        # Pre-screen: could this meeting count for anything?
        dc_day = _parse_pt_day(mtg.get("date_created"))
        set_candidate = dc_day is not None and set_lo_d <= dc_day < set_hi_d

        booked_candidate = False
        sa_day = None
        if _is_next_steps(mtg.get("title") or ""):
            sa_day = _parse_pt_day(mtg.get("starts_at"))
            if sa_day is not None and bs_lo_d <= sa_day < bs_hi_d:
                booked_candidate = True

        if not (set_candidate or booked_candidate):
            continue

        lead_id = mtg.get("lead_id")
        if not lead_id:
            continue
        prescreen_pass += 1

        if lead_id not in lead_cache:
            try:
                ld = close_get(f"/lead/{lead_id}/", {
                    "_fields": (
                        f"id,custom.{FIELD_SETTER_NAME},"
                        f"custom.{FIELD_FUNNEL_NAME},"
                        f"custom.{FIELD_FIRST_CALL_SHOWUP}"
                    )
                })
                funnel = get_custom(ld, FIELD_FUNNEL_NAME)
                setter = (get_custom(ld, FIELD_SETTER_NAME) or "").strip()
                showed = (get_custom(ld, FIELD_FIRST_CALL_SHOWUP) or "").strip().lower() == "yes"
                if funnel == "Reactivation Scrapers" and setter in setter_to_uid:
                    lead_cache[lead_id] = (setter, showed)
                else:
                    lead_cache[lead_id] = (None, False)
            except requests.HTTPError:
                lead_cache[lead_id] = (None, False)

        setter, showed = lead_cache[lead_id]
        if not setter:
            continue

        uid = setter_to_uid[setter]

        if set_candidate:
            result[uid]["meetings_set"][dc_day.isoformat()] += 1
            n_set += 1

        if booked_candidate:
            result[uid]["meetings_booked"][sa_day.isoformat()] += 1
            n_booked += 1
            if showed:
                result[uid]["meetings_shown"][sa_day.isoformat()] += 1
                n_shown += 1

    print(f"  scraper meetings bulk: scanned {total_seen} org-wide meetings, "
          f"{prescreen_pass} passed pre-screen, "
          f"{len(lead_cache)} unique leads looked up",
          flush=True)
    print(f"    → set={n_set} · booked={n_booked} (Next Steps titles) · shown={n_shown}",
          flush=True)

    return {
        uid: {k: dict(v) for k, v in data.items()}
        for uid, data in result.items()
    }


def aggregate_scraper_for_period(per_day, start_pt, end_exclusive_pt):
    """Aggregate a scraper's per-day data into period totals."""
    s, e = start_pt.date(), end_exclusive_pt.date()
    oc = _sum_in_range(per_day.get("outbound_calls"),  s, e)
    oe = _sum_in_range(per_day.get("outbound_emails"), s, e)
    os = _sum_in_range(per_day.get("outbound_sms"),    s, e)

    # Flatten closed_leads from in-range days into one list, sorted by value desc
    closed_leads = []
    for day_str, leads in (per_day.get("meetings_closed_leads") or {}).items():
        try:
            d = datetime.strptime(day_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if s <= d < e:
            for ld in (leads or []):
                closed_leads.append({**ld, "day": day_str})
    closed_leads.sort(key=lambda r: r.get("value") or 0, reverse=True)

    return {
        "activities_total": oc + oe + os,
        "activities_breakdown": {
            "outbound_calls":  oc,
            "outbound_emails": oe,
            "outbound_sms":    os,
        },
        "meetings_booked":         _sum_in_range(per_day.get("meetings_booked"),         s, e),
        "meetings_shown":          _sum_in_range(per_day.get("meetings_shown"),          s, e),
        "meetings_closed":         _sum_in_range(per_day.get("meetings_closed"),         s, e),
        "meetings_closed_revenue": _sum_in_range(per_day.get("meetings_closed_revenue"), s, e),
        "meetings_closed_leads":   closed_leads,
    }


# ─ Setter discovery meetings ─────────────────────────────────────────────────

def _meeting_shown(meeting, host_user_id):
    """A discovery meeting is "shown" iff:
       - the meeting status is NOT canceled, AND
       - the host's attendee entry is NOT "declined"
    A no-show in this workflow surfaces as either (a) William declining the
    invite from his calendar (the Calendly→Close integration syncs that back
    as a declined attendee status), or (b) the meeting being canceled outright.
    """
    status = (meeting.get("status") or "").lower()
    if status in ("canceled", "cancelled", "no-show"):
        return False
    for att in (meeting.get("attendees") or []):
        # Match by user_id (internal attendee) — anonymous external attendees
        # don't have user_id populated.
        if att.get("user_id") == host_user_id:
            if (att.get("attendance_status") or "").lower() == "declined":
                return False
            break
    return True


def fetch_setter_discovery_per_day(setter, fetch_start, fetch_end_exclusive):
    """Per-day buckets of discovery meetings this setter hosts.

    A "discovery meeting" = an /activity/meeting/ activity where:
      - user_id == setter's user_id (this person hosts the call), AND
      - title.startswith(setter['discovery_title_prefix']) (e.g. "Vendingpreneurs Quick Discovery")

    Day = the meeting's `starts_at` (when it happened), bucketed in PT.
    `discovery_shown` is the subset that was not declined/canceled.

    Returns:
      {
        "discovery_held":  {day: int},
        "discovery_shown": {day: int},
      }
    """
    uid = setter["user_id"]
    prefix = setter["discovery_title_prefix"]
    since_iso = fetch_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = fetch_end_exclusive.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    held_pd  = defaultdict(int)
    shown_pd = defaultdict(int)
    seen = 0

    # We extend the filter to date_created__gte=fetch_start because Close's
    # /activity/meeting/ filters on date_created, not starts_at. Most discovery
    # meetings are created shortly before they happen so this is fine — we
    # still bucket by starts_at PT, the date the meeting actually occurred.
    for mtg in close_paginate_skip("/activity/meeting/", {
        "user_id":           uid,
        "date_created__gte": since_iso,
    }):
        seen += 1
        title = (mtg.get("title") or "")
        if not title.startswith(prefix):
            continue

        starts_at = mtg.get("starts_at")
        if not starts_at:
            continue
        try:
            dt = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
        except ValueError:
            continue
        day_pt = dt.astimezone(TIMEZONE).date()

        # starts_at could be earlier than fetch_start (rare — old meetings
        # backfilled into the fetch window) or after fetch_end_exclusive
        # (upcoming meetings). Either is fine — we keep them, and the
        # period aggregation slices by day at output time.
        if day_pt < fetch_start.date() or day_pt >= fetch_end_exclusive.date():
            # Outside the fetch window we care about — skip
            continue

        day = day_pt.isoformat()
        held_pd[day]  += 1
        if _meeting_shown(mtg, uid):
            shown_pd[day] += 1

    print(f"  setter {setter['name']}: {sum(held_pd.values())} held, "
          f"{sum(shown_pd.values())} shown (scanned {seen} meetings)",
          flush=True)

    return {
        "discovery_held":  dict(held_pd),
        "discovery_shown": dict(shown_pd),
    }


def aggregate_setter_for_period(per_day, start_pt, end_exclusive_pt,
                                  scraper_per_day=None):
    """Aggregate a setter's per-day data into period totals.

    `scraper_per_day` lets us pull "Discovery Set" from the matching scraper's
    meetings_booked bucket — same setter, same booking event, no second API call.
    Inbound / Outbound revenue come from the setter's own per_day buckets that
    were populated by fetch_closes_per_day.
    """
    s, e = start_pt.date(), end_exclusive_pt.date()
    held  = _sum_in_range(per_day.get("discovery_held"),  s, e)
    shown = _sum_in_range(per_day.get("discovery_shown"), s, e)
    set_count = 0
    if scraper_per_day:
        set_count = _sum_in_range(scraper_per_day.get("meetings_booked"), s, e)
    show_pct = round(100.0 * shown / held, 1) if held else 0.0

    # Inbound / Outbound revenue + lead lists (sliced to period, sorted by value desc)
    inbound_revenue  = _sum_in_range(per_day.get("inbound_revenue"),  s, e)
    outbound_revenue = _sum_in_range(per_day.get("outbound_revenue"), s, e)

    def _flatten_leads(day_dict):
        out = []
        for day_str, leads in (day_dict or {}).items():
            try:
                d = datetime.strptime(day_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if s <= d < e:
                for ld in (leads or []):
                    out.append({**ld, "day": day_str})
        out.sort(key=lambda r: r.get("value") or 0, reverse=True)
        return out

    return {
        "discovery_held":   held,
        "discovery_shown":  shown,
        "discovery_set":    set_count,
        "show_pct":         show_pct,
        "inbound_revenue":  round(inbound_revenue,  2),
        "outbound_revenue": round(outbound_revenue, 2),
        "inbound_leads":    _flatten_leads(per_day.get("inbound_leads")),
        "outbound_leads":   _flatten_leads(per_day.get("outbound_leads")),
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
            set(pd.get("meetings_set",    {})) |
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
                "meetings_set":    pd.get("meetings_set",    {}).get(day, 0),
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
            "activities_mtd_total":          agg["activities_total"],
            "activities_breakdown":          agg["activities_breakdown"],
            "meetings_booked_mtd":           agg["meetings_booked"],
            "meetings_shown_mtd":            agg["meetings_shown"],
            "meetings_closed_ever":          agg["meetings_closed"],
            "meetings_closed_revenue_ever":  round(agg["meetings_closed_revenue"], 2),
            "meetings_closed_leads":         agg["meetings_closed_leads"],
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

    # ─ Backfill mode ──────────────────────────────────────────────────────
    # If the workflow was dispatched with BACKFILL_MONTH=YYYY-MM, pretend `now`
    # is the last day of that month so the whole month falls into the fetch
    # range. We rewrite that month's archives (monthly + every Mon-Fri weekly
    # archive in it) but skip data.json so the live dashboard stays current.
    backfill_month_env = os.environ.get("BACKFILL_MONTH", "").strip()
    if backfill_month_env:
        try:
            year, month = map(int, backfill_month_env.split("-"))
            assert 1 <= month <= 12
        except (ValueError, AssertionError):
            raise ValueError(
                f"BACKFILL_MONTH must be YYYY-MM (got {backfill_month_env!r})"
            )
        first_of_next = (datetime(year + 1, 1, 1, tzinfo=TIMEZONE)
                         if month == 12
                         else datetime(year, month + 1, 1, tzinfo=TIMEZONE))
        now = (first_of_next - timedelta(days=1)).replace(
            hour=12, minute=0, second=0, microsecond=0
        )
        print(f"⚙ BACKFILL MODE — target month: {backfill_month_env}", flush=True)
        print(f"  effective now = {now.isoformat()}", flush=True)
        print(f"  will rewrite archives/data_{backfill_month_env}.json + every "
              f"Mon-Fri week whose Monday is in this month", flush=True)
        print(f"  live data.json will NOT be touched\n", flush=True)
    else:
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

    # Close-date attribution for scraper closes / revenue AND setter Inbound /
    # Outbound revenue.  Single org-wide opp query + one lead lookup each →
    # classifies each closed-won lead for both roles in one pass.
    print("\n[closes] computing scraper + setter close attribution", flush=True)
    closes_by_uid, setter_closes_by_uid = fetch_closes_per_day(
        SCRAPERS, SETTERS, fetch_start, month_end
    )
    for uid, closes in closes_by_uid.items():
        if uid in scraper_per_day_by_uid:
            scraper_per_day_by_uid[uid].update(closes)

    # ─ Scraper meetings (Set / Booked / Shown) via single org-wide query ────
    # ALL three metrics come from one meeting-activity pull:
    #   - Set:    counted by date_created (any meeting on a Reactivation Scrapers
    #             lead with matching Setter Name)
    #   - Booked: counted by starts_at, ONLY meetings whose title matches one
    #             of NEXT_STEPS_TITLE_PREFIXES. Replaces the older FSCBD-based
    #             logic that missed 2nd/3rd Next Steps meetings on the same lead.
    #   - Shown:  subset of Booked where lead's First Call Show Up = "Yes"
    #
    # Scoped narrower than fetch_start on normal runs — we only need meetings-set
    # for the archives we're actually writing on this run (current + 2 backfill
    # weeks). In backfill mode we cover the full target month.
    if backfill_month_env:
        meetings_set_start = fetch_start
    else:
        meetings_set_start = earliest_week
    print(f"\n[scrapers] fetching meetings (bulk: set / booked / shown, "
          f"set range {meetings_set_start.date()} → "
          f"{(month_end - timedelta(days=1)).date()})", flush=True)
    scraper_meetings_by_uid = fetch_scraper_meetings_bulk(
        SCRAPERS, meetings_set_start, fetch_start, month_end
    )
    for uid, metrics in scraper_meetings_by_uid.items():
        if uid in scraper_per_day_by_uid:
            # Replaces the (now-removed) FSCBD-based meetings_booked/shown.
            scraper_per_day_by_uid[uid]["meetings_set"]    = metrics["meetings_set"]
            scraper_per_day_by_uid[uid]["meetings_booked"] = metrics["meetings_booked"]
            scraper_per_day_by_uid[uid]["meetings_shown"]  = metrics["meetings_shown"]

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
            "activities_total":         w.get("activities_mtd_total", 0),
            "activities_breakdown":     w.get("activities_breakdown", {}),
            "meetings_booked":          w.get("meetings_booked_mtd", 0),
            "meetings_shown":           w.get("meetings_shown_mtd", 0),
            "meetings_closed":          w.get("meetings_closed_ever", 0),
            "meetings_closed_revenue":  w.get("meetings_closed_revenue_ever", 0),
            "meetings_closed_leads":    w.get("meetings_closed_leads", []),
        }
        scrapers_combined.append({**s, "wtd": wtd_block})

    # ─ Setter discovery meetings ──────────────────────────────────────────
    # Same person may also hold their own discovery calls. We fetch meeting
    # activities once (per setter) over the fetch_start range and bucket per-day,
    # then aggregate for MTD + WTD + each backfill week from the same per-day data.
    print("\n[setters] fetching discovery meetings", flush=True)
    setter_per_day_by_uid = {}
    setter_meta = []
    for setter_cfg in SETTERS:
        per_day = fetch_setter_discovery_per_day(setter_cfg, fetch_start, month_end)
        setter_per_day_by_uid[setter_cfg["user_id"]] = per_day
        setter_meta.append({"user_id": setter_cfg["user_id"], "name": setter_cfg["name"]})

    # Merge Inbound / Outbound close attribution (computed earlier alongside
    # scraper closes) into each setter's per_day dict so aggregate_setter_for_period
    # can slice by period.
    for uid, closes in setter_closes_by_uid.items():
        if uid in setter_per_day_by_uid:
            setter_per_day_by_uid[uid].update(closes)

    def _build_setters_view(start_pt, end_pt):
        out = []
        for meta in setter_meta:
            uid = meta["user_id"]
            agg = aggregate_setter_for_period(
                setter_per_day_by_uid.get(uid, {}),
                start_pt, end_pt,
                scraper_per_day_by_uid.get(uid),
            )
            out.append({**meta, **agg})
        out.sort(key=lambda x: x["discovery_held"], reverse=True)
        return out

    setters_mtd = _build_setters_view(month_start, month_end)
    setters_wtd = _build_setters_view(week_start, week_end_excl)
    by_uid_setters_wtd = {x["user_id"]: x for x in setters_wtd}
    setters_combined = []
    for sm in setters_mtd:
        w = by_uid_setters_wtd.get(sm["user_id"], {})
        wtd_block = {
            "discovery_held":   w.get("discovery_held",   0),
            "discovery_shown":  w.get("discovery_shown",  0),
            "discovery_set":    w.get("discovery_set",    0),
            "show_pct":         w.get("show_pct",         0.0),
            "inbound_revenue":  w.get("inbound_revenue",  0),
            "outbound_revenue": w.get("outbound_revenue", 0),
            "inbound_leads":    w.get("inbound_leads",    []),
            "outbound_leads":   w.get("outbound_leads",   []),
        }
        setters_combined.append({**sm, "wtd": wtd_block})

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
        "setters": setters_combined,
    }

    repo_root = Path(__file__).resolve().parent.parent
    if not backfill_month_env:
        (repo_root / "data.json").write_text(json.dumps(output, indent=2))
        print(f"\nwrote {repo_root / 'data.json'}", flush=True)
    else:
        print(f"\n(backfill mode — data.json left untouched)", flush=True)

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

        # Reps & scrapers & setters — period totals at top level (this whole file IS the period)
        rep_view     = _build_rep_view(rep_meta, rep_per_day_by_uid,
                                        ws, we_excl, biz_days)
        scraper_view = _build_scraper_view(scraper_meta, scraper_per_day_by_uid,
                                            ws, we_excl)
        setter_view  = []
        for sm in setter_meta:
            uid = sm["user_id"]
            agg = aggregate_setter_for_period(
                setter_per_day_by_uid.get(uid, {}),
                ws, we_excl,
                scraper_per_day_by_uid.get(uid),
            )
            setter_view.append({**sm, **agg})
        setter_view.sort(key=lambda x: x["discovery_held"], reverse=True)

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
            "setters": setter_view,
        }
        path = archives / f"week_{label}.json"
        path.write_text(json.dumps(week_archive, indent=2))
        print(f"wrote {path}", flush=True)
        return path

    if backfill_month_env:
        # Rewrite every Mon-Fri week whose Monday falls within the target month
        d = month_start
        weeks_written = 0
        while d < month_end:
            if d.weekday() == 0:   # Monday
                _write_week_archive(
                    d, d + timedelta(days=5), d.strftime("%Y-%m-%d"), 5
                )
                weeks_written += 1
            d += timedelta(days=1)
        print(f"backfill: rewrote {weeks_written} weekly archives for {backfill_month_env}",
              flush=True)
    else:
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
