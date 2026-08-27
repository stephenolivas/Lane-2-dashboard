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
# Scrapers reach out to leads and book them onto closers' calendars via unique
# Calendly links. Attribution: the LEAD's `Reactivation - Setter Name` custom
# field gets stamped with the scraper's name by the update_field.py automation.
# Each entry needs: Close user_id (for activity attribution — calls/emails/sms)
# and the exact string in the Setter Name dropdown (case + spaces sensitive).
#
# Source of truth for the current roster + Calendly link titles:
# SCRAPER_SETTER_SETUP_082626.md (in the project files).
# Amy Mulch is on that doc but excluded here — she doesn't have a Close user
# account yet, so her activities can't be tracked.
SCRAPERS = [
    {"name": "Charlie Ingram",
     "user_id": "user_yZWJTiMjUBfJt8pUPQG6hS7QfKUxwt322aYEABSUrQb",
     "setter_field_value": "Charlie Ingram"},
    {"name": "Jacob Hepner",
     "user_id": "user_IeWR2TlhpjqoXy3K6jX7u9C8c83iBnHXSIvFZpotF3z",
     "setter_field_value": "Jacob Hepner"},
    {"name": "Vince Bartolini",
     "user_id": "user_dQi0iL0igjCKtEXPSsv8ALDZMAz9orJxL60O7Q921jy",
     "setter_field_value": "Vince Bartolini"},
    {"name": "Pearl Sathekge",
     "user_id": "user_0SuNg0OWd2reYMeyuDVqiVvjiGcRiFheKKOXXZpyaPZ",
     "setter_field_value": "Pearl Sathekge"},
    {"name": "Kelly Schrader",
     "user_id": "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7",
     "setter_field_value": "Kelly Schrader"},
    {"name": "Jacob Herbig",
     "user_id": "user_p2y1gLbIgUb9xognGTvuXoRpzp4Ro8QkO20ltgF1CvJ",
     "setter_field_value": "Jacob Herbig"},
    {"name": "William Nowak",
     "user_id": "user_ZNKG1S9eI71qxhSozBK4jskTVtJqXzfNCPWqmADRR9F",
     "setter_field_value": "William Nowak"},
    {"name": "August Young",
     "user_id": "user_wH5PGq1Wm84UW6KrKCt6YCioWocmlffYkbadH6rN43H",
     "setter_field_value": "August Young"},
    {"name": "Spencer Reynolds",
     "user_id": "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",
     "setter_field_value": "Spencer Reynolds"},
    {"name": "Cassie Caraballo",
     "user_id": "user_Hoijs8g8hxab7NN7tMVvC4dpzwHcxSgkIuHeBRphyUL",
     "setter_field_value": "Cassie Caraballo"},
    {"name": "Jessica Zatkin",
     "user_id": "user_WmBJj4uIsE9WRLKMn5Y1i8MinIDJG5GjOHPeX2sUJCp",
     "setter_field_value": "Jessica Zatkin"},
    {"name": "Abigail Garza",
     "user_id": "user_O9qFgDidrldSA1zU3pKPpz5zUbCcNpoEBTCrtAolDUi",
     "setter_field_value": "Abigail Garza"},
    {"name": "Connor George",
     "user_id": "user_YlAbrpKa9iKWFt351Dk1BC4Cmr4SXHKDsSDMG4hnVHi",
     "setter_field_value": "Connor George"},
    {"name": "Dana Lesiuk",
     "user_id": "user_OsKxvuqk3YYRh22NXqonG3PbfFoTC39bn4vGFRKBdMZ",
     "setter_field_value": "Dana Lesiuk"},
    {"name": "Naria Torres",
     "user_id": "user_PBMfAYkPSkMaYK58gXuG70Vu1SLd2bsG3Mvys6RZNgY",
     "setter_field_value": "Naria Torres"},
    {"name": "Melia King",
     "user_id": "user_32021LR58tWOSl2MX2nFVMSP8PoaGV1DjZEF0v0yGXs",
     "setter_field_value": "Melia King"},
]

# Setters have a distinct role from scrapers on this dashboard:
#   - William Nowak hosts his own Vendingpreneurs Quick Discovery calls, and
#     also uses a "Vending Consult Call" Calendly link to book follow-ups on
#     his own calendar after those discoveries. Both surface on the Setter tab.
#   - Spencer & Pearl do NOT host discovery calls. They appear on the Setter
#     tab because they're setters in the business flow — the tab shows their
#     Meetings Set + Inbound / Outbound revenue split for their own bookings.
#     (Held / Shown will always be zero for them.)
#
# Fields per setter:
#   - discovery_title_prefix (str | None): title prefix for meetings this setter
#       HOSTS as a discovery call. None means they don't host discos → Held/Shown
#       are zero and the discovery meetings query is skipped entirely.
#   - scraper_setter_field_value (str): matches the setter's SCRAPERS entry —
#       used to bucket their scraper-credited closes as Outbound.
#   - extra_set_title_prefixes (tuple): additional meeting titles that count
#       toward "Meetings Set" on the Setter tab beyond what's in the scraper
#       Meetings Booked. Attribution is via meeting user_id (host of the
#       meeting), not lead Setter Name — used for links like William's
#       Vending Consult Call which don't populate Setter Name.
SETTERS = [
    {"name": "William Nowak",
     "user_id": "user_ZNKG1S9eI71qxhSozBK4jskTVtJqXzfNCPWqmADRR9F",
     "discovery_title_prefix": "Vendingpreneurs Quick Discovery",
     "scraper_setter_field_value": "William Nowak",
     "extra_set_title_prefixes": ("Vending Consult Call",)},
    {"name": "Spencer Reynolds",
     "user_id": "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",
     "discovery_title_prefix": None,
     "scraper_setter_field_value": "Spencer Reynolds",
     "extra_set_title_prefixes": ()},
    {"name": "Pearl Sathekge",
     "user_id": "user_0SuNg0OWd2reYMeyuDVqiVvjiGcRiFheKKOXXZpyaPZ",
     "discovery_title_prefix": None,
     "scraper_setter_field_value": "Pearl Sathekge",
     "extra_set_title_prefixes": ()},
]

# Close custom field IDs (verified from sibling dashboards)
FIELD_LEAD_OWNER         = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
FIELD_HANDRAISER         = "cf_Q1hRv8It46xsAEmpv4PRKdI1y0sPJnrnQrgRbIlF8uL"
FIELD_FUNNEL_NAME        = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
FIELD_FIRST_CALL_BOOKED  = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
FIELD_FIRST_CALL_SHOWUP  = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"
FIELD_SETTER_NAME        = "cf_vz6kNiu4ItFxRA8Y9HKlWIoQMq3TsdaQqKekQ2YuxVk"

# Meeting outcome IDs (queried from Close org outcomes)
# Used to determine if a meeting was Shown (previously we used lead's
# First Call Show Up field, which only tracked the first call; the outcome
# is per-meeting so it handles 2nd/3rd Next Steps meetings correctly).
OUTCOME_COMPLETED_ID = "outcome_032Djn4dfeNuEoCunojA7K"

EXCLUDED_FUNNELS = {"LTF - Quiz Funnel"}

# Title prefixes that identify a scraper-booked "Next Steps" meeting. Each
# scraper has a unique title on their Calendly link (the automation uses the
# title to attribute the meeting to them). Source of truth for the current
# list: SCRAPER_SETTER_SETUP_082626.md.
NEXT_STEPS_TITLE_PREFIXES = (
    "Vendingpreneurs - Next Steps Call",       # Charlie Ingram
    "Vendingpreneurs Call - Next Steps",       # Jacob Hepner
    "Vendingpreneurs Next Steps Call",         # Vince Bartolini
    "Vendingpreneurs Next Steps Session",      # Pearl Sathekge
    "Vendingpreneurs Discovery - Next Steps",  # Kelly Schrader
    "Vendingpreneurs - Next Steps",            # Jacob Herbig
    "Vendingpreneur Next Steps",               # William Nowak (reactivations)
    "Vending Discovery Call - Next Steps",     # August Young
    "Vending Discovery - Next Steps",          # Spencer Reynolds
    "Vending Opportunity - Next Steps",        # Cassie Caraballo
    "Vendingpreneurs Connect - Next Steps",    # Jessica Zatkin
    "Vending Success - Next Steps",            # Abigail Garza
    "Vendingpreneurs Momentum - Next Steps",   # Connor George
    "Vendingpreneurs Launch - Next Steps",     # Dana Lesiuk
    "Vendingpreneurs Pathway - Next Steps",    # Naria Torres
    "Vendingpreneurs Blueprint - Next Steps",  # Melia King
    # NOT included: "Vending Consult Call" (William's follow-up-after-disco
    # link). That counts on the Setter tab only, via extra_set_title_prefixes.
    # Also excluded: Amy Mulch's "Vendingpreneurs Strategy - Next Steps" —
    # she has a Calendly link but no Close user account yet.
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
# SCRAPERS
# ============================================================================

def fetch_all_scrapers_activities_per_day(scrapers, fetch_start):
    """Outbound activities per day per scraper — via one org-wide query per
    activity type, filtered by scraper user_ids client-side.

    Prior version made 3 activity queries × N scrapers = 3N per-user queries.
    With ~16 scrapers that's 48 round-trips. Bundling to just 3 org-wide
    queries (call / email / sms) is a major speed-up for a busy org where
    scrapers do most of the outbound activity anyway. We paginate through more
    total rows than strictly needed (non-scraper users are filtered out
    client-side), but pagination is cheap next to per-request overhead.

    Per spec, voicemails are NOT counted — the underlying call activity
    already accounts for the contact attempt.

    Returns:
      {scraper_user_id: {
        "outbound_calls":  {day: n},
        "outbound_emails": {day: n},
        "outbound_sms":    {day: n},
      }}
    """
    scraper_uids = {s["user_id"] for s in scrapers}
    since_iso = fetch_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    result = {uid: {
        "outbound_calls":  defaultdict(int),
        "outbound_emails": defaultdict(int),
        "outbound_sms":    defaultdict(int),
    } for uid in scraper_uids}

    def _bucket(path, outbound_values, bucket_name):
        n_total = 0
        n_scraper = 0
        for act in close_paginate_skip(path, {"date_created__gte": since_iso}):
            n_total += 1
            uid = act.get("user_id")
            if uid not in scraper_uids:
                continue
            if (act.get("direction") or "").lower() not in outbound_values:
                continue
            ts = act.get("date_created")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                day = dt.astimezone(TIMEZONE).date().isoformat()
                result[uid][bucket_name][day] += 1
                n_scraper += 1
            except ValueError:
                pass
        print(f"  {path}: {n_scraper} scraper items / {n_total} total org-wide",
              flush=True)

    _bucket("/activity/call/",  {"outbound"},              "outbound_calls")
    _bucket("/activity/email/", {"outgoing", "outbound"},  "outbound_emails")
    _bucket("/activity/sms/",   {"outbound", "outgoing"},  "outbound_sms")

    return {uid: {k: dict(v) for k, v in data.items()} for uid, data in result.items()}


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

            # A setter with no discovery_title_prefix (e.g. Spencer, Pearl) can
            # never have Inbound revenue — Inbound requires them to have hosted
            # a discovery call. Skip the (expensive) meeting lookup for them.
            if prefix:
                has_vqd = _lead_has_vqd_by(lead_id, setter_uid, prefix)
            else:
                has_vqd = False
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
      - **Meetings Shown** — subset of Booked where the meeting's
        `outcome_id` is Completed. Per-meeting attribute, so a lead with
        multiple Next Steps meetings each get their own shown determination.

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

    # lead_id -> setter_field_value (or None if lead doesn't attribute to any
    # configured scraper). No shown state cached — shown is per-meeting now.
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
                        f"custom.{FIELD_FUNNEL_NAME}"
                    )
                })
                funnel = get_custom(ld, FIELD_FUNNEL_NAME)
                setter = (get_custom(ld, FIELD_SETTER_NAME) or "").strip()
                if funnel == "Reactivation Scrapers" and setter in setter_to_uid:
                    lead_cache[lead_id] = setter
                else:
                    lead_cache[lead_id] = None
            except requests.HTTPError:
                lead_cache[lead_id] = None

        setter = lead_cache[lead_id]
        if not setter:
            continue

        uid = setter_to_uid[setter]

        if set_candidate:
            result[uid]["meetings_set"][dc_day.isoformat()] += 1
            n_set += 1

        if booked_candidate:
            result[uid]["meetings_booked"][sa_day.isoformat()] += 1
            n_booked += 1
            if _meeting_shown(mtg):
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

def _meeting_shown(meeting):
    """A meeting counts as "shown" iff its outcome_id is Completed.

    We switched from the earlier attendee-based check (canceled meeting OR host
    attendee status=declined) to the native meeting outcome field. The outcome
    is a per-meeting attribute set by whoever runs the call in Close, so it
    correctly handles 2nd/3rd Next Steps meetings on the same lead — the older
    approach that relied on the LEAD's `First Call Show Up` field could only
    ever tell us about the first call.
    """
    return meeting.get("outcome_id") == OUTCOME_COMPLETED_ID


def fetch_setter_discovery_per_day(setter, fetch_start, fetch_end_exclusive):
    """Per-day counts for a setter's own meetings, in three buckets:

      - discovery_held  — meetings this setter HOSTS as a discovery call
        (title.startswith(discovery_title_prefix)), bucketed by starts_at PT
      - discovery_shown — subset of held where outcome = Completed
      - extra_set       — meetings whose title matches any of
        `extra_set_title_prefixes` (e.g. William's Vending Consult Call),
        credited to this setter via meeting user_id and bucketed by
        date_created PT. Adds to the setter's Meetings Set on the tab.

    Setters without a `discovery_title_prefix` (Spencer, Pearl) return zero
    counts for held / shown — the per-lead activity queries are skipped
    entirely for them.

    Returns:
      {"discovery_held": {day: int},
       "discovery_shown": {day: int},
       "extra_set": {day: int}}
    """
    uid = setter["user_id"]
    disco_prefix = setter.get("discovery_title_prefix")
    extra_prefixes = tuple(setter.get("extra_set_title_prefixes") or ())

    held_pd  = defaultdict(int)
    shown_pd = defaultdict(int)
    extra_pd = defaultdict(int)

    # Nothing to fetch → skip the API call entirely.
    if not disco_prefix and not extra_prefixes:
        return {"discovery_held": {}, "discovery_shown": {}, "extra_set": {}}

    since_iso = fetch_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    fs_d = fetch_start.date()
    fe_d = fetch_end_exclusive.date()
    seen = 0

    for mtg in close_paginate_skip("/activity/meeting/", {
        "user_id":           uid,
        "date_created__gte": since_iso,
    }):
        seen += 1
        title = (mtg.get("title") or "")

        # ─ Discovery held / shown (bucketed by starts_at) ─
        if disco_prefix and title.startswith(disco_prefix):
            sa_day = _parse_pt_day(mtg.get("starts_at"))
            if sa_day is not None and fs_d <= sa_day < fe_d:
                day = sa_day.isoformat()
                held_pd[day] += 1
                if _meeting_shown(mtg):
                    shown_pd[day] += 1

        # ─ Extra set (bucketed by date_created) — e.g. Vending Consult Call ─
        if extra_prefixes and any(title.startswith(p) for p in extra_prefixes):
            dc_day = _parse_pt_day(mtg.get("date_created"))
            if dc_day is not None and fs_d <= dc_day < fe_d:
                extra_pd[dc_day.isoformat()] += 1

    print(f"  setter {setter['name']}: {sum(held_pd.values())} held, "
          f"{sum(shown_pd.values())} shown, "
          f"{sum(extra_pd.values())} extra-set (scanned {seen} meetings)",
          flush=True)

    return {
        "discovery_held":  dict(held_pd),
        "discovery_shown": dict(shown_pd),
        "extra_set":       dict(extra_pd),
    }


def aggregate_setter_for_period(per_day, start_pt, end_exclusive_pt,
                                  scraper_per_day=None):
    """Aggregate a setter's per-day data into period totals.

    `scraper_per_day` lets us pull most of "Discovery Set" from the matching
    scraper's meetings_booked bucket (same person, same booking event, no
    extra API call). Any "extra_set" bookings (e.g. William's Vending Consult
    Call follow-ups, which don't populate Setter Name so aren't in the scraper
    count) are added on top.

    Inbound / Outbound revenue come from the setter's own per_day buckets that
    were populated by fetch_closes_per_day.
    """
    s, e = start_pt.date(), end_exclusive_pt.date()
    held  = _sum_in_range(per_day.get("discovery_held"),  s, e)
    shown = _sum_in_range(per_day.get("discovery_shown"), s, e)
    extra_set = _sum_in_range(per_day.get("extra_set"), s, e)
    set_count = extra_set
    if scraper_per_day:
        set_count += _sum_in_range(scraper_per_day.get("meetings_booked"), s, e)
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

    # Backfill the previous 1 completed week (reduced from 2 to keep runtime
    # under the 15-min cron budget with the larger scraper roster). If a week
    # needs a rebuild older than that, use BACKFILL_MONTH.
    backfill_weeks = prev_n_weeks(week_start, 1)
    earliest_week = min([week_start] + [bw["start"] for bw in backfill_weeks])
    fetch_start = min(month_start, earliest_week)

    print(f"Run: {now.isoformat()}", flush=True)
    print(f"Month: {month_label} ({month_start.date()} → "
          f"{(month_end - timedelta(days=1)).date()}) biz_days={biz_days_mtd}", flush=True)
    print(f"Week:  {week_iso} ({week_start.date()} → "
          f"{(week_end_excl - timedelta(days=1)).date()}) biz_days={biz_days_wtd}", flush=True)
    print(f"Backfill weeks: {[bw['label'] for bw in backfill_weeks]}", flush=True)
    print(f"Fetch from: {fetch_start.date()} (covers all of the above)\n", flush=True)

    # ─ Per-scraper activity (batched org-wide, filtered client-side) ──────
    # One org-wide query per activity type covers all scrapers in one pass —
    # far fewer API round-trips than looping per-scraper with the current
    # 16-person roster.
    print("[scrapers] fetching outbound activities (batched org-wide)", flush=True)
    activity_by_uid = fetch_all_scrapers_activities_per_day(SCRAPERS, fetch_start)
    scraper_meta = [{"user_id": s["user_id"], "name": s["name"]} for s in SCRAPERS]
    scraper_per_day_by_uid = {
        s["user_id"]: activity_by_uid.get(s["user_id"], {
            "outbound_calls": {}, "outbound_emails": {}, "outbound_sms": {},
        }) for s in SCRAPERS
    }

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
    scrapers_mtd = _build_scraper_view(scraper_meta, scraper_per_day_by_uid,
                                         month_start, month_end)
    scrapers_wtd = _build_scraper_view(scraper_meta, scraper_per_day_by_uid,
                                         week_start, week_end_excl)

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

    # ─ Calendar slicing (scraper calendar only — rep view retired) ────────
    week_days = _mon_to_fri(week_start)

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
        "week_start": week_start.date().isoformat(),
        "week_end": (week_end_excl - timedelta(days=1)).date().isoformat(),
        "week_label": format_week_label(week_start, week_end_excl - timedelta(days=1)),
        "business_days_elapsed_wtd": biz_days_wtd,
        # Scraper calendar data — MTD
        "scraper_calendar": month_scraper_calendar,
        "scraper_calendar_breakdowns": month_scraper_cal_breakdowns,
        "scraper_daily_breakdowns": scraper_daily_breakdowns_mtd,
        # Scraper calendar data — WTD
        "scraper_week_calendar": week_scraper_calendar,
        "scraper_week_calendar_breakdowns": week_scraper_cal_breakdowns,
        "scraper_week_daily_breakdowns": scraper_daily_breakdowns_wtd,
        # Rep-related fields kept as empty for HTML backwards compat.
        # The rep view has been fully removed; these will render as no-ops.
        "calendar": {},
        "calendar_breakdowns": {},
        "daily_breakdowns": {},
        "week_calendar": {},
        "week_calendar_breakdowns": {},
        "week_daily_breakdowns": {},
        "reps": [],
        # Scrapers + setters
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
        scr_cal, scr_cal_bd = _build_scraper_calendar(
            scraper_meta, scraper_per_day_by_uid, ws, we_excl, _mon_to_fri(ws)
        )
        scr_daily_bd = _build_scraper_daily_breakdowns(
            scraper_meta, scraper_per_day_by_uid, ws, we_excl
        )

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
            "scraper_calendar": scr_cal,
            "scraper_calendar_breakdowns": scr_cal_bd,
            "scraper_daily_breakdowns": scr_daily_bd,
            # Empty rep-related fields kept for HTML backwards compat
            "calendar": {},
            "calendar_breakdowns": {},
            "daily_breakdowns": {},
            "reps": [],
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
