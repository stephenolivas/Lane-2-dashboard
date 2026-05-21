# Lane 2 Activity Dashboard

GitHub Pages dashboard tracking lead assignments and rep activity for Lane 2 closers.
Pulls from Close CRM every 15 min via GitHub Actions + cron-job.org.

## What it shows

**Calendar (top)** — Day-by-day count of distinct leads assigned to any Lane 2 rep
that day. A lead is counted **once per day** regardless of how many times it
bounces between owners. LTF - Quiz Funnel leads are excluded.

**Rep details (bottom)** — Per Lane 2 rep:
- Currently owned leads (snapshot) + breakdown by `⚠️ Lane 2 Handraiser` value
- Activities MTD, outbound calls MTD, outbound emails MTD
- Leads with **zero communications ever** (the actionable "untouched pipeline" number)
- Calls booked MTD (uses the `First Sales Call Booked Date` field)
- Deals closed / lost MTD (Sales Pipeline only)

Rows are sorted by **most leads w/ 0 comms first** so the stalest pipeline floats to the top.

## Setup (one-time)

1. **Add Close API key as a secret**
   - Repo → Settings → Secrets and variables → Actions → New repository secret
   - Name: `CLOSE_API_KEY`  · Value: your Close API key

2. **Enable GitHub Pages**
   - Repo → Settings → Pages → Build from branch · `main` / `(root)`

3. **Wire up cron-job.org**
   - Create a job that POSTs to:
     `https://api.github.com/repos/<you>/<this-repo>/actions/workflows/update-dashboard.yml/dispatches`
   - Body: `{"ref":"main"}`
   - Headers: `Accept: application/vnd.github+json`, `Authorization: Bearer <PAT>`
   - Schedule: every 15 min, Mon–Fri, 6 AM–5 PM PT

4. **First run**
   - Trigger manually: Actions tab → "Update Lane 2 Dashboard" → Run workflow.
   - First run takes ~2-5 min depending on lead volume.
   - On success, `data.json` and `archives/data_YYYY-MM.json` will be committed,
     and the dashboard at `https://<you>.github.io/<repo>/` becomes live.

## Files

| File | Purpose |
|---|---|
| `scripts/fetch_data.py` | Pulls from Close, writes JSON |
| `index.html` | Live current-month dashboard |
| `archive.html` | Historical month picker + viewer |
| `data.json` | Current month data (auto-generated) |
| `archives/data_YYYY-MM.json` | Monthly snapshots (auto-generated; last run of month becomes permanent) |
| `archives/index.json` | Catalog of available months (auto-generated) |
| `.github/workflows/update-dashboard.yml` | GitHub Actions workflow |

## Adjusting reps

Edit `LANE2_REPS` in `scripts/fetch_data.py`. The dashboard re-renders to whatever
list is configured — no other files need changes. Mark someone as a team lead by
adding `"is_lead": True`.

## Notes / gotchas

- **30-day event horizon.** Close's `/event/` API retains events for ~30 days.
  As long as the script runs at least once per month, you'll have full data
  for the current month. Archives are immutable snapshots once the next month
  starts running.
- **PT bucketing.** Calendar days are bucketed in PT (fixed `-7` offset).
  Switch to `-8` (PST) in November if needed, or move to `zoneinfo` for proper DST.
- **Activity totals** include all activity types (calls, emails, SMS, notes,
  meetings, custom activities). Outbound calls and emails are counted separately.
- **0-comms** uses `last_communication_date` — a lead with any logged call,
  email, or SMS will have a value here, so this is the cleanest "untouched lead"
  signal.

## Related dashboards

| Dashboard | Repo | Purpose |
|---|---|---|
| Sales Manager Scorecard | `stephenolivas/sales-manager-dashboard` | Weekly team scorecard |
| Rep Performance | `stephenolivas/rep-dashboard` | MTD rep leaderboard |
| MTD Funnel | `stephenolivas/mtd-funnel-dashboard` | Funnel performance |
| Call Capacity | `stephenolivas/call-capacity-dashboard` | Daily booking capacity |
