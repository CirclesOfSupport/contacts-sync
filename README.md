# users-sync

Cloud Run service: nightly full sync of the BigQuery `users` table from TextIt
(source of truth). Automates the ITDO-423 manual sync procedure.

## What it does (`POST /sync`)

1. Pulls all contacts from the TextIt contacts API (49 diffable fields), cleaning
   garbage orgcode values to empty.
2. Loads them into `RESPONSES.itdo423_textit_full` (staging, all STRING, WRITE_TRUNCATE).
3. Logs every changed cell to `RESPONSES.itdo423_sync_diff` (run_id, old_value,
   new_value, is_volatile) — the diff report AND the rollback source.
4. MERGEs `RESPONSES.users`: uniform TextIt-wins, case-insensitive comparison
   (LOWER+TRIM, NULL==''==blank), verbatim writes, 5 INT64 cols via SAFE_CAST.
5. Writes run metadata to `RESPONSES.users_sync_runlog`.

Design rationale and full history: see ITDO-423 in Atlas (`/early_alert/`).

## Scope

- UPDATE-only on contacts present in both systems. Contacts in TextIt with no
  `users` row are NOT inserted (out of scope; mostly unsubscribed/test/demo).
- 5 volatile counter fields (userWeek, sessionActivityRecent,
  sessionActivityRecentCount, sessionactivityall, checkinrepliestotal) are synced
  and tagged is_volatile=true in the diff log (expected, not bugs).
- Does NOT write back to TextIt. TextIt -> BQ only.

## Rollback

Each run is reversible via the value-based reverse-merge keyed on run_id
(`itdo423_rollback_reverse_merge.sql` in kriton-dev/EarlyAlert). It restores
old_value only where the current value still equals what the run wrote.

## Env vars (set in Cloud Run console, Variables & Secrets — NOT in repo)

- `SYNC_PASSWORD` — required in the POST body (matches vamc-sync auth pattern)
- `TEXTIT_TOKEN` — TextIt API token
- `GCP_PROJECT` — defaults to early-alert-responses

## Deploy

Push to main -> Cloud Build trigger -> Cloud Run (us-east1). Same pattern as
vamc-sync. Service account needs BigQuery Data Editor + Job User on
early-alert-responses.

## Schedule

Cloud Scheduler nightly, sequenced before vamc-sync (which reads vamc_presumed
that this job may change). See the nightly-jobs ordering in Atlas.

## Future

- Core sync is a callable `run_sync()` — lifts into a unified nightly
  orchestrator (backup -> contacts sync -> state/vamc -> vamc-sync) when built.
- Delta sync (`?after=` modified-since) as an efficiency enhancement over the
  full pull, with a periodic full-sync safety net.
