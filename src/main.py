import os
import time
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BQ_PROJECT = os.environ.get("GCP_PROJECT", "early-alert-responses")
BQ_DATASET = "RESPONSES"
USERS_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.users"
STAGING_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.itdo423_textit_full"
DIFF_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.itdo423_sync_diff"
RUNLOG_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.contacts_sync_runlog"

SYNC_PASSWORD = os.environ.get("SYNC_PASSWORD", "")
TEXTIT_TOKEN = os.environ.get("TEXTIT_TOKEN", "")
TEXTIT_BASE = "https://textit.com/api/v2"

# orgcode validity pattern (matches get_all_fields.ps1): lowercase alnum + hyphen, <=40
import re
VALID_ORGCODE = re.compile(r"^[a-z0-9\-]{1,40}$")

# (bq_column, textit_field_key, is_int64) — the 49 diffable fields. Order = staging col order.
FIELD_MAP = [
    ("orgID", "orgid", False), ("orgCode", "orgcode", False), ("userType", "usertype", False),
    ("userWeek", "userweek", True), ("unit", "unit", False), ("veteran", "veteran", False),
    ("gender", "gender", False), ("firstGen", "firstgen", False),
    ("employmentStatus", "employmentstatus", False), ("ethnicity", "ethnicity", False),
    ("graduateAssistant", "graduateassistant", False), ("membershipScope", "membershipscope", False),
    ("class", "class", True), ("startYear", "startyear", False), ("pgy", "pgy", True),
    ("studentType", "studenttype", False), ("grade", "grade", False), ("subscribed", "subscribed", False),
    ("ageapproximate", "ageapproximate", False), ("zipcode", "zipcode", False),
    ("militaryseparationdate", "militaryseparationdate", False), ("vbaconnected", "vbaconnected", False),
    ("vhaconnected", "vhaconnected", False), ("militarybranch", "militarybranch", False),
    ("vhaengaged", "vhaengaged", False), ("state", "state", False), ("source", "source", False),
    ("giftcardsubscriptionreceived", "giftcardsubscriptionreceived", False),
    ("giftcardsubscriptionreceivedts", "giftcardsubscriptionreceivedts", False),
    ("giftcardsubscriptionamount", "giftcardsubscriptionamount", False),
    ("unsubscribereason", "unsubscribereason", False), ("toxicexposure", "toxicexposure", False),
    ("veteranstatusverified", "veteranstatusverified", False), ("enrolledby", "enrolledby", False),
    ("enrollmentmethod", "enrollmentmethod", False), ("campaigncode", "campaigncode", False),
    ("referralprovidedever", "referralprovidedever", False),
    ("referralfollowuputilizedever", "referralfollowuputilizedever", False),
    ("referralfollowuputilizedeverva", "referralfollowuputilizedeverva", False),
    ("testaccount", "testaccount", False), ("checkinrepliestotal", "checkinrepliestotal", True),
    ("unsubscribetime", "unsubscribetime", False), ("va_baa_affiliated", "va_baa_affiliated", False),
    ("sessionactivityall", "sessionactivityall", False), ("enrollmenttype", "enrollmenttype", False),
    ("sessionActivityRecentCount", "sessionactivityrecentcount", True), ("ageband", "ageband", False),
    ("vamc_presumed", "vamc_presumed", False), ("sessionActivityRecent", "sessionactivityrecent", False),
]
VOLATILE = {"userWeek", "sessionActivityRecent", "sessionActivityRecentCount",
            "sessionactivityall", "checkinrepliestotal"}

# Staging table column order: uuid + the 49 textit field keys
STAGING_COLS = ["uuid"] + [tx for (_bq, tx, _i) in FIELD_MAP]


# ---------------------------------------------------------------------------
# TextIt pull
# ---------------------------------------------------------------------------

def _textit_get(url, headers):
    """GET with the TextIt 2,500-req/hr rate limit handled: on a 429, parse the
    'available in N seconds' body, sleep N+3, and retry the SAME request. Holds
    until the request succeeds — never dies on a throttle, never skips a page.
    (ITDO-454. Canonical form shared across contacts-sync, state-vamc-sync,
    backup-textit-flows; mirrors referral-journey-ingest._textit_get.)"""
    attempt = 0
    while True:
        attempt += 1
        resp = requests.get(url, headers=headers, timeout=120)
        if resp.status_code == 429:
            wait = 60
            m = re.search(r"available in (\d+)", resp.text)
            if m:
                wait = int(m.group(1)) + 3
            logger.warning(f"textit 429; sleeping {wait}s (attempt {attempt})")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()


def pull_all_contacts():
    """Paginate the full TextIt contacts list. Returns list of dicts keyed by
    staging column name (uuid + 49 field keys), orgcode cleaned."""
    headers = {"Authorization": f"Token {TEXTIT_TOKEN}"}
    url = f"{TEXTIT_BASE}/contacts.json?page_size=250"
    rows = []
    pages = 0
    while url:
        data = _textit_get(url, headers)
        for c in data.get("results", []):
            fields = c.get("fields", {}) or {}
            row = {"uuid": c.get("uuid")}
            for (_bq, tx, _i) in FIELD_MAP:
                row[tx] = fields.get(tx)
            # clean garbage orgcode -> empty (same rule as get_all_fields.ps1)
            oc = row.get("orgcode")
            if oc and not VALID_ORGCODE.match(oc):
                row["orgcode"] = ""
            rows.append(row)
        pages += 1
        url = data.get("next")
    logger.info(f"Pulled {len(rows)} contacts across {pages} pages")
    return rows


# ---------------------------------------------------------------------------
# Staging load (WRITE_TRUNCATE, all STRING)
# ---------------------------------------------------------------------------

def load_staging(client, rows):
    schema = [bigquery.SchemaField(c, "STRING") for c in STAGING_COLS]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    # Normalize: every value to str or None; keeps load schema all-STRING
    norm = []
    for r in rows:
        norm.append({c: (None if r.get(c) is None else str(r.get(c))) for c in STAGING_COLS})
    job = client.load_table_from_json(norm, STAGING_TABLE, job_config=job_config)
    job.result()
    n = client.get_table(STAGING_TABLE).num_rows
    logger.info(f"Staging loaded: {n} rows")
    return n


# ---------------------------------------------------------------------------
# SQL builders (case-insensitive compare, verbatim write, INT64 cast)
# ---------------------------------------------------------------------------

# Fields where BQ-side '' must stay DISTINCT from TextIt NULL, so a stale empty
# string registers as changed and the MERGE rewrites it to NULL (SET already
# emits NULLIF(...,'')). Scoped to testaccount only pending upstream add-to-db fix
# (add-to-db writes '' for blank STRING fields; dashboard filter is testaccount IS NULL,
# so '' silently drops real contacts off VA-BAA counts). Expand set if we generalize.
BLANK_DISTINCT = {"testaccount"}

def _cmp_bq(bq, isint, alias):
    inner = f"CAST({alias}.`{bq}` AS STRING)" if isint else f"{alias}.`{bq}`"
    if bq in BLANK_DISTINCT:
        # keep '' distinct from NULL on the BQ side; do NOT collapse blank -> NULL
        return f"LOWER(TRIM({inner}))"
    return f"LOWER(NULLIF(TRIM({inner}),''))"

def _cmp_tx(tx):
    return f"LOWER(NULLIF(TRIM(t.`{tx}`),''))"

def _changed_pred(alias):
    return " OR\n      ".join(
        f"{_cmp_bq(bq, isint, alias)} IS DISTINCT FROM {_cmp_tx(tx)}"
        for (bq, tx, isint) in FIELD_MAP
    )

def build_log_sql(run_id):
    blocks = []
    for (bq, tx, isint) in FIELD_MAP:
        vol = "true" if bq in VOLATILE else "false"
        old_raw = f"CAST(s.`{bq}` AS STRING)" if isint else f"s.`{bq}`"
        blocks.append(
            f"    SELECT s.uuid, '{bq}' AS field, "
            f"NULLIF(TRIM({old_raw}),'') AS old_value, NULLIF(TRIM(t.`{tx}`),'') AS new_value, "
            f"{vol} AS is_volatile\n"
            f"    FROM `{USERS_TABLE}` s JOIN tx t ON t.uuid=s.uuid\n"
            f"    WHERE s.uuid IS NOT NULL AND ({_cmp_bq(bq, isint, 's')} IS DISTINCT FROM {_cmp_tx(tx)})"
        )
    union = "\n    UNION ALL\n".join(blocks)
    return f"""INSERT INTO `{DIFF_TABLE}`
  (run_id, uuid, field, old_value, new_value, is_volatile, logged_at)
WITH tx AS (SELECT * FROM `{STAGING_TABLE}`)
SELECT @run_id, uuid, field, old_value, new_value, is_volatile, CURRENT_TIMESTAMP()
FROM (
{union}
)"""

def build_merge_sql():
    set_clause = ",\n    ".join(
        f"`{bq}` = " + (f"SAFE_CAST(NULLIF(TRIM(t.`{tx}`),'') AS INT64)" if isint
                        else f"NULLIF(TRIM(t.`{tx}`),'')")
        for (bq, tx, isint) in FIELD_MAP
    )
    return f"""MERGE `{USERS_TABLE}` U
USING `{STAGING_TABLE}` t
ON U.uuid = t.uuid
WHEN MATCHED AND (
      {_changed_pred('U')}
  )
THEN UPDATE SET
    {set_clause}"""

DDL_DIFF = f"""CREATE TABLE IF NOT EXISTS `{DIFF_TABLE}` (
  run_id STRING, uuid STRING, field STRING, old_value STRING, new_value STRING,
  is_volatile BOOL, logged_at TIMESTAMP
)"""

DDL_RUNLOG = f"""CREATE TABLE IF NOT EXISTS `{RUNLOG_TABLE}` (
  run_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP,
  contacts_pulled INT64, staging_rows INT64, cells_logged INT64,
  rows_patched INT64, status STRING, error STRING
)"""


# ---------------------------------------------------------------------------
# Core sync (callable — lifts directly into a future orchestrator as a step)
# ---------------------------------------------------------------------------

def run_sync():
    client = bigquery.Client(project=BQ_PROJECT)
    run_id = "contacts_sync_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    started = datetime.now(timezone.utc)
    client.query(DDL_DIFF).result()
    client.query(DDL_RUNLOG).result()

    contacts_pulled = staging_rows = cells_logged = rows_patched = 0
    status, error = "success", None
    try:
        rows = pull_all_contacts()
        contacts_pulled = len(rows)
        staging_rows = load_staging(client, rows)

        # STEP 1: log changed cells (run_id parameterized)
        log_job = client.query(
            build_log_sql(run_id),
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
            ),
        )
        log_job.result()
        cells_logged = log_job.num_dml_affected_rows or 0

        # STEP 2: patch users (uniform TextIt-wins, verbatim)
        merge_job = client.query(build_merge_sql())
        merge_job.result()
        rows_patched = merge_job.num_dml_affected_rows or 0
    except Exception as e:
        logger.exception("contacts-sync failed")
        status, error = "error", str(e)

    finished = datetime.now(timezone.utc)
    client.query(
        f"""INSERT INTO `{RUNLOG_TABLE}`
        (run_id, started_at, finished_at, contacts_pulled, staging_rows,
         cells_logged, rows_patched, status, error)
        VALUES (@run_id, @started, @finished, @cp, @sr, @cl, @rp, @status, @error)""",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("started", "TIMESTAMP", started),
            bigquery.ScalarQueryParameter("finished", "TIMESTAMP", finished),
            bigquery.ScalarQueryParameter("cp", "INT64", contacts_pulled),
            bigquery.ScalarQueryParameter("sr", "INT64", staging_rows),
            bigquery.ScalarQueryParameter("cl", "INT64", cells_logged),
            bigquery.ScalarQueryParameter("rp", "INT64", rows_patched),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("error", "STRING", error),
        ]),
    ).result()

    result = {
        "status": status, "run_id": run_id,
        "contacts_pulled": contacts_pulled, "staging_rows": staging_rows,
        "cells_logged": cells_logged, "rows_patched": rows_patched,
    }
    if error:
        result["error"] = error
    return result


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/sync", methods=["POST"])
def sync():
    """Full TextIt -> BQ users sync. Pulls all contacts, loads staging, logs
    every changed cell to itdo423_sync_diff (run_id), MERGEs users
    (uniform TextIt-wins, case-insensitive compare, verbatim write), and writes
    run metadata to contacts_sync_runlog.

    Body: {"password": "<SYNC_PASSWORD>"}
    """
    body = request.get_json(force=True, silent=True) or {}
    if SYNC_PASSWORD and body.get("password") != SYNC_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    if not TEXTIT_TOKEN:
        return jsonify({"status": "error", "message": "TEXTIT_TOKEN not set"}), 500
    result = run_sync()
    code = 200 if result["status"] == "success" else 500
    return jsonify(result), code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
