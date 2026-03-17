"""
wikimedia_load_dag.py
---------------------
Single DAG that does everything:
  1. Fetches ALL Wikimedia recent changes using pagination (rccontinue)
     - Uses rcstart/rcend to get edits for a specific 1-hour window
     - Keeps paginating until all edits for that window are fetched
     - Typically gets 3,000-8,000 edits per hour window
  2. Routes directly to correct BigQuery tables:
     - Bot edits    -> wiki_bot_edits
     - Human edits  -> wiki_edits
     - Bad records  -> wiki_dlq
  3. De-duplicates on event_id

Schedule: Every 1 hour
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

import requests
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

PROJECT_ID  = "jenish-my-first-dog"
DATASET_ID  = "wikimedia_streaming"
BUCKET_NAME = "wikimedia-raw-data"
RAW_PREFIX  = "raw"

HUMAN_TABLE = "wiki_edits"
BOT_TABLE   = "wiki_bot_edits"
DLQ_TABLE   = "wiki_dlq"

API_URL = "https://en.wikipedia.org/w/api.php"
API_HEADERS = {
    "User-Agent": (
        "wikimedia-airflow-pipeline/1.0 "
        "(https://github.com/sumanlamichhane984; "
        "sumanlamichhane984@gmail.com)"
    )
}

MAX_PAGES = 20

DEFAULT_ARGS = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

log = logging.getLogger(__name__)


def is_valid(rc: dict) -> bool:
    return bool(rc.get("title") and rc.get("timestamp") and rc.get("user"))


def fetch_all_changes(window_start, window_end):
    rc_start = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    rc_end   = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")

    all_changes = []
    continue_token = None
    page_count = 0

    log.info("Fetching changes from %s to %s", rc_end, rc_start)

    while page_count < MAX_PAGES:
        params = {
            "action":   "query",
            "list":     "recentchanges",
            "rcprop":   "title|ids|sizes|flags|user|userid|comment|timestamp|type",
            "rclimit":  "500",
            "rctype":   "edit|new",
            "rcstart":  rc_start,
            "rcend":    rc_end,
            "rcdir":    "older",
            "format":   "json",
        }

        if continue_token:
            params["rccontinue"] = continue_token

        try:
            resp = requests.get(API_URL, params=params, headers=API_HEADERS, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.error("API request failed on page %d: %s", page_count + 1, exc)
            break

        changes = data.get("query", {}).get("recentchanges", [])
        all_changes.extend(changes)
        page_count += 1

        log.info("Page %d: got %d changes (total so far: %d)", page_count, len(changes), len(all_changes))

        if "continue" in data and "rccontinue" in data["continue"]:
            continue_token = data["continue"]["rccontinue"]
            time.sleep(0.5)
        else:
            log.info("No more pages - all changes fetched!")
            break

    log.info("Pagination complete: %d total changes across %d pages", len(all_changes), page_count)
    return all_changes


def fetch_and_route(**context):
    execution_date = context["execution_date"]
    window_start   = execution_date.replace(minute=0, second=0, microsecond=0)
    window_end     = window_start + timedelta(hours=1)

    ds      = context["ds"]
    run_id  = context["run_id"].replace(":", "_").replace("+", "_")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    win_str = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info("Processing window: %s to %s", window_start, window_end)

    changes = fetch_all_changes(window_start, window_end)
    log.info("Total changes fetched: %d", len(changes))

    if not changes:
        log.warning("No changes found for window %s - %s", window_start, window_end)
        return

    human_rows = []
    bot_rows   = []
    dlq_rows   = []

    for rc in changes:
        try:
            if not is_valid(rc):
                dlq_rows.append({
                    "raw_payload": json.dumps(rc),
                    "error":       "missing required fields",
                    "ingested_at": now_utc,
                })
                continue

            old_len = rc.get("oldlen") or 0
            new_len = rc.get("newlen") or 0

            record = {
                "event_id":         str(rc.get("rcid", "")),
                "revision_id":      rc.get("revid"),
                "wiki":             "enwiki",
                "title":            rc.get("title", ""),
                "normalized_title": (rc.get("title") or "").strip(),
                "namespace":        rc.get("ns"),
                "event_type":       rc.get("type", ""),
                "user":             rc.get("user", ""),
                "user_is_anon":     rc.get("userid") == 0,
                "byte_delta":       new_len - old_len,
                "is_minor":         "minor" in rc,
                "comment":          (rc.get("comment") or "")[:500],
                "server_name":      "en.wikipedia.org",
                "event_timestamp":  rc.get("timestamp"),
                "publisher_ts":     now_utc,
                "ingested_at":      now_utc,
                "window_start":     win_str,
            }

            if rc.get("bot") or "bot" in rc.get("user", "").lower():
                bot_rows.append(record)
            else:
                human_rows.append(record)

        except Exception as exc:
            dlq_rows.append({
                "raw_payload": json.dumps(rc),
                "error":       str(exc),
                "ingested_at": now_utc,
            })

    log.info("Routed -> human: %d | bot: %d | dlq: %d", len(human_rows), len(bot_rows), len(dlq_rows))

    all_rows = human_rows + bot_rows
    if all_rows:
        ndjson      = "\n".join(json.dumps(r) for r in all_rows) + "\n"
        object_name = f"{RAW_PREFIX}/{ds}/{run_id}.ndjson"
        gcs = GCSHook(gcp_conn_id="google_cloud_default")
        gcs.upload(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            data=ndjson.encode("utf-8"),
            mime_type="application/x-ndjson",
        )
        log.info("Saved raw -> gs://%s/%s", BUCKET_NAME, object_name)

    bq     = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    client = bq.get_client()

    def insert_rows_batched(table_id: str, rows: list, batch_size: int = 500):
        if not rows:
            log.info("No rows for %s - skipping", table_id)
            return
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        for i in range(0, len(rows), batch_size):
            batch  = rows[i:i + batch_size]
            errors = client.insert_rows_json(table_ref, batch)
            if errors:
                log.error("BQ batch errors for %s: %s", table_id, errors)
            else:
                log.info("Inserted batch %d-%d -> %s", i, i + len(batch), table_id)
        log.info("All %d rows inserted into %s", len(rows), table_id)

    insert_rows_batched(HUMAN_TABLE, human_rows)
    insert_rows_batched(BOT_TABLE,   bot_rows)
    insert_rows_batched(DLQ_TABLE,   dlq_rows)

    for table_id in [HUMAN_TABLE, BOT_TABLE]:
        dedupe_sql = f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{table_id}` AS
            SELECT * EXCEPT(rn)
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY event_id ORDER BY ingested_at DESC
                ) AS rn
                FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}`
            )
            WHERE rn = 1
        """
        log.info("Running dedupe for %s", table_id)
        client.query(dedupe_sql).result()
        log.info("Dedupe complete for %s", table_id)

    log.info("All done! Processed %d total events for window %s", len(changes), win_str)


with DAG(
    dag_id="wikimedia_load_dag",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["wikimedia", "bigquery", "gcs", "pipeline"],
    description="Fetches ALL Wikipedia recent changes via paginated REST API and routes to BigQuery tables with deduplication.",
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_route_to_bigquery",
        python_callable=fetch_and_route,
    )
