#!/usr/bin/env python
# coding: utf-8

# ## Airflow DAG for:
# ingest_clickstream -> ingest_transactions -> ingest_currency_api -> transform -> validate_data -> load_to_gcs -> log_metadata
# 
# Key behaviors:
# - Reads clickstream and transactions from GCS (supports .xlsb via pyxlsb; fallback to CSV)
# - Calls exchange rates API: https://api.exchangerate-api.com/v4/latest/USD
# - Transforms + joins datasets
# - Validates (null checks, positive amounts, currency code validity)
# - Writes final dataset to GCS (CSV and Parquet)
# - Logs metadata to BigQuery OR to GCS as CSV
# - Sends alerts on failures and appends alerts to alerts.log in GCS

# In[3]:


from datetime import datetime, timedelta
import json
import io
import os
import traceback

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email_smtp

from google.cloud import storage
from google.cloud import bigquery


# In[ ]:


# ---------- CONFIG ----------
# Replace below placeholders with your actual values:
GCP_PROJECT = "avian-slice-471510-q7"
GCS_BUCKET = "lsa_storyai_internship2025"  # e.g. <your-composer-bucket>
CLICKSTREAM_GCS_PATH = "raw/clickstream.xlsb"         # where you upload clickstream.xlsb
TRANSACTIONS_GCS_PATH = "raw/transactions.xlsb"       # where you upload transactions.xlsb
STAGING_PREFIX = "staging/etl_pipeline"               # where intermediate files will live
OUTPUT_PREFIX = "output/etl_pipeline"                 # where final output lives
ALERTS_LOG_PATH = "logs/alerts.log"                   # path in bucket to append alerts
METADATA_CSV_PATH = "metadata/etl_metadata.csv"       # fallback metadata storage in GCS
BQ_DATASET = "etl_metadata"                           # BigQuery dataset (if using BQ)
BQ_TABLE = "pipeline_runs"                            # BigQuery table for metadata
EXCHANGE_API_URL = "https://api.exchangerate-api.com/v4/latest/USD"


# In[ ]:



# In[5]:


# How to write metadata: set to "bigquery" or "gcs"
METADATA_BACKEND = "gcs"

# Slack webhook (optional) - put in Airflow Variables or Connections in production.
SLACK_WEBHOOK = None  # "https://hooks.slack.com/services/XXXX/YYY/ZZZ"

# Email alert settings (Composer needs SMTP set up)
ALERT_EMAILS = ["sureshav2004@gmail.com"]


# In[7]:


# Utils
storage_client = storage.Client()
bq_client = bigquery.Client()

def _gcs_blob_exists(bucket_name, blob_name):
    bucket = storage_client.bucket(bucket_name)
    return storage.Blob(bucket=bucket, name=blob_name).exists(storage_client)

def download_from_gcs(bucket_name, blob_name) -> bytes:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket_name}/{blob_name} does not exist")
    return blob.download_as_bytes()

def upload_bytes_to_gcs(bucket_name, blob_name, data_bytes, content_type="text/csv"):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data_bytes, content_type=content_type)
    return f"gs://{bucket_name}/{blob_name}"

def append_to_gcs(bucket_name, blob_name, text_line):
    """Append by reading existing content and re-uploading (GCS doesn't have append)"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    existing = ""
    if blob.exists():
        existing = blob.download_as_text()
        if not existing.endswith("\n"):
            existing += "\n"
    new_content = existing + text_line + "\n"
    blob.upload_from_string(new_content, content_type="text/plain")
    return f"gs://{bucket_name}/{blob_name}"


# In[ ]:


# ---------- Task implementations ----------

def ingest_clickstream(**context):
    """Read clickstream source from GCS. Supports .xlsb or CSV. Writes staging CSV to GCS and XComs the staging path & row count."""
    ti = context['ti']
    try:
        raw_bytes = download_from_gcs(GCS_BUCKET, CLICKSTREAM_GCS_PATH)
    except Exception as e:
        raise AirflowException(f"Failed to read clickstream file: {e}")

    # Try to read .xlsb using pandas with pyxlsb
    df = None
    try:
        # If file is .xlsb we expect the user uploaded .xlsb; pandas + pyxlsb can read from bytes via BytesIO
        if CLICKSTREAM_GCS_PATH.lower().endswith(".xlsb"):
            df = pd.read_excel(io.BytesIO(raw_bytes), engine="pyxlsb")
        else:
            # Try CSV fallback
            df = pd.read_csv(io.BytesIO(raw_bytes))
    except Exception as e:
        raise AirflowException(f"Could not parse clickstream file: {e}")

    row_count = len(df)
    staging_path = f"{STAGING_PREFIX}/clickstream_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    upload_bytes_to_gcs(GCS_BUCKET, staging_path, csv_bytes, content_type="text/csv")

    ti.xcom_push(key="clickstream_staging_path", value=staging_path)
    ti.xcom_push(key="clickstream_rows", value=row_count)
    return {"rows": row_count, "staging_gcs": staging_path}

def ingest_transactions(**context):
    ti = context['ti']
    try:
        raw_bytes = download_from_gcs(GCS_BUCKET, TRANSACTIONS_GCS_PATH)
    except Exception as e:
        raise AirflowException(f"Failed to read transactions file: {e}")

    # parse .xlsb or CSV
    try:
        if TRANSACTIONS_GCS_PATH.lower().endswith(".xlsb"):
            df = pd.read_excel(io.BytesIO(raw_bytes), engine="pyxlsb")
        else:
            df = pd.read_csv(io.BytesIO(raw_bytes))
    except Exception as e:
        raise AirflowException(f"Could not parse transactions file: {e}")

    row_count = len(df)
    staging_path = f"{STAGING_PREFIX}/transactions_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    upload_bytes_to_gcs(GCS_BUCKET, staging_path, csv_bytes, content_type="text/csv")

    ti.xcom_push(key="transactions_staging_path", value=staging_path)
    ti.xcom_push(key="transactions_rows", value=row_count)
    return {"rows": row_count, "staging_gcs": staging_path}

def ingest_currency_api(**context):
    ti = context['ti']
    try:
        resp = requests.get(EXCHANGE_API_URL, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        raise AirflowException(f"Failed to fetch exchange rates: {e}")

    # Save the JSON to staging
    staging_path = f"{STAGING_PREFIX}/exchange_rates_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
    upload_bytes_to_gcs(GCS_BUCKET, staging_path, json.dumps(payload).encode("utf-8"), content_type="application/json")

    # Push rates as XCom (small)
    rates = payload.get("rates", {})
    ti.xcom_push(key="exchange_rates_staging_path", value=staging_path)
    ti.xcom_push(key="exchange_rates", value=rates)
    ti.xcom_push(key="exchange_base", value=payload.get("base"))
    ti.xcom_push(key="exchange_date", value=payload.get("date"))


# In[ ]:


def _read_csv_from_gcs_path(gcs_path):
    # gcs_path like 'staging/...csv'
    raw = download_from_gcs(GCS_BUCKET, gcs_path)
    return pd.read_csv(io.BytesIO(raw))

def transform(**context):
    ti = context['ti']
    click_gcs = ti.xcom_pull(key="clickstream_staging_path")
    trans_gcs = ti.xcom_pull(key="transactions_staging_path")
    rates = ti.xcom_pull(key="exchange_rates") or {}

    if not click_gcs or not trans_gcs:
        raise AirflowException("Missing staging paths for transformation")

    click_df = _read_csv_from_gcs_path(click_gcs)
    trans_df = _read_csv_from_gcs_path(trans_gcs)

    # Example transformation:
    # - Ensure timestamps are parsed
    # - Join transactions with clickstream on user/session id (assumes columns named 'user_id' / 'session_id')
    # - Enrich transactions with USD and local currency amounts if currency column exists

    # Parse timestamps if present
    for col in ['timestamp', 'event_time', 'ts']:
        if col in click_df.columns:
            click_df[col] = pd.to_datetime(click_df[col], errors='coerce')
    for col in ['timestamp', 'transaction_time', 'ts']:
        if col in trans_df.columns:
            trans_df[col] = pd.to_datetime(trans_df[col], errors='coerce')

    # Basic join strategy: if 'user_id' exists in both, left-join transactions with aggregated clickstream features
    merged = trans_df.copy()
    if 'user_id' in trans_df.columns and 'user_id' in click_df.columns:
        # simple aggregate: number of clicks per user, last_page
        clicks_agg = click_df.groupby('user_id').agg(
            clicks_count=pd.NamedAgg(column='user_id', aggfunc='count')
        ).reset_index()
        merged = merged.merge(clicks_agg, on='user_id', how='left')

    # Enrich with exchange rates: create amount_usd if transaction has currency and amount
    if 'currency' in merged.columns and 'amount' in merged.columns:
        def _to_usd(row):
            cur = row['currency']
            amt = row['amount']
            if pd.isna(cur) or pd.isna(amt):
                return None
            rate = rates.get(cur)
            if rate is None:
            # if API base is USD and rates map USD->1 or USD->..., here the provided API returns rates of other currencies per base USD
                # If rate not found, return None
                return None
            # The exchangerate API gives rates where 1 USD = rate * CUR? (the exact mapping depends on provider)
            # We'll store amount_in_usd as amount / rate if rate is amount of CUR per 1 USD; check your API docs.
            try:
                return float(amt) / float(rate)
            except Exception:
                return None
        merged['amount_usd'] = merged.apply(_to_usd, axis=1)


# In[ ]:


# write transformed dataframe to GCS staging
    transformed_gcs_path = f"{STAGING_PREFIX}/transformed_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.parquet"
    out_bytes = io.BytesIO()
    merged.to_parquet(out_bytes, index=False)
    upload_bytes_to_gcs(GCS_BUCKET, transformed_gcs_path, out_bytes.getvalue(), content_type="application/octet-stream")

    ti.xcom_push(key="transformed_rows", value=len(merged))
    ti.xcom_push(key="transformed_staging_path", value=transformed_gcs_path)
    return {"rows": len(merged), "staging_gcs": transformed_gcs_path}


# In[9]:


def validate_data(**context):
    ti = context['ti']
    transformed_path = ti.xcom_pull(key="transformed_staging_path")
    rates = ti.xcom_pull(key="exchange_rates") or {}

    if not transformed_path:
        raise AirflowException("No transformed file found for validation")

    # download parquet
    raw = download_from_gcs(GCS_BUCKET, transformed_path)
    df = pd.read_parquet(io.BytesIO(raw))

    # Validation rules:
    fail_reasons = []
    # 1) Null checks for critical fields (define set)
    critical_fields = ['transaction_id', 'user_id', 'amount', 'currency']
    missing_counts = {}
    for col in critical_fields:
        if col in df.columns:
            missing = df[col].isna().sum()
            missing_counts[col] = int(missing)
            if missing > 0:
                fail_reasons.append(f"{missing} nulls in {col}")
        else:
            # if missing entirely, it's a failure
            missing_counts[col] = None
            fail_reasons.append(f"Missing required column: {col}")

    # 2) Positive amounts: amount > 0
    if 'amount' in df.columns:
        nonpos = (df['amount'] <= 0).sum()
        if nonpos > 0:
            fail_reasons.append(f"{int(nonpos)} non-positive amounts")

    # 3) Valid currency codes: check currencies present vs. rates keys (we assume rates keys are valid ISO codes list)
    invalid_currency_count = 0
    if 'currency' in df.columns and rates:
        invalid_currency_count = df[~df['currency'].isin(rates.keys())]['currency'].notna().sum()
        if invalid_currency_count > 0:
            fail_reasons.append(f"{int(invalid_currency_count)} invalid currency codes not found in exchange rates")

    validation_pass = len(fail_reasons) == 0
    validation_summary = {
        "pass": validation_pass,
        "fail_reasons": fail_reasons,
        "missing_counts": missing_counts,
        "non_positive_amounts": int(nonpos) if 'nonpos' in locals() else None,
        "invalid_currency_count": int(invalid_currency_count)
    }

    ti.xcom_push(key="validation_summary", value=validation_summary)

    # If validation fails, raise AirflowException to trigger failure callbacks / stop pipeline
    if not validation_pass:
        raise AirflowException(f"Validation failed: {validation_summary}")

    return validation_summary


# In[ ]:


def load_to_gcs(**context):
    ti = context['ti']
    transformed_path = ti.xcom_pull(key="transformed_staging_path")
    if not transformed_path:
        raise AirflowException("No transformed file to load")

    # Read transformed parquet
    raw = download_from_gcs(GCS_BUCKET, transformed_path)
    df = pd.read_parquet(io.BytesIO(raw))

    out_csv_path = f"{OUTPUT_PREFIX}/final_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    out_parquet_path = f"{OUTPUT_PREFIX}/final_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.parquet"

    # upload CSV
    upload_bytes_to_gcs(GCS_BUCKET, out_csv_path, df.to_csv(index=False).encode('utf-8'), content_type='text/csv')
    # upload parquet
    out_bytes = io.BytesIO()
    df.to_parquet(out_bytes, index=False)
    upload_bytes_to_gcs(GCS_BUCKET, out_parquet_path, out_bytes.getvalue(), content_type='application/octet-stream')

    ti.xcom_push(key="loaded_rows", value=len(df))
    ti.xcom_push(key="loaded_gcs_csv", value=out_csv_path)
    ti.xcom_push(key="loaded_gcs_parquet", value=out_parquet_path)

    return {"rows_loaded": len(df), "gcs_csv": out_csv_path, "gcs_parquet": out_parquet_path}

def log_metadata(**context):
    ti = context['ti']
    dag_run = context.get('dag_run')
    run_id = dag_run.run_id if dag_run else f"manual__{datetime.utcnow().isoformat()}"
    click_rows = ti.xcom_pull(key="clickstream_rows")
    trans_rows = ti.xcom_pull(key="transactions_rows")
    transformed_rows = ti.xcom_pull(key="transformed_rows")
    loaded_rows = ti.xcom_pull(key="loaded_rows")
    validation_summary = ti.xcom_pull(key="validation_summary") or {}

    metadata = {
        "run_id": run_id,
        "run_ts": datetime.utcnow().isoformat(),
        "clickstream_rows": click_rows,
        "transactions_rows": trans_rows,
        "transformed_rows": transformed_rows,
        "loaded_rows": loaded_rows,
        "validation_pass": validation_summary.get("pass"),
        "validation_fail_reasons": json.dumps(validation_summary.get("fail_reasons", []))
    }

    if METADATA_BACKEND == "bigquery":
        # Write to BigQuery
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
        rows_to_insert = [metadata]
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            raise AirflowException(f"BigQuery insert errors: {errors}")
        return {"metadata_target": table_id}
    else:
        # Append metadata to CSV in GCS
        # Read existing metadata CSV if any, append new row
        import csv
        # build CSV line order
        keys = ["run_id", "run_ts", "clickstream_rows", "transactions_rows", "transformed_rows", "loaded_rows", "validation_pass", "validation_fail_reasons"]
        # fetch existing
        new_line = ",".join([str(metadata.get(k, "")) for k in keys])
        # Check if file exists
        full_path = METADATA_CSV_PATH
        # We will append, but since GCS doesn't support append we re-create; helper append_to_gcs handles it.
        append_to_gcs(GCS_BUCKET, full_path, new_line)
        return {"metadata_target": f"gs://{GCS_BUCKET}/{full_path}"}

# ---------- Alerting / failure callback ----------
def _format_alert_message(context):
    dag_id = context.get('dag').dag_id if context.get('dag') else "unknown_dag"
    task_id = context.get('task_instance').task_id if context.get('task_instance') else "unknown_task"
    run_id = context.get('run_id')
    exception = context.get('exception')
    ts = datetime.utcnow().isoformat()
    tb = traceback.format_exc()
    msg = f"ALERT | DAG: {dag_id} | Task: {task_id} | run_id: {run_id} | time: {ts}\nException: {exception}\nTraceback: {tb}"
    return msg

def failure_callback(context):
    message = _format_alert_message(context)
    # send email (if SMTP configured)
    try:
        send_email_smtp(to=ALERT_EMAILS, subject=f"Airflow failure: {context.get('dag').dag_id if context.get('dag') else 'dag'}", html_content=message)
    except Exception as e:
        # ignore email sending failure, still log to GCS
        pass

    # send to Slack if configured (simple webhook)
    if SLACK_WEBHOOK:
        try:
            requests.post(SLACK_WEBHOOK, json={"text": message}, timeout=5)
        except Exception:
            pass

    # append to alerts.log in GCS
    try:
        append_to_gcs(GCS_BUCKET, ALERTS_LOG_PATH, f"{datetime.utcnow().isoformat()} | {message}")
    except Exception:
        # last resort: write to /tmp/alerts.log (will be ephemeral)
        try:
            with open("/tmp/alerts.log", "a") as f:
                f.write(message + "\n")
        except Exception:
            pass


# In[ ]:


# ---------- Define DAG ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ALERT_EMAILS,
    "email_on_failure": False,  # we use custom on_failure_callback
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback
}

with DAG(
    dag_id="etl_pipeline_clicks_transactions",
    default_args=default_args,
    description="ETL pipeline: clickstream + transactions + exchange rates -> transform -> validate -> load",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "clickstream", "transactions"]
) as dag:

    t_ingest_clickstream = PythonOperator(
        task_id="ingest_clickstream",
        python_callable=ingest_clickstream,
        provide_context=True
    )

    t_ingest_transactions = PythonOperator(
        task_id="ingest_transactions",
        python_callable=ingest_transactions,
        provide_context=True
    )

    t_ingest_currency_api = PythonOperator(
        task_id="ingest_currency_api",
        python_callable=ingest_currency_api,
        provide_context=True
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
        provide_context=True
    )

    t_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True
    )

    t_load = PythonOperator(
        task_id="load_to_gcs",
        python_callable=load_to_gcs,
        provide_context=True
    )

    t_log_metadata = PythonOperator(
        task_id="log_metadata",
        python_callable=log_metadata,
        provide_context=True
    )

    # DAG order
    [t_ingest_clickstream, t_ingest_transactions] >> t_ingest_currency_api >> t_transform >> t_validate >> t_load >> t_log_metadata

