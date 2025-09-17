from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email_smtp

from datetime import datetime, timedelta
import pandas as pd
import requests
import io
import json
import traceback

from google.cloud import storage, bigquery

# ---------------- CONFIG ----------------
GCP_PROJECT = "avian-slice-471510-q7"
GCS_BUCKET = "lsa_storyai_internship2025"
ALERTS_LOG_PATH = "logs/alerts.log"

# For alerts
ALERT_EMAILS = ["sureshav2004@google.com"]
SLACK_WEBHOOK = None  # put your webhook if you want Slack alerts

# Metadata backend: "bigquery" or "gcs"
METADATA_BACKEND = "bigquery"
BQ_DATASET = "etl_metadata"
BQ_TABLE = "pipeline_runs"

storage_client = storage.Client()
bq_client = bigquery.Client()
# ----------------------------------------


# ✅ Utility: Append logs to GCS
def append_to_gcs(bucket_name, blob_name, text_line):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    existing = ""
    if blob.exists():
        existing = blob.download_as_text()
        if not existing.endswith("\n"):
            existing += "\n"
    new_content = existing + text_line + "\n"
    blob.upload_from_string(new_content, content_type="text/plain")


# ✅ 3. Validation Task
def validate_data(**context):
    ti = context["ti"]

    # Pull transformed dataframe from previous task
    transformed_bytes = ti.xcom_pull(key="transformed_data")
    df = pd.read_json(io.BytesIO(transformed_bytes), orient="records")

    fail_reasons = []

    # --- Null checks ---
    critical_fields = ["transaction_id", "user_id", "timestamp", "amount", "currency"]
    for col in critical_fields:
        if col not in df.columns:
            fail_reasons.append(f"Missing column: {col}")
        elif df[col].isna().sum() > 0:
            fail_reasons.append(f"Nulls found in {col}")

    # --- Positive amounts ---
    if "amount" in df.columns and (df["amount"] <= 0).any():
        fail_reasons.append("Non-positive amounts detected")

    # --- Valid currency codes ---
    rates = ti.xcom_pull(key="exchange_rates")  # assume set earlier
    if "currency" in df.columns and rates:
        invalid_count = df[~df["currency"].isin(rates.keys())].shape[0]
        if invalid_count > 0:
            fail_reasons.append(f"{invalid_count} invalid currency codes")

    # Result
    validation_pass = len(fail_reasons) == 0
    validation_summary = {
        "pass": validation_pass,
        "fail_reasons": fail_reasons,
    }
    ti.xcom_push(key="validation_summary", value=validation_summary)

    if not validation_pass:
        raise AirflowException(f"Validation failed: {fail_reasons}")


# ✅ 4. Log Metadata
def log_metadata(**context):
    ti = context["ti"]
    run_id = context["run_id"]

    metadata = {
        "run_id": run_id,
        "ts": datetime.utcnow().isoformat(),
        "clickstream_rows": ti.xcom_pull(key="clickstream_rows"),
        "transactions_rows": ti.xcom_pull(key="transactions_rows"),
        "transformed_rows": ti.xcom_pull(key="transformed_rows"),
        "loaded_rows": ti.xcom_pull(key="loaded_rows"),
        "validation": ti.xcom_pull(key="validation_summary"),
    }

    if METADATA_BACKEND == "bigquery":
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
        errors = bq_client.insert_rows_json(table_id, [metadata])
        if errors:
            raise AirflowException(f"BigQuery insert error: {errors}")
    else:
        append_to_gcs(GCS_BUCKET, "metadata/etl_metadata.csv", json.dumps(metadata))


# ✅ 5. Alerts
def failure_callback(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    exception = context.get("exception")
    ts = datetime.utcnow().isoformat()

    msg = f"ALERT: DAG={dag_id}, Task={task_id}, Time={ts}, Error={exception}"

    # Email
    try:
        send_email_smtp(to=ALERT_EMAILS, subject=f"Airflow failure in {dag_id}", html_content=msg)
    except Exception:
        pass

    # Slack
    if SLACK_WEBHOOK:
        try:
            requests.post(SLACK_WEBHOOK, json={"text": msg})
        except Exception:
            pass

    # Log to GCS
    append_to_gcs(GCS_BUCKET, ALERTS_LOG_PATH, msg)


# ✅ DAG definition
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
}

with DAG(
    "etl_validation_metadata_alerts",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None,
) as dag:

    t_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )

    t_log_metadata = PythonOperator(
        task_id="log_metadata",
        python_callable=log_metadata,
        provide_context=True,
    )

    t_validate >> t_log_metadata