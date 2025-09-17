End-to-End ETL Pipeline Orchestration for Clickstream & Transaction Data using Cloud Composer (Airflow)

# 1\. Introduction

This document provides a comprehensive guide to building an ETL pipeline using Google Cloud Composer (Airflow). It explains each step theoretically and includes inline code snippets with explanations. The datasets include clickstream data (user activity logs) and transactions data, supplemented with real-time currency conversion from an external API.

## 2.1 Ingest Clickstream Data

Clickstream data represents the sequence of user actions (page views, clicks, navigation) collected from a website or app. We ingest this dataset to understand user behavior patterns.

Code snippet:

def ingest_clickstream(\*\*context):  
df = pd.read_excel('/mnt/data/clickstream.xlsb', engine='pyxlsb')  
context\['ti'\].xcom_push(key='clickstream_data', value=df.to_json(orient='records'))  
context\['ti'\].xcom_push(key='clickstream_rows', value=len(df))

Explanation: The task reads the raw \`.xlsb\` file using pandas with the pyxlsb engine, converts it to JSON, and pushes it to XCom for downstream tasks. It also logs the row count for metadata tracking.

## 2.2 Ingest Transactions Data

Transaction data contains user purchases with timestamps, transaction IDs, amounts, and currencies. It provides financial insights into user activities.

Code snippet:

def ingest_transactions(\*\*context):  
df = pd.read_excel('/mnt/data/transactions.xlsb', engine='pyxlsb')  
context\['ti'\].xcom_push(key='transactions_data', value=df.to_json(orient='records'))  
context\['ti'\].xcom_push(key='transactions_rows', value=len(df))

Explanation: The task ingests the transaction dataset and stores it in XCom. Row count is tracked for metadata.

## 2.3 Ingest Currency API

The exchange rate API (<https://api.exchangerate-api.com/v4/latest/USD>) provides real-time currency conversion rates. This allows normalization of transactions into a consistent currency (USD).

Code snippet:

def ingest_currency_api(\*\*context):  
url = "<https://api.exchangerate-api.com/v4/latest/USD>"  
response = requests.get(url)  
rates = response.json().get('rates', {})  
context\['ti'\].xcom_push(key='exchange_rates', value=rates)

Explanation: This task fetches exchange rates as JSON and pushes them into XCom for use in transformation.

## 2.4 Transformation

This step cleans and integrates datasets. It joins clickstream and transactions on user_id, normalizes amounts to USD using exchange rates, and aggregates metrics.

Code snippet:

def transform(\*\*context):  
clickstream = pd.read_json(io.StringIO(context\['ti'\].xcom_pull(key='clickstream_data')))  
transactions = pd.read_json(io.StringIO(context\['ti'\].xcom_pull(key='transactions_data')))  
rates = context\['ti'\].xcom_pull(key='exchange_rates')  
<br/>df = transactions.merge(clickstream, on='user_id', how='inner')  
df\['amount_usd'\] = df.apply(lambda x: x\['amount'\] / rates.get(x\['currency'\], 1), axis=1)  
<br/>context\['ti'\].xcom_push(key='transformed_data', value=df.to_json(orient='records'))  
context\['ti'\].xcom_push(key='transformed_rows', value=len(df))

Explanation: The task merges transactions with clickstream data, computes normalized USD amounts, and prepares the dataset for validation. Row counts are tracked.

## 2.5 Validation

Validation ensures data quality before loading into storage. Rules include: no nulls in critical fields, positive amounts, and valid currency codes.

Code snippet:

def validate_data(\*\*context):  
df = pd.read_json(io.StringIO(context\['ti'\].xcom_pull(key='transformed_data')))  
fail_reasons = \[\]  
<br/>if df\['transaction_id'\].isna().any(): fail_reasons.append('Missing transaction_id')  
if df\['user_id'\].isna().any(): fail_reasons.append('Missing user_id')  
if (df\['amount'\] <= 0).any(): fail_reasons.append('Non-positive amounts')  
invalid_currencies = df\[~df\['currency'\].isin(context\['ti'\].xcom_pull(key='exchange_rates').keys())\]  
if not invalid_currencies.empty: fail_reasons.append('Invalid currency codes')  
<br/>if fail_reasons:  
raise AirflowException(f'Validation failed: {fail_reasons}')  
<br/>context\['ti'\].xcom_push(key='validation_summary', value={'pass': True})

Explanation: If validation fails, the DAG stops by raising an AirflowException. Otherwise, validation passes.

## 2.6 Load to GCS

After validation, data is stored in Google Cloud Storage as CSV or Parquet. Partitioning by date is recommended for efficient querying.

Code snippet:

def load_to_gcs(\*\*context):  
df = pd.read_json(io.StringIO(context\['ti'\].xcom_pull(key='transformed_data')))  
bucket = storage.Client().bucket(GCS_BUCKET)  
blob = bucket.blob('final/processed_data.json')  
blob.upload_from_string(df.to_json(orient='records'), content_type='application/json')  
context\['ti'\].xcom_push(key='loaded_rows', value=len(df))

Explanation: The final processed dataset is uploaded to a GCS bucket. Row count is logged for metadata.

## 2.7 Metadata Logging

Pipeline observability is achieved by tracking metadata (row counts, validation status). This can be logged into BigQuery or GCS.

Code snippet:

def log_metadata(\*\*context):  
metadata = {  
'clickstream_rows': context\['ti'\].xcom_pull(key='clickstream_rows'),  
'transactions_rows': context\['ti'\].xcom_pull(key='transactions_rows'),  
'transformed_rows': context\['ti'\].xcom_pull(key='transformed_rows'),  
'loaded_rows': context\['ti'\].xcom_pull(key='loaded_rows'),  
'validation': context\['ti'\].xcom_pull(key='validation_summary')  
}  
table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"  
bq_client.insert_rows_json(table_id, \[metadata\])

Explanation: Metadata is inserted into BigQuery for tracking. Alternatively, it can be written to a GCS CSV file.

## 2.8 Monitoring & Alerts

Monitoring is critical for detecting failures. Alerts are configured via email, Slack, and log files in GCS.

Code snippet:

def failure_callback(context):  
dag_id = context\['dag'\].dag_id  
task_id = context\['task_instance'\].task_id  
error = str(context.get('exception'))  
ts = datetime.utcnow().isoformat()  
msg = f"Failure in DAG={dag_id}, Task={task_id}, Time={ts}, Error={error}"  
send_email_smtp(to=ALERT_EMAILS, subject=f"Airflow failure in {dag_id}", html_content=msg)  
append_to_gcs(GCS_BUCKET, 'logs/alerts.log', msg)

Explanation: On task failure, alerts are sent via email, optionally via Slack, and logged into GCS.

# 3\. Conclusion

The ETL pipeline ensures structured ingestion, transformation, validation, and monitoring of clickstream and transaction data. By leveraging Cloud Composer (Airflow), this approach provides scalability, observability, and reliability.
