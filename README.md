# Week3 Spark Jobs Repository

## Repository Layout

/week3_spark_jobs/  
├─ clickstream_job.py  
├─ transactions_job.py  
├─ requirements.txt  
├─ gcloud_dataproc.sh  
├─ local_test.sh  
├─ bigquery/  
│ ├─ ddl_raw_spark_outputs.sql  
│ ├─ ddl_curated_dw.sql  
│ ├─ load_parquet.sql  
│ └─ error_table.sql  
├─ queries/  
│ └─ week3_analysis.sql  
└─ README.md  

## clickstream_job.py

from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, to_timestamp, to_date, row_number  
from pyspark.sql.window import Window  
import sys  
<br/>INPUT_PATH = sys.argv\[1\] if len(sys.argv) > 1 else "data/clickstream/\*.json"  
OUTPUT_PATH = sys.argv\[2\] if len(sys.argv) > 2 else "gs://{YOUR_BUCKET}/outputs/clickstream/"  
ERROR_PATH = sys.argv\[3\] if len(sys.argv) > 3 else "gs://{YOUR_BUCKET}/errors/clickstream/"  
<br/>spark = SparkSession.builder.appName("clickstream_processing").getOrCreate()  
<br/>raw = spark.read.option("mode", "PERMISSIVE").json(INPUT_PATH)  
<br/>df = raw.withColumn("click_time_ts", to_timestamp(col("click_time")))  
valid = df.filter((col("session_id").isNotNull()) & (col("click_time_ts").isNotNull()))  
invalid = df.subtract(valid)  
<br/>invalid.write.mode("append").parquet(ERROR_PATH)  
<br/>w = Window.partitionBy("session_id").orderBy(col("click_time_ts").desc())  
ranked = valid.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")  
<br/>out = ranked.withColumn("click_date", to_date(col("click_time_ts")))  
out_repart = out.repartition(col("click_date"))  
<br/>(out_repart.write  
.mode("append")  
.partitionBy("click_date")  
.option("compression", "snappy")  
.parquet(OUTPUT_PATH))  
<br/>spark.stop()

## transactions_job.py

from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, to_timestamp, to_date  
import sys  
<br/>TXN_INPUT = sys.argv\[1\] if len(sys.argv) > 1 else "data/transactions/\*.csv"  
EXCHANGE_PATH = sys.argv\[2\] if len(sys.argv) > 2 else "gs://{YOUR_BUCKET}/references/exchange_rates/\*.csv"  
OUTPUT_PATH = sys.argv\[3\] if len(sys.argv) > 3 else "gs://{YOUR_BUCKET}/outputs/transactions/"  
ERROR_PATH = sys.argv\[4\] if len(sys.argv) > 4 else "gs://{YOUR_BUCKET}/errors/transactions/"  
<br/>spark = SparkSession.builder.appName("transactions_processing").getOrCreate()  
<br/>txn = spark.read.option("header", "true").option("inferSchema", "true").csv(TXN_INPUT)  
txn = txn.withColumn("txn_ts", to_timestamp(col("txn_time"))).withColumn("txn_date", to_date(col("txn_ts")))  
<br/>rates = (spark.read.option("header","true").option("inferSchema","true").csv(EXCHANGE_PATH)  
.withColumnRenamed("currency","rate_currency"))  
<br/>joined = txn.join(rates, txn.currency == rates.rate_currency, how="left")  
joined = joined.withColumn("amount_in_usd", col("amount") \* col("rate_to_usd"))  
<br/>valid = joined.filter(col("txn_ts").isNotNull() & col("amount").isNotNull() & col("amount_in_usd").isNotNull())  
invalid = joined.subtract(valid)  
invalid.write.mode("append").parquet(ERROR_PATH)  
<br/>out = valid.withColumn("txn_date", to_date(col("txn_ts")))  
out_repart = out.repartition(col("txn_date"), col("user_id"))  
<br/>(out_repart.write  
.mode("append")  
.partitionBy("txn_date")  
.option("compression", "snappy")  
.parquet(OUTPUT_PATH))  
<br/>spark.stop()

## requirements.txt

pyspark==3.3.2  
google-cloud-storage==2.10.0

## gcloud_dataproc.sh

# !/bin/bash  
PROJECT_ID="your-gcp-project"  
REGION="us-central1"  
CLUSTER_NAME="week3-dataproc"  
ZONE="us-central1-a"  
BUCKET="your-bucket-name"  
<br/>gcloud dataproc clusters create \${CLUSTER_NAME} \\  
\--project=\${PROJECT_ID} \\  
\--region=\${REGION} \\  
\--zone=\${ZONE} \\  
\--single-node \\  
\--image-version=2.1-debian11 \\  
\--optional-components=ANACONDA,JUPYTER \\  
\--enable-component-gateway  
<br/>gsutil cp clickstream_job.py gs://\${BUCKET}/jobs/  
gsutil cp transactions_job.py gs://\${BUCKET}/jobs/  
<br/>gcloud dataproc jobs submit pyspark gs://\${BUCKET}/jobs/clickstream_job.py \\  
\--region=\${REGION} --cluster=\${CLUSTER_NAME} -- gs://\${BUCKET}/raw/clickstream/\*.json gs://\${BUCKET}/outputs/clickstream/ gs://\${BUCKET}/errors/clickstream/  
<br/>gcloud dataproc jobs submit pyspark gs://\${BUCKET}/jobs/transactions_job.py \\  
\--region=\${REGION} --cluster=\${CLUSTER_NAME} -- gs://\${BUCKET}/raw/transactions/\*.csv gs://\${BUCKET}/references/exchange_rates/\*.csv gs://\${BUCKET}/outputs/transactions/ gs://\${BUCKET}/errors/transactions/

## local_test.sh

# !/bin/bash  
python3 clickstream_job.py data/clickstream/\*.json ./local_outputs/clickstream/ ./local_errors/clickstream/  
python3 transactions_job.py data/transactions/\*.csv data/exchange_rates/\*.csv ./local_outputs/transactions/ ./local_errors/transactions/

## bigquery/ddl_raw_spark_outputs.sql

CREATE SCHEMA IF NOT EXISTS \`your-gcp-project.raw_spark_outputs\`;  
<br/>CREATE OR REPLACE TABLE \`your-gcp-project.raw_spark_outputs.clickstream\` (  
session_id STRING,  
user_id STRING,  
click_time TIMESTAMP,  
page STRING,  
country STRING  
)  
PARTITION BY DATE(click_time);  
<br/>CREATE OR REPLACE TABLE \`your-gcp-project.raw_spark_outputs.transactions\` (  
txn_id STRING,  
user_id STRING,  
amount FLOAT64,  
currency STRING,  
txn_time TIMESTAMP,  
amount_in_usd FLOAT64  
)  
PARTITION BY DATE(txn_time);

## bigquery/ddl_curated_dw.sql

CREATE SCHEMA IF NOT EXISTS \`your-gcp-project.curated_dw\`;  
<br/>CREATE OR REPLACE TABLE \`your-gcp-project.curated_dw.transactions\` (  
txn_id STRING,  
user_id STRING,  
amount FLOAT64,  
currency STRING,  
txn_time TIMESTAMP,  
amount_in_usd FLOAT64  
)  
PARTITION BY DATE(txn_time)  
CLUSTER BY user_id;  
<br/>CREATE OR REPLACE TABLE \`your-gcp-project.curated_dw.clickstream\` (  
session_id STRING,  
user_id STRING,  
click_time TIMESTAMP,  
page STRING,  
country STRING  
)  
PARTITION BY DATE(click_time)  
CLUSTER BY user_id;

## bigquery/load_parquet.sql

CREATE OR REPLACE EXTERNAL TABLE \`your-gcp-project.raw_spark_outputs.clickstream_external\`  
OPTIONS (  
format = 'PARQUET',  
uris = \['gs://your-bucket/outputs/clickstream/\*/\*.parquet'\]  
);  
<br/>CREATE OR REPLACE TABLE \`your-gcp-project.raw_spark_outputs.clickstream\`  
PARTITION BY DATE(click_time) AS  
SELECT \* FROM \`your-gcp-project.raw_spark_outputs.clickstream_external\`;

## bigquery/error_table.sql

CREATE OR REPLACE TABLE \`your-gcp-project.curated_dw.clickstream_errors\` (  
raw_payload STRING,  
error_reason STRING,  
load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()  
);  
<br/>CREATE OR REPLACE TABLE \`your-gcp-project.curated_dw.transactions_errors\` (  
raw_payload STRING,  
error_reason STRING,  
load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()  
);

## queries/week3_analysis.sql

\-- Daily active users by country  
SELECT DATE(click_time) AS day, country, COUNT(DISTINCT user_id) AS dau  
FROM \`your-gcp-project.curated_dw.clickstream\`  
WHERE DATE(click_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()  
GROUP BY day, country  
ORDER BY day DESC, country;  
<br/>\-- Revenue per currency vs USD  
SELECT currency, SUM(amount) AS total_amount, SUM(amount_in_usd) AS total_amount_usd  
FROM \`your-gcp-project.curated_dw.transactions\`  
WHERE DATE(txn_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()  
GROUP BY currency  
ORDER BY total_amount_usd DESC;

## README.md

\# Week 3 - Spark on GCP (Dataproc) Deliverables  
<br/>This repo contains PySpark jobs, Dataproc helper scripts, BigQuery DDL, and analytical queries.  
<br/>\## Overview  
\- Setup Dataproc cluster (script gcloud_dataproc.sh)  
\- Run clickstream and transactions PySpark jobs  
\- Spark writes Parquet to GCS partitioned by date columns  
\- Load Parquet into BigQuery datasets: raw_spark_outputs and curated_dw  
\- Maintain error tables  
<br/>\## Partitioning strategy  
\- Partition by date columns (click_date / txn_date)  
\- Repartition in Spark before write by date (and user_id for transactions)  
<br/>\## Error handling  
\- Invalid rows are written by Spark to errors/ in GCS  
\- Periodically load them into BigQuery error tables


# Data-Engineering-Internship
Transactions Dataset

Rows: 100,000

Columns: 5

Schema:

txn_id → String

user_id → Integer

amount → Float

currency → String

txn_time → Timestamp string

Nulls: None

Duplicates: 0

Clickstream Dataset

Rows: 200,000

Columns: 6

Schema:

user_id → Integer

session_id → String

page_url → String

click_time → Timestamp string

device → String

location → String

Nulls: None

Duplicates: 0

⚙️ Pipeline Tasks
Task 1: Explore Datasets

Inspected schemas, null values, and duplicates.

Documented findings in this README.

Task 2: Extract

Read data from .xlsb files in chunks of 50,000 rows.

Fetched currency exchange rates via API.

Task 3: Transform

Standardized column names (lowercase, underscores).

Converted timestamps to UTC timezone.

Removed duplicates.

Enriched transactions with amount_in_usd using exchange rates.

Task 4: Load

Wrote cleaned outputs to Google Cloud Storage (GCS).

Partitioned by ingest_date=YYYY-MM-DD/ for each dataset.

Task 5: Logging & Alerts

Logged row counts at each stage.

Captured API failures or missing inputs.

Task 6: Architecture Diagram
+------------------+       +-------------------+       +-------------------------+
| Raw Data (XLSB)  |  -->  |  ETL Scripts      |  -->  | GCS Partitioned Storage |
| transactions     |       |  (Python, Pandas) |       | ingest_date=YYYY-MM-DD/|
| clickstream      |       |                   |       | transactions,clickstream|
+------------------+       +-------------------+       +-------------------------+
