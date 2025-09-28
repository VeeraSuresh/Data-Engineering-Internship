**1\. Why Spark for Large-Scale ETL?**

**1️⃣ Distributed Processing**

- Spark breaks data into **partitions** and distributes them across a **cluster of nodes**.
- Each node processes its partition **in parallel**, dramatically speeding up computation.
- This makes Spark ideal for **large datasets** that cannot fit on a single machine.

**Example:** Loading 1 TB of transaction data can be split across 50 nodes, reducing processing time from hours to minutes.

**2️⃣ In-Memory Computation**

- Unlike Hadoop MapReduce, which writes intermediate results to disk, Spark **keeps intermediate data in memory**.
- This **reduces I/O overhead** and significantly improves performance for iterative computations.

**Benefit:** Transformations like aggregations, joins, or filtering are much faster in Spark, especially when data is reused across multiple steps in an ETL pipeline.

**3️⃣ Scalability**

- Spark can easily handle **terabytes to petabytes** of structured, semi-structured, and unstructured data.
- Cluster size can be **scaled horizontally** by adding more nodes.
- This makes it suitable for **enterprise-level ETL pipelines** that grow over time.

**Use case:** Processing clickstream logs, financial transactions, or IoT sensor data at scale.

**4️⃣ Connector Ecosystem**

- Spark has **built-in connectors** for major data sources:
  - **GCP services:** Cloud Storage, BigQuery, Pub/Sub
  - **Databases:** MySQL, PostgreSQL, Cassandra
  - **File formats:** CSV, Parquet, Avro, JSON, ORC, XLSB
- This enables **seamless integration** without writing custom connectors.

**Example:** Read raw data from Cloud Storage, transform it, and write it directly to BigQuery in one Spark job.

**5️⃣ Resiliency and Fault Tolerance**

- Spark tracks all operations in a **DAG (Directed Acyclic Graph)**.
- If a node fails, Spark can **recompute only the lost partitions** using lineage information.
- This ensures **robust ETL pipelines** without manual intervention.

**Benefit:** ETL jobs can run reliably in production even with hardware or network failures.

**6️⃣ Unified Framework**

- Spark provides a **single engine** for multiple workloads:
  - **Batch ETL:** Process large static datasets.
  - **Streaming ETL:** Process real-time data streams with Spark Structured Streaming.
  - **Machine Learning:** Use MLlib for predictive models directly on transformed data.
  - **Graph processing:** GraphX for relationship or network analysis.
- This eliminates the need for separate tools for each task, simplifying architecture.

**Advantage:** A company can use Spark for **end-to-end data pipelines**, from ingestion to transformation to analytics, using **one scalable engine**.

**2\. ETL Workflow on GCP with Spark**

**Step A. Run Spark Jobs on GCP Dataproc**

You can run Spark in two ways:

- **Cluster mode (Dataproc cluster):**
  - Create a Dataproc cluster (e.g., 3 worker nodes).
  - Submit Spark jobs via gcloud dataproc jobs submit pyspark job.py --cluster my-cluster.
  - Use GCS buckets (gs://bucket-name/...) as input/output.
- **Serverless mode (Dataproc Serverless for Spark):**
  - No cluster management needed.
  - Run Spark jobs directly:
  - gcloud dataproc batches submit pyspark job.py \\
  - \--region=us-central1 \\
  - \--subnet=default \\
  - \--batch=batch-id \\
  - \--deps-bucket=gs://bucket-name

**Step B. Transform Large Datasets in Parallel**

- Read data from **GCS** or **BigQuery**:
- from pyspark.sql import SparkSession
- spark = SparkSession.builder.appName("etl-job").getOrCreate()
- \# Example: Read CSV from GCS
- df = spark.read.csv("gs://bucket/input-data/\*.csv", header=True, inferSchema=True)
- \# Transformations
- transformed_df = (
- df.withColumn("year", df\["date"\].substr(1,4))
- .filter(df\["amount"\] > 1000)
- )
- Spark automatically distributes the work across cluster workers.

**Step C. Load Results into BigQuery (Partitioned/Clustered Tables)**

- Write to BigQuery directly with Spark connector:
- transformed_df.write \\
- .format("bigquery") \\
- .option("table", "project.dataset.transactions_partitioned") \\
- .option("partitionField", "year") \\
- .option("clusteredFields", "customer_id") \\
- .mode("append") \\
- .save()
- Partitioning helps with **time-based queries**; clustering helps with **filter efficiency**.

**Step D. Incremental Loading, Error Handling, Optimization**

- **Incremental loading (CDC or Delta loads):**
  - Use **watermarking** or **last_updated timestamp** to fetch only new/changed records.
  - Store **checkpoint state** in GCS/BigQuery for resuming jobs.
- **Error handling:**
  - Wrap Spark transformations in try/except blocks.
  - Write rejected/error rows to a **dead-letter GCS bucket** or BigQuery error table.
  - Use df.write.mode("append") carefully to avoid overwriting valid data.
- **Query optimization:**
  - Use **partition pruning** in BigQuery (only scan relevant partitions).
  - Enable **predicate pushdown** with Spark when reading data.
  - Cache reused DataFrames in Spark with .cache().

✅ **In summary:**  
Spark on Dataproc lets you:

- Run jobs (cluster or serverless).
- Transform huge datasets in parallel.
- Load clean data into BigQuery partitioned/clustered tables.
- Apply **incremental strategies, error handling, and optimizations** for real-world pipelines.


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
