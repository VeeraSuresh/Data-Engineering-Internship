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
