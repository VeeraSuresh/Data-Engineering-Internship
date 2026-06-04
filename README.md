# Data Engineering Internship
### ETL Pipeline: Transactions & Clickstream Data Processing

## Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that processes transactional and clickstream datasets, enriches transaction records using real-time exchange rates, and stores cleaned outputs in partitioned Google Cloud Storage (GCS).

The solution demonstrates key data engineering concepts including data ingestion, transformation, enrichment, validation, logging, and cloud-based storage.

---

## Objectives

* Extract data from XLSB source files
* Process large datasets efficiently using chunk-based ingestion
* Standardize and clean data
* Convert timestamps to UTC
* Enrich transaction data using exchange rates
* Validate data quality through duplicate and null checks
* Store processed data in partitioned cloud storage
* Implement logging and failure handling

---

## Datasets

### Transactions Dataset

| Attribute | Data Type |
| --------- | --------- |
| txn_id    | String    |
| user_id   | Integer   |
| amount    | Float     |
| currency  | String    |
| txn_time  | Timestamp |

**Statistics**

* Rows: 100,000
* Columns: 5
* Null Values: 0
* Duplicate Records: 0

---

### Clickstream Dataset

| Attribute  | Data Type |
| ---------- | --------- |
| user_id    | Integer   |
| session_id | String    |
| page_url   | String    |
| click_time | Timestamp |
| device     | String    |
| location   | String    |

**Statistics**

* Rows: 200,000
* Columns: 6
* Null Values: 0
* Duplicate Records: 0

---

## ETL Workflow

### 1. Extract

Data is extracted from:

* transactions.xlsb
* clickstream.xlsb

The files are processed in chunks of 50,000 rows to improve scalability and memory efficiency.

Additionally, exchange rates are retrieved from an external API for currency conversion.

---

### 2. Transform

The transformation layer performs:

#### Data Standardization

* Convert column names to lowercase
* Replace spaces with underscores

#### Timestamp Processing

* Convert all timestamps to UTC format

#### Data Quality Checks

* Remove duplicate records
* Validate schema consistency
* Check for missing values

#### Data Enrichment

Transaction records are enriched using exchange rates:

amount_in_usd = amount × exchange_rate

This enables consistent financial reporting across currencies.

---

### 3. Load

Processed datasets are written to Google Cloud Storage (GCS).

Partitioning strategy:

```text
gs://bucket-name/transactions/ingest_date=YYYY-MM-DD/
gs://bucket-name/clickstream/ingest_date=YYYY-MM-DD/
```

Benefits:

* Faster querying
* Improved scalability
* Easier data lifecycle management
* Optimized downstream analytics

---

## Logging & Monitoring

The pipeline includes operational monitoring capabilities:

* Record counts logged at each stage
* Input file validation
* API failure detection
* Processing status tracking
* Warning messages for missing files
* Error handling and exception logging

---

## Architecture

```text
+------------------+
| Raw Data Sources |
+------------------+
| transactions.xlsb|
| clickstream.xlsb |
+------------------+
          |
          v
+----------------------+
| Extraction Layer     |
| Python + Pandas      |
| Chunk Processing     |
+----------------------+
          |
          v
+----------------------+
| Transformation Layer |
| Standardization      |
| UTC Conversion       |
| Deduplication        |
| Data Enrichment      |
+----------------------+
          |
          v
+----------------------+
| Exchange Rate API    |
| Currency Conversion  |
+----------------------+
          |
          v
+----------------------+
| Loading Layer        |
| Google Cloud Storage |
| Partitioned Output   |
+----------------------+
          |
          v
+----------------------+
| Analytics Ready Data |
+----------------------+
```

---

## Technology Stack

### Programming

* Python

### Data Processing

* Pandas
* Pyxlsb

### Cloud

* Google Cloud Storage (GCS)

### APIs

* ExchangeRate API

### Monitoring

* Python Logging

### Data Engineering Concepts

* ETL Pipelines
* Data Validation
* Data Quality Checks
* Data Enrichment
* Partitioning
* Cloud Storage
* Batch Processing

---

## Key Learning Outcomes

* Built an end-to-end ETL pipeline
* Processed large datasets using chunk-based ingestion
* Integrated external APIs for data enrichment
* Implemented cloud storage partitioning
* Applied data quality validation techniques
* Developed logging and monitoring mechanisms
* Worked with Google Cloud Storage for scalable data storage

---

## Future Enhancements

* Apache Airflow orchestration
* Apache Spark processing
* Incremental data loading
* Automated data quality dashboards
* BigQuery integration
* CI/CD deployment pipeline
* Real-time streaming ingestion using Kafka

```
```
