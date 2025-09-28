from pyspark.sql import SparkSession, functions as F, types as T
import requests

spark = SparkSession.builder.appName("process_transactions").getOrCreate()

# Read transactions
df = spark.read.option("header", "true").csv("gs://dataengg_intershipsep2025/raw_inputs/transactions.csv")

df = df.withColumn("txn_ts",
    F.coalesce(
        F.to_timestamp("txn_time", "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp("txn_time")
    )
).withColumn("amount", F.col("amount").cast("double"))

valid = df.filter(
    (F.col("txn_id").isNotNull()) &
    (F.col("user_id").isNotNull()) &
    (F.col("currency").isNotNull()) &
    (F.col("amount").isNotNull()) &
    (F.col("txn_ts").isNotNull())
)
invalid = df.subtract(valid)

# Fetch exchange rates
resp = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=20)
j = resp.json()
rates = [(c, float(r)) for c,r in j["rates"].items()]
rate_df = spark.createDataFrame(rates, schema="currency STRING, rate DOUBLE")

joined = valid.join(F.broadcast(rate_df), on="currency", how="left") \
    .withColumn("amount_in_usd", F.col("amount") / F.col("rate"))

out = joined.withColumn("date", F.to_date("txn_ts"))

out.repartition("date","user_id").write.mode("overwrite") \
    .partitionBy("date") \
    .parquet("gs://my-data-bucket/processed/transactions/")

invalid.withColumn("ingest_ts", F.current_timestamp()) \
    .write.mode("append").parquet("gs://my-data-bucket/errors/transactions_invalid/")

spark.stop()