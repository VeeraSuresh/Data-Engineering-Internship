from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("process_clicks").getOrCreate()

# Read clickstream
df = spark.read.option("header", "true").csv("gs://dataengg_intershipsep2025/raw_inputs/clickstream.csv")

# Parse timestamps
df = df.withColumn("click_ts",
    F.coalesce(
        F.to_timestamp("click_time", "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp("click_time")
    )
)

# Filter invalid rows
valid = df.filter((F.col("session_id").isNotNull()) & (F.col("click_ts").isNotNull()))
invalid = df.subtract(valid)

# Deduplicate by session_id, keep latest click_ts
w = Window.partitionBy("session_id").orderBy(F.col("click_ts").desc())
dedup = valid.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

# Partition by date(click_time)
out = dedup.withColumn("date", F.to_date("click_ts"))

out.repartition("date").write.mode("overwrite") \
    .partitionBy("date") \
    .parquet("gs://dataengg_intershipsep2025/processed/clicks/")

# Write invalids
invalid.withColumn("ingest_ts", F.current_timestamp()) \
    .write.mode("append").parquet("gs://dataengg_intershipsep2025/errors/clicks_invalid/")

spark.stop()