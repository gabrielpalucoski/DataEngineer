from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder.appName("DeltaLakeExample").getOrCreate()

# Read data from the raw events table stored in parquet format
raw_events = spark.read \
  .parquet("/mnt/parquet/raw_events")

# Transform the data by adding a new column and filtering out unwanted events
transformed_events = raw_events \
  .withColumn("timestamp", from_unixtime(col("timestamp") / 1000, "yyyy-MM-dd HH:mm:ss")) \
  .filter(col("event_type") != "unwanted_event_type")

# Write the transformed data to a Delta Lake table
transformed_events.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/mnt/delta/transformed_events")

# Read the transformed data from the Delta Lake table
delta_events = spark.read \
  .format("delta") \
  .load("/mnt/delta/transformed_events")

# Run aggregation queries on the transformed data
delta_events \
  .groupBy("event_type") \
  .agg(count("*").alias("event_count")) \
  .show()

# Stop the Spark session
spark.stop()
