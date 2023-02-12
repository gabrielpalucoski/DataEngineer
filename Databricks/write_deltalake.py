from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("DeltaLakeExample").getOrCreate()

# Create a DataFrame with sample data
data = [("John Doe", 25), ("Jane Doe", 30), ("Jim Brown", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Write the DataFrame to a Delta Lake table
df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/mnt/delta/people")

# Stop the Spark session
spark.stop()
