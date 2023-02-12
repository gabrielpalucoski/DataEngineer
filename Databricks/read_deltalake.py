from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("DeltaLakeExample").getOrCreate()

# Read data from the Delta Lake table
df = spark.read \
  .format("delta") \
  .load("/mnt/delta/events")

# Show the first 10 rows of the data
df.show(10)

# Stop the Spark session
spark.stop()
