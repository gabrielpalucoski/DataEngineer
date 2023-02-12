# Importing Required Libraries
from pyspark.sql import SparkSession
from delta.tables import *

# Starting Spark Session
spark = SparkSession.builder.appName("DeltaLakeExample").getOrCreate()

# Defining the Sat, Hub and Link Paths
sat_path = "s3://<your-bucket>/sat"
hub_path = "s3://<your-bucket>/hub"
link_path = "s3://<your-bucket>/link"

# Creating the Sat Table
df = spark.createDataFrame([(1, "John", 30), (2, "Jane", 25), (3, "Jim", 35)], ["id", "name", "age"])
df.write.format("delta").mode("overwrite").save(sat_path)

# Creating the Hub Table
df.write.format("delta").mode("overwrite").save(hub_path)

# Creating the Link to the Hub Table
create_link_to_hub = DeltaTable.forPath(spark, link_path).create(hub_path, overwrite=True)

# Loading Data to the Hub via the Link
df_update = spark.createDataFrame([(4, "Janet", 45), (5, "Jerry", 40)], ["id", "name", "age"])
df_update.write.format("delta").mode("append").save(link_path)

# Reading Data from the Hub
df_hub = spark.read.format("delta").load(hub_path)
df_hub.show()
