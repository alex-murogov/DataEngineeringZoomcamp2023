from pyspark.sql import SparkSession
import pyspark


# create a SparkSession
spark = SparkSession.builder.appName("PySparkTest").getOrCreate()

# create a dummy dataframe
data = [("Alice", 23), ("Bob", 32), ("Charlie", 45)]
df = spark.createDataFrame(data, ["Name", "Age"])

# print the dataframe
df.show()