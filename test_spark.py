from pyspark.sql import SparkSession

# If running on HOST machine, use localhost:7077
# If running inside a container in the same 'datanet', use spark-master:7077
spark = SparkSession.builder \
    .appName("DockerSparkTest") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

print("✅ Spark Session created successfully!")

# Simple test data
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["name", "age"])

print("✅ DataFrame created. Showing data:")
df.show()

# Some transformation
df2 = df.groupBy("age").count()
print("✅ Grouped DataFrame:")
df2.show()

spark.stop()
print("✅ Spark test finished successfully!")
