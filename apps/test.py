from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSparkConnection") \
    .master("spark://spark-master:7077") \
    .config("spark.log.level", "DEBUG") \
    .getOrCreate()

data = [("test", 1)]
df = spark.createDataFrame(data, ["name", "value"])
df.show()

spark.stop()