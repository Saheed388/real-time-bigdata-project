import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType
)
from pyspark.sql.functions import from_json, col, from_unixtime
import snowflake.connector

# Load env
load_dotenv()

# Kafka config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "reddit-json-stream")

# Snowflake config
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SF_ROLE = os.getenv("SNOWFLAKE_ROLE", None)
TARGET_TABLE = os.getenv("SNOWFLAKE_TABLE", "REDDIT_STREAM")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reddit-sf")

# Validate env vars
required = {
    "SNOWFLAKE_ACCOUNT": SF_ACCOUNT,
    "SNOWFLAKE_USER": SF_USER,
    "SNOWFLAKE_PASSWORD": SF_PASSWORD,
    "SNOWFLAKE_WAREHOUSE": SF_WAREHOUSE,
    "SNOWFLAKE_DATABASE": SF_DATABASE,
}
missing = [k for k, v in required.items() if not v]
if missing:
    logger.error(f"Missing required Snowflake env vars: {missing}. Aborting.")
    raise SystemExit(1)

# Spark Session
spark = SparkSession.builder \
    .appName("RedditKafkaToSnowflake") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Reddit schema
reddit_schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("upvote_ratio", DoubleType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("url", StringType(), True),
    StructField("selftext", StringType(), True)
])

# Read from Kafka
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
parsed = raw.selectExpr("CAST(value AS STRING) AS value_str", "timestamp AS kafka_timestamp", "partition AS kafka_partition", "offset AS kafka_offset") \
    .withColumn("json", from_json(col("value_str"), reddit_schema)) \
    .select(
        col("json.id").alias("id"),
        col("json.title").alias("title"),
        col("json.author").alias("author"),
        col("json.subreddit").alias("subreddit"),
        col("json.score").alias("score"),
        col("json.upvote_ratio").alias("upvote_ratio"),
        col("json.num_comments").alias("num_comments"),
        from_unixtime(col("json.created_utc")).cast(TimestampType()).alias("created_utc"),
        col("json.url").alias("url"),
        col("json.selftext").alias("selftext"),
        col("kafka_timestamp"),
        col("kafka_partition"),
        col("kafka_offset")
    )

# Snowflake options
sfURL = f"{SF_ACCOUNT}.snowflakecomputing.com" if not SF_ACCOUNT.endswith(".snowflakecomputing.com") else SF_ACCOUNT
sfOptions = {
    "sfURL": sfURL,
    "sfUser": SF_USER,
    "sfPassword": SF_PASSWORD,
    "sfDatabase": SF_DATABASE,
    "sfSchema": SF_SCHEMA,
    "sfWarehouse": SF_WAREHOUSE,
}
if SF_ROLE:
    sfOptions["sfRole"] = SF_ROLE

# Create Snowflake table
full_table_name = f"{SF_DATABASE}.{SF_SCHEMA}.{TARGET_TABLE}"
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
    id VARCHAR(128),
    title VARCHAR(1024),
    author VARCHAR(256),
    subreddit VARCHAR(256),
    score INTEGER,
    upvote_ratio FLOAT,
    num_comments INTEGER,
    created_utc TIMESTAMP_NTZ,
    url VARCHAR(2048),
    selftext TEXT,
    kafka_timestamp TIMESTAMP_NTZ,
    kafka_partition INTEGER,
    kafka_offset BIGINT
)
"""

logger.info("Connecting to Snowflake to ensure target table exists...")
try:
    conn_kwargs = {
        "user": SF_USER,
        "password": SF_PASSWORD,
        "account": SF_ACCOUNT,
        "warehouse": SF_WAREHOUSE,
        "database": SF_DATABASE,
        "schema": SF_SCHEMA,
    }
    if SF_ROLE:
        conn_kwargs["role"] = SF_ROLE
    with snowflake.connector.connect(**conn_kwargs) as conn:
        with conn.cursor() as cur:
            logger.info(f"Executing DDL to create table: {full_table_name}")
            cur.execute(create_table_sql)
            logger.info("DDL executed successfully.")
except Exception as e:
    logger.exception("Failed to create or verify Snowflake table. Aborting.")
    raise

# Write batch to Snowflake
def write_batch_to_snowflake(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id} is empty. Skipping write.")
        return

    batch_df = batch_df.repartition(6, col("subreddit"))  
    rows = batch_df.cache().count()  # 
    logger.info(f"Writing batch {batch_id} with {rows} rows to Snowflake...")

    batch_df.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", TARGET_TABLE) \
        .mode("append") \
        .save()


# Start streaming
checkpoint = os.getenv("STREAM_CHECKPOINT", "/tmp/spark_reddit_snowflake_checkpoint")
query = parsed.writeStream \
    .foreachBatch(write_batch_to_snowflake) \
    .option("checkpointLocation", checkpoint) \
    .outputMode("append") \
    .start()

logger.info("Streaming to Snowflake started. Awaiting termination...")
query.awaitTermination()