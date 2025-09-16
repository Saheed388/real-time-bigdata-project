from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
import praw
import os
import logging
import json
import time
from pyspark.sql.functions import struct
from pyspark.sql.avro.functions import to_avro

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Reddit API setup
try:
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID",'RJTKBKAjjB46HKLve9Y2Fw'),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET",'AENo4iD-r9zXWdvgFOPzR_naruIHQQ'),
        user_agent="my_streaming_pipeline"
    )
    logger.info("Reddit API initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Reddit API: {e}")
    exit(1)

# Kafka configuration (Docker container hostname for Redpanda)
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda-1:9092")
kafka_topic = "reddit-avro-stream"

# Avro schema
avro_schema = {
    "type": "record",
    "name": "RedditPost",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "title", "type": "string"},
        {"name": "author", "type": "string"},
        {"name": "subreddit", "type": "string"},
        {"name": "score", "type": "int"},
        {"name": "upvote_ratio", "type": "float"},
        {"name": "num_comments", "type": "int"},
        {"name": "created_utc", "type": "double"},
        {"name": "url", "type": "string"},
        {"name": "selftext", "type": "string"}
    ]
}
avro_schema_str = json.dumps(avro_schema)

# Spark schema (matches Avro schema)
spark_schema = StructType([
    StructField("id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("upvote_ratio", FloatType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("url", StringType(), True),
    StructField("selftext", StringType(), True)
])

# Initialize Spark session (connect to Spark master in Docker)
spark = SparkSession.builder \
    .appName("RedditAvroStream") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.5.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.files.maxPartitionBytes", "256MB") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "400") \
    .getOrCreate()

# Function to fetch Reddit posts
def fetch_reddit_stream():
    """Yield Reddit posts one at a time for streaming"""
    try:
        for submission in reddit.subreddit("all").stream.submissions(skip_existing=True):
            post = {
                "id": submission.id,
                "title": submission.title or "",
                "author": str(submission.author) if submission.author else "unknown",
                "subreddit": str(submission.subreddit),
                "score": int(submission.score),
                "upvote_ratio": float(submission.upvote_ratio),
                "num_comments": int(submission.num_comments),
                "created_utc": float(submission.created_utc),
                "url": submission.url or "",
                "selftext": submission.selftext or ""
            }
            logger.info(f"Fetched post {submission.id}")
            yield post
    except Exception as e:
        logger.error(f"Error fetching Reddit posts: {e}")
        time.sleep(10)  # Wait before retrying

# Main streaming logic
def process_reddit_stream():
    def foreach_batch_function(df, epoch_id):
        if not df.isEmpty():
            # Serialize to Avro
            df_avro = df.select(to_avro(struct("*"), avro_schema_str).alias("value"))
            # Write to Kafka
            df_avro.writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", kafka_topic) \
                .option("checkpointLocation", "/opt/spark-data/checkpoint") \
                .outputMode("append") \
                .start()
            logger.info(f"Processed batch {epoch_id} with {df.count()} posts to Kafka topic {kafka_topic}")

    reddit_stream = fetch_reddit_stream()
    
    while True:
        try:
            post = next(reddit_stream, None)
            if post:
                df = spark.createDataFrame([post], schema=spark_schema)
                query = df.writeStream \
                    .foreachBatch(foreach_batch_function) \
                    .outputMode("append") \
                    .start()
                query.awaitTermination(timeout=30)
                query.stop()
            else:
                logger.info("No new posts, waiting...")
                time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Received shutdown signal. Stopping stream...")
            break
        except Exception as e:
            logger.error(f"Error in streaming process: {e}")
            time.sleep(10)

    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    process_reddit_stream()
