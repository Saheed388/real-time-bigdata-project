import logging
import os
import json
import praw
from kafka import KafkaProducer
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv   # <-- add this

load_dotenv()

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logging.getLogger('kafka').setLevel(logging.DEBUG)  # Enable detailed Kafka logging
logger = logging.getLogger(__name__)

# --- Reddit API Setup ---
try:
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent="my_streaming_pipeline"
    )
    logger.info("Reddit API initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Reddit API: {e}")
    exit(1)

# --- Kafka Configuration ---
server = 'localhost:9092'  # Adjust if your broker uses a different host/port
topic_name = 'reddit-json-stream'
client_id = 'kafka-python-producer-1'

# JSON serializer for Kafka messages
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
def init_producer():
    """Initialize Kafka producer with retry logic"""
    return KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer,
        client_id=client_id
    )

def main():
    # Initialize Kafka producer
    try:
        logger.info("Initializing Kafka producer...")
        producer = init_producer()
        logger.info("Kafka producer initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer after retries: {e}")
        exit(1)

    # Stream Reddit posts
    logger.info("Starting Reddit stream...")
    try:
        for submission in reddit.subreddit("all").stream.submissions(skip_existing=True):
            reddit_data = {
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

            try:
                producer.send(topic_name, value=reddit_data)
                logger.info(f"Sent post {submission.id} to Kafka in JSON format.")
            except Exception as e:
                logger.error(f"Failed to send post {submission.id}: {e}")

    except KeyboardInterrupt:
        logger.info("Received shutdown signal. Closing stream...")
    except Exception as e:
        logger.error(f"Error while streaming Reddit data: {e}")
    finally:
        logger.info("Flushing and closing Kafka producer...")
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed.")

if __name__ == "__main__":
    main()
