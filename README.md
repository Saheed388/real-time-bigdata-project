# real-time-bigdata-project

Data Pipeline Project
This project implements a data pipeline that extracts data from Reddit, processes it, and loads it into a Snowflake data warehouse.
Overview

Data Source: Reddit data is extracted using a Python script.
Streaming: Apache Kafka is used to handle the streaming of data from Reddit.
Processing: Apache Spark processes the data from Kafka and prepares it for the data warehouse.
Data Warehouse: Snowflake serves as the final destination for the processed data.
Containerization: The pipeline components (Python script with Kafka and Spark) are deployed using Docker.

Setup and Deployment

Extract Data from Reddit:

A Python script is used to fetch data from Reddit and send it to Kafka.
The script is containerized using Docker for easy deployment.


Kafka Streaming:

Kafka manages the data stream, with partitioning to handle data distribution.
Deployed within a Docker container.


Spark Processing:

Apache Spark processes the Kafka-streamed data and loads it into Snowflake.
This component is also containerized using Docker.


Snowflake Integration:

Processed data is stored in a Snowflake data warehouse.



Prerequisites

Docker
Python
Apache Kafka
Apache Spark
Snowflake account

Running the Pipeline

Build and run the Docker containers for Kafka and Spark.
Execute the Python script to start data extraction from Reddit to Kafka.
Configure Spark to process data from Kafka and load it into Snowflake.

Notes

Ensure proper partitioning is configured in Kafka and Spark for optimal performance.
Check Docker logs for any deployment issues.


If having issue connecting to kafka use this to check the port is listining to

-- docker exec -it redpanda-1 rpk cluster info


To check the consumer via terminal

-- docker exec -it redpanda-1 rpk topic consume reddit-json-stream




-------
to check the prefarred dependencies

https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/
``````````

To test the consumer before deployment using kafka
-- spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,net.snowflake:spark-snowflake_2.12:3.1.3 \
  consumer.py

  docker compose exec flink-jobmanager ./bin/flinks run -py /opt/src/jobs/reddit_top_authors_snowflake.py --pyFiles /opt/src -d

  

  Producer Deployment

  --docker build -t reddit-kafka-producer .

Run to see see the log

--docker run -d --env-file .env --network="host" reddit-kafka-producer

To run in background
  -- docker run -d --env-file .env --network="host" reddit-kafka-producer

Consumer Deployment

docker build -t reddit-spark-consumer .
docker run -d --env-file .env --network="host" reddit-spark-consumer

