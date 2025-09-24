# real-time-bigdata-project

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

