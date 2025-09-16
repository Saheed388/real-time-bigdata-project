# real-time-bigdata-project

If having issue connecting to kafka use this to check the port is listining to

-- docker exec -it redpanda-1 rpk cluster info


To check the consumer via terminal

-- docker exec -it redpanda-1 rpk topic consume reddit-json-stream


mkdir -p ./data/checkpoint
chmod -R 777 ./data/checkpoint


mkdir -p ./include
echo "REDDIT_CLIENT_ID=RJTKBKAjjB46HKLve9Y2Fw" >> ./include/.env
echo "REDDIT_CLIENT_SECRET=AENo4iD-r9zXWdvgFOPzR_naruIHQQ" >> ./include/.env
echo "KAFKA_BOOTSTRAP_SERVERS=kafka:9092" >> ./include/.env