Docker compose taken from https://github.com/bitnami/bitnami-docker-kafka

run using:
```bash
docker-compose up -d
```

To create topics, first ssh into the kafka docker container:
```bash
docker exec -it doodlechallenge_kafka_1 bash
```
then, create a topic using:
```bash
/opt/bitnami/kafka/bin/kafka-topics.sh --create \
    --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 1 \
    --topic doodle
```

To listen to messages from the topic:
```bash
/opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic doodle-out
```

```bash
cat /xx/stream.jsonl | /opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic doodle
``` 