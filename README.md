# Doodle Data Engineering Hiring Challenge - derlin

This repository contains the code and the report for the Doodle Hiring Challenge described here: 
https://github.com/tamediadigital/hiring-challenges/tree/master/data-engineer-challenge 

**⇨ see [report.md](report.md) for details ⇦**

## Structure

Folders:

* `cmd` contains the main program,
* `baseline` contains a basic baseline program that just read frames from Kafka,
* `benchmarks` contains the script + results of a little benchmark experiment

Files:

* `report.md`: explains and discuss the current solution and what else we could do,
* `docker-compose-yml`: what I used to run Kafka on my Mac

## Prerequisites

1. have a working go installation
2. have kafka running on `localhost:9092`
3. have two kafka topics `doodle` and `doodle-out` (see constants in `cmd/main.go`)
4. have the content of `stream.jsonl` in the kafka topic `doodle` (see their readme) (do it only once !)
5. (if using my docker-compose) have a folder `data` with the `stream.jsonl` file (see below) 

## Install and run

1. clone this repo
2. install dependencies: `go get github.com/segmentio/kafka-go` or `go get cmd/..`
2. build and run using `go run cmd/*.go`, or build *then* run using `go build -o doodlechallenge cmd/*.go && ./doodlechallenge`

## Using docker for Kafka

Get the data:
```bash
mkdir data
cd data
wget http://tx.tamedia.ch.s3.amazonaws.com/challenge/data/stream.jsonl.gz
gunzip -k stream.jsonl.gz
cd ..
```

Launch the containers:
```bash
docker-compose up -d
```

Get a bash into the kafka container:
```bash
docker exec -it doodlechallenge_kafka_1
```

Setup the topics and the data (still inside the kafka container):

```bash
/opt/bitnami/kafka/bin/kafka-topics.sh --create \
    --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 1 \
    --topic doodle

/opt/bitnami/kafka/bin/kafka-topics.sh --create \
    --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 1 \
    --topic doodle-out

# optionally, set a very low retention policy on the out topic, in case we run many times the program
/opt/bitnami/kafka/bin/kafka-configs.sh \
    --zookeeper localhost:2181 \
    --alter \
    --topic doodle-out \
    --config retention.ms=1000

cat /data/stream.jsonl | /opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic doodle
```
Note that the last command may take a while, there are 1M records to ingest !


----------------------------------

Derlin @ 2020 (during the corona virus outbreak)
