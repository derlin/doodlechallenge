# Doodle Hiring Challenge - derlin

## Setup & run

### Prerequisites

1. have a working go installation
2. have kafka running on `localhost:9092`
3. have two kafka topics `doodle` and `doodle-out` (see constants in `cmd/main.go`)

To create the topics, you can use:

```bash
kafka-topics.sh --create \
    --zookeeper zookeeper:2181 \
    --replication-factor 1 --partitions 1 \
    --topic doodle
```


### Install and run

1. clone this repo
2. run `go build cmd/*.go && go run`