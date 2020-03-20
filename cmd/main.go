package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

const (
	KAFKA_BROKER    = "localhost:9092"
	KAFKA_TOPIC_IN  = "doodle"
	KAFKA_TOPIC_OUT = "doodle-out"
	MAX_LAG         = 5  // in seconds
	GRANULARITY     = 60 // in seconds
	N_WORKERS       = 4
	OUTPUT_TO_KAFKA = true
)

type JsonData struct {
	Ts     int32    `json:"ts"`
	UserId string `json:"uid"`
}

func dispatcher(channel chan []byte, nWorkers int) {

	workers := make(map[int]chan JsonData)
	// ticker := time.NewTicker(GRANULARITY)
	ticker := make(chan bool)
	uids := make(map[string]int)
	nextWorker := 0

	// start the workers
	for i := 0; i < nWorkers; i++ {
		c := make(chan JsonData)
		go aggregator(c, ticker)
		workers[i] = c
	}

	var jd JsonData

	for {
		bs := <-channel
		if bs == nil {
			// timeout
			ticker <- true
			continue
		}

		// TODO: parallelize the unmarshal
		if err := json.Unmarshal(bs, &jd); err == nil {
			key := jd.UserId
			if _, ok := uids[key]; !ok {
				uids[jd.UserId] = nextWorker
				nextWorker = (nextWorker + 1) % nWorkers
			}
			workers[uids[key]] <- jd
		}

	}
}

func aggregator(indata chan JsonData, flush chan bool) {
	sw := NewAggregatorState(!OUTPUT_TO_KAFKA)

	for {
		select {
		case ok := <-flush:
			if !ok {
				break
			}
			sw.Advance()
		case jd := <-indata:
			sw.Add(&jd)
		}
	}
}

func main() {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{KAFKA_BROKER},
		Topic:     KAFKA_TOPIC_IN,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	channel := make(chan []byte)
	timeout := (GRANULARITY + MAX_LAG) * time.Second
	defer close(channel)
	go dispatcher(channel, N_WORKERS)

	start := time.Now()
	log.Println("Start")

	for {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		m, err := r.ReadMessage(ctx)
		if err != nil {
			// TODO not very clean...
			channel <- nil
			break
		} else {
			channel <- m.Value
		}
	}

	elapsed := time.Now().Sub(start)
	log.Printf("Elapsed: %s", elapsed)
	log.Printf("Elapsed-timeout: %fs\n", elapsed.Seconds()-timeout.Seconds())

}
