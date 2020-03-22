package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

const (
	KAFKA_BROKER   = "localhost:9092"
	KAFKA_TOPIC_IN = "doodle"
	KAFKA_TIMEOUT = 10
)

type JsonData struct {
	Ts     int    `json:"ts"`
	UserId string `json:"uid"`
}

// completely basic processing, just to see how long it takes to just read frames from Kafka
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

	start := time.Now()
	read(r)
	fmt.Printf("Elapsed: %v\n", time.Now().Sub(start).Seconds())
}

// just read frames from kafka, nothing else
func read(r *kafka.Reader){
	for {
		ctx, _ := context.WithTimeout(context.Background(), KAFKA_TIMEOUT*time.Second)
		_, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
	}
}