package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

const (
	KAFKA_BROKER    = "localhost:9092"
	KAFKA_TOPIC_IN  = "doodle"
	KAFKA_TOPIC_OUT = "doodle-out"
	KAFKA_TIMEOUT   = 10 // reading timeout: stops the program if reached

	GRANULARITY = 60 // in seconds
	MAX_LAG     = 5  // in seconds

	N_UNMARSHALLERS = 20   // number of unmarshal goroutines
	OUTPUT_TO_KAFKA = true // if false, just print to stdout
)

type JsonData struct {
	Ts     int32  `json:"ts"`
	UserId string `json:"uid"`
}

func (jd *JsonData) IsValid() bool {
	return jd.UserId != "" && jd.Ts > 0
}

func unmarshaller(cIn chan []byte, cOut chan JsonData) {

	var jd JsonData
	for {
		bs := <-cIn
		if err := json.Unmarshal(bs, &jd); err == nil && jd.IsValid() {
			cOut <- jd
		}
	}
}

func aggregator(cIn chan JsonData, flush chan bool) {
	sw := NewAggregatorState(!OUTPUT_TO_KAFKA)

	for {
		select {
		case ok := <-flush:
			if !ok {
				break
			}
			sw.Advance()
		case jd := <-cIn:
			sw.Add(&jd)
		}
	}
}

func main() {
	//log.SetOutput(os.Stdout)
	//trace.Start(os.Stderr)
	//defer trace.Stop()
	f, err := os.Create("cpuprofile-new")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{KAFKA_BROKER},
		Topic:     KAFKA_TOPIC_IN,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	// create the channels

	cMessages := make(chan []byte, N_UNMARSHALLERS)         // raw messages from kafka, with some buffering
	cUnmarshalled := make(chan JsonData, N_UNMARSHALLERS*2) // unmarshalled messages
	cTimeout := make(chan bool)                             // to tell the aggregator to flush the remaining windows

	// start the unmarshallers
	for i := 0; i < N_UNMARSHALLERS; i++ {
		go unmarshaller(cMessages, cUnmarshalled)
	}

	// start the aggregator
	go aggregator(cUnmarshalled, cTimeout)

	// GO !
	start := time.Now()
	log.Println("Start")

	for {
		ctx, _ := context.WithTimeout(context.Background(), KAFKA_TIMEOUT*time.Second)
		m, err := r.ReadMessage(ctx)
		if err != nil {
			// TODO not very clean...
			cTimeout <- false
			break
		} else {
			if len(m.Value) > 1 { // ensure we have some bytes
				cMessages <- m.Value
			}
		}
	}

	elapsed := time.Now().Sub(start)
	log.Printf("Elapsed: %s", elapsed)
	log.Printf("Elapsed-timeout: %fs\n", elapsed.Seconds()-KAFKA_TIMEOUT)

}
