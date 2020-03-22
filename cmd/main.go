package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

const (
	KAFKA_BROKER    = "localhost:9092"
	KAFKA_TOPIC_IN  = "doodle"
	KAFKA_TOPIC_OUT = "doodle-out"
	KAFKA_TIMEOUT   = 10 // reading timeout: stops the program if reached

	GRANULARITY = 60 // in seconds
	MAX_LAG     = 5  // in seconds

	N_UNMARSHALLERS = 1   // number of unmarshal goroutines
	OUTPUT_TO_KAFKA = true // if false, just print to stdout
)

// Data to extract from the JSON messages
type JsonData struct {
	Ts     int32  `json:"ts"`
	UserId string `json:"uid"`
}

// IsValid returns true only if both uid and ts are present
func (jd *JsonData) IsValid() bool {
	return jd.UserId != "" && jd.Ts > 0
}

// goroutine in charge of the unmarshalling, ie. parsing the JSON
// ideally, multiple unmarshallers should run at the same time, as it is quite slow.
// NOTE: ensure to add some buffering to the channels !
func unmarshaller(cIn chan []byte, cOut chan JsonData) {
	var jd JsonData
	for {
		bs := <-cIn
		if err := json.Unmarshal(bs, &jd); err == nil && jd.IsValid() {
			cOut <- jd
		}
	}
}

// goroutine in charge of aggregation.
// closed windows will be outputted to cOut, flush is a special channel one can use to
// advance the internal clock and hence close dangling windows; sending false also makes the routine stop.
func aggregator(cIn chan JsonData, cOut chan Aggregation, flush chan bool) {
	sw := NewAggregatorState(cOut)

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

// goroutine that emits closed windows to a new kafka topic.
// Note that each send is done in a new goroutine.
func kafkaEmitter(c chan Aggregation) {
	// make a writer that produces to topic, using the least-bytes distribution
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{KAFKA_BROKER}, // TODO not very clean
		Topic:    KAFKA_TOPIC_OUT,
		Balancer: &kafka.LeastBytes{},
	})

	for {
		w := <-c
		go func(timeKey int32, cnt int) {
			err := writer.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(fmt.Sprintf("%f", timeKey)),
				Value: []byte(fmt.Sprintf(`{"ts": %d, "cnt": %d}`, timeKey, cnt)),
			})
			if err != nil {
				log.Panic(err)
			}
		}(w.TimeKey, w.Count())
	}
}

// goroutine that emits closed windows to stdout.
func stdEmitter(c chan Aggregation) {
	for {
		w := <-c
		fmt.Printf("%d\t%s\t%d\n", w.TimeKey, w.Count())
	}
}

func main() {
	// == tracing
	//log.SetOutput(os.Stdout)
	//trace.Start(os.Stderr)
	//defer trace.Stop()

	// == pprof CPU
	//f, err := os.Create("cpuprofile-new")
	//if err != nil {
	//	log.Fatal("could not create CPU profile: ", err)
	//}
	//defer f.Close()
	//if err := pprof.StartCPUProfile(f); err != nil {
	//	log.Fatal("could not start CPU profile: ", err)
	//}
	//defer pprof.StopCPUProfile()

	// make a new reader. Note that it will always starts at the beginning (no offset committing !)
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
	cEmitter := make(chan Aggregation, 10)                  // to output closed windows

	// start the unmarshallers
	for i := 0; i < N_UNMARSHALLERS; i++ {
		go unmarshaller(cMessages, cUnmarshalled)
	}

	// start the aggregator
	go aggregator(cUnmarshalled, cEmitter, cTimeout)

	// start the emitter
	if OUTPUT_TO_KAFKA {
		go kafkaEmitter(cEmitter)
	} else {
		go stdEmitter(cEmitter)
	}

	// GO !
	start := time.Now()
	log.Printf("Start: unmarshallers=%d, max_lag=%d, kafka_timeout=%d, output_to_kafka=%t",
		N_UNMARSHALLERS, MAX_LAG, KAFKA_TIMEOUT, OUTPUT_TO_KAFKA)

	for {
		// here, we know that that timestamps are way in the past (2017), we are not processing realtime
		// data. Hence, we can have a timeout < GRANULARITY and simply stop when we processed all messages.
		ctx, _ := context.WithTimeout(context.Background(), KAFKA_TIMEOUT*time.Second)
		m, err := r.ReadMessage(ctx)
		if err != nil {
			// TODO not very clean...
			cTimeout <- false // sending false will also make the aggregator stop
			break
		} else {
			if len(m.Value) > 1 { // ensure we have some bytes
				cMessages <- m.Value
			}
		}
	}

	elapsed := time.Now().Sub(start)
	log.Printf("Elapsed: %f\n", elapsed.Seconds())
	log.Printf("Elapsed-timeout: %fs\n", elapsed.Seconds()-KAFKA_TIMEOUT)

}
