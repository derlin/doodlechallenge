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
	KAFKA_TIMEOUT   = 10
	MAX_LAG         = 5  // in seconds
	GRANULARITY     = 60 // in seconds
	N_AGGREGATORS   = 5
	N_UNMARSHALLERS = 10
	OUTPUT_TO_KAFKA = true
)

type JsonData struct {
	Ts     int32  `json:"ts"`
	UserId string `json:"uid"`
}

func dispatcher(channel chan []byte, nWorkers int) {

	cUnmarshalled := make(chan *JsonData, N_UNMARSHALLERS*4)

	workers := make(map[int]chan JsonData, N_AGGREGATORS*4)
	// ticker := time.NewTicker(GRANULARITY)
	ticker := make(chan bool)
	uids := make(map[string]int)
	nextWorker := 0

	// start the unmarshallers
	for i := 0; i < N_UNMARSHALLERS; i++ {
		go unmarshaller(channel, cUnmarshalled)
	}
	// start the workers
	for i := 0; i < nWorkers; i++ {
		c := make(chan JsonData)
		go aggregator(c, ticker)
		workers[i] = c
	}

	for {
		jd := <-cUnmarshalled
		if jd == nil {
			// timeout
			for i := 0; i < N_AGGREGATORS; i++ {
				ticker <- true
			}
			continue
		}

		key := jd.UserId
		if _, ok := uids[key]; !ok {
			uids[jd.UserId] = nextWorker
			nextWorker = (nextWorker + 1) % nWorkers
		}
		workers[uids[key]] <- *jd

	}
}
func unmarshaller(cIn chan []byte, cOut chan *JsonData) {

	for {
		var jd JsonData
		bs := <-cIn
		if bs == nil {
			cOut <- nil
		} else if err := json.Unmarshal(bs, &jd); err == nil {
			cOut <- &jd
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
	//log.SetOutput(os.Stdout)
	//trace.Start(os.Stderr)
	//defer trace.Stop()
	//f, err := os.Create("cpuprofile-kafka")
	//if err != nil {
	//	log.Fatal("could not create CPU profile: ", err)
	//}
	//defer f.Close()
	//if err := pprof.StartCPUProfile(f); err != nil {
	//	log.Fatal("could not start CPU profile: ", err)
	//}
	//defer pprof.StopCPUProfile()
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{KAFKA_BROKER},
		Topic:     KAFKA_TOPIC_IN,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	channel := make(chan []byte, 10) // add some buffering
	defer close(channel)
	go dispatcher(channel, N_AGGREGATORS)

	start := time.Now()
	log.Println("Start")

	for {
		ctx, _ := context.WithTimeout(context.Background(), KAFKA_TIMEOUT*time.Second)
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
	log.Printf("Elapsed-timeout: %fs\n", elapsed.Seconds()-KAFKA_TIMEOUT)

}
