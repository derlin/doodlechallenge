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
	MAX_LAG         = 5  // in seconds
	GRANULARITY     = 60 // in seconds
	N_WORKERS       = 1
)

type JsonData struct {
	Ts     int    `json:"ts"`
	UserId string `json:"uid"`
}

type Window struct {
	Ts int
	m  map[string]int
}

func (w *Window) Reset(ts int) {
	w.Ts = ts
	w.m = make(map[string]int)
}

func (w *Window) Add(jd *JsonData) {
	if _, ok := w.m[jd.UserId]; !ok {
		w.m[jd.UserId] = 0
	}
	w.m[jd.UserId] += 1
}

func (w *Window) Close(writer *kafka.Writer) {
	msg := make([]kafka.Message, len(w.m))
	i := 0
	for user, cnt := range w.m {
		msg[i].Key = []byte(user)
		msg[i].Value = []byte(fmt.Sprintf(`{"ts": %d, "user":"%s", "cnt": %d}`, w.Ts, user, cnt))
		i += 1
	}
	if err := writer.WriteMessages(context.Background(), msg...); err != nil {
		log.Panic(err)
	}
}

type SlidingWindows struct {
	timeAdvance    int
	lastCleanup    int
	windows        *Ring
	maxOpenWindows int
	writer         *kafka.Writer
}

func NewSlidingWindows() *SlidingWindows {
	var sw SlidingWindows
	sw.windows = NewRing((MAX_LAG % GRANULARITY) + 2)
	sw.writer = // make a writer that produces to topic-A, using the least-bytes distribution
		kafka.NewWriter(kafka.WriterConfig{
			Brokers:  []string{KAFKA_BROKER},
			Topic:    KAFKA_TOPIC_OUT,
			Balancer: &kafka.LeastBytes{},
		})
	return &sw
}

func (sw *SlidingWindows) Add(jd *JsonData) {

	key := jd.Ts - (jd.Ts % GRANULARITY)

	if jd.Ts > sw.timeAdvance {
		sw.timeAdvance = jd.Ts
		// potentially create a new window
		if sw.windows.Size() == 0 || sw.windows.Get(0).Ts < key {
			sw.windows.Add(key)
		}
		// potentially cleanup windows
		sw.Cleanup()
	}

	added := false
	for i := 0; i < sw.windows.Size(); i++ {
		w := sw.windows.Get(i)
		if w.Ts == key {
			w.Add(jd)
			added = true
			break
		}
	}

	if !added {
		// end of the opened windows ... the data is too far in the past
		log.Printf("Dropped frame: user=%s, ts=%d, time=%d\n", jd.UserId, jd.Ts, sw.timeAdvance)
	}
}

func (sw *SlidingWindows) Advance() {
	sw.timeAdvance = int(time.Now().Unix())
	sw.Cleanup()
}

func (sw *SlidingWindows) Cleanup() {
	sw.lastCleanup = sw.timeAdvance
	//log.Printf("Trying cleanup\n")

	for ; sw.windows.Size() > 0; {
		w := sw.windows.Get(-1) // get oldest
		if w.Ts >= sw.timeAdvance-GRANULARITY-MAX_LAG {
			break // the oldest may still be in use, stop
		}

		log.Printf("Closing window %d (cnt: %d, lag: %d)\n", w.Ts, len(w.m), sw.timeAdvance-w.Ts)
		go w.Close(sw.writer) // risky !! but we assume closing takes less time than filling buffers...
		sw.windows.Remove(1)

	}
}

func aggregator(indata chan JsonData, flush chan bool) {
	sw := NewSlidingWindows()

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
			// TODO
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
