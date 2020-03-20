package main

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

const (
	KAFKA_BROKER = "localhost:9092"
	KAFKA_TOPIC  = "doodle"
	MAX_LAG      = 5  // in seconds
	GRANULARITY  = 60 // in seconds
)

type JsonData struct {
	Ts     int    `json:"ts"`
	UserId string `json:"uid"`
}

type Window struct {
	Ts int
	m  map[string]int
}

func NewWindow(ts int) *Window {
	return &Window{Ts: ts, m: make(map[string]int)}
}

func (w *Window) Add(jd *JsonData) {
	if _, ok := w.m[jd.UserId]; !ok {
		w.m[jd.UserId] = 0
	}
	w.m[jd.UserId] += 1
}

func (w *Window) Close() {
	for user, cnt := range w.m {
		_ = fmt.Sprintf("@@ CLOSE %d\t%s\t%d\n", w.Ts, user, cnt)
	}
}

type SlidingWindows struct {
	timeAdvance    int
	lastCleanup    int
	windows        *list.List
	maxOpenWindows int
}

func NewSlidingWindows() *SlidingWindows {
	var sw SlidingWindows
	sw.windows = list.New()
	sw.maxOpenWindows = 2
	return &sw
}

func (sw *SlidingWindows) Add(jd *JsonData) {

	key := jd.Ts - (jd.Ts % GRANULARITY)

	if jd.Ts > sw.timeAdvance {
		sw.timeAdvance = jd.Ts
		// potentially create a new window
		if sw.windows.Front() == nil || sw.windows.Front().Value.(*Window).Ts < key {
			sw.windows.PushFront(NewWindow(key))
		}
		// potentially cleanup windows
		sw.Cleanup()
	}

	for e := sw.windows.Front(); ; e = e.Next() {
		if e == nil {
			// end of the opened windows ... the data is too far in the past
			log.Printf("Dropped frame: user=%s, ts=%d, time=%d\n", jd.UserId, jd.Ts, sw.timeAdvance)
			return
		}
		if e.Value.(*Window).Ts == key {
			e.Value.(*Window).Add(jd)
			break
		}
	}
}

func (sw *SlidingWindows) Advance() {
	sw.timeAdvance = int(time.Now().Unix())
	sw.Cleanup()
}

func (sw *SlidingWindows) Cleanup() {
	sw.lastCleanup = sw.timeAdvance
	//log.Printf("Trying cleanup\n")

	var prev *list.Element
	for e := sw.windows.Back(); e != nil; e = prev {
		w := e.Value.(*Window)
		prev = e.Prev()
		if w.Ts < sw.timeAdvance-GRANULARITY-MAX_LAG {
			log.Printf("Closing window %d (cnt: %d, lag: %d)\n", w.Ts, len(w.m), sw.timeAdvance-w.Ts)
			go w.Close()
			sw.windows.Remove(e)
		}
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
		Topic:     KAFKA_TOPIC,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	channel := make(chan []byte)
	timeout := (GRANULARITY + MAX_LAG) * time.Second

	go dispatcher(channel, 3)

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
	log.Printf("Elapsed-timeout: %sf\n", elapsed.Seconds()-timeout.Seconds())

	// cleanup
	close(channel)
	r.Close()
}
