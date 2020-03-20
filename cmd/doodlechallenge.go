package main

import (
	"context"
	"encoding/json"
	_ "encoding/json"
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

type Window map[string]int

func NewWindow() Window {
	return make(Window)
}

func (w Window) Add(jd *JsonData) {
	if _, ok := w[jd.UserId]; !ok {
		w[jd.UserId] = 0
	}
	w[jd.UserId] += 1
}

func (w Window) Close(ts int) {
	//for user, cnt := range w {
	//	fmt.Printf("@@ CLOSE %d\t%s\t%d\n", ts, user, cnt)
	//}
}

type SlidingWindows struct {
	timeAdvance int
	lastCleanup int
	windows     map[int]Window
}

func NewSlidingWindows() *SlidingWindows {
	var sw SlidingWindows
	sw.windows = make(map[int]Window)
	return &sw
}

func (sw *SlidingWindows) Add(jd *JsonData) {
	//fmt.Print(".")
	key := jd.Ts - (jd.Ts % GRANULARITY)
	// add user
	if _, ok := sw.windows[key]; !ok {
		sw.windows[key] = NewWindow()
	}
	sw.windows[key].Add(jd)
	// update stats
	if sw.timeAdvance == 0 {
		// first value ever !
		sw.timeAdvance = jd.Ts
		sw.lastCleanup = jd.Ts
	} else if jd.Ts > sw.timeAdvance {
		sw.timeAdvance = jd.Ts
		sw.cleanup()
	}
}

func (sw *SlidingWindows) Advance() {
	sw.timeAdvance = int(time.Now().Unix())
	sw.cleanup()
}

func (sw *SlidingWindows) cleanup() {
	if sw.timeAdvance-sw.lastCleanup >= GRANULARITY+MAX_LAG {
		sw.lastCleanup = sw.timeAdvance
		log.Printf("Trying cleanup\n")
		for key, w := range sw.windows {
			if key < sw.timeAdvance+GRANULARITY+MAX_LAG {
				log.Printf("Closing window %d (cnt: %d, lag: %d)\n", key, len(w), sw.timeAdvance-key)
				go w.Close(key)
				delete(sw.windows, key)
			}
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

	var jsonData JsonData
	sw := NewSlidingWindows()

	for {
		ctx, _ := context.WithTimeout(context.Background(), (GRANULARITY+MAX_LAG)*time.Second)
		m, err := r.ReadMessage(ctx)
		if err != nil {
			sw.Advance()
		}
		if err := json.Unmarshal(m.Value, &jsonData); err != nil {
			log.Println(err)
		} else {
			sw.Add(&jsonData)
		}
	}

	r.Close()
}
