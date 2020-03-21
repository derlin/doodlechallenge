package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

// State and variables needed for one aggregator to do its job
type AggregatorState struct {
	timeAdvance int32         // latest timestamp consumed so far, or time of the latest read timeout
	windows     *Ring         // a circular buffer for storing opened aggregation windows
	writer      *kafka.Writer // kafka writer to output the results on window close
}

// Create a new aggregator state, this will also initialize a new Kafka writer.
// If 'toStd' is true, the aggregations will be printed to stdout instead of pushed to a kafka topic
func NewAggregatorState(toStd bool) *AggregatorState {
	var sw AggregatorState
	sw.windows = NewRing((MAX_LAG % GRANULARITY) + 2) // make 1 time bigger than strictly needed
	if !toStd {
		sw.writer = // make a writer that produces to topic, using the least-bytes distribution
			kafka.NewWriter(kafka.WriterConfig{
				Brokers:  []string{KAFKA_BROKER}, // TODO not very clean
				Topic:    KAFKA_TOPIC_OUT,
				Balancer: &kafka.LeastBytes{},
			})
	}
	return &sw
}

// Process one record
// TODO: we may have problems if we receive a record with a timestamp far in the future
func (sw *AggregatorState) Add(jd *JsonData) {
	// window start this record belongs to
	key := jd.Ts - (jd.Ts % GRANULARITY)

	if jd.Ts > sw.timeAdvance {
		// this is expected: we have a record older than any other seen before
		sw.timeAdvance = jd.Ts // advance the inner clock
		if sw.windows.Size() == 0 || sw.windows.Get(0).TimeKey < key {
			// first record ever, or it passed the granularity threshold: open a new window
			sw.windows.Add(key)
		}
		// the time advanced: potentially close older windows
		sw.Cleanup()
	}

	// go through the list of opened windows, latest first, to see if we can add the record
	// 99% of the time, the first window is the one we look for, so it is O(1)
	added := false
	for i := 0; i < sw.windows.Size(); i++ {
		w := sw.windows.Get(i) // positive number: latest first
		if w.TimeKey == key {
			w.Increment(jd.UserId)
			added = true
			break
		}
	}

	if !added {
		// no opened window matched. This can happen only if the records timestamp is too far in the past
		// TODO end of the opened windows ... the data is too far in the past
		log.Printf("Dropped frame: user=%s, ts=%d, time=%d\n", jd.UserId, jd.Ts, sw.timeAdvance)
	}
}

// Should be called only if no new record arrived in a long time. This ensures
// the hangling windows are closed/flushed
func (sw *AggregatorState) Advance() {
	sw.timeAdvance = int32(time.Now().Unix())
	sw.Cleanup()
}

// Close all opened windows that are too far in the past, depending on 'timeAdvanced'
func (sw *AggregatorState) Cleanup() {

	for ; sw.windows.Size() > 0; {
		w := sw.windows.Get(-1) // get the oldest
		if w.TimeKey >= sw.timeAdvance-GRANULARITY-MAX_LAG {
			break // the oldest may still be in use, stop
		}

		log.Printf("Closing window %d (cnt: %d, lag: %d)\n", w.TimeKey, len(w.m), sw.timeAdvance-w.TimeKey)
		go sw.flushAggregation(*w) // passed by copy to avoid the window to be changed before flush ends
		sw.windows.Remove(1)

	}
}

// private function

func (sw *AggregatorState) flushAggregation(w Aggregation) {
	if sw.writer != nil {
		err := sw.writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(fmt.Sprintf("%f", w.TimeKey)),
			Value: []byte(fmt.Sprintf(`{"ts": %d, "cnt": %d}`, w.TimeKey, w.Count())),
		})
		if err != nil {
			log.Panic(err)
		}
	} else {
		// just output to std
		fmt.Printf("%d\t%s\t%d\n", w.TimeKey, w.Count())
	}
}
