package main

// Aggregation traces the number of records for each unique user for one Window of time (1 granularity, e.g. one minute).
type Aggregation struct {
	TimeKey int32          // window start
	m       map[string]int // map of UserId -> counter
}

// Reset this Aggregation object by deleting the user map and setting a new "window start"
func (w *Aggregation) Reset(key int32) {
	w.TimeKey = key
	w.m = make(map[string]int) // make is faster than actually deleting each record
}

// Update the count for the given 'user'
func (w *Aggregation) Increment(user string) {
	if _, ok := w.m[user]; !ok {
		w.m[user] = 0
	}
	w.m[user] += 1
}

//func (w *Aggregation) Close(writer *kafka.Writer) {
//	msg := make([]kafka.Message, len(w.m))
//	i := 0
//	for user, cnt := range w.m {
//		msg[i].Key = []byte(user)
//		msg[i].Value = []byte(fmt.Sprintf(`{"ts": %d, "user":"%s", "cnt": %d}`, w.TimeKey, user, cnt))
//		i += 1
//	}
//	if err := writer.WriteMessages(context.Background(), msg...); err != nil {
//		log.Panic(err)
//	}
//}
