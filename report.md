# Report

## Why Go ?

I haven't programmed in Go in more than a year. I spent most of my time writing Python and JS apps. 
However, Go seems a good fit for this challenge. Here are some pros and cons:

* (+) fast language that compiles into native code,
* (+) officially supported by Confluent/Kafka,
* (+) cheap and easy concurrency: goroutines and channels make it easy to test different parallelisation techniques,
* (+) if the code compiles, it is a good sign it can go into production :)
* (+) I love it
* (-) the garbage collector can easily become a bottleneck => rust or C/C++ is better for that
* (-) I am rusty in Go...

## Doing aggregations

### Assumptions

1. records are mostly ordered in time (small lag possible),
2. the "unordered" records have a timestamp in the past, never in the future,
3. the app would likely ingest (near) real-time data, not only historical -> develop with that in mind

### Main structures used

* hash map for storing unique users per minute
* circular buffer for storing opened "windows" (one per minute)

### Aggregation logic

Records are read from kafka and then consumed by an *aggregator*. It has the following state variables:

* `timeAdvance`: this is the internal clock of the aggregator. Each time it consumes a timestamp, it will advance the clock (if the timestamp is more recent);
* `windows`: a collection of opened windows, that is an "aggregation" structure that keeps the count of unique users seen in a given minute.

For each time window (here each minute), we need to keep track of unique users. 
A HashSet makes sense, but it doesn't exist in go, so a map will do.

The aggregator keeps the windows in a circular buffer and the structures are reused, so we avoid too many malloc and free. Given the max lag and the granularity, we know how many windows at most should be opened: `(max_lag % granulary) + 1`. To be sure, I do `+2`. 

Why multiple windows ? This is because we know some frames may be "late". Hence, we open new windows as the time advance, and only close them when the minute they represent is old enough, say `GRANULARY+MAX_LAG` (configurable).
This `MAX_LAG` is of course a compromise: by increasing it, we add more latency but we also limit the risk of dropping late frames.

Here is the pseudo-code for the aggregator:

```python
windows = <circular buffer>
timeAdvance = 0

for record in records:
    # compute the minute it belongs to
    minute = record.timestamp - (record.timestamp % 60)

    # time keeping + open/close windows
    if record.timestamp > timeAdvance:
        # the record is "in-order", advance the clock
        timeAdvance = record.timestamp
        if minute != windows.head().timestamp:
            # we "passed" a new minute, open a new window for it
            windows.AddFront(minute)
        # see if we need to close some old windows
        tryClosingWindows()
    
    # process current record: 99% of the time, the first window is the one, O(1)
    for window in windowsFromFront():
        if window.minute == minute:
            window.add(record)
            break
```

... and the pseudo-code for closing the windows (`tryClosingWindows` in the pseudo-code above):
```python
# check windows, oldest first and stop early
for window in windowsFromBack():
    if window.key >= timeAdvance - GRANULARITY - MAX_LAG:
        break 

    window.close() # e.g. print to stdout, write to a kafka topic
    windows.remove(window)
```

## Overall program

**structure**

The most basic program is a loop that:

1. reads a record from kafka
2. unmarshals the JSON
3. calls the aggregator

To terminate the program properly, reading from kafka has a timeout and if reached,
the remaining opened windows are flushed and the program exits (see `KAFKA_TIMEOUT`).

**output**

The aggregator can output the closed windows either to stdout or a new kafka topic (`doodle-out`). If kafka is used, the message value is a JSON with two fields:

* `ts`: the timestamp of the window start, as a unix epoch (in our case: minutes)
* `cnt`: the number of unique users.

The value of `ts` is also used as the message key (may be useful for downstream processing).
The format is JSON to be coherent, and the field names are both short (saves bytes) and significant.

## Improvements

### Parallelisation

The aggregator doesn't do much, and it is hard to parallelise since most records are ordered and we need to ensure all records from the same minute are forwarded to the same aggregator (or we need to add a layer of synchronisation/gathering that is likely to be more costly than the speed we could gain by parallelising). This would be easier if the timestamp were used as the key to the kafka messages.

The real bottlenecks are reading/writing to kafka and decoding the JSON message, which is proven by a little CPU profiling using `pprof`.

To parallelise the **JSON parsing**, we can launch multiple goroutines that share the same in/out channels. The kafka reader sends the raw bytes to the *in* channel, the aggregator reads from the *out* channel. \
For this to work, we need to ensure the channels are buffered: the kafka reader can send at least *number of unmarshallers* before blocking, and likewise the unmarshallers can send the decoded structures in parallel.

For **Kafka**, we can easily parallelise the writing process. For that, the aggregator sends windows to close to a new channel, that is consumed by a new goroutine, which is in charge of writing messages to kafka. To be even faster, each `kafka.SendMessages` is executed in an anonymous goroutine, so it doesn't block.

To parallelise the reader (haven't tried), we would need a single consumer group with multiple kafka partitions. Feasible, not how sure we would gain though.


### Edge cases

**wrong messages**

If the message read from Kafka is not of the expected structure (`uid` and `ts` set), the frame is ignored.

**unordered timestamps**

With the current implementation, here are how edge cases are handled:

1. unordered timestamps, in the past:
    a. less than "granularity+max_lag": OK, will be processed properly since the window will still be in memory;
    b. more than "granularity+max_lag": those frames will be dropped (logged to stderr)

2. unordered timestamps, in the future: this is problematic, because it will advance the aggregator clock... We may then drop a lot of frames, by thinking they are "in the past"... 

3. random timestamps: see (1) or (2)

For 1.b, what we could do is publish a special message in the Kafka queue to let the downstream application know the window must be updated. 


Note that I did it like this because in my mind, the application would likely process data published by some kind of web application. There are more chances a frame will be delayed (hence timestamp in the past), than a random future timestamp to be emitted.

If we would like to handle future timestamps as well: instead of using an internal clock ("timeAdvance") to open/close windows, we could open windows as records come (no time track) and decide to close any window that has been idle for more than a certain period of time.

**no more messages**

Another important edge case is when we stop receiving messages from Kafka for a long time. We need a way to flush the opened windows. In this implementation, I simply add a reading timeout and if reached, I advance the internal clock of the aggregator and trigger the closing function. This ensures we don't have hanging windows around for too long.

## Final solution

The final solution implements the following pipeline (all connected by buffered channels):
```text
kafka reader -> JSON unmarshallers -> aggregator -> kafka writer
```
The kafka reader runs in the main thread.
The number of unmarshallers is controlled by the `N_UNMARSHALLERS` constant.
The kafka writer spawns one anonymous goroutine for each `kafka.sendMessage`.

The kafka broker and topic are all controlled by constants at the top of `main.go`, 
as well as the granularity and max lag allowed. In a real program, those would be commandline arguments.

The program stops when the kafka reader hits a timeout (`KAFKA_TIMEOUT`), but in a realtime scenario it will simply inform the aggregator (no stopping!).


## Hardware and software

* Go version 1.14 darwin/amd64
* Macbook Pro 2015
* Dockerised Kafka (see `docker-compose.yml`)
* data from `stream.jsonl` (the program reads from the beginning (offset 0) each time)
* Goland IDE 2019.3.3 for development


## Benchmarks and debug

### Tools I used

* `pprof`: for profiling
* `go tool tracer`: for tracing
* `time` library, `fmt.Print` and logging
* `htop`

### Benchmark experiment

**overview**

Since I haven't lots of time, the only "benchmark" I did was running the program 10 times for different `N_UNMARSHALLER` values. 
I also wrote a "baseline" program that only reads frames from Kafka (no JSON parsing), to have a point of comparison.

**results**

The results are shown in the following table (the first line is the baseline):

| #u | cnt |  mean  |  std  |   min  |   25%  |   50%  |   75%  |   max  |
|:--:|:---:|:------:|:-----:|:------:|:------:|:------:|:------:|:------:|
|  - |  10 | 104.60 | 10.88 |  80.48 | 105.92 | 106.15 | 110.90 | 117.87 |
|  1 |  10 | 125.23 |  8.38 | 103.44 | 124.80 | 125.67 | 129.54 | 135.08 |
|  2 |  10 | 120.76 |  2.17 | 118.17 | 119.28 | 120.00 | 122.33 | 124.47 |
|  5 |  10 | 120.58 |  1.60 | 116.80 | 120.02 | 121.01 | 121.33 | 122.36 |
| 10 |  10 | 116.59 |  1.21 | 114.52 | 115.89 | 116.36 | 117.45 | 118.76 |
| 20 |  10 | 109.74 |  7.39 |  94.00 | 108.61 | 109.77 | 113.95 | 120.18 |


Hence, a basic program that *only* reads from Kafka (no JSON parsing or anything else), takes a mean of 104s on my machine (well, 94s if you subtract the `KAFKA_TIMEOUT`).

My solution, using the same timeout, takes between 110 and 125 seconds, depending on the number of unmarshaller processes (`#u`).

We have 1M records, which makes **8-9K frames/second**.
The whole processing (unmarshalling, aggregation, write) thus adds between 6 and 21 microseconds by frame compared to just a read.

**notes**

Those measures are not perfect and should be taken only as hints. Among others:

* I ran the benchmarks on my machine (Mac), with lots of other stuff running (even though I tried not to use it actively during the execution);
* kafka runs inside a docker container, and we know how unstable Docker for Mac is;
* I ran each experiment only 10 times,
* ...


## What else to try / TODO

With the current implementation:

* have a proper commandline interface instead of using constants in the code
* try other implementations of the kafka client. I used `kafka-go` because it is written completely in Go, but maybe the official client (or other libraries) is faster. It is important since kafka IO is a bottleneck
* try having multiple readers+aggregators (using kafka partitions cleverly)
* better profiling + implement tests !!!!!!!
* if needed: commit kafka offsets

## Bonus questions

**JSON, a good format ?**

The goods: it is human readable and flexible (no schema required). The cons: it is very verbose (more bytes to store/transfer) and slow to parse !
Since our data are structured and made mostly for machine consumption, there are format way more suitable. For example: protobuf (my favorite) or MessagePack.

**count things in different time frames, parse JSON once**

This is kind of easy. Since we have goroutines that do each step (kafka reader -> unmarshaller -> aggregator -> emitter), if we want multiple time frame aggregations we just add a step between the unmarshaller and the aggregator, that is in charge of forwarding the same JSON data to the (now) multiple aggregators.

**cope with failure if the app crashes**

This is what kafka offsets are for, so we can restart where we left off. The only problem is for the opened windows at the time of the crash. Without proper handling those would be either lost or incorrect (count less than expected).

**handle late/random timestamps**

See the edge cases section above. Handling late timestamps is "easy", handling random timestamps is way harder !

**overhead of the json parser**

In the time I had, the only sure thing is that more than 15% of the CPU time is spent inside the `json.Unmarshal` routine. 


## Other notes

I misread the assignment at first, and wrote a program that counted the number of frames per user each minute.
This was more fun, because I could then implement some dispatching/partitioning scheme and split the work between multiple aggregators based on the user id. But it was not the use case ! 
I also designed so the program would work with real-time data, not only historical.

Also, I received the challenge at the same time I learned I would be hospitalised. I was thus not totally focused on the assignment and couldn't spend 8h on it. But anyway, I wanted to try, so here it is !