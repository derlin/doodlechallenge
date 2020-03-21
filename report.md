# Report

## Overview

### Why Go ?

I haven't programmed in Go in more than a year. I spent most of my time writing Python and JS apps. 
However, Go seems a good fit for this challenge. Here are some pros and cons:

* (+) fast language that compiles into native code,
* (+) officially supported by Confluent/Kafka,
* (+) cheap and easy concurrency: goroutines and channels makes it easy to test different parallelisation techniques,
* (+) if the code compiles, it is a good sign it can go into production :)
* (-) the garbage collector can easily become a bottleneck => rust or C/C++ is better for that
* (-) I am rusty in Go...

### Doing aggregations

#### Assumptions

1. records are mostly ordered in time (small lag possible),
2. the "unordered" records have a timestamp in the past, never in the future.

#### Main structures used

* hash map for storing unique users per minute
* circular buffer for storing opened "windows" (one per minute)

#### Overall process

Records are consumed by an *aggregator*. It has the following state variables:

* `timeAdvance`: this is the internal clock of the aggregator. Each time it consumes a timestamp, it will advance the clock if the timestamp is in the future;
* `windows`: a collection of opened windows, that is a structure that keeps the count of unique users seen in a given minute

For each time window (here each minute), we need to keep track of unique users. A HashSet makes sense, but it doesn't exist in go, so a map will do.
Since we can have lags, we need to keep multiple windows opened in memory and close a window only when the minute it represents is old enough, say `GRANULARY+MAX_LAG`.

The windows are kept in a circular buffer and the structures are reused, so we avoid too many malloc and free. Given the max lag and the granularity, we know how many windows at most should be opened: `(max_lag % granulary) + 1`. To be sure, I do `+2`. 

When I open a new window, I add it at the front. When I advance my clock, I check if there are any windows that should be closed, by starting at the back. Since windows are ordered in time, I can stop as soon as the time key of oldest window is not old enough to be closed.

The pseudo code for processing a record is the following:

```text

windows := <circular buffer>
timeAdvance := 0

for record in records:
    minute = record.timestamp - (record.timestamp % 60)
    if record.timestamp > timeAdvance:
        timeAdvance = record.timestamp
        if minute != windows.head().timestamp:
            windows.AddFront(minute)
        tryClosingWindows()
    
    for window in windowsFromFront():
        if window.minute == minute:
            window.add(record)
            break
```

The pseudo code for closing a window is the following:
```text

for window in windowsFromBack():
    if window.key >= timeAdvance - GRANULARITY - MAX_LAG:
        break 

    window.close()
    windows.remove(window)
```

### Basic program

The basic program has one aggregator and is a basic loop that:

1. reads a record from kafka
2. unmarshals the JSON
3. calls the aggregator

To terminate the program properly, reading from kafka has a timeout and if reached, 
the remaining opened windows are flushed and the program exits (see `KAFKA_TIMEOUT`).

## Improvements

### Parallelisation

The aggregator doesn't do much, and it is hard to parallelise since most records are ordered and we need to ensure all records from the same minute are forwarded to the same aggregator (or we need to add a layer of synchronisation/gathering that is likely to be more costly than the speed we could gain by parallelising).

The real bottlenecks are reading/writing to kafka and decoding the JSON message, which is proven by a little CPU profiling using `pprof`.

To parallelise the unmarshalling, we can launch multiple goroutines that share the same in/out channels. The kafka reader sends the raw bytes to the in channel, the aggregator reads from the out channel. For this to work, we need to ensure the channels are buffered: the kafka reader can send at least *number of unmarshallers* before blocking, and likewise the unmarshallers can send the decoded structures in parallel.

For Kafka, we could try to parallelise at least the writing process (again, the reading needs to keep order, hence is more complex). But in a real-life scenario, I guess the best would be to use libraries and tools especially made for that (Kafka streams, Apache Spark, Apache Flink) and benefit from Kafka partitions.

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

## Benchmarks

What I used:

* `pprof`: for profiling
* `go tool tracer`: for tracing
* `time` library, `fmt.Print` and logging

Other experiments:

* write basic programs that only read messages or read+unmarshal to have a grasp of how long this takes / what is the lower bound

## Hardware and software

* Go version 1.14 darwin/amd64
* Macbook Pro 2015
* Dockerised Kafka (see `docker-compose.yml`)
* data from `stream.jsonl` (the program reads from the beginning (offset 0) each time)
* Goland IDE 2019.3.3 for development

## Other

I misread the assignment at first, and wrote a program that counted the number of frames per user each minute.
This was more fun, because I could then implement some dispatching/partitioning scheme and split the work between multiple aggregators based on the user id. But it was not the usecase !