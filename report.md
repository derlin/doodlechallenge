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

1. records are mostly ordered in time,
2. the "unordered" records have a timestamp in the past, never in the future.

#### Structures

Records are consumed by an *aggregator*. It has the following state variables:

* `timeAdvance`: this is the internal clock of the aggregator. Each time it consumes a timestamp, it will advance the clock if the timestamp is in the future;
* `windows`: a collection of opened windows, that is a structure that keeps the count of unique users seen in a given minute

For each time window (here each minute), we need to count how many times each user appears:  a map *string,int* makes sense.
Since we can have lags, we need to keep multiple windows opened in memory and only close a window when the minute it represents is old enough, say `GRANULARY+MAX_LAG`.

The windows are kept in a circular buffer and the structures are reused, so we avoid too many malloc and free. 

### Little benchmarks

(!! we would need to run this multiple times + maybe get rid of docker + run on a server doing ONLY that, not my mac where I work at the same time and have 100 Chrome tabs opened):

```text
    N_AGGREGATORS   = 2
    N_UNMARSHALLERS = 5

2020/03/21 08:39:05 Elapsed: 2m49.058303033s
2020/03/21 08:39:05 Elapsed-timeout: 159.058303s


    N_AGGREGATORS   = 5
    N_UNMARSHALLERS = 10

2020/03/21 08:48:44 Elapsed: 1m54.464564056s
2020/03/21 08:48:44 Elapsed-timeout: 104.464564s

    N_AGGREGATORS   = 1
    N_UNMARSHALLERS = 10

2020/03/21 08:52:30 Elapsed: 2m48.359906045s
2020/03/21 08:52:30 Elapsed-timeout: 158.359906s

    N_AGGREGATORS   = 3
    N_UNMARSHALLERS = 10

2020/03/21 08:56:52 Elapsed: 2m1.351747669s
2020/03/21 08:56:52 Elapsed-timeout: 111.351748s


```

Using `pprof`, we can see that one of the clear bottleneck is the JSON unmarshalling (15%)...
The aggregator also take some time: 7%.

 
Using a very basic program that only reads messages from Kafka (no unmarshalling), it takes ~70s
By doing the unmarshalling as well (single thread), it takes 85s to process the whole `stream.jsonl`.

I saw a lot of variability in the kafka reading/writing. Maybe docker doesn't help. 