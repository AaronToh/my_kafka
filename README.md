# Description

My attempt at an implementation of the Kafka wire protocol in C++, with reference to the [Kafka protocol documentation](https://kafka.apache.org/protocol.html). I implement Produce (Api Key 0), Metadata (Api Key 3), and ApiVersions (Api Key 18).

## Parsing Flexibility

Kafka encodes messages differently depending on the version and record format, so the same field at a known offset can tell you how to parse the rest. For example, a magic byte in each record batch indicates how we should parse the record. Similarly, some API versions encode lengths as `varint` using bit manipulation rather than fixed `int32_t`. Signed values use zigzag encoding on top of varint to keep negative numbers small.

## Concurrency Strategy

I first considered a thread pool to handle concurrent requests, but if a single request takes a long time and many accumulate, a bottleneck occurs. Instead I used `epoll`, which listens for incoming data from any socket we are interested in without blocking on any one of them. Since TCP is a byte stream, messages may arrive incomplete - so we accumulate bytes per connection until a full length-prefixed frame arrives before parsing.

## Build

Since `epoll` is Linux-only, Docker is required on macOS:

```bash
docker build -t my_kafka . && docker run -p 9092:9092 my_kafka
```

On Linux, build and run directly:

```bash
cmake -S . -B build && cmake --build build && ./build/my_kafka
```
