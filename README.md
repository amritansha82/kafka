# Kafka Clone in C++

A custom implementation of a Kafka broker built from scratch in C++. This project demonstrates handling the Kafka wire protocol, serving ApiVersions and Fetch requests, and efficiently managing multiple concurrent clients.

## Supported Features
- **ApiVersions (API key 18):** Responds with supported API versions.
- **DescribeTopicPartitions (API key 75):** Provides partition metadata for requested topics.
- **Fetch (API key 1):** Reads records from topics and partitions.
- **Produce (API key 0):** Handles incoming records.
- **KRaft Metadata:** Parses KRaft combined logs to accurately build cluster metadata.

## Highlights
- **Custom Protocol Parsing:** Complete from-scratch parsing and serialization of the Kafka wire protocol, including encoding and decoding of complex message bodies and headers.
- **High-Performance Multi-Client Handling:** Built with a highly scalable, multi-threaded architecture to support multiple clients simultaneously.
  - **Epoll-based Event Loop:** Utilizes Linux `epoll` for efficient non-blocking I/O multiplexing.
  - **Worker Pool:** A pool of worker threads distributes incoming connections (round-robin), ensuring no single thread becomes a bottleneck.
  - **Dedicated I/O Threads:** Dedicated I/O worker threads offload tasks using a thread-safe task queue, allowing the main network loops to remain highly responsive.

## Load Testing
The broker architecture is built for scale. In my load tests, this single broker successfully handles **185,000+ maximum concurrent clients** locally, maintaining an impressive connection rate of over 42,000 connections/sec. This scale is achieved by relying on its epoll and round-robin multi-threaded worker pool. The test peaked at exactly this number due to the default Linux TCP SYN backlog and socket memory limits of the local environment, but the broker software itself has no hardcoded bottlenecks.

## Building the Project

Ensure you have CMake (>= 3.13) and a C++23 compatible compiler installed.

```sh
mkdir build
cd build
cmake ..
make
```

## Running the Broker

```sh
./kafka
```

This will start the Kafka broker on port 9092, ready to accept client connections and requests.
