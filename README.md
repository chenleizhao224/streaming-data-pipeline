# Streaming Data Pipeline

A hands-on Kafka-based streaming pipeline project built for technical refresh, interview preparation, and distributed systems practice.

## Architecture

Python Producer -> Kafka / Redpanda -> Python Consumer -> Partitioned Local Storage

## Goals

- Practice Kafka producer / consumer basics
- Understand partitions, offsets, and consumer groups
- Simulate consumer lag and crash scenarios
- Explore at-least-once delivery and duplicate processing
- Build a small but explainable streaming system

## Current Features

- Python producer generating user events
- Kafka-compatible local broker using Redpanda
- Python consumer writing events into partitioned local files
- Crash simulation before offset commit
- Offset / duplicate-processing experiments

## Experiments

### 1. Multi-partition topic
Created a topic with multiple partitions and produced keyed events to observe partition distribution.

### 2. Consumer lag
Slowed down consumer processing and increased producer speed to simulate lag.

### 3. Consumer crash before commit
Simulated a crash after processing but before committing offsets.
Observed that Kafka re-delivered the same message after restart, demonstrating at-least-once delivery semantics.

## Tech Stack

- Python
- Kafka API (via Redpanda)
- Docker Compose

## Next Steps

- Add idempotent processing
- Add SQL query layer for downstream analytics
- Add optional S3-style / cloud storage integration
