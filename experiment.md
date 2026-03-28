## Experiment: Idempotent Consumer and Duplicate Handling

### Objective

To understand how duplicate message delivery occurs in Kafka and how application-level idempotency can be used to ensure correct processing results.

---

### Background

Kafka provides **at-least-once delivery semantics** by default.  
This means that in failure scenarios (e.g., consumer crash before offset commit), the same message may be delivered more than once.

While Kafka tracks consumption progress using offsets, it does not guarantee that a message has been fully processed at the application level.

---

### Setup

- Topic: `user_events_v2`
- Consumer group: `idempotent-test-group`
- Auto offset commit: disabled (`enable_auto_commit=False`)
- Offset committed manually after processing
- Each message contains a unique `event_id`

---

### Implementation

To achieve idempotent processing, the consumer:

1. Loads previously processed `event_id`s from a local file
2. For each incoming message:
   - Checks if the `event_id` has already been processed
   - If yes → skip processing and commit offset
   - If no → process the message and persist the `event_id`
3. Writes processed events to partitioned local storage

---

### Observations

- Kafka may re-deliver the same message after consumer restart
- Without idempotency, duplicate messages would result in duplicate writes
- With idempotency logic:
  - Duplicate messages were detected using `event_id`
  - Reprocessing was skipped
  - Offset was still committed to advance consumption

---

### Example Output

```text
[RECEIVED] partition=1, offset=0, event_id=abc123
[SKIP DUPLICATE] event_id=abc123
[COMMIT DUPLICATE] partition=1, offset=0