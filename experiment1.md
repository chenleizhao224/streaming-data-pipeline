# Kafka Experiments

## Experiment: Consumer crash before commit

### Setup
- Topic: `user_events_v2`
- Consumer group: `crash-test-group`
- Auto commit disabled
- Manual commit after processing

### Behavior
The consumer processed the event and wrote it to local storage.
Before committing the offset, the process was intentionally crashed.

### Result
After restarting the consumer, Kafka delivered the same message again.

### Conclusion
This demonstrates at-least-once delivery semantics:
if processing succeeds but offset commit does not happen, the message may be reprocessed.
