from kafka import KafkaProducer
import json
import time
import uuid
from datetime import datetime, timezone


def build_event(i: int) -> dict:
    event_types = ["click", "view", "purchase"]
    sources = ["web", "mobile", "api"]

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": i % 5 + 1,
        "event_type": event_types[i % len(event_types)],
        "source": sources[i % len(sources)],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting producer...")

    for i in range(30):
        event = build_event(i)
        key = event["user_id"]
        producer.send("user_events_v2", key=key, value=event)
        print(f"Sent event with key={key}: {event}")
        time.sleep(0.2)

    producer.flush()
    producer.close()
    print("Producer finished.")


if __name__ == "__main__":
    main()