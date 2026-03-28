from kafka import KafkaConsumer
import json
import time
from pathlib import Path
from datetime import datetime


OUTPUT_DIR = Path("data")
PROCESSED_IDS_FILE = Path("data/processed_event_ids.txt")


def get_output_file(created_at: str) -> Path:
    """
    Create a partitioned local file path based on event timestamp.
    Example:
    data/year=2026/month=03/day=28/events.jsonl
    """
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    folder = OUTPUT_DIR / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / "events.jsonl"


def load_processed_ids() -> set[str]:
    """
    Load processed event IDs from local file.
    """
    if not PROCESSED_IDS_FILE.exists():
        return set()

    with PROCESSED_IDS_FILE.open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def save_processed_id(event_id: str) -> None:
    """
    Persist a processed event ID.
    """
    PROCESSED_IDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with PROCESSED_IDS_FILE.open("a", encoding="utf-8") as f:
        f.write(event_id + "\n")


def main() -> None:
    consumer = KafkaConsumer(
        "user_events_v2",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="idempotent-test-group",
    )

    print("Starting idempotent consumer...")

    processed_ids = load_processed_ids()
    print(f"Loaded {len(processed_ids)} processed event IDs.")

    try:
        for message in consumer:
            event = message.value
            event_id = event["event_id"]

            print(
                f"[RECEIVED] partition={message.partition}, "
                f"offset={message.offset}, event_id={event_id}"
            )

            if event_id in processed_ids:
                print(f"[SKIP DUPLICATE] event_id={event_id}")
                consumer.commit()
                print(f"[COMMIT DUPLICATE] partition={message.partition}, offset={message.offset}")
                continue

            # Simulate processing time
            time.sleep(1)

            # Write event to local file
            output_file = get_output_file(event["created_at"])
            with output_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event) + "\n")

            # Mark event as processed
            save_processed_id(event_id)
            processed_ids.add(event_id)

            print(
                f"[PROCESSED] partition={message.partition}, "
                f"offset={message.offset}, event_id={event_id}"
            )

            # Commit after processing is complete
            consumer.commit()
            print(f"[COMMIT] partition={message.partition}, offset={message.offset}")

    except Exception as e:
        print(f"Consumer crashed with error: {e}")
        raise

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()