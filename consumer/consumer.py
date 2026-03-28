from kafka import KafkaConsumer
import json
import time
from pathlib import Path
from datetime import datetime


OUTPUT_DIR = Path("data")


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


def main() -> None:
    consumer = KafkaConsumer(
        "user_events_v2",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # disable auto commit so we can control offset commits manually
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="crash-test-group",
    )

    print("Starting consumer...")

    count = 0

    try:
        for message in consumer:
            event = message.value

            print(
                f"[BEFORE PROCESS] partition={message.partition}, "
                f"offset={message.offset}, event_id={event['event_id']}"
            )

            # Simulate processing time
            time.sleep(1)

            # Write event to local file
            output_file = get_output_file(event["created_at"])
            with output_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event) + "\n")

            print(
                f"[AFTER PROCESS] partition={message.partition}, "
                f"offset={message.offset}, event_id={event['event_id']}"
            )

            count += 1

            # Simulate a crash before committing the offset
            if count == 5:
                print("💥 Simulating crash BEFORE commit!")
                raise Exception("Simulated crash")

            # Commit only after processing is complete
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