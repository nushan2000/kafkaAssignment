import json

from confluent_kafka import Consumer

DLQ_TOPIC = "orders_dlq"
DLQ_GROUP = "dlq-consumer-group-2"


def create_dlq_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": DLQ_GROUP,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(config)
    consumer.subscribe([DLQ_TOPIC])
    return consumer


def main():
    consumer = create_dlq_consumer()
    print("DLQ consumer started (Ctrl+C to stop)...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"[ERROR] DLQ consumer error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                print(f"[ERROR] Failed to decode DLQ message: {exc}")
                continue

            print("\n========== DLQ EVENT ==========")
            print(f"Key: {msg.key().decode('utf-8') if msg.key() else 'None'}")
            print(json.dumps(data, indent=2))

    except KeyboardInterrupt:
        print("Stopping DLQ consumer...")

    finally:
        consumer.close()
        print("DLQ consumer closed.")


if __name__ == "__main__":
    main()
