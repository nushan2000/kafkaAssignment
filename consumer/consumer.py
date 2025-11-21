import utils.session
import json
import os
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Dict

from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader, parse_schema

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "schemas",
    "order.avsc"
)

INPUT_TOPIC = "orders"
DLQ_TOPIC = "orders_dlq"
CONSUMER_GROUP = "order-consumer-group-2"
MAX_RETRIES = 3


def load_avro_schema():
    print(f"Loading schema from: {SCHEMA_PATH}")
    with open(SCHEMA_PATH, "r") as f:
        schema_dict = json.load(f)
    return parse_schema(schema_dict)


ORDER_SCHEMA = load_avro_schema()


def avro_deserialize(payload: bytes) -> Dict:
    buffer = BytesIO(payload)
    return schemaless_reader(buffer, ORDER_SCHEMA)


@dataclass
class RunningStatistics:
    total_price: float = 0.0
    message_count: int = 0

    def update(self, price: float) -> float:
        self.total_price += price
        self.message_count += 1
        return self.total_price / self.message_count if self.message_count else 0.0


class OrderProcessor:
    def __init__(self):
        self.stats = RunningStatistics()

    def should_fail(self, order: Dict) -> bool:

        return order.get("price", 0.0) > 78.0

    def handle_order(self, order: Dict):
        avg_price = self.stats.update(order["price"])
        print(
            f"[PROCESSED] orderId={order['orderId']} "
            f"product={order['product']} "
            f"price={order['price']} "
            f"running_avg={avg_price:.2f}"
        )


def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
    }
    print("Creating consumer with config:", config)
    consumer = Consumer(config)
    consumer.subscribe([INPUT_TOPIC])
    print(f"Subscribed to topic: {INPUT_TOPIC}")
    return consumer


def create_producer() -> Producer:
    print("Creating DLQ producer…")
    return Producer({"bootstrap.servers": "localhost:9092"})


def send_to_dlq(producer: Producer, order: Dict, reason: str):
    dlq_payload = {
        "order": order,
        "error_reason": reason,
        "timestamp": int(time.time()),
    }

    payload_bytes = json.dumps(dlq_payload).encode("utf-8")

    producer.produce(
        DLQ_TOPIC,
        key=str(order.get("orderId", "")),
        value=payload_bytes,
    )
    producer.flush()

    print(
        f"[DLQ] orderId={order.get('orderId')} moved to {DLQ_TOPIC} "
        f"reason='{reason}'"
    )


def process_with_retries(processor: OrderProcessor, order: Dict, producer: Producer):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if processor.should_fail(order):
                raise ValueError("Price is above allowed limit for this consumer")

            processor.handle_order(order)
            return

        except Exception as exc:
            print(
                f"[WARN] Processing failed for orderId={order.get('orderId')} "
                f"attempt={attempt}/{MAX_RETRIES} error={exc}"
            )

            if attempt == MAX_RETRIES:
                send_to_dlq(producer, order, str(exc))
            else:
                time.sleep(1.0)


def main():
    consumer = create_consumer()
    producer = create_producer()
    processor = OrderProcessor()

    print("Main consumer started (Ctrl+C to stop)...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # uncomment to verify loop is running
                # print("No message…")
                continue

            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            print("[DEBUG] Received message from Kafka")

            try:
                order = avro_deserialize(msg.value())
                print("[DEBUG] Deserialized order:", order)
            except Exception as exc:
                print(f"[ERROR] Failed to deserialize Avro message: {exc}")
                continue

            process_with_retries(processor, order, producer)

    except KeyboardInterrupt:
        print("Stopping main consumer...")

    finally:
        consumer.close()
        print("Main consumer closed.")


if __name__ == "__main__":
    main()
