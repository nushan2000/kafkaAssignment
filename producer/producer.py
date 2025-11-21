import json
import os
import random
import time
import uuid
from io import BytesIO
from dataclasses import dataclass

from confluent_kafka import Producer
from fastavro import schemaless_writer, parse_schema

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "schemas",
    "order.avsc"
)


def load_avro_schema():
    with open(SCHEMA_PATH, "r") as f:
        schema_dict = json.load(f)
    return parse_schema(schema_dict)


ORDER_SCHEMA = load_avro_schema()


@dataclass
class Order:
    orderId: str
    product: str
    price: float

    def to_dict(self) -> dict:
        return {
            "orderId": self.orderId,
            "product": self.product,
            "price": self.price,
        }


class OrderGenerator:
    PRODUCTS = ["Phone", "Laptop", "Keyboard", "Mouse", "Headphones"]

    @classmethod
    def random_order(cls) -> Order:
        return Order(
            orderId=str(uuid.uuid4()),
            product=random.choice(cls.PRODUCTS),
            price=round(random.uniform(10, 150), 2),
        )


def avro_serialize(order: dict) -> bytes:
    buffer = BytesIO()
    schemaless_writer(buffer, ORDER_SCHEMA, order)
    return buffer.getvalue()


def create_producer() -> Producer:
    config = {
        "bootstrap.servers": "localhost:9092"
    }
    return Producer(config)


def delivery_callback(err, msg):
    if err is not None:
        print(f"[ERROR] Failed to deliver message: {err}")
    else:
        print(
            f"[OK] Sent to topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main():
    producer = create_producer()
    topic = "orders"

    print("Order producer is running (Ctrl+C to stop)...")

    try:
        while True:
            order_obj = OrderGenerator.random_order()
            order_dict = order_obj.to_dict()

            encoded_value = avro_serialize(order_dict)

            producer.produce(
                topic=topic,
                key=order_obj.orderId,
                value=encoded_value,
                callback=delivery_callback
            )

            # Trigger delivery callbacks
            producer.poll(0)

            print(f"[PRODUCED] {order_dict}")

            time.sleep(1.0)

    except KeyboardInterrupt:
        print("Shutting down producer...")

    finally:
        producer.flush()
        print("Producer closed.")


if __name__ == "__main__":
    main()
