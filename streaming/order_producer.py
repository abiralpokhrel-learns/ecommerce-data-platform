"""Produce simulated order events to Kafka topic 'orders_stream'."""
import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from loguru import logger

fake = Faker("pt_BR")
TOPIC = "orders_stream"
BOOTSTRAP_SERVERS = "localhost:29092"

CATEGORIES = [
    "electronics", "books", "fashion", "home_decor", "sports",
    "beauty", "toys", "automotive", "food", "garden"
]


def generate_order() -> dict:
    return {
        "order_id": fake.uuid4(),
        "customer_id": fake.uuid4(),
        "product_category": random.choice(CATEGORIES),
        "item_count": random.randint(1, 5),
        "order_value": round(random.uniform(10, 500), 2),
        "freight_value": round(random.uniform(5, 50), 2),
        "state": random.choice(["SP", "RJ", "MG", "RS", "PR", "BA", "CE"]),
        "event_time": datetime.utcnow().isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    
    logger.info(f"Producing to topic: {TOPIC}")
    
    try:
        while True:
            order = generate_order()
            producer.send(TOPIC, order)
            logger.info(f"Sent order {order['order_id'][:8]}... value=${order['order_value']}")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()