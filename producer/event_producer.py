import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'ecommerce_events'
EVENTS_PER_SECOND = 5

PRODUCTS = [
    {"product_id": "P001", "name": "Wireless Headphones", "category": "Electronics", "price": 79.99},
    {"product_id": "P002", "name": "Running Shoes", "category": "Sports", "price": 59.99},
    {"product_id": "P003", "name": "Coffee Maker", "category": "Kitchen", "price": 49.99},
    {"product_id": "P004", "name": "Python Book", "category": "Books", "price": 39.99},
    {"product_id": "P005", "name": "Yoga Mat", "category": "Sports", "price": 29.99},
    {"product_id": "P006", "name": "Bluetooth Speaker", "category": "Electronics", "price": 89.99},
    {"product_id": "P007", "name": "Desk Lamp", "category": "Home", "price": 24.99},
    {"product_id": "P008", "name": "Water Bottle", "category": "Sports", "price": 19.99},
]

EVENT_TYPES = ['page_view', 'page_view', 'page_view', 'add_to_cart', 'add_to_cart', 'purchase', 'refund']

def generate_event():
    product = random.choice(PRODUCTS)
    event_type = random.choice(EVENT_TYPES)
    quantity = random.randint(1, 3)
    return {
        "event_id": str(fake.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.utcnow().isoformat(),
        "user_id": f"U{random.randint(1000, 9999)}",
        "session_id": str(fake.uuid4()),
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": quantity,
        "total_amount": round(product["price"] * quantity, 2),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "country": fake.country(),
        "city": fake.city(),
    }

def main():
    print("Connecting to Kafka at " + KAFKA_BROKER)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Connected! Sending events to topic: " + TOPIC)
    print("Press Ctrl+C to stop.\n")

    count = 0
    while True:
        event = generate_event()
        producer.send(TOPIC, value=event)
        count += 1
        msg = "[" + str(count) + "] " + event['event_type'] + " | User: " + event['user_id'] + " | Product: " + event['product_name'] + " | Amount: " + str(event['total_amount'])
        print(msg)
        time.sleep(1 / EVENTS_PER_SECOND)

if __name__ == "__main__":
    main()
