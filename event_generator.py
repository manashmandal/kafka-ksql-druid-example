from faker import Faker
import time
from confluent_kafka import Producer
import json

producer = Producer({"bootstrap.servers": "localhost:29092"})
faker = Faker()


def delivery_report(error, message):
    if error is not None:
        print(f"Message delivery failed: {error}")
    else:
        print(
            f"Message delivered to {message.topic()} [{message.partition()}] at offset {message.offset()}"
        )


while True:
    try:
        data = {
            "profileId": faker.uuid4(),
            "latitude": float(faker.latitude()),
            "longitude": float(faker.longitude()),
        }
        print(data)
        producer.produce(
            "locations", json.dumps(data).encode("utf-8"), callback=delivery_report
        )
    except BufferError:
        print("Local producer queue is full")
        producer.flush()
    # time.sleep(5)
