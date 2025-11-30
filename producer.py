from confluent_kafka import Producer
import uuid
import json

producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "my-producer"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


data = {
    "id": str(uuid.uuid4()),
    "name": "test",
    "age": 26,
    "city": "New York"
}

value = json.dumps(data).encode("utf-8")

producer.produce("UserCreated", value=value, callback=delivery_report)

producer.flush()
