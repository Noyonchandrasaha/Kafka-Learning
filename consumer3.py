from confluent_kafka import Consumer
import json

def process_message(message):
    data = json.loads(message)
    print(f"Processing message: {data}")

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

consumer = Consumer(consumer_config)

consumer.subscribe(["UserCreated"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        process_message(msg.value().decode('utf-8'))
        consumer.commit()
        print(f"[CONSUMER 3] Message: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()