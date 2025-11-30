from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "dashboard-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["UserCreated"])

user_count = 0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        data_str = msg.value().decode('utf-8')
        data = json.loads(data_str)
        user_count += 1
        print("Data:", data)
        print(f"Total registered users: {user_count}")

except KeyboardInterrupt:
    print(f"\nExiting. Total users registered during this session: {user_count}")
finally:
    consumer.close()
