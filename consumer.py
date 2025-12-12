from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "bonus_22B030608",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for msg in consumer:
    print(msg.value)
