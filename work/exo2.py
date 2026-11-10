import json

from kafka import KafkaConsumer

TOPIC = "weather_stream"   # topic a lire
BROKER = "kafka:9092"      # Kafka depuis les conteneurs

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

for msg in consumer:
    print(f"[{msg.topic} p{msg.partition}@{msg.offset}] {msg.value}", flush=True)
