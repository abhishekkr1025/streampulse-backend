# producer/producer_kafka.py
from kafka import KafkaProducer
import json, random, time, uuid
from datetime import datetime, timezone

import os
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

PRODUCTS = ["Biryani","Pizza","Burger","Sushi","Pasta",
            "Dosa","Noodles","Thali","Sandwich","Wrap"]
CITIES   = ["Delhi","Mumbai","Bangalore","Pune",
            "Hyderabad","Chennai","Kolkata","Jaipur"]
STATUSES = ["placed","confirmed","preparing","delivered"]

def generate_order(anomaly=False):
    return {
        "order_id":  str(uuid.uuid4()),
        "user_id":   f"user_{random.randint(1, 500)}",
        "product":   random.choice(PRODUCTS),
        "amount":    round(random.uniform(5000,15000) if anomaly
                          else random.uniform(80,1800), 2),
        "city":      random.choice(CITIES),
        "status":    random.choice(STATUSES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

count = 0
print("Publishing to Kafka topic: orders-stream")
while True:
    anomaly = random.random() < 0.05
    order   = generate_order(anomaly)
    producer.send("orders-stream", value=order)
    count += 1
    tag = " ⚠ ANOMALY" if anomaly else ""
    print(f"[{count}] {order['city']} ₹{order['amount']} "
          f"{order['product']}{tag}")
    time.sleep(1)