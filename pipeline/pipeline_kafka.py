# pipeline/pipeline_kafka.py
from kafka import KafkaConsumer
import json, psycopg2, logging
from datetime import datetime, timezone

import os
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_HOST       = os.getenv("DB_HOST", "localhost")

logging.basicConfig(level=logging.INFO)

# ── Database connection ────────────────────────────────────
conn = psycopg2.connect(
    host=DB_HOST, port=5432,
    dbname="orders_analytics",
    user="admin", password="password"
)
conn.autocommit = True
cur = conn.cursor()

# ── Kafka consumer ─────────────────────────────────────────
consumer = KafkaConsumer(
    "orders-stream",
    bootstrap_servers=KAFKA_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="pipeline-consumer",
    auto_offset_reset="latest",
)

KNOWN_CITIES = {"Delhi","Mumbai","Bangalore","Pune",
                "Hyderabad","Chennai","Kolkata","Jaipur"}

def detect_fraud(order):
    if order["amount"] > 2000:
        return "high_value"
    if order["amount"] > 1000 and order["city"] not in KNOWN_CITIES:
        return "geo_anomaly"
    return None

def write_raw(order):
    cur.execute("""
        INSERT INTO orders_raw
        (order_id, user_id, product, amount, city,
         status, created_at, ingested_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (
        order["order_id"], order["user_id"], order["product"],
        float(order["amount"]), order["city"],
        order.get("status","unknown"), order["created_at"],
        datetime.now(timezone.utc)
    ))

def write_fraud(order, reason):
    cur.execute("""
        INSERT INTO flagged_orders
        (order_id, user_id, amount, city, flag_reason, created_at)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        order["order_id"], order["user_id"],
        float(order["amount"]), order["city"],
        reason, order["created_at"]
    ))

def write_dlq(raw, error, step):
    cur.execute("""
        INSERT INTO dead_letter_queue
        (raw_message, error_message, error_type,
         failed_at, pipeline_step)
        VALUES (%s,%s,%s,%s,%s)
    """, (
        str(raw), str(error), type(error).__name__,
        datetime.now(timezone.utc), step
    ))

count = 0
print("Pipeline consuming from Kafka...")

for msg in consumer:
    try:
        order = msg.value

        # validate
        required = {"order_id","user_id","product",
                    "amount","city","created_at"}
        missing  = required - set(order.keys())
        if missing:
            raise ValueError(f"Missing fields: {missing}")

        # write raw
        write_raw(order)

        # fraud detection
        reason = detect_fraud(order)
        if reason:
            write_fraud(order, reason)

        count += 1
        if count % 10 == 0:
            logging.info(f"Processed {count} orders")

    except Exception as e:
        logging.error(f"[DLQ] {e}")
        write_dlq(msg.value, e, "pipeline")