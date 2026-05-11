# aggregator.py
# Runs every minute — replaces Airflow for aggregation
import psycopg2, time, logging
from datetime import datetime, timezone
import os
DB_HOST = os.getenv("DB_HOST", "localhost")

logging.basicConfig(level=logging.INFO)

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=5432,
        dbname="orders_analytics",
        user="admin", password="password"
    )

def aggregate():
    conn = get_conn()
    conn.autocommit = True
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO orders_per_minute
        (window_start, window_end, city,
         total_orders, total_revenue, avg_order_val)
        SELECT
            date_trunc('minute', ingested_at),
            date_trunc('minute', ingested_at) + interval '1 minute',
            city,
            COUNT(*),
            ROUND(SUM(amount)::numeric, 2),
            ROUND(AVG(amount)::numeric, 2)
        FROM orders_raw
        WHERE ingested_at >= NOW() - interval '2 minutes'
          AND ingested_at <  NOW() - interval '1 minute'
        GROUP BY date_trunc('minute', ingested_at), city
        ON CONFLICT DO NOTHING
    """)
    conn.close()
    logging.info(f"Aggregated at {datetime.now(timezone.utc)}")

print("Aggregator running — aggregates every 60 seconds")
while True:
    try:
        aggregate()
    except Exception as e:
        logging.error(f"Aggregation error: {e}")
    time.sleep(60)