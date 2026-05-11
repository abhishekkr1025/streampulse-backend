# airflow/dags/aggregate_orders.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

def aggregate_orders():
    conn = psycopg2.connect(
        host="timescaledb", port=5432,
        dbname="orders_analytics",
        user="admin", password="password"
    )
    cur  = conn.cursor()

    cur.execute("""
        INSERT INTO orders_per_minute
        (window_start, window_end, city,
         total_orders, total_revenue, avg_order_val)
        SELECT
            date_trunc('minute', ingested_at)        AS window_start,
            date_trunc('minute', ingested_at)
              + interval '1 minute'                  AS window_end,
            city,
            COUNT(*)                                 AS total_orders,
            ROUND(SUM(amount)::numeric, 2)           AS total_revenue,
            ROUND(AVG(amount)::numeric, 2)           AS avg_order_val
        FROM orders_raw
        WHERE ingested_at >= NOW() - interval '2 minutes'
          AND ingested_at <  NOW() - interval '1 minute'
        GROUP BY date_trunc('minute', ingested_at), city
        ON CONFLICT DO NOTHING
    """)
    conn.commit()
    conn.close()

with DAG(
    "aggregate_orders",
    schedule_interval="* * * * *",   # every minute
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    PythonOperator(
        task_id="aggregate",
        python_callable=aggregate_orders
    )