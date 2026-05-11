# backend/routers/orders.py
from fastapi import APIRouter
from db import get_conn

router = APIRouter()

@router.get("/kpis")
def kpis():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*)                     AS total_orders,
            ROUND(SUM(amount)::numeric,2) AS total_revenue,
            ROUND(AVG(amount)::numeric,2) AS avg_order_val,
            COUNT(DISTINCT city)          AS active_cities
        FROM orders_raw
        WHERE ingested_at >= date_trunc('day', NOW())
    """)
    row     = cur.fetchone()
    conn.close()
    return {
        "total_orders":  row[0],
        "total_revenue": float(row[1] or 0),
        "avg_order_val": float(row[2] or 0),
        "active_cities": row[3],
    }

@router.get("/orders-per-minute")
def orders_per_minute():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT window_start, city,
               total_orders, total_revenue, avg_order_val
        FROM orders_per_minute
        WHERE window_start >= NOW() - interval '1 hour'
        ORDER BY window_start DESC
        LIMIT 500
    """)
    rows    = cur.fetchall()
    conn.close()
    return [
        {
            "window_start":  str(r[0]),
            "city":          r[1],
            "total_orders":  r[2],
            "total_revenue": float(r[3]),
            "avg_order_val": float(r[4]),
        }
        for r in rows
    ]

@router.get("/city-breakdown")
def city_breakdown():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT city,
               COUNT(*)                      AS total_orders,
               ROUND(SUM(amount)::numeric,2) AS total_revenue
        FROM orders_raw
        WHERE ingested_at >= NOW() - interval '1 hour'
        GROUP BY city
        ORDER BY total_revenue DESC
    """)
    rows    = cur.fetchall()
    conn.close()
    return [
        {
            "city":          r[0],
            "total_orders":  r[1],
            "total_revenue": float(r[2]),
        }
        for r in rows
    ]

@router.get("/top-products")
def top_products():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT product,
               COUNT(*)                      AS total_orders,
               ROUND(SUM(amount)::numeric,2) AS total_revenue
        FROM orders_raw
        WHERE ingested_at >= NOW() - interval '1 hour'
        GROUP BY product
        ORDER BY total_orders DESC
        LIMIT 10
    """)
    rows    = cur.fetchall()
    conn.close()
    return [
        {
            "product":       r[0],
            "total_orders":  r[1],
            "total_revenue": float(r[2]),
        }
        for r in rows
    ]