from fastapi import APIRouter
from db import get_conn

router = APIRouter()

@router.get("/fraud-alerts")
def fraud_alerts():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT order_id, user_id, amount,
               city, flag_reason, created_at
        FROM flagged_orders
        WHERE created_at >= NOW() - interval '2 hours'
        ORDER BY created_at DESC
        LIMIT 100
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "order_id":    r[0],
            "user_id":     r[1],
            "amount":      float(r[2]),
            "city":        r[3],
            "flag_reason": r[4],
            "created_at":   str(r[5]),
        }
        for r in rows
    ]

@router.get("/fraud-summary")
def fraud_summary():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT flag_reason,
               COUNT(*)                       AS total,
               ROUND(AVG(amount)::numeric, 2) AS avg_amount
        FROM flagged_orders
        WHERE created_at >= date_trunc('day', NOW())
        GROUP BY flag_reason
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "flag_reason": r[0],
            "total":       r[1],
            "avg_amount":  float(r[2]),
        }
        for r in rows
    ]

@router.get("/dead-letter-queue")
def dead_letter_queue():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT raw_message, error_message,
               error_type, failed_at, pipeline_step
        FROM dead_letter_queue
        WHERE failed_at >= NOW() - interval '24 hours'
        ORDER BY failed_at DESC
        LIMIT 100
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "raw_message":   r[0],
            "error_message": r[1],
            "error_type":    r[2],
            "failed_at":     str(r[3]),
            "pipeline_step": r[4],
        }
        for r in rows
    ]
