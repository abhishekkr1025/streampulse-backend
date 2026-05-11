# backend/db.py
import psycopg2, os

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        dbname="orders_analytics",
        user="admin",
        password="password"
    )