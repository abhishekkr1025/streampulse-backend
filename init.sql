CREATE TABLE IF NOT EXISTS orders_raw (
    order_id    TEXT,
    user_id     TEXT,
    product     TEXT,
    amount      FLOAT,
    city        TEXT,
    status      TEXT,
    created_at   TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders_per_minute (
    window_start  TIMESTAMPTZ,
    window_end    TIMESTAMPTZ,
    city          TEXT,
    total_orders  INTEGER,
    total_revenue FLOAT,
    avg_order_val FLOAT
);

CREATE TABLE IF NOT EXISTS flagged_orders (
    order_id    TEXT,
    user_id     TEXT,
    amount      FLOAT,
    city        TEXT,
    flag_reason TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW() 
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    raw_message   TEXT,
    error_message TEXT,
    error_type    TEXT,
    failed_at     TIMESTAMPTZ DEFAULT NOW(),
    pipeline_step TEXT
);

SELECT create_hypertable('orders_raw',        'ingested_at', if_not_exists => TRUE);
SELECT create_hypertable('orders_per_minute', 'window_start', if_not_exists => TRUE);
SELECT create_hypertable('flagged_orders',    'created_at',    if_not_exists => TRUE);
