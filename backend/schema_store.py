SCHEMA = {
    "orders_raw": {
        "description": "Raw individual orders placed by users in real time",
        "columns": {
            "order_id":    "Unique UUID for each order",
            "user_id":     "User who placed the order e.g. user_123",
            "product":     "Product ordered: Biryani, Pizza, Burger, Sushi, Pasta, Dosa, Noodles, Thali, Sandwich, Wrap",
            "amount":      "Order value in Indian Rupees",
            "city":        "City: Delhi, Mumbai, Bangalore, Pune, Hyderabad, Chennai, Kolkata, Jaipur",
            "status":      "Order status: placed, confirmed, preparing, delivered",
            "created_at":   "When the order was placed (UTC)",
            "ingested_at": "When the record arrived in PostgreSQL (UTC)",
        }
    },
    "orders_per_minute": {
        "description": "Pre-aggregated order statistics per city per 1-minute window",
        "columns": {
            "window_start":  "Start of the 1-minute window",
            "window_end":    "End of the 1-minute window",
            "city":          "City name",
            "total_orders":  "Number of orders in this window",
            "total_revenue": "Total revenue in rupees",
            "avg_order_val": "Average order value in rupees",
        }
    },
    "flagged_orders": {
        "description": "Orders flagged as suspicious by fraud detection",
        "columns": {
            "order_id":    "UUID of the flagged order",
            "user_id":     "User who placed the flagged order",
            "amount":      "Order amount in rupees",
            "city":        "City where order was placed",
            "flag_reason": "Why flagged: high_value or geo_anomaly",
            "created_at":   "When the order was placed",
        }
    },
    "dead_letter_queue": {
        "description": "Malformed or invalid events that failed pipeline processing",
        "columns": {
            "raw_message":   "The original raw message that failed",
            "error_message": "What went wrong",
            "error_type":    "Python exception type",
            "failed_at":     "When the failure occurred",
            "pipeline_step": "Which pipeline step failed",
        }
    }
}

def build_schema_prompt(project: str, dataset: str) -> str:
    lines = [
        f"You have access to a PostgreSQL database called '{dataset}'",
        "It contains the following tables:\n"
    ]
    for table, meta in SCHEMA.items():
        lines.append(f"TABLE: {table}")
        lines.append(f"Description: {meta['description']}")
        lines.append("Columns:")
        for col, desc in meta["columns"].items():
            lines.append(f"  - {col}: {desc}")
        lines.append("")
    return "\n".join(lines)
