# gemini.py
import google.generativeai as genai
import re, os, json
from schema_store import build_schema_prompt

def get_model():
    """Initialize model lazily so env var is loaded first."""
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
    return genai.GenerativeModel("gemini-2.5-flash")

def generate_sql(question: str, project: str, dataset: str) -> dict:
    model          = get_model()
    schema_context = build_schema_prompt(project, dataset)

    prompt = f"""
You are an expert PostgreSQL and TimescaleDB SQL analyst.

{schema_context}

RULES:
- Use PostgreSQL syntax (NOT BigQuery)
- Do NOT use backticks (`), use plain table names
- Do NOT use project.dataset.table format
- Use table names directly (e.g., orders_raw)
- Column for time is `created_at`
- For date filtering, use: created_at::date = CURRENT_DATE
- For last 24 hours, use: created_at >= NOW() - INTERVAL '24 hours'
- For aggregation, always include GROUP BY when needed
- Always use LIMIT 1000 unless user specifies otherwise
- For "top N", use ORDER BY ... DESC LIMIT N
- Never use SELECT *
- Use ROUND(column::numeric, 2) for float columns
- If the question is ambiguous, make a reasonable assumption

USER QUESTION: {question}

Respond ONLY with a JSON object in this exact format, no markdown:
{{
  "sql": "SELECT ... FROM ... WHERE ... LIMIT ...",
  "explanation": "One sentence explaining what this query does",
  "chart_type": "bar" or "line" or "table",
  "x_axis": "column name",
  "y_axis": "column name"
}}
"""

    response = model.generate_content(prompt)
    text     = response.text.strip()
    text     = re.sub(r"```json|```", "", text).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        sql_match = re.search(r'"sql":\s*"([^"]+)"', text)
        return {
            "sql":         sql_match.group(1) if sql_match else "",
            "explanation": "Query generated",
            "chart_type":  "table",
            "x_axis":      "",
            "y_axis":      ""
        }
