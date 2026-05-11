from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from gemini import generate_sql
from db     import get_conn
import re

router  = APIRouter()
PROJECT = "streampulse"
DATASET = "orders_analytics"

class QueryRequest(BaseModel):
    question: str

def run_query(sql: str) -> dict:
    if not sql.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries allowed")
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    rows    = []
    for row in cur.fetchall():
        rows.append({
            col: (float(val) if isinstance(val, (int, float))
                  else str(val) if val is not None else None)
            for col, val in zip(columns, row)
        })
    conn.close()
    return {"columns": columns, "rows": rows, "row_count": len(rows)}

@router.post("/ask")
def ask(req: QueryRequest):
    if not req.question.strip():
        raise HTTPException(400, "Question cannot be empty")
    try:
        llm_result = generate_sql(req.question, PROJECT, DATASET)
        sql        = llm_result.get("sql", "")
        if not sql:
            raise HTTPException(500, "Could not generate SQL")
        
        # fix table names — remove project prefix if Gemini adds it
        sql = re.sub(r'`?streampulse\.orders_analytics\.',
                     '', sql)
        sql = re.sub(r'`', '', sql)
        
        bq_result = run_query(sql)
        return {
            "question":    req.question,
            "sql":         sql,
            "explanation": llm_result.get("explanation", ""),
            "chart_type":  llm_result.get("chart_type", "table"),
            "x_axis":      llm_result.get("x_axis", ""),
            "y_axis":      llm_result.get("y_axis", ""),
            "columns":     bq_result["columns"],
            "rows":        bq_result["rows"],
            "row_count":   bq_result["row_count"],
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@router.get("/ask/suggestions")
def suggestions():
    return [
        "Show me top 5 cities by revenue today",
        "Which product has the most orders in the last hour?",
        "How many fraud alerts were there today?",
        "What is the total revenue today?",
        "Which city has the highest average order value?",
        "Show me revenue per minute for the last 30 minutes",
        "Which user placed the most orders today?",
        "Show me all high value fraud alerts today",
    ]
