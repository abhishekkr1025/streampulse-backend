from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import orders, fraud, askdata


app = FastAPI(title="StreamPulse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(orders.router, prefix="/api")
app.include_router(fraud.router,  prefix="/api")
app.include_router(askdata.router, prefix="/api")
@app.get("/health")
def health():
    return {"status": "ok"}
