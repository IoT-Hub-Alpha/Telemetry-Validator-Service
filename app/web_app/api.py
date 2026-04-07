from fastapi import FastAPI
from iot_logging import FastAPIRequestContextMiddleware, StructuredJsonFormatter
import logging

app = FastAPI()


logging.basicConfig(level=logging.INFO)
for handler in logging.root.handlers:
    handler.setFormatter(StructuredJsonFormatter())


@app.get("/health")
async def ready():
    return {"health": "ok"}
