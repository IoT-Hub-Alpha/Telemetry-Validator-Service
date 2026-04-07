from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from iot_logging import FastAPIRequestContextMiddleware, StructuredJsonFormatter
import logging

app = FastAPI()


logging.basicConfig(level=logging.INFO)
for handler in logging.root.handlers:
    handler.setFormatter(StructuredJsonFormatter())

app.add_middleware(FastAPIRequestContextMiddleware)


# allow everyone, since its dev, why the hell not, right???
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def ready():
    return {"health": "ok"}
