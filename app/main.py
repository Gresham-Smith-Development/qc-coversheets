from __future__ import annotations

from fastapi import FastAPI

from app.api.ingest import router as ingest_router
from app.db import close_db_pool, init_db_pool
from app.logging_config import configure_logging

configure_logging()

app = FastAPI(title="QC Coversheets Ingest API", version="0.1.0")
app.include_router(ingest_router)


@app.on_event("startup")
async def startup() -> None:
    await init_db_pool()


@app.on_event("shutdown")
async def shutdown() -> None:
    await close_db_pool()
