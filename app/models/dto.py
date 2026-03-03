from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class IngestRequest(BaseModel):
    qcUdicID: str = Field(min_length=1)
    event_id: UUID
    event_type: str = Field(min_length=1)
    event_time: datetime | None = None
    correlation_id: str | None = None


class IngestResponse(BaseModel):
    status: str
    qcUdicID: str
    correlation_id: str
