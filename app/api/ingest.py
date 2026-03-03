from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from app.db import get_pool
from app.models.dto import IngestRequest
from app.security.hmac_verifier import HmacVerifier
from app.services.correlation import CorrelationProvider
from app.services.ingest_service import IngestService
from app.state import get_ingest_service, get_hmac_verifier

router = APIRouter(tags=["ingest"])


@router.post("/ingest")
async def ingest_endpoint(
    payload: IngestRequest,
    request: Request,
    pool=Depends(get_pool),
    verifier: HmacVerifier = Depends(get_hmac_verifier),
    service: IngestService = Depends(get_ingest_service),
) -> JSONResponse:
    raw_body = await request.body()

    timestamp = request.headers.get("X-Timestamp")
    signature = request.headers.get("X-Signature")
    if not timestamp or not signature:
        raise HTTPException(status_code=401, detail="Missing HMAC headers")

    verifier.verify(timestamp=timestamp, signature=signature, raw_body=raw_body)

    correlation_id = CorrelationProvider.resolve(request, payload.correlation_id)
    status_code, response = await service.handle_ingest(pool=pool, request=payload, correlation_id=correlation_id)

    body = response.model_dump()
    headers = {"X-Correlation-Id": correlation_id}
    return JSONResponse(content=body, status_code=status_code, headers=headers)
