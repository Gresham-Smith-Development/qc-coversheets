from __future__ import annotations

from functools import lru_cache

from app.security.hmac_verifier import HmacVerifier
from app.services.concurrency import ConcurrencyLimiter
from app.services.erp_client import ErpClient
from app.services.ingest_service import IngestService
from app.settings import get_settings


@lru_cache(maxsize=1)
def get_hmac_verifier() -> HmacVerifier:
    settings = get_settings()
    return HmacVerifier(secret=settings.erp_hmac_secret)


@lru_cache(maxsize=1)
def get_limiter() -> ConcurrencyLimiter:
    settings = get_settings()
    return ConcurrencyLimiter(limit=settings.ingest_concurrency_limit)


@lru_cache(maxsize=1)
def get_erp_client() -> ErpClient:
    settings = get_settings()
    return ErpClient(
        base_url=settings.erp_fetch_base_url,
        token_url=settings.erp_token_url,
        stored_procedure=settings.erp_stored_procedure,
        username=settings.erp_username,
        password=settings.erp_password,
        grant_type=settings.erp_grant_type,
        integrated=settings.erp_integrated,
        database=settings.erp_database,
        refresh_token=settings.erp_refresh_token,
        client_id=settings.erp_client_id,
        client_secret=settings.erp_client_secret,
        scope=settings.erp_scope,
    )


def get_ingest_service() -> IngestService:
    settings = get_settings()
    return IngestService(erp_client=get_erp_client(), limiter=get_limiter(), ingest_mode=settings.ingest_mode)
