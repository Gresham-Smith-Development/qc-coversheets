from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg
from fastapi import HTTPException

from app.models.dto import IngestRequest, IngestResponse
from app.services.concurrency import ConcurrencyLimiter
from app.services.erp_client import ErpClient

logger = logging.getLogger("app.ingest")


UPSERT_INGEST_EVENT_SQL = """
INSERT INTO qc_coversheet.ingest_event (
    event_id, qc_udic_id, event_type, event_time, correlation_id,
    status, attempt_count, first_seen_at, last_seen_at, last_error, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, 'received', 0, now(), now(), NULL, now(), now())
ON CONFLICT (event_id) DO UPDATE
SET qc_udic_id = EXCLUDED.qc_udic_id,
    event_type = EXCLUDED.event_type,
    event_time = COALESCE(EXCLUDED.event_time, qc_coversheet.ingest_event.event_time),
    correlation_id = EXCLUDED.correlation_id,
    last_seen_at = now(),
    updated_at = now()
RETURNING status, attempt_count;
"""

MARK_PROCESSING_SQL = """
UPDATE qc_coversheet.ingest_event
SET status = 'processing',
    attempt_count = attempt_count + 1,
    last_error = NULL,
    last_seen_at = now(),
    updated_at = now()
WHERE event_id = $1;
"""

MARK_PROCESSED_SQL = """
UPDATE qc_coversheet.ingest_event
SET status = 'processed',
    last_error = NULL,
    last_seen_at = now(),
    updated_at = now()
WHERE event_id = $1;
"""

MARK_FAILED_SQL = """
UPDATE qc_coversheet.ingest_event
SET status = 'failed',
    last_error = $2,
    last_seen_at = now(),
    updated_at = now()
WHERE event_id = $1;
"""

UPSERT_INGEST_JOB_SQL = """
INSERT INTO qc_coversheet.ingest_job (
    qc_udic_id, latest_event_id, status, attempt_count,
    run_after, locked_at, locked_by, last_error, created_at, updated_at
)
VALUES ($1, $2, 'queued', 0, now(), NULL, NULL, NULL, now(), now())
ON CONFLICT (qc_udic_id) DO UPDATE
SET latest_event_id = EXCLUDED.latest_event_id,
    status = 'queued',
    run_after = now(),
    last_error = NULL,
    updated_at = now();
"""

UPSERT_PROJECT_EXECUTION_SQL = """
INSERT INTO qc_coversheet.project_execution_record (
    pep_udic_id, received_at, source_payload, created_at, updated_at
)
VALUES ($1, $2, $3::jsonb, now(), now())
ON CONFLICT (pep_udic_id) DO UPDATE
SET received_at = EXCLUDED.received_at,
    source_payload = EXCLUDED.source_payload,
    updated_at = now()
RETURNING id;
"""

UPSERT_PROJECT_SQL = """
INSERT INTO qc_coversheet.project (
    project_wbs, project_name_current, market_current, location_current, created_at, updated_at
)
VALUES ($1, $2, $3, $4, now(), now())
ON CONFLICT (project_wbs) DO UPDATE
SET project_name_current = COALESCE(EXCLUDED.project_name_current, qc_coversheet.project.project_name_current),
    market_current = COALESCE(EXCLUDED.market_current, qc_coversheet.project.market_current),
    location_current = COALESCE(EXCLUDED.location_current, qc_coversheet.project.location_current),
    updated_at = now()
RETURNING id;
"""

UPSERT_CONTACT_SQL = """
INSERT INTO qc_coversheet.contact (
    erp_contact_id, email, display_name, last_seen_at, created_at, updated_at
)
VALUES ($1, $2, $3, now(), now(), now())
ON CONFLICT (erp_contact_id) DO UPDATE
SET email = COALESCE(EXCLUDED.email, qc_coversheet.contact.email),
    display_name = COALESCE(EXCLUDED.display_name, qc_coversheet.contact.display_name),
    last_seen_at = now(),
    updated_at = now()
RETURNING id;
"""

UPSERT_DISCIPLINE_SQL = """
INSERT INTO qc_coversheet.discipline (
    erp_discipline_code, discipline_name, active, created_at, updated_at
)
VALUES ($1, $2, true, now(), now())
ON CONFLICT (erp_discipline_code) DO UPDATE
SET discipline_name = COALESCE(EXCLUDED.discipline_name, qc_coversheet.discipline.discipline_name),
    active = true,
    updated_at = now()
RETURNING id;
"""

SELECT_COVERSHEET_BY_QC_UDIC_SQL = """
SELECT id FROM qc_coversheet.qc_coversheet_coversheet
WHERE qc_coversheet_udic_id = $1
ORDER BY ingested_at DESC
LIMIT 1;
"""

UPDATE_COVERSHEET_SQL = """
UPDATE qc_coversheet.qc_coversheet_coversheet
SET project_execution_record_id = $2,
    project_id = $3,
    ingested_at = $4,
    source_created_at = $5,
    project_wbs = $6,
    submittal_name = $7,
    submittal_date = $8,
    project_name_snapshot = $9,
    client_name_snapshot = $10,
    market_snapshot = $11,
    location_snapshot = $12,
    pm_name_snapshot = $13,
    pm_email_snapshot = $14,
    pp_name_snapshot = $15,
    pp_email_snapshot = $16,
    updated_at = now()
WHERE id = $1;
"""

INSERT_COVERSHEET_SQL = """
INSERT INTO qc_coversheet.qc_coversheet_coversheet (
    project_execution_record_id, project_id, qc_coversheet_udic_id, ingested_at, source_created_at,
    project_wbs, submittal_name, submittal_date, project_name_snapshot, client_name_snapshot,
    market_snapshot, location_snapshot, pm_name_snapshot, pm_email_snapshot, pp_name_snapshot,
    pp_email_snapshot, updated_at
)
VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15,
    $16, now()
)
RETURNING id;
"""

INSERT_INGEST_AUDIT_EVENT_SQL = """
INSERT INTO qc_coversheet.review_request_event (
    review_request_id, event_type, occurred_at, details, created_at
)
SELECT rr.id, $2, now(), $3::jsonb, now()
FROM qc_coversheet.review_request rr
JOIN qc_coversheet.qc_coversheet_coversheet c ON c.id = rr.qc_coversheet_coversheet_id
WHERE c.id = $1
LIMIT 1;
"""


class IngestService:
    def __init__(self, *, erp_client: ErpClient, limiter: ConcurrencyLimiter, ingest_mode: str) -> None:
        self._erp_client = erp_client
        self._limiter = limiter
        self._ingest_mode = ingest_mode.lower()

    async def handle_ingest(
        self,
        *,
        pool: asyncpg.Pool,
        request: IngestRequest,
        correlation_id: str,
    ) -> tuple[int, IngestResponse]:
        now = datetime.now(timezone.utc)
        event_time = request.event_time or now

        logger.info("trigger_received correlation_id=%s event_id=%s qcUdicID=%s", correlation_id, request.event_id, request.qcUdicID)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                UPSERT_INGEST_EVENT_SQL,
                request.event_id,
                request.qcUdicID,
                request.event_type,
                event_time,
                correlation_id,
            )

            if row and row["status"] == "processed":
                logger.info("processing_complete correlation_id=%s event_id=%s qcUdicID=%s short_circuit=true", correlation_id, request.event_id, request.qcUdicID)
                return 200, IngestResponse(status="processed", qcUdicID=request.qcUdicID, correlation_id=correlation_id)

            if self._ingest_mode == "queue":
                await conn.execute(MARK_PROCESSING_SQL, request.event_id)
                await conn.execute(UPSERT_INGEST_JOB_SQL, request.qcUdicID, request.event_id)
                await conn.execute(
                    "UPDATE qc_coversheet.ingest_event SET status='received', last_seen_at=now(), updated_at=now() WHERE event_id=$1",
                    request.event_id,
                )
                logger.info("processing_complete correlation_id=%s event_id=%s qcUdicID=%s status=queued", correlation_id, request.event_id, request.qcUdicID)
                return 202, IngestResponse(status="queued", qcUdicID=request.qcUdicID, correlation_id=correlation_id)

            await conn.execute(MARK_PROCESSING_SQL, request.event_id)

        acquired = await self._limiter.try_acquire()
        if not acquired:
            await self._mark_failed(pool, request.event_id, "Concurrency limit reached")
            return 429, IngestResponse(status="busy", qcUdicID=request.qcUdicID, correlation_id=correlation_id)

        try:
            logger.info("erp_fetch_start correlation_id=%s event_id=%s qcUdicID=%s", correlation_id, request.event_id, request.qcUdicID)
            payload = await self._erp_client.fetch_qc_payload(request.qcUdicID)
            logger.info("erp_fetch_success correlation_id=%s event_id=%s qcUdicID=%s", correlation_id, request.event_id, request.qcUdicID)

            logger.info("db_upsert_start correlation_id=%s event_id=%s qcUdicID=%s", correlation_id, request.event_id, request.qcUdicID)
            async with pool.acquire() as conn:
                async with conn.transaction():
                    coversheet_id = await self._upsert_state(conn, request.qcUdicID, payload, now)
                    details = {
                        "event_id": str(request.event_id),
                        "event_type": request.event_type,
                        "correlation_id": correlation_id,
                        "qc_udic_id": request.qcUdicID,
                    }
                    # review_request_event requires review_request_id; only write when one exists for this coversheet.
                    await conn.execute(INSERT_INGEST_AUDIT_EVENT_SQL, coversheet_id, "ingest_processed", details)
                    await conn.execute(MARK_PROCESSED_SQL, request.event_id)
            logger.info("db_upsert_success correlation_id=%s event_id=%s qcUdicID=%s", correlation_id, request.event_id, request.qcUdicID)
        except Exception as exc:  # noqa: BLE001
            logger.exception("db_upsert_failed correlation_id=%s event_id=%s qcUdicID=%s", correlation_id, request.event_id, request.qcUdicID)
            await self._mark_failed(pool, request.event_id, str(exc))
            if isinstance(exc, HTTPException):
                raise exc
            raise HTTPException(status_code=503, detail="Failed to process ingest event") from exc
        finally:
            await self._limiter.release()

        logger.info("processing_complete correlation_id=%s event_id=%s qcUdicID=%s", correlation_id, request.event_id, request.qcUdicID)
        return 200, IngestResponse(status="processed", qcUdicID=request.qcUdicID, correlation_id=correlation_id)

    async def _mark_failed(self, pool: asyncpg.Pool, event_id: Any, error: str) -> None:
        async with pool.acquire() as conn:
            await conn.execute(MARK_FAILED_SQL, event_id, error[:2000])

    async def _upsert_state(self, conn: asyncpg.Connection, qc_udic_id: str, payload: dict, now: datetime) -> Any:
        pep_udic_id = self._pick(payload, "pep_udic_id", "pepUdicID", "pepUdicId")
        project_wbs = self._pick(payload, "project_wbs", "projectWbs")
        if not pep_udic_id or not project_wbs:
            raise HTTPException(status_code=422, detail="ERP payload missing required fields: pep_udic_id/project_wbs")

        per_id = await conn.fetchval(UPSERT_PROJECT_EXECUTION_SQL, str(pep_udic_id), now, payload)

        project_name = self._pick(payload, "project_name", "projectName")
        market = self._pick(payload, "market")
        location = self._pick(payload, "location", "department")

        project_id = await conn.fetchval(UPSERT_PROJECT_SQL, str(project_wbs), project_name, market, location)

        pm_email = self._normalize_email(self._pick(payload, "pm_email", "pmEmail"))
        pm_name = self._pick(payload, "pm_name", "pmName")
        pp_email = self._normalize_email(self._pick(payload, "pp_email", "ppEmail"))
        pp_name = self._pick(payload, "pp_name", "ppName")

        if pm_email:
            await conn.fetchval(UPSERT_CONTACT_SQL, f"EMAIL:{pm_email}", pm_email, pm_name)
        if pp_email:
            await conn.fetchval(UPSERT_CONTACT_SQL, f"EMAIL:{pp_email}", pp_email, pp_name)

        for item in self._extract_disciplines(payload):
            code = item.get("code")
            if code:
                name = item.get("name") or code
                await conn.fetchval(UPSERT_DISCIPLINE_SQL, code, name)

        source_created_at = self._pick(payload, "record_created_date", "recordCreatedDate", "source_created_at")
        submittal_name = self._pick(payload, "submittal_name", "submittalName")
        submittal_date = self._pick(payload, "submittal_date", "submittalDate")
        client_name = self._pick(payload, "client_name", "clientName")

        existing_id = await conn.fetchval(SELECT_COVERSHEET_BY_QC_UDIC_SQL, qc_udic_id)

        args = [
            existing_id,
            per_id,
            project_id,
            now,
            source_created_at,
            str(project_wbs),
            submittal_name,
            submittal_date,
            project_name,
            client_name,
            market,
            location,
            pm_name,
            pm_email,
            pp_name,
            pp_email,
        ]

        if existing_id:
            await conn.execute(UPDATE_COVERSHEET_SQL, *args)
            return existing_id

        return await conn.fetchval(
            INSERT_COVERSHEET_SQL,
            per_id,
            project_id,
            qc_udic_id,
            now,
            source_created_at,
            str(project_wbs),
            submittal_name,
            submittal_date,
            project_name,
            client_name,
            market,
            location,
            pm_name,
            pm_email,
            pp_name,
            pp_email,
        )

    @staticmethod
    def _pick(data: dict, *keys: str) -> Any:
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return None

    @staticmethod
    def _normalize_email(value: Any) -> str | None:
        if not value:
            return None
        text = str(value).strip()
        if not text or text.upper() == "NULL":
            return None
        return text.lower()

    @staticmethod
    def _extract_disciplines(payload: dict) -> list[dict[str, str]]:
        raw = payload.get("disciplines")
        if not isinstance(raw, list):
            return []

        result: list[dict[str, str]] = []
        for item in raw:
            if isinstance(item, str):
                code = item.strip().upper()
                if code:
                    result.append({"code": code, "name": code})
                continue

            if isinstance(item, dict):
                code = item.get("erp_discipline_code") or item.get("discipline_code") or item.get("code")
                if not code:
                    continue
                code_text = str(code).strip().upper()
                if not code_text:
                    continue
                name = item.get("discipline_name") or item.get("name") or code_text
                result.append({"code": code_text, "name": str(name).strip()})

        return result
