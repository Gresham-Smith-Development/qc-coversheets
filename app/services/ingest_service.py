from __future__ import annotations

import json
import logging
from datetime import date as dt_date
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
    erp_contact_id, email, display_name, company_erp_id, erp_company_name, last_seen_at, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, now(), now(), now())
ON CONFLICT (erp_contact_id) DO UPDATE
SET email = COALESCE(EXCLUDED.email, qc_coversheet.contact.email),
    display_name = COALESCE(EXCLUDED.display_name, qc_coversheet.contact.display_name),
    erp_company_name = COALESCE(EXCLUDED.erp_company_name, qc_coversheet.contact.erp_company_name),
    company_erp_id = CASE
        WHEN EXCLUDED.company_erp_id IS NULL AND EXCLUDED.erp_company_name = 'Gresham Smith' THEN NULL
        ELSE COALESCE(EXCLUDED.company_erp_id, qc_coversheet.contact.company_erp_id)
    END,
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
    constructability_start_date = $9,
    project_name_snapshot = $10,
    client_id_snapshot = $11,
    client_name_snapshot = $12,
    market_snapshot = $13,
    location_snapshot = $14,
    pm_name_snapshot = $15,
    pm_email_snapshot = $16,
    pp_name_snapshot = $17,
    pp_email_snapshot = $18,
    updated_at = now()
WHERE id = $1;
"""

INSERT_COVERSHEET_SQL = """
INSERT INTO qc_coversheet.qc_coversheet_coversheet (
    project_execution_record_id, project_id, qc_coversheet_udic_id, ingested_at, source_created_at,
    project_wbs, submittal_name, submittal_date, constructability_start_date, project_name_snapshot, client_id_snapshot, client_name_snapshot,
    market_snapshot, location_snapshot, pm_name_snapshot, pm_email_snapshot, pp_name_snapshot,
    pp_email_snapshot, updated_at
)
VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15,
    $16, $17, $18, now()
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
    def __init__(
        self, *, erp_client: ErpClient, limiter: ConcurrencyLimiter, ingest_mode: str
    ) -> None:
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

        logger.info(
            "trigger_received correlation_id=%s event_id=%s qcUdicID=%s",
            correlation_id,
            request.event_id,
            request.qcUdicID,
        )

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
                logger.info(
                    "processing_complete correlation_id=%s event_id=%s qcUdicID=%s short_circuit=true",
                    correlation_id,
                    request.event_id,
                    request.qcUdicID,
                )
                return 200, IngestResponse(
                    status="processed",
                    qcUdicID=request.qcUdicID,
                    correlation_id=correlation_id,
                )

            if self._ingest_mode == "queue":
                await conn.execute(MARK_PROCESSING_SQL, request.event_id)
                await conn.execute(
                    UPSERT_INGEST_JOB_SQL, request.qcUdicID, request.event_id
                )
                await conn.execute(
                    "UPDATE qc_coversheet.ingest_event SET status='received', last_seen_at=now(), updated_at=now() WHERE event_id=$1",
                    request.event_id,
                )
                logger.info(
                    "processing_complete correlation_id=%s event_id=%s qcUdicID=%s status=queued",
                    correlation_id,
                    request.event_id,
                    request.qcUdicID,
                )
                return 202, IngestResponse(
                    status="queued",
                    qcUdicID=request.qcUdicID,
                    correlation_id=correlation_id,
                )

            await conn.execute(MARK_PROCESSING_SQL, request.event_id)

        acquired = await self._limiter.try_acquire()
        if not acquired:
            await self._mark_failed(pool, request.event_id, "Concurrency limit reached")
            return 429, IngestResponse(
                status="busy", qcUdicID=request.qcUdicID, correlation_id=correlation_id
            )

        try:
            logger.info(
                "erp_fetch_start correlation_id=%s event_id=%s qcUdicID=%s",
                correlation_id,
                request.event_id,
                request.qcUdicID,
            )
            payload = await self._erp_client.fetch_qc_payload(request.qcUdicID)
            logger.info(
                "erp_fetch_success correlation_id=%s event_id=%s qcUdicID=%s",
                correlation_id,
                request.event_id,
                request.qcUdicID,
            )

            logger.info(
                "db_upsert_start correlation_id=%s event_id=%s qcUdicID=%s",
                correlation_id,
                request.event_id,
                request.qcUdicID,
            )
            async with pool.acquire() as conn:
                async with conn.transaction():
                    coversheet_id = await self._upsert_state(
                        conn, request.qcUdicID, payload, now
                    )
                    details = {
                        "event_id": str(request.event_id),
                        "event_type": request.event_type,
                        "correlation_id": correlation_id,
                        "qc_udic_id": request.qcUdicID,
                    }
                    # review_request_event requires review_request_id; only write when one exists for this coversheet.
                    await conn.execute(
                        INSERT_INGEST_AUDIT_EVENT_SQL,
                        coversheet_id,
                        "ingest_processed",
                        json.dumps(details),
                    )
                    await conn.execute(MARK_PROCESSED_SQL, request.event_id)
            logger.info(
                "db_upsert_success correlation_id=%s event_id=%s qcUdicID=%s",
                correlation_id,
                request.event_id,
                request.qcUdicID,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "db_upsert_failed correlation_id=%s event_id=%s qcUdicID=%s",
                correlation_id,
                request.event_id,
                request.qcUdicID,
            )
            await self._mark_failed(pool, request.event_id, str(exc))
            if isinstance(exc, HTTPException):
                raise exc
            raise HTTPException(
                status_code=503, detail="Failed to process ingest event"
            ) from exc
        finally:
            await self._limiter.release()

        logger.info(
            "processing_complete correlation_id=%s event_id=%s qcUdicID=%s",
            correlation_id,
            request.event_id,
            request.qcUdicID,
        )
        return 200, IngestResponse(
            status="processed", qcUdicID=request.qcUdicID, correlation_id=correlation_id
        )

    async def _mark_failed(self, pool: asyncpg.Pool, event_id: Any, error: str) -> None:
        async with pool.acquire() as conn:
            await conn.execute(MARK_FAILED_SQL, event_id, error[:2000])

    async def _upsert_state(
        self, conn: asyncpg.Connection, qc_udic_id: str, payload: dict, now: datetime
    ) -> Any:
        pep_udic_id = self._pick(payload, "pep_udic_id", "pepUdicID", "pepUdicId")
        project_wbs = self._pick(payload, "project_wbs", "projectWbs")
        if not pep_udic_id or not project_wbs:
            raise HTTPException(
                status_code=422,
                detail="ERP payload missing required fields: pep_udic_id/project_wbs",
            )

        per_id = await conn.fetchval(
            UPSERT_PROJECT_EXECUTION_SQL, str(pep_udic_id), now, json.dumps(payload)
        )

        project_name = self._pick(payload, "project_name", "projectName")
        market = self._pick(payload, "market")
        location = self._pick(payload, "location", "department")

        project_id = await conn.fetchval(
            UPSERT_PROJECT_SQL, str(project_wbs), project_name, market, location
        )

        pm_email = self._normalize_email(self._pick(payload, "pm_email", "pmEmail"))
        pm_name = self._pick(payload, "pm_name", "pmName")
        pp_email = self._normalize_email(self._pick(payload, "pp_email", "ppEmail"))
        pp_name = self._pick(payload, "pp_name", "ppName")
        pm_id = self._pick(payload, "pm_id", "pmID")
        pp_id = self._pick(payload, "pp_id", "ppID")

        if pm_id or pm_email:
            pm_company_erp_id = None
            pm_company_name = None
            if pm_email and (
                pm_email.endswith("@greshamsmith.com")
                or pm_email.endswith("@gspnet.com")
            ):
                pm_company_name = "Gresham Smith"
            pm_contact_id = str(pm_id).strip() if pm_id else f"EMAIL:{pm_email}"
            await conn.fetchval(
                UPSERT_CONTACT_SQL,
                pm_contact_id,
                pm_email,
                pm_name,
                pm_company_erp_id,
                pm_company_name,
            )
        if pp_id or pp_email:
            pp_company_erp_id = None
            pp_company_name = None
            if pp_email and (
                pp_email.endswith("@greshamsmith.com")
                or pp_email.endswith("@gspnet.com")
            ):
                pp_company_name = "Gresham Smith"
            pp_contact_id = str(pp_id).strip() if pp_id else f"EMAIL:{pp_email}"
            await conn.fetchval(
                UPSERT_CONTACT_SQL,
                pp_contact_id,
                pp_email,
                pp_name,
                pp_company_erp_id,
                pp_company_name,
            )

        reviewer_data = payload.get("reviewer_data", [])
        if isinstance(reviewer_data, list):
            for reviewer in reviewer_data:
                if not isinstance(reviewer, dict):
                    continue
                reviewer_id = reviewer.get("reviewerID")
                reviewer_email = self._normalize_email(reviewer.get("reviewerEmail"))
                reviewer_name = reviewer.get("reviewerContactName")
                reviewer_company_id = reviewer.get("reviewerCompanyID")
                reviewer_company_name = reviewer.get("reviewerCompany")

                if reviewer_id:
                    reviewer_contact_id = str(reviewer_id).strip()
                elif reviewer_email:
                    reviewer_contact_id = f"EMAIL:{reviewer_email}"
                else:
                    continue

                await conn.fetchval(
                    UPSERT_CONTACT_SQL,
                    reviewer_contact_id,
                    reviewer_email,
                    reviewer_name,
                    reviewer_company_id,
                    reviewer_company_name,
                )

        for item in self._extract_disciplines(payload):
            code = item.get("code")
            if code:
                name = item.get("name") or code
                await conn.fetchval(UPSERT_DISCIPLINE_SQL, code, name)

        source_created_at_raw = self._pick(
            payload, "record_created_date", "recordCreatedDate", "source_created_at"
        )
        submittal_name = self._pick(payload, "submittal_name", "submittalName")
        submittal_date_raw = self._pick(payload, "submittal_date", "submittalDate")
        constructability_start_date_raw = self._pick(
            payload, "constructability_start_date", "constructabilityStartDate"
        )
        client_id = self._pick(payload, "client_id", "clientNameID")
        client_name = self._pick(payload, "client_name", "clientName")

        source_created_at = self._to_datetime_or_none(
            source_created_at_raw, "recordCreatedDate"
        )
        submittal_date = self._to_date_or_none(submittal_date_raw, "submittalDate")
        constructability_start_date = self._to_date_or_none(
            constructability_start_date_raw, "constructabilityStartDate"
        )

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
            constructability_start_date,
            project_name,
            client_id,
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
            constructability_start_date,
            project_name,
            client_id,
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
                code = (
                    item.get("erp_discipline_code")
                    or item.get("discipline_code")
                    or item.get("code")
                )
                if not code:
                    continue
                code_text = str(code).strip().upper()
                if not code_text:
                    continue
                name = item.get("discipline_name") or item.get("name") or code_text
                result.append({"code": code_text, "name": str(name).strip()})

        return result

    @staticmethod
    def _to_datetime_or_none(value: Any, field_name: str) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid datetime for {field_name}: {value}",
                ) from exc
        raise HTTPException(
            status_code=422, detail=f"Invalid datetime type for {field_name}: {value}"
        )

    @staticmethod
    def _to_date_or_none(value: Any, field_name: str) -> dt_date | None:
        if value is None or value == "":
            return None
        if isinstance(value, dt_date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            try:
                if "T" in raw:
                    return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
                return dt_date.fromisoformat(raw)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422, detail=f"Invalid date for {field_name}: {value}"
                ) from exc
        raise HTTPException(
            status_code=422, detail=f"Invalid date type for {field_name}: {value}"
        )
