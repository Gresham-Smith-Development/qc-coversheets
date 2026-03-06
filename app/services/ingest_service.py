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

ACTIVE_REVIEW_REQUEST_STATUSES = [
    "draft",
    "queued",
    "sent",
    "opened",
    "in_progress",
    "overdue",
]


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

GET_ACTIVE_TEMPLATE_VERSION_SQL = """
SELECT
    ft.id AS form_template_id,
    ftv.version
FROM qc_coversheet.form_template ft
JOIN qc_coversheet.form_template_version ftv
    ON ftv.form_template_id = ft.id
WHERE ft.template_key = $1
  AND ftv.is_active = true
ORDER BY ftv.created_at DESC, ftv.version DESC
LIMIT 1;
"""

SELECT_DISCIPLINE_IDS_BY_CODE_SQL = """
SELECT erp_discipline_code, id
FROM qc_coversheet.discipline
WHERE erp_discipline_code = ANY($1::text[]);
"""

UPSERT_REVIEW_REQUEST_SQL = """
INSERT INTO qc_coversheet.review_request (
    qc_coversheet_coversheet_id,
    reviewer_contact_id,
    reviewer_name_used,
    expected_form_template_id,
    expected_form_version,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, now(), now())
ON CONFLICT (qc_coversheet_coversheet_id, reviewer_contact_id) DO UPDATE
SET expected_form_template_id = EXCLUDED.expected_form_template_id,
    expected_form_version = EXCLUDED.expected_form_version,
    reviewer_name_used = EXCLUDED.reviewer_name_used,
    updated_at = now()
RETURNING id, (xmax = 0) AS inserted;
"""

INSERT_REVIEW_REQUEST_DISCIPLINE_SQL = """
INSERT INTO qc_coversheet.review_request_discipline (
    review_request_id, discipline_id, created_at
)
VALUES ($1, $2, now())
ON CONFLICT (review_request_id, discipline_id) DO NOTHING
RETURNING 1;
"""

DELETE_REVIEW_REQUEST_DISCIPLINE_SQL = """
DELETE FROM qc_coversheet.review_request_discipline
WHERE review_request_id = $1
  AND discipline_id <> ALL($2::uuid[])
RETURNING discipline_id;
"""

CANCEL_REMOVED_REVIEWERS_SQL = """
UPDATE qc_coversheet.review_request
SET status = 'cancelled',
    updated_at = now()
WHERE qc_coversheet_coversheet_id = $1
  AND status = ANY($2::text[])
  AND reviewer_contact_id <> ALL($3::uuid[])
RETURNING id;
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
        reviewer_groups: dict[str, dict[str, Any]] = {}
        if isinstance(reviewer_data, list):
            for reviewer in reviewer_data:
                if not isinstance(reviewer, dict):
                    continue
                reviewer_id = reviewer.get("reviewerID")
                reviewer_email = self._normalize_email(reviewer.get("reviewerEmail"))
                reviewer_name = reviewer.get("reviewerContactName")
                reviewer_company_id = reviewer.get("reviewerCompanyID")
                reviewer_company_name = reviewer.get("reviewerCompany")

                if not reviewer_email:
                    logger.error(
                        "reviewer_missing_email qcUdicID=%s reviewerID=%s reviewerName=%s",
                        qc_udic_id,
                        reviewer_id,
                        reviewer_name,
                    )
                    continue

                if reviewer_id:
                    reviewer_contact_key = str(reviewer_id).strip()
                else:
                    reviewer_contact_key = f"EMAIL:{reviewer_email}"

                group = reviewer_groups.setdefault(
                    reviewer_contact_key,
                    {
                        "reviewer_name": None,
                        "reviewer_email": reviewer_email,
                        "reviewer_company_id": None,
                        "reviewer_company_name": None,
                        "discipline_codes": set(),
                    },
                )
                if reviewer_name and not group["reviewer_name"]:
                    group["reviewer_name"] = reviewer_name
                if reviewer_company_id and not group["reviewer_company_id"]:
                    group["reviewer_company_id"] = reviewer_company_id
                if reviewer_company_name and not group["reviewer_company_name"]:
                    group["reviewer_company_name"] = reviewer_company_name

                discipline_code = reviewer.get("disciplineID")
                if discipline_code:
                    code_text = str(discipline_code).strip().upper()
                    if code_text:
                        group["discipline_codes"].add(code_text)

        reviewer_contact_ids: dict[str, Any] = {}
        for reviewer_contact_key, group in reviewer_groups.items():
            contact_id = await conn.fetchval(
                UPSERT_CONTACT_SQL,
                reviewer_contact_key,
                group["reviewer_email"],
                group["reviewer_name"],
                group["reviewer_company_id"],
                group["reviewer_company_name"],
            )
            reviewer_contact_ids[reviewer_contact_key] = contact_id

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
            coversheet_id = existing_id
        else:
            coversheet_id = await conn.fetchval(
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

        template_row = None
        discipline_ids_by_code: dict[str, Any] = {}
        created_requests = 0
        updated_requests = 0
        disciplines_added = 0
        disciplines_removed = 0

        if reviewer_groups:
            template_row = await conn.fetchrow(
                GET_ACTIVE_TEMPLATE_VERSION_SQL, "qc_subconsultant_review"
            )
            if template_row is None:
                raise HTTPException(
                    status_code=500,
                    detail="Active template version not found for qc_subconsultant_review",
                )

            discipline_codes: list[str] = sorted(
                {
                    code
                    for group in reviewer_groups.values()
                    for code in group["discipline_codes"]
                }
            )
            if discipline_codes:
                rows = await conn.fetch(
                    SELECT_DISCIPLINE_IDS_BY_CODE_SQL, discipline_codes
                )
                discipline_ids_by_code = {
                    row["erp_discipline_code"]: row["id"] for row in rows
                }

            for reviewer_contact_key, group in reviewer_groups.items():
                contact_id = reviewer_contact_ids[reviewer_contact_key]
                request_row = await conn.fetchrow(
                    UPSERT_REVIEW_REQUEST_SQL,
                    coversheet_id,
                    contact_id,
                    group["reviewer_name"],
                    template_row["form_template_id"],
                    template_row["version"],
                )
                if request_row and request_row["inserted"]:
                    created_requests += 1
                else:
                    updated_requests += 1

                review_request_id = request_row["id"]
                missing_codes = [
                    code
                    for code in group["discipline_codes"]
                    if code not in discipline_ids_by_code
                ]
                for code in missing_codes:
                    logger.warning(
                        "reviewer_missing_discipline_code qcUdicID=%s reviewerKey=%s disciplineCode=%s",
                        qc_udic_id,
                        reviewer_contact_key,
                        code,
                    )

                desired_discipline_ids = [
                    discipline_ids_by_code[code]
                    for code in sorted(group["discipline_codes"])
                    if code in discipline_ids_by_code
                ]
                for discipline_id in desired_discipline_ids:
                    inserted = await conn.fetchval(
                        INSERT_REVIEW_REQUEST_DISCIPLINE_SQL,
                        review_request_id,
                        discipline_id,
                    )
                    if inserted:
                        disciplines_added += 1

                removed_rows = await conn.fetch(
                    DELETE_REVIEW_REQUEST_DISCIPLINE_SQL,
                    review_request_id,
                    desired_discipline_ids,
                )
                disciplines_removed += len(removed_rows)

        reviewer_contact_id_list = list(reviewer_contact_ids.values())
        cancelled_rows = await conn.fetch(
            CANCEL_REMOVED_REVIEWERS_SQL,
            coversheet_id,
            ACTIVE_REVIEW_REQUEST_STATUSES,
            reviewer_contact_id_list,
        )
        requests_cancelled = len(cancelled_rows)

        logger.info(
            "review_request_sync qcUdicID=%s coversheet_id=%s reviewers=%s requests_created=%s "
            "requests_updated=%s disciplines_added=%s disciplines_removed=%s requests_cancelled=%s",
            qc_udic_id,
            coversheet_id,
            len(reviewer_groups),
            created_requests,
            updated_requests,
            disciplines_added,
            disciplines_removed,
            requests_cancelled,
        )

        return coversheet_id

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
