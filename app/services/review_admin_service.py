from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from fastapi import HTTPException

from app.models.forms import ActiveReviewRequestItem, ReassignTemplateVersionResponse

if TYPE_CHECKING:
    import asyncpg


LIST_ACTIVE_REVIEW_REQUESTS_SQL = """
SELECT
    rr.id AS review_request_id,
    rr.status,
    rr.due_at,
    rr.sent_at,
    rr.completed_at,
    c.display_name AS reviewer_name,
    c.email AS reviewer_email,
    rr.reviewer_name_used,
    cv.project_wbs AS project_number,
    cv.project_name_snapshot AS project_name,
    cv.submittal_name,
    cv.submittal_date,
    ft.template_key,
    rr.expected_form_version,
    active_v.version AS active_template_version,
    COUNT(rrd.discipline_id) AS discipline_count,
    rr.updated_at
FROM qc_coversheet.review_request rr
JOIN qc_coversheet.contact c
    ON c.id = rr.reviewer_contact_id
JOIN qc_coversheet.qc_coversheet_coversheet cv
    ON cv.id = rr.qc_coversheet_coversheet_id
JOIN qc_coversheet.form_template ft
    ON ft.id = rr.expected_form_template_id
LEFT JOIN LATERAL (
    SELECT ftv.version
    FROM qc_coversheet.form_template_version ftv
    WHERE ftv.form_template_id = rr.expected_form_template_id
      AND ftv.is_active = true
    ORDER BY ftv.created_at DESC, ftv.version DESC
    LIMIT 1
) active_v ON true
LEFT JOIN qc_coversheet.review_request_discipline rrd
    ON rrd.review_request_id = rr.id
WHERE rr.status = ANY($1::text[])
GROUP BY
    rr.id, rr.status, rr.due_at, rr.sent_at, rr.completed_at,
    c.display_name, c.email, rr.reviewer_name_used,
    cv.project_wbs, cv.project_name_snapshot, cv.submittal_name, cv.submittal_date,
    ft.template_key, rr.expected_form_version, active_v.version, rr.updated_at
ORDER BY rr.updated_at DESC;
"""

LIST_ACTIVE_REVIEW_REQUESTS_FOR_INTERNAL_SQL = """
SELECT
    rr.id AS review_request_id,
    rr.status,
    rr.due_at,
    rr.sent_at,
    rr.completed_at,
    c.display_name AS reviewer_name,
    c.email AS reviewer_email,
    rr.reviewer_name_used,
    cv.project_wbs AS project_number,
    cv.project_name_snapshot AS project_name,
    cv.submittal_name,
    cv.submittal_date,
    ft.template_key,
    rr.expected_form_version,
    active_v.version AS active_template_version,
    COUNT(rrd.discipline_id) AS discipline_count,
    rr.updated_at
FROM qc_coversheet.review_request rr
JOIN qc_coversheet.contact c
    ON c.id = rr.reviewer_contact_id
JOIN qc_coversheet.qc_coversheet_coversheet cv
    ON cv.id = rr.qc_coversheet_coversheet_id
JOIN qc_coversheet.form_template ft
    ON ft.id = rr.expected_form_template_id
LEFT JOIN LATERAL (
    SELECT ftv.version
    FROM qc_coversheet.form_template_version ftv
    WHERE ftv.form_template_id = rr.expected_form_template_id
      AND ftv.is_active = true
    ORDER BY ftv.created_at DESC, ftv.version DESC
    LIMIT 1
) active_v ON true
LEFT JOIN qc_coversheet.review_request_discipline rrd
    ON rrd.review_request_id = rr.id
JOIN qc_coversheet.app_user_contact_link ucl
    ON ucl.app_user_id = $1
   AND ucl.is_active = true
JOIN qc_coversheet.contact pc
    ON pc.id = ucl.contact_id
WHERE rr.status = ANY($2::text[])
  AND (
    (cv.pm_email_snapshot IS NOT NULL AND lower(pc.email::text) = lower(cv.pm_email_snapshot::text))
    OR
    (cv.pp_email_snapshot IS NOT NULL AND lower(pc.email::text) = lower(cv.pp_email_snapshot::text))
  )
GROUP BY
    rr.id, rr.status, rr.due_at, rr.sent_at, rr.completed_at,
    c.display_name, c.email, rr.reviewer_name_used,
    cv.project_wbs, cv.project_name_snapshot, cv.submittal_name, cv.submittal_date,
    ft.template_key, rr.expected_form_version, active_v.version, rr.updated_at
ORDER BY rr.updated_at DESC;
"""

GET_TEMPLATE_VERSION_TARGET_SQL = """
SELECT
    ft.id AS form_template_id,
    ft.template_key,
    ftv.version
FROM qc_coversheet.form_template ft
JOIN qc_coversheet.form_template_version ftv
    ON ftv.form_template_id = ft.id
WHERE ft.template_key = $1
  AND ftv.version = $2;
"""

GET_REVIEW_REQUEST_CURRENT_SQL = """
SELECT
    rr.id AS review_request_id,
    ft.template_key AS old_template_key,
    rr.expected_form_version AS old_version
FROM qc_coversheet.review_request rr
JOIN qc_coversheet.form_template ft
    ON ft.id = rr.expected_form_template_id
WHERE rr.id = $1;
"""

UPDATE_REVIEW_REQUEST_TEMPLATE_SQL = """
UPDATE qc_coversheet.review_request
SET expected_form_template_id = $2,
    expected_form_version = $3,
    updated_at = now()
WHERE id = $1
RETURNING id, updated_at;
"""


class ReviewAdminService:
    _active_statuses = ["draft", "queued", "sent", "opened", "in_progress", "overdue"]

    async def list_active_review_requests(
        self, conn: asyncpg.Connection
    ) -> list[ActiveReviewRequestItem]:
        rows = await conn.fetch(LIST_ACTIVE_REVIEW_REQUESTS_SQL, self._active_statuses)
        return [
            ActiveReviewRequestItem(
                review_request_id=row["review_request_id"],
                status=row["status"],
                due_at=row["due_at"],
                sent_at=row["sent_at"],
                completed_at=row["completed_at"],
                reviewer_name=row["reviewer_name"],
                reviewer_email=row["reviewer_email"],
                reviewer_name_used=row["reviewer_name_used"],
                project_number=row["project_number"],
                project_name=row["project_name"],
                submittal_name=row["submittal_name"],
                submittal_date=row["submittal_date"].isoformat() if row["submittal_date"] else None,
                template_key=row["template_key"],
                expected_form_version=row["expected_form_version"],
                active_template_version=row["active_template_version"],
                discipline_count=row["discipline_count"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def list_active_review_requests_for_internal(
        self, conn: asyncpg.Connection, *, app_user_id: UUID
    ) -> list[ActiveReviewRequestItem]:
        rows = await conn.fetch(LIST_ACTIVE_REVIEW_REQUESTS_FOR_INTERNAL_SQL, app_user_id, self._active_statuses)
        return [
            ActiveReviewRequestItem(
                review_request_id=row["review_request_id"],
                status=row["status"],
                due_at=row["due_at"],
                sent_at=row["sent_at"],
                completed_at=row["completed_at"],
                reviewer_name=row["reviewer_name"],
                reviewer_email=row["reviewer_email"],
                reviewer_name_used=row["reviewer_name_used"],
                project_number=row["project_number"],
                project_name=row["project_name"],
                submittal_name=row["submittal_name"],
                submittal_date=row["submittal_date"].isoformat() if row["submittal_date"] else None,
                template_key=row["template_key"],
                expected_form_version=row["expected_form_version"],
                active_template_version=row["active_template_version"],
                discipline_count=row["discipline_count"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    async def reassign_review_request_template(
        self,
        conn: asyncpg.Connection,
        *,
        review_request_id: UUID,
        template_key: str,
        version: int,
    ) -> ReassignTemplateVersionResponse:
        async with conn.transaction():
            current = await conn.fetchrow(GET_REVIEW_REQUEST_CURRENT_SQL, review_request_id)
            if current is None:
                raise HTTPException(status_code=404, detail=f"Review request '{review_request_id}' not found")

            target = await conn.fetchrow(GET_TEMPLATE_VERSION_TARGET_SQL, template_key, version)
            if target is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Template '{template_key}' version '{version}' not found",
                )

            updated = await conn.fetchrow(
                UPDATE_REVIEW_REQUEST_TEMPLATE_SQL,
                review_request_id,
                target["form_template_id"],
                target["version"],
            )
            if updated is None:
                raise HTTPException(status_code=404, detail=f"Review request '{review_request_id}' not found")

        return ReassignTemplateVersionResponse(
            review_request_id=review_request_id,
            old_template_key=current["old_template_key"],
            old_version=current["old_version"],
            new_template_key=target["template_key"],
            new_version=target["version"],
            updated_at=updated["updated_at"],
        )
