from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID

from fastapi import HTTPException

from app.models.forms import (
    DisciplineResolved,
    FormTemplateSchema,
    ReviewFormContext,
    ReviewFormSubmissionRequest,
    ReviewFormSubmissionResponse,
    ReviewFormValidationRequest,
    ReviewFormValidationResult,
)

if TYPE_CHECKING:
    import asyncpg


GET_REVIEW_FORM_CONTEXT_SQL = """
SELECT
    rr.id AS review_request_id,
    rr.status,
    rr.expected_form_template_id AS form_template_id,
    rr.expected_form_version AS form_version,
    ft.template_key,
    c.display_name AS reviewer_name,
    c.email AS reviewer_email,
    cv.project_name_snapshot,
    cv.project_wbs,
    cv.client_name_snapshot,
    cv.submittal_name,
    cv.submittal_date
FROM qc_coversheet.review_request rr
JOIN qc_coversheet.form_template ft
    ON ft.id = rr.expected_form_template_id
JOIN qc_coversheet.contact c
    ON c.id = rr.reviewer_contact_id
JOIN qc_coversheet.qc_coversheet_coversheet cv
    ON cv.id = rr.qc_coversheet_coversheet_id
WHERE rr.id = $1;
"""

GET_REVIEW_FORM_SCHEMA_SQL = """
SELECT schema_json
FROM qc_coversheet.form_template_version
WHERE form_template_id = $1 AND version = $2;
"""

GET_REVIEW_FORM_DISCIPLINES_SQL = """
SELECT d.id, d.discipline_name
FROM qc_coversheet.review_request_discipline rrd
JOIN qc_coversheet.discipline d
    ON d.id = rrd.discipline_id
WHERE rrd.review_request_id = $1
ORDER BY d.discipline_name;
"""

INSERT_REVIEW_SUBMISSION_SQL = """
INSERT INTO qc_coversheet.review_submission (
    review_request_id, form_template_id, form_version, submitted_at, answers,
    overall_result, risk_level, created_at
)
VALUES ($1, $2, $3, now(), $4::jsonb, NULL, NULL, now())
RETURNING id;
"""

UPDATE_REVIEW_REQUEST_SUBMITTED_SQL = """
UPDATE qc_coversheet.review_request
SET status = 'submitted',
    completed_at = now(),
    updated_at = now()
WHERE id = $1;
"""


class ReviewFormService:
    def __init__(
        self,
        *,
        test_mode: bool = False,
        test_reviewer_name: str = "J Smiley Baltz",
        test_reviewer_email: str = "jsbaltz@oulook.com",
    ) -> None:
        self._test_mode = test_mode
        self._test_reviewer_name = test_reviewer_name
        self._test_reviewer_email = test_reviewer_email

    def _resolve_reviewer_identity(self, db_name: str | None, db_email: str | None) -> tuple[str, str | None]:
        if self._test_mode:
            return self._test_reviewer_name, self._test_reviewer_email
        reviewer_name = (db_name or "").strip()
        if not reviewer_name:
            raise HTTPException(
                status_code=422,
                detail="Reviewer display name is missing for this review request",
            )
        return reviewer_name, db_email

    async def resolve_review_form(
        self, conn: asyncpg.Connection, review_request_id: UUID
    ) -> ReviewFormContext:
        row = await conn.fetchrow(GET_REVIEW_FORM_CONTEXT_SQL, review_request_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Review request '{review_request_id}' not found")

        schema_row = await conn.fetchrow(
            GET_REVIEW_FORM_SCHEMA_SQL, row["form_template_id"], row["form_version"]
        )
        if schema_row is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Template version not found for form_template_id='{row['form_template_id']}' "
                    f"version='{row['form_version']}'"
                ),
            )
        raw_schema = schema_row["schema_json"]
        if isinstance(raw_schema, str):
            try:
                raw_schema = json.loads(raw_schema)
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=500,
                    detail="Stored form_template_version.schema_json is invalid JSON text",
                ) from exc
        schema = FormTemplateSchema.model_validate(raw_schema)

        discipline_rows = await conn.fetch(GET_REVIEW_FORM_DISCIPLINES_SQL, review_request_id)
        disciplines = [
            DisciplineResolved(discipline_id=item["id"], discipline_name=item["discipline_name"])
            for item in discipline_rows
        ]
        if not disciplines:
            raise HTTPException(
                status_code=422,
                detail=f"Review request '{review_request_id}' has no disciplines assigned",
            )

        reviewer_name, reviewer_email = self._resolve_reviewer_identity(
            row["reviewer_name"], row["reviewer_email"]
        )

        auto_values = {
            "project_name": row["project_name_snapshot"],
            "project_number": row["project_wbs"],
            "owner_end_user": row["client_name_snapshot"],
            "submittal_name": row["submittal_name"],
            "submittal_date": row["submittal_date"].isoformat() if row["submittal_date"] else None,
            "reviewer_name": reviewer_name,
        }
        return ReviewFormContext(
            review_request_id=row["review_request_id"],
            status=row["status"],
            form_template_id=row["form_template_id"],
            form_version=row["form_version"],
            template_key=row["template_key"],
            reviewer_name=reviewer_name,
            reviewer_email=reviewer_email,
            auto_values=auto_values,
            disciplines=disciplines,
            template_schema=schema,
        )

    def validate_submission_payload(
        self,
        *,
        context: ReviewFormContext,
        submission: ReviewFormSubmissionRequest | ReviewFormValidationRequest,
    ) -> ReviewFormValidationResult:
        errors: list[str] = []
        expected_discipline_ids = {item.discipline_id for item in context.disciplines}
        submitted_ids = {item.discipline_id for item in submission.discipline_responses}

        if expected_discipline_ids != submitted_ids:
            missing = expected_discipline_ids - submitted_ids
            extra = submitted_ids - expected_discipline_ids
            if missing:
                errors.append(f"Missing discipline responses: {', '.join(str(item) for item in sorted(missing))}")
            if extra:
                errors.append(f"Unexpected discipline responses: {', '.join(str(item) for item in sorted(extra))}")

        section_configs = {item.section_key: item for item in context.template_schema.discipline_repeat.items}
        expected_section_keys = set(section_configs.keys())

        def format_section_list(keys: set[str]) -> str:
            labels = []
            for key in sorted(keys):
                config = section_configs.get(key)
                labels.append(config.section_label if config else key)
            return ", ".join(labels)

        for response in submission.discipline_responses:
            section_keys = set(response.sections.keys())
            if section_keys != expected_section_keys:
                missing_sections = expected_section_keys - section_keys
                extra_sections = section_keys - expected_section_keys
                if missing_sections:
                    errors.append(
                        f"Discipline '{response.discipline_name}' missing sections: "
                        f"{format_section_list(missing_sections)}"
                    )
                if extra_sections:
                    errors.append(
                        f"Discipline '{response.discipline_name}' has unexpected sections: "
                        f"{format_section_list(extra_sections)}"
                    )
                continue

            for section_key, section_answer in response.sections.items():
                config = section_configs[section_key]
                section_label = config.section_label
                status = section_answer.status
                if config.choice.required and not status:
                    errors.append(
                        f"Section '{section_label}' for discipline '{response.discipline_name}' "
                        "is missing required status"
                    )
                    continue

                if status and config.signature.required_when_choice_selected:
                    signature_name = (section_answer.signature_name or "").strip()
                    if not signature_name:
                        errors.append(
                            f"Section '{section_label}' for discipline '{response.discipline_name}' "
                            "is missing required signature name"
                        )
                    elif config.signature.type_name_must_match_reviewer:
                        expected_name = context.reviewer_name.strip().lower()
                        actual_name = signature_name.lower()
                        if expected_name != actual_name:
                            errors.append(
                                f"Section '{section_label}' for discipline '{response.discipline_name}' "
                                "has a signature name that does not match reviewer name"
                            )
                    if config.signature.capture_timestamp and not section_answer.signed_at:
                        errors.append(
                            f"Section '{section_label}' for discipline '{response.discipline_name}' "
                            "is missing required signature timestamp"
                        )

                notes = section_answer.notes or ""
                if config.notes.required and not notes.strip():
                    errors.append(
                        f"Section '{section_label}' for discipline '{response.discipline_name}' "
                        "is missing required notes"
                    )
                if notes and len(notes) > config.notes.max_length:
                    errors.append(
                        f"Section '{section_label}' for discipline '{response.discipline_name}' "
                        f"notes exceeds {config.notes.max_length} characters"
                    )

        return ReviewFormValidationResult(valid=len(errors) == 0, errors=errors)

    async def submit_review_form(
        self,
        conn: asyncpg.Connection,
        *,
        context: ReviewFormContext,
        submission: ReviewFormSubmissionRequest,
    ) -> ReviewFormSubmissionResponse:
        answers = {
            "review_request_id": str(context.review_request_id),
            "reviewer_name_expected": context.reviewer_name,
            "discipline_responses": [item.model_dump(mode="json") for item in submission.discipline_responses],
        }
        async with conn.transaction():
            submission_id = await conn.fetchval(
                INSERT_REVIEW_SUBMISSION_SQL,
                context.review_request_id,
                context.form_template_id,
                context.form_version,
                json.dumps(answers),
            )
            await conn.execute(
                UPDATE_REVIEW_REQUEST_SUBMITTED_SQL,
                context.review_request_id,
            )
        return ReviewFormSubmissionResponse(
            status="submitted",
            submission_id=submission_id,
            review_request_id=context.review_request_id,
        )

    @staticmethod
    def stamp_signature_now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
