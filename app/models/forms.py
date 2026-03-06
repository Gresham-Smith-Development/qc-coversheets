from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BrandingConfig(BaseModel):
    org_name: str
    logo_url: str | None = None


class SectionChoiceConfig(BaseModel):
    type: Literal["single_select"]
    options: list[Literal["complete", "na"]]
    required: bool = True

    @field_validator("options")
    @classmethod
    def validate_options(cls, value: list[Literal["complete", "na"]]) -> list[Literal["complete", "na"]]:
        if set(value) != {"complete", "na"}:
            raise ValueError("choice.options must contain exactly 'complete' and 'na'")
        return value


class SignatureConfig(BaseModel):
    required_when_choice_selected: bool = True
    type_name_must_match_reviewer: bool = True
    match_mode: Literal["case_insensitive_exact"] = "case_insensitive_exact"
    capture_timestamp: bool = True


class NotesConfig(BaseModel):
    type: Literal["text"] = "text"
    required: bool = False
    max_length: int = 4000

    @field_validator("max_length")
    @classmethod
    def validate_max_length(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("notes.max_length must be positive")
        return value


class SectionConfig(BaseModel):
    section_key: str = Field(min_length=1)
    section_label: str = Field(min_length=1)
    choice: SectionChoiceConfig
    signature: SignatureConfig
    notes: NotesConfig


class DisciplineRepeatConfig(BaseModel):
    source: Literal["review_request_discipline"] = "review_request_discipline"
    label_field: str = "discipline_name"
    items: list[SectionConfig]

    @field_validator("items")
    @classmethod
    def validate_items(cls, value: list[SectionConfig]) -> list[SectionConfig]:
        if not value:
            raise ValueError("discipline_repeat.items must include at least one section")
        keys = [item.section_key for item in value]
        if len(keys) != len(set(keys)):
            raise ValueError("discipline_repeat.items contains duplicate section_key values")
        return value


class FormTemplateSchema(BaseModel):
    schema_version: str = "1.0"
    template_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    branding: BrandingConfig
    auto_fields: list[str]
    discipline_repeat: DisciplineRepeatConfig

    @field_validator("auto_fields")
    @classmethod
    def validate_auto_fields(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("auto_fields must include at least one item")
        if len(value) != len(set(value)):
            raise ValueError("auto_fields contains duplicate values")
        return value


class FormTemplateRecord(BaseModel):
    id: UUID
    template_key: str
    display_name: str
    description: str | None = None
    created_at: datetime
    updated_at: datetime


class FormTemplateVersionRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: UUID
    form_template_id: UUID
    version: int
    schema_payload: dict[str, Any] = Field(alias="schema_json")
    is_active: bool
    created_at: datetime


class SaveTemplateVersionRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    template_schema: FormTemplateSchema = Field(alias="schema_json")
    description: str | None = None


class TemplateVersionResponse(BaseModel):
    template: FormTemplateRecord
    version: FormTemplateVersionRecord


class DisciplineResolved(BaseModel):
    discipline_id: UUID
    discipline_name: str


class ReviewFormContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    review_request_id: UUID
    status: str = "draft"
    form_template_id: UUID
    form_version: int
    template_key: str
    reviewer_name: str
    reviewer_email: str | None = None
    auto_values: dict[str, Any]
    disciplines: list[DisciplineResolved]
    template_schema: FormTemplateSchema = Field(alias="schema_json")


class SectionAnswer(BaseModel):
    status: Literal["complete", "na"]
    signature_name: str = Field(min_length=1)
    signed_at: datetime
    notes: str | None = ""


class SectionAnswerDraft(BaseModel):
    status: Literal["complete", "na"] | None = None
    signature_name: str | None = None
    signed_at: datetime | None = None
    notes: str | None = None


class DisciplineAnswer(BaseModel):
    discipline_id: UUID
    discipline_name: str
    sections: dict[str, SectionAnswer]


class DisciplineAnswerDraft(BaseModel):
    discipline_id: UUID
    discipline_name: str
    sections: dict[str, SectionAnswerDraft]


class ReviewFormSubmissionRequest(BaseModel):
    review_request_id: UUID | None = None
    reviewer_name_expected: str | None = None
    discipline_responses: list[DisciplineAnswer]

    @field_validator("discipline_responses")
    @classmethod
    def validate_discipline_responses(cls, value: list[DisciplineAnswer]) -> list[DisciplineAnswer]:
        if not value:
            raise ValueError("discipline_responses must include at least one discipline")
        seen = set()
        for item in value:
            if item.discipline_id in seen:
                raise ValueError("discipline_responses contains duplicate discipline_id values")
            seen.add(item.discipline_id)
        return value


class ReviewFormValidationRequest(BaseModel):
    review_request_id: UUID | None = None
    reviewer_name_expected: str | None = None
    discipline_responses: list[DisciplineAnswerDraft]

    @field_validator("discipline_responses")
    @classmethod
    def validate_discipline_responses(cls, value: list[DisciplineAnswerDraft]) -> list[DisciplineAnswerDraft]:
        if not value:
            raise ValueError("discipline_responses must include at least one discipline")
        seen = set()
        for item in value:
            if item.discipline_id in seen:
                raise ValueError("discipline_responses contains duplicate discipline_id values")
            seen.add(item.discipline_id)
        return value


class ReviewFormValidationResult(BaseModel):
    valid: bool
    errors: list[str] = Field(default_factory=list)


class ReviewFormSubmissionResponse(BaseModel):
    status: Literal["submitted"]
    submission_id: UUID
    review_request_id: UUID


class ImportTemplateRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    template_schema: FormTemplateSchema = Field(alias="schema_json")
    description: str | None = None


class TemplateListItem(BaseModel):
    template_key: str
    display_name: str
    latest_version: int | None = None
    active_version: int | None = None


class TemplateVersionListItem(BaseModel):
    version: int
    is_active: bool
    created_at: datetime


class ActiveReviewRequestItem(BaseModel):
    review_request_id: UUID
    status: str
    due_at: datetime | None = None
    sent_at: datetime | None = None
    completed_at: datetime | None = None
    reviewer_name: str | None = None
    reviewer_email: str | None = None
    reviewer_name_used: str | None = None
    project_number: str | None = None
    project_name: str | None = None
    submittal_name: str | None = None
    submittal_date: str | None = None
    template_key: str
    expected_form_version: int
    active_template_version: int | None = None
    discipline_count: int = 0
    updated_at: datetime


class ReassignTemplateVersionRequest(BaseModel):
    template_key: str = Field(min_length=1)
    version: int = Field(gt=0)


class ReassignTemplateVersionResponse(BaseModel):
    review_request_id: UUID
    old_template_key: str
    old_version: int
    new_template_key: str
    new_version: int
    updated_at: datetime
