from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from app.auth.dependencies import require_admin_templates_read, require_admin_templates_write
from app.db import get_pool
from app.models.forms import (
    ImportTemplateRequest,
    SaveTemplateVersionRequest,
    TemplateListItem,
    TemplateVersionListItem,
    TemplateVersionResponse,
)
from app.services.form_template_service import FormTemplateService
from app.state import get_form_template_service

router = APIRouter(prefix="/admin/form-templates", tags=["form-templates"])


@router.get("", response_model=list[TemplateListItem], dependencies=[Depends(require_admin_templates_read)])
async def list_form_templates(
    pool=Depends(get_pool),
    service: FormTemplateService = Depends(get_form_template_service),
) -> list[TemplateListItem]:
    async with pool.acquire() as conn:
        return await service.list_templates(conn)


@router.get(
    "/{template_key}/versions",
    response_model=list[TemplateVersionListItem],
    dependencies=[Depends(require_admin_templates_read)],
)
async def list_template_versions(
    template_key: str,
    pool=Depends(get_pool),
    service: FormTemplateService = Depends(get_form_template_service),
) -> list[TemplateVersionListItem]:
    async with pool.acquire() as conn:
        return await service.list_template_versions(conn, template_key)


@router.get(
    "/{template_key}/versions/{version}",
    response_model=TemplateVersionResponse,
    dependencies=[Depends(require_admin_templates_read)],
)
async def get_template_version(
    template_key: str,
    version: int,
    pool=Depends(get_pool),
    service: FormTemplateService = Depends(get_form_template_service),
) -> TemplateVersionResponse:
    async with pool.acquire() as conn:
        return await service.get_template_version(conn, template_key, version)


@router.post(
    "/{template_key}/versions",
    response_model=TemplateVersionResponse,
    dependencies=[Depends(require_admin_templates_write)],
)
async def create_template_version(
    template_key: str,
    payload: SaveTemplateVersionRequest,
    pool=Depends(get_pool),
    service: FormTemplateService = Depends(get_form_template_service),
) -> TemplateVersionResponse:
    async with pool.acquire() as conn:
        return await service.create_new_version(
            conn,
            template_key=template_key,
            schema=payload.template_schema,
            description=payload.description,
        )


@router.post("/import", response_model=TemplateVersionResponse, dependencies=[Depends(require_admin_templates_write)])
async def import_template(
    payload: ImportTemplateRequest,
    pool=Depends(get_pool),
    service: FormTemplateService = Depends(get_form_template_service),
) -> TemplateVersionResponse:
    async with pool.acquire() as conn:
        return await service.create_new_version(
            conn,
            template_key=payload.template_schema.template_key,
            schema=payload.template_schema,
            description=payload.description,
        )


@router.get(
    "/{template_key}/versions/{version}/export",
    response_model=dict[str, Any],
    dependencies=[Depends(require_admin_templates_read)],
)
async def export_template(
    template_key: str,
    version: int,
    pool=Depends(get_pool),
    service: FormTemplateService = Depends(get_form_template_service),
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        response = await service.get_template_version(conn, template_key, version)
    return {
        "template_key": response.template.template_key,
        "display_name": response.template.display_name,
        "version": response.version.version,
        "is_active": response.version.is_active,
        "schema_json": response.version.schema_payload,
    }
