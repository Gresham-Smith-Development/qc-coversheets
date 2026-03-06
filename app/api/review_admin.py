from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from app.auth.dependencies import (
    require_active_review_requests_read,
    require_admin_review_requests_write,
)
from app.auth.models import AuthenticatedUser
from app.db import get_pool
from app.models.forms import (
    ActiveReviewRequestItem,
    ReassignTemplateVersionRequest,
    ReassignTemplateVersionResponse,
)
from app.services.review_admin_service import ReviewAdminService
from app.state import get_review_admin_service

router = APIRouter(prefix="/admin/review-requests", tags=["review-admin"])


@router.get(
    "/active",
    response_model=list[ActiveReviewRequestItem],
)
async def list_active_review_requests(
    user: AuthenticatedUser = Depends(require_active_review_requests_read),
    pool=Depends(get_pool),
    service: ReviewAdminService = Depends(get_review_admin_service),
) -> list[ActiveReviewRequestItem]:
    async with pool.acquire() as conn:
        if user.is_admin:
            return await service.list_active_review_requests(conn)
        return await service.list_active_review_requests_for_internal(
            conn, app_user_id=user.app_user_id
        )


@router.post(
    "/{review_request_id}/reassign-template",
    response_model=ReassignTemplateVersionResponse,
    dependencies=[Depends(require_admin_review_requests_write)],
)
async def reassign_template_version(
    review_request_id: UUID,
    payload: ReassignTemplateVersionRequest,
    pool=Depends(get_pool),
    service: ReviewAdminService = Depends(get_review_admin_service),
) -> ReassignTemplateVersionResponse:
    async with pool.acquire() as conn:
        return await service.reassign_review_request_template(
            conn,
            review_request_id=review_request_id,
            template_key=payload.template_key,
            version=payload.version,
        )
