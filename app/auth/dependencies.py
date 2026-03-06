from __future__ import annotations

from uuid import UUID

from fastapi import Depends, HTTPException, Request

from app.auth.models import AuthenticatedUser
from app.auth.repository import AuthRepository
from app.auth.session import clear_user_session, get_session_user_id
from app.db import get_pool
from app.settings import Settings, get_settings
from app.state import get_auth_repository


def _forbidden(detail: str = "Forbidden") -> HTTPException:
    return HTTPException(status_code=403, detail=detail)


def _pending_access() -> HTTPException:
    return HTTPException(status_code=403, detail="pending_access")


def _unauthenticated() -> HTTPException:
    return HTTPException(status_code=401, detail="Unauthenticated")


def _bypass_is_allowed(settings: Settings) -> bool:
    env = settings.environment_name.strip().lower()
    if settings.auth_bypass_enabled and env not in {"local", "development"}:
        raise HTTPException(status_code=500, detail="Auth bypass cannot be enabled outside local/development")
    return settings.auth_bypass_enabled and env in {"local", "development"}


def _resolve_bypass_user(settings: Settings) -> AuthenticatedUser:
    return AuthenticatedUser(
        app_user_id=settings.auth_bypass_app_user_id,
        tenant_id=settings.auth_bypass_tenant_id,
        entra_object_id=settings.auth_bypass_object_id,
        email=settings.auth_bypass_email,
        display_name=settings.auth_bypass_display_name,
        is_active=True,
        is_approved=True,
        roles=["admin"],
        permissions=sorted(
            {
                "admin.access",
                "admin.templates.read",
                "admin.templates.write",
                "admin.review_requests.read",
                "admin.review_requests.write",
                "reviewer.access",
                "reviewer.form.read",
                "reviewer.form.validate",
                "reviewer.form.submit",
                "internal.form.read",
                "internal.assignment.read",
            }
        ),
        linked_contact_ids=[],
        linked_contact_emails=[],
    )


async def resolve_optional_user(
    request: Request,
    pool=Depends(get_pool),
    settings: Settings = Depends(get_settings),
    repo: AuthRepository = Depends(get_auth_repository),
) -> AuthenticatedUser | None:
    if _bypass_is_allowed(settings):
        return _resolve_bypass_user(settings)

    app_user_id = get_session_user_id(request)
    if app_user_id is None:
        return None

    async with pool.acquire() as conn:
        user = await repo.get_authenticated_user(conn, app_user_id=app_user_id)
    if user is None:
        clear_user_session(request)
    return user


async def require_authenticated_user(
    user: AuthenticatedUser | None = Depends(resolve_optional_user),
) -> AuthenticatedUser:
    if user is None:
        raise _unauthenticated()
    return user


async def require_active_user(
    user: AuthenticatedUser = Depends(require_authenticated_user),
) -> AuthenticatedUser:
    if user.is_pending_access:
        raise _pending_access()
    return user


def _require_permission(user: AuthenticatedUser, permission: str) -> None:
    if user.is_admin:
        return
    if not user.has_permission(permission):
        raise _forbidden()


async def require_admin_templates_read(
    user: AuthenticatedUser = Depends(require_active_user),
) -> AuthenticatedUser:
    _require_permission(user, "admin.access")
    _require_permission(user, "admin.templates.read")
    return user


async def require_admin_access(
    user: AuthenticatedUser = Depends(require_active_user),
) -> AuthenticatedUser:
    _require_permission(user, "admin.access")
    return user


async def require_admin_templates_write(
    user: AuthenticatedUser = Depends(require_active_user),
) -> AuthenticatedUser:
    _require_permission(user, "admin.access")
    _require_permission(user, "admin.templates.write")
    return user


async def require_admin_review_requests_read(
    user: AuthenticatedUser = Depends(require_active_user),
) -> AuthenticatedUser:
    _require_permission(user, "admin.access")
    _require_permission(user, "admin.review_requests.read")
    return user


async def require_admin_review_requests_write(
    user: AuthenticatedUser = Depends(require_active_user),
) -> AuthenticatedUser:
    _require_permission(user, "admin.access")
    _require_permission(user, "admin.review_requests.write")
    return user


async def require_active_review_requests_read(
    user: AuthenticatedUser = Depends(require_active_user),
) -> AuthenticatedUser:
    if user.is_admin:
        return user
    if user.has_permission("internal.form.read") and user.has_permission("internal.assignment.read"):
        return user
    raise _forbidden()


async def require_review_form_view_access(
    review_request_id: UUID,
    user: AuthenticatedUser = Depends(require_active_user),
    pool=Depends(get_pool),
    repo: AuthRepository = Depends(get_auth_repository),
) -> AuthenticatedUser:
    if user.is_admin:
        return user

    async with pool.acquire() as conn:
        if user.has_permission("reviewer.form.read"):
            if await repo.can_reviewer_access_request(
                conn, app_user_id=user.app_user_id, review_request_id=review_request_id
            ):
                return user
        if user.has_permission("internal.form.read") and user.has_permission("internal.assignment.read"):
            if await repo.can_internal_view_request(
                conn, app_user_id=user.app_user_id, review_request_id=review_request_id
            ):
                return user
    raise _forbidden()


async def require_review_form_submit_access(
    review_request_id: UUID,
    user: AuthenticatedUser = Depends(require_active_user),
    pool=Depends(get_pool),
    repo: AuthRepository = Depends(get_auth_repository),
) -> AuthenticatedUser:
    if user.is_admin:
        return user
    if not user.has_permission("reviewer.form.validate") or not user.has_permission("reviewer.form.submit"):
        raise _forbidden()

    async with pool.acquire() as conn:
        if not await repo.can_reviewer_access_request(
            conn, app_user_id=user.app_user_id, review_request_id=review_request_id
        ):
            raise _forbidden()
    return user
