from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse

from app.auth.dependencies import require_authenticated_user, resolve_optional_user
from app.auth.models import AuthenticatedUser, MeResponse
from app.auth.oidc import EntraOidcClient
from app.auth.repository import AuthRepository
from app.auth.session import (
    clear_user_session,
    ensure_session_id,
    new_auth_state_nonce,
    pop_auth_flow,
    store_auth_flow,
    store_user_session,
)
from app.db import get_pool
from app.settings import Settings, get_settings
from app.state import get_auth_repository, get_entra_oidc_client

router = APIRouter(tags=["auth"])

KNOWN_LOCAL_ROLES = {"admin", "reviewer", "internal_readonly", "user"}


def _map_entra_roles(claim_roles: list[str] | None) -> list[str]:
    if not claim_roles:
        return []
    mapped: set[str] = set()
    for raw in claim_roles:
        token = str(raw or "").strip().lower()
        if not token:
            continue
        token = token.replace("-", "_").replace(" ", "_")
        tail = token.split(".")[-1].split("/")[-1]
        aliases = {token, tail}
        if "internalreadonly" in aliases:
            aliases.add("internal_readonly")
        for candidate in aliases:
            if candidate in KNOWN_LOCAL_ROLES:
                mapped.add(candidate)
    return sorted(mapped)


@router.get("/auth/login")
async def auth_login(
    request: Request,
    settings: Settings = Depends(get_settings),
    oidc: EntraOidcClient = Depends(get_entra_oidc_client),
):
    if not settings.auth_entra_tenant_id or not settings.auth_entra_client_id:
        raise HTTPException(status_code=500, detail="Auth settings are incomplete")

    state, nonce = new_auth_state_nonce()
    store_auth_flow(request, state=state, nonce=nonce)
    ensure_session_id(request)
    return RedirectResponse(url=oidc.build_login_url(state=state, nonce=nonce), status_code=302)


@router.get("/auth/callback")
async def auth_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    settings: Settings = Depends(get_settings),
    oidc: EntraOidcClient = Depends(get_entra_oidc_client),
    pool=Depends(get_pool),
    repo: AuthRepository = Depends(get_auth_repository),
):
    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state")

    expected_state, expected_nonce = pop_auth_flow(request)
    if not expected_state or state != expected_state:
        raise HTTPException(status_code=401, detail="Invalid auth state")

    id_token = await oidc.exchange_code_for_id_token(code)
    claims = oidc.validate_id_token_claims(id_token, expected_nonce=expected_nonce)

    async with pool.acquire() as conn:
        app_user_id = await repo.upsert_app_user(
            conn,
            tenant_id=claims.tid,
            entra_object_id=claims.oid,
            email=claims.email or claims.preferred_username,
            display_name=claims.name,
            given_name=claims.given_name,
            family_name=claims.family_name,
            preferred_username=claims.preferred_username,
        )
        synced_roles: list[str] = []
        if settings.auth_sync_entra_app_roles:
            mapped_roles = _map_entra_roles(claims.roles)
            synced_roles = await repo.sync_entra_roles(
                conn, app_user_id=app_user_id, role_names=mapped_roles
            )
        await repo.log_session_event(
            conn,
            app_user_id=app_user_id,
            session_id=ensure_session_id(request),
            event_type="login_success",
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            details={"tenant_id": claims.tid, "oid": claims.oid, "synced_roles": synced_roles},
        )

    store_user_session(request, app_user_id=app_user_id)
    return RedirectResponse(url=settings.auth_login_success_redirect, status_code=302)


@router.post("/auth/logout")
async def auth_logout(
    request: Request,
    user: AuthenticatedUser | None = Depends(resolve_optional_user),
    pool=Depends(get_pool),
    repo: AuthRepository = Depends(get_auth_repository),
):
    if user is not None:
        async with pool.acquire() as conn:
            await repo.log_session_event(
                conn,
                app_user_id=user.app_user_id,
                session_id=ensure_session_id(request),
                event_type="logout",
                ip_address=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent"),
                details={},
            )
    clear_user_session(request)
    return {"status": "logged_out"}


@router.get("/me", response_model=MeResponse)
async def me(
    user: AuthenticatedUser | None = Depends(resolve_optional_user),
) -> MeResponse:
    if user is None:
        return MeResponse(
            auth_status="unauthenticated",
            effective_access_state="unauthenticated",
        )
    return MeResponse(
        auth_status="authenticated",
        effective_access_state="pending_access" if user.is_pending_access else "active",
        app_user_id=user.app_user_id,
        tenant_id=user.tenant_id,
        entra_object_id=user.entra_object_id,
        email=user.email,
        display_name=user.display_name,
        roles=user.roles,
        permissions=user.permissions,
        linked_contact_ids=user.linked_contact_ids,
    )


@router.post("/auth/bootstrap-admin")
async def bootstrap_admin(
    user: AuthenticatedUser = Depends(require_authenticated_user),
    settings: Settings = Depends(get_settings),
    pool=Depends(get_pool),
    repo: AuthRepository = Depends(get_auth_repository),
):
    allowlist = settings.admin_bootstrap_allowlist()
    if not allowlist:
        raise HTTPException(status_code=403, detail="bootstrap_disabled")
    if user.entra_object_id.lower() not in allowlist:
        raise HTTPException(status_code=403, detail="bootstrap_not_allowed")

    async with pool.acquire() as conn:
        await repo.ensure_user_role(conn, app_user_id=user.app_user_id, role_name="admin")

    return {"status": "ok", "role": "admin", "app_user_id": str(user.app_user_id)}
