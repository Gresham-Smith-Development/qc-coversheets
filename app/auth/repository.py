from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from uuid import UUID

from app.auth.models import AuthenticatedUser

if TYPE_CHECKING:
    import asyncpg

UPSERT_APP_USER_SQL = """
INSERT INTO qc_coversheet.app_user (
    tenant_id, entra_object_id, email, display_name, given_name, family_name, preferred_username,
    is_active, last_login_at, updated_at
)
VALUES (
    $1, $2, $3::citext, $4, $5, $6, $7, true, now(), now()
)
ON CONFLICT (tenant_id, entra_object_id)
DO UPDATE SET
    email = EXCLUDED.email,
    display_name = EXCLUDED.display_name,
    given_name = EXCLUDED.given_name,
    family_name = EXCLUDED.family_name,
    preferred_username = EXCLUDED.preferred_username,
    is_active = true,
    last_login_at = now(),
    updated_at = now()
RETURNING id;
"""

GET_USER_BASE_SQL = """
SELECT id, tenant_id, entra_object_id, email::text AS email, display_name, is_active, is_approved
FROM qc_coversheet.app_user
WHERE id = $1;
"""

GET_USER_ROLES_SQL = """
SELECT r.role_name
FROM qc_coversheet.app_user_role ur
JOIN qc_coversheet.app_role r ON r.id = ur.role_id
WHERE ur.app_user_id = $1;
"""

GET_USER_PERMISSIONS_SQL = """
SELECT DISTINCT p.permission_key
FROM qc_coversheet.app_user_role ur
JOIN qc_coversheet.app_role_permission rp ON rp.role_id = ur.role_id
JOIN qc_coversheet.app_permission p ON p.id = rp.permission_id
WHERE ur.app_user_id = $1;
"""

GET_USER_CONTACT_LINKS_SQL = """
SELECT l.contact_id, c.email::text AS email
FROM qc_coversheet.app_user_contact_link l
JOIN qc_coversheet.contact c ON c.id = l.contact_id
WHERE l.app_user_id = $1
  AND l.is_active = true;
"""

LOG_SESSION_AUDIT_SQL = """
INSERT INTO qc_coversheet.app_session_audit (
    app_user_id, session_id, event_type, ip_address, user_agent, details, created_at
)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, now());
"""

ENSURE_ROLE_SQL = """
INSERT INTO qc_coversheet.app_user_role (app_user_id, role_id, created_at)
SELECT $1, r.id, now()
FROM qc_coversheet.app_role r
WHERE r.role_name = $2
ON CONFLICT (app_user_id, role_id) DO NOTHING;
"""

APPROVE_USER_SQL = """
UPDATE qc_coversheet.app_user
SET is_approved = true, is_active = true, updated_at = now()
WHERE id = $1;
"""

CAN_REVIEWER_ACCESS_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM qc_coversheet.review_request rr
    JOIN qc_coversheet.app_user_contact_link l
      ON l.contact_id = rr.reviewer_contact_id
     AND l.app_user_id = $1
     AND l.is_active = true
    WHERE rr.id = $2
) AS allowed;
"""

CAN_INTERNAL_VIEW_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM qc_coversheet.review_request rr
    JOIN qc_coversheet.qc_coversheet_coversheet cv
      ON cv.id = rr.qc_coversheet_coversheet_id
    JOIN qc_coversheet.app_user_contact_link l
      ON l.app_user_id = $1
     AND l.is_active = true
    JOIN qc_coversheet.contact c
      ON c.id = l.contact_id
    WHERE rr.id = $2
      AND (
        (cv.pm_email_snapshot IS NOT NULL AND lower(c.email::text) = lower(cv.pm_email_snapshot::text))
        OR
        (cv.pp_email_snapshot IS NOT NULL AND lower(c.email::text) = lower(cv.pp_email_snapshot::text))
      )
) AS allowed;
"""

LIST_ROLES_SQL = """
SELECT role_name, description
FROM qc_coversheet.app_role
ORDER BY role_name;
"""

LIST_APP_USERS_SQL = """
SELECT
    u.id AS app_user_id,
    u.tenant_id,
    u.entra_object_id,
    u.email::text AS email,
    u.display_name,
    u.is_active,
    u.is_approved,
    COALESCE(
        ARRAY(
            SELECT DISTINCT r.role_name
            FROM qc_coversheet.app_user_role ur
            JOIN qc_coversheet.app_role r ON r.id = ur.role_id
            WHERE ur.app_user_id = u.id
            ORDER BY r.role_name
        ),
        ARRAY[]::text[]
    ) AS roles,
    COALESCE(
        ARRAY(
            SELECT DISTINCT p.permission_key
            FROM qc_coversheet.app_user_role ur
            JOIN qc_coversheet.app_role_permission rp ON rp.role_id = ur.role_id
            JOIN qc_coversheet.app_permission p ON p.id = rp.permission_id
            WHERE ur.app_user_id = u.id
            ORDER BY p.permission_key
        ),
        ARRAY[]::text[]
    ) AS permissions,
    COALESCE(
        ARRAY(
            SELECT l.contact_id
            FROM qc_coversheet.app_user_contact_link l
            WHERE l.app_user_id = u.id
              AND l.is_active = true
            ORDER BY l.contact_id
        ),
        ARRAY[]::uuid[]
    ) AS linked_contact_ids
FROM qc_coversheet.app_user u
ORDER BY COALESCE(u.display_name, u.email::text, u.entra_object_id), u.created_at DESC;
"""

GET_ROLE_IDS_BY_NAME_SQL = """
SELECT id, role_name
FROM qc_coversheet.app_role
WHERE role_name = ANY($1::text[]);
"""

DELETE_USER_ROLES_SQL = """
DELETE FROM qc_coversheet.app_user_role
WHERE app_user_id = $1;
"""

INSERT_USER_ROLE_SQL = """
INSERT INTO qc_coversheet.app_user_role (app_user_id, role_id, created_at)
VALUES ($1, $2, now())
ON CONFLICT (app_user_id, role_id) DO NOTHING;
"""

UPDATE_USER_APPROVAL_SQL = """
UPDATE qc_coversheet.app_user
SET is_active = COALESCE($2, is_active),
    is_approved = COALESCE($3, is_approved),
    updated_at = now()
WHERE id = $1;
"""


class AuthRepository:
    async def upsert_app_user(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: str,
        entra_object_id: str,
        email: str | None,
        display_name: str | None,
        given_name: str | None,
        family_name: str | None,
        preferred_username: str | None,
    ) -> UUID:
        async with conn.transaction():
            app_user_id = await conn.fetchval(
                UPSERT_APP_USER_SQL,
                tenant_id,
                entra_object_id,
                email,
                display_name,
                given_name,
                family_name,
                preferred_username,
            )
            await conn.execute(ENSURE_ROLE_SQL, app_user_id, "user")
        return app_user_id

    async def get_authenticated_user(
        self, conn: asyncpg.Connection, *, app_user_id: UUID
    ) -> AuthenticatedUser | None:
        row = await conn.fetchrow(GET_USER_BASE_SQL, app_user_id)
        if row is None:
            return None

        role_rows = await conn.fetch(GET_USER_ROLES_SQL, app_user_id)
        roles = sorted({str(item["role_name"]) for item in role_rows if item["role_name"]})

        permission_rows = await conn.fetch(GET_USER_PERMISSIONS_SQL, app_user_id)
        permissions = sorted(
            {str(item["permission_key"]) for item in permission_rows if item["permission_key"]}
        )

        link_rows = await conn.fetch(GET_USER_CONTACT_LINKS_SQL, app_user_id)
        linked_contact_ids = [item["contact_id"] for item in link_rows]
        linked_contact_emails = [
            str(item["email"]).lower() for item in link_rows if item["email"]
        ]

        user = AuthenticatedUser(
            app_user_id=row["id"],
            tenant_id=row["tenant_id"],
            entra_object_id=row["entra_object_id"],
            email=row["email"],
            display_name=row["display_name"],
            is_active=bool(row["is_active"]),
            is_approved=bool(row["is_approved"]),
            roles=roles,
            permissions=permissions,
            linked_contact_ids=linked_contact_ids,
            linked_contact_emails=linked_contact_emails,
        )
        if not user.is_approved or not user.is_active:
            user.permissions = []
        return user

    async def log_session_event(
        self,
        conn: asyncpg.Connection,
        *,
        app_user_id: UUID,
        session_id: str,
        event_type: str,
        ip_address: str | None,
        user_agent: str | None,
        details: dict[str, Any] | None = None,
    ) -> None:
        await conn.execute(
            LOG_SESSION_AUDIT_SQL,
            app_user_id,
            session_id,
            event_type,
            ip_address,
            user_agent,
            json.dumps(details or {}),
        )

    async def ensure_user_role(
        self, conn: asyncpg.Connection, *, app_user_id: UUID, role_name: str
    ) -> None:
        async with conn.transaction():
            await conn.execute(APPROVE_USER_SQL, app_user_id)
            await conn.execute(ENSURE_ROLE_SQL, app_user_id, role_name)

    async def sync_entra_roles(
        self, conn: asyncpg.Connection, *, app_user_id: UUID, role_names: list[str]
    ) -> list[str]:
        if not role_names:
            return []
        async with conn.transaction():
            role_rows = await conn.fetch(GET_ROLE_IDS_BY_NAME_SQL, role_names)
            found_names = sorted({str(r["role_name"]) for r in role_rows})
            for role in found_names:
                await conn.execute(ENSURE_ROLE_SQL, app_user_id, role)
            if any(role != "user" for role in found_names):
                await conn.execute(APPROVE_USER_SQL, app_user_id)
        return found_names

    async def can_reviewer_access_request(
        self, conn: asyncpg.Connection, *, app_user_id: UUID, review_request_id: UUID
    ) -> bool:
        return bool(await conn.fetchval(CAN_REVIEWER_ACCESS_SQL, app_user_id, review_request_id))

    async def can_internal_view_request(
        self, conn: asyncpg.Connection, *, app_user_id: UUID, review_request_id: UUID
    ) -> bool:
        return bool(await conn.fetchval(CAN_INTERNAL_VIEW_SQL, app_user_id, review_request_id))

    async def list_roles(self, conn: asyncpg.Connection) -> list[dict[str, Any]]:
        rows = await conn.fetch(LIST_ROLES_SQL)
        return [{"role_name": r["role_name"], "description": r["description"]} for r in rows]

    async def list_app_users(self, conn: asyncpg.Connection) -> list[dict[str, Any]]:
        rows = await conn.fetch(LIST_APP_USERS_SQL)
        return [
            {
                "app_user_id": r["app_user_id"],
                "tenant_id": r["tenant_id"],
                "entra_object_id": r["entra_object_id"],
                "email": r["email"],
                "display_name": r["display_name"],
                "is_active": bool(r["is_active"]),
                "is_approved": bool(r["is_approved"]),
                "roles": list(r["roles"] or []),
                "permissions": list(r["permissions"] or []),
                "linked_contact_ids": list(r["linked_contact_ids"] or []),
            }
            for r in rows
        ]

    async def set_user_roles(
        self,
        conn: asyncpg.Connection,
        *,
        app_user_id: UUID,
        role_names: list[str],
        is_active: bool | None,
        is_approved: bool | None,
    ) -> None:
        async with conn.transaction():
            await conn.execute(UPDATE_USER_APPROVAL_SQL, app_user_id, is_active, is_approved)
            await conn.execute(DELETE_USER_ROLES_SQL, app_user_id)
            if role_names:
                role_rows = await conn.fetch(GET_ROLE_IDS_BY_NAME_SQL, role_names)
                found_names = {str(r["role_name"]) for r in role_rows}
                missing = [name for name in role_names if name not in found_names]
                if missing:
                    raise ValueError(f"Unknown roles: {', '.join(missing)}")
                for role in role_rows:
                    await conn.execute(INSERT_USER_ROLE_SQL, app_user_id, role["id"])
