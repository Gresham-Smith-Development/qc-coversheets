from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field
from pydantic import field_validator


class EntraTokenClaims(BaseModel):
    tid: str
    oid: str
    aud: str
    iss: str
    nonce: str | None = None
    exp: int
    nbf: int | None = None
    iat: int | None = None
    email: str | None = None
    preferred_username: str | None = None
    name: str | None = None
    given_name: str | None = None
    family_name: str | None = None
    roles: list[str] = Field(default_factory=list)


class AuthenticatedUser(BaseModel):
    app_user_id: UUID
    tenant_id: str
    entra_object_id: str
    email: str | None = None
    display_name: str | None = None
    is_active: bool = True
    is_approved: bool = False
    roles: list[str] = Field(default_factory=list)
    permissions: list[str] = Field(default_factory=list)
    linked_contact_ids: list[UUID] = Field(default_factory=list)
    linked_contact_emails: list[str] = Field(default_factory=list)

    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions

    @property
    def is_admin(self) -> bool:
        return self.has_permission("admin.access")

    @property
    def is_pending_access(self) -> bool:
        return (not self.is_active) or (not self.is_approved) or (len(self.permissions) == 0)


class MeResponse(BaseModel):
    auth_status: Literal["unauthenticated", "authenticated"]
    effective_access_state: Literal["unauthenticated", "pending_access", "active"]
    app_user_id: UUID | None = None
    tenant_id: str | None = None
    entra_object_id: str | None = None
    email: str | None = None
    display_name: str | None = None
    roles: list[str] = Field(default_factory=list)
    permissions: list[str] = Field(default_factory=list)
    linked_contact_ids: list[UUID] = Field(default_factory=list)


class AdminUserAccessItem(BaseModel):
    app_user_id: UUID
    tenant_id: str
    entra_object_id: str
    email: str | None = None
    display_name: str | None = None
    is_active: bool
    is_approved: bool
    roles: list[str] = Field(default_factory=list)
    permissions: list[str] = Field(default_factory=list)
    linked_contact_ids: list[UUID] = Field(default_factory=list)


class AdminRoleItem(BaseModel):
    role_name: str
    description: str | None = None


class AdminUpdateUserRolesRequest(BaseModel):
    roles: list[str] = Field(default_factory=list)
    is_active: bool | None = None
    is_approved: bool | None = None

    @field_validator("roles")
    @classmethod
    def validate_roles(cls, value: list[str]) -> list[str]:
        cleaned = sorted({item.strip() for item in value if item and item.strip()})
        return cleaned
