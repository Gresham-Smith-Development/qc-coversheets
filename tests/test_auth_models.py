from uuid import UUID

from app.auth.models import AuthenticatedUser


def test_pending_access_for_unapproved_user() -> None:
    user = AuthenticatedUser(
        app_user_id=UUID("00000000-0000-0000-0000-000000000100"),
        tenant_id="t",
        entra_object_id="o",
        is_active=True,
        is_approved=False,
        roles=["reviewer"],
        permissions=["reviewer.form.read"],
    )
    assert user.is_pending_access is True


def test_admin_detection() -> None:
    user = AuthenticatedUser(
        app_user_id=UUID("00000000-0000-0000-0000-000000000101"),
        tenant_id="t",
        entra_object_id="o",
        is_active=True,
        is_approved=True,
        roles=["admin"],
        permissions=["admin.access"],
    )
    assert user.is_admin is True
    assert user.is_pending_access is False
