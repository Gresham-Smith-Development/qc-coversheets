from __future__ import annotations

import secrets
from typing import Any
from uuid import UUID

from starlette.requests import Request

SESSION_AUTH_STATE_KEY = "auth_state"
SESSION_AUTH_NONCE_KEY = "auth_nonce"
SESSION_USER_ID_KEY = "auth_user_id"
SESSION_SESSION_ID_KEY = "auth_session_id"


def ensure_session_id(request: Request) -> str:
    session = request.session
    session_id = session.get(SESSION_SESSION_ID_KEY)
    if not session_id:
        session_id = secrets.token_urlsafe(24)
        session[SESSION_SESSION_ID_KEY] = session_id
    return session_id


def new_auth_state_nonce() -> tuple[str, str]:
    return secrets.token_urlsafe(24), secrets.token_urlsafe(24)


def store_auth_flow(request: Request, *, state: str, nonce: str) -> None:
    request.session[SESSION_AUTH_STATE_KEY] = state
    request.session[SESSION_AUTH_NONCE_KEY] = nonce


def pop_auth_flow(request: Request) -> tuple[str | None, str | None]:
    session = request.session
    state = session.pop(SESSION_AUTH_STATE_KEY, None)
    nonce = session.pop(SESSION_AUTH_NONCE_KEY, None)
    return state, nonce


def store_user_session(request: Request, app_user_id: UUID) -> None:
    request.session[SESSION_USER_ID_KEY] = str(app_user_id)


def clear_user_session(request: Request) -> None:
    request.session.pop(SESSION_USER_ID_KEY, None)
    request.session.pop(SESSION_AUTH_STATE_KEY, None)
    request.session.pop(SESSION_AUTH_NONCE_KEY, None)


def get_session_user_id(request: Request) -> UUID | None:
    value: Any = request.session.get(SESSION_USER_ID_KEY)
    if not value:
        return None
    try:
        return UUID(str(value))
    except ValueError:
        return None
