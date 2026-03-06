import base64
import json
import time

import pytest
from fastapi import HTTPException

from app.auth.oidc import EntraOidcClient


def _b64(data: dict) -> str:
    raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def make_token(payload: dict) -> str:
    header = {"alg": "none", "typ": "JWT"}
    return f"{_b64(header)}.{_b64(payload)}."


def test_validate_id_token_claims_success() -> None:
    now = int(time.time())
    client = EntraOidcClient(
        authority="https://login.microsoftonline.com",
        tenant_id="tenant-123",
        client_id="client-123",
        client_secret="secret",
        redirect_uri="http://localhost:8000/auth/callback",
        scope="openid profile email",
    )
    token = make_token(
        {
            "tid": "tenant-123",
            "oid": "object-123",
            "aud": "client-123",
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "nonce": "abc",
            "exp": now + 600,
            "iat": now - 10,
            "nbf": now - 10,
            "email": "user@example.com",
            "name": "User One",
        }
    )
    claims = client.validate_id_token_claims(token, expected_nonce="abc")
    assert claims.tid == "tenant-123"
    assert claims.oid == "object-123"


def test_validate_id_token_claims_nonce_mismatch() -> None:
    now = int(time.time())
    client = EntraOidcClient(
        authority="https://login.microsoftonline.com",
        tenant_id="tenant-123",
        client_id="client-123",
        client_secret="secret",
        redirect_uri="http://localhost:8000/auth/callback",
        scope="openid profile email",
    )
    token = make_token(
        {
            "tid": "tenant-123",
            "oid": "object-123",
            "aud": "client-123",
            "iss": "https://login.microsoftonline.com/tenant-123/v2.0",
            "nonce": "wrong",
            "exp": now + 600,
        }
    )
    with pytest.raises(HTTPException) as exc:
        client.validate_id_token_claims(token, expected_nonce="expected")
    assert exc.value.status_code == 401
