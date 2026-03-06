from __future__ import annotations

import base64
import json
import time
import urllib.parse
from dataclasses import dataclass

import httpx
from fastapi import HTTPException

from app.auth.models import EntraTokenClaims


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}".encode("utf-8"))


def decode_jwt_payload(token: str) -> dict:
    parts = token.split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid ID token format")
    try:
        payload_raw = _base64url_decode(parts[1])
        payload = json.loads(payload_raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=401, detail="Invalid ID token payload") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=401, detail="Invalid ID token payload")
    return payload


@dataclass(slots=True)
class EntraOidcClient:
    authority: str
    tenant_id: str
    client_id: str
    client_secret: str
    redirect_uri: str
    scope: str

    @property
    def authorization_endpoint(self) -> str:
        return f"{self.authority.rstrip('/')}/{self.tenant_id}/oauth2/v2.0/authorize"

    @property
    def token_endpoint(self) -> str:
        return f"{self.authority.rstrip('/')}/{self.tenant_id}/oauth2/v2.0/token"

    @property
    def expected_issuer(self) -> str:
        return f"{self.authority.rstrip('/')}/{self.tenant_id}/v2.0"

    def build_login_url(self, *, state: str, nonce: str) -> str:
        query = urllib.parse.urlencode(
            {
                "client_id": self.client_id,
                "response_type": "code",
                "redirect_uri": self.redirect_uri,
                "response_mode": "query",
                "scope": self.scope,
                "state": state,
                "nonce": nonce,
            }
        )
        return f"{self.authorization_endpoint}?{query}"

    async def exchange_code_for_id_token(self, code: str) -> str:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                self.token_endpoint,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": self.redirect_uri,
                    "scope": self.scope,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        if response.status_code >= 400:
            detail = response.text
            raise HTTPException(status_code=401, detail=f"Token exchange failed: {detail}")
        payload = response.json()
        token = payload.get("id_token")
        if not token:
            raise HTTPException(status_code=401, detail="Token endpoint did not return id_token")
        return token

    def validate_id_token_claims(self, token: str, *, expected_nonce: str | None) -> EntraTokenClaims:
        claims = EntraTokenClaims.model_validate(decode_jwt_payload(token))
        now = int(time.time())

        if claims.aud != self.client_id:
            raise HTTPException(status_code=401, detail="ID token audience mismatch")

        allowed_issuers = {
            self.expected_issuer,
            f"{self.expected_issuer}/",
            f"{self.authority.rstrip('/')}/{claims.tid}/v2.0",
            f"{self.authority.rstrip('/')}/{claims.tid}/v2.0/",
        }
        if claims.iss not in allowed_issuers:
            raise HTTPException(status_code=401, detail="ID token issuer mismatch")
        if claims.tid != self.tenant_id:
            raise HTTPException(status_code=401, detail="ID token tenant mismatch")
        if expected_nonce and claims.nonce != expected_nonce:
            raise HTTPException(status_code=401, detail="ID token nonce mismatch")
        if claims.exp <= now:
            raise HTTPException(status_code=401, detail="ID token expired")
        if claims.nbf and claims.nbf > now + 60:
            raise HTTPException(status_code=401, detail="ID token not yet valid")
        return claims
