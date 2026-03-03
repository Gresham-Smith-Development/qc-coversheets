from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException


class HmacVerifier:
    def __init__(self, secret: str, allowed_skew: timedelta = timedelta(minutes=5)) -> None:
        self._secret = secret.encode("utf-8")
        self._allowed_skew = allowed_skew

    def verify(self, *, timestamp: str, signature: str, raw_body: bytes, now: datetime | None = None) -> None:
        if not self._secret:
            raise HTTPException(status_code=500, detail="ERP_HMAC_SECRET is not configured")

        parsed_ts = self._parse_timestamp(timestamp)
        current = now or datetime.now(timezone.utc)
        if abs(current - parsed_ts) > self._allowed_skew:
            raise HTTPException(status_code=401, detail="Stale request timestamp")

        signing_input = timestamp.encode("utf-8") + b"." + raw_body
        digest = hmac.new(self._secret, signing_input, hashlib.sha256).digest()
        expected = base64.b64encode(digest).decode("ascii")

        if not hmac.compare_digest(expected, signature):
            raise HTTPException(status_code=401, detail="Invalid request signature")

    @staticmethod
    def _parse_timestamp(raw: str) -> datetime:
        try:
            if raw.isdigit():
                return datetime.fromtimestamp(int(raw), tz=timezone.utc)
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=401, detail="Invalid timestamp format") from exc
