from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from app.security.hmac_verifier import HmacVerifier


@pytest.mark.parametrize(
    "timestamp",
    [
        str(int(datetime.now(tz=timezone.utc).timestamp())),
        datetime.now(tz=timezone.utc).isoformat(),
    ],
)
def test_hmac_verifier_accepts_valid_signature(timestamp: str) -> None:
    verifier = HmacVerifier(secret="secret")
    raw = b'{"ok":true}'

    import base64
    import hashlib
    import hmac

    digest = hmac.new(b"secret", timestamp.encode("utf-8") + b"." + raw, hashlib.sha256).digest()
    signature = base64.b64encode(digest).decode("ascii")

    verifier.verify(timestamp=timestamp, signature=signature, raw_body=raw)


def test_hmac_verifier_rejects_bad_signature() -> None:
    verifier = HmacVerifier(secret="secret")
    raw = b"{}"
    ts = str(int(datetime.now(tz=timezone.utc).timestamp()))

    with pytest.raises(HTTPException) as exc:
        verifier.verify(timestamp=ts, signature="bad", raw_body=raw)

    assert exc.value.status_code == 401
