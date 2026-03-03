from __future__ import annotations

from uuid import uuid4

from fastapi import Request


class CorrelationProvider:
    @staticmethod
    def resolve(request: Request, body_correlation_id: str | None) -> str:
        header_value = request.headers.get("X-Correlation-Id") or request.headers.get("x-correlation-id")
        if body_correlation_id:
            return body_correlation_id
        if header_value:
            return header_value
        return str(uuid4())
