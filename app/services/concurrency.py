from __future__ import annotations

import asyncio


class ConcurrencyLimiter:
    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._in_flight = 0
        self._lock = asyncio.Lock()

    async def try_acquire(self) -> bool:
        async with self._lock:
            if self._in_flight >= self._limit:
                return False
            self._in_flight += 1
            return True

    async def release(self) -> None:
        async with self._lock:
            if self._in_flight > 0:
                self._in_flight -= 1
