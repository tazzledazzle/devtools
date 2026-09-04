from __future__ import annotations

from collections import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

@dataclass
class CachedResult:
    response: str
    expires_at: datetime

@dataclass
class IdempotencyStore:
    ttl: timedelta = field(default=timedelta(minutes=10))
    _store: dict[str, CachedResult] = field(default_factory=dict, init=False, repr=False)

    def get_or_compute(self, key: str, compute: Callable[[], str]) -> str:
        now = datetime.now(timezone.utc)
        existing = self._store.get(key)

        if existing is not None and now < existing.expires_at:
            return existing.response # replay - do NOT compute charge

        result = compute()
        self._store[key] = CachedResult(result, expires_at=now + self.ttl)
        return result
