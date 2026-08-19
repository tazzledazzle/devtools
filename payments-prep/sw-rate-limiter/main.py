
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

@dataclass
class SlidingWindowRateLimiter:
    max_requests: int
    window_ms: int
    _timestamps: deque[int] = field(default_factory=deque, init=False, repr=False)

    def allow(self, at: int | None = None) -> bool:
        now = at if at is not None else now_ms()
        cutoff = now - self.window_ms

        # evict timestamps outside window
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()

        if len(self._timestamps) < self.max_requests:
            self._timestamps.append(now)
            return True
        return False


if __name__ == "__main__":
    limiter = SlidingWindowRateLimiter(max_requests=3, window_ms=1000)
    for i in range(5):
        print(f"request {i} allowed: {limiter.allow()}")
