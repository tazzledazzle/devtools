"""Synthetic payment event generator based on the shared schema contract."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import random
from typing import Iterator

from generators.schema import EventSources

_USER_IDS = ("user_001", "user_002", "user_003", "user_004", "user_005")
_DEVICE_IDS = ("ios_phone", "android_phone", "web_chrome", "web_safari")
_LOCATIONS = ("NYC", "SF", "LA", "SEA", "AUS")


def _build_event(rng: random.Random, base_time: datetime) -> EventSources:
    amount = min(max(rng.lognormvariate(3.2, 0.45), 1.0), 1500.0)
    timestamp = (base_time + timedelta(seconds=rng.randint(0, 600))).isoformat()

    return {
        "user_id": rng.choice(_USER_IDS),
        "amount": round(float(amount), 2),
        "timestamp": timestamp,
        "device_id": rng.choice(_DEVICE_IDS),
        "location": rng.choice(_LOCATIONS),
    }


def generate_synthetic_payments(
    n: int,
    *,
    seed: int | None = 42,
    start_time: datetime | None = None,
) -> Iterator[EventSources]:
    """Yield ``n`` schema-conformant synthetic payment events."""
    rng = random.Random(seed)
    base_time = start_time or datetime.now(timezone.utc)

    for _ in range(n):
        yield _build_event(rng, base_time)

