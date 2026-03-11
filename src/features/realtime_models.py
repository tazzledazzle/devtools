from __future__ import annotations

from typing import Any, Mapping, MutableMapping

import faust

"""
Faust models for the real-time feature processing pipeline.

These records mirror the synthetic payment event schema produced by
EventSources-backed generators and define a generic feature event envelope that
downstream consumers can rely on regardless of which specific features are
attached.
"""


class IngestionEvent(faust.Record, serializer="json"):
    """Schema for raw ingestion events emitted by the StreamIngester."""

    user_id: str
    amount: float
    timestamp: str
    device_id: str
    location: str


class FeatureEvent(faust.Record, serializer="json"):
    """Envelope for aggregated or derived feature signals.

    The concrete velocity, geo-anomaly, and device features will be added as
    keys on the ``features`` mapping in later plans so that the core streaming
    topology can remain stable while feature definitions evolve.
    """

    user_id: str
    timestamp: str
    features: Mapping[str, Any]
    metadata: MutableMapping[str, Any] | None = None

