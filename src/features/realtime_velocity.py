from __future__ import annotations

from datetime import timedelta
from typing import Any, Mapping

from .real_time_feat_processor import app, features_topic, ingestion_topic
from .realtime_config import (
    VELOCITY_LONG_WINDOW_SECONDS,
    VELOCITY_SHORT_WINDOW_SECONDS,
)
from .realtime_models import FeatureEvent, IngestionEvent

"""
Velocity window tables and agent for RealTimeFeatProcessor.

This module defines 1-minute and 1-hour tumbling window tables keyed by
``user_id`` and an agent that consumes from the shared ingestion topic,
updates the windowed counts, and publishes per-user velocity features
to the shared features topic.

Importing this module is sufficient to register the tables and agent
with the shared Faust app:

    from src.features import realtime_velocity  # noqa: F401

To run the worker locally (after Kafka is available and the ingestion
pipeline is producing events), start the Faust worker using:

    faust -A src.features.real_time_feat_processor worker

Each `FeatureEvent` emitted by this module currently includes:
    - ``user_id``: the user identifier from the ingestion event
    - ``timestamp``: the original ingestion event timestamp
    - ``features['velocity_1m']``: count of events in the last 1-minute window
    - ``features['velocity_1h']``: count of events in the last 1-hour window
"""


VELOCITY_1M_TABLE_NAME = "velocity_1m"
VELOCITY_1H_TABLE_NAME = "velocity_1h"


velocity_1m = app.Table(
    VELOCITY_1M_TABLE_NAME,
    default=int,
).tumbling(
    size=timedelta(seconds=VELOCITY_SHORT_WINDOW_SECONDS),
    # Keep short-window state around long enough for inspection while
    # still bounding retention by the long window horizon.
    expires=timedelta(seconds=VELOCITY_LONG_WINDOW_SECONDS),
)


velocity_1h = app.Table(
    VELOCITY_1H_TABLE_NAME,
    default=int,
).tumbling(
    size=timedelta(seconds=VELOCITY_LONG_WINDOW_SECONDS),
    expires=timedelta(seconds=VELOCITY_LONG_WINDOW_SECONDS * 2),
)


def _build_velocity_features(user_id: str) -> Mapping[str, Any]:
    """
    Construct the velocity feature mapping for the given user.

    The underlying Faust windowed tables expose window-aware values; casting
    to ``int`` ensures the payload is JSON-serializable regardless of the
    internal representation used by faust-streaming.
    """

    return {
        "velocity_1m": int(velocity_1m[user_id]),
        "velocity_1h": int(velocity_1h[user_id]),
    }


@app.agent(ingestion_topic)
async def velocity_features(stream) -> None:
    """
    Consume ingestion events, update velocity windows, and emit FeatureEvent.

    For each incoming ``IngestionEvent`` the agent:
    - Increments both 1-minute and 1-hour tumbling windows for that user.
    - Reads the current counts from the windowed tables.
    - Emits a ``FeatureEvent`` on the shared ``features_topic`` with
      ``velocity_1m`` and ``velocity_1h`` fields populated.
    """

    async for event in stream:  # type: ignore[assignment]
        # Faust will deserialize the event to IngestionEvent based on the
        # topic's value_type; we rely on that schema here.
        assert isinstance(event, IngestionEvent)

        user_id = event.user_id

        # Update windowed counts for this user.
        velocity_1m[user_id] += 1
        velocity_1h[user_id] += 1

        features = _build_velocity_features(user_id)

        feature_event = FeatureEvent(
            user_id=user_id,
            timestamp=event.timestamp,
            features=features,
            metadata=None,
        )

        await features_topic.send(value=feature_event)

