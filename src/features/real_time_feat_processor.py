from __future__ import annotations

"""
Real-time feature processing topology built on faust-streaming.

This module wires the shared ingestion topic into a Faust app and exposes a
features topic that downstream consumers (decision engine, sinks, etc.) can
subscribe to.

Importing this module (and the side-effect-only ``realtime_velocity`` module)
registers:

* The shared Faust ``app`` instance.
* The ingestion and feature topics.
* The per-user velocity agent that emits ``FeatureEvent`` records with
  ``velocity_1m`` and ``velocity_1h`` fields derived from the ingestion stream.

Later plans will register additional agents for geo-anomaly and device-based
features using the same shared app and topics.
"""

import faust

from .realtime_config import FEATURES_TOPIC, INGESTION_TOPIC, KAFKA_BROKER_URL
from .realtime_models import FeatureEvent, IngestionEvent
from . import realtime_geo  # noqa: F401  (register geo-anomaly agent)
from . import realtime_velocity  # noqa: F401  -- register velocity tables and agent


APP_NAME = "real-time-feature-processor"


app = faust.App(
    APP_NAME,
    broker=KAFKA_BROKER_URL,
)


ingestion_topic = app.topic(
    INGESTION_TOPIC,
    value_type=IngestionEvent,
)

features_topic = app.topic(
    FEATURES_TOPIC,
    value_type=FeatureEvent,
)


def get_app() -> faust.App:
    """Return the shared Faust app instance for feature modules."""

    return app


def get_topics() -> tuple[faust.Topic[IngestionEvent], faust.Topic[FeatureEvent]]:
    """Return the shared ingestion and features topics."""

    return ingestion_topic, features_topic


if __name__ == "__main__":
    # When invoked directly, delegate to Faust's entrypoint so this module can
    # be run with ``python -m src.features.real_time_feat_processor`` during
    # local development. In production, prefer the `faust` CLI:
    #
    #   faust -A src.features.real_time_feat_processor worker
    #
    app.main()

