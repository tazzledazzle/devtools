from __future__ import annotations

"""
Real-time feature processing topology built on faust-streaming.

This module wires the shared ingestion topic into a Faust app and exposes a
features topic that downstream consumers (decision engine, sinks, etc.) can
subscribe to.

Importing this module (and the side-effect-only feature modules) registers:

* The shared Faust ``app`` instance.
* The ingestion and feature topics.
* The per-user velocity agent that emits ``FeatureEvent`` records with
  ``velocity_1m`` and ``velocity_1h`` fields derived from the ingestion stream.
* The geo-anomaly agent that tracks last-known locations and emits distance,
  time delta, and anomaly flags.
* The known/new device agent that tracks per-user devices in a changelog-
  backed table and emits an ``is_new_device`` flag for each event.

Restart safety is provided by Faust's RocksDB-backed tables and Kafka
changelog topics (via the ``faust-streaming[rocksdb]`` extra). To verify
restart behavior manually:

1. Start a local Kafka broker and run the worker:

   ``faust -A src.features.real_time_feat_processor worker``

2. Produce a few ingestion events for the same user/device.
3. Stop the worker and then restart it with the same state directory.
4. Send additional events for the same user/device and confirm that velocity,
   geo, and device features continue from the preserved state instead of
   recomputing from the beginning.

Feature store integration: Use ``RealTimeFeatProcessor`` with injected
``FeatureStores`` and optional ``StatefulStore`` to persist computed features
for online lookup. Keys used: ``features:user:{user_id}`` always;
``features:tx:{tx_id}`` when tx_id is non-empty (e.g. authorization ID).
"""

import faust

from .feat_processor_store import RealTimeFeatProcessor
from .realtime_config import FEATURES_TOPIC, INGESTION_TOPIC, KAFKA_BROKER_URL
from .realtime_models import FeatureEvent, IngestionEvent


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


# Import feature modules for their side-effect registration of tables and agents.
from . import realtime_device  # noqa: F401
from . import realtime_geo  # noqa: F401
from . import realtime_velocity  # noqa: F401


if __name__ == "__main__":
    # When invoked directly, delegate to Faust's entrypoint so this module can
    # be run with ``python -m src.features.real_time_feat_processor`` during
    # local development. In production, prefer the `faust` CLI:
    #
    #   faust -A src.features.real_time_feat_processor worker
    #
    app.main()

