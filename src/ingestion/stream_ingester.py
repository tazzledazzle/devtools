"""Streaming ingestion from EventSources into Kafka.

This module wires together EventSources, the archive layer, and a Kafka-
compatible broker. It follows an **archive-then-produce** ordering so that
raw events are durably persisted before being streamed downstream.
"""

from __future__ import annotations

from typing import Mapping, MutableMapping

try:  # pragma: no cover - exercised indirectly via patched symbols in tests
    from confluent_kafka.admin import AdminClient as _KafkaAdminClient, NewTopic as _KafkaNewTopic
except ImportError:  # graceful degradation when Kafka client is not installed
    _KafkaAdminClient = None  # type: ignore[assignment]
    _KafkaNewTopic = None  # type: ignore[assignment]

# Expose patchable aliases so tests can monkeypatch AdminClient/NewTopic even
# when confluent-kafka is not available in the environment.
AdminClient = _KafkaAdminClient  # type: ignore[assignment]
NewTopic = _KafkaNewTopic  # type: ignore[assignment]


def ensure_topic_exists(
    *,
    bootstrap_servers: str,
    topic: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    config: Mapping[str, str] | None = None,
) -> None:
    """Ensure that the given topic exists on the Kafka cluster.

    This uses :class:`confluent_kafka.admin.AdminClient` with ``create_topics``
    in a best-effort, idempotent fashion. If the topic already exists, the
    broker will ignore the create request; callers can safely invoke this on
    every startup without needing a separate provisioning step.
    """
    topic_config: MutableMapping[str, str] = dict(config or {})

    if AdminClient is None or NewTopic is None:
        msg = (
            "confluent-kafka is required to ensure topic existence; "
            "install 'confluent-kafka>=2.3' and retry."
        )
        raise RuntimeError(msg)

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})  # type: ignore[operator]
    new_topic = NewTopic(  # type: ignore[call-arg]
        topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config=topic_config or None,
    )

    # confluent_kafka returns a mapping of futures keyed by topic. We only
    # need to surface hard failures; topic-already-exists errors are safe to
    # ignore so that re-running this function is idempotent. In tests the
    # result may be a MagicMock rather than a real mapping, so we guard on
    # the concrete type.
    futures = admin.create_topics([new_topic])
    if isinstance(futures, dict):
        for future in futures.values():
            try:
                future.result()
            except Exception as exc:  # pragma: no cover - defensive
                message = str(exc)
                if "Topic already exists" in message or "TOPIC_ALREADY_EXISTS" in message:
                    continue
                raise

