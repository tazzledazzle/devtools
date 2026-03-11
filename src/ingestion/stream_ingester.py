"""Streaming ingestion from EventSources into Kafka.

This module wires together EventSources, the archive layer, and a Kafka-
compatible broker. It follows an **archive-then-produce** ordering so that
raw events are durably persisted before being streamed downstream.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, MutableMapping

from archive.storage import ArchiveStorage
from sources.event_sources import EventSources

try:  # pragma: no cover - exercised indirectly via patched symbols in tests
    from confluent_kafka import Producer as _KafkaProducer
    from confluent_kafka.admin import (
        AdminClient as _KafkaAdminClient,
        NewTopic as _KafkaNewTopic,
    )
except ImportError:  # graceful degradation when Kafka client is not installed
    _KafkaProducer = None  # type: ignore[assignment]
    _KafkaAdminClient = None  # type: ignore[assignment]
    _KafkaNewTopic = None  # type: ignore[assignment]

# Expose patchable aliases so tests can monkeypatch AdminClient/NewTopic/Producer
# even when confluent-kafka is not available in the environment.
Producer = _KafkaProducer  # type: ignore[assignment]
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


class StreamIngester:
    """Consume events from an EventSources backend and stream them to Kafka.

    For each event produced by the source, this class:

    1. Writes the raw event to the configured archive backend.
    2. Produces the JSON-serialized event to the configured Kafka topic.
    3. Uses ``acks='all'`` and retries on the Producer configuration to provide
       at-least-once delivery semantics.
    4. Calls ``flush()`` at the end of the run and treats any remaining queued
       messages as a failure.
    """

    def __init__(
        self,
        *,
        source: EventSources,
        archive: ArchiveStorage,
        producer: Any,
        topic: str,
        key_field: str | None = "user_id",
    ) -> None:
        self._source = source
        self._archive = archive
        self._producer = producer
        self._topic = topic
        self._key_field = key_field
        self._delivery_errors: list[str] = []

    @classmethod
    def from_config(
        cls,
        *,
        source: EventSources,
        archive: ArchiveStorage,
        bootstrap_servers: str,
        topic: str,
        producer_config: Mapping[str, Any] | None = None,
        key_field: str | None = "user_id",
    ) -> "StreamIngester":
        """Construct a StreamIngester from Kafka configuration.

        The resulting Producer is configured for at-least-once semantics via
        ``acks='all'`` and a small number of retries.
        """
        if Producer is None:
            msg = (
                "confluent-kafka is required for streaming ingestion; "
                "install 'confluent-kafka>=2.3' and retry."
            )
            raise RuntimeError(msg)

        config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
        }
        if producer_config:
            # Caller-supplied config can override defaults if needed.
            config.update(producer_config)

        producer = Producer(config)  # type: ignore[call-arg]
        return cls(
            source=source,
            archive=archive,
            producer=producer,
            topic=topic,
            key_field=key_field,
        )

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        """Delivery callback that records any asynchronous errors."""
        if err is not None:
            self._delivery_errors.append(str(err))

    def run(self, *, timeout: float | None = 30.0) -> None:
        """Consume events from the source, archive them, and produce to Kafka.

        This method blocks until all events from ``source.events()`` have been
        written, produced, and ``flush()`` has completed or the timeout elapses.
        """
        self._delivery_errors.clear()

        for event in self._source.events():
            # Archive-then-produce ordering: persist before streaming.
            self._archive.write(event)

            value_bytes = json.dumps(event).encode("utf-8")
            key_bytes: bytes | None = None
            if self._key_field and self._key_field in event:
                key_bytes = str(event[self._key_field]).encode("utf-8")

            self._producer.produce(
                self._topic,
                key=key_bytes,
                value=value_bytes,
                on_delivery=self._on_delivery,
            )
            # Drive delivery callbacks without blocking.
            self._producer.poll(0)

        remaining = self._producer.flush(timeout if timeout is not None else 30.0)
        # confluent_kafka.Producer.flush() returns the number of messages still
        # queued for delivery. In tests this may be a MagicMock rather than an
        # int, so we only treat non-zero integers as a failure.
        if isinstance(remaining, int) and remaining:
            raise RuntimeError(
                f"Kafka producer flush reported {remaining} message(s) still queued"
            )

        if self._delivery_errors:
            joined = "; ".join(self._delivery_errors)
            raise RuntimeError(f"Kafka delivery errors encountered: {joined}")

