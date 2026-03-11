import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

try:
    from confluent_kafka import Consumer  # type: ignore[import]
except ImportError:  # pragma: no cover - optional integration dependency
    Consumer = None  # type: ignore[assignment]

from archive.local import LocalArchiveStorage
from ingestion.stream_ingester import StreamIngester, ensure_topic_exists
from sources.event_sources import EventSourcesApps


_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")


def _kafka_available() -> bool:
    return Consumer is not None and bool(_BOOTSTRAP_SERVERS)


@unittest.skipUnless(
    _kafka_available(),
    "Kafka integration test requires confluent-kafka and KAFKA_BOOTSTRAP_SERVERS",
)
class TestStreamIngesterIntegration(unittest.TestCase):
    def setUp(self) -> None:
        assert _BOOTSTRAP_SERVERS is not None  # for type checkers
        self.bootstrap = _BOOTSTRAP_SERVERS
        self.topic = f"ingestion-events-{uuid4().hex[:8]}"

        # Ensure topic exists before producing.
        try:
            ensure_topic_exists(bootstrap_servers=self.bootstrap, topic=self.topic)
        except Exception as exc:  # pragma: no cover - best-effort integration guard
            raise unittest.SkipTest(f"Kafka broker unreachable: {exc}") from exc

        self._tmpdir = TemporaryDirectory()
        archive_path = Path(self._tmpdir.name) / "events.jsonl"
        self.archive = LocalArchiveStorage(archive_path)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _consume_all(self, *, expected: int) -> int:
        """Consume up to ``expected`` messages from the topic and return count."""
        assert Consumer is not None

        consumer_conf: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap,
            "group.id": f"test-stream-ingester-{uuid4().hex[:6]}",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([self.topic])

        seen = 0
        try:
            while seen < expected:
                msg = consumer.poll(5.0)
                if msg is None:
                    break
                if msg.error():
                    raise RuntimeError(msg.error())
                seen += 1
        finally:
            consumer.close()

        return seen

    def test_produced_and_consumed_counts_match(self) -> None:
        """Produce N events and verify consumer observes N messages."""
        n_events = 5
        source = EventSourcesApps(limit=n_events, seed=7)

        ingester = StreamIngester.from_config(
            source=source,
            archive=self.archive,
            bootstrap_servers=self.bootstrap,
            topic=self.topic,
        )

        ingester.run(timeout=30.0)

        consumed = self._consume_all(expected=n_events)
        self.assertEqual(
            consumed,
            n_events,
            "Kafka consumer should observe the same number of messages produced",
        )


if __name__ == "__main__":
    unittest.main()

