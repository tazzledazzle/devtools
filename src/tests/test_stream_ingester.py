import json
import unittest
from typing import Iterator
from unittest.mock import MagicMock, patch

from archive.storage import ArchiveStorage
from ingestion.stream_ingester import StreamIngester, ensure_topic_exists
from sources.event_sources import EventSources


class TestEnsureTopicExists(unittest.TestCase):
    @patch("ingestion.stream_ingester.NewTopic")
    @patch("ingestion.stream_ingester.AdminClient")
    def test_ensure_topic_exists_creates_topic_with_admin_client(
        self, mock_admin_client: MagicMock, mock_new_topic: MagicMock
    ) -> None:
        """
        ensure_topic_exists should construct an AdminClient for the given
        bootstrap servers and invoke create_topics() with a NewTopic for the
        requested topic name.
        """
        bootstrap_servers = "localhost:9092"
        topic_name = "ingestion-events"

        admin_instance = mock_admin_client.return_value

        ensure_topic_exists(
            bootstrap_servers=bootstrap_servers,
            topic=topic_name,
            num_partitions=3,
            replication_factor=1,
        )

        mock_admin_client.assert_called_once_with({"bootstrap.servers": bootstrap_servers})
        mock_new_topic.assert_called_once()
        admin_instance.create_topics.assert_called_once()


class _DummySource(EventSources):
    def __init__(self, events: list[dict]):
        self._events = events

    def events(self) -> Iterator[dict]:
        return iter(self._events)


class TestStreamIngester(unittest.TestCase):
    def test_archives_then_produces_and_flushes(self) -> None:
        """StreamIngester should write to archive, then produce to Kafka, then flush."""
        events = [
            {"user_id": "user-1", "amount": 10.0},
            {"user_id": "user-2", "amount": 20.0},
        ]
        source = _DummySource(events)
        archive = MagicMock(spec=ArchiveStorage)
        producer = MagicMock()

        ingester = StreamIngester(
            source=source,
            archive=archive,
            producer=producer,
            topic="ingestion-events",
        )

        ingester.run(timeout=1.0)

        # Each event is first written to archive, then produced.
        self.assertEqual(archive.write.call_count, len(events))
        self.assertEqual(producer.produce.call_count, len(events))
        self.assertEqual(producer.poll.call_count, len(events))

        # Flush is invoked once at the end with the provided timeout.
        producer.flush.assert_called_once_with(1.0)

        # Verify that values are JSON-encoded bytes.
        for call in producer.produce.call_args_list:
            _, kwargs = call
            self.assertIsInstance(kwargs["value"], (bytes, bytearray))
            decoded = json.loads(kwargs["value"].decode("utf-8"))
            self.assertIn("user_id", decoded)

    @patch("ingestion.stream_ingester.Producer")
    def test_from_config_uses_at_least_once_semantics(self, mock_producer_cls: MagicMock) -> None:
        """from_config should configure Producer for at-least-once semantics."""
        events: list[dict] = []
        source = _DummySource(events)
        archive = MagicMock(spec=ArchiveStorage)

        _ = StreamIngester.from_config(
            source=source,
            archive=archive,
            bootstrap_servers="localhost:9092",
            topic="ingestion-events",
        )

        mock_producer_cls.assert_called_once()
        args, _ = mock_producer_cls.call_args
        config = args[0]

        self.assertEqual(config["bootstrap.servers"], "localhost:9092")
        self.assertEqual(config["acks"], "all")
        self.assertGreaterEqual(config["retries"], 1)



if __name__ == "__main__":
    unittest.main()

