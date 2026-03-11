import unittest
from unittest.mock import MagicMock, patch

from ingestion.stream_ingester import ensure_topic_exists


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


if __name__ == "__main__":
    unittest.main()

