import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from archive.local import LocalArchiveStorage


class TestLocalArchiveStorage(unittest.TestCase):
    def test_write_list_replay_roundtrip(self) -> None:
        """write N events, list returns N items, replay yields same events in order."""
        with TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / "events.jsonl"
            storage = LocalArchiveStorage(archive_path)

            events = [{"id": i, "value": f"event-{i}"} for i in range(5)]
            for event in events:
                storage.write(event)

            keys = storage.list()
            self.assertEqual(len(keys), len(events))

            replayed_events = list(storage.replay())
            self.assertEqual(replayed_events, events)


if __name__ == "__main__":
    unittest.main()

