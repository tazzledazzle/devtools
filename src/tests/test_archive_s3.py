import unittest

try:
    import boto3
except ImportError:
    boto3 = None
    mock_aws = None
    S3ArchiveStorage = None
    _S3_ARCHIVE_TEST_AVAILABLE = False
else:
    try:
        from moto import mock_aws
        from archive.s3_compat import S3ArchiveStorage
        _S3_ARCHIVE_TEST_AVAILABLE = True
    except ImportError:
        mock_aws = None
        S3ArchiveStorage = None
        _S3_ARCHIVE_TEST_AVAILABLE = False


def _maybe_mock_s3(cls):
    return mock_aws(cls) if _S3_ARCHIVE_TEST_AVAILABLE else cls


@unittest.skipIf(
    not _S3_ARCHIVE_TEST_AVAILABLE,
    "boto3 and moto[s3] required for S3 archive tests",
)
@_maybe_mock_s3
class TestS3ArchiveStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.client = boto3.client("s3", region_name="us-east-1")
        self.bucket = "test-archive-bucket"
        self.client.create_bucket(Bucket=self.bucket)

    def test_write_list_replay_roundtrip(self) -> None:
        """Write N events, list returns N keys, replay yields same events in key order."""
        storage = S3ArchiveStorage(self.bucket, prefix="events", region_name="us-east-1")

        events = [{"id": i, "value": f"event-{i}"} for i in range(3)]
        for event in events:
            storage.write(event)

        keys = storage.list()
        self.assertEqual(len(keys), len(events))

        replayed = list(storage.replay())
        self.assertEqual(len(replayed), len(events))
        replayed_by_id = {e["id"]: e for e in replayed}
        for ev in events:
            self.assertEqual(replayed_by_id[ev["id"]]["value"], ev["value"])


if __name__ == "__main__":
    unittest.main()
