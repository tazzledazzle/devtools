# S3-compatible archive backend (boto3). Supports endpoint_url for MinIO.
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Iterator, List

import boto3

from archive.storage import ArchiveStorage


class S3ArchiveStorage(ArchiveStorage):
    """Archive backend that stores one JSON object per event in S3."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "events",
        *,
        endpoint_url: str | None = None,
        region_name: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name or "us-east-1",
        )

    def _key(self, event_id: str) -> str:
        date_part = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        return f"{self.prefix}/{date_part}/{event_id}.json"

    def write(self, event: dict[str, Any]) -> None:
        """Store one event as a JSON object; key = prefix/date/event_id.json."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        unique_id = f"{ts}_{uuid.uuid4()}"
        key = self._key(unique_id)
        body = json.dumps(event)
        self._client.put_object(Bucket=self.bucket, Key=key, Body=body)

    def list(self) -> List[str]:
        """Return object keys for stored events, in list order (prefix order)."""
        keys: List[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix + "/"):
            for obj in page.get("Contents") or []:
                keys.append(obj["Key"])
        keys.sort()
        return keys

    def replay(self) -> Iterator[dict[str, Any]]:
        """Yield stored events in key order."""
        for key in self.list():
            resp = self._client.get_object(Bucket=self.bucket, Key=key)
            body = resp["Body"].read().decode("utf-8")
            yield json.loads(body)
