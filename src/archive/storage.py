# Archive layer: unified interface for persisting raw events to local FS or S3.
#
# Ordering: This implementation uses **archive-then-produce**. StreamIngester
# should call write(event) to persist each event, then produce to Kafka.
# That ensures events are durable before streaming; replay can refeed from
# archive if needed.

from abc import ABC, abstractmethod
from typing import Any, Iterator, List


class ArchiveStorage(ABC):
    """Interface for archive backends: write(event), list(), replay()."""

    @abstractmethod
    def write(self, event: dict[str, Any]) -> None:
        """Persist one raw event (e.g. as JSON)."""
        ...

    @abstractmethod
    def list(self) -> List[str]:
        """Return identifiers or paths for stored events, in replay order."""
        ...

    @abstractmethod
    def replay(self) -> Iterator[dict[str, Any]]:
        """Yield stored events as dicts in deterministic order."""
        ...
