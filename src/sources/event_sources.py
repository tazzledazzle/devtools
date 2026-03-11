from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator


class EventSources(ABC):
    """Abstract base class for event sources that produce payment events.

    Concrete subclasses implement :meth:`events` to pull events from specific
    backends (apps, databases, APIs, etc.). The iterator yields dictionaries
    containing at least the schema fields: ``user_id``, ``amount``,
    ``timestamp``, ``device_id``, and ``location``.
    """

    @abstractmethod
    def events(self) -> Iterator[dict]:
        """Return an iterator over event dictionaries."""
        raise NotImplementedError


class EventSourcesApps:
    """Event source for application-originated events (implemented in Task 2)."""
    pass
