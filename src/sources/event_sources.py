from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator

from generators.synthetic_payments import generate_synthetic_payments


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


class EventSourcesApps(EventSources):
    """Event source for application-originated events backed by synthetic data.

    This implementation uses :func:`generate_synthetic_payments` to produce
    schema-conformant payment events so that the ingester can consume events
    from a unified interface regardless of backend.
    """

    def __init__(self, *, limit: int | None = None, seed: int | None = 42) -> None:
        self._limit = limit
        self._seed = seed

    def events(self) -> Iterator[dict]:
        """Yield events from the synthetic payment generator.

        An optional ``limit`` provided at construction time bounds the stream
        so tests and callers can control how many events are produced.
        """
        if not self._limit or self._limit <= 0:
            return iter(())

        # The synthetic generator yields schema-conformant dictionaries that
        # satisfy the shared EventSources schema contract.
        return generate_synthetic_payments(self._limit, seed=self._seed)
