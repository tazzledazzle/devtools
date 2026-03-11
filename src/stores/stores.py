"""
Feature and state store abstractions for online scoring and streaming state.

Keyspace conventions (consistent across backends):
- Feature vectors by user:  features:user:{user_id}
- Feature vectors by transaction/authorization:  features:tx:{tx_id}
- General state keys are backend-specific; prefer a prefix (e.g. state:...) for clarity.

Configuration: Redis connection is configured via REDIS_URL environment variable
(e.g. redis://localhost:6379/0). Default for local dev: redis://localhost:6379/0.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Keyspace helpers (documented conventions)
# ---------------------------------------------------------------------------

def feature_key_user(user_id: str) -> str:
    """Key for feature vector keyed by user_id."""
    return f"features:user:{user_id}"


def feature_key_tx(tx_id: str) -> str:
    """Key for feature vector keyed by transaction/authorization ID."""
    return f"features:tx:{tx_id}"


# ---------------------------------------------------------------------------
# FeatureStores: key-value API for feature vectors
# ---------------------------------------------------------------------------

@runtime_checkable
class FeatureStores(Protocol):
    """
    Key-value API for reading and writing feature vectors keyed by user_id
    or transaction/authorization ID. Keys are built using feature_key_user()
    and feature_key_tx(); implementors may accept raw keys or high-level
    (user_id, tx_id) helpers.
    """

    def get_features(self, key: str) -> dict[str, Any] | None:
        """Return the feature vector for the given key, or None if missing."""
        ...

    def set_features(self, key: str, features: Mapping[str, Any]) -> None:
        """Store the feature vector under the given key (overwrites)."""
        ...


# ---------------------------------------------------------------------------
# StatefulStore: general state for streaming layer
# ---------------------------------------------------------------------------

class StatefulStore(ABC):
    """
    General state abstraction for the streaming layer. Supports get/set over
    opaque JSON-serialisable payloads. Subclasses may add incr/hmset-style
    operations. Keys are arbitrary but should follow a consistent prefix
    convention (e.g. state:...) to avoid collisions.
    """

    @abstractmethod
    def get(self, key: str) -> dict[str, Any] | None:
        """Return the value for key (JSON-decoded), or None if missing."""
        ...

    @abstractmethod
    def set(self, key: str, value: Mapping[str, Any] | None) -> None:
        """Store the value (JSON-serialised) under key. None may clear the key."""
        ...
