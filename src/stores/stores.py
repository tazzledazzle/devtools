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

import json
import os
from abc import ABC, abstractmethod
from typing import Any, Mapping, Protocol, runtime_checkable

import redis
from redis.connection import ConnectionPool


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


# ---------------------------------------------------------------------------
# Redis-backed implementations
# ---------------------------------------------------------------------------

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_SOCKET_TIMEOUT = 5.0
_SOCKET_CONNECT_TIMEOUT = 2.0
_MAX_CONNECTIONS = 10


def _get_redis_url() -> str:
    """Redis URL from REDIS_URL env or localhost default."""
    return os.environ.get("REDIS_URL", _DEFAULT_REDIS_URL)


def _default_pool() -> ConnectionPool:
    """Shared connection pool for local-dev; reasonable timeouts and max connections."""
    return ConnectionPool.from_url(
        _get_redis_url(),
        socket_timeout=_SOCKET_TIMEOUT,
        socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT,
        max_connections=_MAX_CONNECTIONS,
        decode_responses=True,
    )


_pool: ConnectionPool | None = None


def _get_pool() -> ConnectionPool:
    """Lazy singleton pool so callers can use get_default_* without managing Redis details."""
    global _pool
    if _pool is None:
        _pool = _default_pool()
    return _pool


class RedisFeatureStore:
    """FeatureStores implementation using Redis; keys follow feature_key_user / feature_key_tx."""

    def __init__(self, client: redis.Redis | None = None) -> None:
        self._client = client or redis.Redis(connection_pool=_get_pool())

    def get_features(self, key: str) -> dict[str, Any] | None:
        raw = self._client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def set_features(self, key: str, features: Mapping[str, Any]) -> None:
        self._client.set(key, json.dumps(dict(features)))


class RedisStatefulStore(StatefulStore):
    """StatefulStore implementation using Redis; values stored as JSON."""

    def __init__(self, client: redis.Redis | None = None) -> None:
        self._client = client or redis.Redis(connection_pool=_get_pool())

    def get(self, key: str) -> dict[str, Any] | None:
        raw = self._client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def set(self, key: str, value: Mapping[str, Any] | None) -> None:
        if value is None:
            self._client.delete(key)
            return
        self._client.set(key, json.dumps(dict(value)))


def get_default_feature_store() -> FeatureStores:
    """Return a FeatureStores instance (Redis-backed) without exposing Redis details."""
    return RedisFeatureStore()


def get_default_stateful_store() -> StatefulStore:
    """Return a StatefulStore instance (Redis-backed) without exposing Redis details."""
    return RedisStatefulStore()
