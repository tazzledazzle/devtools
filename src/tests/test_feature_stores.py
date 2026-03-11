"""Contract tests for FeatureStores and StatefulStore abstractions and Redis-backed implementations."""

import json
import unittest
from unittest.mock import MagicMock, patch

from stores.stores import (
    StatefulStore,
    feature_key_tx,
    feature_key_user,
    get_default_feature_store,
    get_default_stateful_store,
    RedisFeatureStore,
    RedisStatefulStore,
)


# ---------------------------------------------------------------------------
# In-memory fakes (no Redis required)
# ---------------------------------------------------------------------------


class InMemoryFeatureStore:
    """In-memory FeatureStores implementation for tests."""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def get_features(self, key: str) -> dict | None:
        raw = self._data.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def set_features(self, key: str, features: dict) -> None:
        self._data[key] = json.dumps(dict(features))


class InMemoryStatefulStore(StatefulStore):
    """In-memory StatefulStore for tests."""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def get(self, key: str) -> dict | None:
        raw = self._data.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def set(self, key: str, value: dict | None) -> None:
        if value is None:
            self._data.pop(key, None)
            return
        self._data[key] = json.dumps(dict(value))


# ---------------------------------------------------------------------------
# Key conventions
# ---------------------------------------------------------------------------


class TestKeyConventions(unittest.TestCase):
    def test_feature_key_user_format(self) -> None:
        self.assertEqual(feature_key_user("u42"), "features:user:u42")

    def test_feature_key_tx_format(self) -> None:
        self.assertEqual(feature_key_tx("tx-abc"), "features:tx:tx-abc")


# ---------------------------------------------------------------------------
# FeatureStores contract (in-memory fake)
# ---------------------------------------------------------------------------


class TestFeatureStoresContract(unittest.TestCase):
    def test_set_get_round_trip_preserves_features(self) -> None:
        store = InMemoryFeatureStore()
        key = feature_key_user("u1")
        features = {"velocity_1m": 5.0, "geo_anomaly": True}
        store.set_features(key, features)
        self.assertEqual(store.get_features(key), features)

    def test_get_missing_returns_none(self) -> None:
        store = InMemoryFeatureStore()
        self.assertIsNone(store.get_features("features:user:nonexistent"))

    def test_set_overwrites(self) -> None:
        store = InMemoryFeatureStore()
        key = feature_key_tx("tx-1")
        store.set_features(key, {"a": 1})
        store.set_features(key, {"b": 2})
        self.assertEqual(store.get_features(key), {"b": 2})

    def test_user_and_tx_keys_independent(self) -> None:
        store = InMemoryFeatureStore()
        store.set_features(feature_key_user("u1"), {"for_user": 1})
        store.set_features(feature_key_tx("tx-1"), {"for_tx": 2})
        self.assertEqual(store.get_features(feature_key_user("u1")), {"for_user": 1})
        self.assertEqual(store.get_features(feature_key_tx("tx-1")), {"for_tx": 2})


# ---------------------------------------------------------------------------
# StatefulStore contract (in-memory fake)
# ---------------------------------------------------------------------------


class TestStatefulStoreContract(unittest.TestCase):
    def test_set_get_round_trip(self) -> None:
        store = InMemoryStatefulStore()
        store.set("state:foo", {"x": 1, "y": "z"})
        self.assertEqual(store.get("state:foo"), {"x": 1, "y": "z"})

    def test_get_missing_returns_none(self) -> None:
        store = InMemoryStatefulStore()
        self.assertIsNone(store.get("state:missing"))

    def test_set_none_clears_key(self) -> None:
        store = InMemoryStatefulStore()
        store.set("state:foo", {"a": 1})
        store.set("state:foo", None)
        self.assertIsNone(store.get("state:foo"))


# ---------------------------------------------------------------------------
# Redis-backed implementations (mocked client)
# ---------------------------------------------------------------------------


class TestRedisFeatureStore(unittest.TestCase):
    def test_redis_feature_store_uses_json_round_trip(self) -> None:
        # In-memory dict as fake Redis: set(key, str) / get(key) -> str
        fake: dict[str, str] = {}
        client = MagicMock()
        client.get.side_effect = lambda k: fake.get(k)
        client.set.side_effect = lambda k, v: fake.__setitem__(k, v)

        store = RedisFeatureStore(client=client)
        key = feature_key_user("u1")
        features = {"v": 1.0, "flag": True}
        store.set_features(key, features)
        self.assertEqual(store.get_features(key), features)
        client.set.assert_called_once()
        self.assertEqual(json.loads(fake[key]), features)

    def test_redis_feature_store_connection_error_surfaces(self) -> None:
        client = MagicMock()
        client.get.side_effect = ConnectionError("connection refused")
        store = RedisFeatureStore(client=client)
        with self.assertRaises(ConnectionError):
            store.get_features("features:user:u1")


class TestRedisStatefulStore(unittest.TestCase):
    def test_redis_stateful_store_set_get_round_trip(self) -> None:
        fake: dict[str, str] = {}
        client = MagicMock()
        client.get.side_effect = lambda k: fake.get(k)
        client.set.side_effect = lambda k, v: fake.__setitem__(k, v)
        client.delete.side_effect = lambda k: fake.pop(k, None)

        store = RedisStatefulStore(client=client)
        store.set("state:k", {"a": 1})
        self.assertEqual(store.get("state:k"), {"a": 1})
        store.set("state:k", None)
        self.assertIsNone(store.get("state:k"))

    def test_redis_stateful_store_timeout_surfaces(self) -> None:
        client = MagicMock()
        client.get.side_effect = TimeoutError("timeout")
        store = RedisStatefulStore(client=client)
        with self.assertRaises(TimeoutError):
            store.get("state:any")


# ---------------------------------------------------------------------------
# Default factory (ensure they return correct types)
# ---------------------------------------------------------------------------


class TestDefaultStores(unittest.TestCase):
    @patch("stores.stores._get_pool")
    def test_get_default_feature_store_returns_feature_store(self, mock_pool: MagicMock) -> None:
        mock_pool.return_value = MagicMock()
        store = get_default_feature_store()
        self.assertIsInstance(store, RedisFeatureStore)

    @patch("stores.stores._get_pool")
    def test_get_default_stateful_store_returns_stateful_store(self, mock_pool: MagicMock) -> None:
        mock_pool.return_value = MagicMock()
        store = get_default_stateful_store()
        self.assertIsInstance(store, RedisStatefulStore)
