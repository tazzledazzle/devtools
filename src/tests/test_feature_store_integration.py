"""
Integration tests for RealTimeFeatProcessor writing to FeatureStores/StatefulStore.

Uses an in-memory fake store so tests run without Redis. Verifies that the
processor writes feature vectors under the expected user_id and tx_id keys.
"""

import json
import unittest

from features.feat_processor_store import RealTimeFeatProcessor
from stores.stores import feature_key_tx, feature_key_user


# ---------------------------------------------------------------------------
# In-memory fake (conforms to FeatureStores; no Redis)
# ---------------------------------------------------------------------------


class FakeFeatureStore:
    """Minimal FeatureStores implementation for integration tests."""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def get_features(self, key: str) -> dict | None:
        raw = self._data.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    def set_features(self, key: str, features: dict) -> None:
        self._data[key] = json.dumps(dict(features))


# ---------------------------------------------------------------------------
# Processor-to-store wiring
# ---------------------------------------------------------------------------


class TestRealTimeFeatProcessorStoreIntegration(unittest.TestCase):
    """Verify RealTimeFeatProcessor writes to a store under expected keys."""

    def test_persist_features_writes_under_user_key(self) -> None:
        fake = FakeFeatureStore()
        processor = RealTimeFeatProcessor(feature_store=fake)
        processor.persist_features(
            user_id="u42",
            tx_id="",
            features={"velocity_1m": 3.0, "velocity_1h": 10.0},
        )
        key = feature_key_user("u42")
        self.assertEqual(
            fake.get_features(key),
            {"velocity_1m": 3.0, "velocity_1h": 10.0},
        )

    def test_persist_features_writes_under_user_and_tx_keys_when_tx_id_non_empty(
        self,
    ) -> None:
        fake = FakeFeatureStore()
        processor = RealTimeFeatProcessor(feature_store=fake)
        processor.persist_features(
            user_id="u1",
            tx_id="tx-abc",
            features={"velocity_1m": 1.0, "is_new_device": True},
        )
        self.assertEqual(
            fake.get_features(feature_key_user("u1")),
            {"velocity_1m": 1.0, "is_new_device": True},
        )
        self.assertEqual(
            fake.get_features(feature_key_tx("tx-abc")),
            {"velocity_1m": 1.0, "is_new_device": True},
        )

    def test_persist_features_overwrites_user_key_on_repeated_call(self) -> None:
        fake = FakeFeatureStore()
        processor = RealTimeFeatProcessor(feature_store=fake)
        processor.persist_features("u1", "", {"a": 1})
        processor.persist_features("u1", "", {"a": 2, "b": 3})
        self.assertEqual(fake.get_features(feature_key_user("u1")), {"a": 2, "b": 3})

    def test_processor_accepts_optional_stateful_store(self) -> None:
        fake = FakeFeatureStore()
        processor = RealTimeFeatProcessor(
            feature_store=fake,
            stateful_store=None,
        )
        processor.persist_features("u1", "tx-1", {"x": 1})
        self.assertEqual(fake.get_features(feature_key_user("u1")), {"x": 1})
        self.assertEqual(fake.get_features(feature_key_tx("tx-1")), {"x": 1})
