"""
Store integration surface for the real-time feature processor.

This module defines RealTimeFeatProcessor with no Faust dependency so that
integration tests and other callers can use the processor without importing
the streaming runtime. The main real_time_feat_processor module re-exports
this class for use from Faust agents.
"""

from __future__ import annotations

from typing import Any, Mapping

from stores.stores import (
    FeatureStores,
    StatefulStore,
    feature_key_tx,
    feature_key_user,
)


class RealTimeFeatProcessor:
    """
    Integration surface for persisting computed features into a feature store.

    Accepts FeatureStores and optional StatefulStore via constructor so that
    the streaming layer can write feature vectors without depending on Redis
    or other backend details. Intended for use from Faust agents (or tests)
    that have user_id, optional tx_id, and a features dict to persist.

    Key usage:
    - User-scoped features are always written under features:user:{user_id}.
    - Transaction-scoped features are written under features:tx:{tx_id} when
      tx_id is non-empty (e.g. authorization ID from the event or metadata).
    """

    def __init__(
        self,
        feature_store: FeatureStores,
        stateful_store: StatefulStore | None = None,
    ) -> None:
        self._feature_store = feature_store
        self._stateful_store = stateful_store

    def persist_features(
        self,
        user_id: str,
        tx_id: str,
        features: Mapping[str, Any],
    ) -> None:
        """
        Write the computed feature vector into the store under user and optionally tx keys.

        - Always writes to the user key (features:user:{user_id}) so online scoring
          can look up by user_id.
        - If tx_id is non-empty, also writes to the transaction key (features:tx:{tx_id})
          for authorization-level lookup.
        """
        key_user = feature_key_user(user_id)
        self._feature_store.set_features(key_user, dict(features))
        if tx_id:
            key_tx = feature_key_tx(tx_id)
            self._feature_store.set_features(key_tx, dict(features))
