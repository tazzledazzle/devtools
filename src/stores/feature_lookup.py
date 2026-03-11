"""
Python and HTTP feature lookup API for online scoring.

Provides:
- get_features_for_id(user_id, tx_id): in-process Python function to fetch a feature
  vector from the configured FeatureStores (Redis-backed by default). Use this from
  model-serving or scoring code when running in the same process as the store.
- FastAPI app (app): minimal HTTP service exposing GET /features/by-user/{user_id}
  and GET /features/by-tx/{tx_id}. A future ModelServer can run this via
  uvicorn (e.g. uvicorn stores.feature_lookup:app) or mount it as a sub-application.

Configuration: Redis is configured via REDIS_URL (see stores.stores). Keyspace follows
features:user:{id} and features:tx:{id}.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Query

from stores.stores import (
    feature_key_tx,
    feature_key_user,
    get_default_feature_store,
)


def get_features_for_id(
    user_id: str | None = None,
    tx_id: str | None = None,
    *,
    store: Any = None,
) -> dict[str, Any] | None:
    """
    Fetch the feature vector for the given user_id and/or tx_id.

    Lookup key is resolved as: tx_id when provided (features:tx:{tx_id}), otherwise
    user_id (features:user:{user_id}). At least one of user_id or tx_id must be
    provided.

    Args:
        user_id: Optional user identifier.
        tx_id: Optional transaction/authorization identifier (takes precedence).
        store: Optional FeatureStores instance; if None, uses get_default_feature_store().

    Returns:
        Feature vector dict, or None if no features are stored for the resolved key.

    Raises:
        ValueError: If neither user_id nor tx_id is provided.
    """
    if not tx_id and not user_id:
        raise ValueError("At least one of user_id or tx_id must be provided")
    if store is None:
        store = get_default_feature_store()
    key = feature_key_tx(tx_id) if tx_id else feature_key_user(user_id or "")
    return store.get_features(key)


# ---------------------------------------------------------------------------
# FastAPI app for HTTP feature lookup (for future ModelServer / external callers)
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Feature Lookup API",
    description="Thin HTTP API to fetch feature vectors by user_id or tx_id for online scoring.",
    version="0.1.0",
)


@app.get("/features/by-user/{user_id}")
def get_features_by_user(user_id: str) -> dict[str, Any]:
    """
    Return the feature vector for the given user_id (key: features:user:{user_id}).
    Returns 404 if no features are found.
    """
    features = get_features_for_id(user_id=user_id)
    if features is None:
        raise HTTPException(status_code=404, detail="No features found for user_id")
    return features


@app.get("/features/by-tx/{tx_id}")
def get_features_by_tx(tx_id: str) -> dict[str, Any]:
    """
    Return the feature vector for the given transaction/authorization id
    (key: features:tx:{tx_id}). Returns 404 if no features are found.
    """
    features = get_features_for_id(tx_id=tx_id)
    if features is None:
        raise HTTPException(status_code=404, detail="No features found for tx_id")
    return features


@app.get("/features")
def get_features_query(
    user_id: str | None = Query(None, description="User identifier"),
    tx_id: str | None = Query(None, description="Transaction/authorization identifier"),
) -> dict[str, Any]:
    """
    Return the feature vector for the given user_id and/or tx_id (query params).
    Prefer tx_id when both are provided. Returns 400 if neither is provided, 404 if not found.
    """
    if not user_id and not tx_id:
        raise HTTPException(
            status_code=400,
            detail="At least one of user_id or tx_id query parameter is required",
        )
    features = get_features_for_id(user_id=user_id, tx_id=tx_id)
    if features is None:
        raise HTTPException(status_code=404, detail="No features found")
    return features
