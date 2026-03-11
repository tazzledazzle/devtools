from __future__ import annotations

import os
from typing import Final

"""
Configuration for the real-time feature processing stack.

This module centralizes Kafka broker settings, topic names, processing
guarantees, window durations, and basic geo/device thresholds so that both the
ingestion pipeline and the RealTimeFeatProcessor can share a single source of
truth.

Values are loaded from environment variables with sensible local defaults so
that developers can run the stack without additional configuration while still
allowing production deployments to override behavior.
"""


def _get_env(name: str, default: str) -> str:
    """Fetch an environment variable with a string default."""
    return os.getenv(name, default)


# Kafka broker configuration
KAFKA_BROKER_URL: Final[str] = _get_env("KAFKA_BROKER_URL", "localhost:9092")


# Topic names shared between ingestion and real-time features
INGESTION_TOPIC: Final[str] = _get_env("INGESTION_TOPIC", "ingestion-events")
FEATURES_TOPIC: Final[str] = _get_env("FEATURES_TOPIC", "feature-events")


# Processing guarantees (start with at-least-once semantics)
PROCESSING_GUARANTEE: Final[str] = _get_env(
    "REALTIME_PROCESSING_GUARANTEE",
    "at_least_once",
)


# Velocity window sizes (in seconds)
VELOCITY_SHORT_WINDOW_SECONDS: Final[int] = int(
    _get_env("VELOCITY_SHORT_WINDOW_SECONDS", "60")
)
VELOCITY_LONG_WINDOW_SECONDS: Final[int] = int(
    _get_env("VELOCITY_LONG_WINDOW_SECONDS", "3600")
)


# Geo-anomaly thresholds
GEO_ANOMALY_MAX_DISTANCE_KM: Final[float] = float(
    _get_env("GEO_ANOMALY_MAX_DISTANCE_KM", "500.0")
)
GEO_ANOMALY_MAX_TIME_DELTA_SECONDS: Final[int] = int(
    _get_env("GEO_ANOMALY_MAX_TIME_DELTA_SECONDS", "3600")
)


# Device-related thresholds / flags
NEW_DEVICE_LOOKBACK_HOURS: Final[int] = int(
    _get_env("NEW_DEVICE_LOOKBACK_HOURS", "24")
)

# Cap the number of devices tracked per user to keep table state bounded.
MAX_DEVICES_PER_USER: Final[int] = int(
    _get_env("MAX_DEVICES_PER_USER", "20")
)

