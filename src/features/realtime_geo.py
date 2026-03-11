from __future__ import annotations

"""
Geo-anomaly state and feature computation for the RealTimeFeatProcessor.

This module defines a per-user last-known location table and a Faust agent that
consumes ingestion events, computes distance-over-time between consecutive
locations, and emits geo-anomaly feature events on the shared features topic.

Geo features exposed:
  - geo_distance_km: great-circle distance from previous location (kilometers)
  - geo_dt_minutes: time delta between events (minutes)
  - geo_anomaly: True when distance is large while time delta is small
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

import faust
from haversine import Unit, haversine

from .real_time_feat_processor import app, features_topic, ingestion_topic
from .realtime_config import (
    GEO_ANOMALY_MAX_DISTANCE_KM,
    GEO_ANOMALY_MAX_TIME_DELTA_SECONDS,
)
from .realtime_models import FeatureEvent, IngestionEvent


@dataclass
class LastLocation:
    """Last known location for a user."""

    lat: float
    lon: float
    timestamp: datetime


# Table keyed by user_id storing their last known location and event timestamp.
last_location: faust.Table[str, Optional[LastLocation]] = app.Table(
    "last_location",
    default=lambda: None,
)


def _parse_location(location: str) -> Optional[Tuple[float, float]]:
    """Parse a 'lat,lon' string into a coordinate pair.

    If the format is invalid, returns None so the caller can skip geo features.
    """

    if not location:
        return None

    parts = [p.strip() for p in location.split(",", maxsplit=1)]
    if len(parts) != 2:
        return None

    try:
        lat = float(parts[0])
        lon = float(parts[1])
    except ValueError:
        return None

    return lat, lon


def _parse_timestamp(ts: str) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp string into a datetime."""

    if not ts:
        return None

    # Accept common "Z" suffix by normalizing to "+00:00".
    normalized = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


@app.agent(ingestion_topic)
async def geo_anomaly_agent(
    events: faust.Stream[IngestionEvent],
) -> None:
    """Compute geo-anomaly features for users with location-bearing events."""

    async for event in events:
        coords = _parse_location(event.location)
        current_ts = _parse_timestamp(event.timestamp)

        # Skip events that do not contain a parseable location or timestamp.
        if coords is None or current_ts is None:
            continue

        lat, lon = coords
        previous = last_location[event.user_id]

        distance_km: Optional[float] = None
        dt_minutes: Optional[float] = None
        geo_anomaly = False

        if previous is not None:
            # Compute great-circle distance and time delta.
            distance_km = haversine(
                (previous.lat, previous.lon),
                (lat, lon),
                unit=Unit.KILOMETERS,
            )
            dt_seconds = (current_ts - previous.timestamp).total_seconds()
            dt_minutes = dt_seconds / 60.0 if dt_seconds >= 0 else None

            if (
                distance_km is not None
                and dt_minutes is not None
                and distance_km > GEO_ANOMALY_MAX_DISTANCE_KM
                and dt_seconds < GEO_ANOMALY_MAX_TIME_DELTA_SECONDS
            ):
                geo_anomaly = True

        # Update per-user last-known location state.
        last_location[event.user_id] = LastLocation(
            lat=lat,
            lon=lon,
            timestamp=current_ts,
        )

        # Emit feature event only when we have a previous point to compare
        # against, so distance and dt are meaningful.
        if distance_km is not None and dt_minutes is not None:
            feature = FeatureEvent(
                user_id=event.user_id,
                timestamp=event.timestamp,
                features={
                    "geo_distance_km": distance_km,
                    "geo_dt_minutes": dt_minutes,
                    "geo_anomaly": geo_anomaly,
                },
                metadata={"source": "geo_anomaly"},
            )
            await features_topic.send(value=feature)

