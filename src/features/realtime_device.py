from __future__ import annotations

from typing import Set

from .real_time_feat_processor import app, features_topic, ingestion_topic
from .realtime_config import MAX_DEVICES_PER_USER
from .realtime_models import FeatureEvent, IngestionEvent


known_devices = app.Table("known_devices", default=set)


@app.agent(ingestion_topic)
async def flag_new_device(events: ingestion_topic.stream[IngestionEvent]) -> None:
    async for event in events:
        user_id = event.user_id
        device_id = event.device_id

        devices: Set[str] = known_devices[user_id]
        is_new_device = device_id not in devices

        if is_new_device:
            updated_devices = set(devices)
            if len(updated_devices) >= MAX_DEVICES_PER_USER and updated_devices:
                # Simple pruning strategy: drop an arbitrary device to keep state bounded.
                updated_devices.pop()

            updated_devices.add(device_id)
            known_devices[user_id] = updated_devices

        feature_event = FeatureEvent(
            user_id=user_id,
            timestamp=event.timestamp,
            features={
                "device_id": device_id,
                "is_new_device": is_new_device,
            },
        )

        await features_topic.send(
            key=user_id,
            value=feature_event,
        )

