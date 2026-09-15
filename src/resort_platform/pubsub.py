from __future__ import annotations

import json
from collections.abc import Iterable
from concurrent.futures import wait
from typing import Any

from google.cloud import pubsub_v1

from resort_platform.config import get_settings


def publish_events(
    events: Iterable[dict[str, Any]],
    *,
    topic_id: str,
    ordering_key_field: str | None = None,
) -> int:
    """Publish contract-valid events with stable attributes and optional ordering keys."""

    settings = get_settings()
    publisher = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=bool(ordering_key_field))
    )
    topic_path = publisher.topic_path(settings.gcp_project_id, topic_id)
    futures = []

    for event in events:
        payload = json.dumps(event, default=str, separators=(",", ":")).encode("utf-8")
        attributes = {
            "event_id": str(event["event_id"]),
            "event_type": str(event["event_type"]),
            "schema_version": str(event.get("schema_version", "1")),
            "property_code": str(event.get("property_code", "UNKNOWN")),
            "trace_id": str(event.get("trace_id", "")),
        }
        ordering_key = str(event.get(ordering_key_field, "")) if ordering_key_field else ""
        futures.append(publisher.publish(topic_path, payload, ordering_key=ordering_key, **attributes))

    wait(futures, timeout=settings.publish_timeout_seconds)
    failures = [future.exception() for future in futures if future.exception()]
    if failures:
        raise RuntimeError(f"Pub/Sub publish failed for {len(failures)} event(s): {failures[0]}")
    return len(futures)
