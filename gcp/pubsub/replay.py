from __future__ import annotations

from datetime import datetime, timezone

from google.cloud import pubsub_v1


def seek_subscription_to_timestamp(project_id: str, subscription_id: str, timestamp: datetime) -> None:
    """Operational replay helper. Requires explicit caller IAM and should be audited."""
    if timestamp.tzinfo is None:
        raise ValueError("Replay timestamp must be timezone-aware")
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    subscriber.seek(
        request={
            "subscription": subscription_path,
            "time": timestamp.astimezone(timezone.utc),
        }
    )
