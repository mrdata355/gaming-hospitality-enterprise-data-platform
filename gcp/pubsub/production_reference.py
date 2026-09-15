"""Pub/Sub publisher/consumer reference for generated gaming/hospitality events.

Patterns included:
- stable event IDs
- schema/version attributes
- ordering keys
- synchronous and batch publishing
- push-envelope decoding
- idempotency
- retry with exponential backoff
- dead-letter naming
- topic/subscription catalog
- handler routing
- replay metadata

Synthetic portfolio code only; not private Venetian infrastructure.
"""
from __future__ import annotations

import base64
import dataclasses
import datetime as dt
import hashlib
import json
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

LOG = logging.getLogger(__name__)

TOPIC_CATALOG = {
    "guest.mobile-events.v1": {
        "domain": "guest",
        "dlq": "guest.mobile-events.v1.dlq",
        "schema_version": "1",
    },
    "hotel.reservation-events.v1": {
        "domain": "hotel",
        "dlq": "hotel.reservation-events.v1.dlq",
        "schema_version": "1",
    },
    "hotel.checkin-events.v1": {
        "domain": "hotel",
        "dlq": "hotel.checkin-events.v1.dlq",
        "schema_version": "1",
    },
    "gaming.slot-play-events.v1": {
        "domain": "gaming",
        "dlq": "gaming.slot-play-events.v1.dlq",
        "schema_version": "1",
    },
    "gaming.table-play-events.v1": {
        "domain": "gaming",
        "dlq": "gaming.table-play-events.v1.dlq",
        "schema_version": "1",
    },
    "rewards.earn-events.v1": {
        "domain": "rewards",
        "dlq": "rewards.earn-events.v1.dlq",
        "schema_version": "1",
    },
    "rewards.redemption-events.v1": {
        "domain": "rewards",
        "dlq": "rewards.redemption-events.v1.dlq",
        "schema_version": "1",
    },
    "marketing.offer-events.v1": {
        "domain": "marketing",
        "dlq": "marketing.offer-events.v1.dlq",
        "schema_version": "1",
    },
    "property.transaction-events.v1": {
        "domain": "payments",
        "dlq": "property.transaction-events.v1.dlq",
        "schema_version": "1",
    },
    "ops.pipeline-events.v1": {
        "domain": "observability",
        "dlq": "ops.pipeline-events.v1.dlq",
        "schema_version": "1",
    },
}


@dataclasses.dataclass(frozen=True)
class Message:
    event_id: str
    topic: str
    property_code: str
    event_time: str
    payload: Mapping[str, Any]
    schema_version: str = "1"
    trace_id: str | None = None
    replay_id: str | None = None

    def json_bytes(self) -> bytes:
        return json.dumps(
            dataclasses.asdict(self),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    def attributes(self) -> dict[str, str]:
        attrs = {
            "event_id": self.event_id,
            "property_code": self.property_code,
            "schema_version": self.schema_version,
            "trace_id": self.trace_id or self.event_id,
        }
        if self.replay_id:
            attrs["replay_id"] = self.replay_id
        return attrs

    def ordering_key(self) -> str:
        key = self.payload.get("business_key") or self.event_id
        return f"{self.property_code}:{key}"


class PublishError(RuntimeError):
    pass


class ContractError(ValueError):
    pass


def stable_event_id(
    topic: str,
    property_code: str,
    business_key: str,
    version: int,
) -> str:
    raw = f"{topic}|{property_code}|{business_key}|{version}".encode()
    return hashlib.sha256(raw).hexdigest()[:32].upper()


def validate_message(message: Message) -> list[str]:
    errors: list[str] = []
    if message.topic not in TOPIC_CATALOG:
        errors.append("UNKNOWN_TOPIC")
    if not message.event_id:
        errors.append("EVENT_ID_REQUIRED")
    if not message.property_code:
        errors.append("PROPERTY_REQUIRED")
    expected = TOPIC_CATALOG.get(message.topic, {}).get("schema_version")
    if expected and message.schema_version != expected:
        errors.append("SCHEMA_VERSION_UNSUPPORTED")
    if not isinstance(message.payload, Mapping):
        errors.append("PAYLOAD_NOT_OBJECT")
    return errors


def publish_sync(
    publisher: Any,
    project_id: str,
    message: Message,
    timeout: float = 20.0,
) -> str:
    errors = validate_message(message)
    if errors:
        raise ContractError(",".join(errors))
    topic_path = publisher.topic_path(project_id, message.topic)
    future = publisher.publish(
        topic_path,
        message.json_bytes(),
        ordering_key=message.ordering_key(),
        **message.attributes(),
    )
    try:
        return str(future.result(timeout=timeout))
    except Exception as exc:
        raise PublishError(str(exc)) from exc


def publish_many(
    publisher: Any,
    project_id: str,
    messages: Iterable[Message],
    timeout: float = 30.0,
) -> list[str]:
    futures = []
    for message in messages:
        errors = validate_message(message)
        if errors:
            raise ContractError(f"{message.event_id}:{errors}")
        topic_path = publisher.topic_path(project_id, message.topic)
        futures.append(
            publisher.publish(
                topic_path,
                message.json_bytes(),
                ordering_key=message.ordering_key(),
                **message.attributes(),
            )
        )
    return [str(f.result(timeout=timeout)) for f in futures]


def decode_push_envelope(
    body: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    outer = body.get("message") or {}
    raw = base64.b64decode(outer.get("data", ""))
    payload = json.loads(raw.decode())
    attrs = {
        str(k): str(v)
        for k, v in (outer.get("attributes") or {}).items()
    }
    return payload, attrs


class IdempotencyStore:
    def __init__(self) -> None:
        self._seen: set[str] = set()

    def first_seen(self, event_id: str) -> bool:
        if event_id in self._seen:
            return False
        self._seen.add(event_id)
        return True


class Router:
    def __init__(
        self,
        handlers: Mapping[str, Callable[[dict[str, Any]], Any]],
    ) -> None:
        self.handlers = dict(handlers)

    def route(self, topic: str, payload: dict[str, Any]) -> Any:
        handler = self.handlers.get(topic)
        if handler is None:
            raise KeyError(f"no handler for {topic}")
        return handler(payload)


def process_push(
    body: Mapping[str, Any],
    store: IdempotencyStore,
    router: Router,
) -> dict[str, Any]:
    payload, attrs = decode_push_envelope(body)
    event_id = str(attrs.get("event_id") or payload.get("event_id") or "")
    topic = str(payload.get("topic") or attrs.get("topic") or "")
    if not event_id:
        return {"status": "quarantine", "reason": "EVENT_ID_REQUIRED"}
    if not store.first_seen(event_id):
        return {"status": "duplicate_ignored", "event_id": event_id}
    try:
        result = router.route(topic, payload)
        return {"status": "processed", "event_id": event_id, "result": result}
    except Exception as exc:
        LOG.exception("event processing failed")
        return {"status": "retry", "event_id": event_id, "error": str(exc)}


def exponential_backoff(
    attempt: int,
    base: float = 0.25,
    cap: float = 30.0,
) -> float:
    return min(cap, base * (2 ** max(0, attempt - 1)))


def retry(operation: Callable[[], Any], attempts: int = 5) -> Any:
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            last = exc
            if attempt == attempts:
                break
            time.sleep(exponential_backoff(attempt))
    assert last is not None
    raise last


def dead_letter_topic(topic: str) -> str:
    return str(TOPIC_CATALOG[topic]["dlq"])


def subscription_name(topic: str, environment: str) -> str:
    token = topic.replace(".", "-")
    return f"{environment}-{token}-consumer-v1"


def build_message(
    topic: str,
    property_code: str,
    business_key: str,
    version: int,
    payload: Mapping[str, Any],
    replay_id: str | None = None,
) -> Message:
    return Message(
        event_id=stable_event_id(topic, property_code, business_key, version),
        topic=topic,
        property_code=property_code,
        event_time=dt.datetime.now(dt.UTC).isoformat(),
        schema_version=str(TOPIC_CATALOG[topic]["schema_version"]),
        trace_id=f"trace-{business_key}-{version}",
        replay_id=replay_id,
        payload={
            "business_key": business_key,
            "source_version": version,
            **dict(payload),
        },
    )


def build_mobile_event(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("guest.mobile-events.v1", property_code, key, version, payload)


def build_reservation_event(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("hotel.reservation-events.v1", property_code, key, version, payload)


def build_checkin_event(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("hotel.checkin-events.v1", property_code, key, version, payload)


def build_slot_event(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("gaming.slot-play-events.v1", property_code, key, version, payload)


def build_table_event(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("gaming.table-play-events.v1", property_code, key, version, payload)


def build_reward_earn(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("rewards.earn-events.v1", property_code, key, version, payload)


def build_reward_redemption(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("rewards.redemption-events.v1", property_code, key, version, payload)


def build_offer_event(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("marketing.offer-events.v1", property_code, key, version, payload)


def build_transaction_event(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("property.transaction-events.v1", property_code, key, version, payload)


def build_pipeline_event(property_code: str, key: str, version: int, payload: Mapping[str, Any]) -> Message:
    return build_message("ops.pipeline-events.v1", property_code, key, version, payload)


def replay(message: Message, replay_id: str) -> Message:
    return dataclasses.replace(message, replay_id=replay_id)


def demo_messages() -> list[Message]:
    return [
        build_slot_event(
            "VENETIAN_MAIN",
            "PLAY-1001",
            1,
            {"machine_id": "M-DEMO-99", "coin_in": 125.0, "payout": 87.5},
        ),
        build_reservation_event(
            "PALAZZO",
            "RES-2001",
            1,
            {"status": "CONFIRMED", "rooms": 1},
        ),
    ]
