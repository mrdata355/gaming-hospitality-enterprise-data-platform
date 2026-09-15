from __future__ import annotations

import base64
import json
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from resort_platform.models.events import (
    HotelReservationEvent,
    MobileAppEvent,
    RewardActivityEvent,
    SlotPlayEvent,
    TablePlayEvent,
)
from resort_platform.quality import QualityEngine, no_plaintext_sensitive_fields, required
from resort_platform.storage.medallion import MedallionStorage

app = FastAPI(title="Gaming Hospitality PubSub Consumer", version="1.0.0")

MODELS = {
    "SLOT_PLAY": SlotPlayEvent,
    "TABLE_PLAY": TablePlayEvent,
    "HOTEL_RESERVATION": HotelReservationEvent,
    "REWARD_ACTIVITY": RewardActivityEvent,
    "MOBILE_APP": MobileAppEvent,
}
QUALITY = QualityEngine(
    [required("event_id", "event_type", "event_ts"), no_plaintext_sensitive_fields()]
)


class PushMessage(BaseModel):
    data: str
    attributes: dict[str, str] = {}
    messageId: str | None = None
    publishTime: str | None = None


class PushEnvelope(BaseModel):
    message: PushMessage
    subscription: str | None = None


def decode_payload(envelope: PushEnvelope) -> dict[str, Any]:
    try:
        return json.loads(base64.b64decode(envelope.message.data).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid Pub/Sub payload: {exc}") from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "pubsub-consumer"}


@app.post("/v1/pubsub/push", status_code=204)
async def consume(request: Request) -> None:
    envelope = PushEnvelope.model_validate(await request.json())
    payload = decode_payload(envelope)
    event_type = str(payload.get("event_type", "")).upper()
    model = MODELS.get(event_type)
    if not model:
        raise HTTPException(status_code=422, detail=f"Unsupported event_type={event_type!r}")
    event = model.model_validate(payload)
    normalized = event.model_dump(mode="json")
    result = QUALITY.evaluate(normalized)
    storage = MedallionStorage()
    business_key = str(
        normalized.get("session_id")
        or normalized.get("reservation_id")
        or normalized.get("guest_token")
        or normalized["event_id"]
    )
    domain = event_type.split("_")[0].lower()
    if not result.valid:
        quarantine = dict(normalized)
        quarantine["dq_violations"] = [v.__dict__ for v in result.violations]
        storage.write_json(
            layer="quarantine",
            domain=domain,
            entity=event_type.lower(),
            business_key=business_key,
            payload=quarantine,
        )
        raise HTTPException(status_code=422, detail="Data-quality validation failed")
    storage.write_json(
        layer="bronze",
        domain=domain,
        entity=event_type.lower(),
        business_key=business_key,
        payload=normalized,
    )
