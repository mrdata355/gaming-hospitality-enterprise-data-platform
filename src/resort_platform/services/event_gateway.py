from __future__ import annotations

from typing import Annotated, Literal

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from resort_platform.models.events import (
    HotelReservationEvent,
    MobileAppEvent,
    RewardActivityEvent,
    SlotPlayEvent,
    TablePlayEvent,
)
from resort_platform.pubsub import publish_events
from resort_platform.storage.medallion import MedallionStorage

app = FastAPI(title="Gaming Hospitality Event Gateway", version="1.0.0")

TOPICS = {
    "SLOT_PLAY": "gaming.slot-play-events.v1",
    "TABLE_PLAY": "gaming.table-play-events.v1",
    "HOTEL_RESERVATION": "hotel.reservation-events.v1",
    "REWARD_ACTIVITY": "rewards.earn-events.v1",
    "MOBILE_APP": "guest.mobile-events.v1",
}
MODELS = {
    "SLOT_PLAY": SlotPlayEvent,
    "TABLE_PLAY": TablePlayEvent,
    "HOTEL_RESERVATION": HotelReservationEvent,
    "REWARD_ACTIVITY": RewardActivityEvent,
    "MOBILE_APP": MobileAppEvent,
}


class GatewayResponse(BaseModel):
    event_id: str
    topic: str
    landed_object: str
    status: Literal["ACCEPTED"] = "ACCEPTED"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "event-gateway"}


@app.post("/v1/events/{event_type}", response_model=GatewayResponse)
def ingest_event(
    event_type: str,
    body: dict,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> GatewayResponse:
    normalized = event_type.upper()
    model = MODELS.get(normalized)
    if not model:
        raise HTTPException(status_code=404, detail="Unsupported event type")
    event = model.model_validate(body)
    payload = event.model_dump(mode="json")
    if idempotency_key and idempotency_key != str(event.event_id):
        raise HTTPException(status_code=409, detail="Idempotency key must match event_id")

    business_key = str(
        payload.get("reservation_id")
        or payload.get("session_id")
        or payload.get("guest_token")
        or payload["event_id"]
    )
    landed = MedallionStorage().land_json(
        domain="events", entity=normalized.lower(), business_key=business_key, payload=payload
    )
    topic = TOPICS[normalized]
    publish_events([payload], topic_id=topic, ordering_key_field="guest_token")
    return GatewayResponse(event_id=str(event.event_id), topic=topic, landed_object=landed.object_name)
