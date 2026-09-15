from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


Money = Annotated[Decimal, Field(max_digits=18, decimal_places=2)]


class EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    event_ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    schema_version: Literal["1"] = "1"
    source_system: str
    property_code: str
    trace_id: UUID = Field(default_factory=uuid4)

    @field_validator("event_ts")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_ts must be timezone-aware")
        return value.astimezone(timezone.utc)


class ReservationStatus(StrEnum):
    BOOKED = "BOOKED"
    CHECKED_IN = "CHECKED_IN"
    CHECKED_OUT = "CHECKED_OUT"
    CANCELLED = "CANCELLED"
    NO_SHOW = "NO_SHOW"


class HotelReservationEvent(EventBase):
    event_type: Literal["HOTEL_RESERVATION"] = "HOTEL_RESERVATION"
    reservation_id: str
    guest_token: str
    arrival_date: datetime
    departure_date: datetime
    room_type_code: str
    status: ReservationStatus
    room_revenue: Money = Decimal("0.00")
    channel: str


class SlotPlayEvent(EventBase):
    event_type: Literal["SLOT_PLAY"] = "SLOT_PLAY"
    session_id: str
    device_token: str
    guest_token: str | None = None
    coin_in: Money
    payout: Money
    jackpot_amount: Money = Decimal("0.00")

    @property
    def net_win(self) -> Decimal:
        return self.coin_in - self.payout - self.jackpot_amount


class TablePlayEvent(EventBase):
    event_type: Literal["TABLE_PLAY"] = "TABLE_PLAY"
    session_id: str
    table_token: str
    game_code: str
    guest_token: str | None = None
    buy_in: Money
    cash_out: Money
    average_bet: Money
    minutes_played: int = Field(ge=0, le=1440)


class RewardActivityEvent(EventBase):
    event_type: Literal["REWARD_ACTIVITY"] = "REWARD_ACTIVITY"
    reward_event_id: str
    guest_token: str
    activity_type: Literal["EARN", "REDEEM", "ADJUST"]
    points_delta: int
    source_domain: Literal["GAMING", "HOTEL", "DINING", "ENTERTAINMENT", "SPA", "RETAIL"]
    monetary_value: Money = Decimal("0.00")


class MobileAppEvent(EventBase):
    event_type: Literal["MOBILE_APP"] = "MOBILE_APP"
    mobile_event_id: str
    guest_token: str | None = None
    anonymous_session_id: str | None = None
    action: str
    screen_name: str
    offer_token: str | None = None
    device_family: str | None = None
