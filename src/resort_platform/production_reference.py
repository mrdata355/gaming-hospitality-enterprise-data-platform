"""Synthetic production-style runtime for hospitality/gaming portfolio.

This module models the kind of long-lived engineering conventions a mature
enterprise platform would use: immutable raw capture, contract validation,
quarantine, deterministic merge, replay-safe processing, reconciliation,
path construction, observability, and domain-specific canonicalization.

It does not represent private Venetian code or infrastructure.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import logging
import os
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any

LOG = logging.getLogger(__name__)
PORTFOLIO_NOTICE = "Generated architecture; not private Venetian infrastructure."
ALLOWED_PROPERTIES = {"VENETIAN_MAIN", "PALAZZO"}
ALLOWED_SCHEMA_VERSIONS = {"1", "2"}


@dataclasses.dataclass(frozen=True)
class PathConfig:
    environment: str
    project: str
    region: str = "us-west1"

    @property
    def bucket_prefix(self) -> str:
        return f"{self.project}-{self.environment}"

    def zone_uri(
        self,
        zone: str,
        domain: str,
        entity: str,
        business_date: dt.date,
        hour: int | None = None,
    ) -> str:
        base = (
            f"gs://{self.bucket_prefix}-{zone}-{self.region}/"
            f"{domain}/{entity}/business_date={business_date.isoformat()}/"
        )
        return f"{base}hour={hour:02d}/" if hour is not None else base

    def quarantine_uri(
        self,
        domain: str,
        entity: str,
        reason: str,
        business_date: dt.date,
    ) -> str:
        return (
            f"gs://{self.bucket_prefix}-quarantine-{self.region}/"
            f"{domain}/{entity}/reason={reason}/"
            f"business_date={business_date.isoformat()}/"
        )

    def checkpoint_uri(self, pipeline: str, query: str) -> str:
        return (
            f"gs://{self.bucket_prefix}-checkpoints-{self.region}/"
            f"{pipeline}/{query}/"
        )


@dataclasses.dataclass(frozen=True)
class EventEnvelope:
    event_id: str
    event_type: str
    source_system: str
    property_code: str
    event_time: dt.datetime
    schema_version: str
    payload: Mapping[str, Any]
    trace_id: str | None = None
    source_partition: int | None = None
    source_offset: int | None = None
    source_watermark: int | None = None

    def stable_hash(self) -> str:
        body = json.dumps(
            dataclasses.asdict(self),
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(body.encode()).hexdigest()


@dataclasses.dataclass(frozen=True)
class CanonicalRecord:
    entity: str
    business_key: str
    property_code: str
    source_version: int
    source_updated_at: dt.datetime
    event_id: str
    attributes: Mapping[str, Any]

    @property
    def canonical_key(self) -> str:
        return f"{self.entity}:{self.property_code}:{self.business_key}"


class ContractError(ValueError):
    """Raised when an event cannot satisfy the declared contract."""


class DuplicateEvent(RuntimeError):
    """Raised only by strict duplicate-processing modes."""


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def normalize_code(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).strip().split())
    return text or None


def decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def iso_datetime(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def validate_envelope(event: EventEnvelope) -> list[str]:
    errors: list[str] = []
    if not event.event_id:
        errors.append("EVENT_ID_REQUIRED")
    if not event.event_type:
        errors.append("EVENT_TYPE_REQUIRED")
    if not event.source_system:
        errors.append("SOURCE_SYSTEM_REQUIRED")
    if event.property_code not in ALLOWED_PROPERTIES:
        errors.append("PROPERTY_UNSUPPORTED")
    if event.event_time > utcnow() + dt.timedelta(minutes=5):
        errors.append("FUTURE_EVENT_TIME")
    if event.schema_version not in ALLOWED_SCHEMA_VERSIONS:
        errors.append("UNSUPPORTED_SCHEMA_VERSION")
    if not isinstance(event.payload, Mapping):
        errors.append("PAYLOAD_NOT_OBJECT")
    return errors


def validate_reservation(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    reservation_id = normalize_text(payload.get("reservation_id"))
    arrival = payload.get("arrival_date")
    departure = payload.get("departure_date")
    rooms = int_or_none(payload.get("rooms"))
    revenue = decimal_or_none(payload.get("revenue_amount"))
    if not reservation_id:
        errors.append("RESERVATION_ID_REQUIRED")
    if not arrival or not departure:
        errors.append("STAY_DATES_REQUIRED")
    elif dt.date.fromisoformat(str(departure)) < dt.date.fromisoformat(str(arrival)):
        errors.append("DEPARTURE_BEFORE_ARRIVAL")
    if rooms is not None and rooms < 0:
        errors.append("ROOMS_NEGATIVE")
    if revenue is not None and revenue < 0:
        errors.append("REVENUE_NEGATIVE")
    return errors


def validate_slot_play(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if not normalize_text(payload.get("play_id")):
        errors.append("PLAY_ID_REQUIRED")
    if not normalize_text(payload.get("machine_id")):
        errors.append("MACHINE_ID_REQUIRED")
    coin_in = decimal_or_none(payload.get("coin_in"))
    payout = decimal_or_none(payload.get("payout"))
    if coin_in is not None and coin_in < 0:
        errors.append("COIN_IN_NEGATIVE")
    if payout is not None and payout < 0:
        errors.append("PAYOUT_NEGATIVE")
    return errors


def validate_reward(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if not normalize_text(payload.get("reward_event_id")):
        errors.append("REWARD_EVENT_ID_REQUIRED")
    if not normalize_text(payload.get("guest_id")):
        errors.append("GUEST_ID_REQUIRED")
    return errors


def validate_payment(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if not normalize_text(payload.get("transaction_id")):
        errors.append("TRANSACTION_ID_REQUIRED")
    if normalize_code(payload.get("currency")) not in {"USD"}:
        errors.append("CURRENCY_UNSUPPORTED")
    return errors


def validate_work_order(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if not normalize_text(payload.get("work_order_id")):
        errors.append("WORK_ORDER_ID_REQUIRED")
    opened = payload.get("opened_at")
    closed = payload.get("closed_at")
    if opened and closed and iso_datetime(closed) < iso_datetime(opened):
        errors.append("CLOSED_BEFORE_OPENED")
    return errors


DOMAIN_VALIDATORS: dict[str, Any] = {
    "HOTEL_RESERVATION": validate_reservation,
    "SLOT_PLAY": validate_slot_play,
    "REWARD_EARN": validate_reward,
    "REWARD_REDEMPTION": validate_reward,
    "PAYMENT_TRANSACTION": validate_payment,
    "PROPERTY_WORK_ORDER": validate_work_order,
}


class InMemoryCheckpointStore:
    def __init__(self) -> None:
        self._values: dict[str, int] = {}

    def read(self, pipeline: str) -> int:
        return self._values.get(pipeline, 0)

    def commit(self, pipeline: str, value: int) -> None:
        if value < self.read(pipeline):
            raise ValueError("checkpoint regression")
        self._values[pipeline] = value


class CanonicalMergeStore:
    def __init__(self) -> None:
        self._rows: dict[str, dict[str, Any]] = {}
        self._event_hashes: set[str] = set()
        self._audit: list[dict[str, Any]] = []

    def remember_event(self, event_hash: str) -> bool:
        if event_hash in self._event_hashes:
            return False
        self._event_hashes.add(event_hash)
        return True

    def merge(self, record: CanonicalRecord) -> str:
        key = record.canonical_key
        current = self._rows.get(key)
        candidate_order = (
            record.source_version,
            record.source_updated_at,
            record.event_id,
        )
        if current is None:
            self._rows[key] = dataclasses.asdict(record)
            outcome = "INSERTED"
        else:
            current_order = (
                int(current["source_version"]),
                current["source_updated_at"],
                str(current["event_id"]),
            )
            if candidate_order > current_order:
                self._rows[key] = dataclasses.asdict(record)
                outcome = "UPDATED"
            elif candidate_order == current_order:
                outcome = "DUPLICATE_IGNORED"
            else:
                outcome = "STALE_IGNORED"
        self._audit.append(
            {
                "canonical_key": key,
                "event_id": record.event_id,
                "outcome": outcome,
                "observed_at": utcnow(),
            }
        )
        return outcome

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return dict(self._rows)

    def audit(self) -> list[dict[str, Any]]:
        return list(self._audit)


class QualityRouter:
    def route(self, event: EventEnvelope) -> tuple[str, list[str]]:
        reasons = validate_envelope(event)
        validator = DOMAIN_VALIDATORS.get(normalize_code(event.event_type) or "")
        if validator is not None:
            reasons.extend(validator(event.payload))
        return ("quarantine", reasons) if reasons else ("clean", [])


class PipelineMetrics:
    def __init__(self) -> None:
        self.counts: defaultdict[str, int] = defaultdict(int)

    def inc(self, metric: str, value: int = 1) -> None:
        self.counts[metric] += value

    def as_dict(self) -> dict[str, int]:
        return dict(self.counts)


class EnterprisePipeline:
    def __init__(
        self,
        paths: PathConfig,
        merge_store: CanonicalMergeStore | None = None,
    ) -> None:
        self.paths = paths
        self.merge_store = merge_store or CanonicalMergeStore()
        self.quality = QualityRouter()
        self.metrics = PipelineMetrics()

    def process(self, events: Iterable[EventEnvelope]) -> dict[str, Any]:
        outputs: dict[str, list[dict[str, Any]]] = {
            "raw": [],
            "clean": [],
            "quarantine": [],
            "merge": [],
        }
        for event in events:
            self.metrics.inc("source")
            raw = {
                "envelope": dataclasses.asdict(event),
                "event_hash": event.stable_hash(),
                "ingested_at": utcnow(),
            }
            outputs["raw"].append(raw)
            self.metrics.inc("raw")
            if not self.merge_store.remember_event(raw["event_hash"]):
                self.metrics.inc("duplicate")
                outputs["merge"].append(
                    {"event_id": event.event_id, "outcome": "DUPLICATE_IGNORED"}
                )
                continue
            route, reasons = self.quality.route(event)
            if route == "quarantine":
                outputs["quarantine"].append(
                    {"event": raw, "reasons": sorted(set(reasons))}
                )
                self.metrics.inc("quarantine")
                continue
            clean = self._normalize(event)
            outputs["clean"].append(clean)
            self.metrics.inc("clean")
            outcome = self.merge_store.merge(self._canonicalize(clean))
            outputs["merge"].append(
                {"event_id": event.event_id, "outcome": outcome}
            )
            self.metrics.inc(outcome.lower())
        return {
            "outputs": outputs,
            "metrics": self.metrics.as_dict(),
            "reconciliation": reconcile_metrics(self.metrics.as_dict()),
        }

    def _normalize(self, event: EventEnvelope) -> dict[str, Any]:
        payload = dict(event.payload)
        event_type = normalize_code(event.event_type) or "UNKNOWN"
        business_key = self._business_key(event_type, payload, event.event_id)
        return {
            "business_key": business_key,
            "event_id": event.event_id,
            "event_type": event_type,
            "property_code": normalize_code(event.property_code),
            "event_time": event.event_time.astimezone(dt.UTC),
            "source_version": int(payload.get("source_version", 1)),
            "source_updated_at": iso_datetime(
                payload.get("source_updated_at") or event.event_time
            ),
            "payload": payload,
            "trace_id": event.trace_id,
            "source_system": normalize_code(event.source_system),
        }

    @staticmethod
    def _business_key(
        event_type: str,
        payload: Mapping[str, Any],
        fallback: str,
    ) -> str:
        key_candidates = {
            "HOTEL_RESERVATION": "reservation_id",
            "SLOT_PLAY": "play_id",
            "REWARD_EARN": "reward_event_id",
            "REWARD_REDEMPTION": "reward_event_id",
            "PAYMENT_TRANSACTION": "transaction_id",
            "PROPERTY_WORK_ORDER": "work_order_id",
        }
        field = key_candidates.get(event_type)
        return str(payload.get(field) or payload.get("business_key") or fallback)

    @staticmethod
    def _canonicalize(clean: Mapping[str, Any]) -> CanonicalRecord:
        return CanonicalRecord(
            entity=str(clean["event_type"]),
            business_key=str(clean["business_key"]),
            property_code=str(clean["property_code"]),
            source_version=int(clean["source_version"]),
            source_updated_at=clean["source_updated_at"],
            event_id=str(clean["event_id"]),
            attributes=dict(clean["payload"]),
        )


def reconcile_metrics(metrics: Mapping[str, int]) -> dict[str, Any]:
    source = int(metrics.get("source", 0))
    clean = int(metrics.get("clean", 0))
    quarantine = int(metrics.get("quarantine", 0))
    duplicate = int(metrics.get("duplicate", 0))
    explained = clean + quarantine + duplicate
    return {
        "source_count": source,
        "explained_count": explained,
        "balanced": source == explained,
    }


def reservation_path(cfg: PathConfig, zone: str, date: dt.date, hour: int) -> str:
    return cfg.zone_uri(zone, "hotel", "reservation", date, hour)


def slot_play_path(cfg: PathConfig, zone: str, date: dt.date, hour: int) -> str:
    return cfg.zone_uri(zone, "gaming", "slot_play", date, hour)


def reward_path(cfg: PathConfig, zone: str, date: dt.date, hour: int) -> str:
    return cfg.zone_uri(zone, "rewards", "activity", date, hour)


def payment_path(cfg: PathConfig, zone: str, date: dt.date, hour: int) -> str:
    return cfg.zone_uri(zone, "payments", "transaction", date, hour)


def work_order_path(cfg: PathConfig, zone: str, date: dt.date, hour: int) -> str:
    return cfg.zone_uri(zone, "operations", "work_order", date, hour)


def build_demo_events(now: dt.datetime | None = None) -> list[EventEnvelope]:
    now = now or utcnow()
    return [
        EventEnvelope(
            event_id="EVT-DEMO-0001",
            event_type="HOTEL_RESERVATION",
            source_system="DB2_HOSPITALITY",
            property_code="VENETIAN_MAIN",
            event_time=now,
            schema_version="1",
            payload={
                "reservation_id": "RES-DEMO-1001",
                "arrival_date": now.date().isoformat(),
                "departure_date": (now.date() + dt.timedelta(days=3)).isoformat(),
                "status": "CONFIRMED",
                "rooms": 1,
                "revenue_amount": "1197.00",
                "source_version": 1,
            },
        ),
        EventEnvelope(
            event_id="EVT-DEMO-0002",
            event_type="SLOT_PLAY",
            source_system="PUBSUB_GAMING",
            property_code="PALAZZO",
            event_time=now,
            schema_version="1",
            payload={
                "play_id": "PLAY-DEMO-2001",
                "machine_id": "M-DEMO-992",
                "coin_in": "125.00",
                "payout": "87.50",
                "source_version": 1,
            },
        ),
    ]


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    cfg = PathConfig(
        environment=os.getenv("ENVIRONMENT", "dev"),
        project=os.getenv("GCP_PROJECT_ID", "vh-portfolio"),
        region=os.getenv("GCP_REGION", "us-west1"),
    )
    pipeline = EnterprisePipeline(cfg)
    result = pipeline.process(build_demo_events())
    print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()
