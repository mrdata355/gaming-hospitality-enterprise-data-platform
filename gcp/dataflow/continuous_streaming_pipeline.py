"""GCP Pub/Sub + Dataflow continuous streaming reference.

Synthetic portfolio code only. Project IDs, topics, subscriptions, buckets,
datasets, service accounts and thresholds are generated.

Demonstrates unbounded Pub/Sub ingestion, event-time assignment, fixed/sliding/
session windows, allowed lateness, stable event IDs, quarantine routing and
BigQuery streaming output. Apache Beam imports are lazy so normal repository
validation does not require the Dataflow runtime.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

WindowMode = Literal["fixed", "sliding", "session"]


@dataclass(frozen=True)
class PubSubSource:
    project_id: str
    topic: str
    subscription: str
    dead_letter_topic: str

    @property
    def topic_path(self) -> str:
        return f"projects/{self.project_id}/topics/{self.topic}"

    @property
    def subscription_path(self) -> str:
        return f"projects/{self.project_id}/subscriptions/{self.subscription}"


@dataclass(frozen=True)
class StreamingSink:
    project_id: str
    dataset: str
    table: str
    raw_bucket: str
    quarantine_bucket: str

    @property
    def bigquery_table(self) -> str:
        return f"{self.project_id}:{self.dataset}.{self.table}"


@dataclass(frozen=True)
class DomainSpec:
    name: str
    event_type: str
    business_key: str
    source: PubSubSource
    sink: StreamingSink
    window_mode: WindowMode = "fixed"
    window_seconds: int = 60
    period_seconds: int = 15
    session_gap_seconds: int = 300
    allowed_lateness_seconds: int = 600


@dataclass(frozen=True)
class DataflowRuntime:
    project_id: str
    region: str
    temp_location: str
    staging_location: str
    service_account_email: str
    max_num_workers: int = 12
    machine_type: str = "n2-standard-4"
    streaming_engine: bool = True


def parse_event_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event_ts must include timezone information")
    return parsed.astimezone(timezone.utc)


def validate_event(payload: dict[str, Any], spec: DomainSpec) -> tuple[bool, str | None]:
    required = (
        "event_id",
        "event_type",
        "event_ts",
        "schema_version",
        "source_system",
        "property_code",
        spec.business_key,
    )
    for name in required:
        if payload.get(name) in (None, ""):
            return False, f"MISSING_{name.upper()}"

    if str(payload["event_type"]).upper() != spec.event_type.upper():
        return False, "INVALID_EVENT_TYPE"

    if str(payload["schema_version"]) not in {"1", "1.0"}:
        return False, "UNSUPPORTED_SCHEMA_VERSION"

    try:
        parse_event_time(str(payload["event_ts"]))
    except (TypeError, ValueError):
        return False, "INVALID_EVENT_TIME"

    blocked = {"ssn", "pan", "credit_card", "tax_id", "passport_number"}
    if any(key.lower() in blocked and payload.get(key) for key in payload):
        return False, "PLAINTEXT_SENSITIVE_IDENTIFIER"

    return True, None


def pipeline_options(runtime: DataflowRuntime, job_name: str) -> list[str]:
    options = [
        "--runner=DataflowRunner",
        f"--project={runtime.project_id}",
        f"--region={runtime.region}",
        f"--job_name={job_name}",
        f"--temp_location={runtime.temp_location}",
        f"--staging_location={runtime.staging_location}",
        f"--service_account_email={runtime.service_account_email}",
        f"--max_num_workers={runtime.max_num_workers}",
        f"--machine_type={runtime.machine_type}",
        "--streaming",
        "--save_main_session",
    ]
    if runtime.streaming_engine:
        options.append("--enable_streaming_engine")
    return options


def window_transform(spec: DomainSpec):
    import apache_beam as beam  # type: ignore

    if spec.window_mode == "fixed":
        return beam.WindowInto(
            beam.window.FixedWindows(spec.window_seconds),
            allowed_lateness=spec.allowed_lateness_seconds,
        )
    if spec.window_mode == "sliding":
        return beam.WindowInto(
            beam.window.SlidingWindows(
                size=spec.window_seconds,
                period=spec.period_seconds,
            ),
            allowed_lateness=spec.allowed_lateness_seconds,
        )
    if spec.window_mode == "session":
        return beam.WindowInto(
            beam.window.Sessions(spec.session_gap_seconds),
            allowed_lateness=spec.allowed_lateness_seconds,
        )
    raise ValueError(f"unsupported window_mode={spec.window_mode}")


def build_domain(pipeline, spec: DomainSpec):
    import apache_beam as beam  # type: ignore
    from apache_beam import pvalue  # type: ignore

    class ParseAndValidate(beam.DoFn):
        INVALID = "invalid"

        def process(self, message):
            raw = message.data.decode("utf-8")
            attributes = dict(message.attributes or {})
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                yield pvalue.TaggedOutput(
                    self.INVALID,
                    {
                        "reason": "INVALID_JSON",
                        "raw": raw,
                        "attributes": attributes,
                        "quarantined_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                return

            valid, reason = validate_event(payload, spec)
            if not valid:
                yield pvalue.TaggedOutput(
                    self.INVALID,
                    {
                        "reason": reason,
                        "payload": payload,
                        "attributes": attributes,
                        "quarantined_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                return

            event_time = parse_event_time(str(payload["event_ts"]))
            payload["_pubsub_attributes"] = attributes
            payload["_received_at"] = datetime.now(timezone.utc).isoformat()
            yield beam.window.TimestampedValue(payload, event_time.timestamp())

    class LatestEvent(beam.CombineFn):
        def create_accumulator(self):
            return None

        def add_input(self, current, candidate):
            if current is None:
                return candidate
            current_ts = parse_event_time(str(current["event_ts"]))
            candidate_ts = parse_event_time(str(candidate["event_ts"]))
            return candidate if candidate_ts >= current_ts else current

        def merge_accumulators(self, values):
            selected = None
            for value in values:
                if value is not None:
                    selected = self.add_input(selected, value)
            return selected

        def extract_output(self, value):
            return value

    class Normalize(beam.DoFn):
        def process(self, item):
            _event_id, payload = item
            if payload is None:
                return
            row = dict(payload)
            row["_business_key"] = str(row[spec.business_key])
            row["_stream_domain"] = spec.name
            row["_normalized_at"] = datetime.now(timezone.utc).isoformat()
            yield row

    messages = (
        pipeline
        | f"{spec.name}:ReadPubSub"
        >> beam.io.ReadFromPubSub(
            subscription=spec.source.subscription_path,
            with_attributes=True,
        )
    )

    parsed = (
        messages
        | f"{spec.name}:ParseValidate"
        >> beam.ParDo(ParseAndValidate()).with_outputs(
            ParseAndValidate.INVALID,
            main="valid",
        )
    )

    valid = parsed.valid
    invalid = parsed[ParseAndValidate.INVALID]

    windowed = valid | f"{spec.name}:Window" >> window_transform(spec)

    deduped = (
        windowed
        | f"{spec.name}:KeyEventId" >> beam.Map(lambda row: (str(row["event_id"]), row))
        | f"{spec.name}:LatestEvent" >> beam.CombinePerKey(LatestEvent())
        | f"{spec.name}:Normalize" >> beam.ParDo(Normalize())
    )

    _ = (
        deduped
        | f"{spec.name}:BigQuery"
        >> beam.io.WriteToBigQuery(
            spec.sink.bigquery_table,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
    )

    _ = (
        invalid
        | f"{spec.name}:QuarantineJson" >> beam.Map(json.dumps)
        | f"{spec.name}:QuarantineGCS"
        >> beam.io.WriteToText(
            f"gs://{spec.sink.quarantine_bucket}/{spec.name}/events",
            file_name_suffix=".jsonl",
            num_shards=1,
        )
    )

    return deduped


def domain_specs(project_id: str, env: str) -> tuple[DomainSpec, ...]:
    raw_bucket = f"vh-portfolio-{env}-raw-us-west1"
    quarantine = f"vh-portfolio-{env}-quarantine-us-west1"
    dataset = f"vh_streaming_{env}"

    def sink(table: str) -> StreamingSink:
        return StreamingSink(project_id, dataset, table, raw_bucket, quarantine)

    return (
        DomainSpec(
            name="hotel-reservations",
            event_type="HOTEL_RESERVATION",
            business_key="reservation_id",
            source=PubSubSource(
                project_id,
                "hotel.reservation-events.v1",
                "hotel-reservation-dataflow-v1",
                "hotel.reservation-events.dlq.v1",
            ),
            sink=sink("hotel_reservation_events"),
            window_mode="fixed",
            window_seconds=60,
        ),
        DomainSpec(
            name="gaming-slot-play",
            event_type="SLOT_PLAY",
            business_key="play_id",
            source=PubSubSource(
                project_id,
                "gaming.slot-play-events.v1",
                "gaming-slot-dataflow-v1",
                "gaming.slot-play-events.dlq.v1",
            ),
            sink=sink("gaming_slot_play_events"),
            window_mode="sliding",
            window_seconds=300,
            period_seconds=30,
        ),
        DomainSpec(
            name="mobile-app",
            event_type="MOBILE_APP",
            business_key="mobile_event_id",
            source=PubSubSource(
                project_id,
                "guest.mobile-events.v1",
                "guest-mobile-dataflow-v1",
                "guest.mobile-events.dlq.v1",
            ),
            sink=sink("guest_mobile_events"),
            window_mode="session",
            session_gap_seconds=600,
        ),
    )


def run(runtime: DataflowRuntime, specs: tuple[DomainSpec, ...], job_name: str):
    import apache_beam as beam  # type: ignore
    from apache_beam.options.pipeline_options import PipelineOptions  # type: ignore

    pipeline = beam.Pipeline(options=PipelineOptions(pipeline_options(runtime, job_name)))
    for spec in specs:
        build_domain(pipeline, spec)
    return pipeline.run()
