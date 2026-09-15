"""Synthetic continuous-streaming reference for the portfolio.

This is project-owned code and does not represent private Venetian systems.
It demonstrates event-time processing, watermarking, stable-ID deduplication,
checkpointing, replay-safe sinks, query health, and the distinction between a
continuously running Structured Streaming query and Spark's special Continuous
Processing trigger.
"""
from __future__ import annotations

import json
import logging
import signal
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal

LOGGER = logging.getLogger(__name__)
ProcessingMode = Literal["microbatch", "continuous"]


class QueryState(StrEnum):
    STARTING = "STARTING"
    ACTIVE = "ACTIVE"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class EventContract:
    event_type: str
    required_fields: tuple[str, ...]
    business_key_fields: tuple[str, ...]
    event_time_field: str = "event_ts"
    event_id_field: str = "event_id"
    schema_version: str = "1"


@dataclass(frozen=True)
class SourceSpec:
    name: str
    topic: str
    bootstrap_servers: str
    starting_offsets: str = "latest"
    fail_on_data_loss: bool = False
    max_offsets_per_trigger: int | None = None
    min_partitions: int | None = None


@dataclass(frozen=True)
class SinkSpec:
    name: str
    raw_path: str
    clean_path: str
    quarantine_path: str
    checkpoint_path: str
    current_table: str
    history_table: str


@dataclass(frozen=True)
class QuerySpec:
    name: str
    source: SourceSpec
    sink: SinkSpec
    contract: EventContract
    watermark_delay: str = "10 minutes"
    trigger_interval: str = "15 seconds"
    processing_mode: ProcessingMode = "microbatch"
    dedupe_columns: tuple[str, ...] = ("event_id",)
    max_batch_duration_seconds: float = 120.0
    max_input_lag_seconds: float = 300.0


@dataclass(frozen=True)
class RuntimeConfig:
    app_name: str
    environment: str
    shuffle_partitions: int = 96
    timezone: str = "UTC"
    stop_timeout_seconds: int = 60
    queries: tuple[QuerySpec, ...] = ()


@dataclass(frozen=True)
class ProgressSnapshot:
    query_name: str
    query_id: str
    run_id: str
    batch_id: int | None
    input_rows: int
    input_rows_per_second: float
    processed_rows_per_second: float
    batch_duration_ms: float
    event_time_watermark: str | None
    state_rows_total: int
    state_rows_updated: int
    raw: Mapping[str, Any] = field(repr=False, default_factory=dict)


@dataclass(frozen=True)
class QueryHealth:
    name: str
    state: QueryState
    reasons: tuple[str, ...]
    last_progress: ProgressSnapshot | None


def parse_duration_ms(progress: Mapping[str, Any]) -> float:
    durations = progress.get("durationMs") or {}
    if not isinstance(durations, Mapping):
        return 0.0
    values: list[float] = []
    for value in durations.values():
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    return max(values, default=0.0)


def parse_state_metrics(progress: Mapping[str, Any]) -> tuple[int, int]:
    total = 0
    updated = 0
    for operator in progress.get("stateOperators") or []:
        if not isinstance(operator, Mapping):
            continue
        total += int(operator.get("numRowsTotal") or 0)
        updated += int(operator.get("numRowsUpdated") or 0)
    return total, updated


def parse_progress(name: str, payload: str | Mapping[str, Any]) -> ProgressSnapshot:
    progress = json.loads(payload) if isinstance(payload, str) else dict(payload)
    state_total, state_updated = parse_state_metrics(progress)
    event_time = progress.get("eventTime") or {}
    return ProgressSnapshot(
        query_name=name,
        query_id=str(progress.get("id") or ""),
        run_id=str(progress.get("runId") or ""),
        batch_id=int(progress["batchId"]) if progress.get("batchId") is not None else None,
        input_rows=int(progress.get("numInputRows") or 0),
        input_rows_per_second=float(progress.get("inputRowsPerSecond") or 0),
        processed_rows_per_second=float(progress.get("processedRowsPerSecond") or 0),
        batch_duration_ms=parse_duration_ms(progress),
        event_time_watermark=event_time.get("watermark") if isinstance(event_time, Mapping) else None,
        state_rows_total=state_total,
        state_rows_updated=state_updated,
        raw=progress,
    )


def evaluate_health(spec: QuerySpec, snapshot: ProgressSnapshot | None, active: bool) -> QueryHealth:
    if not active:
        return QueryHealth(spec.name, QueryState.STOPPED, ("query_not_active",), snapshot)
    if snapshot is None:
        return QueryHealth(spec.name, QueryState.STARTING, ("no_progress_yet",), None)

    reasons: list[str] = []
    if snapshot.batch_duration_ms > spec.max_batch_duration_seconds * 1000:
        reasons.append("trigger duration exceeds the configured SLO")
    if (
        snapshot.input_rows_per_second > 0
        and snapshot.processed_rows_per_second > 0
        and snapshot.processed_rows_per_second < snapshot.input_rows_per_second * 0.8
    ):
        reasons.append("processing rate materially trails input rate")
    state = QueryState.DEGRADED if reasons else QueryState.ACTIVE
    return QueryHealth(spec.name, state, tuple(reasons), snapshot)


def validate_query_spec(spec: QuerySpec) -> None:
    if not spec.name:
        raise ValueError("query name is required")
    if not spec.source.topic:
        raise ValueError(f"{spec.name}: topic is required")
    if not spec.contract.business_key_fields:
        raise ValueError(f"{spec.name}: business key is required")
    if spec.processing_mode == "continuous":
        # The special Spark Continuous Processing trigger has a narrower
        # operator surface. This project uses micro-batch for stateful
        # watermark + dedupe + transactional foreachBatch merge patterns.
        if spec.watermark_delay or spec.dedupe_columns:
            raise ValueError(
                f"{spec.name}: stateful watermark/dedupe design uses microbatch; "
                "do not silently switch it to Spark Continuous Processing"
            )


def build_spark(config: RuntimeConfig):
    from pyspark.sql import SparkSession  # type: ignore

    return (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.session.timeZone", config.timezone)
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_event_stream(spark, source: SourceSpec):
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", source.bootstrap_servers)
        .option("subscribe", source.topic)
        .option("startingOffsets", source.starting_offsets)
        .option("failOnDataLoss", str(source.fail_on_data_loss).lower())
    )
    if source.max_offsets_per_trigger is not None:
        reader = reader.option("maxOffsetsPerTrigger", source.max_offsets_per_trigger)
    if source.min_partitions is not None:
        reader = reader.option("minPartitions", source.min_partitions)
    return reader.load()


def add_source_metadata(df):
    from pyspark.sql import functions as F  # type: ignore

    return df.select(
        F.col("key").cast("string").alias("_source_key"),
        F.col("value").cast("string").alias("_source_value"),
        F.col("topic").alias("_source_topic"),
        F.col("partition").alias("_source_partition"),
        F.col("offset").alias("_source_offset"),
        F.col("timestamp").alias("_source_timestamp"),
        F.current_timestamp().alias("_ingested_at"),
    )


def envelope_schema(contract: EventContract):
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType  # type: ignore

    fields = [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("schema_version", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("property_code", StringType(), False),
        StructField("business_key", StringType(), True),
        StructField("source_version", StringType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("payload_json", StringType(), True),
    ]
    known = {field.name for field in fields}
    for name in contract.required_fields:
        if name not in known:
            fields.append(StructField(name, StringType(), True))
            known.add(name)
    return StructType(fields)


def parse_envelope(df, contract: EventContract):
    from pyspark.sql import functions as F  # type: ignore

    parsed = df.withColumn(
        "_event",
        F.from_json(F.col("_source_value"), envelope_schema(contract)),
    )
    return parsed.select("*", F.col("_event.*")).drop("_event")


def add_quality_status(df, contract: EventContract):
    from pyspark.sql import functions as F  # type: ignore

    missing = None
    for name in contract.required_fields:
        condition = F.col(name).isNull() | (F.trim(F.col(name).cast("string")) == "")
        missing = condition if missing is None else (missing | condition)

    valid = (
        (~missing if missing is not None else F.lit(True))
        & (F.upper(F.col("event_type")) == F.lit(contract.event_type.upper()))
        & (F.col("schema_version") == F.lit(contract.schema_version))
        & F.col(contract.event_id_field).isNotNull()
        & F.col(contract.event_time_field).isNotNull()
    )
    return (
        df.withColumn("_dq_valid", valid)
        .withColumn(
            "_dq_reason",
            F.when(F.col(contract.event_id_field).isNull(), F.lit("MISSING_EVENT_ID"))
            .when(F.col(contract.event_time_field).isNull(), F.lit("MISSING_EVENT_TIME"))
            .when(missing if missing is not None else F.lit(False), F.lit("MISSING_REQUIRED_FIELD"))
            .when(F.upper(F.col("event_type")) != F.lit(contract.event_type.upper()), F.lit("INVALID_EVENT_TYPE"))
            .when(F.col("schema_version") != F.lit(contract.schema_version), F.lit("UNSUPPORTED_SCHEMA_VERSION"))
            .otherwise(F.lit(None).cast("string")),
        )
    )


def clean_stateful_stream(df, spec: QuerySpec):
    from pyspark.sql import functions as F  # type: ignore

    valid = df.filter(F.col("_dq_valid"))
    stateful = valid.withWatermark(spec.contract.event_time_field, spec.watermark_delay)
    if hasattr(stateful, "dropDuplicatesWithinWatermark"):
        stateful = stateful.dropDuplicatesWithinWatermark(list(spec.dedupe_columns))
    else:
        stateful = stateful.dropDuplicates(list(spec.dedupe_columns))

    key = F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column).cast("string"), F.lit("<null>"))
            for column in spec.contract.business_key_fields
        ],
    )
    return (
        stateful.withColumn("_business_key", key)
        .withColumn("_normalized_at", F.current_timestamp())
        .withColumn("_processing_date", F.to_date(F.col("_normalized_at")))
    )


def quarantine_stream(df):
    from pyspark.sql import functions as F  # type: ignore

    return (
        df.filter(~F.col("_dq_valid"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_quarantine_date", F.to_date(F.col("_quarantined_at")))
    )


def append_stream(df, *, path: str, checkpoint: str, name: str, interval: str):
    return (
        df.writeStream.format("delta")
        .queryName(name)
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=interval)
        .start()
    )


def merge_batch_factory(
    spark,
    target_table: str,
    history_table: str,
    key_columns: tuple[str, ...],
) -> Callable[[Any, int], None]:
    def merge_batch(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        view = f"_stream_batch_{batch_id}"
        batch_df.createOrReplaceTempView(view)
        keys = " AND ".join(f"t.`{c}` <=> s.`{c}`" for c in key_columns)

        spark.sql(
            f"""
            INSERT INTO {history_table}
            SELECT *, {batch_id} AS _stream_batch_id,
                   current_timestamp() AS _history_loaded_at
            FROM {view}
            """
        )
        spark.sql(
            f"""
            MERGE INTO {target_table} t
            USING {view} s
              ON {keys}
            WHEN MATCHED AND (
                 coalesce(cast(s.source_version as bigint), 0)
                   > coalesce(cast(t.source_version as bigint), 0)
                 OR (
                   coalesce(cast(s.source_version as bigint), 0)
                     = coalesce(cast(t.source_version as bigint), 0)
                   AND s.source_updated_at > t.source_updated_at
                 )
            ) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    return merge_batch


def start_query_group(spark, spec: QuerySpec) -> list[Any]:
    validate_query_spec(spec)
    source = read_event_stream(spark, spec.source)
    bronze = add_source_metadata(source)
    parsed = parse_envelope(bronze, spec.contract)
    quality = add_quality_status(parsed, spec.contract)
    clean = clean_stateful_stream(quality, spec)
    quarantine = quarantine_stream(quality)

    raw_query = append_stream(
        parsed,
        path=spec.sink.raw_path,
        checkpoint=f"{spec.sink.checkpoint_path}/raw",
        name=f"{spec.name}-raw",
        interval=spec.trigger_interval,
    )
    clean_query = append_stream(
        clean,
        path=spec.sink.clean_path,
        checkpoint=f"{spec.sink.checkpoint_path}/clean",
        name=f"{spec.name}-clean",
        interval=spec.trigger_interval,
    )
    quarantine_query = append_stream(
        quarantine,
        path=spec.sink.quarantine_path,
        checkpoint=f"{spec.sink.checkpoint_path}/quarantine",
        name=f"{spec.name}-quarantine",
        interval=spec.trigger_interval,
    )

    merge_fn = merge_batch_factory(
        spark,
        target_table=spec.sink.current_table,
        history_table=spec.sink.history_table,
        key_columns=spec.contract.business_key_fields,
    )
    merge_query = (
        clean.writeStream.queryName(f"{spec.name}-canonical-merge")
        .foreachBatch(merge_fn)
        .option("checkpointLocation", f"{spec.sink.checkpoint_path}/merge")
        .trigger(processingTime=spec.trigger_interval)
        .start()
    )
    return [raw_query, clean_query, quarantine_query, merge_query]


class QuerySupervisor:
    def __init__(self, spark, config: RuntimeConfig):
        self.spark = spark
        self.config = config
        self.queries: list[Any] = []
        self.specs = {spec.name: spec for spec in config.queries}
        self._stopping = False

    def start(self) -> None:
        for spec in self.config.queries:
            self.queries.extend(start_query_group(self.spark, spec))

    def snapshots(self) -> dict[str, ProgressSnapshot]:
        result: dict[str, ProgressSnapshot] = {}
        for query in self.queries:
            if query.lastProgress:
                result[query.name] = parse_progress(query.name, query.lastProgress)
        return result

    def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        deadline = time.monotonic() + self.config.stop_timeout_seconds
        for query in self.queries:
            if query.isActive:
                query.stop()
        for query in self.queries:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                break
            try:
                query.awaitTermination(int(remaining * 1000))
            except Exception:
                LOGGER.exception("graceful stop failed query=%s", query.name)

    def await_any_termination(self) -> None:
        self.spark.streams.awaitAnyTermination()


def install_signal_handlers(supervisor: QuerySupervisor) -> None:
    def handler(signum: int, _frame: Any) -> None:
        LOGGER.warning("received signal=%s; stopping streaming queries", signum)
        supervisor.stop()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def default_specs(bootstrap_servers: str, env: str) -> tuple[QuerySpec, ...]:
    root = f"gs://vh-portfolio-{env}"
    checkpoint = f"gs://vh-portfolio-{env}-checkpoints"

    definitions = (
        ("hotel-reservations", "hotel.reservation-events.v1", "HOTEL_RESERVATION", "reservation_id"),
        ("gaming-slot-play", "gaming.slot-play-events.v1", "SLOT_PLAY", "play_id"),
        ("reward-activity", "rewards.earn-events.v1", "REWARD_ACTIVITY", "reward_event_id"),
    )
    specs: list[QuerySpec] = []
    for name, topic, event_type, key in definitions:
        contract = EventContract(
            event_type=event_type,
            required_fields=(
                "event_id",
                "event_type",
                "event_ts",
                "schema_version",
                "source_system",
                "property_code",
                key,
            ),
            business_key_fields=(key,),
        )
        specs.append(
            QuerySpec(
                name=name,
                source=SourceSpec(
                    name=f"{name}-source",
                    topic=topic,
                    bootstrap_servers=bootstrap_servers,
                    max_offsets_per_trigger=250_000,
                ),
                sink=SinkSpec(
                    name=f"{name}-sink",
                    raw_path=f"{root}/raw/{name}",
                    clean_path=f"{root}/clean/{name}",
                    quarantine_path=f"{root}/quarantine/{name}",
                    checkpoint_path=f"{checkpoint}/{name}",
                    current_table=f"vh_merge_{env}.{name.replace('-', '_')}_current",
                    history_table=f"vh_merge_{env}.{name.replace('-', '_')}_history",
                ),
                contract=contract,
                processing_mode="microbatch",
                watermark_delay="10 minutes",
                trigger_interval="15 seconds",
            )
        )
    return tuple(specs)
