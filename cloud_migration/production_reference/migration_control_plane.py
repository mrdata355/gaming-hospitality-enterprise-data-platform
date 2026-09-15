"""Synthetic cloud migration control-plane reference.

Models the lifecycle of moving legacy DB2/SQL Server/SSIS workloads into a
hybrid ADF + GCP platform with reconciliation, dual-run evidence, cutover
gates, and rollback checkpoints.

This is a portfolio implementation, not evidence of private-system access.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import hashlib
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any


class WaveStatus(str, enum.Enum):
    PLANNED = "PLANNED"
    READY = "READY"
    MIGRATING = "MIGRATING"
    DUAL_RUN = "DUAL_RUN"
    VALIDATING = "VALIDATING"
    CUTOVER = "CUTOVER"
    COMPLETE = "COMPLETE"
    ROLLED_BACK = "ROLLED_BACK"


@dataclasses.dataclass(frozen=True)
class Workload:
    workload_id: str
    domain: str
    source_platform: str
    target_platform: str
    criticality: str
    daily_rows: int
    sla_minutes: int
    contains_pii: bool
    dependency_ids: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class MigrationEvidence:
    workload_id: str
    source_count: int
    target_count: int
    source_checksum: str
    target_checksum: str
    freshness_minutes: int
    schema_compatible: bool
    quality_passed: bool
    observed_utc: dt.datetime

    @property
    def reconciled(self) -> bool:
        return (
            self.source_count == self.target_count
            and self.source_checksum == self.target_checksum
        )

    @property
    def cutover_ready(self) -> bool:
        return (
            self.reconciled
            and self.schema_compatible
            and self.quality_passed
        )


@dataclasses.dataclass(frozen=True)
class CutoverCheckpoint:
    workload_id: str
    source_watermark: str
    target_watermark: str
    previous_route: str
    new_route: str
    created_utc: dt.datetime


@dataclasses.dataclass(frozen=True)
class MigrationDecision:
    workload_id: str
    approved: bool
    reasons: tuple[str, ...]
    decided_utc: dt.datetime


def checksum(
    rows: Iterable[Mapping[str, Any]],
    key_fields: tuple[str, ...],
) -> str:
    digest = hashlib.sha256()
    sortable = sorted(
        rows,
        key=lambda row: tuple(str(row.get(key, "")) for key in key_fields),
    )
    for row in sortable:
        payload = "|".join(str(row.get(key, "")) for key in key_fields)
        digest.update(payload.encode())
    return digest.hexdigest()


def risk_score(workload: Workload) -> int:
    score = 0
    score += {
        "LOW": 1,
        "MEDIUM": 3,
        "HIGH": 5,
        "CRITICAL": 8,
    }.get(workload.criticality, 5)
    score += 3 if workload.contains_pii else 0
    score += 3 if workload.daily_rows > 100_000_000 else 0
    score += 2 if 1_000_000 < workload.daily_rows <= 100_000_000 else 0
    score += 2 if workload.sla_minutes <= 5 else 0
    score += 1 if 5 < workload.sla_minutes <= 30 else 0
    score += min(4, len(workload.dependency_ids))
    return score


def recommended_pattern(workload: Workload) -> str:
    if workload.sla_minutes <= 5:
        return "EVENT_DRIVEN_DUAL_RUN"
    if workload.daily_rows > 50_000_000:
        return "BULK_INCREMENTAL_PARALLEL"
    if workload.source_platform.startswith("DB2"):
        return "BOUNDED_WATERMARK_BATCH"
    return "BATCH_INCREMENTAL_RECONCILED"


class MigrationRegistry:
    def __init__(self, workloads: Iterable[Workload]) -> None:
        self.workloads = {w.workload_id: w for w in workloads}
        self.status = {
            w.workload_id: WaveStatus.PLANNED
            for w in workloads
        }
        self.evidence: dict[str, list[MigrationEvidence]] = defaultdict(list)
        self.checkpoints: dict[str, CutoverCheckpoint] = {}
        self.decisions: list[MigrationDecision] = []

    def dependencies_complete(self, workload_id: str) -> bool:
        return all(
            self.status[dependency] == WaveStatus.COMPLETE
            for dependency in self.workloads[workload_id].dependency_ids
        )

    def mark_ready(self, workload_id: str) -> None:
        if not self.dependencies_complete(workload_id):
            raise RuntimeError("dependencies incomplete")
        self.status[workload_id] = WaveStatus.READY

    def start(self, workload_id: str) -> None:
        if self.status[workload_id] != WaveStatus.READY:
            raise RuntimeError("workload not ready")
        self.status[workload_id] = WaveStatus.MIGRATING

    def start_dual_run(self, workload_id: str) -> None:
        if self.status[workload_id] != WaveStatus.MIGRATING:
            raise RuntimeError("migration not started")
        self.status[workload_id] = WaveStatus.DUAL_RUN

    def validate(self, evidence: MigrationEvidence) -> None:
        if evidence.workload_id not in self.workloads:
            raise KeyError(evidence.workload_id)
        self.evidence[evidence.workload_id].append(evidence)
        self.status[evidence.workload_id] = WaveStatus.VALIDATING

    def record_checkpoint(
        self,
        workload_id: str,
        source_watermark: str,
        target_watermark: str,
        previous_route: str,
        new_route: str,
    ) -> CutoverCheckpoint:
        checkpoint = CutoverCheckpoint(
            workload_id=workload_id,
            source_watermark=source_watermark,
            target_watermark=target_watermark,
            previous_route=previous_route,
            new_route=new_route,
            created_utc=dt.datetime.now(dt.UTC),
        )
        self.checkpoints[workload_id] = checkpoint
        return checkpoint

    def cutover(self, workload_id: str) -> MigrationDecision:
        history = self.evidence[workload_id]
        reasons: list[str] = []
        if len(history) < 2:
            reasons.append("TWO_VALIDATIONS_REQUIRED")
        elif not all(e.cutover_ready for e in history[-2:]):
            reasons.append("RECENT_VALIDATION_FAILED")
        if workload_id not in self.checkpoints:
            reasons.append("ROLLBACK_CHECKPOINT_REQUIRED")
        approved = not reasons
        decision = MigrationDecision(
            workload_id=workload_id,
            approved=approved,
            reasons=tuple(reasons),
            decided_utc=dt.datetime.now(dt.UTC),
        )
        self.decisions.append(decision)
        if approved:
            self.status[workload_id] = WaveStatus.CUTOVER
        return decision

    def complete(self, workload_id: str) -> None:
        if self.status[workload_id] != WaveStatus.CUTOVER:
            raise RuntimeError("cutover not started")
        self.status[workload_id] = WaveStatus.COMPLETE

    def rollback(self, workload_id: str) -> CutoverCheckpoint:
        checkpoint = self.checkpoints[workload_id]
        self.status[workload_id] = WaveStatus.ROLLED_BACK
        return checkpoint


def default_workloads() -> list[Workload]:
    return [
        Workload(
            "hotel_reservation",
            "hotel",
            "DB2+SSIS",
            "GCS+BigQuery",
            "CRITICAL",
            12_000_000,
            15,
            True,
        ),
        Workload(
            "gaming_slot_event",
            "gaming",
            "DB2+SQLServer",
            "PubSub+CloudRun+BigQuery",
            "CRITICAL",
            250_000_000,
            1,
            False,
        ),
        Workload(
            "reward_activity",
            "rewards",
            "DB2+SSIS",
            "GCS+BigQuery",
            "HIGH",
            20_000_000,
            15,
            True,
            ("hotel_reservation",),
        ),
        Workload(
            "payment_transaction",
            "payments",
            "SQLServer+SSIS",
            "GCS+BigQuery",
            "CRITICAL",
            50_000_000,
            5,
            True,
        ),
        Workload(
            "property_operation",
            "operations",
            "SQLServer",
            "GCS+BigQuery",
            "MEDIUM",
            2_000_000,
            30,
            False,
        ),
        Workload(
            "guest_360",
            "guest",
            "SQLServer EDW",
            "BigQuery curated",
            "CRITICAL",
            8_000_000,
            30,
            True,
            ("hotel_reservation", "reward_activity"),
        ),
        Workload(
            "occupancy_daily",
            "hotel",
            "SQLServer mart",
            "BigQuery mart",
            "HIGH",
            200_000,
            60,
            False,
            ("hotel_reservation",),
        ),
        Workload(
            "gaming_5min",
            "gaming",
            "SQLServer mart",
            "BigQuery mart",
            "HIGH",
            5_000_000,
            5,
            False,
            ("gaming_slot_event",),
        ),
    ]


CUTOVER_CHECKS = (
    "source_watermark_frozen",
    "schema_snapshot_captured",
    "target_schema_deployed",
    "service_account_verified",
    "encryption_verified",
    "historical_backfill_complete",
    "incremental_catchup_complete",
    "dual_run_enabled",
    "source_target_counts_match",
    "checksums_match",
    "dq_thresholds_pass",
    "freshness_slo_pass",
    "consumer_lag_slo_pass",
    "dashboard_validated",
    "business_owner_signoff",
    "rollback_checkpoint_recorded",
    "route_switch_planned",
    "monitoring_alerts_enabled",
    "oncall_notified",
    "post_cutover_reconcile_scheduled",
)


def evaluate_cutover_checklist(
    context: Mapping[str, Any],
) -> dict[str, bool]:
    return {
        check: bool(context.get(check, False))
        for check in CUTOVER_CHECKS
    }


def checklist_passed(context: Mapping[str, Any]) -> bool:
    return all(evaluate_cutover_checklist(context).values())


def migration_summary(
    registry: MigrationRegistry,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for workload_id, workload in registry.workloads.items():
        rows.append(
            {
                "workload_id": workload_id,
                "domain": workload.domain,
                "status": registry.status[workload_id].value,
                "risk_score": risk_score(workload),
                "pattern": recommended_pattern(workload),
                "evidence_count": len(registry.evidence[workload_id]),
                "has_rollback_checkpoint": workload_id in registry.checkpoints,
            }
        )
    return sorted(
        rows,
        key=lambda row: (-row["risk_score"], row["workload_id"]),
    )
