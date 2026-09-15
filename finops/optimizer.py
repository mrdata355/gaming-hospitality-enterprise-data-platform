from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class WorkloadCost:
    name: str
    monthly_cost: float
    utilization_pct: float
    p95_latency_ms: float
    slo_latency_ms: float


@dataclass(frozen=True)
class Recommendation:
    workload: str
    action: str
    estimated_monthly_savings: float
    risk: str


def recommend(workloads: Iterable[WorkloadCost]) -> list[Recommendation]:
    recommendations: list[Recommendation] = []
    for workload in workloads:
        if workload.utilization_pct < 15 and workload.p95_latency_ms < workload.slo_latency_ms * 0.5:
            recommendations.append(Recommendation(workload.name, "RIGHTSIZE", workload.monthly_cost * 0.25, "LOW"))
        elif workload.utilization_pct < 5:
            recommendations.append(Recommendation(workload.name, "SCHEDULE_OR_SCALE_TO_ZERO", workload.monthly_cost * 0.50, "MEDIUM"))
        elif workload.p95_latency_ms > workload.slo_latency_ms:
            recommendations.append(Recommendation(workload.name, "PERFORMANCE_FIRST_NO_COST_CUT", 0.0, "HIGH"))
    return recommendations
