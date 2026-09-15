from __future__ import annotations

from dataclasses import dataclass, replace
from random import Random
from statistics import mean, quantiles


@dataclass(frozen=True)
class Scenario:
    name: str
    reservations_per_hour: int = 120
    gaming_sessions_per_hour: int = 900
    mobile_events_per_hour: int = 5000
    hotel_pipeline_latency_seconds: float = 240.0
    gaming_pipeline_latency_seconds: float = 45.0
    offer_response_lift: float = 0.0
    outage_minutes: int = 0


@dataclass(frozen=True)
class TwinResult:
    scenario: str
    reservations_processed: int
    gaming_sessions_processed: int
    mobile_events_processed: int
    mean_latency_seconds: float
    p95_latency_seconds: float
    estimated_offer_responses: int
    lost_events: int


def simulate(scenario: Scenario, *, hours: int = 24, seed: int = 42) -> TwinResult:
    rng = Random(seed)
    latencies: list[float] = []
    lost_events = 0
    reservations = gaming = mobile = responses = 0
    outage_fraction = min(max(scenario.outage_minutes / (hours * 60), 0), 1)

    for _ in range(hours):
        hourly_res = max(0, int(rng.gauss(scenario.reservations_per_hour, scenario.reservations_per_hour * 0.12)))
        hourly_gaming = max(0, int(rng.gauss(scenario.gaming_sessions_per_hour, scenario.gaming_sessions_per_hour * 0.15)))
        hourly_mobile = max(0, int(rng.gauss(scenario.mobile_events_per_hour, scenario.mobile_events_per_hour * 0.18)))
        reservations += hourly_res
        gaming += hourly_gaming
        mobile += hourly_mobile
        for _ in range(hourly_res):
            latencies.append(max(0.01, rng.lognormvariate(0, 0.25) * scenario.hotel_pipeline_latency_seconds))
        for _ in range(min(hourly_gaming, 1000)):
            latencies.append(max(0.01, rng.lognormvariate(0, 0.20) * scenario.gaming_pipeline_latency_seconds))
        responses += int(hourly_mobile * 0.01 * (1 + scenario.offer_response_lift))

    total_events = reservations + gaming + mobile
    lost_events = int(total_events * outage_fraction * 0.002)
    p95 = quantiles(latencies, n=100)[94] if len(latencies) >= 100 else max(latencies, default=0)
    return TwinResult(
        scenario=scenario.name,
        reservations_processed=reservations,
        gaming_sessions_processed=gaming,
        mobile_events_processed=mobile,
        mean_latency_seconds=mean(latencies) if latencies else 0,
        p95_latency_seconds=p95,
        estimated_offer_responses=responses,
        lost_events=lost_events,
    )


def paired_experiment(baseline: Scenario, candidate: Scenario, *, hours: int = 24, seed: int = 42) -> dict[str, object]:
    base = simulate(baseline, hours=hours, seed=seed)
    cand = simulate(candidate, hours=hours, seed=seed)
    return {
        "baseline": base,
        "candidate": cand,
        "latency_improvement_pct": round(100 * (base.p95_latency_seconds - cand.p95_latency_seconds) / max(base.p95_latency_seconds, 1e-9), 2),
        "additional_offer_responses": cand.estimated_offer_responses - base.estimated_offer_responses,
        "lost_event_delta": cand.lost_events - base.lost_events,
    }


def modernization_candidate(baseline: Scenario) -> Scenario:
    return replace(
        baseline,
        name=f"{baseline.name}-cloud-modernized",
        hotel_pipeline_latency_seconds=max(30, baseline.hotel_pipeline_latency_seconds * 0.45),
        gaming_pipeline_latency_seconds=max(5, baseline.gaming_pipeline_latency_seconds * 0.35),
        offer_response_lift=baseline.offer_response_lift + 0.12,
    )
