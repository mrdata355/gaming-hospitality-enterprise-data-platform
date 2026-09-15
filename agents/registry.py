from __future__ import annotations

from dataclasses import dataclass
from itertools import product


DOMAINS = (
    "Data Platform", "Hotel", "Gaming", "Loyalty", "Digital Product", "Marketing",
    "Finance", "Revenue Management", "Property Operations", "Security", "Compliance",
    "Data Science", "SRE", "Executive Analytics"
)
SPECIALIZATIONS = (
    "Sentinel", "Forecaster", "Optimizer", "Reconciler",
    "Contract Guardian", "Incident Analyst", "Capacity Planner", "Portfolio Advisor"
)


@dataclass(frozen=True)
class AgentProfile:
    agent_id: str
    domain: str
    specialization: str
    objective: str
    permitted_actions: tuple[str, ...]


def build_registry() -> tuple[AgentProfile, ...]:
    profiles: list[AgentProfile] = []
    for domain, specialization in product(DOMAINS, SPECIALIZATIONS):
        slug = f"{domain}-{specialization}".lower().replace(" ", "-")
        profiles.append(
            AgentProfile(
                agent_id=slug,
                domain=domain,
                specialization=specialization,
                objective=f"Provide evidence-backed {specialization.lower()} recommendations for {domain}.",
                permitted_actions=("READ_METRICS", "READ_CONTRACTS", "READ_WORK_ITEMS", "PROPOSE_RECOMMENDATION"),
            )
        )
    return tuple(profiles)


REGISTRY = build_registry()
assert len(REGISTRY) == 112
