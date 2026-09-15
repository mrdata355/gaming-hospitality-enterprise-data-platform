from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Signal:
    name: str
    value: float
    threshold: float
    direction: str
    evidence: str

    @property
    def breached(self) -> bool:
        return self.value > self.threshold if self.direction == "MAX" else self.value < self.threshold


@dataclass(frozen=True)
class Recommendation:
    owner_domain: str
    title: str
    priority: float
    rationale: str
    evidence: tuple[str, ...]


def recommend(owner_domain: str, signals: list[Signal]) -> list[Recommendation]:
    breached = [s for s in signals if s.breached]
    recommendations = []
    for signal in breached:
        severity = abs(signal.value - signal.threshold) / max(abs(signal.threshold), 1e-9)
        recommendations.append(
            Recommendation(
                owner_domain=owner_domain,
                title=f"Investigate {signal.name}",
                priority=min(100.0, 50 + severity * 50),
                rationale=f"Observed {signal.value:.4f} against {signal.direction} threshold {signal.threshold:.4f}.",
                evidence=(signal.evidence,),
            )
        )
    return sorted(recommendations, key=lambda item: item.priority, reverse=True)
