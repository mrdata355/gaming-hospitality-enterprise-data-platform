from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class WorkItem:
    key: str
    title: str
    requester_department: str
    owner_department: str
    priority: str
    business_value: float
    risk_reduction: float
    urgency: float
    effort_hours: float
    due_date: date | None = None
    blocker_count: int = 0
    downstream_department_count: int = 0


def recommendation_score(item: WorkItem, *, today: date | None = None) -> float:
    today = today or date.today()
    value = item.business_value * 0.35 + item.risk_reduction * 0.30 + item.urgency * 0.20
    dependency = min(10.0, item.downstream_department_count * 1.5 + item.blocker_count * 2.0)
    due_pressure = 0.0
    if item.due_date:
        days = (item.due_date - today).days
        due_pressure = 10.0 if days <= 0 else max(0.0, 10.0 - days * 0.5)
    priority_bonus = {"P0": 15.0, "P1": 10.0, "P2": 5.0, "P3": 0.0}.get(item.priority, 0.0)
    effort_penalty = min(20.0, item.effort_hours / 8.0)
    return round(value + dependency + due_pressure + priority_bonus - effort_penalty, 2)
