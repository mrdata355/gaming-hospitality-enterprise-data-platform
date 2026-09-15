from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

Predicate = Callable[[dict[str, Any]], bool]


@dataclass(frozen=True)
class Rule:
    name: str
    predicate: Predicate
    severity: str = "ERROR"
    description: str = ""


@dataclass(frozen=True)
class Violation:
    rule_name: str
    severity: str
    description: str


@dataclass(frozen=True)
class QualityResult:
    valid: bool
    evaluated_at: datetime
    violations: tuple[Violation, ...]


class QualityEngine:
    """Composable record-level contract checks used by batch and event ingestion."""

    def __init__(self, rules: Iterable[Rule]):
        self.rules = tuple(rules)

    def evaluate(self, record: dict[str, Any]) -> QualityResult:
        violations: list[Violation] = []
        for rule in self.rules:
            try:
                passed = bool(rule.predicate(record))
            except Exception:
                passed = False
            if not passed:
                violations.append(Violation(rule.name, rule.severity, rule.description))
        return QualityResult(
            valid=not any(v.severity == "ERROR" for v in violations),
            evaluated_at=datetime.now(timezone.utc),
            violations=tuple(violations),
        )


def required(*fields: str) -> Rule:
    return Rule(
        name=f"required:{','.join(fields)}",
        description="Required business key or event metadata is missing.",
        predicate=lambda row: all(row.get(field) not in (None, "") for field in fields),
    )


def allowed(field: str, values: set[str]) -> Rule:
    return Rule(
        name=f"allowed:{field}",
        description=f"{field} must be in the governed domain.",
        predicate=lambda row: str(row.get(field, "")).upper() in values,
    )


def non_negative(field: str) -> Rule:
    return Rule(
        name=f"non_negative:{field}",
        description=f"{field} cannot be negative.",
        predicate=lambda row: row.get(field) is not None and float(row[field]) >= 0,
    )


def no_plaintext_sensitive_fields() -> Rule:
    blocked = {"ssn", "tax_id", "credit_card", "pan", "passport_number", "drivers_license"}
    return Rule(
        name="sensitive_data_tokenization",
        description="Raw sensitive identifiers may not enter analytics/event payloads.",
        predicate=lambda row: not any(key.lower() in blocked and row.get(key) for key in row),
    )
