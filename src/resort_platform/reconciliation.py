from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping


@dataclass(frozen=True)
class ReconciliationResult:
    control_name: str
    source_count: int
    target_count: int
    source_amount: Decimal
    target_amount: Decimal
    explained_count: int = 0
    explained_amount: Decimal = Decimal("0")

    @property
    def unexplained_count_delta(self) -> int:
        return self.source_count - self.target_count - self.explained_count

    @property
    def unexplained_amount_delta(self) -> Decimal:
        return self.source_amount - self.target_amount - self.explained_amount

    @property
    def balanced(self) -> bool:
        return self.unexplained_count_delta == 0 and self.unexplained_amount_delta == Decimal("0")

    def evidence(self) -> Mapping[str, object]:
        return {
            "control_name": self.control_name,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "explained_count": self.explained_count,
            "unexplained_count_delta": self.unexplained_count_delta,
            "source_amount": str(self.source_amount),
            "target_amount": str(self.target_amount),
            "explained_amount": str(self.explained_amount),
            "unexplained_amount_delta": str(self.unexplained_amount_delta),
            "balanced": self.balanced,
        }


def reconcile(
    control_name: str,
    *,
    source_count: int,
    target_count: int,
    source_amount: Decimal = Decimal("0"),
    target_amount: Decimal = Decimal("0"),
    explained_count: int = 0,
    explained_amount: Decimal = Decimal("0"),
) -> ReconciliationResult:
    return ReconciliationResult(
        control_name=control_name,
        source_count=source_count,
        target_count=target_count,
        source_amount=source_amount,
        target_amount=target_amount,
        explained_count=explained_count,
        explained_amount=explained_amount,
    )
