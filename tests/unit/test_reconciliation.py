from decimal import Decimal

from resort_platform.reconciliation import reconcile


def test_explained_quarantine_balances() -> None:
    result = reconcile(
        "hotel",
        source_count=100,
        target_count=97,
        source_amount=Decimal("1000"),
        target_amount=Decimal("970"),
        explained_count=3,
        explained_amount=Decimal("30"),
    )
    assert result.balanced
    assert result.unexplained_count_delta == 0
