from finops.optimizer import WorkloadCost, recommend


def test_low_utilization_workload_gets_rightsize_recommendation() -> None:
    results = recommend([WorkloadCost("batch-worker", 1000, 10, 100, 500)])
    assert results[0].action == "RIGHTSIZE"
    assert results[0].estimated_monthly_savings > 0
