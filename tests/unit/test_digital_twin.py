from digital_twin.resort_twin import Scenario, modernization_candidate, paired_experiment


def test_modernization_scenario_improves_p95_latency() -> None:
    baseline = Scenario(name="baseline")
    candidate = modernization_candidate(baseline)
    result = paired_experiment(baseline, candidate, hours=2, seed=7)
    assert result["latency_improvement_pct"] > 0
