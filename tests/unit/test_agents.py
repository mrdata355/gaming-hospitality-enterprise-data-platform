from agents.registry import REGISTRY


def test_agent_registry_is_unique_and_complete() -> None:
    assert len(REGISTRY) == 112
    ids = [agent.agent_id for agent in REGISTRY]
    assert len(ids) == len(set(ids))
