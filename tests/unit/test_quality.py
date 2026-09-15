from resort_platform.quality import QualityEngine, allowed, no_plaintext_sensitive_fields, non_negative, required


def test_valid_gaming_record_passes() -> None:
    engine = QualityEngine([
        required("event_id", "session_id"),
        allowed("event_type", {"SLOT_PLAY", "TABLE_PLAY"}),
        non_negative("coin_in"),
        no_plaintext_sensitive_fields(),
    ])
    result = engine.evaluate({"event_id": "e1", "session_id": "s1", "event_type": "SLOT_PLAY", "coin_in": 20})
    assert result.valid


def test_plaintext_sensitive_identifier_fails() -> None:
    engine = QualityEngine([no_plaintext_sensitive_fields()])
    result = engine.evaluate({"event_id": "e1", "ssn": "000-00-0000"})
    assert not result.valid
