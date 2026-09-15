from __future__ import annotations

import pandas as pd

FEATURES = [
    "days_since_last_visit",
    "gaming_sessions_30d",
    "coin_in_30d",
    "actual_win_30d",
    "hotel_nights_90d",
    "room_revenue_90d",
    "reward_redemptions_90d",
    "mobile_sessions_30d",
    "offer_views_30d",
]


def build_guest_value_features(frame: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(set(FEATURES) - set(frame.columns))
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")
    result = frame.copy()
    result[FEATURES] = result[FEATURES].fillna(0)
    result["engagement_intensity"] = (
        result["gaming_sessions_30d"]
        + result["mobile_sessions_30d"] * 0.25
        + result["offer_views_30d"] * 0.10
    )
    result["cross_domain_value"] = result["coin_in_30d"] + result["room_revenue_90d"]
    return result
