from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration with secrets supplied only through environment variables."""

    model_config = SettingsConfigDict(env_prefix="RESORT_", env_file=".env", extra="ignore")

    environment: str = "dev"
    gcp_project_id: str = "gaming-hospitality-dev"
    gcp_region: str = "us-west1"
    gcs_landing_bucket: str = "gh-dev-landing"
    gcs_bronze_bucket: str = "gh-dev-bronze"
    gcs_silver_bucket: str = "gh-dev-silver"
    gcs_gold_bucket: str = "gh-dev-gold"
    gcs_quarantine_bucket: str = "gh-dev-quarantine"

    sqlserver_dsn: str = "GamingHospitalityDW"
    sqlserver_control_schema: str = "ctl"
    db2_dsn: str = "HospitalitySource"

    pubsub_slot_topic: str = "gaming.slot-play-events.v1"
    pubsub_table_topic: str = "gaming.table-play-events.v1"
    pubsub_reservation_topic: str = "hotel.reservation-events.v1"
    pubsub_mobile_topic: str = "guest.mobile-events.v1"
    pubsub_rewards_topic: str = "rewards.earn-events.v1"

    batch_rows: int = Field(default=25_000, ge=100, le=1_000_000)
    publish_timeout_seconds: int = Field(default=30, ge=1, le=300)
    watermark_lag_minutes: int = Field(default=10, ge=1, le=1440)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
