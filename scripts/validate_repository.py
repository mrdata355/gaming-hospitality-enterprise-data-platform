#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

REQUIRED = [
    "sqlserver/ddl/001_control_framework.sql",
    "sqlserver/continuous_querying/operational_streaming_support.sql",
    "ssis/packages/HotelReservations_Incremental.dtsx",
    "ssis/continuous_handoff/CDC_To_EventBus_Continuous_Handoff.dtsx.xml",
    "db2/sql/hotel_reservation_incremental.sql",
    "adf/factory/pipeline/pl_hotel_db2_to_cloud.json",
    "gcp/terraform/main.tf",
    "gcp/pubsub/production_reference.py",
    "gcp/dataflow/continuous_streaming_pipeline.py",
    "azure/streaming/stream_analytics_continuous_queries.sql",
    "streaming/continuous_querying/README.md",
    "streaming/continuous_querying/continuous_streaming_reference.py",
    "src/resort_platform/services/event_gateway.py",
    "contracts/slot_play_event.v1.schema.json",
    "dbt/models/marts/fct_gaming_performance_5min.sql",
    "observability/slo.yml",
]

root = Path(__file__).resolve().parents[1]
missing = [path for path in REQUIRED if not (root / path).exists()]
if missing:
    raise SystemExit(f"Missing required platform assets: {missing}")

for path in root.rglob("*.json"):
    json.loads(path.read_text())

print(f"Repository validation passed: {len(REQUIRED)} architecture anchors + JSON syntax.")
