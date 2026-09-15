# 5-minute interview walkthrough

## 1. SSIS / DB2 / SQL Server
Open `ssis/packages/HotelReservations_Incremental.dtsx`, then `db2/sql/hotel_reservation_incremental.sql` and `sqlserver/procedures/usp_merge_reservation.sql`.

Say: **“The legacy path is restartable. I capture a fixed DB2 high watermark, stage the bounded range, quarantine invalid rows, perform an idempotent SQL Server MERGE, reconcile counts/revenue, and only then commit the watermark.”**

## 2. Python
Open `src/resort_platform/services/event_gateway.py`, `pubsub.py`, `quality.py` and `reconciliation.py`.

Say: **“I treat Python as production software—typed models, configuration, contract enforcement, idempotency, storage preconditions and tests—not notebook glue.”**

## 3. Event-driven GCP
Open `data/topic_catalog.json`, `gcp/terraform/main.tf` and the event gateway.

Say: **“The event path lands an immutable replay object before Pub/Sub publication, carries event/trace metadata, uses DLQs and bounded retries, and separates topic keys from analytical business keys.”**

## 4. ADF cloud migration
Open `adf/factory/pipeline/pl_enterprise_migration_orchestrator.json`.

Say: **“ADF is the migration control plane. It fans out independent domains and fans in only after each bounded source range succeeds.”**

## 5. Data products + AI
Open `dbt/models/marts/`, `mlops/`, `observability/`, and `digital_twin/`.

Say: **“I don't stop at ingestion. The platform publishes conformed hotel/gaming/guest products, monitors freshness and reconciliation, attributes cost, and evaluates model/data drift.”**
