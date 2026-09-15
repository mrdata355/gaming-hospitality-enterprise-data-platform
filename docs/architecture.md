# Architecture

## Why the platform is hybrid

The portfolio models the migration problem described by the role: mature DB2 + SQL Server/SSIS workloads must continue operating while enterprise data products move toward a cloud lake and event-driven services.

The design therefore avoids a flag-day rewrite.

### Control plane

- SQL Server `ctl.pipeline_watermark` owns replayable source ranges for legacy batch ingestion.
- SSIS remains a first-class runtime for required/on-prem workloads.
- ADF orchestrates dependencies, hybrid integration runtimes and cloud-migration waves.
- Pub/Sub is the asynchronous event backbone for gaming, hotel, rewards, mobile and operations events.
- Cloud Run performs stateless validation/routing services.
- GCS buckets form the raw/bronze/silver/gold/quarantine object boundaries.
- BigQuery/dbt provides a cloud semantic/serving example without replacing the SQL Server EDW overnight.

## Reliability invariants

1. **Raw is replayable.** A source range or immutable landing object is preserved before derived state changes.
2. **Keys are explicit.** Event ID, source sequence, reservation ID, session ID and guest token serve different purposes and are not conflated.
3. **No watermark on partial success.** Legacy batch watermarks advance only after merge + reconciliation succeeds.
4. **No silent data loss.** Invalid records are quarantined with reason codes and source context.
5. **Event consumers are idempotent.** Stable event IDs and target uniqueness make redelivery safe.
6. **Sensitive identifiers are tokenized before analytical/event layers.**
7. **Cloud migration is observable.** Every pipeline publishes execution, DQ and reconciliation evidence.

## Patterns ported from the owner's casino platform

The repository intentionally reuses the strongest concepts from the existing casino streaming/AI project: versioned event contracts, slot/gaming stream semantics, hotel occupancy marts, guest 360, ML drift checks, semantic metrics, SLOs, FinOps recommendations and a command-center/digital-twin mindset. Snowflake-specific implementation details were not copied blindly; they were translated into this role's SQL Server + SSIS + DB2 + ADF + GCP stack.
