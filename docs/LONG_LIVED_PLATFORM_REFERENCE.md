# Long-Lived Enterprise Platform Reference

This repository is intentionally designed to look mature and production-oriented, but it is a portfolio simulation. It does **not** claim employment at The Venetian, access to private systems, or knowledge of proprietary schemas.

## Public property anchors used

- The Venetian Resort Hotel Casino — 3355 Las Vegas Blvd. South, Las Vegas, NV 89109.
- The Palazzo Resort Hotel Casino — 3325 S Las Vegas Blvd, Las Vegas, NV 89109.
- Publicly described campus areas are used as logical analytics zones: The Venetian / Palazzo towers, Convention & Expo Center, Restaurant Row, and the public pedestrian connection toward Sphere.

## Synthetic internal conventions

- SQL Server schemas: `ctl/raw/stg/core/curated/mart/dq/audit`
- DB2 libraries: `HOSPITALITY/GAMING/LOYALTY/FINANCE/PROPERTY_OPS`
- GCS zones: `landing/raw/clean/merge/curated/quarantine/replay/checkpoints`
- Pub/Sub domains: `guest/hotel/gaming/rewards/marketing/payments/property-ops/observability`
- ADF: parameterized migration orchestrators
- SSIS: restartable bounded-watermark packages
- Canonical merge policy: business key + source version + source updated timestamp
- Reconciliation: source = clean + quarantine + duplicate/stale explanations

## Interview walkthrough order

1. `ssis/production_reference/`
2. `sqlserver/production_reference/`
3. `src/resort_platform/production_reference.py`
4. `db2/production_reference/`
5. `adf/production_reference/`
6. `gcp/production_reference/`
7. `gcp/pubsub/production_reference.py`
8. `cloud_migration/production_reference/`
9. `lake/raw/`, `lake/clean/`, `lake/merge/`, `lake/curated/`
10. `contracts/`, `observability/`, `tests/`

## Why this ordering

SSIS, SQL, Python and DB2 are the role's highest-signal required skills. ADF and GCP show modernization. Pub/Sub demonstrates event-driven design. The lake folders show how immutable raw data becomes validated clean data, then deterministic current-state merges, then curated business-serving models.

## Long-lived platform behaviors modeled

- Bounded high-watermark extraction so batch reruns are deterministic.
- Immutable raw retention before normalization.
- Explicit schema versions and contract validation.
- Dead-letter and quarantine handling rather than silent drops.
- Idempotent event processing using stable event IDs.
- Deterministic merge ordering using version and source update time.
- Source-to-target reconciliation on every run.
- Replay paths that do not bypass DQ or lineage.
- Runtime configuration separated from source code.
- Environment-specific secrets referenced from managed secret stores.
- Blue/green and dual-run migration patterns for critical domains.
- Dependency-aware cutovers and documented rollback checkpoints.
- Observability around freshness, lag, duplicates, quarantine, and reconciliation.
- Data-serving models separated from ingestion and conformance layers.
