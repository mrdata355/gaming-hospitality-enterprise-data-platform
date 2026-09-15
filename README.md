# Gaming & Hospitality Enterprise Data Platform

A synthetic, production-style portfolio platform for modernizing gaming and hospitality data workloads across on-premises SQL Server/SSIS and DB2, Azure Data Factory orchestration, and Google Cloud event-driven data services.

> **Portfolio / generated-data notice:** Public property names and public addresses are used only as business context. All guests, players, reservations, gaming sessions, loyalty events, transactions, source-system names, identifiers, volumes, SLAs, topics, buckets, schemas, internal paths, server names, service accounts, and infrastructure conventions in this repository are generated or project-owned. This repository does not claim employment at The Venetian, access to private Venetian systems, or knowledge of proprietary schemas.

## Public property anchors

- **The Venetian Resort Hotel Casino** — 3355 Las Vegas Blvd. South, Las Vegas, NV 89109.
- **The Palazzo Resort Hotel Casino** — 3325 S Las Vegas Blvd, Las Vegas, NV 89109.
- Publicly described areas such as the Venetian/Palazzo towers, Convention & Expo Center, Restaurant Row, and the pedestrian connection toward Sphere are represented as logical analytics zones in `config/property_topology.yml`.

## Role-aligned stack

- **SQL / SQL Server** — dimensional warehouse, control framework, CDC watermarks, MERGE procedures, reconciliation, performance tuning
- **SSIS** — DB2 + SQL Server ingestion, incremental loads, restartability, package logging, quarantine paths
- **Python** — reusable ingestion framework, contract validation, Pub/Sub producers/consumers, merge logic, reconciliation, operational tooling
- **DB2** — generated hotel/gaming/rewards/payment/operations source extracts and change-watermark patterns
- **Azure Data Factory** — orchestration, parameterized pipelines, dependency handling, validation gates, migration bridge
- **Google Cloud** — Pub/Sub, Cloud Run, Cloud Storage zones, BigQuery serving layers, IAM and observability
- **Event-driven architecture** — gaming, hotel, rewards, mobile, property transaction and operations topics
- **Cloud migration** — inventory, dependency waves, dual-run validation, cutover evidence and rollback checkpoints
- **AI/ML** — guest-value features, anomaly detection, propensity scoring, drift monitoring and operational agents

## Long-lived architecture

```text
DB2 hotel/gaming/rewards          SQL Server finance/operations
          |                                  |
          +--------> SSIS / Python <---------+
                       |
               SQL Server control/audit
                       |
                Azure Data Factory
                       |
          +------------+-------------+
          |                          |
       batch files                 events
          |                          |
          v                          v
      GCS landing                Pub/Sub
          |                          |
          +-------> RAW <------------+
                     |
                  CLEAN
                     |
                  MERGE
                     |
                 CURATED
                     |
          +----------+----------+
          |                     |
       BigQuery            operational APIs
          |                     |
          +-------- command center
```

## Data lake layout

```text
lake/raw/       Immutable source-preserving events and batch rows
lake/clean/     Contract-validated normalized rows
lake/merge/     Deterministic current-state/history merge rules
lake/curated/   Business-serving facts, marts and Guest 360 subjects
```

Synthetic URI convention:

```text
gs://vh-portfolio-{env}-raw-{region}/{domain}/{entity}/business_date={yyyy-mm-dd}/hour={hh}/
gs://vh-portfolio-{env}-clean-{region}/{domain}/{entity}/business_date={yyyy-mm-dd}/hour={hh}/
gs://vh-portfolio-{env}-merge-{region}/{domain}/{entity}/business_date={yyyy-mm-dd}/
gs://vh-portfolio-{env}-curated-{region}/{subject}/business_date={yyyy-mm-dd}/
```

## Repository map

```text
config/                     Public property anchors + generated internal topology
lake/raw/                   Raw schemas and generated sample events
lake/clean/                 Quality contracts and normalization rules
lake/merge/                 Deterministic merge/reconciliation policy
lake/curated/               Business-serving model contracts
ssis/production_reference/  Deep restartable SSIS package reference
sqlserver/production_reference/ SQL Server schemas, procedures, merge and audit
src/resort_platform/        Production Python runtime and reference implementation
db2/production_reference/   Bounded DB2 extraction and source diagnostics
adf/production_reference/   Hybrid migration orchestration
GCP note: see gcp/production_reference/ and gcp/pubsub/
gcp/production_reference/   Storage, Pub/Sub, BigQuery, Cloud Run and IAM Terraform
gcp/pubsub/                 Event publisher/consumer reference patterns
cloud_migration/            Migration waves, dual-run, evidence, cutover and rollback
contracts/                  Event and batch data contracts
warehouse/                  Conformed dimensions, facts and semantic warehouse
analytics/                  Gaming/hotel/rewards operational SQL
dbt/                        Semantic transformation project
mlops/                      Features, training, model monitoring and promotion
observability/              SLOs, health queries, lineage and incident signals
agents/                     Evidence-backed operational assistants
tests/                      Unit, contract, reconciliation and architecture tests
docs/                       Architecture, runbooks and walkthroughs
```

## Core event topics

```text
guest.mobile-events.v1
hotel.reservation-events.v1
hotel.checkin-events.v1
gaming.slot-play-events.v1
gaming.table-play-events.v1
rewards.earn-events.v1
rewards.redemption-events.v1
marketing.offer-events.v1
property.transaction-events.v1
ops.pipeline-events.v1
```

## Interview-first folders

For the Senior Data Engineer role, use this order:

1. `ssis/production_reference/` — required SSIS competency, watermarks, restartability, DQ and reconciliation
2. `sqlserver/production_reference/` — SQL engineering, control tables, MERGE, canonical models and audit
3. `src/resort_platform/production_reference.py` — advanced Python, contracts, paths, dedupe, merge and replay-safe processing
4. `db2/production_reference/` — bounded DB2 extraction, source profiling and watermark patterns
5. `adf/production_reference/` — cloud migration orchestration and dependency gates
6. `gcp/production_reference/` — GCS, BigQuery, Cloud Run, IAM and lifecycle management
7. `gcp/pubsub/production_reference.py` — event-driven Pub/Sub patterns, ordering, DLQ, retry and idempotency
8. `cloud_migration/production_reference/` — migration wave control, dual-run validation, cutover and rollback
9. `lake/raw/`, `lake/clean/`, `lake/merge/`, `lake/curated/` — complete data lifecycle
10. `contracts/`, `observability/`, `tests/` — governance and operational proof

The strongest interview narrative is: **legacy DB2/SQL Server + SSIS remains controlled and restartable while ADF coordinates modernization into a GCP event-driven platform; raw data is preserved, clean data is contract validated, merge is deterministic and replay-safe, curated models are reproducible, and every run is reconciled.**
