# Gaming & Hospitality Enterprise Data Platform

A synthetic, production-style portfolio platform for modernizing gaming and hospitality data workloads across on-premises SQL Server/SSIS and DB2, Azure orchestration/stream processing, and Google Cloud event-driven data services.

> **Portfolio / generated-data notice:** Public property names and public addresses are used only as business context. All guests, players, reservations, gaming sessions, loyalty events, transactions, source-system names, identifiers, volumes, SLAs, topics, buckets, schemas, internal paths, server names, service accounts, and infrastructure conventions in this repository are generated or project-owned. This repository does not claim employment at The Venetian, access to private Venetian systems, or knowledge of proprietary schemas.

## Public property anchors

- **The Venetian Resort Hotel Casino** — 3355 Las Vegas Blvd. South, Las Vegas, NV 89109.
- **The Palazzo Resort Hotel Casino** — 3325 S Las Vegas Blvd, Las Vegas, NV 89109.
- Publicly described areas such as the Venetian/Palazzo towers, Convention & Expo Center, Restaurant Row, and the pedestrian connection toward Sphere are represented as logical analytics zones in `config/property_topology.yml`.

## Role-aligned stack

- **SQL / SQL Server** — dimensional warehouse, control framework, CDC watermarks, transactional outbox, idempotent MERGE, reconciliation, performance tuning
- **SSIS** — DB2 + SQL Server ingestion, incremental CDC, restartability, package logging, quarantine, event-bus handoff
- **Continuous streaming / continuous querying** — event time, watermarks, stateful dedupe, windows, checkpoints, idempotent sinks, replay, lag/throughput/state monitoring
- **Python** — reusable ingestion framework, contract validation, streaming runtimes, Pub/Sub producers/consumers, merge logic, reconciliation, operational tooling
- **DB2** — generated hotel/gaming/rewards/payment/operations source extracts and change-watermark patterns
- **Azure Data Factory** — orchestration, parameterized pipelines, dependency handling, validation gates, migration bridge and replay coordination
- **Azure streaming** — Event Hubs / Stream Analytics continuous SQL examples with tumbling, hopping and session windows
- **Google Cloud** — Pub/Sub, Dataflow, Cloud Run, Cloud Storage zones, BigQuery serving layers, IAM and observability
- **Event-driven architecture** — gaming, hotel, rewards, mobile, property transaction and operations topics
- **Cloud migration** — inventory, dependency waves, dual-run validation, cutover evidence and rollback checkpoints
- **AI/ML** — guest-value features, anomaly detection, propensity scoring, drift monitoring and operational agents

## Long-lived architecture

```text
DB2 hotel/gaming/rewards          SQL Server finance/operations
          |                                  |
          +--------> SSIS / Python <---------+
                       |
              transactional outbox
                       |
          +------------+-------------+
          |                          |
          v                          v
      GCP Pub/Sub                Azure Event Hubs
          |                          |
          v                          v
       Dataflow                Stream Analytics
          |                          |
          +------------+-------------+
                       |
                     RAW
                       |
                    CLEAN
                       |
                    MERGE
                       |
                   CURATED
                       |
          +------------+-------------+
          |                          |
       BigQuery               operational SQL
          |                          |
          +---------- analytics / APIs

ADF = migration, dependency, batch and replay orchestration
Continuous engines = Dataflow / Stream Analytics / Spark Structured Streaming
```

## Continuous-query sequence

```text
SOURCE
  -> CONTRACT
  -> EVENT TIME
  -> WATERMARK
  -> DQ / QUARANTINE
  -> STABLE EVENT-ID DEDUPE
  -> WINDOW / STATE
  -> CHECKPOINT
  -> IDEMPOTENT / TRANSACTIONAL SINK
  -> LAG + THROUGHPUT + STATE HEALTH
  -> REPLAY + RECONCILIATION
```

The repo explicitly distinguishes broad **continuous streaming** from Spark's specific **Continuous Processing** trigger. Stateful Structured Streaming workloads here use micro-batch execution when watermarking, dedupe, state, and transactional `foreachBatch` merge semantics are required.

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
streaming/continuous_querying/    Continuous query concepts + Structured Streaming runtime
ssis/production_reference/        Restartable SSIS package reference
ssis/continuous_handoff/          CDC -> transactional outbox -> event-bus handoff
sqlserver/production_reference/   SQL Server schemas, procedures, merge and audit
sqlserver/continuous_querying/    Outbox, idempotency, stream audit, replay and reconciliation
gcp/pubsub/                       Pub/Sub publisher/consumer patterns
gcp/dataflow/                     Pub/Sub -> Dataflow continuous processing
gcp/production_reference/         GCS, BigQuery, Cloud Run and IAM Terraform
azure/streaming/                   Stream Analytics continuous SQL patterns
adf/production_reference/         Hybrid migration orchestration
src/resort_platform/              Production Python runtime and reference implementation
db2/production_reference/         Bounded DB2 extraction and source diagnostics
cloud_migration/                  Migration waves, dual-run, cutover and rollback
config/                           Public property anchors + generated internal topology
lake/raw/                         Raw schemas and generated sample events
lake/clean/                       Quality contracts and normalization rules
lake/merge/                       Deterministic merge/reconciliation policy
lake/curated/                     Business-serving model contracts
contracts/                        Event and batch data contracts
warehouse/                        Conformed dimensions, facts and semantic warehouse
analytics/                        Gaming/hotel/rewards operational SQL
dbt/                              Semantic transformation project
mlops/                            Features, training, model monitoring and promotion
observability/                    SLOs, health queries, lineage and incident signals
agents/                           Evidence-backed operational assistants
tests/                            Unit, contract, reconciliation and architecture tests
docs/                             Architecture, runbooks and walkthroughs
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

## Interview-first folders after the latest feedback

The strongest walkthrough order is now:

1. `ssis/production_reference/` + `ssis/continuous_handoff/` — required SSIS skill plus the correct boundary between legacy CDC and a true event-streaming engine
2. `sqlserver/production_reference/` + `sqlserver/continuous_querying/` — SQL engineering, MERGE, outbox, idempotency, stream audit and reconciliation
3. `streaming/continuous_querying/` — continuous query definition, event time, watermarks, state, checkpoints, replay, lag and Spark execution-mode distinction
4. `gcp/pubsub/` + `gcp/dataflow/` — GCP Pub/Sub, windows, lateness, Dataflow continuous processing and quarantine
5. `azure/streaming/` + `adf/production_reference/` — Stream Analytics continuous SQL plus ADF orchestration/migration
6. `src/resort_platform/production_reference.py` — advanced Python, contracts, paths, dedupe, merge and replay-safe processing
7. `db2/production_reference/` — bounded DB2 extraction, source profiling and watermark patterns
8. `cloud_migration/production_reference/` — migration wave control, dual-run validation, cutover and rollback
9. `lake/raw/`, `lake/clean/`, `lake/merge/`, `lake/curated/` — complete source-to-serving lifecycle
10. `contracts/`, `observability/`, `tests/` — governance and operational proof

## Strong interview narrative

**Legacy DB2/SQL Server + SSIS remains controlled and restartable. SSIS captures bounded CDC and hands canonical events to a transactional outbox. Pub/Sub/Dataflow or Event Hubs/Stream Analytics owns the continuously running stream. Event-time watermarks bound state, stable event IDs make retries safe, checkpoints preserve progress, invalid records are quarantined, current-state writes are version-aware and idempotent, and every run is reconciled. ADF coordinates migration, dependencies and replay rather than pretending to be the stateful streaming engine.**
