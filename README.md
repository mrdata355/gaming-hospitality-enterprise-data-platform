# Gaming & Hospitality Enterprise Data Platform

A synthetic, production-style portfolio platform for modernizing gaming and hospitality data workloads across on-premises SQL Server/SSIS and DB2, Azure Data Factory orchestration, and Google Cloud event-driven data services.

> **Portfolio / generated-data notice:** All guests, players, reservations, gaming sessions, loyalty events, transactions, source-system names, identifiers, volumes, SLAs, topics, buckets, schemas, and infrastructure names in this repository are generated or project-owned. This repository does not represent The Venetian Resort Las Vegas private schemas, customer data, internal architecture, or proprietary systems.

## Role-aligned stack

- **SQL / SQL Server** — dimensional warehouse, control framework, CDC watermarks, MERGE procedures, reconciliation, performance tuning
- **SSIS** — DB2 + SQL Server ingestion, incremental loads, restartability, package logging, quarantine paths
- **Python** — reusable ingestion framework, contract validation, Pub/Sub producers/consumers, Cloud Run services, reconciliation and operational tooling
- **DB2** — generated hotel/gaming source extracts and change-watermark patterns
- **Azure Data Factory** — orchestration, parameterized pipelines, dependency handling and migration bridge
- **Google Cloud** — Pub/Sub, Cloud Run, Cloud Storage medallion lake, BigQuery serving examples, observability
- **Event-driven architecture** — gaming, hotel, rewards, mobile and property-operation topics
- **AI/ML** — guest-value features, anomaly detection, propensity scoring, drift monitoring and operational agents

## Architecture

```text
DB2 hotel + gaming sources         SQL Server enterprise systems
            \                         /
             \                       /
              +--> SSIS / Python ETL +
                         |
                  SQL Server EDW
                         |
                 Azure Data Factory
                         |
       +-----------------+------------------+
       |                                    |
 historical/batch                    event-driven
       |                                    |
       v                                    v
 GCS landing/bronze                  GCP Pub/Sub
       |                                    |
       +-------------> Cloud Run <----------+
                         |
                    Medallion lake
              Bronze -> Silver -> Gold
                         |
              +----------+----------+
              |          |          |
           BigQuery   semantic    ML/AI
              |          |          |
              +------ command center+
```

## Repository map

```text
src/resort_platform/    Production Python runtime
sqlserver/              SQL Server DDL, procedures, reconciliation and tuning
ssis/                   SSIS package sources, environments and deployment assets
db2/                    DB2 extraction/CDC SQL and source contracts
adf/                    Azure Data Factory pipelines, datasets and linked services
gcp/                    Pub/Sub, Cloud Run, storage and Terraform assets
contracts/              Event and batch data contracts
warehouse/              Conformed dimensions, facts and semantic warehouse
analytics/              Gaming/hotel/rewards operational SQL
dbt/                    Optional semantic transformation project
mlops/                   Features, training, model monitoring and promotion
observability/           SLOs, health queries, lineage and incident signals
agents/                  Evidence-backed operational assistants
collaboration/           Cross-department work and recommendation engine
tests/                   Unit, contract, reconciliation and architecture tests
docs/                    Architecture, runbooks, ADRs and interview walkthrough
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

## Interview-first files

For the Senior Data Engineer role, start with:

1. `ssis/packages/` — required SSIS competency and restartable ETL design
2. `sqlserver/` — SQL engineering, dimensional models, MERGE and reconciliation
3. `src/resort_platform/` — advanced Python / streaming / production software
4. `gcp/pubsub/` + `gcp/cloud_run/` — event-driven / Pub/Sub / real-time processing
5. `adf/` — cloud migration and orchestration
6. `db2/` — hotel/gaming source extraction patterns

The project borrows engineering patterns from the owner's existing casino streaming/AI and banking platform repositories, but the implementation here is reorganized around the Microsoft SQL Server + SSIS + DB2 + Azure ADF + GCP stack required by this role.
