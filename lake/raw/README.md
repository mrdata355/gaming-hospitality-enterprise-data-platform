# Raw zone

Immutable landing-preservation layer for generated portfolio data.

Path pattern:
`gs://vh-portfolio-{env}-raw-{region}/{domain}/{entity}/business_date={yyyy-mm-dd}/hour={hh}/`

Rules:
- append only
- preserve source payload
- attach ingest metadata
- never silently coerce invalid values
- preserve duplicate arrivals for lineage
- retain source partition/offset or batch watermark
- PII fields in samples are generated; no real customer data

Required metadata:
`ingest_run_id`, `source_system`, `schema_version`, `event_id`, `business_key`, `property_code`, `source_updated_at`, `ingested_at`.
