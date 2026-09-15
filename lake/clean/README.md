# Clean zone

Contract-enforced and normalized records. Invalid rows are written to quarantine with reason codes.

Path pattern:
`gs://vh-portfolio-{env}-clean-{region}/{domain}/{entity}/business_date={yyyy-mm-dd}/hour={hh}/`

Transformations:
- trim and uppercase domain codes
- parse timestamps and amounts
- require business keys
- validate property codes
- reject impossible dates
- preserve event_id and lineage
- deduplicate stable event IDs within processing horizon
- add quality_status and normalized_at
