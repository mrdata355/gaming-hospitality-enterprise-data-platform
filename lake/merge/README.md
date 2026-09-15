# Merge zone

Deterministic canonical-current and history merge layer.

Path pattern:
`gs://vh-portfolio-{env}-merge-{region}/{domain}/{entity}/business_date={yyyy-mm-dd}/`

Ordering policy:
1. stable business key
2. higher source_version wins
3. later source_updated_at wins when versions tie
4. stable event_id breaks exact timestamp ties
5. stale and duplicate arrivals are retained in audit, not applied

Outputs:
- current state
- history state
- merge audit
- duplicate/stale explanations
- reconciliation snapshot
