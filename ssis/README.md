# SSIS migration bridge

The SSIS layer demonstrates the role's required SQL Server Integration Services competency while keeping the platform migration-friendly.

## Packages

- `HotelReservations_Incremental.dtsx` — DB2 watermark capture → extract → validation/quarantine → SQL Server staging → transactional MERGE → watermark commit.
- `GamingActivity_Incremental.dtsx` — high-volume DB2 gaming extract → staging → reconciliation → event publication handoff.

## Restartability

1. A package creates `pipeline_run_id` and captures a fixed high watermark before extraction.
2. Source reads are bounded by `(last_success_value, high_watermark]`.
3. Every staged record carries `pipeline_run_id` and source sequence.
4. Invalid rows go to `dq.quarantine` rather than silently disappearing.
5. Target procedures are idempotent by business/event key.
6. The watermark advances only inside the same successful transaction as the target merge.
7. Re-running a failed package uses the same source range and is safe.

## Deployment

`deployment/Deploy-SSIS.ps1` publishes the project ISPAC and creates the environment references. Secrets belong in SSISDB environment variables / secret stores and are never committed here.
