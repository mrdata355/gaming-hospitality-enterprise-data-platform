# Production break/fix runbook

## Streaming lag / Pub/Sub backlog
1. Confirm affected subscription, oldest unacked age and publish/ack rates.
2. Check Cloud Run errors, instance saturation and downstream BigQuery/GCS latency.
3. Preserve DLQ and source landing objects; do not purge backlog to make dashboards green.
4. Scale within configured max instances or temporarily reduce noncritical consumers.
5. Validate contract/schema changes and poison-message patterns.
6. Replay from the landing object or subscription seek point after the fix.
7. Reconcile event IDs/counts and business measures before closing.

## DB2/SSIS batch failure
1. Read `ctl.pipeline_run` and identify low/high watermark and last successful task.
2. Keep the watermark unchanged.
3. Check DB2 connectivity, Integration Runtime, SSISDB execution messages and staging row counts.
4. Fix the failed component; do not manually skip quarantined records without a reason code.
5. Re-run the same bounded range.
6. Execute `ctl.usp_reconcile_pipeline`.
7. Advance the watermark only after a balanced result.

## Revenue mismatch
1. Freeze the affected Gold/semantic publication if the variance is material.
2. Compare source/stage/target counts and amount controls by business key/grain.
3. Profile duplicate keys and many-to-many joins.
4. Inspect late-arriving SCD versions and status reversals.
5. Correct the grain/key logic, replay the bounded range, and re-run reconciliation.
6. Add a preventive uniqueness/reconciliation test before restoring publication.
