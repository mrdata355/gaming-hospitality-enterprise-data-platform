WITH recent_runs AS (
    SELECT pipeline_name, status, started_utc, completed_utc,
           source_rows, target_rows, quarantined_rows,
           DATEDIFF(second, started_utc, COALESCE(completed_utc, SYSUTCDATETIME())) duration_seconds
    FROM ctl.pipeline_run
    WHERE started_utc >= DATEADD(hour, -24, SYSUTCDATETIME())
), aggregated AS (
    SELECT pipeline_name,
           COUNT(*) run_count,
           SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) failures,
           SUM(source_rows) source_rows,
           SUM(target_rows) target_rows,
           SUM(quarantined_rows) quarantined_rows,
           MAX(duration_seconds) max_duration_seconds,
           MAX(started_utc) last_started_utc
    FROM recent_runs
    GROUP BY pipeline_name
)
SELECT *,
       CAST(100.0 * failures / NULLIF(run_count,0) AS decimal(9,4)) failure_pct,
       CAST(100.0 * quarantined_rows / NULLIF(source_rows,0) AS decimal(9,4)) quarantine_pct,
       CASE
         WHEN failures > 0 THEN 'ALERT'
         WHEN quarantined_rows > source_rows * 0.01 THEN 'WARN'
         ELSE 'HEALTHY'
       END health_status
FROM aggregated;
