-- Generated performance strategy for large event/fact workloads.
-- Validate with actual execution plans and workload telemetry before production rollout.

CREATE PARTITION FUNCTION pf_event_month (date)
AS RANGE RIGHT FOR VALUES (
    '2026-01-01','2026-02-01','2026-03-01','2026-04-01','2026-05-01','2026-06-01',
    '2026-07-01','2026-08-01','2026-09-01','2026-10-01','2026-11-01','2026-12-01','2027-01-01'
);
GO

-- Example covering indexes for interview/demo workloads.
CREATE INDEX IX_reward_guest_time
ON dw.fact_reward_activity(guest_sk, activity_utc DESC)
INCLUDE(activity_type, source_domain, points_delta, monetary_value);
GO

CREATE INDEX IX_mobile_guest_time
ON dw.fact_mobile_event(guest_sk, event_utc DESC)
INCLUDE(action, screen_name, offer_token, property_sk);
GO

-- Operational guidance:
-- 1. Start with Query Store and actual plans.
-- 2. Remove duplicate/overlapping indexes before adding new ones.
-- 3. Use incremental statistics on partitioned large facts.
-- 4. Compress read-heavy historical partitions.
-- 5. Keep staging heaps short-lived and batch-scoped.
-- 6. Measure logical reads, CPU, elapsed time and spill before/after each change.
