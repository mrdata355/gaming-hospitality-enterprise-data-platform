{{ config(materialized='incremental', unique_key=['property_code','window_start'], partition_by={'field':'event_date','data_type':'date'}, cluster_by=['property_code']) }}

select
  date(event_ts) as event_date,
  property_code,
  timestamp_seconds(div(unix_seconds(event_ts), 300) * 300) as window_start,
  count(*) as event_count,
  count(distinct session_id) as gaming_sessions,
  count(distinct device_token) as active_devices,
  sum(coalesce(coin_in,0)) as coin_in,
  sum(coalesce(payout,0)) as payout,
  sum(coalesce(jackpot_amount,0)) as jackpot_amount,
  sum(coalesce(coin_in,0) - coalesce(payout,0) - coalesce(jackpot_amount,0)) as actual_win,
  max(first_processed_at) as source_updated_at
from {{ ref('stg_gaming_events') }}
{% if is_incremental() %}
where first_processed_at > timestamp_sub((select coalesce(max(source_updated_at), timestamp('1900-01-01')) from {{ this }}), interval 15 minute)
{% endif %}
group by event_date, property_code, window_start
