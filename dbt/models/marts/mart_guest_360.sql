{{ config(materialized='table', cluster_by=['enterprise_guest_id']) }}

with gaming as (
  select enterprise_guest_id,
         max(event_ts) as last_gaming_ts,
         sum(coalesce(coin_in,0)) as lifetime_coin_in,
         sum(coalesce(coin_in,0)-coalesce(payout,0)-coalesce(jackpot_amount,0)) as lifetime_actual_win,
         count(distinct session_id) as gaming_sessions
  from {{ ref('stg_gaming_events') }}
  where enterprise_guest_id is not null
  group by 1
),
rewards as (
  select cast(enterprise_guest_id as string) enterprise_guest_id,
         sum(points_delta) reward_point_delta,
         sum(monetary_value) reward_value,
         max(activity_ts) last_reward_ts
  from {{ source('silver', 'reward_activity') }}
  group by 1
)
select
  coalesce(g.enterprise_guest_id, r.enterprise_guest_id) as enterprise_guest_id,
  g.last_gaming_ts,
  g.lifetime_coin_in,
  g.lifetime_actual_win,
  g.gaming_sessions,
  r.reward_point_delta,
  r.reward_value,
  r.last_reward_ts,
  current_timestamp() as refreshed_at
from gaming g
full outer join rewards r using (enterprise_guest_id)
