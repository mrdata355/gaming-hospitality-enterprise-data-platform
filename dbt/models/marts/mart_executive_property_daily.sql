{{ config(materialized='incremental', unique_key=['property_code','business_date'], partition_by={'field':'business_date','data_type':'date'}) }}

with hotel as (
  select stay_date business_date, property_code,
         sum(rooms_sold) rooms_sold,
         sum(room_revenue) room_revenue,
         avg(occupancy_pct) occupancy_pct
  from {{ ref('fct_hotel_occupancy_daily') }}
  group by 1,2
),
gaming as (
  select event_date business_date, property_code,
         sum(coin_in) coin_in,
         sum(actual_win) gaming_actual_win,
         sum(gaming_sessions) gaming_sessions
  from {{ ref('fct_gaming_performance_5min') }}
  group by 1,2
)
select
  coalesce(h.business_date, g.business_date) business_date,
  coalesce(h.property_code, g.property_code) property_code,
  h.rooms_sold,
  h.room_revenue,
  h.occupancy_pct,
  g.coin_in,
  g.gaming_actual_win,
  g.gaming_sessions,
  coalesce(h.room_revenue,0) + coalesce(g.gaming_actual_win,0) as measured_operating_revenue,
  current_timestamp() refreshed_at
from hotel h
full outer join gaming g using (business_date, property_code)
