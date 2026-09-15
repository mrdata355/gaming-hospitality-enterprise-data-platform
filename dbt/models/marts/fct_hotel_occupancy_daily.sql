{{ config(materialized='incremental', unique_key=['property_code','stay_date'], partition_by={'field':'stay_date','data_type':'date'}, cluster_by=['property_code']) }}

select
  property_code,
  stay_date,
  sum(case when reservation_status in ('BOOKED','CHECKED_IN') then rooms else 0 end) as rooms_sold,
  max(coalesce(property_room_inventory, 0)) as rooms_available,
  safe_divide(
    100 * sum(case when reservation_status in ('BOOKED','CHECKED_IN') then rooms else 0 end),
    nullif(max(coalesce(property_room_inventory, 0)), 0)
  ) as occupancy_pct,
  sum(case when reservation_status in ('BOOKED','CHECKED_IN','CHECKED_OUT') then revenue_amount else 0 end) as room_revenue,
  safe_divide(
    sum(case when reservation_status in ('BOOKED','CHECKED_IN','CHECKED_OUT') then revenue_amount else 0 end),
    nullif(sum(case when reservation_status in ('BOOKED','CHECKED_IN') then rooms else 0 end), 0)
  ) as adr,
  max(source_sequence) as max_source_sequence,
  max(source_updated_at) as source_updated_at
from (
  select r.*, 0 as property_room_inventory
  from {{ ref('stg_hotel_reservations') }} r
)
{% if is_incremental() %}
where source_updated_at > timestamp_sub((select coalesce(max(source_updated_at), timestamp('1900-01-01')) from {{ this }}), interval 1 day)
{% endif %}
group by property_code, stay_date
