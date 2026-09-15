select
  cast(event_id as string) as event_id,
  upper(trim(event_type)) as event_type,
  upper(trim(source_system)) as source_system,
  upper(trim(property_code)) as property_code,
  cast(event_ts as timestamp) as event_ts,
  cast(enterprise_guest_id as string) as enterprise_guest_id,
  cast(session_id as string) as session_id,
  cast(device_token as string) as device_token,
  safe_cast(coin_in as numeric) as coin_in,
  safe_cast(payout as numeric) as payout,
  safe_cast(jackpot_amount as numeric) as jackpot_amount,
  cast(first_processed_at as timestamp) as first_processed_at
from {{ source('silver', 'gaming_event_fact') }}
where event_id is not null
  and event_ts <= timestamp_add(current_timestamp(), interval 5 minute)
