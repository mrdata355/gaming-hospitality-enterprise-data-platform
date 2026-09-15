select
  cast(reservation_id as string) as reservation_id,
  upper(trim(property_code)) as property_code,
  cast(stay_date as date) as stay_date,
  upper(trim(reservation_status)) as reservation_status,
  upper(trim(room_type_code)) as room_type_code,
  cast(rooms as int64) as rooms,
  cast(revenue_amount as numeric) as revenue_amount,
  cast(source_sequence as int64) as source_sequence,
  cast(source_updated_at as timestamp) as source_updated_at
from {{ source('silver', 'hotel_reservation_current') }}
where rooms >= 0
  and stay_date between date_sub(current_date(), interval 1 year)
                    and date_add(current_date(), interval 2 year)
