-- DB2 generated source pattern: hotel reservation incremental extract.
-- :low_watermark and :high_watermark are captured by the control framework.
SELECT
    RESERVATION_ID,
    GUEST_TOKEN,
    PROPERTY_CODE,
    ROOM_TYPE_CODE,
    BOOKING_DATE,
    ARRIVAL_DATE,
    DEPARTURE_DATE,
    RESERVATION_STATUS,
    BOOKING_CHANNEL,
    ROOMS,
    ADULTS,
    CHILDREN,
    ROOM_REVENUE,
    SOURCE_SEQUENCE,
    SOURCE_UPDATED_TS
FROM HOTEL.RESERVATION_CURRENT
WHERE SOURCE_SEQUENCE > :low_watermark
  AND SOURCE_SEQUENCE <= :high_watermark
  AND SOURCE_UPDATED_TS < CURRENT TIMESTAMP + 5 MINUTES
ORDER BY SOURCE_SEQUENCE
WITH UR;
