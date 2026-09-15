/*
Azure Stream Analytics continuous-query reference.
Synthetic portfolio implementation; resources and schemas are generated.

Architecture:
Event Hubs -> Azure Stream Analytics -> operational SQL / ADLS
                                      -> ADF downstream orchestration

Demonstrates event-time queries, tumbling/hopping/session windows, reference
joins, cross-stream correlation, operational data quality, and continuous
property metrics.
*/

-- 1. Canonical hotel reservation stream -----------------------------------
WITH HotelBase AS (
    SELECT
        event_id,
        event_type,
        schema_version,
        source_system,
        property_code,
        reservation_id,
        guest_token,
        room_type_code,
        status,
        TRY_CAST(room_revenue AS float) AS room_revenue,
        TRY_CAST(arrival_date AS datetime) AS arrival_date,
        TRY_CAST(departure_date AS datetime) AS departure_date,
        event_ts
    FROM hotel_reservation_input
    TIMESTAMP BY event_ts
    WHERE
        event_type = 'HOTEL_RESERVATION'
        AND event_id IS NOT NULL
        AND reservation_id IS NOT NULL
        AND property_code IS NOT NULL
),
HotelMinute AS (
    SELECT
        property_code,
        System.Timestamp() AS window_end,
        COUNT(*) AS reservation_event_count,
        SUM(CASE WHEN status = 'BOOKED' THEN 1 ELSE 0 END) AS booked_count,
        SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count,
        SUM(CASE WHEN status = 'CHECKED_IN' THEN 1 ELSE 0 END) AS checked_in_count,
        SUM(CASE WHEN status = 'CHECKED_OUT' THEN 1 ELSE 0 END) AS checked_out_count,
        SUM(room_revenue) AS room_revenue_event_value
    FROM HotelBase
    GROUP BY property_code, TumblingWindow(minute, 1)
)
SELECT
    property_code,
    window_end,
    reservation_event_count,
    booked_count,
    cancelled_count,
    checked_in_count,
    checked_out_count,
    room_revenue_event_value
INTO hotel_operational_minute_output
FROM HotelMinute;


-- 2. Gaming slot stream: 5-minute window refreshed every 30 seconds --------
WITH SlotBase AS (
    SELECT
        event_id,
        schema_version,
        source_system,
        property_code,
        play_id,
        session_id,
        device_token,
        guest_token,
        TRY_CAST(coin_in AS float) AS coin_in,
        TRY_CAST(payout AS float) AS payout,
        TRY_CAST(jackpot_amount AS float) AS jackpot_amount,
        event_ts
    FROM gaming_slot_input
    TIMESTAMP BY event_ts
    WHERE
        event_id IS NOT NULL
        AND play_id IS NOT NULL
        AND property_code IS NOT NULL
),
SlotFiveMinute AS (
    SELECT
        property_code,
        System.Timestamp() AS window_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT session_id) AS session_count,
        SUM(coin_in) AS coin_in,
        SUM(payout) AS payout,
        SUM(jackpot_amount) AS jackpot_amount,
        SUM(coin_in - payout - jackpot_amount) AS net_win,
        AVG(coin_in) AS avg_coin_in_per_event
    FROM SlotBase
    GROUP BY property_code, HoppingWindow(second, 300, 30)
)
SELECT
    property_code,
    window_end,
    event_count,
    session_count,
    coin_in,
    payout,
    jackpot_amount,
    net_win,
    avg_coin_in_per_event
INTO gaming_five_minute_output
FROM SlotFiveMinute;


-- 3. Device-level streaming health ----------------------------------------
WITH DeviceFiveMinute AS (
    SELECT
        property_code,
        device_token,
        System.Timestamp() AS window_end,
        COUNT(*) AS event_count,
        SUM(coin_in) AS coin_in,
        SUM(payout) AS payout,
        SUM(jackpot_amount) AS jackpot_amount,
        SUM(coin_in - payout - jackpot_amount) AS net_win
    FROM SlotBase
    GROUP BY
        property_code,
        device_token,
        HoppingWindow(minute, 5, 1)
)
SELECT
    d.property_code,
    d.device_token,
    d.window_end,
    d.event_count,
    d.coin_in,
    d.payout,
    d.jackpot_amount,
    d.net_win,
    r.zone_code,
    r.device_category,
    r.active_flag
INTO gaming_device_health_output
FROM DeviceFiveMinute d
LEFT JOIN device_reference r
    ON d.device_token = r.device_token;


-- 4. Gaming anomaly candidates --------------------------------------------
WITH PropertyBaseline AS (
    SELECT
        property_code,
        System.Timestamp() AS window_end,
        AVG(coin_in) AS avg_coin_in,
        AVG(payout) AS avg_payout,
        AVG(coin_in - payout - jackpot_amount) AS avg_net_win,
        COUNT(*) AS sample_count
    FROM SlotBase
    GROUP BY property_code, HoppingWindow(minute, 15, 1)
)
SELECT
    property_code,
    window_end,
    avg_coin_in,
    avg_payout,
    avg_net_win,
    sample_count,
    CASE
        WHEN sample_count < 5 THEN 'INSUFFICIENT_SAMPLE'
        WHEN avg_payout > avg_coin_in * 1.50 THEN 'PAYOUT_SPIKE'
        WHEN avg_net_win < -1000 THEN 'NEGATIVE_NET_WIN'
        ELSE 'NORMAL'
    END AS anomaly_candidate
INTO gaming_anomaly_candidate_output
FROM PropertyBaseline
WHERE
    sample_count >= 5
    AND (
        avg_payout > avg_coin_in * 1.50
        OR avg_net_win < -1000
    );


-- 5. Mobile application sessions ------------------------------------------
WITH MobileBase AS (
    SELECT
        event_id,
        property_code,
        guest_token,
        anonymous_session_id,
        action,
        screen_name,
        offer_token,
        device_family,
        event_ts,
        COALESCE(guest_token, anonymous_session_id) AS session_identity
    FROM mobile_app_input
    TIMESTAMP BY event_ts
    WHERE
        event_id IS NOT NULL
        AND COALESCE(guest_token, anonymous_session_id) IS NOT NULL
),
MobileSessions AS (
    SELECT
        property_code,
        session_identity,
        System.Timestamp() AS session_end,
        COUNT(*) AS event_count,
        SUM(CASE WHEN action = 'OFFER_VIEW' THEN 1 ELSE 0 END) AS offer_views,
        SUM(CASE WHEN action = 'BOOKING_START' THEN 1 ELSE 0 END) AS booking_starts,
        SUM(CASE WHEN action = 'CHECK_IN_START' THEN 1 ELSE 0 END) AS checkin_starts
    FROM MobileBase
    GROUP BY
        property_code,
        session_identity,
        SessionWindow(minute, 5, 30)
)
SELECT
    property_code,
    session_identity,
    session_end,
    event_count,
    offer_views,
    booking_starts,
    checkin_starts
INTO mobile_session_output
FROM MobileSessions;


-- 6. Rewards activity ------------------------------------------------------
WITH RewardBase AS (
    SELECT
        event_id,
        property_code,
        guest_token,
        reward_event_id,
        activity_type,
        source_domain,
        TRY_CAST(points_delta AS bigint) AS points_delta,
        TRY_CAST(monetary_value AS float) AS monetary_value,
        event_ts
    FROM reward_activity_input
    TIMESTAMP BY event_ts
    WHERE
        event_id IS NOT NULL
        AND reward_event_id IS NOT NULL
        AND guest_token IS NOT NULL
),
RewardFiveMinute AS (
    SELECT
        property_code,
        source_domain,
        System.Timestamp() AS window_end,
        COUNT(*) AS event_count,
        SUM(points_delta) AS net_points_delta,
        SUM(monetary_value) AS monetary_value
    FROM RewardBase
    GROUP BY
        property_code,
        source_domain,
        TumblingWindow(minute, 5)
)
SELECT
    property_code,
    source_domain,
    window_end,
    event_count,
    net_points_delta,
    monetary_value
INTO reward_activity_output
FROM RewardFiveMinute;


-- 7. Hotel/mobile temporal correlation ------------------------------------
SELECT
    h.property_code,
    h.reservation_id,
    h.guest_token,
    h.status AS reservation_status,
    m.action AS mobile_action,
    m.screen_name,
    m.offer_token,
    h.event_ts AS reservation_event_ts,
    m.event_ts AS mobile_event_ts
INTO hotel_mobile_correlation_output
FROM HotelBase h
JOIN MobileBase m
    ON h.guest_token = m.guest_token
    AND h.property_code = m.property_code
    AND DATEDIFF(minute, h, m) BETWEEN 0 AND 10;


-- 8. Operational DQ streams -----------------------------------------------
SELECT
    'hotel_reservation' AS stream_name,
    System.Timestamp() AS window_end,
    COUNT(*) AS invalid_count
INTO stream_dq_output
FROM hotel_reservation_input
TIMESTAMP BY event_ts
WHERE
    event_id IS NULL
    OR reservation_id IS NULL
    OR property_code IS NULL
GROUP BY TumblingWindow(minute, 1);

SELECT
    'gaming_slot' AS stream_name,
    System.Timestamp() AS window_end,
    COUNT(*) AS invalid_count
INTO stream_dq_output
FROM gaming_slot_input
TIMESTAMP BY event_ts
WHERE
    event_id IS NULL
    OR play_id IS NULL
    OR property_code IS NULL
GROUP BY TumblingWindow(minute, 1);

SELECT
    'mobile_app' AS stream_name,
    System.Timestamp() AS window_end,
    COUNT(*) AS invalid_count
INTO stream_dq_output
FROM mobile_app_input
TIMESTAMP BY event_ts
WHERE
    event_id IS NULL
    OR COALESCE(guest_token, anonymous_session_id) IS NULL
GROUP BY TumblingWindow(minute, 1);


-- Interview anchors --------------------------------------------------------
-- Continuous query: stays active against unbounded input and emits results as
-- data arrives and event-time windows progress.
-- Event time: when the business event happened.
-- Processing time: when the engine handled it.
-- Watermark: event-time progress used to bound late-data state.
-- Tumbling window: fixed and non-overlapping.
-- Hopping window: overlapping fixed-size windows with a smaller hop.
-- Session window: data-driven window separated by inactivity.
-- Idempotency: retrying the same event does not duplicate business state.
-- Replay: preserved source data plus progress/checkpoints permit recomputation.
-- ADF remains orchestration/migration; Stream Analytics is the continuous SQL
-- processing layer in this Azure example.
