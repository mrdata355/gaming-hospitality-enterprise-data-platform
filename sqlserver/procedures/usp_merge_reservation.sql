CREATE OR ALTER PROCEDURE dw.usp_merge_reservation
    @pipeline_run_id uniqueidentifier,
    @high_watermark bigint
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY reservation_id
                ORDER BY source_sequence DESC, source_updated_utc DESC
            ) AS rn
            FROM stg.hotel_reservation
            WHERE pipeline_run_id = @pipeline_run_id
        )
        MERGE dw.fact_reservation WITH (HOLDLOCK) AS target
        USING (
            SELECT * FROM ranked WHERE rn = 1
        ) AS source
        ON target.reservation_id = source.reservation_id
        WHEN MATCHED AND source.source_sequence > target.source_sequence THEN
            UPDATE SET
                guest_sk = source.guest_sk,
                property_sk = source.property_sk,
                room_sk = source.room_sk,
                booking_date = source.booking_date,
                arrival_date = source.arrival_date,
                departure_date = source.departure_date,
                reservation_status = source.reservation_status,
                channel = source.channel,
                rooms = source.rooms,
                adults = source.adults,
                children = source.children,
                room_revenue = source.room_revenue,
                source_sequence = source.source_sequence,
                source_updated_utc = source.source_updated_utc,
                loaded_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                reservation_id, guest_sk, property_sk, room_sk, booking_date, arrival_date,
                departure_date, reservation_status, channel, rooms, adults, children,
                room_revenue, source_sequence, source_updated_utc
            )
            VALUES (
                source.reservation_id, source.guest_sk, source.property_sk, source.room_sk,
                source.booking_date, source.arrival_date, source.departure_date,
                source.reservation_status, source.channel, source.rooms, source.adults,
                source.children, source.room_revenue, source.source_sequence, source.source_updated_utc
            );

        UPDATE ctl.pipeline_watermark
        SET last_success_value = @high_watermark,
            last_batch_id = @pipeline_run_id,
            last_success_utc = SYSUTCDATETIME()
        WHERE pipeline_name = 'hotel_reservation_incremental';

        UPDATE ctl.pipeline_run
        SET status = 'SUCCEEDED',
            completed_utc = SYSUTCDATETIME(),
            target_rows = @@ROWCOUNT
        WHERE pipeline_run_id = @pipeline_run_id;

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        UPDATE ctl.pipeline_run
        SET status = 'FAILED', completed_utc = SYSUTCDATETIME(),
            error_number = ERROR_NUMBER(), error_message = ERROR_MESSAGE()
        WHERE pipeline_run_id = @pipeline_run_id;
        THROW;
    END CATCH
END;
GO
