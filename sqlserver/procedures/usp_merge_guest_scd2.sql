CREATE OR ALTER PROCEDURE dw.usp_merge_guest_scd2
    @pipeline_run_id uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @as_of datetime2(3) = SYSUTCDATETIME();

    BEGIN TRANSACTION;

    UPDATE current_row
       SET current_row.effective_to_utc = DATEADD(millisecond, -1, @as_of),
           current_row.is_current = 0
    FROM dw.dim_guest current_row
    JOIN stg.guest_profile incoming
      ON incoming.enterprise_guest_id = current_row.enterprise_guest_id
     AND incoming.pipeline_run_id = @pipeline_run_id
    WHERE current_row.is_current = 1
      AND current_row.record_hash <> incoming.record_hash;

    INSERT dw.dim_guest (
        enterprise_guest_id, guest_token, loyalty_tier, home_market,
        acquisition_channel, effective_from_utc, effective_to_utc,
        is_current, record_hash, source_system
    )
    SELECT
        s.enterprise_guest_id, s.guest_token, s.loyalty_tier, s.home_market,
        s.acquisition_channel, @as_of, '9999-12-31T23:59:59.997',
        1, s.record_hash, s.source_system
    FROM stg.guest_profile s
    LEFT JOIN dw.dim_guest d
      ON d.enterprise_guest_id = s.enterprise_guest_id
     AND d.is_current = 1
     AND d.record_hash = s.record_hash
    WHERE s.pipeline_run_id = @pipeline_run_id
      AND d.guest_sk IS NULL;

    COMMIT;
END;
GO
