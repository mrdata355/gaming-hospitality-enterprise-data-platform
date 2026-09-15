CREATE OR ALTER PROCEDURE ctl.usp_reconcile_pipeline
    @pipeline_run_id uniqueidentifier,
    @source_count bigint,
    @source_amount decimal(19,2) = 0,
    @target_count bigint,
    @target_amount decimal(19,2) = 0,
    @explained_count bigint = 0,
    @explained_amount decimal(19,2) = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @count_delta bigint = @source_count - @target_count - @explained_count;
    DECLARE @amount_delta decimal(19,2) = @source_amount - @target_amount - @explained_amount;

    SELECT
        @pipeline_run_id AS pipeline_run_id,
        @source_count AS source_count,
        @target_count AS target_count,
        @explained_count AS explained_count,
        @count_delta AS unexplained_count_delta,
        @source_amount AS source_amount,
        @target_amount AS target_amount,
        @explained_amount AS explained_amount,
        @amount_delta AS unexplained_amount_delta,
        CONVERT(bit, CASE WHEN @count_delta = 0 AND @amount_delta = 0 THEN 1 ELSE 0 END) AS balanced;

    IF @count_delta <> 0 OR @amount_delta <> 0
        THROW 51001, 'Pipeline reconciliation failed: unexplained variance remains.', 1;
END;
GO
