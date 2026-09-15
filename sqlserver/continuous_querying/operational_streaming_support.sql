/*
Synthetic SQL Server support layer for continuous streaming.

SQL Server is not presented as the streaming engine. This layer provides the
transactional outbox, CDC handoff, idempotent consumer ledger, canonical MERGE,
audit, replay, and reconciliation controls used around Pub/Sub/Dataflow,
Event Hubs/Stream Analytics, or Spark Structured Streaming.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stream')
    EXEC('CREATE SCHEMA stream');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
    EXEC('CREATE SCHEMA audit');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
    EXEC('CREATE SCHEMA curated');
GO

CREATE TABLE stream.OutboxEvent (
    outbox_event_id        bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    event_id               uniqueidentifier NOT NULL,
    event_type             varchar(100) NOT NULL,
    aggregate_type         varchar(100) NOT NULL,
    aggregate_id           varchar(200) NOT NULL,
    property_code          varchar(50) NOT NULL,
    schema_version         varchar(20) NOT NULL CONSTRAINT DF_Outbox_Schema DEFAULT('1'),
    payload_json           nvarchar(max) NOT NULL,
    source_updated_at      datetime2(3) NOT NULL,
    created_at             datetime2(3) NOT NULL CONSTRAINT DF_Outbox_Created DEFAULT(sysutcdatetime()),
    publish_status         varchar(20) NOT NULL CONSTRAINT DF_Outbox_Status DEFAULT('READY'),
    claim_token            uniqueidentifier NULL,
    claimed_at             datetime2(3) NULL,
    claim_expires_at       datetime2(3) NULL,
    published_at           datetime2(3) NULL,
    publish_attempt_count  int NOT NULL CONSTRAINT DF_Outbox_Attempts DEFAULT(0),
    last_error             nvarchar(2000) NULL,
    CONSTRAINT UQ_Outbox_EventId UNIQUE(event_id),
    CONSTRAINT CK_Outbox_Json CHECK(ISJSON(payload_json) = 1),
    CONSTRAINT CK_Outbox_Status CHECK(
        publish_status IN ('READY','CLAIMED','PUBLISHED','FAILED','QUARANTINED')
    )
);
GO

CREATE INDEX IX_Outbox_Ready
ON stream.OutboxEvent(publish_status, outbox_event_id)
INCLUDE(event_id, event_type, aggregate_id, property_code, source_updated_at)
WHERE publish_status IN ('READY','FAILED');
GO

CREATE TABLE stream.ConsumerLedger (
    consumer_name       varchar(150) NOT NULL,
    event_id            uniqueidentifier NOT NULL,
    event_type          varchar(100) NOT NULL,
    first_seen_at       datetime2(3) NOT NULL CONSTRAINT DF_Ledger_First DEFAULT(sysutcdatetime()),
    last_seen_at        datetime2(3) NOT NULL CONSTRAINT DF_Ledger_Last DEFAULT(sysutcdatetime()),
    delivery_count      int NOT NULL CONSTRAINT DF_Ledger_Count DEFAULT(1),
    source_partition    varchar(100) NULL,
    source_offset       varchar(100) NULL,
    processing_status   varchar(30) NOT NULL,
    processing_error    nvarchar(2000) NULL,
    CONSTRAINT PK_ConsumerLedger PRIMARY KEY(consumer_name, event_id)
);
GO

CREATE TABLE stream.QuarantineEvent (
    quarantine_id       bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    event_id            uniqueidentifier NULL,
    event_type          varchar(100) NULL,
    reason_code         varchar(100) NOT NULL,
    reason_detail       nvarchar(2000) NULL,
    raw_payload         nvarchar(max) NULL,
    source_system       varchar(100) NULL,
    property_code       varchar(50) NULL,
    quarantined_at      datetime2(3) NOT NULL CONSTRAINT DF_Quarantine_At DEFAULT(sysutcdatetime()),
    replay_status       varchar(20) NOT NULL CONSTRAINT DF_Quarantine_Replay DEFAULT('NOT_REQUESTED')
);
GO

CREATE TABLE stream.ReplayRequest (
    replay_request_id      bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    domain_name            varchar(100) NOT NULL,
    start_event_time       datetime2(3) NULL,
    end_event_time         datetime2(3) NULL,
    start_outbox_event_id  bigint NULL,
    end_outbox_event_id    bigint NULL,
    reason                 nvarchar(1000) NOT NULL,
    requested_by           varchar(150) NOT NULL,
    requested_at           datetime2(3) NOT NULL CONSTRAINT DF_Replay_At DEFAULT(sysutcdatetime()),
    status                 varchar(30) NOT NULL CONSTRAINT DF_Replay_Status DEFAULT('REQUESTED'),
    completed_at           datetime2(3) NULL
);
GO

CREATE TABLE audit.StreamBatch (
    stream_batch_id            bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    query_name                 varchar(200) NOT NULL,
    batch_id                   bigint NOT NULL,
    started_at                 datetime2(3) NOT NULL,
    completed_at               datetime2(3) NULL,
    source_rows                bigint NOT NULL CONSTRAINT DF_Stream_Source DEFAULT(0),
    clean_rows                 bigint NOT NULL CONSTRAINT DF_Stream_Clean DEFAULT(0),
    quarantine_rows            bigint NOT NULL CONSTRAINT DF_Stream_Quarantine DEFAULT(0),
    duplicate_rows             bigint NOT NULL CONSTRAINT DF_Stream_Duplicate DEFAULT(0),
    stale_rows                 bigint NOT NULL CONSTRAINT DF_Stream_Stale DEFAULT(0),
    applied_rows               bigint NOT NULL CONSTRAINT DF_Stream_Applied DEFAULT(0),
    input_rows_per_second      decimal(18,4) NULL,
    processed_rows_per_second  decimal(18,4) NULL,
    trigger_duration_ms        bigint NULL,
    event_time_watermark       datetime2(3) NULL,
    state_rows_total           bigint NULL,
    status                     varchar(30) NOT NULL,
    error_message              nvarchar(4000) NULL,
    CONSTRAINT UQ_StreamBatch UNIQUE(query_name, batch_id)
);
GO

CREATE TABLE curated.GamingSessionCurrent (
    session_id          varchar(100) NOT NULL PRIMARY KEY,
    property_code       varchar(50) NOT NULL,
    guest_token         varchar(100) NULL,
    game_type           varchar(50) NOT NULL,
    session_start_ts    datetime2(3) NOT NULL,
    session_end_ts      datetime2(3) NULL,
    coin_in             decimal(19,4) NOT NULL CONSTRAINT DF_Gaming_CoinIn DEFAULT(0),
    payout              decimal(19,4) NOT NULL CONSTRAINT DF_Gaming_Payout DEFAULT(0),
    jackpot_amount      decimal(19,4) NOT NULL CONSTRAINT DF_Gaming_Jackpot DEFAULT(0),
    source_version      bigint NOT NULL,
    source_updated_at   datetime2(3) NOT NULL,
    last_event_id       uniqueidentifier NOT NULL,
    merged_at           datetime2(3) NOT NULL CONSTRAINT DF_Gaming_Merged DEFAULT(sysutcdatetime())
);
GO

CREATE OR ALTER PROCEDURE stream.usp_EnqueueEvent
    @event_id uniqueidentifier,
    @event_type varchar(100),
    @aggregate_type varchar(100),
    @aggregate_id varchar(200),
    @property_code varchar(50),
    @schema_version varchar(20),
    @payload_json nvarchar(max),
    @source_updated_at datetime2(3)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF ISJSON(@payload_json) <> 1
        THROW 51001, 'payload_json must be valid JSON', 1;

    IF NOT EXISTS (
        SELECT 1
        FROM stream.OutboxEvent WITH (UPDLOCK, HOLDLOCK)
        WHERE event_id = @event_id
    )
    BEGIN
        INSERT stream.OutboxEvent (
            event_id, event_type, aggregate_type, aggregate_id,
            property_code, schema_version, payload_json, source_updated_at
        )
        VALUES (
            @event_id, @event_type, @aggregate_type, @aggregate_id,
            @property_code, @schema_version, @payload_json, @source_updated_at
        );
    END
END;
GO

CREATE OR ALTER PROCEDURE stream.usp_ClaimOutboxBatch
    @max_rows int = 1000,
    @lease_seconds int = 60,
    @claim_token uniqueidentifier OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @claim_token = NEWID();
    DECLARE @now datetime2(3) = sysutcdatetime();

    ;WITH candidate AS (
        SELECT TOP (@max_rows) outbox_event_id
        FROM stream.OutboxEvent WITH (READPAST, UPDLOCK, ROWLOCK)
        WHERE
            (
                publish_status IN ('READY','FAILED')
                OR (publish_status = 'CLAIMED' AND claim_expires_at < @now)
            )
            AND publish_attempt_count < 10
        ORDER BY outbox_event_id
    )
    UPDATE o
       SET publish_status = 'CLAIMED',
           claim_token = @claim_token,
           claimed_at = @now,
           claim_expires_at = DATEADD(second, @lease_seconds, @now),
           publish_attempt_count = publish_attempt_count + 1,
           last_error = NULL
    FROM stream.OutboxEvent o
    INNER JOIN candidate c ON c.outbox_event_id = o.outbox_event_id;

    SELECT
        outbox_event_id,
        event_id,
        event_type,
        aggregate_type,
        aggregate_id,
        property_code,
        schema_version,
        payload_json,
        source_updated_at,
        publish_attempt_count
    FROM stream.OutboxEvent
    WHERE claim_token = @claim_token
      AND publish_status = 'CLAIMED'
    ORDER BY outbox_event_id;
END;
GO

CREATE OR ALTER PROCEDURE stream.usp_AcknowledgePublishedEvent
    @claim_token uniqueidentifier,
    @outbox_event_id bigint
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE stream.OutboxEvent
       SET publish_status = 'PUBLISHED',
           published_at = sysutcdatetime(),
           claim_token = NULL,
           claimed_at = NULL,
           claim_expires_at = NULL,
           last_error = NULL
    WHERE outbox_event_id = @outbox_event_id
      AND claim_token = @claim_token
      AND publish_status = 'CLAIMED';

    IF @@ROWCOUNT = 0
        THROW 51002, 'claim not found or no longer valid', 1;
END;
GO

CREATE OR ALTER PROCEDURE stream.usp_RegisterConsumerDelivery
    @consumer_name varchar(150),
    @event_id uniqueidentifier,
    @event_type varchar(100),
    @source_partition varchar(100) = NULL,
    @source_offset varchar(100) = NULL,
    @is_duplicate bit OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF EXISTS (
        SELECT 1
        FROM stream.ConsumerLedger WITH (UPDLOCK, HOLDLOCK)
        WHERE consumer_name = @consumer_name
          AND event_id = @event_id
    )
    BEGIN
        UPDATE stream.ConsumerLedger
           SET last_seen_at = sysutcdatetime(),
               delivery_count = delivery_count + 1,
               source_partition = COALESCE(@source_partition, source_partition),
               source_offset = COALESCE(@source_offset, source_offset)
        WHERE consumer_name = @consumer_name
          AND event_id = @event_id;
        SET @is_duplicate = 1;
        RETURN;
    END

    INSERT stream.ConsumerLedger (
        consumer_name, event_id, event_type,
        source_partition, source_offset, processing_status
    )
    VALUES (
        @consumer_name, @event_id, @event_type,
        @source_partition, @source_offset, 'RECEIVED'
    );
    SET @is_duplicate = 0;
END;
GO

CREATE OR ALTER PROCEDURE stream.usp_MergeGamingSession
    @session_id varchar(100),
    @property_code varchar(50),
    @guest_token varchar(100) = NULL,
    @game_type varchar(50),
    @session_start_ts datetime2(3),
    @session_end_ts datetime2(3) = NULL,
    @coin_in decimal(19,4),
    @payout decimal(19,4),
    @jackpot_amount decimal(19,4),
    @source_version bigint,
    @source_updated_at datetime2(3),
    @event_id uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    MERGE curated.GamingSessionCurrent WITH (HOLDLOCK) AS target
    USING (
        SELECT
            @session_id AS session_id,
            @property_code AS property_code,
            @guest_token AS guest_token,
            @game_type AS game_type,
            @session_start_ts AS session_start_ts,
            @session_end_ts AS session_end_ts,
            @coin_in AS coin_in,
            @payout AS payout,
            @jackpot_amount AS jackpot_amount,
            @source_version AS source_version,
            @source_updated_at AS source_updated_at,
            @event_id AS last_event_id
    ) source
    ON target.session_id = source.session_id
    WHEN MATCHED AND (
        source.source_version > target.source_version
        OR (
            source.source_version = target.source_version
            AND source.source_updated_at > target.source_updated_at
        )
    ) THEN UPDATE SET
        property_code = source.property_code,
        guest_token = source.guest_token,
        game_type = source.game_type,
        session_start_ts = source.session_start_ts,
        session_end_ts = source.session_end_ts,
        coin_in = source.coin_in,
        payout = source.payout,
        jackpot_amount = source.jackpot_amount,
        source_version = source.source_version,
        source_updated_at = source.source_updated_at,
        last_event_id = source.last_event_id,
        merged_at = sysutcdatetime()
    WHEN NOT MATCHED THEN
        INSERT (
            session_id, property_code, guest_token, game_type,
            session_start_ts, session_end_ts, coin_in, payout,
            jackpot_amount, source_version, source_updated_at, last_event_id
        )
        VALUES (
            source.session_id, source.property_code, source.guest_token, source.game_type,
            source.session_start_ts, source.session_end_ts, source.coin_in, source.payout,
            source.jackpot_amount, source.source_version, source.source_updated_at,
            source.last_event_id
        );
END;
GO

CREATE OR ALTER VIEW curated.vw_GamingPerformance5Minute
AS
SELECT
    property_code,
    DATEADD(
        minute,
        DATEDIFF(minute, CONVERT(datetime2(0),'20000101'), session_start_ts) / 5 * 5,
        CONVERT(datetime2(0),'20000101')
    ) AS five_minute_window,
    COUNT_BIG(*) AS session_count,
    SUM(coin_in) AS coin_in,
    SUM(payout) AS payout,
    SUM(jackpot_amount) AS jackpot_amount,
    SUM(coin_in - payout - jackpot_amount) AS net_win
FROM curated.GamingSessionCurrent
GROUP BY
    property_code,
    DATEADD(
        minute,
        DATEDIFF(minute, CONVERT(datetime2(0),'20000101'), session_start_ts) / 5 * 5,
        CONVERT(datetime2(0),'20000101')
    );
GO

CREATE OR ALTER VIEW audit.vw_StreamQueryHealth
AS
WITH latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY query_name
            ORDER BY batch_id DESC, stream_batch_id DESC
        ) AS rn
    FROM audit.StreamBatch
)
SELECT
    query_name,
    batch_id,
    started_at,
    completed_at,
    source_rows,
    clean_rows,
    quarantine_rows,
    duplicate_rows,
    stale_rows,
    applied_rows,
    input_rows_per_second,
    processed_rows_per_second,
    trigger_duration_ms,
    event_time_watermark,
    state_rows_total,
    status,
    CASE
        WHEN status = 'FAILED' THEN 'RED'
        WHEN trigger_duration_ms > 120000 THEN 'AMBER'
        WHEN processed_rows_per_second < input_rows_per_second * 0.8 THEN 'AMBER'
        ELSE 'GREEN'
    END AS health_status
FROM latest
WHERE rn = 1;
GO

CREATE OR ALTER PROCEDURE audit.usp_ReconcileStreamBatch
    @query_name varchar(200),
    @batch_id bigint
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        query_name,
        batch_id,
        source_rows,
        clean_rows,
        quarantine_rows,
        duplicate_rows,
        stale_rows,
        applied_rows,
        source_rows - (
            applied_rows + quarantine_rows + duplicate_rows + stale_rows
        ) AS unexplained_rows,
        CONVERT(
            bit,
            CASE
                WHEN source_rows = applied_rows + quarantine_rows + duplicate_rows + stale_rows
                THEN 1 ELSE 0
            END
        ) AS balanced
    FROM audit.StreamBatch
    WHERE query_name = @query_name
      AND batch_id = @batch_id;
END;
GO
