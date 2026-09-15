-- Synthetic enterprise warehouse reference for a gaming/hospitality portfolio.
-- Public property anchors are real; schemas, tables, paths and identifiers below are generated.
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='ctl') EXEC('CREATE SCHEMA ctl');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='raw') EXEC('CREATE SCHEMA raw');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='curated') EXEC('CREATE SCHEMA curated');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='dq') EXEC('CREATE SCHEMA dq');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='audit') EXEC('CREATE SCHEMA audit');
GO

IF OBJECT_ID('ctl.pipeline_watermark','U') IS NULL
BEGIN
    CREATE TABLE ctl.pipeline_watermark (
        pipeline_name varchar(128) NOT NULL,
        source_system varchar(64) NOT NULL,
        low_watermark bigint NULL,
        high_watermark bigint NULL,
        last_success_utc datetime2(3) NULL,
        status varchar(20) NOT NULL DEFAULT 'READY',
        row_version rowversion,
        CONSTRAINT PK_pipeline_watermark PRIMARY KEY (pipeline_name, source_system)
    );
END;
GO

IF OBJECT_ID('audit.pipeline_run','U') IS NULL
BEGIN
    CREATE TABLE audit.pipeline_run (
        run_id uniqueidentifier NOT NULL DEFAULT NEWID(),
        pipeline_name varchar(128) NOT NULL,
        environment varchar(16) NOT NULL,
        started_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        ended_utc datetime2(3) NULL,
        source_count bigint NULL,
        raw_count bigint NULL,
        clean_count bigint NULL,
        quarantine_count bigint NULL,
        duplicate_count bigint NULL,
        stale_count bigint NULL,
        merged_count bigint NULL,
        status varchar(20) NOT NULL,
        message nvarchar(2000) NULL,
        CONSTRAINT PK_pipeline_run PRIMARY KEY (run_id)
    );
END;
GO

IF OBJECT_ID('dq.quarantine_event','U') IS NULL
BEGIN
    CREATE TABLE dq.quarantine_event (
        quarantine_id bigint IDENTITY(1,1) NOT NULL,
        source_system varchar(64) NOT NULL,
        entity_name varchar(64) NOT NULL,
        business_key varchar(256) NULL,
        event_id varchar(128) NULL,
        property_code varchar(32) NULL,
        reason_code varchar(64) NOT NULL,
        reason_detail nvarchar(1000) NULL,
        raw_payload nvarchar(max) NULL,
        quarantined_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_quarantine_event PRIMARY KEY (quarantine_id)
    );
END;
GO

IF OBJECT_ID('raw.hotel_reservation','U') IS NULL
BEGIN
    CREATE TABLE raw.hotel_reservation (
        ingest_id bigint IDENTITY(1,1) NOT NULL,
        source_scn bigint NOT NULL,
        reservation_id varchar(64) NOT NULL,
        guest_id varchar(64) NOT NULL,
        property_code varchar(32) NOT NULL,
        arrival_date date NOT NULL,
        departure_date date NOT NULL,
        reservation_status varchar(32) NOT NULL,
        room_type_code varchar(32) NULL,
        rooms int NOT NULL,
        revenue_amount decimal(18,2) NULL,
        source_updated_utc datetime2(3) NOT NULL,
        batch_id uniqueidentifier NOT NULL,
        ingested_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_raw_hotel_reservation PRIMARY KEY (ingest_id)
    );
END;
GO

IF OBJECT_ID('raw.slot_play_event','U') IS NULL
BEGIN
    CREATE TABLE raw.slot_play_event (
        ingest_id bigint IDENTITY(1,1) NOT NULL,
        event_id varchar(128) NOT NULL,
        play_id varchar(128) NULL,
        machine_id varchar(64) NOT NULL,
        property_code varchar(32) NOT NULL,
        event_utc datetime2(3) NOT NULL,
        event_type varchar(32) NOT NULL,
        coin_in decimal(18,2) NULL,
        payout decimal(18,2) NULL,
        jackpot_amount decimal(18,2) NULL,
        schema_version varchar(16) NOT NULL,
        source_partition int NULL,
        source_offset bigint NULL,
        raw_payload nvarchar(max) NULL,
        ingested_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_raw_slot_play_event PRIMARY KEY (ingest_id)
    );
END;
GO

IF OBJECT_ID('core.guest','U') IS NULL
BEGIN
    CREATE TABLE core.guest (
        guest_sk bigint IDENTITY(1,1) NOT NULL,
        enterprise_guest_id varchar(64) NOT NULL,
        source_guest_id varchar(64) NULL,
        first_name nvarchar(100) NULL,
        last_name nvarchar(100) NULL,
        email_hash char(64) NULL,
        phone_hash char(64) NULL,
        loyalty_tier varchar(32) NULL,
        effective_from_utc datetime2(3) NOT NULL,
        effective_to_utc datetime2(3) NULL,
        is_current bit NOT NULL DEFAULT 1,
        CONSTRAINT PK_core_guest PRIMARY KEY (guest_sk)
    );
END;
GO

IF OBJECT_ID('core.hotel_reservation','U') IS NULL
BEGIN
    CREATE TABLE core.hotel_reservation (
        reservation_sk bigint IDENTITY(1,1) NOT NULL,
        reservation_id varchar(64) NOT NULL,
        guest_sk bigint NULL,
        property_code varchar(32) NOT NULL,
        arrival_date date NOT NULL,
        departure_date date NOT NULL,
        reservation_status varchar(32) NOT NULL,
        room_type_code varchar(32) NULL,
        rooms int NOT NULL,
        revenue_amount decimal(18,2) NULL,
        source_scn bigint NOT NULL,
        source_updated_utc datetime2(3) NOT NULL,
        last_batch_id uniqueidentifier NOT NULL,
        updated_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_core_hotel_reservation PRIMARY KEY (reservation_sk),
        CONSTRAINT UQ_core_hotel_reservation UNIQUE (reservation_id)
    );
END;
GO

IF OBJECT_ID('core.gaming_event','U') IS NULL
BEGIN
    CREATE TABLE core.gaming_event (
        event_sk bigint IDENTITY(1,1) NOT NULL,
        event_id varchar(128) NOT NULL,
        play_id varchar(128) NULL,
        machine_id varchar(64) NOT NULL,
        property_code varchar(32) NOT NULL,
        event_utc datetime2(3) NOT NULL,
        event_type varchar(32) NOT NULL,
        coin_in decimal(18,2) NULL,
        payout decimal(18,2) NULL,
        net_win AS (ISNULL([coin_in],(0))-ISNULL([payout],(0))) PERSISTED,
        jackpot_amount decimal(18,2) NULL,
        schema_version varchar(16) NOT NULL,
        first_processed_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_core_gaming_event PRIMARY KEY (event_sk),
        CONSTRAINT UQ_core_gaming_event UNIQUE (event_id)
    );
END;
GO

IF OBJECT_ID('curated.guest_360','U') IS NULL
BEGIN
    CREATE TABLE curated.guest_360 (
        enterprise_guest_id varchar(64) NOT NULL,
        preferred_property_code varchar(32) NULL,
        loyalty_tier varchar(32) NULL,
        lifetime_room_revenue decimal(18,2) NOT NULL DEFAULT 0,
        lifetime_coin_in decimal(18,2) NOT NULL DEFAULT 0,
        lifetime_net_win decimal(18,2) NOT NULL DEFAULT 0,
        lifetime_reward_value decimal(18,2) NOT NULL DEFAULT 0,
        last_visit_utc datetime2(3) NULL,
        updated_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_guest_360 PRIMARY KEY (enterprise_guest_id)
    );
END;
GO

IF OBJECT_ID('mart.property_daily','U') IS NULL
BEGIN
    CREATE TABLE mart.property_daily (
        property_code varchar(32) NOT NULL,
        business_date date NOT NULL,
        rooms_sold int NOT NULL,
        rooms_available int NOT NULL,
        occupancy_pct decimal(9,2) NULL,
        room_revenue decimal(18,2) NULL,
        adr decimal(18,2) NULL,
        gaming_coin_in decimal(18,2) NULL,
        gaming_net_win decimal(18,2) NULL,
        reward_redemptions decimal(18,2) NULL,
        pipeline_freshness_minutes int NULL,
        published_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_property_daily PRIMARY KEY (property_code,business_date)
    );
END;
GO

CREATE OR ALTER PROCEDURE ctl.usp_begin_pipeline
    @pipeline_name varchar(128),
    @source_system varchar(64),
    @environment varchar(16),
    @high_watermark bigint,
    @run_id uniqueidentifier OUTPUT,
    @low_watermark bigint OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET @run_id = NEWID();
    BEGIN TRAN;
    SELECT @low_watermark = ISNULL(low_watermark,0)
      FROM ctl.pipeline_watermark WITH (UPDLOCK,HOLDLOCK)
     WHERE pipeline_name=@pipeline_name AND source_system=@source_system;
    IF @low_watermark IS NULL SET @low_watermark=0;
    MERGE ctl.pipeline_watermark AS t
    USING (SELECT @pipeline_name pipeline_name,@source_system source_system) s
       ON t.pipeline_name=s.pipeline_name AND t.source_system=s.source_system
    WHEN MATCHED THEN UPDATE SET high_watermark=@high_watermark,status='RUNNING'
    WHEN NOT MATCHED THEN INSERT(pipeline_name,source_system,low_watermark,high_watermark,status)
         VALUES(@pipeline_name,@source_system,@low_watermark,@high_watermark,'RUNNING');
    INSERT audit.pipeline_run(run_id,pipeline_name,environment,status)
    VALUES(@run_id,@pipeline_name,@environment,'RUNNING');
    COMMIT;
END;
GO

CREATE OR ALTER PROCEDURE core.usp_merge_hotel_reservation
    @batch_id uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    ;WITH src AS (
      SELECT *,
             ROW_NUMBER() OVER(
               PARTITION BY reservation_id
               ORDER BY source_scn DESC,source_updated_utc DESC,ingest_id DESC
             ) rn
      FROM raw.hotel_reservation
      WHERE batch_id=@batch_id
    )
    MERGE core.hotel_reservation WITH (HOLDLOCK) AS t
    USING (SELECT * FROM src WHERE rn=1) AS s
      ON t.reservation_id=s.reservation_id
    WHEN MATCHED AND s.source_scn >= t.source_scn THEN UPDATE SET
      property_code=s.property_code,
      arrival_date=s.arrival_date,
      departure_date=s.departure_date,
      reservation_status=UPPER(LTRIM(RTRIM(s.reservation_status))),
      room_type_code=UPPER(LTRIM(RTRIM(s.room_type_code))),
      rooms=s.rooms,
      revenue_amount=s.revenue_amount,
      source_scn=s.source_scn,
      source_updated_utc=s.source_updated_utc,
      last_batch_id=@batch_id,
      updated_utc=SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT(
      reservation_id,property_code,arrival_date,departure_date,reservation_status,
      room_type_code,rooms,revenue_amount,source_scn,source_updated_utc,last_batch_id
    ) VALUES(
      s.reservation_id,s.property_code,s.arrival_date,s.departure_date,
      UPPER(LTRIM(RTRIM(s.reservation_status))),UPPER(LTRIM(RTRIM(s.room_type_code))),
      s.rooms,s.revenue_amount,s.source_scn,s.source_updated_utc,@batch_id
    );
END;
GO

CREATE OR ALTER PROCEDURE ctl.usp_commit_pipeline
    @run_id uniqueidentifier,
    @pipeline_name varchar(128),
    @source_system varchar(64),
    @high_watermark bigint,
    @source_count bigint,
    @clean_count bigint,
    @quarantine_count bigint,
    @duplicate_count bigint,
    @stale_count bigint,
    @merged_count bigint
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @source_count <> @clean_count + @quarantine_count + @duplicate_count + @stale_count
        THROW 51000,'Unexplained source rows detected',1;
    BEGIN TRAN;
    UPDATE ctl.pipeline_watermark
       SET low_watermark=@high_watermark,
           high_watermark=@high_watermark,
           last_success_utc=SYSUTCDATETIME(),
           status='READY'
     WHERE pipeline_name=@pipeline_name AND source_system=@source_system;
    UPDATE audit.pipeline_run
       SET ended_utc=SYSUTCDATETIME(),
           source_count=@source_count,
           clean_count=@clean_count,
           quarantine_count=@quarantine_count,
           duplicate_count=@duplicate_count,
           stale_count=@stale_count,
           merged_count=@merged_count,
           status='SUCCEEDED'
     WHERE run_id=@run_id;
    COMMIT;
END;
GO

CREATE OR ALTER VIEW mart.vw_platform_reconciliation AS
SELECT
  CAST(SYSUTCDATETIME() AS datetime2(3)) AS observed_utc,
  (SELECT COUNT_BIG(*) FROM raw.hotel_reservation) AS raw_reservations,
  (SELECT COUNT_BIG(*) FROM core.hotel_reservation) AS core_reservations,
  (SELECT COUNT_BIG(*) FROM raw.slot_play_event) AS raw_gaming_events,
  (SELECT COUNT_BIG(*) FROM core.gaming_event) AS core_gaming_events,
  (SELECT COUNT_BIG(*) FROM dq.quarantine_event) AS quarantined_events;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_raw_hotel_reservation_key_scn')
CREATE INDEX IX_raw_hotel_reservation_key_scn ON raw.hotel_reservation(reservation_id,source_scn DESC);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_raw_slot_event_id')
CREATE INDEX IX_raw_slot_event_id ON raw.slot_play_event(event_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_core_hotel_property_arrival')
CREATE INDEX IX_core_hotel_property_arrival ON core.hotel_reservation(property_code,arrival_date);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_core_gaming_property_event')
CREATE INDEX IX_core_gaming_property_event ON core.gaming_event(property_code,event_utc);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_audit_pipeline_run')
CREATE INDEX IX_audit_pipeline_run ON audit.pipeline_run(pipeline_name,started_utc DESC);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_quarantine_entity_time')
CREATE INDEX IX_quarantine_entity_time ON dq.quarantine_event(entity_name,quarantined_utc DESC);
GO
