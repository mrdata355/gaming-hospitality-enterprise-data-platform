SET XACT_ABORT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ctl') EXEC('CREATE SCHEMA ctl');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg') EXEC('CREATE SCHEMA stg');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw') EXEC('CREATE SCHEMA dw');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dq') EXEC('CREATE SCHEMA dq');
GO

CREATE TABLE ctl.pipeline_watermark (
    pipeline_name sysname NOT NULL PRIMARY KEY,
    source_system varchar(40) NOT NULL,
    source_object nvarchar(256) NOT NULL,
    watermark_column sysname NOT NULL,
    last_success_value bigint NOT NULL CONSTRAINT DF_pipeline_watermark_value DEFAULT (0),
    last_batch_id uniqueidentifier NULL,
    last_success_utc datetime2(3) NULL,
    row_version rowversion NOT NULL
);
GO

CREATE TABLE ctl.pipeline_run (
    pipeline_run_id uniqueidentifier NOT NULL PRIMARY KEY,
    pipeline_name sysname NOT NULL,
    batch_id uniqueidentifier NOT NULL,
    status varchar(20) NOT NULL,
    low_watermark bigint NULL,
    high_watermark bigint NULL,
    source_rows bigint NOT NULL DEFAULT (0),
    staged_rows bigint NOT NULL DEFAULT (0),
    target_rows bigint NOT NULL DEFAULT (0),
    quarantined_rows bigint NOT NULL DEFAULT (0),
    duplicate_rows bigint NOT NULL DEFAULT (0),
    started_utc datetime2(3) NOT NULL DEFAULT (SYSUTCDATETIME()),
    completed_utc datetime2(3) NULL,
    error_number int NULL,
    error_message nvarchar(4000) NULL,
    correlation_id uniqueidentifier NOT NULL DEFAULT (NEWID()),
    CONSTRAINT CK_pipeline_run_status CHECK (status IN ('STARTED','SUCCEEDED','FAILED','CANCELLED','REPLAYED'))
);
GO
CREATE INDEX IX_pipeline_run_name_started ON ctl.pipeline_run(pipeline_name, started_utc DESC)
INCLUDE(status, source_rows, target_rows, quarantined_rows);
GO

CREATE TABLE dq.quarantine (
    quarantine_id bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id uniqueidentifier NOT NULL,
    source_system varchar(40) NOT NULL,
    source_object nvarchar(256) NOT NULL,
    business_key nvarchar(256) NULL,
    reason_code varchar(80) NOT NULL,
    reason_detail nvarchar(2000) NULL,
    raw_payload nvarchar(max) NULL,
    quarantined_utc datetime2(3) NOT NULL DEFAULT (SYSUTCDATETIME()),
    resolved_utc datetime2(3) NULL,
    resolution_note nvarchar(1000) NULL,
    CONSTRAINT FK_quarantine_run FOREIGN KEY (pipeline_run_id) REFERENCES ctl.pipeline_run(pipeline_run_id)
);
GO
