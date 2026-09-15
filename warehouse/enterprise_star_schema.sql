-- Conformed enterprise warehouse extension. Synthetic portfolio schema.
-- Core guest/property/room/game/reservation/gaming tables are created under sqlserver/ddl.

CREATE TABLE dw.dim_outlet (
    outlet_sk int IDENTITY(1,1) PRIMARY KEY,
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    outlet_code varchar(40) NOT NULL,
    outlet_name nvarchar(120) NOT NULL,
    outlet_type varchar(30) NOT NULL,
    active_flag bit NOT NULL DEFAULT 1,
    CONSTRAINT UQ_dim_outlet UNIQUE(property_sk, outlet_code)
);
GO
CREATE TABLE dw.dim_offer (
    offer_sk bigint IDENTITY(1,1) PRIMARY KEY,
    offer_token varchar(128) NOT NULL,
    offer_type varchar(40) NOT NULL,
    campaign_code varchar(60) NULL,
    valid_from_utc datetime2(3) NOT NULL,
    valid_to_utc datetime2(3) NOT NULL,
    value_amount decimal(19,2) NULL,
    is_current bit NOT NULL,
    CONSTRAINT UQ_dim_offer_version UNIQUE(offer_token, valid_from_utc)
);
GO
CREATE TABLE dw.dim_channel (
    channel_sk smallint IDENTITY(1,1) PRIMARY KEY,
    channel_code varchar(30) NOT NULL UNIQUE,
    channel_group varchar(30) NOT NULL
);
GO
CREATE TABLE dw.dim_date (
    date_sk int PRIMARY KEY,
    calendar_date date NOT NULL UNIQUE,
    calendar_year smallint NOT NULL,
    calendar_quarter tinyint NOT NULL,
    calendar_month tinyint NOT NULL,
    week_of_year tinyint NOT NULL,
    day_of_week tinyint NOT NULL,
    is_weekend bit NOT NULL
);
GO
CREATE TABLE dw.fact_outlet_spend (
    outlet_spend_sk bigint IDENTITY(1,1) PRIMARY KEY,
    transaction_id varchar(80) NOT NULL UNIQUE,
    guest_sk bigint NULL REFERENCES dw.dim_guest(guest_sk),
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    outlet_sk int NOT NULL REFERENCES dw.dim_outlet(outlet_sk),
    business_date date NOT NULL,
    transaction_utc datetime2(3) NOT NULL,
    gross_amount decimal(19,2) NOT NULL,
    discount_amount decimal(19,2) NOT NULL DEFAULT 0,
    tax_amount decimal(19,2) NOT NULL DEFAULT 0,
    net_amount AS (gross_amount - discount_amount) PERSISTED,
    source_sequence bigint NOT NULL
);
GO
CREATE TABLE dw.fact_offer_redemption (
    offer_redemption_sk bigint IDENTITY(1,1) PRIMARY KEY,
    redemption_id varchar(80) NOT NULL UNIQUE,
    offer_sk bigint NOT NULL REFERENCES dw.dim_offer(offer_sk),
    guest_sk bigint NOT NULL REFERENCES dw.dim_guest(guest_sk),
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    redemption_utc datetime2(3) NOT NULL,
    redeemed_value decimal(19,2) NOT NULL,
    source_domain varchar(30) NOT NULL
);
GO
CREATE TABLE dw.fact_payment (
    payment_sk bigint IDENTITY(1,1) PRIMARY KEY,
    payment_token varchar(128) NOT NULL UNIQUE,
    guest_sk bigint NULL REFERENCES dw.dim_guest(guest_sk),
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    payment_utc datetime2(3) NOT NULL,
    payment_domain varchar(30) NOT NULL,
    payment_method_token varchar(128) NULL,
    amount decimal(19,2) NOT NULL,
    status varchar(20) NOT NULL,
    currency_code char(3) NOT NULL DEFAULT 'USD'
);
GO
CREATE TABLE dw.fact_property_operation (
    operation_sk bigint IDENTITY(1,1) PRIMARY KEY,
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    business_date date NOT NULL,
    operation_type varchar(40) NOT NULL,
    unit_code varchar(40) NULL,
    volume_count bigint NOT NULL DEFAULT 0,
    duration_minutes decimal(12,2) NULL,
    labor_hours decimal(12,2) NULL,
    cost_amount decimal(19,2) NULL,
    service_level_pct decimal(9,4) NULL
);
GO
CREATE TABLE dw.fact_pipeline_execution (
    pipeline_execution_sk bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id uniqueidentifier NOT NULL UNIQUE,
    pipeline_name sysname NOT NULL,
    source_system varchar(40) NULL,
    started_utc datetime2(3) NOT NULL,
    completed_utc datetime2(3) NULL,
    status varchar(20) NOT NULL,
    source_rows bigint NOT NULL DEFAULT 0,
    target_rows bigint NOT NULL DEFAULT 0,
    quarantine_rows bigint NOT NULL DEFAULT 0,
    duration_seconds bigint NULL,
    compute_cost_usd decimal(19,6) NULL
);
GO
CREATE TABLE dw.fact_data_quality (
    data_quality_sk bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id uniqueidentifier NOT NULL,
    rule_name varchar(120) NOT NULL,
    severity varchar(20) NOT NULL,
    evaluated_rows bigint NOT NULL,
    failed_rows bigint NOT NULL,
    failure_rate AS (CONVERT(decimal(19,8), failed_rows) / NULLIF(evaluated_rows,0)) PERSISTED,
    evaluated_utc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
