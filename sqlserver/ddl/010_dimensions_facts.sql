SET XACT_ABORT ON;
GO

CREATE TABLE dw.dim_guest (
    guest_sk bigint IDENTITY(1,1) PRIMARY KEY,
    enterprise_guest_id varchar(64) NOT NULL,
    guest_token varchar(128) NOT NULL,
    loyalty_tier varchar(30) NULL,
    home_market varchar(80) NULL,
    acquisition_channel varchar(40) NULL,
    effective_from_utc datetime2(3) NOT NULL,
    effective_to_utc datetime2(3) NOT NULL,
    is_current bit NOT NULL,
    record_hash binary(32) NOT NULL,
    source_system varchar(40) NOT NULL,
    CONSTRAINT UQ_dim_guest_version UNIQUE (enterprise_guest_id, effective_from_utc)
);
CREATE INDEX IX_dim_guest_current ON dw.dim_guest(enterprise_guest_id, is_current) INCLUDE(guest_sk, loyalty_tier);
GO

CREATE TABLE dw.dim_property (
    property_sk int IDENTITY(1,1) PRIMARY KEY,
    property_code varchar(20) NOT NULL UNIQUE,
    property_name nvarchar(120) NOT NULL,
    market varchar(80) NOT NULL,
    timezone_name varchar(80) NOT NULL
);
GO

CREATE TABLE dw.dim_room (
    room_sk int IDENTITY(1,1) PRIMARY KEY,
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    room_type_code varchar(30) NOT NULL,
    room_category varchar(50) NULL,
    max_occupancy smallint NULL,
    active_flag bit NOT NULL DEFAULT (1),
    CONSTRAINT UQ_dim_room UNIQUE(property_sk, room_type_code)
);
GO

CREATE TABLE dw.dim_game (
    game_sk int IDENTITY(1,1) PRIMARY KEY,
    game_code varchar(40) NOT NULL UNIQUE,
    game_family varchar(40) NOT NULL,
    game_type varchar(40) NOT NULL,
    active_flag bit NOT NULL DEFAULT (1)
);
GO

CREATE TABLE dw.dim_reward_tier (
    reward_tier_sk int IDENTITY(1,1) PRIMARY KEY,
    tier_code varchar(30) NOT NULL UNIQUE,
    tier_rank smallint NOT NULL,
    effective_from date NOT NULL,
    effective_to date NOT NULL
);
GO

CREATE TABLE dw.fact_reservation (
    reservation_sk bigint IDENTITY(1,1) PRIMARY KEY,
    reservation_id varchar(64) NOT NULL UNIQUE,
    guest_sk bigint NULL REFERENCES dw.dim_guest(guest_sk),
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    room_sk int NULL REFERENCES dw.dim_room(room_sk),
    booking_date date NOT NULL,
    arrival_date date NOT NULL,
    departure_date date NOT NULL,
    reservation_status varchar(30) NOT NULL,
    channel varchar(30) NOT NULL,
    rooms smallint NOT NULL,
    adults smallint NULL,
    children smallint NULL,
    room_revenue decimal(19,2) NOT NULL,
    source_sequence bigint NOT NULL,
    source_updated_utc datetime2(3) NOT NULL,
    loaded_utc datetime2(3) NOT NULL DEFAULT (SYSUTCDATETIME())
);
CREATE INDEX IX_fact_reservation_stay ON dw.fact_reservation(property_sk, arrival_date, departure_date)
INCLUDE(reservation_status, rooms, room_revenue, channel);
GO

CREATE TABLE dw.fact_gaming_session (
    gaming_session_sk bigint IDENTITY(1,1) PRIMARY KEY,
    session_id varchar(80) NOT NULL UNIQUE,
    guest_sk bigint NULL REFERENCES dw.dim_guest(guest_sk),
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    game_sk int NULL REFERENCES dw.dim_game(game_sk),
    device_token varchar(128) NULL,
    session_start_utc datetime2(3) NOT NULL,
    session_end_utc datetime2(3) NULL,
    coin_in decimal(19,2) NOT NULL DEFAULT (0),
    payout decimal(19,2) NOT NULL DEFAULT (0),
    jackpot_amount decimal(19,2) NOT NULL DEFAULT (0),
    theoretical_win decimal(19,2) NULL,
    actual_win AS (coin_in - payout - jackpot_amount) PERSISTED,
    event_count int NOT NULL DEFAULT (0),
    last_event_utc datetime2(3) NOT NULL
);
CREATE INDEX IX_fact_gaming_session_time ON dw.fact_gaming_session(property_sk, session_start_utc DESC)
INCLUDE(coin_in, payout, jackpot_amount, guest_sk, game_sk);
GO

CREATE TABLE dw.fact_reward_activity (
    reward_activity_sk bigint IDENTITY(1,1) PRIMARY KEY,
    reward_event_id varchar(80) NOT NULL UNIQUE,
    guest_sk bigint NOT NULL REFERENCES dw.dim_guest(guest_sk),
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    activity_utc datetime2(3) NOT NULL,
    activity_type varchar(20) NOT NULL,
    source_domain varchar(30) NOT NULL,
    points_delta bigint NOT NULL,
    monetary_value decimal(19,2) NOT NULL,
    trace_id uniqueidentifier NULL
);
GO

CREATE TABLE dw.fact_mobile_event (
    mobile_event_sk bigint IDENTITY(1,1) PRIMARY KEY,
    mobile_event_id varchar(80) NOT NULL UNIQUE,
    guest_sk bigint NULL REFERENCES dw.dim_guest(guest_sk),
    property_sk int NOT NULL REFERENCES dw.dim_property(property_sk),
    event_utc datetime2(3) NOT NULL,
    action varchar(80) NOT NULL,
    screen_name varchar(80) NOT NULL,
    anonymous_session_id varchar(80) NULL,
    offer_token varchar(128) NULL,
    device_family varchar(40) NULL
);
GO
