-- BetQuant Pro — ClickHouse init (matches, odds, stats — time series)

CREATE DATABASE IF NOT EXISTS betquant;

USE betquant;

-- Main matches table
CREATE TABLE IF NOT EXISTS matches (
    id UUID DEFAULT generateUUIDv4(),
    date Date,
    datetime DateTime DEFAULT toDateTime(date),
    league LowCardinality(String),
    season LowCardinality(String),
    country LowCardinality(String),
    home_team LowCardinality(String),
    away_team LowCardinality(String),
    home_goals UInt8,
    away_goals UInt8,
    result FixedString(1),
    ht_home_goals UInt8 DEFAULT 0,
    ht_away_goals UInt8 DEFAULT 0,
    home_shots UInt8 DEFAULT 0,
    away_shots UInt8 DEFAULT 0,
    home_shots_on_target UInt8 DEFAULT 0,
    away_shots_on_target UInt8 DEFAULT 0,
    home_corners UInt8 DEFAULT 0,
    away_corners UInt8 DEFAULT 0,
    home_fouls UInt8 DEFAULT 0,
    away_fouls UInt8 DEFAULT 0,
    home_yellow UInt8 DEFAULT 0,
    away_yellow UInt8 DEFAULT 0,
    home_red UInt8 DEFAULT 0,
    away_red UInt8 DEFAULT 0,
    source LowCardinality(String) DEFAULT ''
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, league, home_team)
SETTINGS index_granularity = 8192;

-- Odds table (can be very large — 50+ bookmakers x millions of matches)
CREATE TABLE IF NOT EXISTS odds (
    match_id String,
    date Date,
    league LowCardinality(String),
    home_team LowCardinality(String),
    away_team LowCardinality(String),
    bookmaker LowCardinality(String),
    market LowCardinality(String),
    odds_home Float32,
    odds_draw Float32,
    odds_away Float32,
    odds_over Float32 DEFAULT 0,
    odds_under Float32 DEFAULT 0,
    odds_btts_yes Float32 DEFAULT 0,
    odds_btts_no Float32 DEFAULT 0,
    closing_home Float32 DEFAULT 0,
    closing_draw Float32 DEFAULT 0,
    closing_away Float32 DEFAULT 0,
    margin Float32 MATERIALIZED (1/odds_home + 1/odds_draw + 1/odds_away - 1)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, league, bookmaker)
SETTINGS index_granularity = 8192;

-- Live/historical odds time series
CREATE TABLE IF NOT EXISTS odds_timeseries (
    match_id String,
    bookmaker LowCardinality(String),
    market LowCardinality(String),
    timestamp DateTime,
    odds_1 Float32,
    odds_x Float32,
    odds_2 Float32,
    volume Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (match_id, bookmaker, timestamp)
TTL timestamp + INTERVAL 2 YEAR;

-- Team stats (aggregated)
CREATE TABLE IF NOT EXISTS team_stats (
    team LowCardinality(String),
    league LowCardinality(String),
    season LowCardinality(String),
    home_away LowCardinality(String),
    matches UInt16,
    wins UInt16,
    draws UInt16,
    losses UInt16,
    goals_for UInt16,
    goals_against UInt16,
    xg_for Float32 DEFAULT 0,
    xg_against Float32 DEFAULT 0,
    shots_for UInt16 DEFAULT 0,
    shots_against UInt16 DEFAULT 0,
    shots_on_target_for UInt16 DEFAULT 0,
    shots_on_target_against UInt16 DEFAULT 0,
    corners_for UInt16 DEFAULT 0,
    corners_against UInt16 DEFAULT 0
) ENGINE = SummingMergeTree()
ORDER BY (team, league, season, home_away);

-- xG data (per match)
CREATE TABLE IF NOT EXISTS xg_data (
    match_id String,
    date Date,
    league LowCardinality(String),
    team LowCardinality(String),
    xg Float32,
    xga Float32,
    shots UInt8 DEFAULT 0,
    shots_on_target UInt8 DEFAULT 0,
    deep_completions UInt8 DEFAULT 0,
    ppda Float32 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY (date, team);

-- Tennis
CREATE TABLE IF NOT EXISTS tennis_matches (
    tourney_id String,
    tourney_name LowCardinality(String),
    surface LowCardinality(String),
    tourney_date Date,
    winner_name LowCardinality(String),
    loser_name LowCardinality(String),
    winner_rank UInt16 DEFAULT 0,
    loser_rank UInt16 DEFAULT 0,
    score String,
    best_of UInt8 DEFAULT 3,
    round LowCardinality(String),
    w_1stIn UInt16 DEFAULT 0,
    w_1stWon UInt16 DEFAULT 0,
    w_svpt UInt16 DEFAULT 0,
    l_1stIn UInt16 DEFAULT 0,
    l_svpt UInt16 DEFAULT 0,
    tour LowCardinality(String) DEFAULT 'ATP'
) ENGINE = MergeTree()
PARTITION BY toYear(tourney_date)
ORDER BY (tourney_date, winner_name);

SELECT 'ClickHouse init complete' AS status;
