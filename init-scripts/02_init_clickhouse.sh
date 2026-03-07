#!/bin/bash
# Ждём пока ClickHouse полностью поднимется
until clickhouse-client --query "SELECT 1" 2>/dev/null; do
  echo "Waiting for ClickHouse..."
  sleep 2
done

clickhouse-client --multiquery <<'SQL'
CREATE DATABASE IF NOT EXISTS betquant;

CREATE TABLE IF NOT EXISTS betquant.matches (
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
ORDER BY (date, league, home_team);

CREATE TABLE IF NOT EXISTS betquant.odds (
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
    closing_away Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, league, bookmaker);

CREATE TABLE IF NOT EXISTS betquant.odds_timeseries (
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
ORDER BY (match_id, bookmaker, timestamp);

CREATE TABLE IF NOT EXISTS betquant.team_stats (
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
    xg_against Float32 DEFAULT 0
) ENGINE = SummingMergeTree()
ORDER BY (team, league, season, home_away);

CREATE TABLE IF NOT EXISTS betquant.xg_data (
    match_id String,
    date Date,
    league LowCardinality(String),
    team LowCardinality(String),
    xg Float32,
    xga Float32,
    shots UInt8 DEFAULT 0,
    shots_on_target UInt8 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY (date, team);

CREATE TABLE IF NOT EXISTS betquant.tennis_matches (
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
    tour LowCardinality(String) DEFAULT 'ATP'
) ENGINE = MergeTree()
PARTITION BY toYear(tourney_date)
ORDER BY (tourney_date, winner_name);
SQL

echo "ClickHouse init complete"