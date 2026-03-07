-- =========================================
-- BETTING ADVANCED - ClickHouse Schema
-- Optimized for analytical queries on millions of rows
-- =========================================

-- =================== ODDS HISTORY ===================
-- Full odds movement timeline for every match/market/bookmaker
CREATE TABLE IF NOT EXISTS betting.odds_history (
    match_id String,
    bookmaker String,
    market LowCardinality(String),      -- 1x2, btts, over_under_2_5, asian_handicap, etc
    selection LowCardinality(String),   -- home, draw, away, yes, no, etc
    line Nullable(Float32),             -- for handicap/totals markets
    odds Float32,
    recorded_at DateTime,
    source LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (match_id, market, selection, bookmaker, recorded_at)
TTL recorded_at + INTERVAL 5 YEAR;

-- =================== MATCH EVENTS ===================
-- Every event during a match (goals, cards, substitutions)
CREATE TABLE IF NOT EXISTS betting.match_events (
    match_id String,
    event_type LowCardinality(String),  -- goal, yellow_card, red_card, substitution, var_decision
    minute UInt16,
    extra_time UInt8 DEFAULT 0,
    team LowCardinality(String),        -- home/away
    player_id Nullable(String),
    player_name Nullable(String),
    detail Nullable(String),
    assist_player_id Nullable(String),
    recorded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, minute, event_type);

-- =================== BACKTEST BETS (series data) ===================
-- Stores full bet-by-bet backtest output for chart rendering
CREATE TABLE IF NOT EXISTS betting.backtest_bets (
    backtest_id String,
    bet_number UInt32,
    match_id String,
    match_date Date,
    competition LowCardinality(String),
    home_team String,
    away_team String,
    market LowCardinality(String),
    selection LowCardinality(String),
    odds Float32,
    stake Float32,
    result LowCardinality(String),      -- win, lose, void, push
    profit Float32,
    bankroll Float32,                   -- bankroll after this bet
    edge Nullable(Float32),             -- model edge at time of bet
    metadata String DEFAULT '{}'       -- JSON for additional data
) ENGINE = MergeTree()
ORDER BY (backtest_id, bet_number);

-- =================== TEAM FORM (pre-computed rolling stats) ===================
CREATE TABLE IF NOT EXISTS betting.team_form (
    team_id String,
    competition_id String,
    season String,
    as_of_date Date,
    is_home UInt8,
    -- Last N matches
    last_5_wins UInt8,
    last_5_draws UInt8,
    last_5_losses UInt8,
    last_5_goals_scored Float32,
    last_5_goals_conceded Float32,
    last_5_xg Float32,
    last_5_xga Float32,
    last_10_wins UInt8,
    last_10_draws UInt8,
    last_10_losses UInt8,
    -- Seasonal totals
    played UInt16,
    wins UInt16,
    draws UInt16,
    losses UInt16,
    goals_scored UInt16,
    goals_conceded UInt16,
    clean_sheets UInt16,
    failed_to_score UInt16,
    xg_total Float32,
    xga_total Float32,
    -- Over/Under stats
    over_0_5_pct Float32,
    over_1_5_pct Float32,
    over_2_5_pct Float32,
    over_3_5_pct Float32,
    btts_pct Float32,
    avg_corners Float32,
    avg_cards Float32
) ENGINE = ReplacingMergeTree()
ORDER BY (team_id, competition_id, season, as_of_date, is_home);

-- =================== HEAD TO HEAD STATS ===================
CREATE TABLE IF NOT EXISTS betting.h2h_stats (
    home_team_id String,
    away_team_id String,
    competition_id String,
    last_updated Date,
    total_meetings UInt16,
    home_wins UInt16,
    draws UInt16,
    away_wins UInt16,
    home_goals_avg Float32,
    away_goals_avg Float32,
    over_2_5_pct Float32,
    btts_pct Float32,
    last_5_results String,   -- JSON array
    streak String            -- current H2H streak info
) ENGINE = ReplacingMergeTree()
ORDER BY (home_team_id, away_team_id, competition_id);

-- =================== MARKET EFFICIENCY ===================
-- Tracks bookmaker margins and closing line value over time
CREATE TABLE IF NOT EXISTS betting.market_efficiency (
    match_id String,
    bookmaker LowCardinality(String),
    market LowCardinality(String),
    opening_odds_home Float32,
    opening_odds_draw Nullable(Float32),
    opening_odds_away Float32,
    closing_odds_home Float32,
    closing_odds_draw Nullable(Float32),
    closing_odds_away Float32,
    margin_opening Float32,
    margin_closing Float32,
    clv_home Float32,   -- Closing Line Value
    clv_away Float32,
    match_date Date
) ENGINE = MergeTree()
PARTITION BY toYear(match_date)
ORDER BY (match_date, bookmaker, market);

-- =================== LIVE MATCH STATS ===================
CREATE TABLE IF NOT EXISTS betting.live_stats (
    match_id String,
    minute UInt16,
    recorded_at DateTime,
    home_shots UInt8,
    away_shots UInt8,
    home_shots_on_target UInt8,
    away_shots_on_target UInt8,
    home_possession Float32,
    home_attacks UInt16,
    away_attacks UInt16,
    home_dangerous_attacks UInt16,
    away_dangerous_attacks UInt16,
    in_play_home_odds Float32,
    in_play_draw_odds Nullable(Float32),
    in_play_away_odds Float32
) ENGINE = MergeTree()
ORDER BY (match_id, minute);

-- =================== STRATEGY SIGNALS ===================
-- Logs when strategies generate signals (for strategy comparison)
CREATE TABLE IF NOT EXISTS betting.strategy_signals (
    strategy_id String,
    user_id String,
    match_id String,
    market LowCardinality(String),
    selection LowCardinality(String),
    signal_type LowCardinality(String),  -- buy, sell, watch
    confidence Float32,
    expected_odds Float32,
    metadata String DEFAULT '{}',
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (strategy_id, created_at);

-- =================== MATERIALIZED VIEWS ===================

-- Competition summary stats (pre-aggregated for fast dashboards)
CREATE MATERIALIZED VIEW IF NOT EXISTS betting.competition_season_stats
ENGINE = AggregatingMergeTree()
ORDER BY (competition_id, season)
AS SELECT
    competition_id,
    season,
    count() as total_matches,
    avg(home_goals + away_goals) as avg_total_goals,
    avg(home_goals) as avg_home_goals,
    avg(away_goals) as avg_away_goals,
    countIf(home_goals > away_goals) / count() as home_win_rate,
    countIf(home_goals = away_goals) / count() as draw_rate,
    countIf(home_goals < away_goals) / count() as away_win_rate,
    countIf(home_goals + away_goals > 2) / count() as over_2_5_rate,
    countIf(home_goals > 0 AND away_goals > 0) / count() as btts_rate
FROM (
    SELECT
        m.competition_id,
        m.season,
        m.score_home as home_goals,
        m.score_away as away_goals
    FROM postgresql('postgres:5432', 'betting_advanced', 'matches', 'betting_user', 'BettingPass2024!')
    WHERE status = 'FINISHED'
)
GROUP BY competition_id, season;
