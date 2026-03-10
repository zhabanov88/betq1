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

-- ════════════════════════════════════════════════════════════════
--  BetQuant — Esports Schema
--  Таблицы: esports_matches, esports_games
--  Источники: PandaScore, OpenDota, octane.gg
-- ════════════════════════════════════════════════════════════════

-- Матчи (серии: BO1 / BO3 / BO5)
CREATE TABLE IF NOT EXISTS betquant.esports_matches (
    match_id       String,
    source         LowCardinality(String),   -- pandascore / opendota / octane
    date           Date,
    game           LowCardinality(String),   -- CS2, League of Legends, Dota 2, ...
    game_slug      LowCardinality(String),   -- csgo, lol, dota2, ...
    league         LowCardinality(String),   -- ESL Pro League, LCK, ...
    serie          LowCardinality(String),   -- Season 17, Split 1, ...
    tournament     LowCardinality(String),
    tier           LowCardinality(String),   -- S / A / B / C / unranked
    team1          LowCardinality(String),
    team2          LowCardinality(String),
    team1_id       String DEFAULT '',
    team2_id       String DEFAULT '',
    score1         UInt8 DEFAULT 0,          -- карты/игры выиграно team1
    score2         UInt8 DEFAULT 0,          -- карты/игры выиграно team2
    winner         LowCardinality(String),
    format         UInt8 DEFAULT 1,          -- всего карт в серии (1/3/5)
    match_type     LowCardinality(String) DEFAULT '',
    live_url       String DEFAULT '',
    stream_url     String DEFAULT ''
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (game, date, team1, team2)
SETTINGS index_granularity = 8192;

-- Отдельные карты / игры внутри матча
CREATE TABLE IF NOT EXISTS betquant.esports_games (
    match_id       String,
    game_num       UInt8,
    date           Date,
    game           LowCardinality(String),
    team1          LowCardinality(String),
    team2          LowCardinality(String),
    winner         LowCardinality(String),
    map_name       LowCardinality(String) DEFAULT '',
    length         UInt16 DEFAULT 0,         -- длительность в секундах
    status         LowCardinality(String) DEFAULT ''
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (game, date, match_id, game_num)
SETTINGS index_granularity = 8192;

-- ═══════════════════════════════════════════════════════════
--  BetQuant — Volleyball + NFL Tables
--  Применить:
--    docker exec -i betquant-ch clickhouse-client --multiquery < 04-volleyball-nfl.sql
-- ═══════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS betquant;

-- ─────────────────────────────────────────────────────────
--  VOLLEYBALL MATCHES
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS betquant.volleyball_matches (
    match_id          String,
    source            LowCardinality(String) DEFAULT 'espn',
    date              Date,
    season            LowCardinality(String),
    competition       LowCardinality(String),
    competition_level LowCardinality(String) DEFAULT '',
    gender            LowCardinality(String) DEFAULT 'female',
    round             LowCardinality(String) DEFAULT '',
    venue             LowCardinality(String) DEFAULT '',
    home_team         LowCardinality(String),
    away_team         LowCardinality(String),
    -- Итог
    home_sets         UInt8 DEFAULT 0,
    away_sets         UInt8 DEFAULT 0,
    result            FixedString(1) DEFAULT 'H',
    total_sets        UInt8 DEFAULT 0,
    -- Счёт по партиям
    home_s1 UInt8 DEFAULT 0, away_s1 UInt8 DEFAULT 0,
    home_s2 UInt8 DEFAULT 0, away_s2 UInt8 DEFAULT 0,
    home_s3 UInt8 DEFAULT 0, away_s3 UInt8 DEFAULT 0,
    home_s4 UInt8 DEFAULT 0, away_s4 UInt8 DEFAULT 0,
    home_s5 UInt8 DEFAULT 0, away_s5 UInt8 DEFAULT 0,
    home_total_pts    UInt16 DEFAULT 0,
    away_total_pts    UInt16 DEFAULT 0,
    total_points      UInt16 DEFAULT 0,
    duration_min      UInt16 DEFAULT 0,
    -- Атака
    home_kills        UInt16 DEFAULT 0, away_kills        UInt16 DEFAULT 0,
    home_attack_err   UInt8  DEFAULT 0, away_attack_err   UInt8  DEFAULT 0,
    home_attack_att   UInt16 DEFAULT 0, away_attack_att   UInt16 DEFAULT 0,
    home_hit_pct      Float32 DEFAULT 0, away_hit_pct     Float32 DEFAULT 0,
    -- Подача
    home_aces         UInt8 DEFAULT 0,  away_aces         UInt8 DEFAULT 0,
    home_serve_err    UInt8 DEFAULT 0,  away_serve_err    UInt8 DEFAULT 0,
    -- Блоки
    home_blocks_total UInt8 DEFAULT 0,  away_blocks_total UInt8 DEFAULT 0,
    home_block_solos  UInt8 DEFAULT 0,  away_block_solos  UInt8 DEFAULT 0,
    home_block_assists UInt8 DEFAULT 0, away_block_assists UInt8 DEFAULT 0,
    -- Защита
    home_digs         UInt16 DEFAULT 0, away_digs         UInt16 DEFAULT 0,
    home_reception_err UInt8 DEFAULT 0, away_reception_err UInt8 DEFAULT 0,
    home_assists      UInt16 DEFAULT 0, away_assists      UInt16 DEFAULT 0,
    -- Разбивка очков
    home_pts_from_kills  UInt16 DEFAULT 0, away_pts_from_kills  UInt16 DEFAULT 0,
    home_pts_from_aces   UInt8  DEFAULT 0, away_pts_from_aces   UInt8  DEFAULT 0,
    home_pts_from_blocks UInt8  DEFAULT 0, away_pts_from_blocks UInt8  DEFAULT 0,
    home_opponent_errors UInt8  DEFAULT 0, away_opponent_errors UInt8  DEFAULT 0,
    -- Коэффициенты
    b365_home Float32 DEFAULT 0, b365_away Float32 DEFAULT 0,
    ou_sets_line Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, competition, home_team)
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────
--  VOLLEYBALL SET STATS
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS betquant.volleyball_set_stats (
    match_id     String,
    date         Date,
    competition  LowCardinality(String),
    gender       LowCardinality(String) DEFAULT '',
    home_team    LowCardinality(String),
    away_team    LowCardinality(String),
    set_num      UInt8,
    home_pts     UInt8 DEFAULT 0,
    away_pts     UInt8 DEFAULT 0,
    duration_min UInt8 DEFAULT 0,
    home_kills   UInt8 DEFAULT 0, away_kills   UInt8 DEFAULT 0,
    home_aces    UInt8 DEFAULT 0, away_aces    UInt8 DEFAULT 0,
    home_blocks  UInt8 DEFAULT 0, away_blocks  UInt8 DEFAULT 0,
    home_errors  UInt8 DEFAULT 0, away_errors  UInt8 DEFAULT 0,
    home_hit_pct Float32 DEFAULT 0, away_hit_pct Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, set_num)
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────
--  NFL GAMES
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS betquant.nfl_games (
    game_id      String,
    source       LowCardinality(String) DEFAULT 'espn',
    season       UInt16,
    season_type  LowCardinality(String) DEFAULT 'REG',
    week         UInt8  DEFAULT 0,
    date         Date,
    home_team    LowCardinality(String),
    away_team    LowCardinality(String),
    venue        LowCardinality(String) DEFAULT '',
    roof         LowCardinality(String) DEFAULT '',
    surface      LowCardinality(String) DEFAULT '',
    temp         Int8   DEFAULT -99,
    wind         UInt8  DEFAULT 0,
    div_game     UInt8  DEFAULT 0,
    -- Итог
    home_score   UInt8 DEFAULT 0,
    away_score   UInt8 DEFAULT 0,
    result       FixedString(1) DEFAULT 'H',
    spread       Float32 DEFAULT 0,
    total_line   Float32 DEFAULT 0,
    -- Четверти
    home_q1 UInt8 DEFAULT 0, away_q1 UInt8 DEFAULT 0,
    home_q2 UInt8 DEFAULT 0, away_q2 UInt8 DEFAULT 0,
    home_q3 UInt8 DEFAULT 0, away_q3 UInt8 DEFAULT 0,
    home_q4 UInt8 DEFAULT 0, away_q4 UInt8 DEFAULT 0,
    home_ot UInt8 DEFAULT 0, away_ot UInt8 DEFAULT 0,
    overtime     UInt8 DEFAULT 0,
    -- Продвинутые метрики
    home_epa_total    Float32 DEFAULT 0, away_epa_total    Float32 DEFAULT 0,
    home_epa_per_play Float32 DEFAULT 0, away_epa_per_play Float32 DEFAULT 0,
    home_epa_pass     Float32 DEFAULT 0, away_epa_pass     Float32 DEFAULT 0,
    home_epa_rush     Float32 DEFAULT 0, away_epa_rush     Float32 DEFAULT 0,
    home_success_rate Float32 DEFAULT 0, away_success_rate Float32 DEFAULT 0,
    home_ypp          Float32 DEFAULT 0, away_ypp          Float32 DEFAULT 0,
    home_turnovers    UInt8 DEFAULT 0,   away_turnovers    UInt8 DEFAULT 0,
    home_penalties    UInt8 DEFAULT 0,   away_penalties    UInt8 DEFAULT 0,
    home_penalty_yds  UInt16 DEFAULT 0,  away_penalty_yds  UInt16 DEFAULT 0,
    home_first_downs  UInt8 DEFAULT 0,   away_first_downs  UInt8 DEFAULT 0,
    home_third_pct    Float32 DEFAULT 0, away_third_pct    Float32 DEFAULT 0,
    home_wp_pregame   Float32 DEFAULT 0, away_wp_pregame   Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (date, season, home_team)
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────
--  NFL PLAYER STATS
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS betquant.nfl_player_stats (
    player_id    String DEFAULT '',
    player_name  LowCardinality(String),
    team         LowCardinality(String),
    season       UInt16,
    week         UInt8 DEFAULT 0,
    season_type  LowCardinality(String) DEFAULT 'REG',
    game_id      String DEFAULT '',
    date         Date,
    position     LowCardinality(String) DEFAULT '',
    opponent     LowCardinality(String) DEFAULT '',
    -- Passing
    completions      UInt16 DEFAULT 0,
    attempts         UInt16 DEFAULT 0,
    passing_yards    UInt16 DEFAULT 0,
    passing_tds      UInt8  DEFAULT 0,
    interceptions    UInt8  DEFAULT 0,
    sacks            UInt8  DEFAULT 0,
    sack_yards       UInt8  DEFAULT 0,
    passing_epa      Float32 DEFAULT 0,
    -- Rushing
    carries          UInt16 DEFAULT 0,
    rushing_yards    Int16  DEFAULT 0,
    rushing_tds      UInt8  DEFAULT 0,
    rushing_fumbles_lost UInt8 DEFAULT 0,
    rushing_epa      Float32 DEFAULT 0,
    -- Receiving
    receptions       UInt16 DEFAULT 0,
    targets          UInt16 DEFAULT 0,
    receiving_yards  Int16  DEFAULT 0,
    receiving_tds    UInt8  DEFAULT 0,
    receiving_epa    Float32 DEFAULT 0,
    target_share     Float32 DEFAULT 0,
    air_yards_share  Float32 DEFAULT 0,
    wopr             Float32 DEFAULT 0,
    racr             Float32 DEFAULT 0,
    -- Fantasy
    fantasy_points     Float32 DEFAULT 0,
    fantasy_points_ppr Float32 DEFAULT 0,
    special_teams_tds  UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (season, week, player_name)
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────
--  NFL PBP (пустая, для совместимости)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS betquant.nfl_pbp (
    play_id      UInt32 DEFAULT 0,
    game_id      String,
    date         Date,
    season       UInt16,
    season_type  LowCardinality(String) DEFAULT 'REG',
    week         UInt8 DEFAULT 0,
    home_team    LowCardinality(String) DEFAULT '',
    away_team    LowCardinality(String) DEFAULT '',
    posteam      LowCardinality(String) DEFAULT '',
    defteam      LowCardinality(String) DEFAULT '',
    side_of_field LowCardinality(String) DEFAULT '',
    yardline_100 UInt8 DEFAULT 50,
    quarter      UInt8 DEFAULT 1,
    down         UInt8 DEFAULT 0,
    ydstogo      UInt8 DEFAULT 0,
    yards_gained Int8  DEFAULT 0,
    play_type    LowCardinality(String) DEFAULT '',
    pass_attempt UInt8 DEFAULT 0,
    rush_attempt UInt8 DEFAULT 0,
    complete_pass UInt8 DEFAULT 0,
    touchdown    UInt8 DEFAULT 0,
    interception UInt8 DEFAULT 0,
    fumble       UInt8 DEFAULT 0,
    fumble_lost  UInt8 DEFAULT 0,
    sack         UInt8 DEFAULT 0,
    penalty      UInt8 DEFAULT 0,
    penalty_yards Int8 DEFAULT 0,
    first_down   UInt8 DEFAULT 0,
    ep           Float32 DEFAULT 0,
    epa          Float32 DEFAULT 0,
    wp           Float32 DEFAULT 0,
    wpa          Float32 DEFAULT 0,
    air_yards    Int8 DEFAULT 0,
    success      UInt8 DEFAULT 0,
    passer       LowCardinality(String) DEFAULT '',
    rusher       LowCardinality(String) DEFAULT '',
    receiver     LowCardinality(String) DEFAULT '',
    home_score   UInt8 DEFAULT 0,
    away_score   UInt8 DEFAULT 0,
    score_diff   Int8  DEFAULT 0,
    drive_id     UInt16 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (game_id, play_id)
SETTINGS index_granularity = 8192;

SELECT 'volleyball + nfl tables created' AS status;

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


