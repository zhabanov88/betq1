-- ═══════════════════════════════════════════════════════════════════════════════
--  BetQuant — Extended Sports Schema v2
--  Спорты: Basketball, Cricket, Rugby, American Football, Water Polo, Volleyball
--  Максимальная детализация по всем возможным разрезам
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS betquant;

-- ═══════════════════════════════════════════════════════════════════════════════
--  1. BASKETBALL (NBA + EuroLeague + NCAA)
--  Источники: nba_api (stats.nba.com), balldontlie.io, basketball-reference.com
-- ═══════════════════════════════════════════════════════════════════════════════

-- Матчи с разбивкой по четвертям + расширенные командные метрики
CREATE TABLE IF NOT EXISTS betquant.basketball_matches_v2 (
    match_id        String,
    source          LowCardinality(String),
    date            Date,
    datetime        DateTime DEFAULT toDateTime(date),
    season          LowCardinality(String),
    season_type     LowCardinality(String) DEFAULT 'Regular Season',  -- Regular/Playoffs/Preseason
    league          LowCardinality(String),
    home_team       LowCardinality(String),
    away_team       LowCardinality(String),
    home_team_id    String DEFAULT '',
    away_team_id    String DEFAULT '',
    venue           LowCardinality(String) DEFAULT '',
    attendance      UInt32 DEFAULT 0,

    -- Итоговый счёт
    home_pts        UInt16 DEFAULT 0,
    away_pts        UInt16 DEFAULT 0,
    result          FixedString(1) DEFAULT 'H',  -- H/A (нет ничьих)

    -- Четверти (Q1-Q4 + OT1-OT3)
    home_q1         UInt8 DEFAULT 0,  away_q1 UInt8 DEFAULT 0,
    home_q2         UInt8 DEFAULT 0,  away_q2 UInt8 DEFAULT 0,
    home_q3         UInt8 DEFAULT 0,  away_q3 UInt8 DEFAULT 0,
    home_q4         UInt8 DEFAULT 0,  away_q4 UInt8 DEFAULT 0,
    home_ot1        UInt8 DEFAULT 0,  away_ot1 UInt8 DEFAULT 0,
    home_ot2        UInt8 DEFAULT 0,  away_ot2 UInt8 DEFAULT 0,
    home_ot3        UInt8 DEFAULT 0,  away_ot3 UInt8 DEFAULT 0,
    went_to_ot      UInt8 DEFAULT 0,
    ot_periods      UInt8 DEFAULT 0,

    -- Первая половина / Вторая половина
    home_h1         UInt8 DEFAULT 0,  away_h1 UInt8 DEFAULT 0,
    home_h2         UInt8 DEFAULT 0,  away_h2 UInt8 DEFAULT 0,

    -- === АТАКА ===
    home_fgm        UInt8 DEFAULT 0,  away_fgm UInt8 DEFAULT 0,   -- Field Goals Made
    home_fga        UInt8 DEFAULT 0,  away_fga UInt8 DEFAULT 0,   -- Field Goals Attempted
    home_fg_pct     Float32 DEFAULT 0, away_fg_pct Float32 DEFAULT 0,
    home_fg3m       UInt8 DEFAULT 0,  away_fg3m UInt8 DEFAULT 0,  -- 3-Point Made
    home_fg3a       UInt8 DEFAULT 0,  away_fg3a UInt8 DEFAULT 0,  -- 3-Point Attempted
    home_fg3_pct    Float32 DEFAULT 0, away_fg3_pct Float32 DEFAULT 0,
    home_fg2m       UInt8 DEFAULT 0,  away_fg2m UInt8 DEFAULT 0,  -- 2-Point Made
    home_fg2a       UInt8 DEFAULT 0,  away_fg2a UInt8 DEFAULT 0,
    home_ftm        UInt8 DEFAULT 0,  away_ftm UInt8 DEFAULT 0,   -- Free Throws Made
    home_fta        UInt8 DEFAULT 0,  away_fta UInt8 DEFAULT 0,   -- Free Throws Attempted
    home_ft_pct     Float32 DEFAULT 0, away_ft_pct Float32 DEFAULT 0,

    -- === ПОДБОРЫ (детально) ===
    home_oreb       UInt8 DEFAULT 0,  away_oreb UInt8 DEFAULT 0,  -- Offensive Rebound
    home_dreb       UInt8 DEFAULT 0,  away_dreb UInt8 DEFAULT 0,  -- Defensive Rebound
    home_reb        UInt8 DEFAULT 0,  away_reb UInt8 DEFAULT 0,   -- Total Rebound
    home_oreb_pct   Float32 DEFAULT 0, away_oreb_pct Float32 DEFAULT 0,
    home_dreb_pct   Float32 DEFAULT 0, away_dreb_pct Float32 DEFAULT 0,
    home_reb_pct    Float32 DEFAULT 0, away_reb_pct Float32 DEFAULT 0,

    -- === ПЕРЕДАЧИ ===
    home_ast        UInt8 DEFAULT 0,  away_ast UInt8 DEFAULT 0,
    home_ast_pct    Float32 DEFAULT 0, away_ast_pct Float32 DEFAULT 0,
    home_ast_to_tov Float32 DEFAULT 0, away_ast_to_tov Float32 DEFAULT 0,

    -- === ЗАЩИТА ===
    home_stl        UInt8 DEFAULT 0,  away_stl UInt8 DEFAULT 0,   -- Steals
    home_blk        UInt8 DEFAULT 0,  away_blk UInt8 DEFAULT 0,   -- Blocks
    home_blka       UInt8 DEFAULT 0,  away_blka UInt8 DEFAULT 0,  -- Blocked Attempts (blocks against)
    home_pf         UInt8 DEFAULT 0,  away_pf UInt8 DEFAULT 0,    -- Personal Fouls
    home_pfd        UInt8 DEFAULT 0,  away_pfd UInt8 DEFAULT 0,   -- Personal Fouls Drawn

    -- === ПОТЕРИ ===
    home_tov        UInt8 DEFAULT 0,  away_tov UInt8 DEFAULT 0,   -- Turnovers
    home_tov_pct    Float32 DEFAULT 0, away_tov_pct Float32 DEFAULT 0,

    -- === ОЧКИ ПО ТИПУ ===
    home_pts_paint      UInt8 DEFAULT 0,  away_pts_paint UInt8 DEFAULT 0,      -- Points in the Paint
    home_pts_fb         UInt8 DEFAULT 0,  away_pts_fb UInt8 DEFAULT 0,         -- Fast Break Points
    home_pts_2nd_chance UInt8 DEFAULT 0,  away_pts_2nd_chance UInt8 DEFAULT 0, -- Second Chance Points
    home_pts_off_tov    UInt8 DEFAULT 0,  away_pts_off_tov UInt8 DEFAULT 0,    -- Points off Turnovers
    home_pts_bench      UInt8 DEFAULT 0,  away_pts_bench UInt8 DEFAULT 0,      -- Bench Points
    home_pts_lead_changes UInt8 DEFAULT 0, away_pts_lead_changes UInt8 DEFAULT 0,
    home_largest_lead   Int8 DEFAULT 0,   away_largest_lead Int8 DEFAULT 0,
    times_tied          UInt8 DEFAULT 0,

    -- === ПРОДВИНУТЫЕ МЕТРИКИ ===
    home_ortg       Float32 DEFAULT 0,  away_ortg Float32 DEFAULT 0,   -- Offensive Rating
    home_drtg       Float32 DEFAULT 0,  away_drtg Float32 DEFAULT 0,   -- Defensive Rating
    home_nrtg       Float32 DEFAULT 0,  away_nrtg Float32 DEFAULT 0,   -- Net Rating
    home_pace       Float32 DEFAULT 0,  away_pace Float32 DEFAULT 0,   -- Possessions per 48 min
    home_possessions UInt16 DEFAULT 0,  away_possessions UInt16 DEFAULT 0,
    home_efg_pct    Float32 DEFAULT 0,  away_efg_pct Float32 DEFAULT 0,  -- Effective FG%
    home_ts_pct     Float32 DEFAULT 0,  away_ts_pct Float32 DEFAULT 0,  -- True Shooting%
    home_pie        Float32 DEFAULT 0,  away_pie Float32 DEFAULT 0,     -- Player Impact Estimate
    home_pct_fga_2pt Float32 DEFAULT 0, away_pct_fga_2pt Float32 DEFAULT 0,
    home_pct_fga_3pt Float32 DEFAULT 0, away_pct_fga_3pt Float32 DEFAULT 0,
    home_pct_pts_2pt Float32 DEFAULT 0, away_pct_pts_2pt Float32 DEFAULT 0,
    home_pct_pts_3pt Float32 DEFAULT 0, away_pct_pts_3pt Float32 DEFAULT 0,
    home_pct_pts_ft  Float32 DEFAULT 0, away_pct_pts_ft Float32 DEFAULT 0,

    -- === ТАЙМАУТЫ ===
    home_timeouts   UInt8 DEFAULT 0,  away_timeouts UInt8 DEFAULT 0,
    home_in_bonus   UInt8 DEFAULT 0,  away_in_bonus UInt8 DEFAULT 0,  -- In bonus (4th quarter)

    -- === КОЭФФИЦИЕНТЫ ===
    b365_home       Float32 DEFAULT 0,  b365_away Float32 DEFAULT 0,
    b365_ou_line    Float32 DEFAULT 0,  b365_over Float32 DEFAULT 0,  b365_under Float32 DEFAULT 0,
    pinnacle_home   Float32 DEFAULT 0,  pinnacle_away Float32 DEFAULT 0,
    spread_home     Float32 DEFAULT 0,  spread_away Float32 DEFAULT 0,
    spread_line     Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, league, home_team)
SETTINGS index_granularity = 8192;

-- Статистика игроков за матч (NBA player box score)
CREATE TABLE IF NOT EXISTS betquant.basketball_player_stats (
    match_id        String,
    date            Date,
    season          LowCardinality(String),
    league          LowCardinality(String),
    team            LowCardinality(String),
    opponent        LowCardinality(String),
    is_home         UInt8 DEFAULT 1,
    player_id       String DEFAULT '',
    player_name     LowCardinality(String),
    position        LowCardinality(String) DEFAULT '',
    jersey_num      UInt8 DEFAULT 0,
    starter         UInt8 DEFAULT 0,
    dnp             UInt8 DEFAULT 0,  -- Did Not Play
    min_played      Float32 DEFAULT 0,

    -- Основная статистика
    pts             UInt8 DEFAULT 0,
    fgm             UInt8 DEFAULT 0,  fga UInt8 DEFAULT 0,  fg_pct Float32 DEFAULT 0,
    fg3m            UInt8 DEFAULT 0,  fg3a UInt8 DEFAULT 0, fg3_pct Float32 DEFAULT 0,
    ftm             UInt8 DEFAULT 0,  fta UInt8 DEFAULT 0,  ft_pct Float32 DEFAULT 0,
    oreb            UInt8 DEFAULT 0,
    dreb            UInt8 DEFAULT 0,
    reb             UInt8 DEFAULT 0,
    ast             UInt8 DEFAULT 0,
    stl             UInt8 DEFAULT 0,
    blk             UInt8 DEFAULT 0,
    tov             UInt8 DEFAULT 0,
    pf              UInt8 DEFAULT 0,
    pfd             UInt8 DEFAULT 0,   -- Fouls drawn
    plus_minus      Int8  DEFAULT 0,

    -- Очки по четвертям
    pts_q1          UInt8 DEFAULT 0,  pts_q2 UInt8 DEFAULT 0,
    pts_q3          UInt8 DEFAULT 0,  pts_q4 UInt8 DEFAULT 0,
    pts_ot          UInt8 DEFAULT 0,

    -- Продвинутые
    efg_pct         Float32 DEFAULT 0,
    ts_pct          Float32 DEFAULT 0,
    usage_pct       Float32 DEFAULT 0,
    ortg            Float32 DEFAULT 0,
    drtg            Float32 DEFAULT 0,
    ast_pct         Float32 DEFAULT 0,
    reb_pct         Float32 DEFAULT 0,
    stl_pct         Float32 DEFAULT 0,
    blk_pct         Float32 DEFAULT 0,
    tov_pct         Float32 DEFAULT 0,
    pts_paint       UInt8 DEFAULT 0,
    pts_fb          UInt8 DEFAULT 0,
    pts_2nd_chance  UInt8 DEFAULT 0,
    pts_off_tov     UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, team, player_name)
SETTINGS index_granularity = 8192;

-- Статистика по четвертям на уровне команды
CREATE TABLE IF NOT EXISTS betquant.basketball_quarter_stats (
    match_id        String,
    date            Date,
    season          LowCardinality(String),
    league          LowCardinality(String),
    team            LowCardinality(String),
    opponent        LowCardinality(String),
    is_home         UInt8,
    quarter         UInt8,   -- 1-4, 5=OT1, 6=OT2
    pts             UInt8 DEFAULT 0,
    fgm             UInt8 DEFAULT 0,  fga UInt8 DEFAULT 0,
    fg3m            UInt8 DEFAULT 0,  fg3a UInt8 DEFAULT 0,
    ftm             UInt8 DEFAULT 0,  fta UInt8 DEFAULT 0,
    oreb            UInt8 DEFAULT 0,  dreb UInt8 DEFAULT 0,  reb UInt8 DEFAULT 0,
    ast             UInt8 DEFAULT 0,
    stl             UInt8 DEFAULT 0,  blk UInt8 DEFAULT 0,
    tov             UInt8 DEFAULT 0,  pf UInt8 DEFAULT 0,
    pts_paint       UInt8 DEFAULT 0,  pts_fb UInt8 DEFAULT 0,
    lead_at_end     Int16 DEFAULT 0   -- счёт (home - away) в конце четверти
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, team, quarter)
SETTINGS index_granularity = 8192;

-- Play-by-play события (каждое владение/действие)
CREATE TABLE IF NOT EXISTS betquant.basketball_pbp (
    match_id        String,
    date            Date,
    league          LowCardinality(String),
    event_num       UInt32,
    period          UInt8,
    clock           String DEFAULT '',   -- MM:SS
    seconds_elapsed UInt16 DEFAULT 0,
    event_type      LowCardinality(String),  -- shot/rebound/turnover/foul/free_throw/sub/timeout/jump_ball
    event_detail    LowCardinality(String),  -- made/missed/offensive/defensive/steal/lost_ball...
    player1_id      String DEFAULT '',
    player1_name    LowCardinality(String) DEFAULT '',
    player1_team    LowCardinality(String) DEFAULT '',
    player2_id      String DEFAULT '',   -- assisting/fouling/blocking player
    player2_name    LowCardinality(String) DEFAULT '',
    player2_team    LowCardinality(String) DEFAULT '',
    player3_id      String DEFAULT '',
    player3_name    LowCardinality(String) DEFAULT '',
    location_x      Float32 DEFAULT 0,
    location_y      Float32 DEFAULT 0,
    shot_zone       LowCardinality(String) DEFAULT '',   -- Paint/Midrange/3PT/Corner3
    shot_distance   Float32 DEFAULT 0,
    shot_type       LowCardinality(String) DEFAULT '',   -- Jump/Layup/Dunk/Hook
    shot_result     UInt8 DEFAULT 0,
    shot_value      UInt8 DEFAULT 0,    -- 2 or 3
    home_score      UInt16 DEFAULT 0,
    away_score      UInt16 DEFAULT 0,
    score_margin    Int16 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, period, seconds_elapsed)
SETTINGS index_granularity = 8192;

-- ═══════════════════════════════════════════════════════════════════════════════
--  2. CRICKET
--  Источник: cricsheet.org — 21,000+ матчей, ball-by-ball, бесплатно
--  Форматы: Test, ODI, T20I, IPL, BBL, PSL, CPL, The Hundred...
-- ═══════════════════════════════════════════════════════════════════════════════

-- Матч (meta)
CREATE TABLE IF NOT EXISTS betquant.cricket_matches (
    match_id        String,
    source          LowCardinality(String) DEFAULT 'cricsheet',
    date            Date,
    season          LowCardinality(String),
    match_type      LowCardinality(String),   -- Test/ODI/T20/T20I/IT20
    competition     LowCardinality(String),   -- ipl/bbl/psl/wc/tests/odis/t20s
    gender          LowCardinality(String),   -- male/female
    venue           LowCardinality(String),
    city            LowCardinality(String),
    country         LowCardinality(String),

    team1           LowCardinality(String),
    team2           LowCardinality(String),
    toss_winner     LowCardinality(String),
    toss_decision   LowCardinality(String),  -- bat/field
    winner          LowCardinality(String),
    result          LowCardinality(String),  -- normal/tie/no result/draw
    win_by_runs     UInt16 DEFAULT 0,
    win_by_wickets  UInt8 DEFAULT 0,
    result_margin   String DEFAULT '',       -- "5 wickets" / "34 runs"

    -- Innings totals
    inning1_runs    UInt16 DEFAULT 0,
    inning1_wickets UInt8 DEFAULT 0,
    inning1_overs   Float32 DEFAULT 0,
    inning2_runs    UInt16 DEFAULT 0,
    inning2_wickets UInt8 DEFAULT 0,
    inning2_overs   Float32 DEFAULT 0,
    inning3_runs    UInt16 DEFAULT 0,  -- Test: 3rd innings
    inning3_wickets UInt8 DEFAULT 0,
    inning4_runs    UInt16 DEFAULT 0,  -- Test: 4th innings
    inning4_wickets UInt8 DEFAULT 0,

    -- Ключевые показатели
    total_runs      UInt16 DEFAULT 0,
    total_wickets   UInt8 DEFAULT 0,
    total_balls     UInt16 DEFAULT 0,
    total_extras    UInt16 DEFAULT 0,   -- wides + no-balls + byes + leg-byes

    -- DLS (Duckworth-Lewis) если применялся
    dls_applied     UInt8 DEFAULT 0,
    dls_target      UInt16 DEFAULT 0,

    player_of_match LowCardinality(String) DEFAULT '',
    umpire1         LowCardinality(String) DEFAULT '',
    umpire2         LowCardinality(String) DEFAULT '',
    tv_umpire       LowCardinality(String) DEFAULT '',
    match_referee   LowCardinality(String) DEFAULT '',
    days_of_play    UInt8 DEFAULT 1,  -- для Test матчей

    -- Коэффициенты
    b365_team1      Float32 DEFAULT 0,
    b365_team2      Float32 DEFAULT 0,
    b365_draw       Float32 DEFAULT 0,
    avg_team1       Float32 DEFAULT 0,
    avg_team2       Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (date, competition, team1)
SETTINGS index_granularity = 8192;

-- Ball-by-ball (каждая подача)
CREATE TABLE IF NOT EXISTS betquant.cricket_deliveries (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    match_type      LowCardinality(String),
    gender          LowCardinality(String),
    innings         UInt8,              -- 1-4
    batting_team    LowCardinality(String),
    bowling_team    LowCardinality(String),
    over_num        UInt8,              -- 0-based
    ball_num        UInt8,              -- within over (0-5, + extras)
    over_ball        String DEFAULT '', -- "2.3" = over 2, ball 3

    -- Подающий и отбивающий
    batter          LowCardinality(String),
    non_striker     LowCardinality(String),
    bowler          LowCardinality(String),

    -- Результат подачи
    runs_batter     UInt8 DEFAULT 0,    -- runs scored by batter
    runs_extras     UInt8 DEFAULT 0,    -- extra runs (wides/no-balls etc)
    runs_total      UInt8 DEFAULT 0,    -- total runs off ball
    extras_type     LowCardinality(String) DEFAULT '',  -- wide/noball/bye/legbye/penalty
    extras_wide     UInt8 DEFAULT 0,
    extras_noball   UInt8 DEFAULT 0,
    extras_bye      UInt8 DEFAULT 0,
    extras_legbye   UInt8 DEFAULT 0,
    extras_penalty  UInt8 DEFAULT 0,
    is_wide         UInt8 DEFAULT 0,
    is_noball       UInt8 DEFAULT 0,
    boundary_4      UInt8 DEFAULT 0,    -- 4 runs (boundary)
    boundary_6      UInt8 DEFAULT 0,    -- 6 runs (six)

    -- Выбывание (wicket)
    wicket_fallen   UInt8 DEFAULT 0,
    wicket_type     LowCardinality(String) DEFAULT '',  -- caught/bowled/lbw/run_out/stumped/hit_wicket/retired_hurt
    wicket_player   LowCardinality(String) DEFAULT '',  -- dismissed player
    wicket_fielder  LowCardinality(String) DEFAULT '',  -- catching/fielding player
    wicket_bowler   LowCardinality(String) DEFAULT '',  -- credited bowler (may differ)

    -- Состояние на момент подачи
    innings_runs    UInt16 DEFAULT 0,   -- runs in innings before this ball
    innings_wickets UInt8  DEFAULT 0,   -- wickets fallen before this ball
    batter_runs     UInt16 DEFAULT 0,   -- current batter's score before ball
    batter_balls    UInt16 DEFAULT 0    -- current batter's balls faced before ball
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (match_id, innings, over_num, ball_num)
SETTINGS index_granularity = 8192;

-- Batting scorecard (per innings per player)
CREATE TABLE IF NOT EXISTS betquant.cricket_batting (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    match_type      LowCardinality(String),
    gender          LowCardinality(String),
    innings         UInt8,
    batting_team    LowCardinality(String),
    bowling_team    LowCardinality(String),
    bat_position    UInt8 DEFAULT 0,   -- batting order 1-11
    batter          LowCardinality(String),
    player_id       String DEFAULT '',

    -- Batting stats
    runs            UInt16 DEFAULT 0,
    balls_faced     UInt16 DEFAULT 0,
    fours           UInt8 DEFAULT 0,
    sixes           UInt8 DEFAULT 0,
    strike_rate     Float32 DEFAULT 0,
    minutes_batted  UInt16 DEFAULT 0,

    -- Dismissal
    not_out         UInt8 DEFAULT 1,
    dismissed_by    LowCardinality(String) DEFAULT '',
    dismissal_kind  LowCardinality(String) DEFAULT '',
    fielder         LowCardinality(String) DEFAULT '',

    -- Phase stats (powerplay / middle / death)
    runs_pp         UInt8 DEFAULT 0,   -- runs in powerplay overs
    runs_middle     UInt8 DEFAULT 0,   -- overs 7-15 (T20) or 11-40 (ODI)
    runs_death      UInt8 DEFAULT 0,   -- overs 16-20 (T20) or 41-50 (ODI)
    balls_pp        UInt8 DEFAULT 0,
    balls_middle    UInt8 DEFAULT 0,
    balls_death     UInt8 DEFAULT 0,
    boundary_pct    Float32 DEFAULT 0  -- % of runs from boundaries
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (match_id, innings, bat_position)
SETTINGS index_granularity = 8192;

-- Bowling scorecard
CREATE TABLE IF NOT EXISTS betquant.cricket_bowling (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    match_type      LowCardinality(String),
    gender          LowCardinality(String),
    innings         UInt8,
    bowling_team    LowCardinality(String),
    batting_team    LowCardinality(String),
    bowler          LowCardinality(String),
    player_id       String DEFAULT '',

    -- Bowling stats
    overs           Float32 DEFAULT 0,
    maidens         UInt8  DEFAULT 0,
    runs_conceded   UInt16 DEFAULT 0,
    wickets         UInt8  DEFAULT 0,
    wides           UInt8  DEFAULT 0,
    no_balls        UInt8  DEFAULT 0,
    economy         Float32 DEFAULT 0,  -- runs per over
    bowling_avg     Float32 DEFAULT 0,  -- runs per wicket
    strike_rate     Float32 DEFAULT 0,  -- balls per wicket
    dot_pct         Float32 DEFAULT 0,  -- % of dot balls

    -- Phase breakdown
    runs_pp         UInt8 DEFAULT 0,  wickets_pp UInt8 DEFAULT 0,
    overs_pp        Float32 DEFAULT 0,
    runs_middle     UInt8 DEFAULT 0,  wickets_middle UInt8 DEFAULT 0,
    runs_death      UInt8 DEFAULT 0,  wickets_death UInt8 DEFAULT 0,

    -- Dismissal types
    wickets_bowled  UInt8 DEFAULT 0,
    wickets_caught  UInt8 DEFAULT 0,
    wickets_lbw     UInt8 DEFAULT 0,
    wickets_stumped UInt8 DEFAULT 0,
    wickets_run_out UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (match_id, innings, bowler)
SETTINGS index_granularity = 8192;

-- Fielding stats per match
CREATE TABLE IF NOT EXISTS betquant.cricket_fielding (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    match_type      LowCardinality(String),
    team            LowCardinality(String),
    player          LowCardinality(String),
    player_id       String DEFAULT '',
    is_keeper       UInt8 DEFAULT 0,
    catches         UInt8 DEFAULT 0,
    stumpings       UInt8 DEFAULT 0,
    run_outs        UInt8 DEFAULT 0,
    dropped_catches UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (match_id, team, player)
SETTINGS index_granularity = 8192;

-- ═══════════════════════════════════════════════════════════════════════════════
--  3. RUGBY UNION
--  Источник: ESPN Scrum scraping, rugbypy (Python), open datasets
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS betquant.rugby_matches (
    match_id        String,
    source          LowCardinality(String),
    date            Date,
    datetime        DateTime DEFAULT toDateTime(date),
    season          LowCardinality(String),
    competition     LowCardinality(String),   -- Six Nations/Rugby WC/Premiership/URC/Top14/Super Rugby
    round           LowCardinality(String),
    home_team       LowCardinality(String),
    away_team       LowCardinality(String),
    venue           LowCardinality(String) DEFAULT '',
    attendance      UInt32 DEFAULT 0,

    -- Итог
    home_score      UInt16 DEFAULT 0,
    away_score      UInt16 DEFAULT 0,
    result          FixedString(1) DEFAULT 'H',  -- H/A/D

    -- Счёт по половинам
    home_h1         UInt8 DEFAULT 0,  away_h1 UInt8 DEFAULT 0,
    home_h2         UInt8 DEFAULT 0,  away_h2 UInt8 DEFAULT 0,
    went_to_et      UInt8 DEFAULT 0,  -- Extra Time

    -- === ОЧКИ ПО ТИПУ ===
    home_tries          UInt8 DEFAULT 0,  away_tries UInt8 DEFAULT 0,          -- Попытки (5 pts)
    home_conversions    UInt8 DEFAULT 0,  away_conversions UInt8 DEFAULT 0,    -- Реализации (2 pts)
    home_penalties_kick UInt8 DEFAULT 0,  away_penalties_kick UInt8 DEFAULT 0, -- Штрафные удары (3 pts)
    home_drop_goals     UInt8 DEFAULT 0,  away_drop_goals UInt8 DEFAULT 0,     -- Дроп-голы (3 pts)
    home_penalty_tries  UInt8 DEFAULT 0,  away_penalty_tries UInt8 DEFAULT 0,  -- Штрафные попытки (7 pts)

    -- === АТАКА ===
    home_possession_pct Float32 DEFAULT 0, away_possession_pct Float32 DEFAULT 0,  -- Владение мячом %
    home_territory_pct  Float32 DEFAULT 0, away_territory_pct  Float32 DEFAULT 0,  -- Территория %
    home_meters_carried UInt16 DEFAULT 0,  away_meters_carried UInt16 DEFAULT 0,   -- Метры с мячом
    home_carries        UInt16 DEFAULT 0,  away_carries UInt16 DEFAULT 0,
    home_defenders_beaten UInt8 DEFAULT 0, away_defenders_beaten UInt8 DEFAULT 0,
    home_clean_breaks   UInt8 DEFAULT 0,   away_clean_breaks UInt8 DEFAULT 0,
    home_offloads       UInt8 DEFAULT 0,   away_offloads UInt8 DEFAULT 0,
    home_line_breaks    UInt8 DEFAULT 0,   away_line_breaks UInt8 DEFAULT 0,

    -- === ПЕРЕДАЧИ ===
    home_passes         UInt16 DEFAULT 0,  away_passes UInt16 DEFAULT 0,
    home_passes_backward UInt8 DEFAULT 0,  away_passes_backward UInt8 DEFAULT 0,
    home_handling_errors UInt8 DEFAULT 0,  away_handling_errors UInt8 DEFAULT 0,
    home_knocks_on      UInt8 DEFAULT 0,   away_knocks_on UInt8 DEFAULT 0,

    -- === ЗАЩИТА / СХВАТКА ===
    home_tackles        UInt16 DEFAULT 0,  away_tackles UInt16 DEFAULT 0,
    home_tackles_made   UInt16 DEFAULT 0,  away_tackles_made UInt16 DEFAULT 0,
    home_tackles_missed UInt8 DEFAULT 0,   away_tackles_missed UInt8 DEFAULT 0,
    home_tackle_pct     Float32 DEFAULT 0, away_tackle_pct Float32 DEFAULT 0,

    -- === СХВАТКИ (Scrums) ===
    home_scrums_total   UInt8 DEFAULT 0,   away_scrums_total UInt8 DEFAULT 0,
    home_scrums_won     UInt8 DEFAULT 0,   away_scrums_won UInt8 DEFAULT 0,
    home_scrums_won_pct Float32 DEFAULT 0, away_scrums_won_pct Float32 DEFAULT 0,
    home_scrums_pen_conceded UInt8 DEFAULT 0, away_scrums_pen_conceded UInt8 DEFAULT 0,

    -- === ВЫХОДЫ В АУТ (Lineouts) ===
    home_lineouts_total  UInt8 DEFAULT 0,  away_lineouts_total UInt8 DEFAULT 0,
    home_lineouts_won    UInt8 DEFAULT 0,  away_lineouts_won UInt8 DEFAULT 0,
    home_lineouts_stolen UInt8 DEFAULT 0,  away_lineouts_stolen UInt8 DEFAULT 0,
    home_lineouts_won_pct Float32 DEFAULT 0, away_lineouts_won_pct Float32 DEFAULT 0,

    -- === МОЛИ (Mauls) ===
    home_mauls          UInt8 DEFAULT 0,  away_mauls UInt8 DEFAULT 0,
    home_mauls_won      UInt8 DEFAULT 0,  away_mauls_won UInt8 DEFAULT 0,
    home_ruck_success_pct Float32 DEFAULT 0, away_ruck_success_pct Float32 DEFAULT 0,

    -- === УДАРЫ (Kicks) ===
    home_kicks_total    UInt8 DEFAULT 0,   away_kicks_total UInt8 DEFAULT 0,
    home_kicks_in_play  UInt8 DEFAULT 0,   away_kicks_in_play UInt8 DEFAULT 0,
    home_22m_entries    UInt8 DEFAULT 0,   away_22m_entries UInt8 DEFAULT 0,
    home_22m_pct        Float32 DEFAULT 0, away_22m_pct Float32 DEFAULT 0,

    -- === НАРУШЕНИЯ ===
    home_penalties_conceded UInt8 DEFAULT 0, away_penalties_conceded UInt8 DEFAULT 0,
    home_yellow_cards    UInt8 DEFAULT 0,  away_yellow_cards UInt8 DEFAULT 0,
    home_red_cards       UInt8 DEFAULT 0,  away_red_cards UInt8 DEFAULT 0,
    home_free_kicks      UInt8 DEFAULT 0,  away_free_kicks UInt8 DEFAULT 0,
    home_turnovers_conceded UInt8 DEFAULT 0, away_turnovers_conceded UInt8 DEFAULT 0,

    -- Коэффициенты
    b365_home    Float32 DEFAULT 0,  b365_draw Float32 DEFAULT 0,  b365_away Float32 DEFAULT 0,
    b365_hcap    Float32 DEFAULT 0,  b365_hcap_line Float32 DEFAULT 0,  -- Asian Handicap
    b365_over    Float32 DEFAULT 0,  b365_under Float32 DEFAULT 0,  b365_ou_line Float32 DEFAULT 0,
    pinnacle_home Float32 DEFAULT 0, pinnacle_draw Float32 DEFAULT 0, pinnacle_away Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, competition, home_team)
SETTINGS index_granularity = 8192;

-- Игровые события (очки) — попытки, пенальти, дроп-голы с минутами
CREATE TABLE IF NOT EXISTS betquant.rugby_events (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    minute          UInt8,
    is_extra_time   UInt8 DEFAULT 0,
    event_type      LowCardinality(String),  -- try/conversion/penalty/drop_goal/penalty_try/yellow_card/red_card/sub
    team            LowCardinality(String),
    player          LowCardinality(String) DEFAULT '',
    assist_player   LowCardinality(String) DEFAULT '',
    points_value    UInt8 DEFAULT 0,    -- 5/2/3/7
    home_score      UInt16 DEFAULT 0,   -- cumulative after event
    away_score      UInt16 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, minute, event_type)
SETTINGS index_granularity = 8192;

-- Player stats per match
CREATE TABLE IF NOT EXISTS betquant.rugby_player_stats (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    team            LowCardinality(String),
    opponent        LowCardinality(String),
    is_home         UInt8,
    player          LowCardinality(String),
    player_id       String DEFAULT '',
    position        LowCardinality(String) DEFAULT '',  -- LHP/RHP/Flanker/Fly-Half etc
    number          UInt8 DEFAULT 0,
    started         UInt8 DEFAULT 0,
    minutes_played  UInt8 DEFAULT 80,
    tries           UInt8 DEFAULT 0,
    try_assists     UInt8 DEFAULT 0,
    conversions     UInt8 DEFAULT 0,
    penalties_kick  UInt8 DEFAULT 0,
    drop_goals      UInt8 DEFAULT 0,
    points          UInt8 DEFAULT 0,
    meters_run      UInt16 DEFAULT 0,
    carries         UInt8 DEFAULT 0,
    clean_breaks    UInt8 DEFAULT 0,
    defenders_beaten UInt8 DEFAULT 0,
    offloads        UInt8 DEFAULT 0,
    tackles_made    UInt8 DEFAULT 0,
    tackles_missed  UInt8 DEFAULT 0,
    lineouts_won    UInt8 DEFAULT 0,
    lineouts_lost   UInt8 DEFAULT 0,
    yellow_card     UInt8 DEFAULT 0,
    red_card        UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, team, player)
SETTINGS index_granularity = 8192;

-- ═══════════════════════════════════════════════════════════════════════════════
--  4. AMERICAN FOOTBALL (NFL)
--  Источник: nflverse / nfl_data_py — PbP back to 1999, 372 поля за игровое действие
-- ═══════════════════════════════════════════════════════════════════════════════

-- Game-level stats (агрегат из PbP)
CREATE TABLE IF NOT EXISTS betquant.nfl_games (
    game_id         String,
    source          LowCardinality(String) DEFAULT 'nflverse',
    season          UInt16,
    season_type     LowCardinality(String) DEFAULT 'REG',  -- REG/POST/PRE
    week            UInt8,
    date            Date,
    home_team       LowCardinality(String),
    away_team       LowCardinality(String),
    venue           LowCardinality(String) DEFAULT '',
    roof            LowCardinality(String) DEFAULT '',   -- dome/outdoors/closed/open
    surface         LowCardinality(String) DEFAULT '',
    temp            Int8 DEFAULT -99,    -- Fahrenheit, -99 = indoor/unknown
    wind            UInt8 DEFAULT 0,
    div_game        UInt8 DEFAULT 0,     -- Division game flag

    -- Итог
    home_score      UInt8 DEFAULT 0,
    away_score      UInt8 DEFAULT 0,
    result          FixedString(1) DEFAULT 'H',  -- H/A/T (tie)

    -- Очки по периодам (Q1-Q4 + OT)
    home_q1         UInt8 DEFAULT 0,  away_q1 UInt8 DEFAULT 0,
    home_q2         UInt8 DEFAULT 0,  away_q2 UInt8 DEFAULT 0,
    home_q3         UInt8 DEFAULT 0,  away_q3 UInt8 DEFAULT 0,
    home_q4         UInt8 DEFAULT 0,  away_q4 UInt8 DEFAULT 0,
    home_ot         UInt8 DEFAULT 0,  away_ot UInt8 DEFAULT 0,
    went_to_ot      UInt8 DEFAULT 0,

    -- Первая половина
    home_h1         UInt8 DEFAULT 0,  away_h1 UInt8 DEFAULT 0,
    home_h2         UInt8 DEFAULT 0,  away_h2 UInt8 DEFAULT 0,

    -- === PASSING (команда) ===
    home_pass_att   UInt16 DEFAULT 0,  away_pass_att UInt16 DEFAULT 0,   -- Pass Attempts
    home_pass_cmp   UInt16 DEFAULT 0,  away_pass_cmp UInt16 DEFAULT 0,   -- Completions
    home_pass_cmp_pct Float32 DEFAULT 0, away_pass_cmp_pct Float32 DEFAULT 0,
    home_pass_yds   UInt16 DEFAULT 0,  away_pass_yds UInt16 DEFAULT 0,
    home_pass_tds   UInt8  DEFAULT 0,  away_pass_tds UInt8  DEFAULT 0,
    home_pass_int   UInt8  DEFAULT 0,  away_pass_int UInt8  DEFAULT 0,
    home_pass_sacks UInt8  DEFAULT 0,  away_pass_sacks UInt8 DEFAULT 0,
    home_pass_sack_yds UInt8 DEFAULT 0, away_pass_sack_yds UInt8 DEFAULT 0,
    home_air_yds    UInt16 DEFAULT 0,  away_air_yds UInt16 DEFAULT 0,    -- Air Yards (EPA model)
    home_yac        UInt16 DEFAULT 0,  away_yac UInt16 DEFAULT 0,        -- Yards After Catch
    home_qb_rating  Float32 DEFAULT 0, away_qb_rating Float32 DEFAULT 0,
    home_cpoe       Float32 DEFAULT 0, away_cpoe Float32 DEFAULT 0,      -- Completion% over Expected

    -- === RUSHING (бег) ===
    home_rush_att   UInt16 DEFAULT 0,  away_rush_att UInt16 DEFAULT 0,
    home_rush_yds   UInt16 DEFAULT 0,  away_rush_yds UInt16 DEFAULT 0,
    home_rush_tds   UInt8  DEFAULT 0,  away_rush_tds UInt8  DEFAULT 0,
    home_rush_ypa   Float32 DEFAULT 0, away_rush_ypa Float32 DEFAULT 0,  -- Yards per Attempt
    home_rush_broken_tackles UInt8 DEFAULT 0, away_rush_broken_tackles UInt8 DEFAULT 0,

    -- === RECEIVING ===
    home_rec        UInt16 DEFAULT 0,  away_rec UInt16 DEFAULT 0,
    home_rec_yds    UInt16 DEFAULT 0,  away_rec_yds UInt16 DEFAULT 0,
    home_rec_tds    UInt8  DEFAULT 0,  away_rec_tds UInt8 DEFAULT 0,

    -- === TOTAL OFFENSE ===
    home_total_yds  UInt16 DEFAULT 0,  away_total_yds UInt16 DEFAULT 0,
    home_plays      UInt16 DEFAULT 0,  away_plays UInt16 DEFAULT 0,
    home_ypp        Float32 DEFAULT 0, away_ypp Float32 DEFAULT 0,       -- Yards per Play
    home_first_downs UInt8 DEFAULT 0,  away_first_downs UInt8 DEFAULT 0,
    home_third_att   UInt8 DEFAULT 0,  away_third_att UInt8 DEFAULT 0,
    home_third_cmp   UInt8 DEFAULT 0,  away_third_cmp UInt8 DEFAULT 0,
    home_third_pct   Float32 DEFAULT 0, away_third_pct Float32 DEFAULT 0,
    home_fourth_att  UInt8 DEFAULT 0,  away_fourth_att UInt8 DEFAULT 0,
    home_fourth_cmp  UInt8 DEFAULT 0,  away_fourth_cmp UInt8 DEFAULT 0,
    home_redzone_att UInt8 DEFAULT 0,  away_redzone_att UInt8 DEFAULT 0,
    home_redzone_td  UInt8 DEFAULT 0,  away_redzone_td UInt8 DEFAULT 0,
    home_possession  Float32 DEFAULT 0, away_possession Float32 DEFAULT 0, -- Time of Possession (%)

    -- === SPECIAL TEAMS ===
    home_punts       UInt8 DEFAULT 0,  away_punts UInt8 DEFAULT 0,
    home_punt_avg    Float32 DEFAULT 0, away_punt_avg Float32 DEFAULT 0,
    home_fg_att      UInt8 DEFAULT 0,  away_fg_att UInt8 DEFAULT 0,      -- Field Goal Attempts
    home_fg_made     UInt8 DEFAULT 0,  away_fg_made UInt8 DEFAULT 0,
    home_fg_pct      Float32 DEFAULT 0, away_fg_pct Float32 DEFAULT 0,
    home_kickoff_ret_avg Float32 DEFAULT 0, away_kickoff_ret_avg Float32 DEFAULT 0,

    -- === TURNOVERS ===
    home_fumbles     UInt8 DEFAULT 0,  away_fumbles UInt8 DEFAULT 0,
    home_fumbles_lost UInt8 DEFAULT 0, away_fumbles_lost UInt8 DEFAULT 0,
    home_interceptions UInt8 DEFAULT 0, away_interceptions UInt8 DEFAULT 0,
    home_turnovers   UInt8 DEFAULT 0,  away_turnovers UInt8 DEFAULT 0,
    turnover_diff    Int8  DEFAULT 0,

    -- === PENALTIES ===
    home_penalties   UInt8 DEFAULT 0,  away_penalties UInt8 DEFAULT 0,
    home_penalty_yds UInt8 DEFAULT 0,  away_penalty_yds UInt8 DEFAULT 0,

    -- === EPA / WP (продвинутые модели nflfastR) ===
    home_epa_total   Float32 DEFAULT 0,  away_epa_total Float32 DEFAULT 0,
    home_epa_per_play Float32 DEFAULT 0, away_epa_per_play Float32 DEFAULT 0,
    home_epa_pass    Float32 DEFAULT 0,  away_epa_pass Float32 DEFAULT 0,
    home_epa_rush    Float32 DEFAULT 0,  away_epa_rush Float32 DEFAULT 0,
    home_success_rate Float32 DEFAULT 0, away_success_rate Float32 DEFAULT 0, -- % positive EPA plays
    home_wp_pregame  Float32 DEFAULT 0,  away_wp_pregame Float32 DEFAULT 0,

    -- Линии коэффициентов
    spread           Float32 DEFAULT 0,  -- closing spread (+ = away favored)
    total_line       Float32 DEFAULT 0,
    b365_home        Float32 DEFAULT 0,  b365_away Float32 DEFAULT 0,
    b365_over        Float32 DEFAULT 0,  b365_under Float32 DEFAULT 0,
    pinnacle_home    Float32 DEFAULT 0,  pinnacle_away Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (date, season, home_team)
SETTINGS index_granularity = 8192;

-- Play-by-Play (каждое игровое действие, 372 поля в nflverse)
CREATE TABLE IF NOT EXISTS betquant.nfl_pbp (
    play_id         UInt32,
    game_id         String,
    date            Date,
    season          UInt16,
    season_type     LowCardinality(String),
    week            UInt8,
    home_team       LowCardinality(String),
    away_team       LowCardinality(String),
    posteam         LowCardinality(String),   -- possession team
    defteam         LowCardinality(String),   -- defensive team
    side_of_field   LowCardinality(String),

    -- Ситуация на поле
    yardline_100    UInt8 DEFAULT 50,    -- yards from opponent end zone
    game_date       String DEFAULT '',
    quarter_seconds_remaining UInt16 DEFAULT 0,
    half_seconds_remaining    UInt16 DEFAULT 0,
    game_seconds_remaining    UInt16 DEFAULT 0,
    game_half       LowCardinality(String) DEFAULT '',  -- Half1/Half2/Overtime
    qtr             UInt8 DEFAULT 1,
    down            UInt8 DEFAULT 0,
    goal_to_go      UInt8 DEFAULT 0,
    ydstogo         UInt8 DEFAULT 0,
    ydsnet          Int16 DEFAULT 0,

    -- Результат игрового действия
    play_type       LowCardinality(String) DEFAULT '',  -- pass/run/punt/field_goal/kickoff/extra_point/no_play
    yards_gained    Int8 DEFAULT 0,
    touchdown       UInt8 DEFAULT 0,
    fumble          UInt8 DEFAULT 0,
    fumble_lost     UInt8 DEFAULT 0,
    interception    UInt8 DEFAULT 0,
    sack            UInt8 DEFAULT 0,
    complete_pass   UInt8 DEFAULT 0,
    incomplete_pass UInt8 DEFAULT 0,
    pass_touchdown  UInt8 DEFAULT 0,
    rush_touchdown  UInt8 DEFAULT 0,
    return_touchdown UInt8 DEFAULT 0,
    field_goal_attempt UInt8 DEFAULT 0,
    field_goal_result  LowCardinality(String) DEFAULT '',  -- made/missed/blocked
    kick_distance   UInt8 DEFAULT 0,
    extra_point_attempt UInt8 DEFAULT 0,
    extra_point_result  LowCardinality(String) DEFAULT '',
    two_point_attempt   UInt8 DEFAULT 0,
    two_point_conv_result LowCardinality(String) DEFAULT '',
    penalty         UInt8 DEFAULT 0,
    penalty_type    LowCardinality(String) DEFAULT '',
    penalty_yards   Int8 DEFAULT 0,
    first_down      UInt8 DEFAULT 0,
    third_down_converted UInt8 DEFAULT 0,
    third_down_failed    UInt8 DEFAULT 0,
    fourth_down_converted UInt8 DEFAULT 0,
    fourth_down_failed    UInt8 DEFAULT 0,

    -- Игроки
    passer_id       String DEFAULT '',
    passer          LowCardinality(String) DEFAULT '',
    rusher_id       String DEFAULT '',
    rusher          LowCardinality(String) DEFAULT '',
    receiver_id     String DEFAULT '',
    receiver        LowCardinality(String) DEFAULT '',
    air_yards       Int8 DEFAULT 0,
    yards_after_catch Float32 DEFAULT 0,
    pass_location   LowCardinality(String) DEFAULT '',  -- left/middle/right
    pass_length     LowCardinality(String) DEFAULT '',  -- short/deep
    run_location    LowCardinality(String) DEFAULT '',
    run_gap         LowCardinality(String) DEFAULT '',

    -- Модельные переменные (nflfastR)
    ep              Float32 DEFAULT 0,    -- Expected Points before play
    epa             Float32 DEFAULT 0,    -- EPA of play
    wp              Float32 DEFAULT 0,    -- Win Probability before play
    wpa             Float32 DEFAULT 0,    -- WPA of play
    air_epa         Float32 DEFAULT 0,
    yac_epa         Float32 DEFAULT 0,
    comp_air_epa    Float32 DEFAULT 0,
    comp_yac_epa    Float32 DEFAULT 0,
    cp              Float32 DEFAULT 0,    -- Completion Probability
    cpoe            Float32 DEFAULT 0,    -- CPOE
    success         UInt8 DEFAULT 0,      -- positive EPA play?
    xpass           Float32 DEFAULT 0,    -- Expected pass rate (situational)
    pass_oe         Float32 DEFAULT 0,    -- Pass Over Expected

    -- Счёт
    posteam_score   UInt8 DEFAULT 0,
    defteam_score   UInt8 DEFAULT 0,
    score_differential Int8 DEFAULT 0,
    posteam_score_post UInt8 DEFAULT 0,
    defteam_score_post UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (game_id, play_id)
SETTINGS index_granularity = 8192;

-- Weekly player stats (QB/WR/RB/TE/K/DEF)
CREATE TABLE IF NOT EXISTS betquant.nfl_player_stats (
    player_id       String,
    player_name     LowCardinality(String),
    position        LowCardinality(String),   -- QB/RB/WR/TE/K/DEF
    team            LowCardinality(String),
    opponent        LowCardinality(String),
    season          UInt16,
    week            UInt8,
    date            Date,
    game_id         String DEFAULT '',
    is_home         UInt8 DEFAULT 1,

    -- Passing
    completions     UInt8 DEFAULT 0,  attempts UInt8 DEFAULT 0,
    passing_yards   UInt16 DEFAULT 0,
    passing_tds     UInt8 DEFAULT 0,  interceptions UInt8 DEFAULT 0,
    sacks           UInt8 DEFAULT 0,  sack_yards UInt8 DEFAULT 0,
    sack_fumbles    UInt8 DEFAULT 0,  sack_fumbles_lost UInt8 DEFAULT 0,
    passing_air_yards UInt16 DEFAULT 0,
    passing_yards_after_catch UInt16 DEFAULT 0,
    passing_first_downs UInt8 DEFAULT 0,
    passing_epa     Float32 DEFAULT 0,
    dakota          Float32 DEFAULT 0,  -- CPOE + EPA composite
    pacr            Float32 DEFAULT 0,  -- Passing Air Conversion Ratio

    -- Rushing
    carries         UInt8 DEFAULT 0,
    rushing_yards   UInt16 DEFAULT 0,
    rushing_tds     UInt8 DEFAULT 0,
    rushing_fumbles UInt8 DEFAULT 0,  rushing_fumbles_lost UInt8 DEFAULT 0,
    rushing_first_downs UInt8 DEFAULT 0,
    rushing_epa     Float32 DEFAULT 0,

    -- Receiving
    receptions      UInt8 DEFAULT 0,  targets UInt8 DEFAULT 0,
    receiving_yards UInt16 DEFAULT 0,
    receiving_tds   UInt8 DEFAULT 0,
    receiving_fumbles UInt8 DEFAULT 0, receiving_fumbles_lost UInt8 DEFAULT 0,
    receiving_air_yards UInt8 DEFAULT 0,
    receiving_yards_after_catch UInt8 DEFAULT 0,
    receiving_first_downs UInt8 DEFAULT 0,
    receiving_epa   Float32 DEFAULT 0,
    racr            Float32 DEFAULT 0,  -- Receiver Air Conversion Ratio
    target_share    Float32 DEFAULT 0,
    air_yards_share Float32 DEFAULT 0,
    wopr            Float32 DEFAULT 0,  -- Weighted Opportunity Rating

    -- Special Teams / Defense / Other
    special_teams_tds UInt8 DEFAULT 0,
    fantasy_points  Float32 DEFAULT 0,  -- Standard scoring
    fantasy_points_ppr Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (season, week, player_id)
SETTINGS index_granularity = 8192;

-- ═══════════════════════════════════════════════════════════════════════════════
--  5. WATER POLO
--  Источники: kaggle/international-water-polo, FINA/LEN public scoreboards,
--             total-waterpolo.com данные по чемпионатам
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS betquant.waterpolo_matches (
    match_id        String,
    source          LowCardinality(String),
    date            Date,
    season          LowCardinality(String),
    competition     LowCardinality(String),  -- Olympic/World Champ/LEN Champions/LEN Euro Cup/A1 League
    competition_level LowCardinality(String), -- International/Club
    gender          LowCardinality(String),  -- male/female
    round           LowCardinality(String),
    venue           LowCardinality(String) DEFAULT '',
    home_team       LowCardinality(String),
    away_team       LowCardinality(String),

    -- Итог
    home_score      UInt8 DEFAULT 0,
    away_score      UInt8 DEFAULT 0,
    result          FixedString(1) DEFAULT 'H',

    -- По четвертям (4 четверти по 8 минут)
    home_q1         UInt8 DEFAULT 0,  away_q1 UInt8 DEFAULT 0,
    home_q2         UInt8 DEFAULT 0,  away_q2 UInt8 DEFAULT 0,
    home_q3         UInt8 DEFAULT 0,  away_q3 UInt8 DEFAULT 0,
    home_q4         UInt8 DEFAULT 0,  away_q4 UInt8 DEFAULT 0,
    home_et         UInt8 DEFAULT 0,  away_et UInt8 DEFAULT 0,  -- Extra Time
    went_to_et      UInt8 DEFAULT 0,
    went_to_pen     UInt8 DEFAULT 0,  -- Penalty Shootout

    -- === АТАКА (Shots) ===
    home_shots_total    UInt8 DEFAULT 0,  away_shots_total UInt8 DEFAULT 0,
    home_goals          UInt8 DEFAULT 0,  away_goals UInt8 DEFAULT 0,
    home_shot_pct       Float32 DEFAULT 0, away_shot_pct Float32 DEFAULT 0,  -- Shot Efficiency %

    -- Голы по типу атаки
    home_action_goals       UInt8 DEFAULT 0,  away_action_goals UInt8 DEFAULT 0,       -- Равные составы
    home_powerplay_goals    UInt8 DEFAULT 0,  away_powerplay_goals UInt8 DEFAULT 0,    -- В большинстве
    home_counterattack_goals UInt8 DEFAULT 0, away_counterattack_goals UInt8 DEFAULT 0, -- Контратака
    home_penalty_goals      UInt8 DEFAULT 0,  away_penalty_goals UInt8 DEFAULT 0,      -- 5-метровый
    home_center_goals       UInt8 DEFAULT 0,  away_center_goals UInt8 DEFAULT 0,       -- Центр нападения (6м)
    home_6m_goals           UInt8 DEFAULT 0,  away_6m_goals UInt8 DEFAULT 0,
    home_outside_goals      UInt8 DEFAULT 0,  away_outside_goals UInt8 DEFAULT 0,      -- Дальние броски

    -- Броски (не голы)
    home_shots_missed       UInt8 DEFAULT 0,  away_shots_missed UInt8 DEFAULT 0,       -- мимо ворот
    home_shots_blocked      UInt8 DEFAULT 0,  away_shots_blocked UInt8 DEFAULT 0,      -- заблокированы полевым
    home_shots_post         UInt8 DEFAULT 0,  away_shots_post UInt8 DEFAULT 0,         -- в штангу

    -- Powerplay (большинство / меньшинство)
    home_powerplay_att      UInt8 DEFAULT 0,  away_powerplay_att UInt8 DEFAULT 0,
    home_powerplay_pct      Float32 DEFAULT 0, away_powerplay_pct Float32 DEFAULT 0,
    home_powerplay_against_att UInt8 DEFAULT 0, away_powerplay_against_att UInt8 DEFAULT 0,
    home_pk_goals_allowed   UInt8 DEFAULT 0,   away_pk_goals_allowed UInt8 DEFAULT 0,

    -- Penalty (5-метровый)
    home_penalty_att        UInt8 DEFAULT 0,  away_penalty_att UInt8 DEFAULT 0,
    home_penalty_saved      UInt8 DEFAULT 0,  away_penalty_saved UInt8 DEFAULT 0,
    home_penalty_pct        Float32 DEFAULT 0, away_penalty_pct Float32 DEFAULT 0,

    -- Counterattack
    home_counterattack_att  UInt8 DEFAULT 0,  away_counterattack_att UInt8 DEFAULT 0,
    home_counterattack_pct  Float32 DEFAULT 0, away_counterattack_pct Float32 DEFAULT 0,

    -- === ВРАТАРЬ ===
    home_saves              UInt8 DEFAULT 0,  away_saves UInt8 DEFAULT 0,
    home_save_pct           Float32 DEFAULT 0, away_save_pct Float32 DEFAULT 0,
    home_gk_played          String DEFAULT '', away_gk_played String DEFAULT '',  -- keeper name(s)

    -- === УДАЛЕНИЯ (Exclusions) ===
    home_exclusions         UInt8 DEFAULT 0,  away_exclusions UInt8 DEFAULT 0,      -- 20-сек удаления
    home_exclusions_drawn   UInt8 DEFAULT 0,  away_exclusions_drawn UInt8 DEFAULT 0,
    home_brutality_exc      UInt8 DEFAULT 0,  away_brutality_exc UInt8 DEFAULT 0,   -- грубость (до конца)

    -- === ПРОЧЕЕ ===
    home_sprints_won        UInt8 DEFAULT 0,  away_sprints_won UInt8 DEFAULT 0,     -- Swim-offs
    home_steals             UInt8 DEFAULT 0,  away_steals UInt8 DEFAULT 0,
    home_turnovers          UInt8 DEFAULT 0,  away_turnovers UInt8 DEFAULT 0,
    home_assists            UInt8 DEFAULT 0,  away_assists UInt8 DEFAULT 0,
    home_blocks             UInt8 DEFAULT 0,  away_blocks UInt8 DEFAULT 0,

    -- Коэффициенты
    b365_home    Float32 DEFAULT 0,  b365_away Float32 DEFAULT 0,  b365_draw Float32 DEFAULT 0,
    avg_home     Float32 DEFAULT 0,  avg_away Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (date, competition, home_team)
SETTINGS index_granularity = 8192;

-- Статистика игроков водное поло
CREATE TABLE IF NOT EXISTS betquant.waterpolo_player_stats (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    gender          LowCardinality(String),
    team            LowCardinality(String),
    opponent        LowCardinality(String),
    player          LowCardinality(String),
    player_id       String DEFAULT '',
    cap_number      UInt8 DEFAULT 0,
    position        LowCardinality(String) DEFAULT '',  -- GK/F/C (Goalkeeper/Field/Center)
    is_goalkeeper   UInt8 DEFAULT 0,
    minutes_played  UInt8 DEFAULT 0,

    -- Атака
    goals           UInt8 DEFAULT 0,
    shots           UInt8 DEFAULT 0,
    shot_pct        Float32 DEFAULT 0,
    action_goals    UInt8 DEFAULT 0,
    powerplay_goals UInt8 DEFAULT 0,
    counterattack_goals UInt8 DEFAULT 0,
    penalty_goals   UInt8 DEFAULT 0,
    center_goals    UInt8 DEFAULT 0,
    assists         UInt8 DEFAULT 0,

    -- Вратарь
    saves           UInt8 DEFAULT 0,
    save_pct        Float32 DEFAULT 0,
    goals_against   UInt8 DEFAULT 0,
    penalty_saves   UInt8 DEFAULT 0,

    -- Защита / нарушения
    steals          UInt8 DEFAULT 0,
    blocks          UInt8 DEFAULT 0,
    turnovers       UInt8 DEFAULT 0,
    exclusions      UInt8 DEFAULT 0,  -- удаления игрока
    exclusions_drawn UInt8 DEFAULT 0,
    swim_offs_won   UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (match_id, team, player)
SETTINGS index_granularity = 8192;

-- ═══════════════════════════════════════════════════════════════════════════════
--  6. VOLLEYBALL
--  Источники: openvolley (datavolley), FIVB stats, VNL, open kaggle datasets
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS betquant.volleyball_matches (
    match_id        String,
    source          LowCardinality(String),
    date            Date,
    season          LowCardinality(String),
    competition     LowCardinality(String),  -- VNL/World Champ/CEV Champions/SuperLega/Plus Liga
    competition_level LowCardinality(String),
    gender          LowCardinality(String),  -- male/female
    round           LowCardinality(String),
    venue           LowCardinality(String) DEFAULT '',
    home_team       LowCardinality(String),
    away_team       LowCardinality(String),

    -- Итог
    home_sets       UInt8 DEFAULT 0,   -- sets won
    away_sets       UInt8 DEFAULT 0,
    result          FixedString(1) DEFAULT 'H',  -- H/A
    total_sets      UInt8 DEFAULT 0,

    -- Счёт по партиям (до 5 партий)
    home_s1         UInt8 DEFAULT 0,  away_s1 UInt8 DEFAULT 0,
    home_s2         UInt8 DEFAULT 0,  away_s2 UInt8 DEFAULT 0,
    home_s3         UInt8 DEFAULT 0,  away_s3 UInt8 DEFAULT 0,
    home_s4         UInt8 DEFAULT 0,  away_s4 UInt8 DEFAULT 0,  -- если дошло
    home_s5         UInt8 DEFAULT 0,  away_s5 UInt8 DEFAULT 0,  -- тай-брейк
    total_points    UInt16 DEFAULT 0,
    home_total_pts  UInt16 DEFAULT 0, away_total_pts UInt16 DEFAULT 0,
    duration_min    UInt16 DEFAULT 0, -- длительность матча в минутах

    -- === АТАКА ===
    home_kills          UInt16 DEFAULT 0,  away_kills UInt16 DEFAULT 0,           -- успешные атаки
    home_attack_err     UInt8 DEFAULT 0,   away_attack_err UInt8 DEFAULT 0,       -- ошибки атаки
    home_attack_att     UInt16 DEFAULT 0,  away_attack_att UInt16 DEFAULT 0,      -- попытки атаки
    home_hit_pct        Float32 DEFAULT 0, away_hit_pct Float32 DEFAULT 0,        -- (kills-errors)/att
    home_kills_s1       UInt8 DEFAULT 0,   away_kills_s1 UInt8 DEFAULT 0,
    home_kills_s2       UInt8 DEFAULT 0,   away_kills_s2 UInt8 DEFAULT 0,
    home_kills_s3       UInt8 DEFAULT 0,   away_kills_s3 UInt8 DEFAULT 0,
    home_kills_s4       UInt8 DEFAULT 0,   away_kills_s4 UInt8 DEFAULT 0,
    home_kills_s5       UInt8 DEFAULT 0,   away_kills_s5 UInt8 DEFAULT 0,

    -- === ПОДАЧИ (Serves) ===
    home_aces           UInt8 DEFAULT 0,   away_aces UInt8 DEFAULT 0,             -- эйсы
    home_serve_err      UInt8 DEFAULT 0,   away_serve_err UInt8 DEFAULT 0,        -- ошибки подачи
    home_serve_att      UInt16 DEFAULT 0,  away_serve_att UInt16 DEFAULT 0,
    home_serve_efficiency Float32 DEFAULT 0, away_serve_efficiency Float32 DEFAULT 0,

    -- === БЛОКИ ===
    home_block_solos    UInt8 DEFAULT 0,   away_block_solos UInt8 DEFAULT 0,      -- одиночный блок
    home_block_assists  UInt8 DEFAULT 0,   away_block_assists UInt8 DEFAULT 0,    -- коллективный блок
    home_block_err      UInt8 DEFAULT 0,   away_block_err UInt8 DEFAULT 0,
    home_blocks_total   UInt8 DEFAULT 0,   away_blocks_total UInt8 DEFAULT 0,     -- solos + 0.5*assists

    -- === ПРИЁМ / ПАСОВКА ===
    home_assists        UInt16 DEFAULT 0,  away_assists UInt16 DEFAULT 0,         -- пасовка (assists)
    home_assist_err     UInt8 DEFAULT 0,   away_assist_err UInt8 DEFAULT 0,
    home_digs           UInt16 DEFAULT 0,  away_digs UInt16 DEFAULT 0,            -- защитные приёмы
    home_reception_att  UInt16 DEFAULT 0,  away_reception_att UInt16 DEFAULT 0,
    home_reception_err  UInt8 DEFAULT 0,   away_reception_err UInt8 DEFAULT 0,
    home_reception_pct  Float32 DEFAULT 0, away_reception_pct Float32 DEFAULT 0,  -- % позитивных приёмов

    -- Эффективность по парт
    home_aces_s1        UInt8 DEFAULT 0,  away_aces_s1 UInt8 DEFAULT 0,
    home_aces_s2        UInt8 DEFAULT 0,  away_aces_s2 UInt8 DEFAULT 0,
    home_aces_s3        UInt8 DEFAULT 0,  away_aces_s3 UInt8 DEFAULT 0,
    home_blocks_s1      UInt8 DEFAULT 0,  away_blocks_s1 UInt8 DEFAULT 0,
    home_blocks_s2      UInt8 DEFAULT 0,  away_blocks_s2 UInt8 DEFAULT 0,
    home_blocks_s3      UInt8 DEFAULT 0,  away_blocks_s3 UInt8 DEFAULT 0,

    -- === ПРОЧЕЕ ===
    home_pts_from_kills   UInt16 DEFAULT 0, away_pts_from_kills UInt16 DEFAULT 0,
    home_pts_from_aces    UInt8 DEFAULT 0,  away_pts_from_aces UInt8 DEFAULT 0,
    home_pts_from_blocks  UInt8 DEFAULT 0,  away_pts_from_blocks UInt8 DEFAULT 0,
    home_opponent_errors  UInt8 DEFAULT 0,  away_opponent_errors UInt8 DEFAULT 0,  -- pts from opp errors

    -- Коэффициенты
    b365_home    Float32 DEFAULT 0,  b365_away Float32 DEFAULT 0,
    avg_home     Float32 DEFAULT 0,  avg_away Float32 DEFAULT 0,
    hcap_home    Float32 DEFAULT 0,  hcap_line Float32 DEFAULT 0,   -- Sets handicap
    ou_sets_line Float32 DEFAULT 0,  over_sets Float32 DEFAULT 0,  under_sets Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, competition, home_team)
SETTINGS index_granularity = 8192;

-- Статистика по партиям
CREATE TABLE IF NOT EXISTS betquant.volleyball_set_stats (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    gender          LowCardinality(String),
    home_team       LowCardinality(String),
    away_team       LowCardinality(String),
    set_num         UInt8,              -- 1-5
    home_pts        UInt8 DEFAULT 0,
    away_pts        UInt8 DEFAULT 0,
    duration_min    UInt8 DEFAULT 0,
    home_kills      UInt8 DEFAULT 0,  away_kills UInt8 DEFAULT 0,
    home_aces       UInt8 DEFAULT 0,  away_aces UInt8 DEFAULT 0,
    home_blocks     UInt8 DEFAULT 0,  away_blocks UInt8 DEFAULT 0,
    home_errors     UInt8 DEFAULT 0,  away_errors UInt8 DEFAULT 0,
    home_hit_pct    Float32 DEFAULT 0, away_hit_pct Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, set_num)
SETTINGS index_granularity = 8192;

-- Статистика игроков волейбол
CREATE TABLE IF NOT EXISTS betquant.volleyball_player_stats (
    match_id        String,
    date            Date,
    competition     LowCardinality(String),
    gender          LowCardinality(String),
    team            LowCardinality(String),
    opponent        LowCardinality(String),
    player          LowCardinality(String),
    player_id       String DEFAULT '',
    number          UInt8 DEFAULT 0,
    position        LowCardinality(String) DEFAULT '',  -- OH/OPP/MB/S/L (Outside/Opposite/Middle/Setter/Libero)
    sets_played     UInt8 DEFAULT 0,

    -- Атака
    kills           UInt8 DEFAULT 0,
    attack_err      UInt8 DEFAULT 0,
    attack_att      UInt8 DEFAULT 0,
    hit_pct         Float32 DEFAULT 0,

    -- Подача
    aces            UInt8 DEFAULT 0,
    serve_err       UInt8 DEFAULT 0,
    serve_att       UInt8 DEFAULT 0,

    -- Блок
    block_solos     UInt8 DEFAULT 0,
    block_assists   UInt8 DEFAULT 0,
    block_err       UInt8 DEFAULT 0,

    -- Приём / защита
    digs            UInt8 DEFAULT 0,
    reception_att   UInt8 DEFAULT 0,
    reception_err   UInt8 DEFAULT 0,
    reception_pct   Float32 DEFAULT 0,

    -- Пасовка
    assists         UInt8 DEFAULT 0,
    assist_err      UInt8 DEFAULT 0,

    -- Points summary
    total_pts       UInt8 DEFAULT 0,   -- kills + aces + block_solos + 0.5*block_assists
    points_per_set  Float32 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, team, player)
SETTINGS index_granularity = 8192;

-- ═══════════════════════════════════════════════════════════════════════════════
--  ETL LOG (обновлённый)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS betquant.etl_log_v2 (
    ts              DateTime DEFAULT now(),
    sport           LowCardinality(String),
    source          LowCardinality(String),
    competition     LowCardinality(String) DEFAULT '',
    season          LowCardinality(String) DEFAULT '',
    table_name      LowCardinality(String),
    rows_loaded     UInt32 DEFAULT 0,
    duration_sec    Float32 DEFAULT 0,
    status          LowCardinality(String),
    message         String DEFAULT ''
) ENGINE = MergeTree()
ORDER BY ts
TTL ts + INTERVAL 180 DAY;

SELECT 'BetQuant Sports Schema v2 created successfully' AS status;
