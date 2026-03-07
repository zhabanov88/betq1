-- ═══════════════════════════════════════════════════════════════════════
--  BetQuant Pro — Extended ClickHouse Schema
--  Все виды спорта: Футбол, Хоккей, Теннис, Баскетбол, Бейсбол
-- ═══════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS betquant;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  ФУТБОЛ — основная таблица матчей
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.football_matches (
    -- Идентификация
    match_id       String,
    source         LowCardinality(String),          -- 'football-data.co.uk' | 'understat' | 'fbref'
    date           Date,
    datetime       DateTime DEFAULT toDateTime(date),
    season         LowCardinality(String),           -- '2024-25'
    league_code    LowCardinality(String),           -- 'E0','D1','SP1' etc
    league_name    LowCardinality(String),           -- 'Premier League'
    country        LowCardinality(String),
    home_team      LowCardinality(String),
    away_team      LowCardinality(String),

    -- Результат
    home_goals     UInt8  DEFAULT 0,
    away_goals     UInt8  DEFAULT 0,
    ht_home_goals  UInt8  DEFAULT 0,                 -- Half-time
    ht_away_goals  UInt8  DEFAULT 0,
    result         FixedString(1) DEFAULT 'H',       -- H/D/A

    -- Базовая статистика
    home_shots              UInt8  DEFAULT 0,
    away_shots              UInt8  DEFAULT 0,
    home_shots_on_target    UInt8  DEFAULT 0,
    away_shots_on_target    UInt8  DEFAULT 0,
    home_corners            UInt8  DEFAULT 0,
    away_corners            UInt8  DEFAULT 0,
    home_fouls              UInt8  DEFAULT 0,
    away_fouls              UInt8  DEFAULT 0,
    home_yellow             UInt8  DEFAULT 0,
    away_yellow             UInt8  DEFAULT 0,
    home_red                UInt8  DEFAULT 0,
    away_red                UInt8  DEFAULT 0,

    -- xG (от understat/fbref)
    home_xg        Float32 DEFAULT 0,
    away_xg        Float32 DEFAULT 0,
    home_xga       Float32 DEFAULT 0,
    away_xga       Float32 DEFAULT 0,
    home_npxg      Float32 DEFAULT 0,               -- без пенальти
    away_npxg      Float32 DEFAULT 0,

    -- Продвинутые метрики (от understat)
    home_ppda      Float32 DEFAULT 0,               -- passes per defensive action
    away_ppda      Float32 DEFAULT 0,
    home_deep      UInt8   DEFAULT 0,               -- deep completions
    away_deep      UInt8   DEFAULT 0,
    home_xpts      Float32 DEFAULT 0,               -- expected points
    away_xpts      Float32 DEFAULT 0,

    -- Ожидаемые очки (pre-match forecast от understat)
    forecast_win   Float32 DEFAULT 0,
    forecast_draw  Float32 DEFAULT 0,
    forecast_loss  Float32 DEFAULT 0,

    -- Коэффициенты (основные букмекеры)
    b365_home Float32 DEFAULT 0,
    b365_draw Float32 DEFAULT 0,
    b365_away Float32 DEFAULT 0,
    b365_over  Float32 DEFAULT 0,
    b365_under Float32 DEFAULT 0,
    pinnacle_home Float32 DEFAULT 0,
    pinnacle_draw Float32 DEFAULT 0,
    pinnacle_away Float32 DEFAULT 0,
    max_home   Float32 DEFAULT 0,                   -- максимальный у всех букмекеров
    max_draw   Float32 DEFAULT 0,
    max_away   Float32 DEFAULT 0,
    avg_home   Float32 DEFAULT 0,
    avg_draw   Float32 DEFAULT 0,
    avg_away   Float32 DEFAULT 0,

    -- Азиатский гандикап
    ah_line    Float32 DEFAULT 0,
    ah_home    Float32 DEFAULT 0,
    ah_away    Float32 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, league_code, home_team)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  ФУТБОЛ — события внутри матча (голы, карточки поминутно)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.football_events (
    match_id     String,
    date         Date,
    league_code  LowCardinality(String),
    minute       UInt8,                      -- 0-120
    event_type   LowCardinality(String),     -- 'goal'|'yellow'|'red'|'penalty'|'own_goal'|'sub'
    team         LowCardinality(String),     -- home/away
    team_name    LowCardinality(String),
    player       String DEFAULT '',
    player_id    String DEFAULT '',
    assist       String DEFAULT '',
    xg_shot      Float32 DEFAULT 0,          -- xG конкретного удара (голы)
    x_coord      Float32 DEFAULT 0,          -- позиция удара на поле
    y_coord      Float32 DEFAULT 0,
    situation    LowCardinality(String) DEFAULT '', -- 'OpenPlay'|'SetPiece'|'FromCorner'|'Penalty'
    shot_type    LowCardinality(String) DEFAULT '', -- 'Head'|'RightFoot'|'LeftFoot'
    home_score   UInt8 DEFAULT 0,            -- счёт после события
    away_score   UInt8 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, minute, event_type)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  ФУТБОЛ — агрегированная статистика команды ДО матча
--  Для стратегий и бэктестинга (rolling stats)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.football_team_form (
    match_id    String,
    date        Date,
    team        LowCardinality(String),
    league_code LowCardinality(String),
    season      LowCardinality(String),
    is_home     UInt8,                       -- 1=home, 0=away

    -- Форма — последние N матчей
    form_5      String DEFAULT '',           -- 'WDLWW' последние 5
    form_10     String DEFAULT '',
    pts_5       UInt8 DEFAULT 0,             -- очки за 5 матчей
    pts_10      UInt8 DEFAULT 0,

    -- Текущий сезон (до этого матча)
    season_matches       UInt16 DEFAULT 0,
    season_wins          UInt16 DEFAULT 0,
    season_draws         UInt16 DEFAULT 0,
    season_losses        UInt16 DEFAULT 0,
    season_goals_for     UInt16 DEFAULT 0,
    season_goals_against UInt16 DEFAULT 0,
    season_goal_diff     Int16  DEFAULT 0,
    season_corners_for   UInt16 DEFAULT 0,
    season_corners_against UInt16 DEFAULT 0,
    season_yellow        UInt16 DEFAULT 0,
    season_red           UInt16 DEFAULT 0,
    season_xg_for        Float32 DEFAULT 0,
    season_xg_against    Float32 DEFAULT 0,
    season_shots_for     UInt16 DEFAULT 0,
    season_shots_against UInt16 DEFAULT 0,

    -- Прошлый сезон (полный)
    prev_season          LowCardinality(String) DEFAULT '',
    prev_season_matches  UInt16 DEFAULT 0,
    prev_season_wins     UInt16 DEFAULT 0,
    prev_season_goals_for     UInt16 DEFAULT 0,
    prev_season_goals_against UInt16 DEFAULT 0,
    prev_season_xg_for        Float32 DEFAULT 0,
    prev_season_xg_against    Float32 DEFAULT 0,

    -- За последний месяц (30 дней до матча)
    last30_matches       UInt8 DEFAULT 0,
    last30_goals_for     UInt8 DEFAULT 0,
    last30_goals_against UInt8 DEFAULT 0,
    last30_xg_for        Float32 DEFAULT 0,
    last30_corners       UInt8 DEFAULT 0,
    last30_yellow        UInt8 DEFAULT 0,

    -- За текущий календарный год
    ytd_matches          UInt16 DEFAULT 0,
    ytd_goals_for        UInt16 DEFAULT 0,
    ytd_goals_against    UInt16 DEFAULT 0,
    ytd_xg_for           Float32 DEFAULT 0,

    -- Дома/в гостях раздельно (текущий сезон)
    home_season_matches  UInt8 DEFAULT 0,
    home_season_goals_for  UInt8 DEFAULT 0,
    home_season_goals_against UInt8 DEFAULT 0,
    away_season_matches  UInt8 DEFAULT 0,
    away_season_goals_for  UInt8 DEFAULT 0,
    away_season_goals_against UInt8 DEFAULT 0,

    -- H2H (последние 5 встреч между этими командами)
    h2h_wins    UInt8 DEFAULT 0,
    h2h_draws   UInt8 DEFAULT 0,
    h2h_losses  UInt8 DEFAULT 0,
    h2h_goals_for     UInt8 DEFAULT 0,
    h2h_goals_against UInt8 DEFAULT 0,
    h2h_matches UInt8 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, team, league_code)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  ХОККЕЙ — матчи NHL + KHL + другие лиги
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.hockey_matches (
    match_id    String,
    source      LowCardinality(String),
    date        Date,
    datetime    DateTime DEFAULT toDateTime(date),
    season      LowCardinality(String),
    league      LowCardinality(String),              -- 'NHL'|'KHL'|'SHL'
    home_team   LowCardinality(String),
    away_team   LowCardinality(String),

    -- Результат
    home_goals     UInt8 DEFAULT 0,
    away_goals     UInt8 DEFAULT 0,
    home_goals_p1  UInt8 DEFAULT 0,                 -- Период 1
    away_goals_p1  UInt8 DEFAULT 0,
    home_goals_p2  UInt8 DEFAULT 0,
    away_goals_p2  UInt8 DEFAULT 0,
    home_goals_p3  UInt8 DEFAULT 0,
    away_goals_p3  UInt8 DEFAULT 0,
    home_goals_ot  UInt8 DEFAULT 0,                 -- Овертайм
    away_goals_ot  UInt8 DEFAULT 0,
    went_to_ot     UInt8 DEFAULT 0,
    went_to_so     UInt8 DEFAULT 0,                 -- Буллиты
    result         FixedString(1) DEFAULT 'H',

    -- Броски
    home_shots     UInt8 DEFAULT 0,
    away_shots     UInt8 DEFAULT 0,
    home_shots_p1  UInt8 DEFAULT 0,
    away_shots_p1  UInt8 DEFAULT 0,
    home_shots_p2  UInt8 DEFAULT 0,
    away_shots_p2  UInt8 DEFAULT 0,
    home_shots_p3  UInt8 DEFAULT 0,
    away_shots_p3  UInt8 DEFAULT 0,

    -- Продвинутые NHL stats (Corsi/Fenwick)
    home_corsi_for    UInt16 DEFAULT 0,
    away_corsi_for    UInt16 DEFAULT 0,
    home_fenwick_for  UInt16 DEFAULT 0,
    away_fenwick_for  UInt16 DEFAULT 0,
    home_cf_pct       Float32 DEFAULT 0,            -- Corsi For %
    home_xg_for       Float32 DEFAULT 0,
    away_xg_for       Float32 DEFAULT 0,

    -- Численное превосходство
    home_pp_goals  UInt8 DEFAULT 0,                 -- Power Play Goals
    away_pp_goals  UInt8 DEFAULT 0,
    home_pp_opp    UInt8 DEFAULT 0,                 -- Power Play Opportunities
    away_pp_opp    UInt8 DEFAULT 0,
    home_sh_goals  UInt8 DEFAULT 0,                 -- Shorthanded Goals
    away_sh_goals  UInt8 DEFAULT 0,

    -- Штрафы
    home_pim       UInt8 DEFAULT 0,                 -- Penalty Minutes
    away_pim       UInt8 DEFAULT 0,
    home_penalties UInt8 DEFAULT 0,
    away_penalties UInt8 DEFAULT 0,

    -- Вбрасывания
    home_faceoff_pct Float32 DEFAULT 0,
    away_faceoff_pct Float32 DEFAULT 0,

    -- Вратари
    home_saves       UInt8 DEFAULT 0,
    away_saves       UInt8 DEFAULT 0,
    home_save_pct    Float32 DEFAULT 0,
    away_save_pct    Float32 DEFAULT 0,

    -- Коэффициенты
    b365_home Float32 DEFAULT 0,
    b365_away Float32 DEFAULT 0,
    b365_over  Float32 DEFAULT 0,
    b365_under Float32 DEFAULT 0,
    pinnacle_home Float32 DEFAULT 0,
    pinnacle_away Float32 DEFAULT 0,
    puck_line_home Float32 DEFAULT 0,               -- -1.5 puck line
    puck_line_away Float32 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, league, home_team)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  ХОККЕЙ — события матча поминутно
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.hockey_events (
    match_id    String,
    date        Date,
    league      LowCardinality(String),
    period      UInt8,                       -- 1/2/3/4(OT)
    time_in_period String DEFAULT '',        -- '12:34'
    event_type  LowCardinality(String),      -- 'goal'|'penalty'|'shot'|'blocked_shot'|'missed_shot'|'faceoff'|'hit'
    team        LowCardinality(String),      -- home/away
    team_name   LowCardinality(String),
    player      String DEFAULT '',
    player_id   String DEFAULT '',
    assist1     String DEFAULT '',
    assist2     String DEFAULT '',
    penalty_type LowCardinality(String) DEFAULT '',
    penalty_min  UInt8 DEFAULT 0,
    x_coord     Float32 DEFAULT 0,
    y_coord     Float32 DEFAULT 0,
    shot_type   LowCardinality(String) DEFAULT '',
    home_score  UInt8 DEFAULT 0,
    away_score  UInt8 DEFAULT 0,
    strength    LowCardinality(String) DEFAULT '' -- 'even'|'pp'|'sh'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (match_id, period, time_in_period)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  ХОККЕЙ — форма команды ДО матча
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.hockey_team_form (
    match_id    String,
    date        Date,
    team        LowCardinality(String),
    league      LowCardinality(String),
    season      LowCardinality(String),
    is_home     UInt8,

    form_5      String DEFAULT '',
    pts_5       UInt8 DEFAULT 0,
    pts_10      UInt8 DEFAULT 0,

    season_gp            UInt16 DEFAULT 0,
    season_wins          UInt16 DEFAULT 0,
    season_losses        UInt16 DEFAULT 0,
    season_ot_losses     UInt16 DEFAULT 0,
    season_pts           UInt16 DEFAULT 0,
    season_goals_for     UInt16 DEFAULT 0,
    season_goals_against UInt16 DEFAULT 0,
    season_shots_for     UInt16 DEFAULT 0,
    season_shots_against UInt16 DEFAULT 0,
    season_pp_pct        Float32 DEFAULT 0,
    season_pk_pct        Float32 DEFAULT 0,
    season_xg_for        Float32 DEFAULT 0,
    season_cf_pct        Float32 DEFAULT 0,

    prev_season          LowCardinality(String) DEFAULT '',
    prev_season_pts      UInt16 DEFAULT 0,
    prev_season_goals_for     UInt16 DEFAULT 0,
    prev_season_goals_against UInt16 DEFAULT 0,

    last30_gp            UInt8 DEFAULT 0,
    last30_goals_for     UInt8 DEFAULT 0,
    last30_goals_against UInt8 DEFAULT 0,
    last30_pts           UInt8 DEFAULT 0,

    ytd_gp               UInt16 DEFAULT 0,
    ytd_goals_for        UInt16 DEFAULT 0,
    ytd_goals_against    UInt16 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, team, league)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  ТЕННИС — матчи ATP/WTA/Challengers
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.tennis_matches (
    match_id       String,
    source         LowCardinality(String),
    date           Date,
    tournament     LowCardinality(String),
    surface        LowCardinality(String),    -- Hard/Clay/Grass/Carpet
    tour           LowCardinality(String),    -- ATP/WTA/Challenger/ITF
    round          LowCardinality(String),    -- QF/SF/F/R32 etc
    best_of        UInt8 DEFAULT 3,
    indoor         UInt8 DEFAULT 0,

    winner         LowCardinality(String),
    loser          LowCardinality(String),
    winner_id      String DEFAULT '',
    loser_id       String DEFAULT '',
    score          String DEFAULT '',          -- '6-3 7-5 6-4'
    sets_played    UInt8 DEFAULT 0,
    retired        UInt8 DEFAULT 0,            -- walkover/retirement

    -- Рейтинги
    winner_rank    UInt16 DEFAULT 0,
    loser_rank     UInt16 DEFAULT 0,
    winner_rank_pts UInt32 DEFAULT 0,
    loser_rank_pts  UInt32 DEFAULT 0,
    winner_seed    UInt8 DEFAULT 0,
    loser_seed     UInt8 DEFAULT 0,

    -- Статистика (от ATP/WTA)
    w_aces         UInt8 DEFAULT 0,
    l_aces         UInt8 DEFAULT 0,
    w_df           UInt8 DEFAULT 0,            -- double faults
    l_df           UInt8 DEFAULT 0,
    w_svpt         UInt16 DEFAULT 0,           -- serve points
    l_svpt         UInt16 DEFAULT 0,
    w_1st_in       UInt16 DEFAULT 0,
    l_1st_in       UInt16 DEFAULT 0,
    w_1st_won      UInt16 DEFAULT 0,
    l_1st_won      UInt16 DEFAULT 0,
    w_2nd_won      UInt16 DEFAULT 0,
    l_2nd_won      UInt16 DEFAULT 0,
    w_bp_saved     UInt16 DEFAULT 0,
    l_bp_saved     UInt16 DEFAULT 0,
    w_bp_faced     UInt16 DEFAULT 0,
    l_bp_faced     UInt16 DEFAULT 0,
    minutes        UInt16 DEFAULT 0,

    -- Коэффициенты
    b365_winner  Float32 DEFAULT 0,
    b365_loser   Float32 DEFAULT 0,
    ps_winner    Float32 DEFAULT 0,            -- Pinnacle
    ps_loser     Float32 DEFAULT 0,
    max_winner   Float32 DEFAULT 0,
    max_loser    Float32 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (date, tour, winner)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  БАСКЕТБОЛ — матчи NBA/EuroLeague
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.basketball_matches (
    match_id    String,
    source      LowCardinality(String),
    date        Date,
    season      LowCardinality(String),
    league      LowCardinality(String),       -- 'NBA'|'EuroLeague'|'NBL'
    home_team   LowCardinality(String),
    away_team   LowCardinality(String),

    home_pts    UInt16 DEFAULT 0,
    away_pts    UInt16 DEFAULT 0,
    home_pts_q1 UInt8 DEFAULT 0,
    away_pts_q1 UInt8 DEFAULT 0,
    home_pts_q2 UInt8 DEFAULT 0,
    away_pts_q2 UInt8 DEFAULT 0,
    home_pts_q3 UInt8 DEFAULT 0,
    away_pts_q3 UInt8 DEFAULT 0,
    home_pts_q4 UInt8 DEFAULT 0,
    away_pts_q4 UInt8 DEFAULT 0,
    home_pts_ot UInt8 DEFAULT 0,
    away_pts_ot UInt8 DEFAULT 0,
    went_to_ot  UInt8 DEFAULT 0,
    result      FixedString(1) DEFAULT 'H',

    -- Статистика
    home_fg_pct   Float32 DEFAULT 0,
    away_fg_pct   Float32 DEFAULT 0,
    home_fg3_pct  Float32 DEFAULT 0,
    away_fg3_pct  Float32 DEFAULT 0,
    home_ft_pct   Float32 DEFAULT 0,
    away_ft_pct   Float32 DEFAULT 0,
    home_reb      UInt8 DEFAULT 0,
    away_reb      UInt8 DEFAULT 0,
    home_ast      UInt8 DEFAULT 0,
    away_ast      UInt8 DEFAULT 0,
    home_tov      UInt8 DEFAULT 0,
    away_tov      UInt8 DEFAULT 0,
    home_stl      UInt8 DEFAULT 0,
    away_stl      UInt8 DEFAULT 0,
    home_blk      UInt8 DEFAULT 0,
    away_blk      UInt8 DEFAULT 0,
    home_pace     Float32 DEFAULT 0,
    away_pace     Float32 DEFAULT 0,
    home_ortg     Float32 DEFAULT 0,           -- offensive rating
    away_ortg     Float32 DEFAULT 0,
    home_drtg     Float32 DEFAULT 0,
    away_drtg     Float32 DEFAULT 0,

    -- Коэффициенты
    b365_home  Float32 DEFAULT 0,
    b365_away  Float32 DEFAULT 0,
    b365_ou_line Float32 DEFAULT 0,
    b365_over  Float32 DEFAULT 0,
    b365_under Float32 DEFAULT 0,
    spread_home Float32 DEFAULT 0,
    spread_away Float32 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, league, home_team)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  БЕЙСБОЛ — матчи MLB
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.baseball_matches (
    match_id    String,
    date        Date,
    season      LowCardinality(String),
    league      LowCardinality(String),
    home_team   LowCardinality(String),
    away_team   LowCardinality(String),

    home_runs   UInt8 DEFAULT 0,
    away_runs   UInt8 DEFAULT 0,
    home_hits   UInt8 DEFAULT 0,
    away_hits   UInt8 DEFAULT 0,
    home_errors UInt8 DEFAULT 0,
    away_errors UInt8 DEFAULT 0,
    innings     UInt8 DEFAULT 9,
    result      FixedString(1) DEFAULT 'H',

    home_era    Float32 DEFAULT 0,            -- Earned Run Average
    away_era    Float32 DEFAULT 0,
    home_whip   Float32 DEFAULT 0,
    away_whip   Float32 DEFAULT 0,

    b365_home   Float32 DEFAULT 0,
    b365_away   Float32 DEFAULT 0,
    b365_over   Float32 DEFAULT 0,
    b365_under  Float32 DEFAULT 0,
    ou_line     Float32 DEFAULT 0,
    run_line_home Float32 DEFAULT 0,
    run_line_away Float32 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (date, league, home_team)
SETTINGS index_granularity = 8192;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
--  СЛУЖЕБНАЯ — лог загрузок
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS betquant.etl_log (
    ts           DateTime DEFAULT now(),
    sport        LowCardinality(String),
    source       LowCardinality(String),
    league       LowCardinality(String),
    season       LowCardinality(String),
    rows_loaded  UInt32 DEFAULT 0,
    status       LowCardinality(String),     -- 'ok'|'error'|'skip'
    message      String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY ts
TTL ts + INTERVAL 90 DAY;

SELECT 'Extended schema created OK' AS status;
