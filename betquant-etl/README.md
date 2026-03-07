# BetQuant ETL — Документация

## Структура проекта

```
betquant-etl/
├── schema/
│   └── clickhouse_extended.sql    # Расширенная схема ClickHouse (все виды спорта)
├── scrapers/
│   ├── scraper_football.py        # Футбол
│   ├── scraper_hockey.py          # Хоккей
│   └── scraper_other_sports.py    # Теннис / Баскетбол / Бейсбол
├── run_etl.py                     # Мастер-оркестратор
└── deploy_and_run.sh              # Скрипт деплоя и запуска
```

---

## Источники данных

### ⚽ ФУТБОЛ

#### football-data.co.uk
- **Что даёт:** 25+ лиг, с 1993 года, CSV файлы
- **Данные:** результаты, счёт по таймам, удары, угловые, фолы, жёлтые/красные карточки, коэффициенты 10+ букмекеров (B365, Pinnacle, макс/средний), азиатский гандикап
- **Лиги:** Premier League, Championship, La Liga, Bundesliga, Serie A, Ligue 1, Eredivisie, Primeira Liga, Süper Lig, Super League Greece, Scottish Premiership, MLS, Liga MX, Serie A Brasil, Primera División Argentina и другие
- **Обновление:** дважды в неделю
- **URL паттерн:** `https://www.football-data.co.uk/mmz4281/{SEASON}/{LEAGUE}.csv`

#### understat.com
- **Что даёт:** 6 топ-лиг (EPL, La Liga, Bundesliga, Serie A, Ligue 1, РФПЛ), с 2014 года
- **Данные:** xG, NPxG, xGA, xPTS, PPDA, deep completions, forecast (pre-match win/draw/loss prob), поминутные удары с координатами и xG каждого удара
- **События:** каждый удар содержит: минута, игрок, тип удара (OpenPlay/SetPiece/FromCorner/Penalty), тип ногой (Head/RightFoot/LeftFoot), координаты на поле, xG, результат (Goal/MissedShots/BlockedShot/SavedShot)
- **URL паттерн:** `https://understat.com/league/{LEAGUE}/{YEAR}`, `https://understat.com/match/{ID}`

#### Таблицы в ClickHouse

| Таблица | Описание |
|---------|----------|
| `football_matches` | Основная таблица матчей — все статистики + коэффициенты |
| `football_events` | Поминутные события: голы, удары с xG, жёлтые/красные карточки |
| `football_team_form` | Rolling stats ДО матча: форма, голы за сезон/месяц/год/сезон |

#### Поля `football_team_form` (для стратегий)

```sql
-- Текущий сезон (до матча)
season_matches, season_wins, season_draws, season_losses
season_goals_for, season_goals_against, season_goal_diff
season_corners_for, season_corners_against
season_yellow, season_red
season_xg_for, season_xg_against
season_shots_for, season_shots_against

-- Прошлый сезон (полный)
prev_season, prev_season_matches, prev_season_wins
prev_season_goals_for, prev_season_goals_against
prev_season_xg_for, prev_season_xg_against

-- Последние 30 дней
last30_matches, last30_goals_for, last30_goals_against
last30_xg_for, last30_corners, last30_yellow

-- Календарный год
ytd_matches, ytd_goals_for, ytd_goals_against, ytd_xg_for

-- Дома/в гостях (текущий сезон)
home_season_matches, home_season_goals_for, home_season_goals_against
away_season_matches, away_season_goals_for, away_season_goals_against

-- Форма (последние 5/10 матчей)
form_5 ('WDLWW'), form_10, pts_5, pts_10

-- H2H (последние 5 встреч)
h2h_wins, h2h_draws, h2h_losses, h2h_goals_for, h2h_goals_against
```

---

### 🏒 ХОККЕЙ

#### api-web.nhle.com (NHL Official API)
- **Что даёт:** NHL, все матчи с 2010+ года, официальный API, без ключа
- **Данные матча:** счёт по периодам (1/2/3/OT/SO), броски, pim, powerplay goals/opp, фейсофф%, блоки, хиты
- **Play-by-play:** каждое событие содержит: период, время, тип (goal/penalty/shot/hit/faceoff), команда, игрок, ассист 1/2, тип броска, координаты, счёт после события, ситуация (even/pp/sh)
- **Штрафы:** тип нарушения, длительность (2/4/5/10 мин), виновный игрок
- **URL:** `https://api-web.nhle.com/v1/gamecenter/{GAME_ID}/boxscore`, `/play-by-play`

#### KHL open datasets
- Открытые датасеты на GitHub с историческими данными КХЛ
- Счёт, периоды, ОТ/буллиты, броски, штрафные минуты

#### Таблицы в ClickHouse

| Таблица | Описание |
|---------|----------|
| `hockey_matches` | Матчи NHL/KHL — счёт по периодам, Corsi/Fenwick/xG, PP/PK |
| `hockey_events` | Play-by-play: голы/штрафы/броски поминутно с координатами |
| `hockey_team_form` | Rolling stats ДО матча: форма, голы за сезон/месяц/год |

#### Ключевые поля `hockey_matches`

```sql
-- Счёт по периодам
home_goals_p1, away_goals_p1   -- 1-й период
home_goals_p2, away_goals_p2   -- 2-й период  
home_goals_p3, away_goals_p3   -- 3-й период
home_goals_ot, away_goals_ot   -- Овертайм
went_to_ot, went_to_so         -- Дошли до ОТ/буллитов

-- Продвинутая статистика
home_corsi_for, away_corsi_for      -- Corsi (все броски направления ворот)
home_fenwick_for, away_fenwick_for  -- Fenwick (без блоков)
home_cf_pct                        -- Corsi For %
home_xg_for, away_xg_for           -- Expected Goals

-- Численное превосходство
home_pp_goals, away_pp_goals   -- Голы в большинстве
home_pp_opp, away_pp_opp       -- Попытки большинства
home_sh_goals, away_sh_goals   -- Голы в меньшинстве

-- Вратари
home_saves, away_saves
home_save_pct, away_save_pct

-- Коэффициенты
b365_home, b365_away
b365_over, b365_under          -- Тотал
puck_line_home, puck_line_away -- Puck line (-1.5)
```

---

### 🎾 ТЕННИС

#### Jeff Sackmann ATP/WTA datasets (GitHub)
- **Что даёт:** ATP с 1968, WTA с 1968, Challengers, ITF
- **Данные:** результат, счёт по сетам, поверхность, раунд, рейтинги, seed
- **Статистика подач:** aces, DF, svpt, 1stIn/Won, 2ndWon, bpSaved/Faced
- **Коэффициенты:** Betfair, B365, Pinnacle, MaxOdds, AvgOdds
- **Матчей:** ~200,000+ в базе

#### Таблица `tennis_matches`

```sql
-- Основное
winner, loser, score, sets_played, retired (1=retired/walkover)
surface (Hard/Clay/Grass/Carpet), tour (ATP/WTA/Challenger)
round (F/SF/QF/R16/R32/R64/R128), best_of, indoor

-- Рейтинги
winner_rank, loser_rank, winner_rank_pts, loser_rank_pts
winner_seed, loser_seed

-- Статистика
w_aces, l_aces, w_df, l_df
w_svpt, l_svpt
w_1st_in, l_1st_in      -- 1st serve in
w_1st_won, l_1st_won    -- 1st serve won
w_2nd_won, l_2nd_won    -- 2nd serve won
w_bp_saved, l_bp_saved  -- Break points saved
w_bp_faced, l_bp_faced  -- Break points faced
minutes                  -- Длительность матча

-- Коэффициенты
b365_winner, b365_loser, ps_winner, ps_loser
max_winner, max_loser
```

---

### 🏀 БАСКЕТБОЛ

#### balldontlie.io (NBA Free API)
- **Что даёт:** NBA матчи, результаты, базовая статистика
- **Бесплатно:** без API ключа, 30 запросов/мин
- **Данные:** результат, счёт по четвертям, OT, +-

#### Таблица `basketball_matches`

```sql
home_pts, away_pts
home_pts_q1/q2/q3/q4, away_pts_q1/q2/q3/q4
home_pts_ot, away_pts_ot, went_to_ot

-- Статистика
home_fg_pct, away_fg_pct       -- Field Goal %
home_fg3_pct, away_fg3_pct     -- 3-Point %
home_ft_pct, away_ft_pct       -- Free Throw %
home_reb, away_reb             -- Rebounds
home_ast, away_ast             -- Assists
home_tov, away_tov             -- Turnovers
home_stl, away_stl             -- Steals
home_blk, away_blk             -- Blocks
home_pace, away_pace           -- Pace (possessions/48min)
home_ortg, away_ortg           -- Offensive Rating
home_drtg, away_drtg           -- Defensive Rating

-- Коэффициенты
b365_home, b365_away
b365_ou_line, b365_over, b365_under
spread_home, spread_away        -- Point spread
```

---

### ⚾ БЕЙСБОЛ

#### Chadwick Baseball Databank (Sean Lahman)
- **Что даёт:** MLB с 1871 года, открытые данные
- **Данные:** результаты, хиты, ошибки, иннинги

#### Таблица `baseball_matches`

```sql
home_runs, away_runs
home_hits, away_hits, home_errors, away_errors
innings (9 по умолчанию, >9 если экстра-иннинги)
home_era, away_era   -- ERA стартового питчера
home_whip, away_whip -- WHIP стартового питчера
b365_home, b365_away
b365_over, b365_under, ou_line
run_line_home, run_line_away   -- Run line (-1.5)
```

---

## Установка и запуск

### Быстрый старт (рекомендуется)

```bash
# 1. Убедись что Docker стек запущен
docker compose up -d

# 2. Войди в контейнер или запусти с хоста
chmod +x deploy_and_run.sh

# Быстрый тест (5 лиг, 1 сезон, ~15 мин)
MODE=quick SEASONS=1 ./deploy_and_run.sh

# Полная загрузка (все лиги, 3 сезона, ~2-4 часа)
SEASONS=3 ./deploy_and_run.sh
```

### Запуск через Python напрямую

```bash
# Только схема
python3 run_etl.py --schema-only --ch-host http://localhost:8123

# Только футбол, топ-лиги, 3 сезона
python3 scrapers/scraper_football.py \
  --leagues top \
  --seasons 3 \
  --ch-host http://localhost:8123

# Только конкретные лиги
python3 scrapers/scraper_football.py \
  --leagues E0,D1,SP1 \
  --seasons 5

# Хоккей без play-by-play (быстро)
python3 scrapers/scraper_hockey.py \
  --seasons 2 \
  --skip-pbp

# Всё кроме хоккея
python3 run_etl.py --skip-hockey --seasons 3

# Только отчёт
python3 run_etl.py --report-only
```

### Внутри Docker

```bash
# Копируем и запускаем в контейнере приложения
docker cp betquant-etl/ betquant-app:/opt/
docker exec -it betquant-app bash -c "
  cd /opt/betquant-etl &&
  python3 run_etl.py \
    --ch-host http://clickhouse:8123 \
    --ch-db betquant \
    --seasons 3
"
```

---

## Примеры запросов для бэктестинга

```sql
-- Матчи с формой обеих команд (готово для стратегии)
SELECT
  m.date, m.league_name,
  m.home_team, m.away_team,
  m.home_goals, m.away_goals, m.result,
  -- Коэффициенты
  m.b365_home, m.b365_draw, m.b365_away,
  -- Форма хозяев
  hf.form_5      AS home_form,
  hf.pts_5       AS home_pts_last5,
  hf.season_goals_for   AS home_season_gf,
  hf.season_goals_against AS home_season_ga,
  hf.last30_goals_for   AS home_last30_gf,
  hf.season_xg_for      AS home_season_xg,
  -- Форма гостей
  af.form_5      AS away_form,
  af.pts_5       AS away_pts_last5,
  af.season_goals_for   AS away_season_gf,
  af.season_xg_for      AS away_season_xg,
  -- H2H
  hf.h2h_wins, hf.h2h_draws, hf.h2h_losses
FROM betquant.football_matches m
LEFT JOIN betquant.football_team_form hf
  ON m.match_id = hf.match_id AND hf.team = m.home_team
LEFT JOIN betquant.football_team_form af
  ON m.match_id = af.match_id AND af.team = m.away_team
WHERE m.league_code = 'E0'
  AND m.date >= '2022-01-01'
  AND m.b365_home > 0
ORDER BY m.date;

-- Голы по минутам (для тотал стратегий)
SELECT
  minute,
  countIf(event_type = 'goal') AS goals,
  round(countIf(event_type = 'goal') / count() * 100, 1) AS goal_pct
FROM betquant.football_events
WHERE league_code IN ('E0', 'SP1', 'D1')
GROUP BY minute
ORDER BY minute;

-- NHL матчи с Corsi
SELECT date, home_team, away_team,
  home_goals, away_goals,
  home_cf_pct, away_corsi_for,
  went_to_ot, went_to_so
FROM betquant.hockey_matches
WHERE league = 'NHL'
ORDER BY date DESC LIMIT 20;

-- Теннис: статистика по покрытию
SELECT surface,
  count() AS matches,
  avg(w_aces) AS avg_aces,
  avg(minutes) AS avg_minutes
FROM betquant.tennis_matches
WHERE tour = 'ATP' AND retired = 0
GROUP BY surface;
```

---

## Объём данных (ожидаемый)

| Таблица | Строк (3 сезона) |
|---------|-----------------|
| football_matches | ~150,000 |
| football_events | ~2,000,000 |
| football_team_form | ~300,000 |
| hockey_matches | ~8,000 |
| hockey_events | ~1,500,000 |
| hockey_team_form | ~16,000 |
| tennis_matches | ~200,000 |
| basketball_matches | ~7,000 |
| baseball_matches | ~7,000 |
| **ИТОГО** | **~4,000,000+** |

---

## Служебные запросы

```sql
-- Статус загрузок
SELECT sport, source, status, sum(rows_loaded) as rows
FROM betquant.etl_log
GROUP BY sport, source, status ORDER BY rows DESC;

-- Проверка покрытия лиг
SELECT league_name, league_code, count() as matches,
  min(date) as from_date, max(date) as to_date
FROM betquant.football_matches
GROUP BY league_name, league_code ORDER BY matches DESC;

-- Проверка xG покрытия
SELECT
  countIf(home_xg > 0) as with_xg,
  count() as total,
  round(countIf(home_xg > 0) / count() * 100, 1) as xg_pct
FROM betquant.football_matches;
```
