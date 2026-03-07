# BetQuant Sports ETL v2
## Расширенная статистика по 6 видам спорта

---

## 📊 Источники данных и объём

| Спорт | Источник | Матчей | Ball/Play-by-Play | Игроки |
|---|---|---|---|---|
| 🏀 **Баскетбол (NBA)** | stats.nba.com (nba_api) | ~7,000 | ~2M PbP событий | ~500K строк |
| 🏏 **Крикет** | cricsheet.org (открыто) | ~21,000 | ~15M подач | ~300K scorecard |
| 🏉 **Регби** | ESPN Scrum | ~2,000 | События (попытки/пенальти) | ~50K строк |
| 🏈 **Амер. Футбол (NFL)** | nflverse (открыто) | ~4,800 | ~10M plays (с 1999) | ~200K строк |
| 🤽 **Водное Поло** | ESPN + Kaggle | ~3,000 | — | ~30K строк |
| 🏐 **Волейбол** | ESPN + Sportdevs | ~5,000 | По партиям | ~50K строк |

---

## 🗄️ Таблицы ClickHouse (новые)

### Баскетбол
| Таблица | Поля | Детализация |
|---|---|---|
| `basketball_matches_v2` | 100+ | Матч: Q1-Q4+OT, подборы, передачи, EPA, продвинутые метрики |
| `basketball_quarter_stats` | 25 | Командная статистика по каждой четверти |
| `basketball_player_stats` | 60 | Игрок: PTS/REB/AST/STL/BLK/TO + Advanced (TS%, USG%, ORTG) |
| `basketball_pbp` | 35 | Play-by-play: тип, зона броска, координаты XY, EP/WP |

### Крикет
| Таблица | Поля | Детализация |
|---|---|---|
| `cricket_matches` | 40 | Тип (Test/ODI/T20), составы иннингов, DLS |
| `cricket_deliveries` | 35 | **Каждая подача**: подающий/отбивающий, виды, граница/шесть |
| `cricket_batting` | 25 | Скоркарта: раны, мячи, четвёрки/шестёрки, по фазам (PP/mid/death) |
| `cricket_bowling` | 25 | Скоркарта: оверы, мэйдены, экономика, страйк-рейт, фазы |
| `cricket_fielding` | 10 | Поимки, стемпинги, ран-ауты |

### Регби
| Таблица | Поля | Детализация |
|---|---|---|
| `rugby_matches` | 80+ | Счёт по H1/H2, попытки/реализации/пенальти/дроп-голы, схватки, выходы в аут, владение, метры |
| `rugby_events` | 12 | Каждый балл с минутой: тип, команда, игрок |
| `rugby_player_stats` | 20 | Метры, захваты, попытки, нарушения |

### NFL
| Таблица | Поля | Детализация |
|---|---|---|
| `nfl_games` | 90+ | Q1-Q4+OT, пасс/бег/приём, 3-й даун, EPA, WP |
| `nfl_pbp` | 70 | Каждый play: down/distance, EPA/WPA, CP/CPOE, координаты |
| `nfl_player_stats` | 40 | QB/RB/WR/TE: passing/rushing/receiving + EPA метрики |

### Водное Поло
| Таблица | Поля | Детализация |
|---|---|---|
| `waterpolo_matches` | 60+ | Q1-Q4, голы по типу атаки (равные/большинство/контратака/пенальти/центр), вратари, удаления |
| `waterpolo_player_stats` | 20 | Голы, броски, спасения (GK), удаления, перехваты |

### Волейбол
| Таблица | Поля | Детализация |
|---|---|---|
| `volleyball_matches` | 70+ | S1-S5, убийства/эйсы/блоки/приёмы по партиям и итого, эффективность |
| `volleyball_set_stats` | 15 | Каждая партия отдельно |
| `volleyball_player_stats` | 20 | Позиция (OH/OPP/MB/S/L), kills/aces/blocks/digs |

---

## 🚀 Деплой

### Быстрый старт (только Cricket + Volleyball, ~20 мин):
```bash
# Скопировать папку sports-etl-v2 в проект
cp -r sports-etl-v2 /your/betquant/

# Применить схему
docker compose exec betquant-server \
  python3 /app/sports-etl-v2/run_etl_v2.py \
  --ch-url http://clickhouse:8123 --schema-only

# Загрузить только крикет (быстро, без PbP)
docker compose run --rm etl \
  python3 run_etl_v2.py --sport cricket --quick --seasons 1
```

### Полная загрузка (все виды спорта, 3 сезона, ~3-6 часов):
```bash
docker compose run --rm etl \
  python3 run_etl_v2.py --all --seasons 3
```

### Отдельные виды:
```bash
# Только баскетбол (NBA, 2 сезона)
python3 run_etl_v2.py --sport basketball --seasons 2

# Cricket + Rugby
python3 run_etl_v2.py --sport cricket,rugby --seasons 3

# NFL без PbP (быстрее)
python3 run_etl_v2.py --sport nfl --quick --seasons 3
```

---

## 📈 Что это даёт для беттинга

### Баскетбол
- Ставки на тотал по четвертям (Q1 over/under)
- Гандикап на первую половину
- Ставки на подборы/передачи конкретного игрока
- EPA-модели для оценки эффективности атаки

### Крикет
- Тотал ранов по оверам (PowerPlay/Death)
- Выбывание следующего игрока (Next Wicket)
- Ставки на граница/шесть за матч
- Метрики экономики подающего

### Регби
- Тотал попыток
- Ставки на владение мячом / территорию
- Успех схваток / выходов в аут как предиктор
- Первая половина гандикап

### NFL
- EPA per play как индикатор эффективности
- 3rd Down conversion rate
- Turnovers differential → исход матча
- QB rating + CPOE для ставок на тачдауны

### Водное Поло
- Эффективность большинства (powerplay %)
- Вратарь — % спасений как ключевой фактор
- Контратаки как предиктор победы

### Волейбол
- Hitting % (атакующая эффективность)
- Aces per set → вероятность выигрыша партии
- Блоки как defensive metric
- Тотал очков / партий

---

## 🔧 Добавление в Backtest Engine

```javascript
// В server/index.js — новые виды спорта в backtestQuery():
case 'basketball':
  query = `SELECT * FROM betquant.basketball_matches_v2
           WHERE league='NBA' AND season='${season}'`;
  break;

case 'cricket':
  query = `SELECT * FROM betquant.cricket_matches
           WHERE competition='${league}' AND toYear(date)=${year}`;
  break;

case 'rugby':
  query = `SELECT * FROM betquant.rugby_matches
           WHERE competition='${league}' AND season='${season}'`;
  break;

case 'nfl':
  query = `SELECT * FROM betquant.nfl_games
           WHERE season=${season} AND season_type='REG'`;
  break;

case 'waterpolo':
  query = `SELECT * FROM betquant.waterpolo_matches
           WHERE competition='${league}'`;
  break;

case 'volleyball':
  query = `SELECT * FROM betquant.volleyball_matches
           WHERE competition='${league}'`;
  break;
```
