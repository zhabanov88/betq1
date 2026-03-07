# BetQuant Pro 🎯
**Advanced Betting Analytics & Backtesting Platform**

---

## 🚀 Быстрый старт (3 команды)

```bash
# 1. Скопируй env-файл
cp .env.example .env

# 2. Установи зависимости
npm install

# 3. Запусти сервер
npm start
# → Открывай http://localhost:3000
# → Нажми "DEMO ACCESS" — работает без БД
```

**Примечание:** Без PostgreSQL и ClickHouse платформа работает в **demo-режиме** с генерацией тестовых данных. Все модули (дашборд, бэктест, Monte Carlo, AI, и др.) полностью функциональны.

---

## 🐳 Запуск с Docker (с реальной БД)

```bash
cp .env.example .env
# Отредактируй .env при необходимости

docker compose up -d
# → PostgreSQL + ClickHouse + Redis + Nginx подымутся автоматически
# → http://localhost:80 (nginx) или http://localhost:3000 (прямо)
```

---

## 📋 Требования без Docker

| Компонент | Версия |
|-----------|--------|
| Node.js   | ≥ 18.0 |
| npm       | ≥ 8.0  |

**БД опциональны** — без них работает demo-режим.

---

## 📦 Структура проекта

```
betquant-pro/
├── .env.example          ← скопируй в .env
├── package.json          ← npm install + npm start
├── Dockerfile
├── docker-compose.yml
│
├── public/               ← фронтенд (SPA)
│   ├── index.html
│   ├── css/styles.css
│   └── js/
│       ├── app.js        — ядро, роутинг панелей
│       ├── dashboard.js  — KPI + 6 графиков
│       ├── backtest.js   — движок бэктестинга
│       ├── montecarlo.js — Monte Carlo симуляция
│       ├── optimizer.js  — оптимизатор параметров
│       ├── ai-strategy.js— AI генератор стратегий
│       ├── strategy.js   — билдер стратегий
│       ├── database.js   — браузер БД
│       ├── scraper.js    — сборщик данных
│       ├── charts.js     — анализ odds + статистика
│       ├── alerts.js     — алерты
│       ├── journal.js    — журнал ставок
│       └── library.js    — библиотека стратегий
│
├── server/
│   ├── index.js          ← ГЛАВНЫЙ ФАЙЛ СЕРВЕРА
│   └── data-collectors/
│       ├── football_data_uk.py  — 30+ лиг, 1993–now
│       ├── tennis_collector.py  — ATP/WTA 1968–now
│       ├── nba_collector.py     — NBA 1946–now
│       └── betfair_collector.py — Exchange prices
│
├── init-scripts/
│   ├── 01_init_postgres.sql   — схема PG
│   └── 02_init_clickhouse.sql — схема CH
│
└── nginx/nginx.conf
```

---

## 🔑 AI Strategy Generator

Добавь в `.env`:
```
ANTHROPIC_API_KEY=sk-ant-xxxxxxx
```
Или введи ключ прямо в Settings панели. Claude генерирует полный код стратегии — сразу готовой к бэктестингу.

---

## 📡 Источники данных (Python collectors)

```bash
# Установи Python зависимости
pip3 install requests clickhouse-connect pandas numpy

# Собери данные football-data.co.uk (30+ лиг, 1993–now, бесплатно)
python3 server/data-collectors/football_data_uk.py

# ATP/WTA теннис 1968–now (GitHub Jeff Sackmann, бесплатно)
python3 server/data-collectors/tennis_collector.py

# NBA 1946–now
python3 server/data-collectors/nba_collector.py
```

| Источник | Спорт | Покрытие | Цена |
|----------|-------|----------|------|
| football-data.co.uk | ⚽ | 30+ лиг, 1993–now | Free CSV |
| OpenFootball GitHub | ⚽ | 50+ лиг, 2012–now | Open Source |
| FBref / StatsBomb | ⚽ | xG, xA, 2017–now | Free/Paid |
| Understat | ⚽ | xG per shot, 2014–now | Free scrape |
| Betfair Exchange API | 🌐 | Live + historical | Free API |
| OddsPortal | 🌐 | 50+ букмекеров, 2005–now | Free scrape |
| Pinnacle API | 🌐 | Sharp odds, realtime | Paid |
| Jeff Sackmann / GitHub | 🎾 | ATP 1968 – now | Open Source |
| tennis-data.co.uk | 🎾 | 2000–now + odds | Free CSV |
| NBA Stats API | 🏀 | 1946–now | Free |
| NHL Official API | 🏒 | 1917–now | Free |

---

## 💡 Strategy Script API

```javascript
function evaluate(match, team, h2h, market) {
  // match.team_home, match.team_away, match.league, match.date
  // match.odds_home, match.odds_draw, match.odds_away
  // match.odds_over, match.odds_under, match.odds_btts

  const form = team.form(match.team_home, 5);
  const wins = form.filter(r => r === 'W').length;
  const prob = wins / 5 + 0.05;
  
  const edge = market.value(match.odds_home, prob); // prob - implied
  if (edge > 0.05 && match.odds_home >= 1.6 && match.odds_home <= 3.5) {
    return {
      signal: true,
      market: 'home',      // 'home' | 'draw' | 'away' | 'over' | 'under' | 'btts'
      stake: market.kelly(match.odds_home, prob),
      prob: prob
    };
  }
  return null; // не ставим
}
```

---

## 📊 Метрики бэктестинга

- ROI, Yield %, Win Rate
- Sharpe Ratio, Calmar Ratio
- Max Drawdown, Recovery Factor
- **CLV** (Closing Line Value) — ключевой индикатор навыка
- P-value, Z-score — статистическая значимость
- Monthly heatmap, Equity curve с confidence interval
# betq1
# betq1
# betq1
# betq1
# betq1
